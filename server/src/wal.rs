//! Append-only write-ahead log: the STORAGE layer only.
//!
//! Two strictly separated layers (see the WAL plan §5.6):
//! - **Layer 1 (this file)** moves bytes: record framing + CRC32, segment files, fsync
//!   policies on a dedicated writer thread (the engine never does file I/O), atomic
//!   checkpoint-meta install, and the torn-tail recovery scan. It imports nothing from
//!   `mork`/`pathmap` — data structures reach it only as opaque bytes.
//! - **Layer 2** is the [`Rec`] schema: tag-dispatched, additive evolution. New execution
//!   models add new tags; existing tags are never reinterpreted.
//!
//! Determinism constraint (documented once, here): logical replay assumes the default
//! non-`interning` kernel build (raw symbol bytes in trie paths), `Space.timing == false`,
//! and the `periodic_merkleize` feature off — all defaults. The `interning` build would
//! need symbol-table persistence and is out of scope.
//!
//! On-disk layout under `--data-dir`:
//! ```text
//! checkpoint.meta          JSON, installed by temp+fsync+rename (atomic)
//! checkpoint-<v>.<fmt>     snapshot file named by the meta
//! wal-<seq:06>.log         segments: 8-byte magic, then length-prefixed records
//! *.tmp                    in-flight writes; swept on open
//! ```
//! Record framing (RocksDB-style): `len:u32le | crc32(payload):u32le | payload`, where
//! `payload := tag:u8 …`. A torn write at power loss becomes a cleanly detectable log
//! end, not corruption.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use crate::transaction::TxOk;

/// First 8 bytes of every segment file; identifies the format (and its version).
const MAGIC: &[u8; 8] = b"MORKWAL1";
/// Reader sanity cap: a length above this is treated as corruption.
const MAX_RECORD: u32 = 256 * 1024 * 1024;
/// How stale the page cache may get under [`FsyncPolicy::Everysec`].
const SYNC_INTERVAL: Duration = Duration::from_secs(1);
/// Writer-queue depth: the backpressure valve. If the disk persistently can't keep up,
/// `append` eventually blocks the caller rather than growing without bound.
const QUEUE_DEPTH: usize = 4096;

/// When the log is fsynced — the durability/latency knob (`--fsync`).
///
/// Fsync is the only expensive operation in the write path; how often it runs
/// is exactly the trade between "200 means on disk" and "200 means on disk within ~1 s".
///
/// Enforced entirely on the writer thread — the policy never changes what the
/// engine does, only when a batch is synced and when [`Ack`]s fire (Redis `appendfsync`
/// semantics).
#[derive(Clone, Copy, PartialEq, Debug, clap::ValueEnum)]
pub enum FsyncPolicy {
    /// Acks fire only after their batch is fsynced (group commit): 200 means "on disk".
    Always,
    /// Fsync on a ~1 s timer on the writer thread; power loss may drop the last second.
    Everysec,
    /// Never fsync explicitly; the OS page cache decides. Survives a process crash only.
    No,
}

/// The record schema (Layer 2) — one variant per transaction-lifecycle fact.
///
/// With deterministic sequential execution these three facts are the ENTIRE
/// durable history: what arrived (`Tx`), how far it ran (`Commit.steps`), or that it
/// never happened (`Abort`). Replay = re-execute; no trie changes are ever logged.
/// `Commit.steps` semantically encodes "sequential, deterministic replay" — a future
/// parallel scheduler adds a new tag rather than reinterpreting this one.
///
/// Borrowed fields, because the hot path just encodes into a frame and moves
/// on; [`OwnedRec`] is the decoded twin the recovery scan hands back.
pub enum Rec<'a> {
    Tx {
        id: &'a str,
        source: &'a str,
    },
    Commit {
        id: &'a str,
        steps: u64,
        version: u64,
    },
    Abort {
        id: &'a str,
        reason: &'a str,
    },
}

/// Owned twin of [`Rec`], produced by [`read_segments`] during recovery (the borrowed
/// form can't outlive the file buffer it was decoded from).
#[derive(Clone, Debug, PartialEq)]
pub enum OwnedRec {
    Tx {
        id: String,
        source: String,
    },
    Commit {
        id: String,
        steps: u64,
        version: u64,
    },
    Abort {
        id: String,
        reason: String,
    },
}

/// A client reply to fire once its record is durable per policy.
///
/// Redo-only logging requires durable-before-ACK, not durable-before-apply —
/// so even under `always` the ENGINE never waits on a disk; only the client's 200 does.
///
/// The engine hands the prebuilt `TxOk` + oneshot sender to `append`; the
/// writer thread fires it after the batch fsync (`always`) or right after the batch
/// write (`everysec`/`no` — though there the engine usually replies itself and passes
/// no ack at all).
pub struct Ack {
    pub reply: tokio::sync::oneshot::Sender<Result<TxOk, String>>,
    pub ok: TxOk,
}

// ---------------------------------------------------------------------------
// Record encoding

/// Payload bytes for one record: `tag:u8 | txid_len:u8 | txid | tag-specific rest`.
fn encode_payload(rec: &Rec) -> Vec<u8> {
    let mut p = Vec::new();
    match rec {
        Rec::Tx { id, source } => {
            p.push(1);
            p.push(id.len() as u8);
            p.extend_from_slice(id.as_bytes());
            p.extend_from_slice(source.as_bytes());
        }
        Rec::Commit { id, steps, version } => {
            p.push(2);
            p.push(id.len() as u8);
            p.extend_from_slice(id.as_bytes());
            p.extend_from_slice(&steps.to_le_bytes());
            p.extend_from_slice(&version.to_le_bytes());
        }
        Rec::Abort { id, reason } => {
            p.push(3);
            p.push(id.len() as u8);
            p.extend_from_slice(id.as_bytes());
            p.extend_from_slice(reason.as_bytes());
        }
    }
    p
}

/// A complete on-disk frame: `len:u32le | crc32(payload):u32le | payload`.
fn frame(rec: &Rec) -> Vec<u8> {
    let p = encode_payload(rec);
    let mut out = Vec::with_capacity(8 + p.len());
    out.extend_from_slice(&(p.len() as u32).to_le_bytes());
    out.extend_from_slice(&crc32fast::hash(&p).to_le_bytes());
    out.extend_from_slice(&p);
    out
}

/// Inverse of [`encode_payload`]; any structural violation (or unknown tag) is an error
/// the scan treats as corruption.
fn decode_payload(p: &[u8]) -> Result<OwnedRec, String> {
    let err = || "malformed record payload".to_string();
    if p.len() < 2 {
        return Err(err());
    }
    let id_len = p[1] as usize;
    let rest_at = 2 + id_len;
    if p.len() < rest_at {
        return Err(err());
    }
    let id = str::from_utf8(&p[2..rest_at])
        .map_err(|_| err())?
        .to_string();
    let rest = &p[rest_at..];
    match p[0] {
        1 => Ok(OwnedRec::Tx {
            id,
            source: str::from_utf8(rest).map_err(|_| err())?.to_string(),
        }),
        2 => {
            if rest.len() != 16 {
                return Err(err());
            }
            Ok(OwnedRec::Commit {
                id,
                steps: u64::from_le_bytes(rest[..8].try_into().unwrap()),
                version: u64::from_le_bytes(rest[8..].try_into().unwrap()),
            })
        }
        3 => Ok(OwnedRec::Abort {
            id,
            reason: str::from_utf8(rest).map_err(|_| err())?.to_string(),
        }),
        t => Err(format!(
            "unknown record tag {t} (written by a newer server?)"
        )),
    }
}

// ---------------------------------------------------------------------------
// Segments

/// `<dir>/wal-<seq:06>.log`.
fn seg_path(dir: &Path, seq: u64) -> PathBuf {
    dir.join(format!("wal-{seq:06}.log"))
}

/// Sequence number of a `wal-<seq>.log` filename; `None` for anything else.
fn parse_seg_name(name: &str) -> Option<u64> {
    name.strip_prefix("wal-")?
        .strip_suffix(".log")?
        .parse()
        .ok()
}

/// All segment files in `dir`, sorted by sequence number.
fn list_segments(dir: &Path) -> io::Result<Vec<(u64, PathBuf)>> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if let Some(seq) = entry.file_name().to_str().and_then(parse_seg_name) {
            out.push((seq, entry.path()));
        }
    }
    out.sort_by_key(|(seq, _)| *seq);
    Ok(out)
}

/// Fsync the directory itself — file creations/renames are durable only after this.
fn fsync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

/// Create a fresh, durable, magic-only segment.
fn create_segment(dir: &Path, seq: u64) -> io::Result<PathBuf> {
    let path = seg_path(dir, seq);
    let mut f = File::create(&path)?;
    f.write_all(MAGIC)?;
    f.sync_data()?;
    fsync_dir(dir)?;
    Ok(path)
}

/// `InvalidData` shorthand for the scan's hard-corruption errors.
fn corrupt(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

/// Read every record in segments `>= first_segment`, in order.
///
/// Torn-tail rule: the FIRST bad frame (short read, over-cap length, CRC mismatch,
/// undecodable payload) in the LAST segment marks the log end — the file is truncated
/// there (that's the expected power-loss shape). The same damage in an EARLIER segment
/// is real data loss and a hard error: refuse to serve rather than silently skip.
///
/// Eager `Vec` on purpose: recovery replays everything anyway, and the tail since the
/// last checkpoint is bounded by the checkpoint interval.
pub fn read_segments(dir: &Path, first_segment: u64) -> io::Result<Vec<OwnedRec>> {
    let segments: Vec<(u64, PathBuf)> = list_segments(dir)?
        .into_iter()
        .filter(|(seq, _)| *seq >= first_segment)
        .collect();
    let mut out = Vec::new();

    for (i, (seq, path)) in segments.iter().enumerate() {
        let is_last = i == segments.len() - 1;
        let buf = fs::read(path)?;
        let mut bad: Option<usize> = None; // offset of the first bad frame

        if buf.len() < 8 || &buf[..8] != MAGIC {
            if !is_last {
                return Err(corrupt(format!("segment {seq}: bad magic")));
            }
            // Interrupted segment creation: reset to a fresh magic-only file.
            bad = Some(0);
        } else {
            let mut off = 8usize;
            while off < buf.len() {
                if off + 8 > buf.len() {
                    bad = Some(off);
                    break;
                }
                let len = u32::from_le_bytes(buf[off..off + 4].try_into().unwrap());
                let crc = u32::from_le_bytes(buf[off + 4..off + 8].try_into().unwrap());
                let start = off + 8;
                let end = start + len as usize;
                if len > MAX_RECORD || end > buf.len() || crc32fast::hash(&buf[start..end]) != crc {
                    bad = Some(off);
                    break;
                }
                match decode_payload(&buf[start..end]) {
                    Ok(rec) => out.push(rec),
                    Err(_) => {
                        bad = Some(off);
                        break;
                    }
                }
                off = end;
            }
        }

        if let Some(off) = bad {
            if !is_last {
                return Err(corrupt(format!(
                    "segment {seq}: corrupt record at byte {off}"
                )));
            }
            log::warn!("wal: truncating torn tail of segment {seq} at byte {off}");
            let f = OpenOptions::new().write(true).open(path)?;
            if off < 8 {
                f.set_len(0)?;
                (&f).write_all(MAGIC)?;
            } else {
                f.set_len(off as u64)?;
            }
            f.sync_data()?;
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Checkpoint metadata

/// `checkpoint.meta`: installed only by atomic rename AFTER the snapshot file it names is
/// durable, so "meta parses ⇒ checkpoint valid". `format` names the snapshot serializer
/// (today `paths`); `first_segment` is where replay starts.
#[derive(Clone, Debug, PartialEq)]
pub struct CkptMeta {
    pub snapshot: String,
    pub format: String,
    pub version: u64,
    pub tx_counter: u64,
    pub first_segment: u64,
}

const META_NAME: &str = "checkpoint.meta";

impl CkptMeta {
    /// `Ok(None)` when no checkpoint exists yet; a hard error on unparseable JSON —
    /// the install rename is atomic, so garbage here is real corruption, never a state
    /// to guess around.
    pub fn load(dir: &Path) -> io::Result<Option<CkptMeta>> {
        let bytes = match fs::read(dir.join(META_NAME)) {
            Ok(b) => b,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        let v: serde_json::Value =
            serde_json::from_slice(&bytes).map_err(|e| corrupt(format!("checkpoint.meta: {e}")))?;
        let s = |k: &str| {
            v.get(k)
                .and_then(|x| x.as_str())
                .map(str::to_string)
                .ok_or_else(|| corrupt(format!("checkpoint.meta: missing '{k}'")))
        };
        let n = |k: &str| {
            v.get(k)
                .and_then(|x| x.as_u64())
                .ok_or_else(|| corrupt(format!("checkpoint.meta: missing '{k}'")))
        };
        Ok(Some(CkptMeta {
            snapshot: s("snapshot")?,
            format: s("format")?,
            version: n("version")?,
            tx_counter: n("tx_counter")?,
            first_segment: n("first_segment")?,
        }))
    }

    /// Atomic install: write `.tmp`, fsync, rename over the old meta, fsync the dir.
    #[allow(dead_code)] // caller arrives with the checkpoint commit
    pub fn store(&self, dir: &Path) -> io::Result<()> {
        let json = serde_json::json!({
            "snapshot": self.snapshot,
            "format": self.format,
            "version": self.version,
            "tx_counter": self.tx_counter,
            "first_segment": self.first_segment,
        })
        .to_string();
        let tmp = dir.join(format!("{META_NAME}.tmp"));
        let mut f = File::create(&tmp)?;
        f.write_all(json.as_bytes())?;
        f.sync_data()?;
        drop(f);
        fs::rename(&tmp, dir.join(META_NAME))?;
        fsync_dir(dir)
    }
}

// ---------------------------------------------------------------------------
// The Wal handle + writer thread

/// One unit of work for the writer thread: a pre-framed record and its optional ack.
enum Cmd {
    Append { frame: Vec<u8>, ack: Option<Ack> },
}

/// The engine's handle to the log — queue records, observe poisoning.
///
/// The WAL must never block PathMap mutation, so this handle contains no file;
/// all I/O lives on the `mork-wal` writer thread and the engine's entire per-record cost
/// is encoding + one bounded-channel send.
///
/// **How**:
/// ```ignore
/// let recs = wal::read_segments(dir, first_segment)?;  // scan first: truncates torn tail
/// let wal = Wal::open(dir, FsyncPolicy::Everysec)?;    // appends to what the scan left
/// wal.append(Rec::Tx { id, source }, ack);             // ack fires when durable (µs for the caller)
/// wal.append(Rec::Commit { id, steps, version }, None);
/// // drop (or .shutdown()) drains the queue, final-fsyncs, joins the thread
/// ```
pub struct Wal {
    send: Option<mpsc::SyncSender<Cmd>>,
    poisoned: Arc<AtomicBool>,
    writer: Option<std::thread::JoinHandle<()>>,
}

impl Wal {
    /// Open (or create) the newest segment for append and start the writer thread.
    /// Run [`read_segments`] BEFORE this: the scan is what truncates a torn tail.
    pub fn open(dir: &Path, policy: FsyncPolicy) -> io::Result<Wal> {
        fs::create_dir_all(dir)?;
        // Sweep in-flight temp files from an interrupted checkpoint install.
        for entry in fs::read_dir(dir)? {
            let path = entry?.path();
            if path.extension().is_some_and(|e| e == "tmp") {
                let _ = fs::remove_file(&path);
            }
        }

        let path = match list_segments(dir)?.last() {
            Some((_, p)) => p.clone(),
            None => create_segment(dir, 0)?,
        };
        let file = OpenOptions::new().append(true).open(&path)?;
        let poisoned = Arc::new(AtomicBool::new(false));
        let (send, recv) = mpsc::sync_channel(QUEUE_DEPTH);
        let writer = std::thread::Builder::new().name("mork-wal".into()).spawn({
            let poisoned = poisoned.clone();
            move || writer_loop(recv, file, policy, poisoned)
        })?;
        Ok(Wal {
            send: Some(send),
            poisoned,
            writer: Some(writer),
        })
    }

    /// Queue one record. Non-blocking unless the writer queue is full (the backpressure
    /// valve). If `ack` is given it fires once the record is durable per policy.
    pub fn append(&self, rec: Rec<'_>, ack: Option<Ack>) {
        if self.poisoned() {
            if let Some(a) = ack {
                let _ = a
                    .reply
                    .send(Err("wal is poisoned (earlier write error)".into()));
            }
            return;
        }
        let cmd = Cmd::Append {
            frame: frame(&rec),
            ack,
        };
        if let Err(mpsc::SendError(Cmd::Append { ack: Some(a), .. })) = self
            .send
            .as_ref()
            .expect("wal used after shutdown")
            .send(cmd)
        {
            let _ = a.reply.send(Err("wal writer thread is gone".into()));
        }
    }

    /// A write or fsync failed (e.g. disk full): acked durability can no longer be
    /// promised. The engine should refuse new transactions while this is set.
    pub fn poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Relaxed)
    }

    /// Drain the queue, final-fsync, and join the writer. (Dropping does the same; the
    /// engine relies on drop order, so this explicit form is used by tests only.)
    #[allow(dead_code)]
    pub fn shutdown(self) {}
}

impl Drop for Wal {
    fn drop(&mut self) {
        drop(self.send.take()); // disconnect: the writer drains what's queued, then exits
        if let Some(h) = self.writer.take() {
            let _ = h.join();
        }
    }
}

/// The writer thread's loop: park for work (or the `everysec` sync deadline), drain
/// everything queued into one write — and under `always` one fsync — then fire the
/// batch's acks. Poisons the log on any write/fsync error; final-fsyncs on disconnect.
fn writer_loop(
    recv: mpsc::Receiver<Cmd>,
    mut file: File,
    policy: FsyncPolicy,
    poisoned: Arc<AtomicBool>,
) {
    let mut dirty = false;
    let mut last_sync = Instant::now();
    let poison = |e: &io::Error, acks: &mut Vec<Ack>, poisoned: &AtomicBool| {
        log::error!("wal: write error, poisoning the log: {e}");
        poisoned.store(true, Ordering::Relaxed);
        for a in acks.drain(..) {
            let _ = a.reply.send(Err(format!("wal write failed: {e}")));
        }
    };

    loop {
        // Park for work; with a dirty file under `everysec`, wake at the sync deadline.
        let first = if policy == FsyncPolicy::Everysec && dirty {
            let deadline = last_sync + SYNC_INTERVAL;
            match recv.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
                Ok(c) => Some(c),
                Err(mpsc::RecvTimeoutError::Timeout) => None,
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
        } else {
            match recv.recv() {
                Ok(c) => Some(c),
                Err(_) => break,
            }
        };

        // Batch: one write (and under `always`, one fsync) for everything queued behind.
        let mut batch = Vec::new();
        let mut acks = Vec::new();
        let mut take = |cmd: Cmd, batch: &mut Vec<u8>, acks: &mut Vec<Ack>| {
            let Cmd::Append { frame, ack } = cmd;
            batch.extend_from_slice(&frame);
            if let Some(a) = ack {
                acks.push(a);
            }
        };
        if let Some(cmd) = first {
            take(cmd, &mut batch, &mut acks);
        }
        for cmd in recv.try_iter() {
            take(cmd, &mut batch, &mut acks);
        }

        if !batch.is_empty() {
            if poisoned.load(Ordering::Relaxed) {
                for a in acks.drain(..) {
                    let _ = a.reply.send(Err("wal is poisoned".into()));
                }
                continue;
            }
            if let Err(e) = file.write_all(&batch) {
                poison(&e, &mut acks, &poisoned);
                continue;
            }
            dirty = true;
        }

        match policy {
            FsyncPolicy::Always => {
                if dirty {
                    if let Err(e) = file.sync_data() {
                        poison(&e, &mut acks, &poisoned);
                        continue;
                    }
                    dirty = false;
                    last_sync = Instant::now();
                }
                for Ack { reply, ok } in acks.drain(..) {
                    let _ = reply.send(Ok(ok));
                }
            }
            FsyncPolicy::Everysec => {
                for Ack { reply, ok } in acks.drain(..) {
                    let _ = reply.send(Ok(ok));
                }
                if dirty && last_sync.elapsed() >= SYNC_INTERVAL {
                    if let Err(e) = file.sync_data() {
                        poison(&e, &mut acks, &poisoned);
                        continue;
                    }
                    dirty = false;
                    last_sync = Instant::now();
                }
            }
            FsyncPolicy::No => {
                for Ack { reply, ok } in acks.drain(..) {
                    let _ = reply.send(Ok(ok));
                }
            }
        }
    }

    // Shutdown: everything queued was drained above (recv yields buffered commands
    // before reporting disconnect); leave the file durable.
    if dirty {
        let _ = file.sync_data();
    }
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    fn tmpdir() -> PathBuf {
        static N: AtomicUsize = AtomicUsize::new(0);
        let d = std::env::temp_dir().join(format!(
            "mork-wal-test-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&d);
        fs::create_dir_all(&d).unwrap();
        d
    }

    fn write_frames(path: &Path, recs: &[Rec]) {
        let mut f = OpenOptions::new().append(true).open(path).unwrap();
        for r in recs {
            f.write_all(&frame(r)).unwrap();
        }
    }

    #[test]
    fn roundtrip_all_tags() {
        let recs = [
            Rec::Tx {
                id: "tx1_abcd1234",
                source: "(a b)\n(exec (tx1_abcd1234 0) $x $y)\n",
            },
            Rec::Tx {
                id: "tx2_efgh5678",
                source: "(unicode ⍼ \"quoted \\\" str\")",
            },
            Rec::Commit {
                id: "tx1_abcd1234",
                steps: 42,
                version: 1234567890123,
            },
            Rec::Abort {
                id: "tx2_efgh5678",
                reason: "exec (bad): pattern functor",
            },
        ];
        for r in &recs {
            let f = frame(r);
            let len = u32::from_le_bytes(f[..4].try_into().unwrap()) as usize;
            assert_eq!(len + 8, f.len());
            let dec = decode_payload(&f[8..]).unwrap();
            match (r, &dec) {
                (Rec::Tx { id, source }, OwnedRec::Tx { id: i2, source: s2 }) => {
                    assert_eq!((*id, *source), (i2.as_str(), s2.as_str()));
                }
                (
                    Rec::Commit { id, steps, version },
                    OwnedRec::Commit {
                        id: i2,
                        steps: st2,
                        version: v2,
                    },
                ) => {
                    assert_eq!((*id, *steps, *version), (i2.as_str(), *st2, *v2));
                }
                (Rec::Abort { id, reason }, OwnedRec::Abort { id: i2, reason: r2 }) => {
                    assert_eq!((*id, *reason), (i2.as_str(), r2.as_str()));
                }
                _ => panic!("tag mismatch"),
            }
        }
    }

    #[test]
    fn torn_tail_truncates_and_keeps_prior_records() {
        let dir = tmpdir();
        create_segment(&dir, 0).unwrap();
        let p = seg_path(&dir, 0);
        write_frames(
            &p,
            &[
                Rec::Tx {
                    id: "tx1_aaaaaaaa",
                    source: "(a)",
                },
                Rec::Commit {
                    id: "tx1_aaaaaaaa",
                    steps: 1,
                    version: 2,
                },
                Rec::Tx {
                    id: "tx2_bbbbbbbb",
                    source: "(b)",
                },
            ],
        );
        // tear the last record: chop 3 bytes off the file
        let full = fs::metadata(&p).unwrap().len();
        OpenOptions::new()
            .write(true)
            .open(&p)
            .unwrap()
            .set_len(full - 3)
            .unwrap();

        let recs = read_segments(&dir, 0).unwrap();
        assert_eq!(recs.len(), 2);
        assert!(matches!(
            &recs[1],
            OwnedRec::Commit {
                steps: 1,
                version: 2,
                ..
            }
        ));
        // the file was truncated at the last good record — a re-scan is clean and stable
        let shrunk = fs::metadata(&p).unwrap().len();
        assert!(shrunk < full - 3);
        assert_eq!(read_segments(&dir, 0).unwrap(), recs);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn crc_flip_in_last_segment_truncates_there() {
        let dir = tmpdir();
        create_segment(&dir, 0).unwrap();
        let p = seg_path(&dir, 0);
        write_frames(
            &p,
            &[
                Rec::Tx {
                    id: "tx1_aaaaaaaa",
                    source: "(a)",
                },
                Rec::Tx {
                    id: "tx2_bbbbbbbb",
                    source: "(b)",
                },
            ],
        );
        let mut buf = fs::read(&p).unwrap();
        let last = buf.len() - 1; // inside the second record's payload
        buf[last] ^= 0xff;
        fs::write(&p, &buf).unwrap();

        let recs = read_segments(&dir, 0).unwrap();
        assert_eq!(
            recs,
            vec![OwnedRec::Tx {
                id: "tx1_aaaaaaaa".into(),
                source: "(a)".into()
            }]
        );
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn corruption_in_non_last_segment_is_a_hard_error() {
        let dir = tmpdir();
        create_segment(&dir, 0).unwrap();
        write_frames(
            &seg_path(&dir, 0),
            &[Rec::Tx {
                id: "tx1_aaaaaaaa",
                source: "(a)",
            }],
        );
        create_segment(&dir, 1).unwrap();
        write_frames(
            &seg_path(&dir, 1),
            &[Rec::Tx {
                id: "tx2_bbbbbbbb",
                source: "(b)",
            }],
        );
        // flip a payload byte in segment 0 (not the last)
        let p0 = seg_path(&dir, 0);
        let mut buf = fs::read(&p0).unwrap();
        let last = buf.len() - 1;
        buf[last] ^= 0xff;
        fs::write(&p0, &buf).unwrap();

        assert!(read_segments(&dir, 0).is_err());
        // starting AFTER the damaged segment still works
        assert_eq!(read_segments(&dir, 1).unwrap().len(), 1);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn reads_across_segments_in_order() {
        let dir = tmpdir();
        create_segment(&dir, 0).unwrap();
        write_frames(
            &seg_path(&dir, 0),
            &[Rec::Tx {
                id: "tx1_aaaaaaaa",
                source: "(a)",
            }],
        );
        create_segment(&dir, 1).unwrap();
        write_frames(
            &seg_path(&dir, 1),
            &[Rec::Commit {
                id: "tx1_aaaaaaaa",
                steps: 3,
                version: 4,
            }],
        );
        let recs = read_segments(&dir, 0).unwrap();
        assert_eq!(recs.len(), 2);
        assert!(matches!(recs[0], OwnedRec::Tx { .. }));
        assert!(matches!(recs[1], OwnedRec::Commit { .. }));
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn meta_roundtrip_and_missing() {
        let dir = tmpdir();
        assert_eq!(CkptMeta::load(&dir).unwrap(), None);
        let meta = CkptMeta {
            snapshot: "checkpoint-000123.paths".into(),
            format: "paths".into(),
            version: 123,
            tx_counter: 45,
            first_segment: 7,
        };
        meta.store(&dir).unwrap();
        assert_eq!(CkptMeta::load(&dir).unwrap(), Some(meta));
        // garbage meta is a hard error, not a silent None
        fs::write(dir.join(META_NAME), b"{not json").unwrap();
        assert!(CkptMeta::load(&dir).is_err());
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn wal_appends_are_durable_and_ack_fires_after_fsync_under_always() {
        let dir = tmpdir();
        let wal = Wal::open(&dir, FsyncPolicy::Always).unwrap();
        let (reply, rx) = tokio::sync::oneshot::channel();
        let ok = TxOk {
            tx: "tx1_aaaaaaaa".into(),
            count: 2,
            version: 1,
        };
        wal.append(
            Rec::Tx {
                id: "tx1_aaaaaaaa",
                source: "(a b)",
            },
            Some(Ack { reply, ok }),
        );
        let acked = rx.blocking_recv().unwrap().unwrap();
        assert_eq!(acked.version, 1);
        wal.append(
            Rec::Commit {
                id: "tx1_aaaaaaaa",
                steps: 1,
                version: 2,
            },
            None,
        );
        wal.shutdown(); // drains + joins

        let recs = read_segments(&dir, 0).unwrap();
        assert_eq!(recs.len(), 2);
        assert_eq!(
            recs[0],
            OwnedRec::Tx {
                id: "tx1_aaaaaaaa".into(),
                source: "(a b)".into()
            }
        );
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn wal_open_appends_to_existing_segment_and_sweeps_tmp() {
        let dir = tmpdir();
        fs::write(dir.join("checkpoint.meta.tmp"), b"junk").unwrap();
        {
            let wal = Wal::open(&dir, FsyncPolicy::No).unwrap();
            wal.append(
                Rec::Tx {
                    id: "tx1_aaaaaaaa",
                    source: "(a)",
                },
                None,
            );
        } // drop = drain + join
        assert!(!dir.join("checkpoint.meta.tmp").exists());
        {
            let wal = Wal::open(&dir, FsyncPolicy::No).unwrap();
            wal.append(
                Rec::Tx {
                    id: "tx2_bbbbbbbb",
                    source: "(b)",
                },
                None,
            );
        }
        let recs = read_segments(&dir, 0).unwrap();
        assert_eq!(recs.len(), 2, "reopen must append, not overwrite");
        fs::remove_dir_all(&dir).unwrap();
    }
}
