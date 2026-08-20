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

/// The record schema (Layer 2) — one variant per durable transaction fact.
///
/// Under MVCC an uncommitted transaction is invisible: nothing it did is ever
/// observable until `commit` installs it. So a crash mid-run and an explicit abort
/// are the same fact from the log's point of view — "this never happened" — and
/// neither needs a record. The old `Tx`/`Abort` pairing (log what arrived, log
/// whether it stuck) existed only because the previous sequential engine applied a
/// transaction to the live trie before it knew whether it had succeeded; the new
/// committer never does that, so one record per transaction is the entire history.
///
/// `base_version` is the version of the committed trie the transaction actually ran
/// against. Recovery replays each transaction from ITS OWN base rather than from
/// whatever the serial predecessor happened to leave behind — required because
/// concurrent workers can commit in an order that differs from the order they began.
///
/// Tag `1` is reused from the old `Tx` tag; the two schemas are not
/// forward-compatible in any case, and the format is not versioned across this
/// change (see `decode_payload`'s "written by a newer server?" for what a foreign
/// tag means to a reader).
///
/// **This is not just a versioning footnote — it's a silent-corruption trap for
/// whoever writes recovery next.** An old-format `Rec::Tx` payload (`tag=1 | id_len
/// | id | source`) fed to the new decoder does NOT reliably error: once `source` is
/// ≥24 bytes (true of any real request body), `decode_payload` happily reads 24
/// bytes of what used to be `source` as `base_version`/`steps`/`version` and hands
/// back whatever's left as a truncated `source` — no error, just wrong values. The
/// CRC does not catch this: it validates the bytes as written, not how they're
/// interpreted. Do not point recovery at a data directory written before this
/// change.
///
/// Borrowed fields, because the hot path just encodes into a frame and moves
/// on; [`OwnedRec`] is the decoded twin the recovery scan hands back.
pub enum Rec<'a> {
    Commit {
        id: &'a str,
        base_version: u64,
        source: &'a str,
        steps: u64,
        version: u64,
    },
}

/// Owned twin of [`Rec`], produced by [`read_segments`] during recovery (the borrowed
/// form can't outlive the file buffer it was decoded from).
#[derive(Clone, Debug, PartialEq)]
pub enum OwnedRec {
    Commit {
        id: String,
        base_version: u64,
        source: String,
        steps: u64,
        version: u64,
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

/// What a failed [`Ack`] tells the client. Carries the `unavailable:` prefix so
/// `http.rs` answers 503 rather than 422 — the request was fine, the disk was not.
///
/// Deliberately says "may be visible": under `always` the reply is only handed over
/// *after* `commit` installed and published, so a transaction that fails here is live
/// in the space until a restart replays the log without it. That is the redo-log
/// trade — the alternative is holding every commit until its fsync lands, which would
/// serialize the whole engine behind the disk.
pub const NOT_DURABLE: &str =
    "unavailable: the write-ahead log could not durably record this transaction; it may be \
     visible to readers until the server is restarted, but it is not durable — treat it as \
     lost and retry once the disk is healthy";

// ---------------------------------------------------------------------------
// Record encoding

/// Payload bytes for one record: `tag:u8 | id_len:u8 | id | base_version:u64le |
/// steps:u64le | version:u64le | source (rest of payload)`.
///
/// `source` is unprefixed and trailing — an entire request body routinely exceeds
/// 255 bytes, so unlike `id` it can't use the same `u8` length prefix without
/// silently truncating; keeping it as "everything after the fixed fields" sidesteps
/// the width question the way the old `Tx`/`Abort` trailing strings already did.
fn encode_payload(rec: &Rec) -> Vec<u8> {
    let mut p = Vec::new();
    match rec {
        Rec::Commit { id, base_version, source, steps, version } => {
            debug_assert!(
                id.len() <= u8::MAX as usize,
                "txid too long for the u8 length prefix: {}",
                id.len()
            );
            p.push(1);
            p.push(id.len() as u8);
            p.extend_from_slice(id.as_bytes());
            p.extend_from_slice(&base_version.to_le_bytes());
            p.extend_from_slice(&steps.to_le_bytes());
            p.extend_from_slice(&version.to_le_bytes());
            p.extend_from_slice(source.as_bytes());
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
        1 => {
            if rest.len() < 24 {
                return Err(err());
            }
            let base_version = u64::from_le_bytes(rest[0..8].try_into().unwrap());
            let steps = u64::from_le_bytes(rest[8..16].try_into().unwrap());
            let version = u64::from_le_bytes(rest[16..24].try_into().unwrap());
            let source = str::from_utf8(&rest[24..]).map_err(|_| err())?.to_string();
            Ok(OwnedRec::Commit { id, base_version, source, steps, version })
        }
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

/// **What**: the snapshot serializer a checkpoint runs — the caller captures whatever
/// image it wants persisted (today an O(1) PathMap clone) and this layer never sees the
/// data structure, only bytes flowing into a writer. This is the §5.6 decoupling point:
/// swapping the snapshot format (`.paths` → ACT) is a new closure + a new `format` tag,
/// zero storage-layer changes.
pub type SnapshotFn = Box<dyn FnOnce(&mut dyn Write) -> io::Result<()> + Send>;

/// One unit of work for the writer thread: a pre-framed record and its optional ack, or
/// a rotate-and-checkpoint request (ordered with the appends by the FIFO channel — that
/// ordering is what puts pre-checkpoint records in old segments and post-checkpoint
/// records in the new one).
enum Cmd {
    Append { frame: Vec<u8>, ack: Option<Ack> },
    Checkpoint { version: u64, tx_counter: u64, format: &'static str, serialize: SnapshotFn },
}

/// A rotate-complete checkpoint handed from the writer thread to the checkpointer.
struct CkptJob {
    version: u64,
    tx_counter: u64,
    format: &'static str,
    serialize: SnapshotFn,
    /// The freshly created segment: replay starts here once the meta is installed.
    first_segment: u64,
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
/// wal.append(Rec::Commit { id, base_version, source, steps, version }, ack); // ack fires when durable (µs for the caller)
/// // drop (or .shutdown()) drains the queue, final-fsyncs, joins the thread
/// ```
pub struct Wal {
    send: Option<mpsc::SyncSender<Cmd>>,
    policy: FsyncPolicy,
    poisoned: Arc<AtomicBool>,
    /// One checkpoint in flight at a time; `checkpoint()` skips (returns false) while set.
    ckpt_busy: Arc<AtomicBool>,
    writer: Option<std::thread::JoinHandle<()>>,
    checkpointer: Option<std::thread::JoinHandle<()>>,
}

impl Wal {
    /// Open (or create) the newest segment for append and start the writer +
    /// checkpointer threads. Run [`read_segments`] BEFORE this: the scan is what
    /// truncates a torn tail.
    pub fn open(dir: &Path, policy: FsyncPolicy) -> io::Result<Wal> {
        fs::create_dir_all(dir)?;
        // Sweep leftovers of an interrupted checkpoint install: `.tmp` files, and any
        // snapshot file the (atomically installed) meta doesn't name.
        let kept_snapshot = CkptMeta::load(dir).ok().flatten().map(|m| m.snapshot);
        for entry in fs::read_dir(dir)? {
            let path = entry?.path();
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("").to_string();
            let orphan_snapshot =
                name.starts_with("checkpoint-") && Some(&name) != kept_snapshot.as_ref();
            if path.extension().is_some_and(|e| e == "tmp") || orphan_snapshot {
                let _ = fs::remove_file(&path);
            }
        }

        let (seq, path) = match list_segments(dir)?.last() {
            Some((seq, p)) => (*seq, p.clone()),
            None => (0, create_segment(dir, 0)?),
        };
        let file = OpenOptions::new().append(true).open(&path)?;
        let poisoned = Arc::new(AtomicBool::new(false));
        let ckpt_busy = Arc::new(AtomicBool::new(false));
        let (send, recv) = mpsc::sync_channel(QUEUE_DEPTH);
        // Capacity 1 + the busy gate ⇒ the writer's forward never blocks.
        let (job_send, job_recv) = mpsc::sync_channel::<CkptJob>(1);
        let checkpointer = std::thread::Builder::new().name("mork-wal-ckpt".into()).spawn({
            let dir = dir.to_path_buf();
            let busy = ckpt_busy.clone();
            move || checkpointer_loop(job_recv, dir, busy)
        })?;
        let writer = std::thread::Builder::new().name("mork-wal".into()).spawn({
            let poisoned = poisoned.clone();
            let busy = ckpt_busy.clone();
            let dir = dir.to_path_buf();
            move || writer_loop(recv, file, seq, dir, policy, poisoned, busy, job_send)
        })?;
        Ok(Wal {
            send: Some(send),
            policy,
            poisoned,
            ckpt_busy,
            writer: Some(writer),
            checkpointer: Some(checkpointer),
        })
    }

    /// Whether an [`Ack`] from this log means "fsynced". Only [`FsyncPolicy::Always`]
    /// promises it, so this is what tells a caller whether handing over the client's
    /// reply buys durable-before-ACK or just delays it for nothing.
    pub fn acks_mean_durable(&self) -> bool {
        self.policy == FsyncPolicy::Always
    }

    /// Queue one record. Non-blocking unless the writer queue is full (the backpressure
    /// valve). If `ack` is given it fires once the record is durable per policy.
    pub fn append(&self, rec: Rec<'_>, ack: Option<Ack>) {
        if self.poisoned() {
            if let Some(a) = ack {
                let _ = a
                    .reply
                    .send(Err(NOT_DURABLE.into()));
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
            let _ = a.reply.send(Err(NOT_DURABLE.into()));
        }
    }

    /// A write or fsync failed (e.g. disk full): acked durability can no longer be
    /// promised. The engine should refuse new transactions while this is set.
    pub fn poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Relaxed)
    }

    /// Force the poisoned flag, so tests can exercise the engine's refusal path without
    /// having to induce a real disk error.
    #[cfg(test)]
    pub fn poison_for_test(&self) {
        self.poisoned.store(true, Ordering::Relaxed);
    }

    /// **What**: rotate to a fresh segment and install a checkpoint, asynchronously.
    ///
    /// **Why**: the checkpoint is what lets recovery skip replaying history and lets old
    /// segments be deleted — without it the log grows without bound.
    ///
    /// **How**: the request queues behind pending appends (FIFO ⇒ everything before it
    /// lands in old segments), the writer fsyncs + rotates (fast), and the checkpointer
    /// thread runs `serialize` into a temp file, installs snapshot + meta atomically,
    /// then deletes segments `< first_segment` and superseded snapshots. Returns `false`
    /// — skip, retry at the next trigger — while a previous checkpoint is still writing.
    /// A failed install just leaves the old checkpoint standing (the log keeps growing).
    pub fn checkpoint(
        &self,
        version: u64,
        tx_counter: u64,
        format: &'static str,
        serialize: SnapshotFn,
    ) -> bool {
        if self.ckpt_busy.swap(true, Ordering::AcqRel) {
            return false;
        }
        if self.poisoned() {
            self.ckpt_busy.store(false, Ordering::Release);
            return false;
        }
        let cmd = Cmd::Checkpoint { version, tx_counter, format, serialize };
        if self.send.as_ref().expect("wal used after shutdown").send(cmd).is_err() {
            self.ckpt_busy.store(false, Ordering::Release);
            return false;
        }
        true
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
            let _ = h.join(); // exiting drops the writer's job sender…
        }
        if let Some(h) = self.checkpointer.take() {
            let _ = h.join(); // …so the checkpointer finishes any in-flight install and exits
        }
    }
}

/// The writer thread's loop: park for work (or the `everysec` sync deadline), drain
/// everything queued — appends coalesce into one write (and under `always` one fsync),
/// a checkpoint request flushes + fsyncs the current segment, rotates, and forwards the
/// install job to the checkpointer — then fire the batch's acks. Poisons the log on any
/// write/fsync error; final-fsyncs on disconnect.
#[allow(clippy::too_many_arguments)]
fn writer_loop(
    recv: mpsc::Receiver<Cmd>,
    mut file: File,
    mut seq: u64,
    dir: std::path::PathBuf,
    policy: FsyncPolicy,
    poisoned: Arc<AtomicBool>,
    ckpt_busy: Arc<AtomicBool>,
    job_send: mpsc::SyncSender<CkptJob>,
) {
    let mut dirty = false;
    let mut last_sync = Instant::now();
    let poison = |e: &io::Error, acks: &mut Vec<Ack>, poisoned: &AtomicBool| {
        log::error!("wal: write error, poisoning the log: {e}");
        poisoned.store(true, Ordering::Relaxed);
        for a in acks.drain(..) {
            let _ = a.reply.send(Err(format!("{NOT_DURABLE}: {e}")));
        }
    };

    'outer: loop {
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

        // Drain in order: appends coalesce; a checkpoint splits the stream — everything
        // before it belongs to old segments, everything after to the fresh one.
        let mut batch = Vec::new();
        let mut acks = Vec::new();
        for cmd in first.into_iter().chain(recv.try_iter()) {
            if poisoned.load(Ordering::Relaxed) {
                match cmd {
                    Cmd::Append { ack: Some(a), .. } => {
                        let _ = a.reply.send(Err(NOT_DURABLE.into()));
                    }
                    Cmd::Append { .. } => {}
                    Cmd::Checkpoint { .. } => ckpt_busy.store(false, Ordering::Release),
                }
                continue;
            }
            match cmd {
                Cmd::Append { frame, ack } => {
                    batch.extend_from_slice(&frame);
                    if let Some(a) = ack {
                        acks.push(a);
                    }
                }
                Cmd::Checkpoint { version, tx_counter, format, serialize } => {
                    // Flush + fsync the current segment first: rotation makes it
                    // non-last, and the scan treats a torn tail there as hard
                    // corruption — it must be clean before anything else can happen.
                    if !batch.is_empty() {
                        if let Err(e) = file.write_all(&batch) {
                            poison(&e, &mut acks, &poisoned);
                            ckpt_busy.store(false, Ordering::Release);
                            continue 'outer;
                        }
                        batch.clear();
                        dirty = true;
                    }
                    if dirty {
                        if let Err(e) = file.sync_data() {
                            poison(&e, &mut acks, &poisoned);
                            ckpt_busy.store(false, Ordering::Release);
                            continue 'outer;
                        }
                        dirty = false;
                        last_sync = Instant::now();
                    }
                    match create_segment(&dir, seq + 1) {
                        Ok(path) => match OpenOptions::new().append(true).open(&path) {
                            Ok(f) => {
                                file = f;
                                seq += 1;
                                let _ = job_send.send(CkptJob {
                                    version,
                                    tx_counter,
                                    format,
                                    serialize,
                                    first_segment: seq,
                                });
                                // busy stays set: the checkpointer clears it when done
                            }
                            Err(e) => {
                                log::error!("wal: opening rotated segment failed, checkpoint skipped: {e}");
                                ckpt_busy.store(false, Ordering::Release);
                            }
                        },
                        Err(e) => {
                            log::error!("wal: segment rotation failed, checkpoint skipped: {e}");
                            ckpt_busy.store(false, Ordering::Release);
                        }
                    }
                }
            }
        }

        if !batch.is_empty() {
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

/// The checkpointer thread: runs at most one install at a time (the busy flag gates
/// submissions), and a failure just leaves the previous checkpoint standing.
fn checkpointer_loop(recv: mpsc::Receiver<CkptJob>, dir: std::path::PathBuf, busy: Arc<AtomicBool>) {
    while let Ok(job) = recv.recv() {
        match install_checkpoint(&dir, job) {
            Ok(name) => log::info!("wal: checkpoint '{name}' installed, old segments deleted"),
            Err(e) => log::error!("wal: checkpoint failed (log keeps growing until the next attempt): {e}"),
        }
        busy.store(false, Ordering::Release);
    }
}

/// Serialize the snapshot to `<name>.tmp` → fsync → rename → fsync dir, install the
/// meta the same way, then GC: segments below `first_segment` and superseded snapshot
/// files. A crash at ANY point leaves either the old or the new checkpoint fully valid
/// (the meta rename is the commit point); strays are swept at the next `Wal::open`.
fn install_checkpoint(dir: &Path, job: CkptJob) -> io::Result<String> {
    let name = format!("checkpoint-{:06}.{}", job.version, job.format);
    let tmp = dir.join(format!("{name}.tmp"));
    let mut w = io::BufWriter::new(File::create(&tmp)?);
    (job.serialize)(&mut w)?;
    let f = w.into_inner().map_err(|e| io::Error::other(e.to_string()))?;
    f.sync_data()?;
    drop(f);
    fs::rename(&tmp, dir.join(&name))?;
    fsync_dir(dir)?;

    CkptMeta {
        snapshot: name.clone(),
        format: job.format.to_string(),
        version: job.version,
        tx_counter: job.tx_counter,
        first_segment: job.first_segment,
    }
    .store(dir)?; // the commit point

    for (seq, path) in list_segments(dir)? {
        if seq < job.first_segment {
            let _ = fs::remove_file(path);
        }
    }
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        let fname = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if fname.starts_with("checkpoint-") && fname != name && !fname.ends_with(".tmp") {
            let _ = fs::remove_file(&path);
        }
    }
    Ok(name)
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
    fn roundtrip_commit_tag() {
        // Only one tag remains (Tx/Abort collapsed into Commit); this now exercises
        // encode/decode across varied field values (long source, unicode, zero steps)
        // rather than across distinct tags.
        let recs = [
            Rec::Commit {
                id: "tx1_abcd1234",
                base_version: 0,
                source: "(a b)\n(exec (tx1_abcd1234 0) $x $y)\n",
                steps: 42,
                version: 1234567890123,
            },
            Rec::Commit {
                id: "tx2_efgh5678",
                base_version: 1234567890123,
                source: "(unicode ⍼ \"quoted \\\" str\")",
                steps: 0,
                version: 1234567890124,
            },
        ];
        for r in &recs {
            let f = frame(r);
            let len = u32::from_le_bytes(f[..4].try_into().unwrap()) as usize;
            assert_eq!(len + 8, f.len());
            let dec = decode_payload(&f[8..]).unwrap();
            match (r, &dec) {
                (
                    Rec::Commit { id, base_version, source, steps, version },
                    OwnedRec::Commit {
                        id: i2,
                        base_version: bv2,
                        source: s2,
                        steps: st2,
                        version: v2,
                    },
                ) => {
                    assert_eq!(
                        (*id, *base_version, *source, *steps, *version),
                        (i2.as_str(), *bv2, s2.as_str(), *st2, *v2)
                    );
                }
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
                Rec::Commit { id: "tx1_aaaaaaaa", base_version: 0, source: "(a)", steps: 1, version: 1 },
                Rec::Commit { id: "tx1_aaaaaaaa", base_version: 1, source: "(a2)", steps: 1, version: 2 },
                Rec::Commit { id: "tx2_bbbbbbbb", base_version: 2, source: "(b)", steps: 1, version: 3 },
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
                Rec::Commit { id: "tx1_aaaaaaaa", base_version: 0, source: "(a)", steps: 0, version: 1 },
                Rec::Commit { id: "tx2_bbbbbbbb", base_version: 1, source: "(b)", steps: 0, version: 2 },
            ],
        );
        let mut buf = fs::read(&p).unwrap();
        let last = buf.len() - 1; // inside the second record's payload
        buf[last] ^= 0xff;
        fs::write(&p, &buf).unwrap();

        let recs = read_segments(&dir, 0).unwrap();
        assert_eq!(
            recs,
            vec![OwnedRec::Commit {
                id: "tx1_aaaaaaaa".into(),
                base_version: 0,
                source: "(a)".into(),
                steps: 0,
                version: 1,
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
            &[Rec::Commit { id: "tx1_aaaaaaaa", base_version: 0, source: "(a)", steps: 0, version: 1 }],
        );
        create_segment(&dir, 1).unwrap();
        write_frames(
            &seg_path(&dir, 1),
            &[Rec::Commit { id: "tx2_bbbbbbbb", base_version: 1, source: "(b)", steps: 0, version: 2 }],
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
            &[Rec::Commit { id: "tx1_aaaaaaaa", base_version: 0, source: "(a)", steps: 0, version: 1 }],
        );
        create_segment(&dir, 1).unwrap();
        write_frames(
            &seg_path(&dir, 1),
            &[Rec::Commit { id: "tx2_bbbbbbbb", base_version: 1, source: "(b)", steps: 3, version: 4 }],
        );
        let recs = read_segments(&dir, 0).unwrap();
        assert_eq!(recs.len(), 2);
        let OwnedRec::Commit { id: id0, version: v0, .. } = &recs[0];
        let OwnedRec::Commit { id: id1, version: v1, .. } = &recs[1];
        assert_eq!((id0.as_str(), *v0), ("tx1_aaaaaaaa", 1));
        assert_eq!((id1.as_str(), *v1), ("tx2_bbbbbbbb", 4));
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn commit_record_roundtrips_with_base_version() {
        let dir = tmpdir();
        {
            let w = Wal::open(&dir, FsyncPolicy::Always).unwrap();
            w.append(Rec::Commit {
                id: "tx7_abcdefgh",
                base_version: 41,
                source: "(edge a b)\n",
                steps: 3,
                version: 42,
            }, None);
        }
        let recs = read_segments(&dir, 0).unwrap();
        assert_eq!(recs.len(), 1);
        match &recs[0] {
            OwnedRec::Commit { id, base_version, source, steps, version } => {
                assert_eq!(id, "tx7_abcdefgh");
                assert_eq!(*base_version, 41);
                assert_eq!(source, "(edge a b)\n");
                assert_eq!(*steps, 3);
                assert_eq!(*version, 42);
            }
        }
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn interleaved_commits_are_read_back_in_log_order() {
        // Under MVCC two transactions may share a base and commit in either order.
        // The log must preserve the order they were appended in — that IS the
        // serialization order recovery replays.
        let dir = tmpdir();
        {
            let w = Wal::open(&dir, FsyncPolicy::Always).unwrap();
            w.append(Rec::Commit { id: "txA", base_version: 0, source: "(a)\n", steps: 0, version: 1 }, None);
            w.append(Rec::Commit { id: "txB", base_version: 0, source: "(b)\n", steps: 0, version: 2 }, None);
        }
        let recs = read_segments(&dir, 0).unwrap();
        assert_eq!(recs.len(), 2);
        let OwnedRec::Commit { id: a, version: va, .. } = &recs[0];
        let OwnedRec::Commit { id: b, version: vb, .. } = &recs[1];
        assert_eq!((a.as_str(), *va), ("txA", 1));
        assert_eq!((b.as_str(), *vb), ("txB", 2));
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
            Rec::Commit { id: "tx1_aaaaaaaa", base_version: 0, source: "(a b)", steps: 1, version: 1 },
            Some(Ack { reply, ok }),
        );
        let acked = rx.blocking_recv().unwrap().unwrap();
        assert_eq!(acked.version, 1);
        wal.append(
            Rec::Commit { id: "tx2_bbbbbbbb", base_version: 1, source: "(b)", steps: 1, version: 2 },
            None,
        );
        wal.shutdown(); // drains + joins

        let recs = read_segments(&dir, 0).unwrap();
        assert_eq!(recs.len(), 2);
        assert_eq!(
            recs[0],
            OwnedRec::Commit {
                id: "tx1_aaaaaaaa".into(),
                base_version: 0,
                source: "(a b)".into(),
                steps: 1,
                version: 1,
            }
        );
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn checkpoint_rotates_installs_and_gcs() {
        let dir = tmpdir();
        let wal = Wal::open(&dir, FsyncPolicy::No).unwrap();
        wal.append(Rec::Commit { id: "tx1_aaaaaaaa", base_version: 0, source: "(a)", steps: 0, version: 1 }, None);
        assert!(wal.checkpoint(1, 1, "paths", Box::new(|w| w.write_all(b"SNAP"))));
        // FIFO: this lands in the freshly rotated segment
        wal.append(Rec::Commit { id: "tx2_bbbbbbbb", base_version: 1, source: "(b)", steps: 0, version: 2 }, None);
        wal.shutdown(); // joins writer AND checkpointer: the install is complete

        let meta = CkptMeta::load(&dir).unwrap().unwrap();
        assert_eq!(meta.first_segment, 1);
        assert_eq!(meta.tx_counter, 1);
        assert_eq!(fs::read(dir.join(&meta.snapshot)).unwrap(), b"SNAP");
        assert!(!seg_path(&dir, 0).exists(), "pre-checkpoint segment must be GC'd");
        let tail = read_segments(&dir, meta.first_segment).unwrap();
        assert_eq!(
            tail,
            vec![OwnedRec::Commit {
                id: "tx2_bbbbbbbb".into(),
                base_version: 1,
                source: "(b)".into(),
                steps: 0,
                version: 2,
            }]
        );

        // reopen sweeps snapshot files the meta doesn't name, keeps the one it does
        fs::write(dir.join("checkpoint-000099.paths"), b"orphan").unwrap();
        drop(Wal::open(&dir, FsyncPolicy::No).unwrap());
        assert!(!dir.join("checkpoint-000099.paths").exists());
        assert!(dir.join(&meta.snapshot).exists());
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn wal_open_appends_to_existing_segment_and_sweeps_tmp() {
        let dir = tmpdir();
        fs::write(dir.join("checkpoint.meta.tmp"), b"junk").unwrap();
        {
            let wal = Wal::open(&dir, FsyncPolicy::No).unwrap();
            wal.append(
                Rec::Commit { id: "tx1_aaaaaaaa", base_version: 0, source: "(a)", steps: 0, version: 1 },
                None,
            );
        } // drop = drain + join
        assert!(!dir.join("checkpoint.meta.tmp").exists());
        {
            let wal = Wal::open(&dir, FsyncPolicy::No).unwrap();
            wal.append(
                Rec::Commit { id: "tx2_bbbbbbbb", base_version: 1, source: "(b)", steps: 0, version: 2 },
                None,
            );
        }
        let recs = read_segments(&dir, 0).unwrap();
        assert_eq!(recs.len(), 2, "reopen must append, not overwrite");
        fs::remove_dir_all(&dir).unwrap();
    }
}
