//! `GET /export` — serve query results from the latest published snapshot, entirely off the
//! engine thread. Pattern/template use the same bracket notation as the CLI's
//! `convert --pattern/--template` flags (e.g. `[2] petri [3] ! result $`, `_1`), parsed by
//! the same code path (`mork_expr::parse` + the kernel tokenizer).

use mork::space::{ParDataParser, Space};
use mork_expr::{Expr, ExprZipper};
use mork_frontend::bytestring_parser::Parser;
use mork_interning::SharedMappingHandle;

use crate::transaction::ReadSnapshot;
use crate::wrap;

/// Carries the `unavailable:` marker so `/export` answers 503 rather than 400: an
/// exhausted symbol table is the server being saturated, not a malformed request.
pub(crate) const NO_PERMIT: &str =
    "unavailable: every symbol-table write slot is held by a running transaction; retry, \
     or run the server with fewer --workers";

/// Parse a bracket-notation expression into owned expression bytes (kept alive by the
/// returned Vec; build an `Expr` over `.as_mut_ptr()` at the use site).
pub(crate) fn parse_expr_bytes(text: &str, sm: &SharedMappingHandle) -> Result<Vec<u8>, String> {
    if text.len() > 2048 { return Err("expression too long (max 2048 bytes)".into()) }
    let parsed = std::panic::catch_unwind(|| mork_expr::parse::<4096>(text))
        .map_err(|_| format!("failed to parse expression: {text:?}"))?;
    // Take the write permit here rather than let `ParDataParser::new` unwrap one. The
    // slots are shared with the worker pool, and `--workers` may be the whole ceiling, so
    // every slot can be held while this runs -- `/export` parses on a tokio thread, not
    // the engine thread. Acquiring first turns that into an error a caller can answer 503
    // with; `ParDataParser::new` then re-enters this thread's permit instead of competing
    // for a slot of its own, so its `unwrap` cannot fire.
    let _permit = sm.try_aquire_permission().map_err(|()| NO_PERMIT.to_string())?;
    let mut src = parsed;
    let q = Expr { ptr: src.as_mut_ptr() };
    let mut pdp = ParDataParser::new(sm);
    let mut buf = vec![0u8; 1 << 16];
    let p = Expr { ptr: buf.as_mut_ptr() };
    let used = q.substitute_symbols(&mut ExprZipper::new(p), |x| Parser::tokenizer(&mut pdp, x));
    let len = used.len();
    buf.truncate(len);
    Ok(buf)
}

/// No params → dump everything; both params → pattern/template query. Namespaced exec locs
/// are unwrapped in the output.
pub fn export(
    snap: &ReadSnapshot,
    pattern: Option<&str>,
    template: Option<&str>,
) -> Result<String, String> {
    let mut out = Vec::new();
    match (pattern, template) {
        (None, None) => {
            Space::dump_all_sexpr_from(&snap.btm, &snap.sm, &mut out)?;
        }
        (Some(p), Some(t)) => {
            let mut pb = parse_expr_bytes(p, &snap.sm)?;
            let mut tb = parse_expr_bytes(t, &snap.sm)?;
            Space::dump_sexpr_from(
                &snap.btm,
                &snap.sm,
                Expr { ptr: pb.as_mut_ptr() },
                Expr { ptr: tb.as_mut_ptr() },
                &mut out,
            );
        }
        _ => return Err("pattern and template must be provided together".into()),
    }
    let text = String::from_utf8_lossy(&out);
    let mut unwrapped = String::with_capacity(text.len());
    for line in text.lines() {
        if line.is_empty() { continue }
        unwrapped.push_str(&wrap::unwrap_text(line).0);
        unwrapped.push('\n');
    }
    Ok(unwrapped)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mork_interning::{SharedMapping, MAX_WRITER_THREADS};
    use std::sync::mpsc;

    /// With every write slot held, a parse must report the saturation instead of panicking
    /// inside `ParDataParser::new`. `/export` runs on a tokio thread while the worker pool
    /// runs on its own, so at `--workers MAX_WRITER_THREADS` the pool really can own them
    /// all -- and the `unwrap` this guards is in the kernel, past any HTTP error handling.
    #[test]
    fn a_parse_with_no_free_slot_reports_unavailable() {
        let sm = SharedMapping::new();
        let (holding_tx, holding_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::sync_channel::<()>(0);

        // Permits are !Send and one-per-thread, so exhausting the table takes a thread per
        // slot, each parked until the assertion below has run.
        for _ in 0..MAX_WRITER_THREADS {
            let sm = sm.clone();
            let holding_tx = holding_tx.clone();
            let release_rx_signal = release_tx.clone();
            std::thread::spawn(move || {
                let _permit = sm.try_aquire_permission().expect("slot should be free");
                holding_tx.send(()).unwrap();
                // Parked here holding the slot; the channel closing wakes it up.
                let _ = release_rx_signal.send(());
            });
        }
        drop(holding_tx);
        for _ in 0..MAX_WRITER_THREADS {
            holding_rx.recv().expect("every slot holder should report in");
        }

        // A fresh thread, so the probe cannot short-circuit on a thread-local index left
        // over from another test.
        let probe = {
            let sm = sm.clone();
            std::thread::spawn(move || parse_expr_bytes("[2] foo $", &sm))
        };
        let err = probe.join().expect("the parse must not panic").expect_err(
            "no slot was free, so the parse cannot have succeeded",
        );
        assert_eq!(err, NO_PERMIT);
        assert!(err.starts_with("unavailable:"), "must map to 503, not 400: {err}");

        drop(release_rx);
    }
}
