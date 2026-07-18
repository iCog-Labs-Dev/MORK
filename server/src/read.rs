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

/// Parse a bracket-notation expression into owned expression bytes (kept alive by the
/// returned Vec; build an `Expr` over `.as_mut_ptr()` at the use site).
pub(crate) fn parse_expr_bytes(text: &str, sm: &SharedMappingHandle) -> Result<Vec<u8>, String> {
    if text.len() > 2048 { return Err("expression too long (max 2048 bytes)".into()) }
    let parsed = std::panic::catch_unwind(|| mork_expr::parse::<4096>(text))
        .map_err(|_| format!("failed to parse expression: {text:?}"))?;
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
