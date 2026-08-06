//! Transaction-namespace wrapping.
//!
//! On ingest every `(exec L P T)` subexpression in a submitted transaction — in data,
//! patterns, and templates alike, so pattern matching stays consistent — is rewritten to
//! `(exec (<tx-id> L) P T)`. The tx-id becomes a stable namespace under the VM's exec
//! prefix, which is what lets the engine scope stepping to the running transaction and
//! attribute every step, while each program's own loc-ordering ("inference control") is
//! preserved inside its namespace. Events and exports strip the wrapper back off.
//!
//! CLI-compat rule: this module is the ONLY transform between the wire and the kernel. The
//! tokenizer below accepts exactly the frontend parser's syntax (see
//! `mork-frontend/src/bytestring_parser.rs`): `(`/`)` lists, whitespace-separated atoms,
//! atoms beginning with `"` are strings running to the closing quote with `\` escapes,
//! `$name` variables are ordinary atoms to us. Anything the CLI runs must round-trip here.

use crate::transaction::TxId;

#[derive(Clone, Debug, PartialEq)]
pub enum SExpr {
    Atom(String),
    List(Vec<SExpr>),
}

/// `tx<count>_<unique 8-char alphanumeric>`, e.g. `tx17_si49f8v6`. The count gives
/// human-readable ordering; the random suffix prevents collision and guessing.
/// Returns a validated `TxId` — format and length are checked at construction.
pub fn gen_txid(count: u64) -> TxId {
    use std::hash::{BuildHasher, Hasher};
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    // Randomly-seeded stdlib hasher; 36^8 < 2^64, so one u64 covers all 8 chars.
    let mut n = std::collections::hash_map::RandomState::new().build_hasher().finish();
    let suffix: String = (0..8).map(|_| { let c = ALPHABET[(n % 36) as usize] as char; n /= 36; c }).collect();
    TxId::new(format!("tx{count}_{suffix}")).expect("gen_txid: produced invalid txid")
}

pub fn is_txid(s: &str) -> bool {
    let Some(rest) = s.strip_prefix("tx") else { return false };
    let Some((digits, suffix)) = rest.split_once('_') else { return false };
    !digits.is_empty()
        && digits.bytes().all(|b| b.is_ascii_digit())
        && suffix.len() == 8
        && suffix.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
}

/// The expression-byte prefix of a wrapped loc `(<txid> …)`: `[Arity(2)][SymbolSize(n)]txid`.
/// Appended to the VM's exec prefix this roots `metta_calculus_scoped` at one namespace.
///
/// NOTE: assumes the default non-`interning` symbol encoding (raw bytes, len ≤ 63).
pub fn ns_loc_prefix(txid: &str) -> Vec<u8> {
    use mork_expr::{item_byte, Tag};
    debug_assert!(txid.len() < 64);
    let mut p = Vec::with_capacity(2 + txid.len());
    p.push(item_byte(Tag::Arity(2)));
    p.push(item_byte(Tag::SymbolSize(txid.len() as u8)));
    p.extend_from_slice(txid.as_bytes());
    p
}

/// Parse a whole transaction body into top-level expressions.
pub fn parse_all(src: &str) -> Result<Vec<SExpr>, String> {
    let b = src.as_bytes();
    let mut i = 0usize;
    let mut out = Vec::new();
    loop {
        skip_ws(b, &mut i);
        if i >= b.len() { break }
        out.push(parse_one(b, &mut i)?);
    }
    Ok(out)
}

fn skip_ws(b: &[u8], i: &mut usize) {
    while *i < b.len() && (b[*i] as char).is_whitespace() { *i += 1 }
}

fn parse_one(b: &[u8], i: &mut usize) -> Result<SExpr, String> {
    skip_ws(b, i);
    if *i >= b.len() { return Err("unexpected end of input".into()) }
    match b[*i] {
        b'(' => {
            *i += 1;
            let mut items = Vec::new();
            loop {
                skip_ws(b, i);
                if *i >= b.len() { return Err("unclosed '('".into()) }
                if b[*i] == b')' { *i += 1; return Ok(SExpr::List(items)) }
                items.push(parse_one(b, i)?);
            }
        }
        b')' => Err(format!("unexpected ')' at byte {}", i)),
        b'"' => {
            // string atom: runs to the closing quote, backslash escapes (frontend rules)
            let start = *i;
            *i += 1;
            loop {
                if *i >= b.len() { return Err("unfinished string".into()) }
                match b[*i] {
                    b'"' => { *i += 1; break }
                    b'\\' => { *i += if *i + 1 < b.len() { 2 } else { return Err("unfinished escape sequence".into()) } }
                    _ => { *i += 1 }
                }
            }
            Ok(SExpr::Atom(String::from_utf8_lossy(&b[start..*i]).into_owned()))
        }
        _ => {
            let start = *i;
            while *i < b.len() && b[*i] != b'(' && b[*i] != b')' && !(b[*i] as char).is_whitespace() { *i += 1 }
            Ok(SExpr::Atom(String::from_utf8_lossy(&b[start..*i]).into_owned()))
        }
    }
}

pub fn to_text(e: &SExpr) -> String {
    match e {
        SExpr::Atom(a) => a.clone(),
        SExpr::List(items) => {
            let inner: Vec<String> = items.iter().map(to_text).collect();
            format!("({})", inner.join(" "))
        }
    }
}

fn is_exec4(items: &[SExpr]) -> bool {
    items.len() == 4 && matches!(&items[0], SExpr::Atom(a) if a == "exec")
}

/// Recursively wrap the loc of every `(exec L P T)` subexpression: `L` → `(<ns> L)`.
/// Post-order, so execs nested anywhere (including inside locs/templates) are wrapped
/// exactly once.
pub fn wrap_execs(e: &mut SExpr, ns: &str) {
    if let SExpr::List(items) = e {
        for item in items.iter_mut() { wrap_execs(item, ns) }
        if is_exec4(items) {
            let old = std::mem::replace(&mut items[1], SExpr::Atom(String::new()));
            items[1] = SExpr::List(vec![SExpr::Atom(ns.to_string()), old]);
        }
    }
}

/// Inverse of [`wrap_execs`] for anything shaped `(exec (<txid> L) P T)`. Returns the last
/// txid found (for attribution).
pub fn unwrap_execs(e: &mut SExpr) -> Option<String> {
    let mut found = None;
    if let SExpr::List(items) = e {
        if is_exec4(items) {
            let unwrap_to = match &items[1] {
                SExpr::List(loc) if loc.len() == 2 => match &loc[0] {
                    SExpr::Atom(a) if is_txid(a) => Some((a.clone(), loc[1].clone())),
                    _ => None,
                },
                _ => None,
            };
            if let Some((txid, inner)) = unwrap_to {
                items[1] = inner;
                found = Some(txid);
            }
        }
        for item in items.iter_mut() {
            if let Some(txid) = unwrap_execs(item) { found = Some(txid) }
        }
    }
    found
}

/// Full ingest rewrite: parse, wrap every exec's loc under `ns`, re-serialize.
pub fn rewrite(src: &str, ns: &str) -> Result<String, String> {
    let mut exprs = parse_all(src)?;
    let mut out = String::new();
    for e in exprs.iter_mut() {
        wrap_execs(e, ns);
        out.push_str(&to_text(e));
        out.push('\n');
    }
    Ok(out)
}

/// Strip namespace wrappers from a serialized expression (for events/exports). Returns the
/// unwrapped text and the txid if one was found; on parse failure returns the input as-is.
pub fn unwrap_text(line: &str) -> (String, Option<String>) {
    match parse_all(line) {
        Ok(mut exprs) if !exprs.is_empty() => {
            let mut txid = None;
            let mut out = Vec::with_capacity(exprs.len());
            for e in exprs.iter_mut() {
                if let Some(t) = unwrap_execs(e) { txid = Some(t) }
                out.push(to_text(e));
            }
            (out.join(" "), txid)
        }
        _ => (line.to_string(), None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_2plus2() {
        let src = r#"(exec (IC 0 1 (S Z)) (, (exec (IC $x $y (S $c)) $sp $st) ((exec $x) $p $t)) (, (exec (IC $y $x $c) $sp $st) (exec (R $x) $p $t)))
(petri (! (add result) ((S (S Z)) (S (S Z)))))"#;
        let wrapped = rewrite(src, "tx1_abcd1234").unwrap();
        // every 4-ary exec loc got wrapped, including inside patterns/templates
        assert_eq!(wrapped.matches("(tx1_abcd1234 ").count(), 4);
        // arity-2 dormant (exec $x) is untouched
        assert!(wrapped.contains("((exec $x) $p $t)"));
        // unwrap restores the original (modulo whitespace)
        let (un, txid) = unwrap_text(wrapped.lines().next().unwrap());
        assert_eq!(txid.as_deref(), Some("tx1_abcd1234"));
        assert!(un.starts_with("(exec (IC 0 1 (S Z))"));
        assert!(!un.contains("tx1_"));
    }

    #[test]
    fn txid_format() {
        let id = gen_txid(17);
        assert!(is_txid(&id), "{id}");
        assert!(id.starts_with("tx17_"));
        assert!(!is_txid("exec"));
        assert!(!is_txid("tx_abc"));
        assert!(!is_txid("tx1_short"));
    }

    #[test]
    fn strings_and_vars_roundtrip() {
        let src = r#"(data "a (quoted) \" string" $var)"#;
        let exprs = parse_all(src).unwrap();
        assert_eq!(to_text(&exprs[0]), src);
    }
}
