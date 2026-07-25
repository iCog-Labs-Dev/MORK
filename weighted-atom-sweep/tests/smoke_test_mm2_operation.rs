//! Tests demonstrating `SExprOperation` in the exec model.
//!
//! An `SExprOperation` carries a pattern and a list of (template, effect) pairs.
//! For each pattern match in the subtrie, variables are extracted via mork-expr's
//! `extract_data` and substituted into each template via `substitute`. The
//! resulting expression bytes are applied to the trie per the template effect
//! (Add or Remove).
//!
//! The tests cover:
//! - Exec with no pattern (unconditional template application)
//! - Exec with pattern only (match counting, no templates)
//! - Exec with pattern + Add templates (match-then-add)
//! - Exec with pattern + Remove templates (match-then-remove)
//! - Full sweep loop integration
//! - Encoding verification

use std::time::Duration;

use mork_expr::{item_byte, parse, Tag};
use pathmap::zipper::{ReadZipperTracked, ZipperCreation};
use pathmap::PathMap;

use weighted_atom_sweep::{
    AtomPosition, Operation, OperationObserver, SExprOperation, TemplateEffect,
    TransformOp, TraversalEngine, TraversalError, WeightedAtomSweep, WeightedAtomSweepSettings,
};

/// A no-op engine that always returns the root path (used by the sweep-loop test).
struct FixedRoot;
impl TraversalEngine for FixedRoot {
    fn name(&self) -> &str { "fixed_root" }
    fn next_atom(&self, _z: ReadZipperTracked<u64>) -> Result<AtomPosition, TraversalError> {
        std::thread::sleep(Duration::from_millis(500));
        Ok(vec![])
    }
}

// ---------------------------------------------------------------------------
// Helper: encode expressions with parse! and return byte vecs
// ---------------------------------------------------------------------------

/// `(foo bar)` — arity 2, symbol "foo", symbol "bar"
fn expr_foo_bar() -> Vec<u8> {
    parse!("[2] foo bar").to_vec()
}

/// `(= a a)` — arity 3, symbol "=", symbol "a", symbol "a"
fn expr_eq_a_a() -> Vec<u8> {
    parse!("[3] = a a").to_vec()
}

/// `(= b b)`
fn expr_eq_b_b() -> Vec<u8> {
    parse!("[3] = b b").to_vec()
}

/// `(= a b)`
fn expr_eq_a_b() -> Vec<u8> {
    parse!("[3] = a b").to_vec()
}

/// `(= $ _1)` — co-referential match pattern
fn pattern_eq_x_x() -> Vec<u8> {
    parse!("[3] = $ _1").to_vec()
}

/// `(matched $)` — template that captures the matched variable
fn template_matched_x() -> Vec<u8> {
    parse!("[2] matched $").to_vec()
}

/// `(f (g a) b)` — nested expression
fn expr_f_ga_b() -> Vec<u8> {
    parse!("[3] f [2] g a b").to_vec()
}

// ===========================================================================
// Test 1: Exec with no pattern — unconditional Add
// ===========================================================================

/// An exec with an empty pattern applies templates directly (no matching).
#[test]
fn test_exec_no_pattern_add() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_test_writer()
        .try_init();

    let expr_bytes = expr_foo_bar();
    let space = PathMap::<u64>::new().into_zipper_head([]);

    let op =
        SExprOperation::exec("add_foo_bar", &[], &[(&expr_bytes, TemplateEffect::Add)]);

    {
        let mut wz = space.write_zipper_at_exclusive_path(&[] as &[u8]).unwrap();
        op.apply(&mut wz, &[]);
        space.cleanup_write_zipper_w(wz);
    }

    let map = space.into_map();
    assert!(
        map.get_val_at(&expr_bytes).is_some(),
        "expected value at expression path after unconditional Add"
    );
}

// ===========================================================================
// Test 2: Exec with no pattern — unconditional Remove
// ===========================================================================

#[test]
fn test_exec_no_pattern_remove() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_test_writer()
        .try_init();

    let expr_bytes = expr_foo_bar();

    let mut map = PathMap::<u64>::new();
    map.set_val_at(&expr_bytes, 1u64);
    assert!(map.get_val_at(&expr_bytes).is_some());

    let space = map.into_zipper_head([]);

    let op = SExprOperation::exec(
        "remove_foo_bar",
        &[],
        &[(&expr_bytes, TemplateEffect::Remove)],
    );

    {
        let mut wz = space.write_zipper_at_exclusive_path(&[] as &[u8]).unwrap();
        op.apply(&mut wz, &[]);
        space.cleanup_write_zipper_w(wz);
    }

    let map = space.into_map();
    assert!(
        map.get_val_at(&expr_bytes).is_none(),
        "expected no value after unconditional Remove"
    );
}

// ===========================================================================
// Test 3: Exec with pattern only — match counting
// ===========================================================================

/// Pattern with no templates — just counts matches.
/// Pattern `(= $ _1)` against `(= a a)`, `(= b b)`, `(= a b)`.
/// Should match 2 (the co-referential ones).
#[test]
fn test_exec_match_only() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_test_writer()
        .try_init();

    let mut map = PathMap::<u64>::new();
    map.set_val_at(&expr_eq_a_a(), 1u64);
    map.set_val_at(&expr_eq_b_b(), 1u64);
    map.set_val_at(&expr_eq_a_b(), 1u64);

    let space = map.into_zipper_head([]);
    let pattern = pattern_eq_x_x();

    let op = SExprOperation::exec("match_eq_x_x", &pattern, &[]);

    assert_eq!(op.match_count(), 0);

    {
        let mut wz = space.write_zipper_at_exclusive_path(&[] as &[u8]).unwrap();
        op.apply(&mut wz, &[]);
        space.cleanup_write_zipper_w(wz);
    }

    assert_eq!(
        op.match_count(),
        2,
        "expected 2 co-referential matches for (= $ _1)"
    );
}

// ===========================================================================
// Test 4: Exec with pattern + Add template — match then add
// ===========================================================================

/// Match `(= $x $x)`, for each match add `(matched $x)`.
///
/// Trie starts with: (= a a), (= b b), (= a b)
/// Pattern: (= $ _1) matches (= a a) and (= b b)
/// Template: (matched $) with Add
/// After exec: trie should also contain (matched a) and (matched b)
#[test]
fn test_exec_pattern_then_add() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_test_writer()
        .try_init();

    let mut map = PathMap::<u64>::new();
    map.set_val_at(&expr_eq_a_a(), 1u64);
    map.set_val_at(&expr_eq_b_b(), 1u64);
    map.set_val_at(&expr_eq_a_b(), 1u64);

    let space = map.into_zipper_head([]);

    let pattern = pattern_eq_x_x();
    let template = template_matched_x();

    let op = SExprOperation::exec(
        "match_and_add",
        &pattern,
        &[(&template, TemplateEffect::Add)],
    );

    {
        let mut wz = space.write_zipper_at_exclusive_path(&[] as &[u8]).unwrap();
        op.apply(&mut wz, &[]);
        space.cleanup_write_zipper_w(wz);
    }

    assert_eq!(op.match_count(), 2);

    let map = space.into_map();

    // Original expressions should still be there
    assert!(map.get_val_at(&expr_eq_a_a()).is_some());
    assert!(map.get_val_at(&expr_eq_b_b()).is_some());
    assert!(map.get_val_at(&expr_eq_a_b()).is_some());

    // New expressions from template instantiation
    let matched_a = parse!("[2] matched a").to_vec();
    let matched_b = parse!("[2] matched b").to_vec();

    assert!(
        map.get_val_at(&matched_a).is_some(),
        "expected (matched a) after exec"
    );
    assert!(
        map.get_val_at(&matched_b).is_some(),
        "expected (matched b) after exec"
    );
}

// ===========================================================================
// Test 5: Exec with pattern + Remove template — match then remove
// ===========================================================================

/// Match `(= $x $x)`, for each match remove `(= $x $x)` itself.
///
/// Trie starts with: (= a a), (= b b), (= a b)
/// Pattern: (= $ _1)
/// Template: (= $ _1) with Remove — removes the matched expressions
/// After exec: (= a a) and (= b b) should be gone, (= a b) remains
#[test]
fn test_exec_pattern_then_remove() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_test_writer()
        .try_init();

    let mut map = PathMap::<u64>::new();
    map.set_val_at(&expr_eq_a_a(), 1u64);
    map.set_val_at(&expr_eq_b_b(), 1u64);
    map.set_val_at(&expr_eq_a_b(), 1u64);

    let space = map.into_zipper_head([]);

    let pattern = pattern_eq_x_x();
    // Template is the same as pattern — removes the matched path
    let template = pattern_eq_x_x();

    let op = SExprOperation::exec(
        "match_and_remove",
        &pattern,
        &[(&template, TemplateEffect::Remove)],
    );

    {
        let mut wz = space.write_zipper_at_exclusive_path(&[] as &[u8]).unwrap();
        op.apply(&mut wz, &[]);
        space.cleanup_write_zipper_w(wz);
    }

    assert_eq!(op.match_count(), 2);

    let map = space.into_map();

    // Co-referential expressions should be removed
    assert!(
        map.get_val_at(&expr_eq_a_a()).is_none(),
        "(= a a) should have been removed"
    );
    assert!(
        map.get_val_at(&expr_eq_b_b()).is_none(),
        "(= b b) should have been removed"
    );

    // Non-co-referential expression should remain
    assert!(
        map.get_val_at(&expr_eq_a_b()).is_some(),
        "(= a b) should still be present"
    );
}

// ===========================================================================
// Test 6: Exec with both Add and Remove templates
// ===========================================================================

/// Match `(= $x $x)`, remove the matched expr and add `(matched $x)`.
///
/// This demonstrates a "rewrite rule" — replace co-referential equalities
/// with a simpler form.
#[test]
fn test_exec_rewrite_rule() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_test_writer()
        .try_init();

    let mut map = PathMap::<u64>::new();
    map.set_val_at(&expr_eq_a_a(), 1u64);
    map.set_val_at(&expr_eq_b_b(), 1u64);
    map.set_val_at(&expr_eq_a_b(), 1u64);

    let space = map.into_zipper_head([]);

    let pattern = pattern_eq_x_x();
    let remove_template = pattern_eq_x_x(); // remove the matched self-equal
    let add_template = template_matched_x(); // add (matched $x)

    let op = SExprOperation::exec(
        "rewrite_eq",
        &pattern,
        &[
            (&remove_template, TemplateEffect::Remove),
            (&add_template, TemplateEffect::Add),
        ],
    );

    {
        let mut wz = space.write_zipper_at_exclusive_path(&[] as &[u8]).unwrap();
        op.apply(&mut wz, &[]);
        space.cleanup_write_zipper_w(wz);
    }

    let map = space.into_map();

    // Self-equal expressions should be gone
    assert!(map.get_val_at(&expr_eq_a_a()).is_none());
    assert!(map.get_val_at(&expr_eq_b_b()).is_none());

    // Non-co-referential should remain
    assert!(map.get_val_at(&expr_eq_a_b()).is_some());

    // Rewritten forms should be present
    let matched_a = parse!("[2] matched a").to_vec();
    let matched_b = parse!("[2] matched b").to_vec();
    assert!(map.get_val_at(&matched_a).is_some());
    assert!(map.get_val_at(&matched_b).is_some());
}

// ===========================================================================
// Test 7: Match count accumulates across multiple apply calls
// ===========================================================================

#[test]
fn test_exec_match_count_accumulates() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_test_writer()
        .try_init();

    let mut map = PathMap::<u64>::new();
    map.set_val_at(&expr_eq_a_a(), 1u64);
    map.set_val_at(&expr_eq_b_b(), 1u64);

    let space = map.into_zipper_head([]);
    let pattern = pattern_eq_x_x();
    let op = SExprOperation::exec("match_accum", &pattern, &[]);

    // First apply
    {
        let mut wz = space.write_zipper_at_exclusive_path(&[] as &[u8]).unwrap();
        op.apply(&mut wz, &[]);
        space.cleanup_write_zipper_w(wz);
    }
    assert_eq!(op.match_count(), 2);

    // Second apply — accumulates to 4
    {
        let mut wz = space.write_zipper_at_exclusive_path(&[] as &[u8]).unwrap();
        op.apply(&mut wz, &[]);
        space.cleanup_write_zipper_w(wz);
    }
    assert_eq!(op.match_count(), 4);

    // Reset and apply again
    op.reset_match_count();
    assert_eq!(op.match_count(), 0);
    {
        let mut wz = space.write_zipper_at_exclusive_path(&[] as &[u8]).unwrap();
        op.apply(&mut wz, &[]);
        space.cleanup_write_zipper_w(wz);
    }
    assert_eq!(op.match_count(), 2);
}

// ===========================================================================
// Test 8: Exec in the full sweep loop
// ===========================================================================

#[test]
fn test_exec_in_sweep_loop() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_test_writer()
        .try_init();

    let expr_bytes = expr_foo_bar();

    let mut sweep = WeightedAtomSweep::new(WeightedAtomSweepSettings::default());

    // Build a custom engine directly and add it under a name.
    let process = sweep.add_engine("engine_1", "random_walk");

    // Subscribe an exec operation with no pattern (unconditional Add)
    let add_op =
        SExprOperation::exec("add_foo_bar", &[], &[(&expr_bytes, TemplateEffect::Add)]);
    process.subscribe(Box::new(add_op));

    // Also subscribe a simple fn-pointer operation
    let noop = Operation::new(
        "noop",
        |_wz: &mut pathmap::zipper::WriteZipperTracked<u64>, _atom_path: &[u8]| {},
    );
    process.subscribe(Box::new(noop));

    let _name = sweep.spawn();
    std::thread::sleep(std::time::Duration::from_millis(2000));
    let result = sweep.shutdown_all();
    assert!(result.is_some(), "sweep shutdown should succeed");
}

// ===========================================================================
// Test 9: Verify mm2 encoding round-trip
// ===========================================================================

#[test]
fn test_manual_encoding_matches_parse() {
    // Manually encode `(= a a)`:
    //   Arity(3) | SymbolSize(1) '=' | SymbolSize(1) 'a' | SymbolSize(1) 'a'
    let manual_bytes = vec![
        item_byte(Tag::Arity(3)),
        item_byte(Tag::SymbolSize(1)),
        b'=',
        item_byte(Tag::SymbolSize(1)),
        b'a',
        item_byte(Tag::SymbolSize(1)),
        b'a',
    ];
    assert_eq!(manual_bytes, expr_eq_a_a());

    // Manually encode `(= $ _1)`:
    //   Arity(3) | SymbolSize(1) '=' | NewVar | VarRef(0)
    let manual_pattern = vec![
        item_byte(Tag::Arity(3)),
        item_byte(Tag::SymbolSize(1)),
        b'=',
        item_byte(Tag::NewVar),
        item_byte(Tag::VarRef(0)),
    ];
    assert_eq!(manual_pattern, pattern_eq_x_x());

    // Manually encode `(matched $)`:
    //   Arity(2) | SymbolSize(7) 'matched' | NewVar
    let manual_template = vec![
        item_byte(Tag::Arity(2)),
        item_byte(Tag::SymbolSize(7)),
        b'm',
        b'a',
        b't',
        b'c',
        b'h',
        b'e',
        b'd',
        item_byte(Tag::NewVar),
    ];
    assert_eq!(manual_template, template_matched_x());
}

// ===========================================================================
// Test 10: Exec with noise — unrelated expressions don't interfere
// ===========================================================================

#[test]
fn test_exec_with_noise() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_test_writer()
        .try_init();

    let mut map = PathMap::<u64>::new();
    map.set_val_at(&expr_eq_a_a(), 1u64);
    map.set_val_at(&expr_eq_b_b(), 1u64);
    map.set_val_at(&expr_eq_a_b(), 1u64);
    map.set_val_at(&expr_f_ga_b(), 1u64); // noise

    let space = map.into_zipper_head([]);

    let pattern = pattern_eq_x_x();
    let template = template_matched_x();

    let op = SExprOperation::exec(
        "match_with_noise",
        &pattern,
        &[(&template, TemplateEffect::Add)],
    );

    {
        let mut wz = space.write_zipper_at_exclusive_path(&[] as &[u8]).unwrap();
        op.apply(&mut wz, &[]);
        space.cleanup_write_zipper_w(wz);
    }

    assert_eq!(op.match_count(), 2, "noise should not affect match count");

    let map = space.into_map();
    let matched_a = parse!("[2] matched a").to_vec();
    let matched_b = parse!("[2] matched b").to_vec();
    assert!(map.get_val_at(&matched_a).is_some());
    assert!(map.get_val_at(&matched_b).is_some());

    // Noise expression should be untouched
    assert!(map.get_val_at(&expr_f_ga_b()).is_some());
}
