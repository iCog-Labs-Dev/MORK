use std::collections::HashMap;
use std::sync::Arc;

use pathmap::PathMap;
use pathmap::zipper::ZipperCreation;
use weighted_atom_sweep::random_walk::RandomWalk;
use weighted_atom_sweep::TraversalEngine;

// ===========================================================================
// B1: agg_w parity — stored field matches full catamorphism
// ===========================================================================

#[test]
fn agg_w_parity() {
    let mut map = PathMap::<u64>::new();
    for &(path, val) in &[(&b"ax"[..], 10), (&b"ab"[..], 5), (&b"ac"[..], 3), (&b"b"[..], 20)] {
        let mut wz = map.write_zipper_at_path(path);
        wz.set_val_w(val);
    }

    for path in [&b""[..], &b"a"[..], &b"ax"[..], &b"ab"[..], &b"ac"[..], &b"b"[..]] {
        let z = map.read_zipper_at_path(path);
        let stored = z.agg_w();
        let cata = weighted_atom_sweep::traversal::node_agg_w(z).unwrap();
        assert_eq!(stored, cata,
            "agg_w mismatch at path {:?}: stored={}, catamorphism={}",
            core::str::from_utf8(path).unwrap_or("?"), stored, cata);
    }
}

// ===========================================================================
// B1: Random walk distribution — samples follow weight proportions
// ===========================================================================

#[test]
fn random_walk_distribution() {
    let mut map = PathMap::<u64>::new();
    for &(path, val) in &[(&b"a"[..], 100), (&b"b"[..], 200), (&b"c"[..], 50)] {
        let mut wz = map.write_zipper_at_path(path);
        wz.set_val_w(val);
    }

    let head = Arc::new(map.into_zipper_head([]));
    let rw = weighted_atom_sweep::random_walk::RandomWalk;
    let mut counts: HashMap<Vec<u8>, usize> = HashMap::new();

    for _ in 0..10_000 {
        let z = head.read_zipper_at_path(&[]).unwrap();
        let pos = rw.next_atom(z).unwrap();
        *counts.entry(pos).or_insert(0) += 1;
    }

    let total = counts.values().sum::<usize>();
    let expected = [("a", 28.6f64), ("b", 57.1), ("c", 14.3)];
    for (label, exp_pct) in &expected {
        let path = label.as_bytes().to_vec();
        let count = counts.get(&path).copied().unwrap_or(0);
        let pct = (count as f64 / total as f64) * 100.0;
        assert!(
            (pct - exp_pct).abs() < 5.0,
            "random_walk: {} expected {:.1}% ±5%, got {:.1}% ({} / {})",
            label, exp_pct, pct, count, total
        );
    }
}
