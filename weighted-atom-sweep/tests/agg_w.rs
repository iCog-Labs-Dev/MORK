use std::collections::HashMap;

use pathmap::PathMap;
use pathmap::zipper::{Zipper, ZipperMoving, ZipperValues};
use weighted_atom_sweep::TraversalEngine;
use weighted_atom_sweep::random_walk::RandomWalk;

// ===========================================================================
// B1: agg_w parity — stored field matches full catamorphism
// ===========================================================================

#[test]
fn agg_w_parity() {
    let mut map = PathMap::<u64>::new();
    for &(path, val) in &[
        (&b"ax"[..], 10),
        (&b"ab"[..], 5),
        (&b"ac"[..], 3),
        (&b"b"[..], 20),
    ] {
        let mut wz = map.write_zipper_at_path(path);
        wz.set_val_w(val);
    }

    for path in [
        &b""[..],
        &b"a"[..],
        &b"ax"[..],
        &b"ab"[..],
        &b"ac"[..],
        &b"b"[..],
    ] {
        let z = map.read_zipper_at_path(path);
        let stored = z.agg_w();
        let cata = weighted_atom_sweep::traversal::node_agg_w(z).unwrap();
        assert_eq!(
            stored,
            cata,
            "agg_w mismatch at path {:?}: stored={}, catamorphism={}",
            core::str::from_utf8(path).unwrap_or("?"),
            stored,
            cata
        );
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

    let rw = weighted_atom_sweep::random_walk::RandomWalk;
    let mut counts: HashMap<Vec<u8>, usize> = HashMap::new();

    for _ in 0..10_000 {
        let pos = rw.next_atom(&map).unwrap();
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
            label,
            exp_pct,
            pct,
            count,
            total
        );
    }
}

#[test]
fn test_mork_names_distribution() {
    let mut map = PathMap::<u64>::new();
    let names: Vec<(&str, Vec<u8>, u64)> = vec![
        ("abenezer", vec![2, 196, b'n', b'a', b'm', b'e', 200, b's', b't', b'e', b'v', b'e'], 300),
        ("solomon", vec![2, 196, b'n', b'a', b'm', b'e', 199, b's', b'o', b'l', b'o', b'm', b'o', b'n'], 200),
        ("gete", vec![2, 196, b'n', b'a', b'm', b'e', 196, b'n', b'a', b't', b'e'], 100),
        ("yods", vec![2, 196, b'n', b'a', b'm', b'e', 196, b'y', b'o', b'd', b's'], 50),
    ];

    for (name, path, weight) in &names {
        map.write_zipper_at_path(path).set_val_w(*weight);
    }

    println!("Root agg_w = {}", map.read_zipper().agg_w());
    for (name, path, expected_w) in &names {
        let z = map.read_zipper_at_path(path);
        println!("Path for {}: val = {:?}, agg_w = {}", name, z.val(), z.agg_w());
    }

    let prefix = vec![2, 196, b'n', b'a', b'm', b'e'];
    let mut pz = map.read_zipper_at_path(&prefix);
    println!("Prefix (name): agg_w = {}, child_mask = {:?}", pz.agg_w(), pz.child_mask());
    for b in pz.child_mask().iter() {
        pz.descend_to_byte(b);
        println!("  Child byte {}: val={:?}, agg_w={}", b, pz.val(), pz.agg_w());
        for b2 in pz.child_mask().iter() {
            pz.descend_to_byte(b2);
            println!("    Grandchild byte {}: val={:?}, agg_w={}", b2, pz.val(), pz.agg_w());
            pz.ascend_byte();
        }
        pz.ascend_byte();
    }

    assert_eq!(map.read_zipper().agg_w(), 650);
    assert_eq!(pz.agg_w(), 650);

    let rw = RandomWalk;
    let mut counts: HashMap<String, usize> = HashMap::new();
    for _ in 0..10_000 {
        let pos = rw.next_atom(&map).unwrap();
        let mut matched = false;
        for (name, path, _) in &names {
            if &pos == path {
                *counts.entry(name.to_string()).or_insert(0) += 1;
                matched = true;
                break;
            }
        }
        if !matched {
            *counts.entry(format!("UNKNOWN: {:?}", pos)).or_insert(0) += 1;
        }
    }

    assert!(counts.get("steve").copied().unwrap_or(0) > 3500);
    assert!(counts.get("solomon").copied().unwrap_or(0) > 2000);
    assert!(counts.get("nate").copied().unwrap_or(0) > 800);
    assert!(counts.get("yods").copied().unwrap_or(0) > 300);
    assert_eq!(counts.keys().filter(|k| k.starts_with("UNKNOWN")).count(), 0);
}

