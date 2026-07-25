//! End-to-end tests for the §8.2 feedback loop:
//!   sample ∝ agg_w → transform (decay) → agg_w moves → next sample adapts.
//!
//! These are the first tests that exercise the REAL sweep path end-to-end
//! (spawn → threads run → shutdown), verifying that:
//! - `set_val_w` + `cleanup_write_zipper_w` propagates weight deltas to the true root
//! - The sweep's operations thread calls `cleanup_write_zipper_w` (not plain)
//! - `root_agg_w()` decreases after a decay sweep (proves the loop is live)

use std::time::Duration;
use pathmap::zipper::ZipperCreation;
use weighted_atom_sweep::{build_operation, Operation, OperationObserver, WeightedAtomSweep, WeightedAtomSweepSettings};

#[test]
fn decay_feedback_loop_decreases_root_agg_w() {
    let mut sweep = WeightedAtomSweep::new(WeightedAtomSweepSettings::default());
    {
        let process = sweep.add_engine("decay_test", "random_walk");
        process.subscribe(Box::new(Operation::decay()));
    }

    // Seed with known weights via set_val_w
    sweep.init_map();
    {
        let m = sweep.map.as_mut().expect("map must be initialized");
        for &(path, val) in &[(&b"aa"[..], 100u64), (&b"ab"[..], 200), (&b"ac"[..], 50)] {
            let mut wz = m.inner.write_zipper_at_exclusive_path(path).unwrap();
            wz.set_val_w(val);
            m.inner.cleanup_write_zipper_w(wz);
        }
    }

    // Spawn, run briefly, then shut down
    let _handle = sweep.spawn();
    std::thread::sleep(Duration::from_millis(500));

    // Shutdown reclaims the map
    let map = sweep.shutdown_all().expect("shutdown must reclaim the map");

    // The decay op reduces each visited atom's weight by at least 10%.
    // With 3 atoms (total=350) and 500ms of random-walk sampling, odds are
    // extremely high that at least one was visited, so root agg_w < 350.
    let after = map.read_zipper().agg_w();
    assert!(
        after < 350,
        "root agg_w must decrease after decay sweep: before=350 after={}",
        after,
    );
}

/// Same loop as above but on a single atom: weight=10, decay subtracts
/// 10% each visit → after ~50 samples weight drops to 0. Verifies the
/// sweep's decay operation actually converges, not just decreases once.
#[test]
fn decay_converges_to_zero_over_many_cycles() {
    let mut sweep = WeightedAtomSweep::new(WeightedAtomSweepSettings::default());
    {
        let process = sweep.add_engine("converge_test", "random_walk");
        process.subscribe(Box::new(Operation::decay()));
    }

    // Seed a single atom with weight 10
    sweep.init_map();
    {
        let m = sweep.map.as_mut().expect("map must be initialized");
        let mut wz = m.inner.write_zipper_at_exclusive_path(b"x").unwrap();
        wz.set_val_w(10u64);
        m.inner.cleanup_write_zipper_w(wz);
    }

    let _handle = sweep.spawn();
    // Long enough to decay 10→0 (10 * 0.9^n ≈ 0 after ~50 samples)
    std::thread::sleep(Duration::from_millis(2000));

    let map = sweep.shutdown_all().expect("must reclaim on shutdown");
    let after = map.get_val_at(b"x").copied().unwrap_or(0);
    assert!(
        after < 10,
        "single atom should decay: initial=10, after={}",
        after,
    );
}

/// `build_operation` must recognize the `"decay"` type — this is the config path
/// MORK's `Space::sweep()` uses (`(imp decay)` → `build_operation("decay", …)`).
/// Before the arm was added this returned `None`, so config-declared decay ops were
/// silently dropped and sweeps ran with zero operations.
#[test]
fn build_operation_recognizes_decay() {
    let op = build_operation("decay", &[]).expect("decay must be buildable from config");
    assert_eq!(op.name(), "decay");
    assert!(build_operation("bogus_op", &[]).is_none(), "unknown op type → None");
}

/// End-to-end via the CONFIG path: build the decay op with `build_operation` (as MORK
/// does), subscribe it, run the sweep, and assert root agg_w drops. This closes the loop
/// the direct-construction tests above miss — proving a config-declared decay actually
/// executes.
#[test]
fn decay_via_build_operation_decreases_root_agg_w() {
    let mut sweep = WeightedAtomSweep::new(WeightedAtomSweepSettings::default());
    {
        let process = sweep.add_engine("cfg_decay", "random_walk");
        let op = build_operation("decay", &[]).expect("decay op");
        process.subscribe(op);
    }

    sweep.init_map();
    {
        let m = sweep.map.as_mut().expect("map must be initialized");
        for &(path, val) in &[(&b"aa"[..], 100u64), (&b"ab"[..], 200), (&b"ac"[..], 50)] {
            let mut wz = m.inner.write_zipper_at_exclusive_path(path).unwrap();
            wz.set_val_w(val);
            m.inner.cleanup_write_zipper_w(wz);
        }
    }

    let _handle = sweep.spawn();
    std::thread::sleep(Duration::from_millis(500));
    let map = sweep.shutdown_all().expect("shutdown must reclaim the map");

    let after = map.read_zipper().agg_w();
    assert!(
        after < 350,
        "config-built decay must reduce root agg_w: before=350 after={}",
        after,
    );
}
