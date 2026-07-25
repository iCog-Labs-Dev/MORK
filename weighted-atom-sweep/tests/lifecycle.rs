use std::time::Duration;
use pathmap::zipper::{ZipperCreation, ZipperValues, ZipperWriting};
use weighted_atom_sweep::{WeightedAtomSweep, WeightedAtomSweepSettings};

fn make_sweep() -> WeightedAtomSweep {
    let mut s = WeightedAtomSweep::new(WeightedAtomSweepSettings::default());
    s.init_map();
    s
}

fn add_three_processes(sweep: &mut WeightedAtomSweep) {
    sweep.add_engine("a", "random_walk");
    sweep.add_engine("b", "cpq");
    sweep.add_engine("c", "random_walk");
}

/// Seed the map with known values so threads have atoms to sample.
/// Must call BEFORE spawn — can't write to the map while threads run.
fn seed_map(sweep: &mut WeightedAtomSweep) {
    if let Some(ref mut m) = sweep.map {
        for &(path, val) in &[(&b"aa"[..], 10u64), (&b"ab"[..], 5), (&b"ba"[..], 20), (&b"bb"[..], 15)] {
            let mut wz = m.inner.write_zipper_at_exclusive_path(path).unwrap();
            wz.set_val_w(val);
        }
    }
}

#[test]
fn spawn_is_not_consuming() {
    let mut sweep = make_sweep();
    add_three_processes(&mut sweep);
    seed_map(&mut sweep);

    let name1 = sweep.spawn();
    assert_eq!(sweep.controllers.len(), 1);
    assert!(sweep.controllers.contains_key(&name1));

    // Add more processes and spawn again — WAS is not consumed
    // Note: can't write to map while threads run, so no seed_map here
    sweep.add_engine("d", "random_walk");
    let name2 = sweep.spawn();
    assert_eq!(sweep.controllers.len(), 2);
    assert_ne!(name1, name2, "spawn handles must be unique");
    assert!(sweep.controllers.contains_key(&name2));

    sweep.shutdown_all();
    assert!(sweep.controllers.is_empty());
}

#[test]
fn pause_all_resume_cycle() {
    let mut sweep = make_sweep();
    add_three_processes(&mut sweep);
    seed_map(&mut sweep);
    sweep.spawn();

    // Pause: all threads should park
    let map = sweep.pause_all();
    for ctrl in sweep.controllers.values() {
        assert!(ctrl.is_paused(), "controller should report paused");
        assert!(
            ctrl.parked_count() >= ctrl.thread_count(),
            "all threads must park after pause: parked {} < threads {}",
            ctrl.parked_count(), ctrl.thread_count()
        );
    }

    // Resume: threads restart
    sweep.resume_all(map);
    for ctrl in sweep.controllers.values() {
        assert!(!ctrl.is_paused(), "controller should report active after resume");
    }

    sweep.shutdown_all();
    assert!(sweep.controllers.is_empty());
}

#[test]
fn pause_all_reclaims_correct_map() {
    let mut sweep = make_sweep();
    add_three_processes(&mut sweep);

    // Seed values before spawn
    if let Some(ref mut m) = sweep.map {
        for &(path, val) in &[(&b"x"[..], 42u64), (&b"yy"[..], 99), (&b"zzz"[..], 7)] {
            let mut wz = m.inner.write_zipper_at_exclusive_path(path).unwrap();
            wz.set_val_w(val);
        }
    }

    sweep.spawn();

    // Pause and reclaim
    let map = sweep.pause_all();

    // Verify reclaimed values match
    for &(path, expected) in &[(&b"x"[..], 42u64), (&b"yy"[..], 99), (&b"zzz"[..], 7)] {
        let z = map.read_zipper_at_path(path);
        assert_eq!(z.val(), Some(&expected), "value at {:?} should be preserved after pause", path);
    }

    sweep.resume_all(map);
    sweep.shutdown_all();
}

#[test]
fn multiple_pause_resume_cycles() {
    let mut sweep = make_sweep();
    add_three_processes(&mut sweep);
    seed_map(&mut sweep);
    sweep.spawn();

    for i in 0..3 {
        let map = sweep.pause_all();
        assert!(map.val_count() > 0, "map should have values after cycle {}", i);
        sweep.resume_all(map);
        std::thread::sleep(Duration::from_millis(10));
    }

    sweep.shutdown_all();
}

#[test]
fn shutdown_reclaims_map() {
    let mut sweep = make_sweep();
    sweep.add_engine("single", "cpq");
    if let Some(ref mut m) = sweep.map {
        let mut wz = m.inner.write_zipper_at_exclusive_path(b"test").unwrap();
        wz.set_val_w(42u64);
    }
    let name = sweep.spawn();

    // Shutdown should return the reclaimed map
    let map = sweep.shutdown(&name);
    assert!(map.is_some(), "shutdown of last controller must reclaim the trie");
    assert_eq!(sweep.controllers.len(), 0);

    // The reclaimed map should contain our value
    let z = map.as_ref().unwrap().read_zipper_at_path(b"test");
    assert_eq!(z.val(), Some(&42u64), "reclaimed map should preserve values");

    // self.map was reset to a fresh empty map
    assert!(sweep.map.is_some(), "self.map should be Some after shutdown");
    if let Some(ref m) = sweep.map {
        let z = m.inner.read_zipper_at_path(b"test").unwrap();
        assert_eq!(z.val(), None, "self.map should be a fresh empty map after shutdown");
    }
}

#[test]
fn spawn_after_shutdown() {
    let mut sweep = make_sweep();
    add_three_processes(&mut sweep);
    seed_map(&mut sweep);
    let name = sweep.spawn();

    // Shutdown — last controller returns the reclaimed map
    let map = sweep.shutdown(&name);
    assert!(map.is_some(), "should reclaim map on first shutdown");

    // Add processes and spawn again — fresh lifecycle
    // (self.map was reset to fresh empty, seed_map writes to it)
    add_three_processes(&mut sweep);
    seed_map(&mut sweep);
    let name2 = sweep.spawn();
    assert_eq!(sweep.controllers.len(), 1);
    assert!(sweep.controllers.contains_key(&name2));

    // Pause/resume still works after respawn
    let map2 = sweep.pause_all();
    assert!(map2.val_count() > 0, "respawned sweep should have values");
    sweep.resume_all(map2);

    sweep.shutdown_all();
}

#[test]
fn pause_all_then_spawn_new() {
    let mut sweep = make_sweep();
    sweep.add_engine("first", "random_walk");
    seed_map(&mut sweep);
    sweep.spawn();

    // Pause — the map is reclaimed into the return value
    let reclaimed = sweep.pause_all();

    // While paused, add more processes
    sweep.add_engine("second", "cpq");

    // Push the reclaimed map back, then spawn a new controller
    // that shares the same trie
    sweep.resume_all(reclaimed);
    let name2 = sweep.spawn();
    assert_eq!(sweep.controllers.len(), 2);

    // Both controllers are functional
    for ctrl in sweep.controllers.values() {
        assert!(!ctrl.is_paused());
    }

    sweep.shutdown_all();
}

#[test]
fn pause_all_then_shutdown() {
    let mut sweep = make_sweep();
    add_three_processes(&mut sweep);
    seed_map(&mut sweep);
    sweep.spawn();

    let map = sweep.pause_all();

    // Drop the map without resuming, then shutdown
    drop(map);
    let result = sweep.shutdown_all();
    assert!(result.is_none(), "shutdown should return None when map was already reclaimed by pause");
}

#[test]
fn state_a_at_birth() {
    let s = WeightedAtomSweep::new(WeightedAtomSweepSettings::default());
    assert!(s.map.is_none(), "STATE A at birth: map should be None");
    assert!(s.controllers.is_empty(), "no controllers at birth");
}

/// B3 load-bearing invariant: with MULTIPLE controllers, `pause_all` must reclaim the
/// trie. All controllers spawned by one WAS share ONE lease slot, so when they park the
/// leased head's strong count drops to 1 and `try_unwrap` succeeds. Before the
/// shared-slot fix each spawn minted its own slot, leaving a live clone per extra sweep,
/// and `pause_all` panicked (`strong_count != 1`). Run repeatedly (`cargo test` 5×+) —
/// a refcount race can pass 1-in-3.
#[test]
fn pause_all_reclaims_with_multiple_controllers() {
    let mut sweep = make_sweep();
    sweep.add_engine("first", "random_walk");
    seed_map(&mut sweep);
    let n1 = sweep.spawn();

    // Register a SECOND independent sweep — a new spawn = a new controller sharing the
    // same trie. (Can't seed while threads run.)
    sweep.add_engine("second", "cpq");
    let n2 = sweep.spawn();
    assert_eq!(sweep.controllers.len(), 2, "two spawns = two controllers");
    assert_ne!(n1, n2, "spawn handles must be unique");

    // THE fix under test: pause_all with two controllers must not panic and must reclaim.
    let map = sweep.pause_all();
    assert!(sweep.map.is_none(), "STATE A after pause_all");
    let z = map.read_zipper_at_path(b"aa");
    assert_eq!(z.val(), Some(&10u64), "seeded value survives multi-sweep reclaim");

    // Resume both; both controllers stay registered and active.
    sweep.resume_all(map);
    assert_eq!(sweep.controllers.len(), 2, "both controllers survive resume");
    for ctrl in sweep.controllers.values() {
        assert!(!ctrl.is_paused(), "controller active after resume");
    }

    sweep.shutdown_all();
    assert!(sweep.controllers.is_empty());
}

/// Multi-controller `shutdown_all` must fold B→A and hand back the trie (last controller
/// reclaims). Before the fix, a non-last controller's Drop nulled the shared slot, so the
/// last shutdown found nothing to reclaim and the trie was lost.
#[test]
fn shutdown_all_reclaims_with_multiple_controllers() {
    let mut sweep = make_sweep();
    sweep.add_engine("first", "random_walk");
    seed_map(&mut sweep);
    sweep.spawn();
    sweep.add_engine("second", "cpq");
    sweep.spawn();
    assert_eq!(sweep.controllers.len(), 2);

    let reclaimed = sweep.shutdown_all();
    assert!(sweep.controllers.is_empty(), "all controllers gone");
    let map = reclaimed.expect("last controller must reclaim the trie");
    let z = map.read_zipper_at_path(b"aa");
    assert_eq!(z.val(), Some(&10u64), "seeded value survives multi-sweep shutdown");
}
