use pathmap::PathMap;
use std::collections::HashSet;
use std::time::{Duration, Instant};
use weighted_atom_sweep::{WeightedAtomSweep, WeightedAtomSweepSettings};

fn make_sweep() -> WeightedAtomSweep {
    let mut s = WeightedAtomSweep::new(WeightedAtomSweepSettings::default());
    s.add_engine("e", "random_walk");
    s
}

#[test]
fn spawn_populates_controllers() {
    let mut sweep = make_sweep();
    let name = sweep.spawn();
    assert!(!name.is_empty(), "spawn name should be non-empty");
    assert_eq!(sweep.controllers.len(), 1);
    assert!(sweep.controllers.contains_key(&name));
}

#[test]
fn spawn_returns_unique_names() {
    let mut sweep = WeightedAtomSweep::new(WeightedAtomSweepSettings::default());
    sweep.add_engine("a", "random_walk");
    sweep.add_engine("b", "cpq");
    sweep.add_engine("c", "random_walk");
    let n1 = sweep.spawn();
    let n2 = sweep.spawn();
    let n3 = sweep.spawn();
    assert_ne!(n1, n2);
    assert_ne!(n2, n3);
    assert_eq!(sweep.controllers.len(), 3);
}

#[test]
fn shutdown_removes_controller() {
    let mut sweep = make_sweep();
    let name = sweep.spawn();
    assert!(sweep.controllers.contains_key(&name));
    let _ = sweep.shutdown(&name);
    // controller is removed (map reclamation is best-effort, pre-existing issue)
    assert!(!sweep.controllers.contains_key(&name));
}

#[test]
fn shutdown_empties_all_controllers() {
    let mut sweep = WeightedAtomSweep::new(WeightedAtomSweepSettings::default());
    sweep.add_engine("a", "random_walk");
    sweep.add_engine("b", "cpq");
    sweep.add_engine("c", "random_walk");
    sweep.spawn();
    sweep.spawn();
    sweep.spawn();
    assert_eq!(sweep.controllers.len(), 3);
    let _ = sweep.shutdown_all();
    assert!(sweep.controllers.is_empty());
}

#[test]
fn process_count_tracks_registered_processes() {
    let mut sweep = WeightedAtomSweep::new(WeightedAtomSweepSettings::default());
    assert_eq!(sweep.process_count(), 0);
    sweep.add_engine("a", "random_walk");
    assert_eq!(sweep.process_count(), 1);
    sweep.add_engine("b", "cpq");
    assert_eq!(sweep.process_count(), 2);
}

#[test]
fn get_process_mut_returns_registered_process() {
    let mut sweep = WeightedAtomSweep::new(WeightedAtomSweepSettings::default());
    sweep.add_engine("test_engine", "random_walk");
    let p = sweep.get_process_mut("test_engine");
    assert!(p.is_some());
    assert_eq!(p.unwrap().id.0, "test_engine");
    assert!(sweep.get_process_mut("nonexistent").is_none());
}

#[test]
fn repeated_spawn_keeps_one_candidate_channel() {
    let mut sweep = WeightedAtomSweep::new(WeightedAtomSweepSettings::default());
    sweep.add_engine("first", "random_walk");
    sweep.spawn();
    sweep.add_engine("second", "random_walk");
    sweep.spawn();

    let mut map = PathMap::<u64>::new();
    map.write_zipper_at_path(b"atom").set_val_w(1);
    sweep.publish_snapshot(map, 1);

    let rx = sweep.candidate_rx.take().expect("no candidate receiver");
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut processes = HashSet::new();
    while Instant::now() < deadline && processes.len() < 2 {
        if let Ok(candidate) = rx.recv_timeout(Duration::from_millis(20)) {
            processes.insert(candidate.process_id.0);
        }
    }

    assert!(processes.contains("first"));
    assert!(processes.contains("second"));
    sweep.shutdown_all();
}
