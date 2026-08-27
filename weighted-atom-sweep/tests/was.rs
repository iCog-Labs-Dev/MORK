use pathmap::PathMap;
use pathmap::zipper::ZipperCreation;
use std::time::Duration;
use weighted_atom_sweep::{ProcessId, WeightedAtomSweep, WeightedAtomSweepSettings};

fn make_sweep() -> WeightedAtomSweep {
    WeightedAtomSweep::new(WeightedAtomSweepSettings::default())
}

fn create_map(values: &[(&[u8], u64)]) -> PathMap<u64> {
    let mut map = PathMap::<u64>::new();
    for &(path, val) in values {
        let mut wz = map.write_zipper_at_path(path);
        wz.set_val_w(val);
    }
    map
}

#[test]
fn test_live_map_remains_outside_and_was_cannot_mutate() {
    let mut sweep = make_sweep();
    sweep.add_engine("engine_a", "random_walk");

    // Live map is owned here in the test (simulating MORK Space ownership)
    let mut live_map = create_map(&[(&b"aaaa"[..], 10u64)]);

    sweep.spawn();

    // Publish a clone of the live map
    sweep.publish_snapshot(live_map.clone(), 1);

    let rx = sweep.candidate_rx.take().expect("no candidate receiver");
    let candidate = rx
        .recv_timeout(Duration::from_secs(2))
        .expect("failed to receive candidate");
    assert_eq!(candidate.snapshot_version, 1);

    // Mutate the live map here. WAS has no way to mutate it because it only got a COW clone.
    let mut wz = live_map.write_zipper_at_path(b"aaaa");
    wz.set_val_w(100u64);

    // Publish new snapshot
    sweep.publish_snapshot(live_map.clone(), 2);

    // Receive next candidate, eventually it should have version 2
    let mut seen_v2 = false;
    for _ in 0..2500 {
        if let Ok(c) = rx.recv_timeout(Duration::from_millis(10)) {
            if c.snapshot_version == 2 {
                seen_v2 = true;
                break;
            }
        }
    }
    assert!(seen_v2, "should have updated to version 2");

    sweep.shutdown_all();
}

#[test]
fn test_old_snapshot_remains_readable_during_mutation() {
    let mut sweep = make_sweep();
    sweep.add_engine("engine_a", "random_walk");

    let mut live_map = create_map(&[(&b"aaaa"[..], 10u64)]);
    sweep.spawn();

    // Publish version 1
    sweep.publish_snapshot(live_map.clone(), 1);

    // Mutate live map to a completely different layout
    let mut wz = live_map.write_zipper_at_path(b"bbbb");
    wz.set_val_w(50u64);

    // Do NOT publish version 2 yet. Workers must keep sampling from version 1 snapshot, which is stable and readable.
    let rx = sweep.candidate_rx.take().expect("no candidate receiver");

    let mut sampled_v1_path_aaaa = false;
    for _ in 0..100 {
        if let Ok(c) = rx.recv_timeout(Duration::from_millis(200)) {
            assert_eq!(c.snapshot_version, 1);
            if c.path == b"aaaa" {
                sampled_v1_path_aaaa = true;
                break;
            }
        }
    }
    assert!(
        sampled_v1_path_aaaa,
        "workers should successfully sample 'aaaa' from old snapshot"
    );

    sweep.shutdown_all();
}

#[test]
fn test_shutdown_works_while_waiting_for_snapshots() {
    let mut sweep = make_sweep();
    sweep.add_engine("engine_a", "random_walk");

    sweep.spawn();

    // Workers are running but have no snapshot (they sleep and wait)
    std::thread::sleep(Duration::from_millis(100));

    // Shutdown should join workers cleanly without hanging
    let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let done_clone = done.clone();

    let t = std::thread::spawn(move || {
        sweep.shutdown_all();
        done_clone.store(true, std::sync::atomic::Ordering::SeqCst);
    });

    t.join().expect("shutdown panicked");
    assert!(done.load(std::sync::atomic::Ordering::SeqCst));
}

#[test]
fn test_repeated_snapshot_publication_no_deadlock() {
    let mut sweep = make_sweep();
    sweep.add_engine("engine_a", "random_walk");
    let live_map = create_map(&[(&b"aaaa"[..], 10u64)]);

    sweep.spawn();

    for i in 0..50 {
        sweep.publish_snapshot(live_map.clone(), i);
    }

    let rx = sweep.candidate_rx.take().expect("no candidate receiver");
    let mut saw_latest = false;
    for _ in 0..1500 {
        if let Ok(candidate) = rx.recv_timeout(Duration::from_millis(10)) {
            if candidate.snapshot_version == 49 {
                saw_latest = true;
                break;
            }
        }
    }
    assert!(saw_latest, "worker should converge on the latest snapshot");

    sweep.shutdown_all();
}

#[test]
fn test_cpq_does_not_relabel_old_paths_with_new_snapshot_version() {
    let mut sweep = make_sweep();
    sweep.add_engine("cpq_engine", "cpq");
    sweep.spawn();

    sweep.publish_snapshot(create_map(&[(&b"aaaa"[..], 1)]), 1);
    let rx = sweep.candidate_rx.take().expect("no candidate receiver");
    let first = rx
        .recv_timeout(Duration::from_secs(2))
        .expect("no CPQ candidate");
    assert_eq!(first.path, b"aaaa");
    assert_eq!(first.snapshot_version, 1);

    sweep.publish_snapshot(create_map(&[(&b"bbbb"[..], 1)]), 2);
    let mut version_two = None;
    for _ in 0..1500 {
        if let Ok(candidate) = rx.recv_timeout(Duration::from_millis(10)) {
            if candidate.snapshot_version == 2 {
                version_two = Some(candidate);
                break;
            }
        }
    }

    let candidate = version_two.expect("CPQ did not traverse snapshot version 2");
    assert_eq!(candidate.path, b"bbbb");
    sweep.shutdown_all();
}

#[test]
fn strict_snapshot_validation_preserves_process_routing() {
    let mut sweep = make_sweep();
    let process_a = ProcessId("a".to_string());
    let process_b = ProcessId("b".to_string());
    let live_map = create_map(&[(&b"current"[..], 1)]);

    sweep.candidate_buffers.insert(
        process_a.clone(),
        vec![
            weighted_atom_sweep::AtomCandidate {
                process_id: process_a.clone(),
                path: b"current".to_vec(),
                snapshot_version: 1,
            },
            weighted_atom_sweep::AtomCandidate {
                process_id: process_a.clone(),
                path: b"missing".to_vec(),
                snapshot_version: 2,
            },
            weighted_atom_sweep::AtomCandidate {
                process_id: process_a.clone(),
                path: b"current".to_vec(),
                snapshot_version: 2,
            },
        ]
        .into(),
    );
    sweep.candidate_buffers.insert(
        process_b.clone(),
        vec![weighted_atom_sweep::AtomCandidate {
            process_id: process_b.clone(),
            path: b"current".to_vec(),
            snapshot_version: 2,
        }]
        .into(),
    );

    assert!(sweep.select_existing_candidate(&process_a, &live_map, 2));
    assert_eq!(sweep.metrics.stale_version_candidates, 1);
    assert_eq!(sweep.metrics.missing_path_candidates, 1);
    assert_eq!(sweep.metrics.candidates_consumed, 1);
    assert_eq!(
        sweep
            .take_selected_candidate(&process_a, &live_map, 2)
            .unwrap()
            .process_id,
        process_a
    );
    assert_eq!(sweep.candidate_buffers[&process_b].len(), 1);
}

#[test]
fn replaced_exact_fact_cannot_be_consumed() {
    let mut sweep = make_sweep();
    let process = ProcessId("ecan_af_rent".to_string());
    let old_path = b"(STI atom 10)".to_vec();
    let new_path = b"(STI atom 9)".to_vec();
    let live_map = create_map(&[(&new_path, 1)]);
    sweep.candidate_buffers.insert(
        process.clone(),
        vec![weighted_atom_sweep::AtomCandidate {
            process_id: process.clone(),
            path: old_path,
            snapshot_version: 3,
        }]
        .into(),
    );

    assert!(!sweep.select_existing_candidate(&process, &live_map, 3));
    assert_eq!(sweep.metrics.missing_path_candidates, 1);
    assert_eq!(sweep.metrics.candidates_consumed, 0);
}

#[test]
fn obsolete_buffered_and_reserved_candidates_are_cleared() {
    let mut sweep = make_sweep();
    let process = ProcessId("ecan_af_diffusion".to_string());
    let live_map = create_map(&[(&b"current"[..], 1)]);
    sweep.candidate_buffers.insert(
        process.clone(),
        vec![
            weighted_atom_sweep::AtomCandidate {
                process_id: process.clone(),
                path: b"current".to_vec(),
                snapshot_version: 4,
            },
            weighted_atom_sweep::AtomCandidate {
                process_id: process.clone(),
                path: b"current".to_vec(),
                snapshot_version: 5,
            },
        ]
        .into(),
    );
    assert!(sweep.select_existing_candidate(&process, &live_map, 4));

    sweep.discard_obsolete_candidates(5);

    assert!(
        sweep
            .take_selected_candidate(&process, &live_map, 5)
            .is_none()
    );
    assert_eq!(sweep.candidate_buffers[&process].len(), 1);
    assert_eq!(sweep.metrics.stale_version_candidates, 1);
}
