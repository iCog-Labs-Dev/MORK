use mork::space::{ParDataParser, Space};
use mork::scheduler::{CycleScheduler, SchedulePolicy};
use mork_expr::{Expr, ExprZipper};
use mork_frontend::bytestring_parser::{Context, Parser};
use std::collections::HashSet;
use weighted_atom_sweep::{AtomCandidate, ProcessId};

const RULES: &str = include_str!("../resources/ecan-diffusion.mm2");
const FIXTURE: &str = include_str!("../resources/ecan-shifting-drifting.mm2");

fn encoded(space: &Space, text: &[u8]) -> Vec<u8> {
    let mut stack = vec![0_u8; 1024];
    let mut input = Context::new(text);
    let mut parser = ParDataParser::new(&space.sm);
    let mut zipper = ExprZipper::new(Expr {
        ptr: stack.as_mut_ptr(),
    });
    let (len, _) = parser.sexpr(&mut input, &mut zipper).unwrap();
    stack[..len].to_vec()
}

fn select(space: &mut Space, fact: &[u8], version: u64) {
    let process_id = ProcessId("ecan_af_diffusion".to_string());
    let candidate = AtomCandidate {
        process_id: process_id.clone(),
        path: encoded(space, fact),
        snapshot_version: version,
    };
    space
        .was
        .candidate_buffers
        .entry(process_id)
        .or_default()
        .push_back(candidate);
}

fn dump(space: &Space) -> String {
    let mut output = Vec::new();
    space.dump_all_sexpr(&mut output).unwrap();
    String::from_utf8(output).unwrap()
}

fn diffuse_once(space: &mut Space) {
    let mut scheduler = CycleScheduler::new(SchedulePolicy::Fair {
        foreground_credits: 0,
        background_credits_per_process: 1,
    });
    assert_eq!(scheduler.run_background_round(space), 1);
    assert_eq!(space.metta_calculus(10), 1);
}

#[test]
fn one_edge_diffusion_matches_600_to_372_plus_228_and_conserves_sti() {
    let mut space = Space::new();
    let state = r#"
        (STI source 600.0) (STI target 0.0)
        (LTI source 7.0) (LTI target 3.0)
        (VLTI source 0) (VLTI target 0)
        (BankSTI 1400.0) (BankLTI 1400.0)
        (DiffusionEdge edge-001 source target SimilarityLink 0.24 1.0)
        (ECANParam MAX_SPREAD_PERCENTAGE 0.4)
        (ECANParam HEBBIAN_MAX_ALLOCATION_PERCENTAGE 0.05)
        (Unrelated survives)
    "#;
    space.add_all_sexpr(state.as_bytes()).unwrap();
    space.add_all_sexpr(RULES.as_bytes()).unwrap();
    select(&mut space, b"(STI source 600.0)", 0);

    diffuse_once(&mut space);
    let result = dump(&space);
    assert!(result.contains("(STI source 372.0)"), "{result}");
    assert!(result.contains("(STI target 228.0)"), "{result}");
    assert!(result.contains("(Diffused edge-001 source target 600.0 0.0 372.0 228.0 228.0)"), "{result}");
    assert!(result.contains("(BankSTI 1400.0)"), "{result}");
    assert!(result.contains("(BankLTI 1400.0)"), "{result}");
    assert!(result.contains("(LTI source 7.0)"), "{result}");
    assert!(result.contains("(Unrelated survives)"), "{result}");
    assert_eq!(372.0 + 228.0, 600.0);
}

#[test]
fn missing_target_or_edge_is_a_clean_noop() {
    for state in [
        "(STI source 600.0) (STI target 0.0) (ECANParam MAX_SPREAD_PERCENTAGE 0.4) (ECANParam HEBBIAN_MAX_ALLOCATION_PERCENTAGE 0.05)",
        "(STI source 600.0) (DiffusionEdge edge source missing SimilarityLink 1.0 1.0) (ECANParam MAX_SPREAD_PERCENTAGE 0.4) (ECANParam HEBBIAN_MAX_ALLOCATION_PERCENTAGE 0.05)",
    ] {
        let mut space = Space::new();
        space.add_all_sexpr(state.as_bytes()).unwrap();
        space.add_all_sexpr(RULES.as_bytes()).unwrap();
        select(&mut space, b"(STI source 600.0)", 0);
        diffuse_once(&mut space);
        let result = dump(&space);
        assert!(result.contains("(STI source 600.0)"), "{result}");
        assert!(!result.contains("(Diffused "), "{result}");
    }
}

#[test]
fn fixture_has_at_most_one_directed_edge_per_source() {
    let mut space = Space::new();
    space.add_all_sexpr(FIXTURE.as_bytes()).unwrap();
    let result = dump(&space);
    let mut sources = HashSet::new();
    let mut edge_count = 0;
    for line in result.lines().filter(|line| line.starts_with("(DiffusionEdge ")) {
        let fields: Vec<_> = line.trim_matches(['(', ')']).split_whitespace().collect();
        assert_eq!(fields.len(), 7, "{line}");
        assert_ne!(fields[2], fields[3], "{line}");
        assert!(sources.insert(fields[2].to_string()), "duplicate source: {line}");
        edge_count += 1;
    }
    assert_eq!(edge_count, 30);
}
