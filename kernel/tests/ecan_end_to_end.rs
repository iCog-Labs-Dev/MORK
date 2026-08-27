use mork::scheduler::{CycleScheduler, SchedulePolicy};
use mork::space::{ParDataParser, Space};
use mork_expr::{Expr, ExprZipper};
use mork_frontend::bytestring_parser::{Context, Parser};
use weighted_atom_sweep::{AtomCandidate, ProcessId};

const STIMULATION: &str = include_str!("../resources/ecan-stimulation.mm2");
const DIFFUSION: &str = include_str!("../resources/ecan-diffusion.mm2");
const RENT: &str = include_str!("../resources/ecan-rent.mm2");

fn encoded(space: &Space, text: &[u8]) -> Vec<u8> {
    let mut stack = vec![0_u8; 1024];
    let mut input = Context::new(text);
    let mut parser = ParDataParser::new(&space.sm);
    let mut zipper = ExprZipper::new(Expr { ptr: stack.as_mut_ptr() });
    let (len, _) = parser.sexpr(&mut input, &mut zipper).unwrap();
    stack[..len].to_vec()
}

fn select(space: &mut Space, process: &str, fact: &[u8]) {
    let process_id = ProcessId(process.to_string());
    let candidate = AtomCandidate {
        process_id: process_id.clone(),
        path: encoded(space, fact),
        snapshot_version: space.snapshot_version,
    };
    space.was.candidate_buffers.entry(process_id).or_default().push_back(candidate);
}

fn dump(space: &Space) -> String {
    let mut output = Vec::new();
    space.dump_all_sexpr(&mut output).unwrap();
    String::from_utf8(output).unwrap()
}

#[test]
fn foreground_stimulation_then_ordered_diffusion_and_rent() {
    let mut space = Space::new();
    space.add_all_sexpr(br#"
        (Category insect insect) (Category bridge bridge)
        (STI insect 0.0) (LTI insect 0.0) (VLTI insect 0)
        (STI bridge 0.0) (LTI bridge 0.0) (VLTI bridge 0)
        (BankSTI 2000.0) (BankLTI 2000.0)
        (ECANParam TARGET_STI 2000.0) (ECANParam TARGET_LTI 2000.0)
        (ECANParam STI_FUNDS_BUFFER 1000.0) (ECANParam LTI_FUNDS_BUFFER 1000.0)
        (ECANParam STI_ATOM_WAGE 10.0) (ECANParam LTI_ATOM_WAGE 10.0)
        (ECANParam MAX_SPREAD_PERCENTAGE 0.4)
        (ECANParam HEBBIAN_MAX_ALLOCATION_PERCENTAGE 0.05)
        (ECANParam StartingAtomStiRent 1.0)
        (ECANParam StartingAtomLtiRent 1.0)
        (ECANParam RENT_ELAPSED_ROUNDS 1.0)
        (DiffusionEdge edge insect bridge SimilarityLink 0.24 1.0)
        (Stimulate event insect 60.0)
    "#).unwrap();
    space.add_all_sexpr(STIMULATION.as_bytes()).unwrap();
    assert_eq!(space.metta_calculus(10), 2);
    assert!(dump(&space).contains("(STI insect 600.0)"));

    let mut scheduler = CycleScheduler::new(SchedulePolicy::Fair {
        foreground_credits: 0,
        background_credits_per_process: 1,
    });
    scheduler.configure_process(ProcessId("ecan_af_diffusion".into()), 1);
    scheduler.configure_process(ProcessId("ecan_af_rent".into()), 1);

    space.add_all_sexpr(DIFFUSION.as_bytes()).unwrap();
    select(&mut space, "ecan_af_diffusion", b"(STI insect 600.0)");
    assert_eq!(scheduler.run_background_round(&mut space), 1);
    assert_eq!(space.metta_calculus(10), 1);
    let after_diffusion = dump(&space);
    assert!(after_diffusion.contains("(STI insect 372.0)"), "{after_diffusion}");
    assert!(after_diffusion.contains("(STI bridge 228.0)"), "{after_diffusion}");

    space.add_all_sexpr(RENT.as_bytes()).unwrap();
    select(&mut space, "ecan_af_rent", b"(STI insect 372.0)");
    assert_eq!(scheduler.run_background_round(&mut space), 1);
    assert_eq!(space.metta_calculus(10), 2);

    let result = dump(&space);
    assert!(result.contains("(STI insect 370.4)"), "{result}");
    assert!(result.contains("(LTI insect 598.4)"), "{result}");
    assert!(result.contains("(BankSTI 1401.6)"), "{result}");
    assert!(result.contains("(BankLTI 1401.6)"), "{result}");
    assert!(result.contains("(STI bridge 228.0)"), "{result}");
    assert!(result.contains("(Diffused edge insect bridge"), "{result}");
    assert!(result.contains("(RentCollected insect"), "{result}");
    assert!(result.contains("(WAS ecan_af_diffusion"), "{result}");
    assert!(result.contains("(WAS ecan_af_rent"), "{result}");
    assert_eq!(370.4 + 228.0 + 1401.6, 2000.0);
    assert_eq!(598.4 + 1401.6, 2000.0);
}
