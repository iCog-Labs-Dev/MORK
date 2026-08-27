use mork::scheduler::{CycleScheduler, SchedulePolicy};
use mork::space::{ParDataParser, Space};
use mork_expr::{Expr, ExprZipper};
use mork_frontend::bytestring_parser::{Context, Parser};
use weighted_atom_sweep::{AtomCandidate, ProcessId};

const RULES: &str = include_str!("../resources/ecan-rent.mm2");

fn encoded(space: &Space, text: &[u8]) -> Vec<u8> {
    let mut stack = vec![0_u8; 1024];
    let mut input = Context::new(text);
    let mut parser = ParDataParser::new(&space.sm);
    let mut zipper = ExprZipper::new(Expr { ptr: stack.as_mut_ptr() });
    let (len, _) = parser.sexpr(&mut input, &mut zipper).unwrap();
    stack[..len].to_vec()
}

fn rent_once(space: &mut Space, fact: &[u8]) {
    let process_id = ProcessId("ecan_af_rent".to_string());
    let path = encoded(space, fact);
    space.was.candidate_buffers.entry(process_id.clone()).or_default().push_back(
        AtomCandidate {
            process_id,
            path,
            snapshot_version: space.snapshot_version,
        },
    );
    let mut scheduler = CycleScheduler::new(SchedulePolicy::Fair {
        foreground_credits: 0,
        background_credits_per_process: 1,
    });
    assert_eq!(scheduler.run_background_round(space), 1);
    assert_eq!(space.metta_calculus(10), 2);
}

fn dump(space: &Space) -> String {
    let mut output = Vec::new();
    space.dump_all_sexpr(&mut output).unwrap();
    String::from_utf8(output).unwrap()
}

fn base_state(bank_sti: f64, bank_lti: f64, elapsed: f64) -> String {
    format!(r#"
        (STI atom 10.0) (LTI atom 5.0) (VLTI atom 0)
        (BankSTI {bank_sti:.1}) (BankLTI {bank_lti:.1})
        (ECANParam TARGET_STI 1000.0) (ECANParam TARGET_LTI 1000.0)
        (ECANParam STI_FUNDS_BUFFER 100.0) (ECANParam LTI_FUNDS_BUFFER 100.0)
        (ECANParam StartingAtomStiRent 1.0)
        (ECANParam StartingAtomLtiRent 2.0)
        (ECANParam RENT_ELAPSED_ROUNDS {elapsed})
        (Unrelated survives)
    "#)
}

#[test]
fn bank_at_target_collects_zero_rent() {
    let mut space = Space::new();
    space.add_all_sexpr(base_state(1000.0, 1100.0, 3.0).as_bytes()).unwrap();
    space.add_all_sexpr(RULES.as_bytes()).unwrap();
    rent_once(&mut space, b"(STI atom 10.0)");

    let result = dump(&space);
    assert!(result.contains("(STI atom 10.0)"), "{result}");
    assert!(result.contains("(LTI atom 5.0)"), "{result}");
    assert!(result.contains("(BankSTI 1000.0)"), "{result}");
    assert!(result.contains("(BankLTI 1100.0)"), "{result}");
    assert!(result.contains("(RentCollected atom 10.0 5.0 10.0 5.0 0.0 0.0)"), "{result}");
}

#[test]
fn shortage_scales_rent_and_clamps_without_underflow() {
    let mut space = Space::new();
    space.add_all_sexpr(base_state(900.0, 950.0, 2.0).as_bytes()).unwrap();
    space.add_all_sexpr(RULES.as_bytes()).unwrap();
    rent_once(&mut space, b"(STI atom 10.0)");

    let result = dump(&space);
    assert!(result.contains("(STI atom 6.0)"), "{result}");
    assert!(result.contains("(LTI atom 0.0)"), "{result}");
    assert!(result.contains("(BankSTI 904.0)"), "{result}");
    assert!(result.contains("(BankLTI 955.0)"), "{result}");
    assert!(result.contains("(RentCollected atom 10.0 5.0 6.0 0.0 4.0 5.0)"), "{result}");
    assert_eq!(6.0 + 904.0, 10.0 + 900.0);
    assert_eq!(0.0 + 955.0, 5.0 + 950.0);
    assert!(result.contains("(VLTI atom 0)"), "{result}");
    assert!(result.contains("(Unrelated survives)"), "{result}");
}
