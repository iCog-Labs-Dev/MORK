use mork::space::Space;

const RULE: &str = include_str!("../resources/ecan-stimulation.mm2");

fn dump(space: &Space) -> String {
    let mut output = Vec::new();
    space.dump_all_sexpr(&mut output).unwrap();
    String::from_utf8(output).unwrap()
}

#[test]
fn ants_stimulation_matches_metta_and_conserves_both_currencies() {
    let mut space = Space::new();
    let state = r#"
        (Stimulate event-001 Ants 30)
        (STI Ants 0.0)
        (LTI Ants 0.0)
        (VLTI Ants 0)
        (BankSTI 2000.0)
        (BankLTI 2000.0)
        (ECANParam TARGET_STI 1000.0)
        (ECANParam TARGET_LTI 1000.0)
        (ECANParam STI_FUNDS_BUFFER 1000.0)
        (ECANParam LTI_FUNDS_BUFFER 1000.0)
        (ECANParam STI_ATOM_WAGE 10.0)
        (ECANParam LTI_ATOM_WAGE 10.0)
        (Unrelated survives)
    "#;
    space.add_all_sexpr(state.as_bytes()).unwrap();
    space.add_all_sexpr(RULE.as_bytes()).unwrap();

    assert_eq!(space.metta_calculus(10), 2);
    let result = dump(&space);

    assert!(result.contains("(STI Ants 600.0)"), "{result}");
    assert!(result.contains("(LTI Ants 600.0)"), "{result}");
    assert!(result.contains("(BankSTI 1400.0)"), "{result}");
    assert!(result.contains("(BankLTI 1400.0)"), "{result}");
    assert!(result.contains("(VLTI Ants 0)"), "{result}");
    assert!(result.contains("(Unrelated survives)"), "{result}");
    assert!(result.contains("(Stimulated event-001 Ants 30)"), "{result}");
    assert!(!result.contains("(Stimulate event-001 Ants 30)"), "{result}");
    assert_eq!(600.0 + 1400.0, 2000.0);
}

#[test]
fn request_is_consumed_once_and_rule_is_one_shot() {
    let mut space = Space::new();
    let state = r#"
        (Stimulate event-002 atom 1.0)
        (STI atom 0.0) (LTI atom 0.0) (VLTI atom 0)
        (BankSTI 100.0) (BankLTI 100.0)
        (ECANParam TARGET_STI 100.0) (ECANParam TARGET_LTI 100.0)
        (ECANParam STI_FUNDS_BUFFER 100.0) (ECANParam LTI_FUNDS_BUFFER 100.0)
        (ECANParam STI_ATOM_WAGE 10.0) (ECANParam LTI_ATOM_WAGE 10.0)
    "#;
    space.add_all_sexpr(state.as_bytes()).unwrap();
    space.add_all_sexpr(RULE.as_bytes()).unwrap();

    assert_eq!(space.metta_calculus(10), 2);
    assert_eq!(space.metta_calculus(10), 0);
    let result = dump(&space);
    assert!(result.contains("(STI atom 10.0)"), "{result}");
    assert_eq!(result.matches("(Stimulated event-002 atom 1.0)").count(), 1);
}
