use mork::expr;
use mork::space::Space;

const SOCIAL_MLN: &str = include_str!("../resources/mln_social_network.mm2");
const ATTEMPTS: usize = 192;

fn dump(space: &Space, pattern: mork_expr::Expr, template: mork_expr::Expr) -> String {
    let mut out = Vec::new();
    space.dump_sexpr(pattern, template, &mut out);
    String::from_utf8(out).expect("dump output should be valid UTF-8")
}

fn value(all: &str, atom: &str) -> bool {
    let true_fact = format!("(mln-val {atom} true)");
    let false_fact = format!("(mln-val {atom} false)");
    assert_ne!(
        all.contains(&true_fact),
        all.contains(&false_fact),
        "{atom} must have exactly one Boolean value"
    );
    all.contains(&true_fact)
}

#[test]
fn grounded_social_network_runs_and_preserves_all_mln_invariants() {
    let mut space = Space::new();
    space.add_all_sexpr(SOCIAL_MLN.as_bytes()).unwrap();
    assert_eq!(space.sweep(), "sweep-config");

    let stats = space.cycle_scheduler(1, 32, ATTEMPTS).unwrap();
    assert_eq!(stats.cycles, ATTEMPTS);
    assert_eq!(stats.sweep_steps, ATTEMPTS);
    assert!(stats.metta_steps > ATTEMPTS);

    let all = dump(&space, expr!(space, "$"), expr!(space, "_1"));
    assert!(all.contains("(mln-val (Friends Alice Bob) true)"));
    assert!(all.contains("(mln-val (Friends Bob Carol) true)"));
    assert!(all.contains("(mln-val (Friends Carol Alice) false)"));
    assert!(all.contains(&format!("(mln-rng 424242 {ATTEMPTS})")));

    let smokes_alice = value(&all, "(Smokes Alice)");
    let smokes_bob = value(&all, "(Smokes Bob)");
    let smokes_carol = value(&all, "(Smokes Carol)");
    let cancer_alice = value(&all, "(Cancer Alice)");
    let cancer_bob = value(&all, "(Cancer Bob)");
    let cancer_carol = value(&all, "(Cancer Carol)");

    let expected_caches = [
        (
            "health-alice",
            usize::from(!smokes_alice) + usize::from(cancer_alice),
        ),
        (
            "health-bob",
            usize::from(!smokes_bob) + usize::from(cancer_bob),
        ),
        (
            "health-carol",
            usize::from(!smokes_carol) + usize::from(cancer_carol),
        ),
        (
            "influence-alice-bob",
            usize::from(!smokes_alice) + usize::from(smokes_bob),
        ),
        (
            "influence-bob-carol",
            usize::from(!smokes_bob) + usize::from(smokes_carol),
        ),
        (
            "influence-carol-alice",
            1 + usize::from(!smokes_carol) + usize::from(smokes_alice),
        ),
        ("smoke-prior-alice", usize::from(smokes_alice)),
        ("smoke-prior-bob", usize::from(smokes_bob)),
        ("smoke-prior-carol", usize::from(smokes_carol)),
        ("cancer-prior-alice", usize::from(cancer_alice)),
        ("cancer-prior-bob", usize::from(cancer_bob)),
        ("cancer-prior-carol", usize::from(cancer_carol)),
    ];
    for (clause, count) in expected_caches {
        assert!(
            all.contains(&format!("(mln-sat-count {clause} {count})")),
            "stale cache for {clause}:\n{all}"
        );
    }

    let samples = dump(
        &space,
        expr!(space, "[7] mln-sample $ $ $ $ $ $"),
        expr!(space, "[7] mln-sample _1 _2 _3 _4 _5 _6"),
    );
    assert_eq!(samples.lines().count(), ATTEMPTS);
    for atom in [
        "(Smokes Alice)",
        "(Smokes Bob)",
        "(Smokes Carol)",
        "(Cancer Alice)",
        "(Cancer Bob)",
        "(Cancer Carol)",
    ] {
        assert!(
            samples.lines().any(|line| line.contains(atom)),
            "weighted scheduler never visited {atom}:\n{samples}"
        );
    }

    for transient_prefix in [
        "(was-sampled social-mln",
        "(mln-proposal ",
        "(mln-random ",
        "(mln-delta-part ",
        "(mln-delta ",
        "(mln-decision ",
        "(mln-clause-update ",
        "(mln-activate ",
        "(exec ",
    ] {
        assert!(
            !all.lines().any(|line| line.starts_with(transient_prefix)),
            "transaction transient remained after sampling: {transient_prefix}\n{all}"
        );
    }
}
