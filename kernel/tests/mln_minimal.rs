use mork::expr;
use mork::space::Space;

const MLN_MINIMAL: &str = include_str!("../resources/mln_minimal.mm2");
const METTA_STEPS_PER_ATTEMPT: usize = 32;

fn dump(space: &Space, pattern: mork_expr::Expr, template: mork_expr::Expr) -> String {
    let mut out = Vec::new();
    space.dump_sexpr(pattern, template, &mut out);
    String::from_utf8(out).expect("dump output should be valid UTF-8")
}

fn load_mln() -> Space {
    let mut space = Space::new();
    space.add_all_sexpr(MLN_MINIMAL.as_bytes()).unwrap();
    assert_eq!(space.sweep(), "sweep-config");
    space
}

fn run_attempts(space: &mut Space, attempts: usize) {
    let stats = space
        .cycle_scheduler(1, METTA_STEPS_PER_ATTEMPT, attempts)
        .unwrap();
    assert_eq!(stats.cycles, attempts);
    assert_eq!(stats.sweep_steps, attempts);
    assert!(
        stats.metta_steps > attempts,
        "each attempt should execute its bridge and transaction: {stats:?}"
    );
}

fn all_atoms(space: &Space) -> String {
    dump(space, expr!(space, "$"), expr!(space, "_1"))
}

fn sample_dump(space: &Space) -> String {
    dump(
        space,
        expr!(space, "[7] mln-sample $ $ $ $ $ $"),
        expr!(space, "[7] mln-sample _1 _2 _3 _4 _5 _6"),
    )
}

#[test]
fn mln_minimal_runs_weighted_metropolis_update_and_cleans_transients() {
    let mut space = load_mln();
    run_attempts(&mut space, 1);

    let all = all_atoms(&space);
    assert!(
        all.contains("(mln-sample 0 A "),
        "one sweep cycle should produce one sample:\n{all}"
    );
    assert!(
        all.contains(" A false true -2)") || all.contains(" A false true -2 "),
        "the first proposal should have the exact two-clause delta:\n{all}"
    );
    assert!(
        all.contains("(mln-val B false)"),
        "evidence must remain fixed:\n{all}"
    );
    assert!(
        all.contains("(mln-rng 1729 1)"),
        "the explicit random stream position should advance once:\n{all}"
    );

    for transient_prefix in [
        "(was-sampled mln",
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
            "transaction transient remained after the bounded cycle: {transient_prefix}\n{all}"
        );
    }
}

#[test]
fn mln_minimal_clause_caches_follow_every_accepted_assignment() {
    let mut space = load_mln();

    for _ in 0..64 {
        run_attempts(&mut space, 1);
        let all = all_atoms(&space);
        let a_is_true = all.contains("(mln-val A true)");

        assert_eq!(
            all.contains("(mln-sat-count C1 0)"),
            a_is_true,
            "C1 is !A or false and must be unsatisfied exactly when A=true:\n{all}"
        );
        assert_eq!(
            all.contains("(mln-sat-count C2 1)"),
            a_is_true,
            "C2 is A and must be satisfied exactly when A=true:\n{all}"
        );
        assert!(
            all.contains("(mln-val B false)"),
            "evidence changed during sampling:\n{all}"
        );
    }
}

#[test]
fn mln_minimal_seeded_acceptance_trace_is_reproducible() {
    let mut first = load_mln();
    let mut second = load_mln();

    run_attempts(&mut first, 128);
    run_attempts(&mut second, 128);

    assert_eq!(sample_dump(&first), sample_dump(&second));
}

#[test]
fn mln_minimal_matches_exact_conditional_probability() {
    let mut space = load_mln();
    const BURN_IN: usize = 64;
    const SAMPLES: usize = 384;

    run_attempts(&mut space, BURN_IN + SAMPLES);

    let samples = sample_dump(&space);
    let mut measured = 0usize;
    let mut true_count = 0usize;
    for line in samples.lines() {
        let mut fields = line
            .trim_matches(|c| c == '(' || c == ')')
            .split_whitespace();
        assert_eq!(fields.next(), Some("mln-sample"));
        let step = fields
            .next()
            .expect("sample step")
            .parse::<usize>()
            .expect("numeric sample step");
        assert_eq!(fields.next(), Some("A"));
        let value = fields.next().expect("sample value");
        if step >= BURN_IN {
            measured += 1;
            true_count += usize::from(value == "true");
        }
    }

    assert_eq!(measured, SAMPLES);
    let empirical = true_count as f64 / measured as f64;
    let exact = (-0.5f64).exp() / (1.5f64.exp() + (-0.5f64).exp());
    assert!(
        (empirical - exact).abs() < 0.06,
        "empirical P(A=true|B=false)={empirical}, exact={exact}"
    );
}

#[test]
fn mln_minimal_positive_reverse_delta_is_always_accepted() {
    let mut space = load_mln();
    run_attempts(&mut space, 128);

    let accepted = dump(
        &space,
        expr!(space, "[6] mln-accepted $ A true false $"),
        expr!(space, "[6] mln-accepted _1 A true false _2"),
    );
    assert!(
        accepted.lines().all(|line| line.ends_with(" 2)")),
        "true-to-false transitions should have delta_score=2:\n{accepted}"
    );
    assert!(
        !accepted.is_empty(),
        "the trace should contain true-to-false transitions"
    );

    let rejected = dump(
        &space,
        expr!(space, "[6] mln-rejected $ A true false $"),
        expr!(space, "[6] mln-rejected _1 A true false _2"),
    );
    assert!(
        rejected.is_empty(),
        "positive-delta proposals must not be rejected:\n{rejected}"
    );
}

#[test]
fn mln_evidence_guard_consumes_an_accidentally_scheduled_evidence_site() {
    let mut space = load_mln();
    space.add_all_sexpr(b"(mln-site B (# 100))").unwrap();

    run_attempts(&mut space, 1);

    let all = all_atoms(&space);
    assert!(
        all.contains("(mln-skipped-evidence B)"),
        "scheduled evidence should be reported and skipped:\n{all}"
    );
    assert!(
        all.contains("(mln-val B false)"),
        "evidence value must not be rewritten:\n{all}"
    );
    assert!(
        !all.contains("(mln-sample 0 B "),
        "evidence must not produce an MLN transition sample:\n{all}"
    );
}
