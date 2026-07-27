use mork::expr;
use mork::space::{QueryPolicy, Space};

fn dump(space: &Space, pattern: mork_expr::Expr, template: mork_expr::Expr) -> String {
    let mut out = Vec::new();
    space.dump_sexpr(pattern, template, &mut out);
    String::from_utf8(out).expect("dump output should be valid UTF-8")
}

#[test]
fn query_policy_all_runs_existing_source_sink_path() {
    let mut space = Space::new();
    space.add_all_sexpr(b"(edge A B)(edge A C)").unwrap();

    let pattern = expr!(space, "[2] , [3] edge $ $");
    let template = expr!(space, "[2] O [2] + [3] seen _1 _2");
    let marker = expr!(space, "[1] query-policy-all");

    let (touched, changed) = space.transform_multi_multi_io_with_policy(
        pattern,
        template,
        marker,
        true,
        false,
        QueryPolicy::All,
    );

    assert_eq!(touched, 2);
    assert!(changed);

    let seen = dump(
        &space,
        expr!(space, "[3] seen $ $"),
        expr!(space, "[3] seen _1 _2"),
    );
    assert!(seen.contains("(seen A B)"), "seen:\n{seen}");
    assert!(seen.contains("(seen A C)"), "seen:\n{seen}");
}

fn run_weighted_pick(space: &mut Space, engine: &str) -> String {
    space.remove_all_sexpr(b"(picked A)(picked B)").unwrap();

    let pattern = expr!(space, "[2] , [2] mln-site $");
    let template = expr!(space, "[2] O [2] + [2] picked _1");
    let marker = expr!(space, "[1] query-policy-weighted");

    let (touched, _) = space.transform_multi_multi_io_with_policy(
        pattern,
        template,
        marker,
        true,
        false,
        QueryPolicy::WeightedOne {
            engine: engine.to_string(),
        },
    );

    assert_eq!(touched, 1);
    dump(
        space,
        expr!(space, "[2] picked $"),
        expr!(space, "[2] picked _1"),
    )
}

#[test]
fn query_policy_weighted_one_emits_only_one_output() {
    let mut space = Space::new();
    space.add_all_sexpr(b"(mln-site A)(mln-site B)").unwrap();

    let picked = run_weighted_pick(&mut space, "random_walk");
    let picked_a = picked.contains("(picked A)");
    let picked_b = picked.contains("(picked B)");

    assert_ne!(picked_a, picked_b, "expected exactly one picked atom:\n{picked}");
}

#[test]
fn query_policy_weighted_one_defaults_unweighted_candidates_to_one() {
    let mut space = Space::new();
    space.add_all_sexpr(b"(mln-site A)(mln-site B)").unwrap();

    let picked = run_weighted_pick(&mut space, "random_walk");

    assert!(
        picked.contains("(picked A)") || picked.contains("(picked B)"),
        "unweighted candidates should remain selectable with default weight 1:\n{picked}"
    );
}

#[test]
fn query_policy_weighted_one_prefers_higher_weight_random_walk_candidate() {
    let mut space = Space::new();
    space
        .add_all_sexpr(b"(mln-site A (# 1000))(mln-site B (# 1))")
        .unwrap();

    let mut a = 0;
    let mut b = 0;
    for _ in 0..100 {
        let picked = run_weighted_pick(&mut space, "random_walk");
        if picked.contains("(picked A)") {
            a += 1;
        } else if picked.contains("(picked B)") {
            b += 1;
        } else {
            panic!("expected one picked atom:\n{picked}");
        }
    }

    assert!(a > b, "expected A to be picked more often than B, got A={a} B={b}");
}

#[test]
fn query_policy_weighted_one_cpq_selects_highest_weight_candidate() {
    let mut space = Space::new();
    space
        .add_all_sexpr(b"(mln-site A (# 10))(mln-site B (# 1))")
        .unwrap();

    for _ in 0..5 {
        let picked = run_weighted_pick(&mut space, "cpq");
        assert!(picked.contains("(picked A)"), "cpq should pick highest weight:\n{picked}");
        assert!(!picked.contains("(picked B)"), "cpq should only pick one candidate:\n{picked}");
    }
}
