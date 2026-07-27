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
