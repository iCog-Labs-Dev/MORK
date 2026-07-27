use mork::expr;
use mork::space::Space;

fn dump(space: &Space, pattern: mork_expr::Expr, template: mork_expr::Expr) -> String {
    let mut out = Vec::new();
    space.dump_sexpr(pattern, template, &mut out);
    String::from_utf8(out).expect("dump output should be valid UTF-8")
}

#[test]
fn run_sweep_once_emits_event_through_existing_output_sink() {
    let mut space = Space::new();
    space
        .add_all_sexpr(
            b"
            (mln-site A (# 10))
            (mln-site B (# 1))
            (sweep mln
              (e cpq)
              (src (, (mln-site $x)))
              (sink (O (+ (was-sampled mln (mln-site $x))))))
            ",
        )
        .unwrap();

    assert_eq!(space.sweep(), "sweep-config");

    let (touched, changed) = space.run_sweep_once("mln").unwrap();

    assert_eq!(touched, 1);
    assert!(changed);

    let sampled = dump(
        &space,
        expr!(space, "[3] was-sampled mln [2] mln-site $"),
        expr!(space, "[3] was-sampled mln [2] mln-site _1"),
    );
    assert!(
        sampled.contains("(was-sampled mln (mln-site A))"),
        "cpq should select the higher weighted site:\n{sampled}"
    );
    assert!(
        !sampled.contains("(was-sampled mln (mln-site B))"),
        "weighted sweep should emit one event:\n{sampled}"
    );
}

#[test]
fn run_sweep_once_supports_explicit_btm_source_rules() {
    let mut space = Space::new();
    space
        .add_all_sexpr(
            b"
            (mln-site A (# 10))
            (mln-site B (# 1))
            (sweep mln
              (e cpq)
              (src (I (BTM (mln-site $x))))
              (sink (O (+ (was-sampled mln (mln-site $x))))))
            ",
        )
        .unwrap();

    assert_eq!(space.sweep(), "sweep-config");

    let (touched, changed) = space.run_sweep_once("mln").unwrap();

    assert_eq!(touched, 1);
    assert!(changed);

    let sampled = dump(
        &space,
        expr!(space, "[3] was-sampled mln [2] mln-site $"),
        expr!(space, "[3] was-sampled mln [2] mln-site _1"),
    );
    assert!(
        sampled.contains("(was-sampled mln (mln-site A))"),
        "explicit BTM source should use the matched BTM atom's weight:\n{sampled}"
    );
}

#[test]
fn run_sweep_once_uses_product_weight_policy() {
    let mut space = Space::new();
    space
        .add_all_sexpr(
            b"
            (site A (# 10))
            (ready A (# 2))
            (site B (# 3))
            (ready B (# 20))
            (sweep planner
              (e cpq)
              (src (, (site $x) (ready $x)))
              (weight product)
              (sink (O (+ (was-sampled planner (site $x))))))
            ",
        )
        .unwrap();

    assert_eq!(space.sweep(), "sweep-config");

    let (touched, changed) = space.run_sweep_once("planner").unwrap();

    assert_eq!(touched, 1);
    assert!(changed);

    let sampled = dump(
        &space,
        expr!(space, "[3] was-sampled planner [2] site $"),
        expr!(space, "[3] was-sampled planner [2] site _1"),
    );
    assert!(
        sampled.contains("(was-sampled planner (site B))"),
        "product should prefer B: A=10*2, B=3*20\n{sampled}"
    );
    assert!(
        !sampled.contains("(was-sampled planner (site A))"),
        "weighted sweep should emit one event:\n{sampled}"
    );
}

#[test]
fn run_sweep_once_uses_expr_weight_policy() {
    let mut space = Space::new();
    space
        .add_all_sexpr(
            b"
            (task A)
            (priority A 2)
            (task B)
            (priority B 20)
            (sweep planner
              (e cpq)
              (src (, (task $t) (priority $t $w)))
              (weight expr $w)
              (sink (O (+ (was-sampled planner (task $t))))))
            ",
        )
        .unwrap();

    assert_eq!(space.sweep(), "sweep-config");

    let (touched, changed) = space.run_sweep_once("planner").unwrap();

    assert_eq!(touched, 1);
    assert!(changed);

    let sampled = dump(
        &space,
        expr!(space, "[3] was-sampled planner [2] task $"),
        expr!(space, "[3] was-sampled planner [2] task _1"),
    );
    assert!(
        sampled.contains("(was-sampled planner (task B))"),
        "expr weight should use the bound priority value:\n{sampled}"
    );
    assert!(
        !sampled.contains("(was-sampled planner (task A))"),
        "weighted sweep should emit one event:\n{sampled}"
    );
}

#[test]
fn run_sweep_cycles_runs_each_source_sink_sweep_per_cycle() {
    let mut space = Space::new();
    space
        .add_all_sexpr(
            b"
            (mln-site A (# 10))
            (task T (# 5))
            (sweep mln
              (e cpq)
              (src (, (mln-site $x)))
              (sink (O (+ (was-sampled mln (mln-site $x))))))
            (sweep planner
              (e cpq)
              (src (, (task $t)))
              (sink (O (+ (was-sampled planner (task $t))))))
            ",
        )
        .unwrap();

    assert_eq!(space.sweep(), "sweep-config");

    let (touched, changed) = space.run_sweep_cycles(2).unwrap();

    assert_eq!(touched, 4);
    assert!(changed);

    let mln_sampled = dump(
        &space,
        expr!(space, "[3] was-sampled mln [2] mln-site $"),
        expr!(space, "[3] was-sampled mln [2] mln-site _1"),
    );
    let planner_sampled = dump(
        &space,
        expr!(space, "[3] was-sampled planner [2] task $"),
        expr!(space, "[3] was-sampled planner [2] task _1"),
    );
    assert!(
        mln_sampled.contains("(was-sampled mln (mln-site A))"),
        "mln event missing:\n{mln_sampled}"
    );
    assert!(
        planner_sampled.contains("(was-sampled planner (task T))"),
        "planner event missing:\n{planner_sampled}"
    );
}
