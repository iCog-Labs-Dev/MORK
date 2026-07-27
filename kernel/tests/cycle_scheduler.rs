use mork::expr;
use mork::space::Space;

fn dump(space: &Space, pattern: mork_expr::Expr, template: mork_expr::Expr) -> String {
    let mut out = Vec::new();
    space.dump_sexpr(pattern, template, &mut out);
    String::from_utf8(out).expect("dump output should be valid UTF-8")
}

#[test]
fn cycle_scheduler_runs_exact_metta_step_bound() {
    let mut space = Space::new();
    space
        .add_all_sexpr(
            b"
            (seed A)
            (exec 0
              (, (seed $x))
              (O (- (seed $x))
                 (+ (mid $x))))
            (exec 1
              (, (mid $x))
              (O (- (mid $x))
                 (+ (done $x))))
            ",
        )
        .unwrap();

    let stats = space.cycle_scheduler(0, 1, 1).unwrap();

    assert_eq!(stats.cycles, 1);
    assert_eq!(stats.sweep_steps, 0);
    assert_eq!(stats.sweep_touched, 0);
    assert_eq!(stats.metta_steps, 1);

    let all = dump(&space, expr!(space, "$"), expr!(space, "_1"));
    assert!(all.contains("(mid A)"), "one metta step should run:\n{all}");
    assert!(
        !all.contains("(done A)"),
        "second metta step should not run when bound is 1:\n{all}"
    );
}

#[test]
fn cycle_scheduler_zero_metta_steps_only_emits_sweep_event() {
    let mut space = Space::new();
    space
        .add_all_sexpr(
            b"
            (mln-site A (# 10))
            (sweep mln
              (e cpq)
              (src (, (mln-site $x)))
              (sink (O (+ (was-sampled mln (mln-site $x))))))
            (exec 0
              (, (was-sampled mln (mln-site $x)))
              (O (- (was-sampled mln (mln-site $x)))
                 (+ (mln-propose $x))))
            ",
        )
        .unwrap();
    assert_eq!(space.sweep(), "sweep-config");

    let stats = space.cycle_scheduler(1, 0, 1).unwrap();

    assert_eq!(stats.cycles, 1);
    assert_eq!(stats.sweep_steps, 1);
    assert_eq!(stats.sweep_touched, 1);
    assert!(stats.sweep_new);
    assert_eq!(stats.metta_steps, 0);

    let all = dump(&space, expr!(space, "$"), expr!(space, "_1"));
    assert!(
        all.contains("(was-sampled mln (mln-site A))"),
        "sweep event should be emitted:\n{all}"
    );
    assert!(
        !all.contains("(mln-propose A)"),
        "bridge exec should not run with zero metta steps:\n{all}"
    );
}

#[test]
fn cycle_scheduler_runs_each_registered_source_sink_sweep() {
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

    let stats = space.cycle_scheduler(2, 0, 1).unwrap();

    assert_eq!(stats.cycles, 1);
    assert_eq!(stats.sweep_steps, 2);
    assert_eq!(stats.sweep_touched, 4);
    assert!(stats.sweep_new);
    assert_eq!(stats.metta_steps, 0);

    let all = dump(&space, expr!(space, "$"), expr!(space, "_1"));
    assert!(
        all.contains("(was-sampled mln (mln-site A))"),
        "mln sweep event missing:\n{all}"
    );
    assert!(
        all.contains("(was-sampled planner (task T))"),
        "planner sweep event missing:\n{all}"
    );
}

#[test]
fn cycle_scheduler_turns_weighted_sweep_event_into_mln_update() {
    let mut space = Space::new();
    space
        .add_all_sexpr(
            b"
            (mln-var A)
            (mln-var B)
            (mln-val A false)
            (mln-val B false)
            (mln-clause C1 1500)
            (mln-lit C1 A negative)
            (mln-lit C1 B positive)
            (mln-in-clause A C1 negative)
            (mln-in-clause B C1 positive)
            (mln-sat-count C1 1)
            (mln-site A (# 10))
            (mln-site B (# 1))
            (mln-flip false true)
            (mln-flip true false)
            (mln-lit-sat positive false 0)
            (mln-lit-sat positive true 1)
            (mln-lit-sat negative false 1)
            (mln-lit-sat negative true 0)
            (mln-score-delta C1 0 0 0)
            (mln-score-delta C1 0 1 1500)
            (mln-score-delta C1 0 2 1500)
            (mln-score-delta C1 1 0 -1500)
            (mln-score-delta C1 1 1 0)
            (mln-score-delta C1 1 2 0)
            (mln-score-delta C1 2 0 -1500)
            (mln-score-delta C1 2 1 0)
            (mln-score-delta C1 2 2 0)
            (mln-force-accept A)
            (sweep mln
              (e cpq)
              (src (, (mln-site $x)))
              (sink (O (+ (was-sampled mln (mln-site $x))))))
            (exec 0
              (, (was-sampled mln (mln-site $x)))
              (O (- (was-sampled mln (mln-site $x)))
                 (+ (mln-propose $x))))
            (exec 1
              (, (mln-propose $x)
                 (mln-force-accept $x)
                 (mln-val $x $old)
                 (mln-flip $old $new))
              (O (- (mln-propose $x))
                 (- (mln-val $x $old))
                 (+ (mln-val $x $new))
                 (+ (mln-accepted $x $old $new))
                 (+ (mln-update-caches $x $old $new))))
            (exec 2
              (, (mln-update-caches $x $old $new)
                 (mln-in-clause $x $c $polarity)
                 (mln-lit $c $v $lit_polarity)
                 (mln-val $v $b)
                 (mln-lit-sat $lit_polarity $b $sat))
              (O (sum (mln-new-sat-count $c $total) $total $sat)))
            (exec 3
              (, (mln-update-caches $x $old $new)
                 (mln-in-clause $x $c $polarity)
                 (mln-sat-count $c $old_count)
                 (mln-new-sat-count $c $new_count)
                 (mln-score-delta $c $old_count $new_count $delta))
              (O (- (mln-sat-count $c $old_count))
                 (+ (mln-sat-count $c $new_count))
                 (+ (mln-local-delta $x $c $old_count $new_count $delta))))
            (exec 4
              (, (mln-new-sat-count $c $new_count))
              (O (- (mln-new-sat-count $c $new_count))))
            (exec 5
              (, (mln-update-caches $x $old $new))
              (O (- (mln-update-caches $x $old $new))))
            ",
        )
        .unwrap();

    assert_eq!(space.sweep(), "sweep-config");

    let stats = space.cycle_scheduler(1, 32, 1).unwrap();

    assert_eq!(stats.cycles, 1);
    assert_eq!(stats.sweep_steps, 1);
    assert_eq!(stats.sweep_touched, 1);
    assert!(stats.sweep_new);
    assert!(stats.metta_steps > 0);

    let vals = dump(
        &space,
        expr!(space, "[3] mln-val $ $"),
        expr!(space, "[3] mln-val _1 _2"),
    );
    assert!(vals.contains("(mln-val A true)"), "values:\n{vals}");
    assert!(vals.contains("(mln-val B false)"), "values:\n{vals}");
    assert!(!vals.contains("(mln-val A false)"), "values:\n{vals}");

    let sat_counts = dump(
        &space,
        expr!(space, "[3] mln-sat-count $ $"),
        expr!(space, "[3] mln-sat-count _1 _2"),
    );
    assert!(
        sat_counts.contains("(mln-sat-count C1 0)"),
        "sat counts:\n{sat_counts}"
    );
    assert!(
        !sat_counts.contains("(mln-sat-count C1 1)"),
        "sat counts:\n{sat_counts}"
    );

    let all = dump(&space, expr!(space, "$"), expr!(space, "_1"));
    assert!(
        !all.contains("(was-sampled mln (mln-site A))"),
        "sweep event should be consumed by the bridge:\n{all}"
    );
    assert!(
        !all.contains("(mln-propose A)"),
        "proposal should be consumed by MLN rules:\n{all}"
    );
}
