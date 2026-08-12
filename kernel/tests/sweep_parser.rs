use mork::space::{Space, WeightPolicy};

fn dump_all(space: &Space) -> String {
    let mut out = Vec::new();
    space.dump_all_sexpr(&mut out).unwrap();
    String::from_utf8(out).expect("dump output should be valid UTF-8")
}

fn show_expr(_space: &Space, bytes: &[u8]) -> String {
    let expr = mork_expr::Expr {
        ptr: bytes.as_ptr().cast_mut()
    };
    let mut out = Vec::new();
    #[cfg(feature = "interning")]
    let table = _space.sym_table();
    expr.serialize(&mut out, |s| {
        #[cfg(feature = "interning")]
        {
            let symbol = i64::from_be_bytes(s.try_into().unwrap()).to_be_bytes();
            let text = table
                .get_bytes(symbol)
                .map(unsafe { |x| std::str::from_utf8_unchecked(x) })
                .unwrap_or_else(|| panic!("failed to look up {:?}", symbol));
            unsafe { std::mem::transmute(text) }
        }
        #[cfg(not(feature = "interning"))]
        unsafe {
            std::mem::transmute(std::str::from_utf8(s).unwrap_or_else(|_| panic!("{:?}", s)))
        }
    });
    String::from_utf8(out).unwrap_or_else(|_| {
        unsafe { expr.span().as_ref() }
            .map(mork_expr::serialize)
            .unwrap_or("<null>".to_string())
    })
}

#[test]
fn sweep_source_sink_spec_is_registered_and_config_removed_without_spawning_was() {
    let mut space = Space::new();
    space
        .add_all_sexpr(
            b"
            (seed fact)
            (sweep mln
              (e random_walk)
              (src (, (mln-site $x)))
              (sink (O (+ (was-sampled mln (mln-site $x))))))
            ",
        )
        .unwrap();

    let handle = space.sweep();

    assert_eq!(handle, "sweep-config");
    assert!(
        space.was.map.is_none(),
        "source/sink specs should stay in State A"
    );
    assert!(space.was.controllers.is_empty());

    let spec = space
        .sweep_specs
        .get("mln")
        .expect("mln sweep should be registered");
    assert_eq!(spec.name, "mln");
    assert_eq!(spec.engine_type, "random_walk");
    assert_eq!(spec.weight_policy, WeightPolicy::First);
    assert!(spec.operations.is_empty());

    let rule = spec
        .rule
        .as_ref()
        .expect("source/sink sweep should register a rule");
    let source = show_expr(&space, &rule.source);
    let sink = show_expr(&space, &rule.sink);
    assert!(source.contains("mln-site"), "source:\n{source}");
    assert!(sink.contains("O"), "sink:\n{sink}");
    assert!(sink.contains("+"), "sink:\n{sink}");
    assert!(sink.contains("was-sampled"), "sink:\n{sink}");
    assert!(sink.contains("mln-site"), "sink:\n{sink}");

    let dumped = dump_all(&space);
    assert!(dumped.contains("(seed fact)"), "dumped:\n{dumped}");
    assert!(
        !dumped.contains("(sweep mln"),
        "sweep config should be removed:\n{dumped}"
    );
}

#[test]
fn legacy_sweep_operations_still_spawn_was_and_are_registered() {
    let mut space = Space::new();
    space
        .add_all_sexpr(b"(sweep imp (e random_walk) (o decay) (o log-atom))")
        .unwrap();

    let handle = space.sweep();

    assert!(!handle.is_empty());
    assert_ne!(handle, "sweep-config");
    assert_eq!(space.was.controllers.len(), 1);
    assert!(space.was.map.is_some());

    let spec = space
        .sweep_specs
        .get("imp")
        .expect("legacy sweep should be registered");
    assert_eq!(spec.weight_policy, WeightPolicy::First);
    let op_names: Vec<&str> = spec
        .operations
        .iter()
        .map(|op| op.op_type.as_str())
        .collect();
    assert_eq!(op_names, ["decay", "log-atom"]);
    assert!(spec.rule.is_none());

    let _ = space.was.shutdown_all();
}

#[test]
fn sweep_weight_expr_clause_is_registered() {
    let mut space = Space::new();
    space
        .add_all_sexpr(
            b"
            (sweep planner
              (e cpq)
              (src (, (task $t) (priority $t $w)))
              (weight expr $w)
              (sink (O (+ (was-sampled planner (task $t))))))
            ",
        )
        .unwrap();

    assert_eq!(space.sweep(), "sweep-config");

    let spec = space
        .sweep_specs
        .get("planner")
        .expect("planner sweep should be registered");
    match &spec.weight_policy {
        WeightPolicy::Expr(bytes) => {
            assert!(!bytes.is_empty(), "weight expr bytes should be stored");
        }
        other => panic!("expected weight expr policy, got {other:?}"),
    }
}

#[test]
fn sweep_weight_product_clause_is_registered() {
    let mut space = Space::new();
    space
        .add_all_sexpr(
            b"
            (sweep planner
              (e cpq)
              (src (, (task $t) (ready $t)))
              (weight product)
              (sink (O (+ (was-sampled planner (task $t))))))
            ",
        )
        .unwrap();

    assert_eq!(space.sweep(), "sweep-config");

    let spec = space
        .sweep_specs
        .get("planner")
        .expect("planner sweep should be registered");
    assert_eq!(spec.weight_policy, WeightPolicy::Product);
}

#[test]
fn grouped_legacy_sweep_operations_are_parsed() {
    let mut space = Space::new();
    space
        .add_all_sexpr(b"(sweep grouped (e random_walk) (, (decay) (o log-atom)))")
        .unwrap();

    let handle = space.sweep();

    assert!(!handle.is_empty());
    assert_eq!(space.was.controllers.len(), 1);

    let spec = space
        .sweep_specs
        .get("grouped")
        .expect("grouped sweep should be registered");
    let op_names: Vec<&str> = spec
        .operations
        .iter()
        .map(|op| op.op_type.as_str())
        .collect();
    assert_eq!(op_names, ["decay", "log-atom"]);

    let _ = space.was.shutdown_all();
}

#[test]
fn operation_arguments_are_stored_as_full_expressions() {
    let mut space = Space::new();
    space
        .add_all_sexpr(
            b"
            (sweep event
              (e random_walk)
              (o sexpr
                (, (mln-site $x))
                (O (+ (was-sampled mln (mln-site $x))))))
            ",
        )
        .unwrap();

    let handle = space.sweep();

    assert!(!handle.is_empty());
    assert_eq!(space.was.controllers.len(), 1);

    let spec = space
        .sweep_specs
        .get("event")
        .expect("event sweep should be registered");
    assert_eq!(spec.operations.len(), 1);
    let op = &spec.operations[0];
    assert_eq!(op.op_type, "sexpr");
    assert_eq!(op.args.len(), 2);

    let source = show_expr(&space, &op.args[0]);
    let sink = show_expr(&space, &op.args[1]);
    assert!(source.contains("mln-site"), "source:\n{source}");
    assert!(sink.contains("was-sampled"), "sink:\n{sink}");
    assert!(sink.contains("mln-site"), "sink:\n{sink}");

    let _ = space.was.shutdown_all();
}

#[test]
fn incomplete_source_sink_sweep_is_not_registered_or_removed() {
    let mut space = Space::new();
    space
        .add_all_sexpr(b"(sweep half (e random_walk) (src (, (mln-site $x))))")
        .unwrap();

    let handle = space.sweep();

    assert!(handle.is_empty());
    assert!(!space.sweep_specs.contains_key("half"));
    assert!(space.was.map.is_none());

    let dumped = dump_all(&space);
    assert!(
        dumped.contains("(sweep half"),
        "incomplete source/sink sweep should remain:\n{dumped}"
    );
}

#[test]
fn invalid_engine_sweep_is_not_registered_or_removed() {
    let mut space = Space::new();
    space
        .add_all_sexpr(
            b"(sweep bad (e bogus) (src (, (mln-site $x))) (sink (O (+ (was-sampled bad $x)))))",
        )
        .unwrap();

    let handle = space.sweep();

    assert!(handle.is_empty());
    assert!(!space.sweep_specs.contains_key("bad"));
    assert!(space.was.map.is_none());

    let dumped = dump_all(&space);
    assert!(
        dumped.contains("(sweep bad"),
        "invalid sweep should remain:\n{dumped}"
    );
}

#[test]
fn invalid_weight_clause_sweep_is_not_registered_or_removed() {
    let mut space = Space::new();
    space
        .add_all_sexpr(
            b"
            (sweep bad-weight
              (e cpq)
              (src (, (mln-site $x)))
              (weight expr)
              (sink (O (+ (was-sampled bad-weight (mln-site $x))))))
            ",
        )
        .unwrap();

    let handle = space.sweep();

    assert!(handle.is_empty());
    assert!(!space.sweep_specs.contains_key("bad-weight"));
    assert!(space.was.map.is_none());

    let dumped = dump_all(&space);
    assert!(
        dumped.contains("(sweep bad-weight"),
        "invalid sweep should remain:\n{dumped}"
    );
}

#[test]
fn duplicate_weight_clause_sweep_is_not_registered_or_removed() {
    let mut space = Space::new();
    space
        .add_all_sexpr(
            b"
            (sweep duplicate-weight
              (e cpq)
              (src (, (mln-site $x)))
              (weight first)
              (weight sum)
              (sink (O (+ (was-sampled duplicate-weight (mln-site $x))))))
            ",
        )
        .unwrap();

    let handle = space.sweep();

    assert!(handle.is_empty());
    assert!(!space.sweep_specs.contains_key("duplicate-weight"));
    assert!(space.was.map.is_none());

    let dumped = dump_all(&space);
    assert!(
        dumped.contains("(sweep duplicate-weight"),
        "duplicate weight sweep should remain:\n{dumped}"
    );
}
