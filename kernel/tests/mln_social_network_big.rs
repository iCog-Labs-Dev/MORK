use mork::expr;
use mork::space::Space;
use std::time::{Duration, Instant};

const SMALL_SOCIAL_MLN: &str = include_str!("../resources/mln_social_network.mm2");
const BIG_SOCIAL_MLN: &str = include_str!("../resources/mln_social_network_big.mm2");

fn dump(space: &Space, pattern: mork_expr::Expr, template: mork_expr::Expr) -> String {
    let mut out = Vec::new();
    space.dump_sexpr(pattern, template, &mut out);
    String::from_utf8(out).expect("dump output should be valid UTF-8")
}

fn match_count(space: &Space, pattern: mork_expr::Expr) -> usize {
    dump(space, pattern, expr!(space, "_1")).lines().count()
}

#[test]
fn big_grounded_social_network_loads_and_runs() {
    let mut space = Space::new();
    let loaded = space.add_all_sexpr(BIG_SOCIAL_MLN.as_bytes()).unwrap();
    assert_eq!(loaded, 2_818);
    assert_eq!(space.sweep(), "sweep-config");

    assert_eq!(match_count(&space, expr!(space, "[2] mln-person $")), 50);
    assert_eq!(match_count(&space, expr!(space, "[2] mln-var $")), 250);
    assert_eq!(match_count(&space, expr!(space, "[2] mln-site $")), 100);
    assert_eq!(
        match_count(&space, expr!(space, "[3] mln-evidence $ $")),
        150
    );
    assert_eq!(match_count(&space, expr!(space, "[3] mln-clause $ $")), 300);

    let stats = space.cycle_scheduler(1, 32, 16).unwrap();
    assert_eq!(stats.cycles, 16);
    assert_eq!(stats.sweep_steps, 16);
    assert!(stats.metta_steps > 16);

    let samples = dump(
        &space,
        expr!(space, "[7] mln-sample $ $ $ $ $ $"),
        expr!(space, "[7] mln-sample _1 _2 _3 _4 _5 _6"),
    );
    assert_eq!(samples.lines().count(), 16);
    assert!(!samples.contains("(Friends "));

    let all = dump(&space, expr!(space, "$"), expr!(space, "_1"));
    assert!(all.contains("(mln-val (Friends P000 P001) true)"));
    assert!(all.contains("(mln-val (Friends P049 P018) true)"));
    assert!(all.contains("(mln-rng 987654321 16)"));
}

struct Measurement {
    loaded: usize,
    load: Duration,
    register: Duration,
    run: Duration,
    metta_steps: usize,
}

fn measure(source: &str, attempts: usize) -> Measurement {
    let mut space = Space::new();

    let start = Instant::now();
    let loaded = space.add_all_sexpr(source.as_bytes()).unwrap();
    let load = start.elapsed();

    let start = Instant::now();
    assert_eq!(space.sweep(), "sweep-config");
    let register = start.elapsed();

    let start = Instant::now();
    let stats = space.cycle_scheduler(1, 32, attempts).unwrap();
    let run = start.elapsed();
    assert_eq!(stats.cycles, attempts);
    assert_eq!(stats.sweep_steps, attempts);

    Measurement {
        loaded,
        load,
        register,
        run,
        metta_steps: stats.metta_steps,
    }
}

#[test]
#[ignore = "performance benchmark; run with --ignored --nocapture"]
fn compare_small_and_big_grounded_social_networks() {
    for attempts in [16, 64, 256] {
        for (name, source) in [("small", SMALL_SOCIAL_MLN), ("big", BIG_SOCIAL_MLN)] {
            let measured = measure(source, attempts);
            println!(
                "MLN_BENCH model={name} bytes={} atoms={} attempts={attempts} \
                 load_ms={:.3} register_ms={:.3} run_ms={:.3} us_per_attempt={:.3} \
                 metta_steps={}",
                source.len(),
                measured.loaded,
                measured.load.as_secs_f64() * 1_000.0,
                measured.register.as_secs_f64() * 1_000.0,
                measured.run.as_secs_f64() * 1_000.0,
                measured.run.as_secs_f64() * 1_000_000.0 / attempts as f64,
                measured.metta_steps,
            );
        }
    }
}
