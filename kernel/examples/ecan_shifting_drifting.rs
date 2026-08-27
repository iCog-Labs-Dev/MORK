use mork::scheduler::{CycleScheduler, SchedulePolicy};
use mork::space::Space;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use weighted_atom_sweep::{AtomCandidate, EcanAfTopK, ProcessId, TraversalEngine};

const FIXTURE: &str = include_str!("../resources/ecan-shifting-drifting.mm2");
const STIMULATION: &str = include_str!("../resources/ecan-stimulation.mm2");
const DIFFUSION: &str = include_str!("../resources/ecan-diffusion.mm2");
const RENT: &str = include_str!("../resources/ecan-rent.mm2");
const SETTLING_ROUNDS: usize = 5;

#[derive(Clone)]
struct InputWord {
    phase: String,
    atom: String,
    stimulus: String,
}

fn fields(line: &str, functor: &str) -> Option<Vec<String>> {
    let body = line.strip_prefix('(')?.strip_suffix(')')?;
    let values: Vec<_> = body.split_whitespace().map(str::to_string).collect();
    (values.first().is_some_and(|value| value == functor)).then_some(values)
}

fn input_words() -> Vec<InputWord> {
    FIXTURE.lines().filter_map(|line| {
        let values = fields(line.trim(), "InputWord")?;
        Some(InputWord {
            phase: values[1].trim_end_matches("-phase").to_string(),
            atom: values[3].clone(),
            stimulus: values[4].clone(),
        })
    }).collect()
}

fn dump(space: &Space) -> String {
    let mut output = Vec::new();
    space.dump_all_sexpr(&mut output).unwrap();
    String::from_utf8(output).unwrap()
}

fn top_k(space: &Space) -> Vec<(String, f64)> {
    let state = dump(space);
    let mut atoms: Vec<_> = state.lines().filter_map(|line| {
        let values = fields(line, "STI")?;
        Some((values[1].clone(), values[2].parse::<f64>().ok()?))
    }).collect();
    atoms.sort_by(|left, right| {
        right.1.total_cmp(&left.1)
            .then(left.0.len().cmp(&right.0.len()))
            .then(left.0.cmp(&right.0))
    });
    atoms.truncate(12);
    atoms
}

fn queue_top_candidate(space: &mut Space, process: &str) {
    let engine = EcanAfTopK::new();
    let path = engine.next_atom(&space.btm).expect("fixture must have an AF candidate");
    let process_id = ProcessId(process.to_string());
    let candidate = AtomCandidate {
        process_id: process_id.clone(),
        path,
        snapshot_version: space.snapshot_version,
    };
    space.was.candidate_buffers.entry(process_id).or_default().push_back(candidate);
}

fn run_background(
    space: &mut Space,
    scheduler: &mut CycleScheduler,
    process: &str,
    rules: &str,
) {
    // The calculation rule is persistent. Its generic application continuation is
    // re-submitted because current MM2 pure resolves one tuple result per invocation.
    space.add_all_sexpr(rules.as_bytes()).unwrap();
    queue_top_candidate(space, process);
    assert_eq!(scheduler.run_background_round(space), 1);
    let expected_apply_steps = if process == "ecan_af_rent" { 2 } else { 1 };
    assert_eq!(space.metta_calculus(10), expected_apply_steps);
}

fn record(
    writer: &mut BufWriter<File>,
    space: &Space,
    round: usize,
    phase: &str,
    input_atom: &str,
    stimulated: &HashSet<String>,
) {
    let state = dump(space);
    let categories: HashMap<_, _> = state.lines().filter_map(|line| {
        let values = fields(line, "Category")?;
        Some((values[1].clone(), values[2].clone()))
    }).collect();
    let sti: HashMap<_, _> = state.lines().filter_map(|line| {
        let values = fields(line, "STI")?;
        Some((values[1].clone(), values[2].parse::<f64>().ok()?))
    }).collect();
    let lti: HashMap<_, _> = state.lines().filter_map(|line| {
        let values = fields(line, "LTI")?;
        Some((values[1].clone(), values[2].parse::<f64>().ok()?))
    }).collect();
    let bank_sti = state.lines().find_map(|line| fields(line, "BankSTI"))
        .unwrap()[1].parse::<f64>().unwrap();
    let bank_lti = state.lines().find_map(|line| fields(line, "BankLTI"))
        .unwrap()[1].parse::<f64>().unwrap();
    let af = top_k(space);
    let ranks: HashMap<_, _> = af.iter().enumerate()
        .map(|(rank, (atom, _))| (atom.clone(), rank + 1)).collect();
    let diffused: HashSet<_> = state.lines().filter_map(|line| {
        let values = fields(line, "Diffused")?;
        Some(values[3].clone())
    }).collect();
    let mut atoms: Vec<_> = categories.keys().cloned().collect();
    atoms.sort();
    let total_sti: f64 = atoms.iter().map(|atom| sti[atom]).sum();
    let total_lti: f64 = atoms.iter().map(|atom| lti[atom]).sum();
    for atom in atoms {
        let rank = ranks.get(&atom).copied();
        writeln!(writer,
            "{round},{},{phase},{input_atom},{atom},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            space.snapshot_version,
            categories[&atom], sti[&atom], lti[&atom],
            u8::from(rank.is_some()), rank.map_or(String::new(), |r| r.to_string()),
            u8::from(stimulated.contains(&atom)), u8::from(diffused.contains(&atom)),
            bank_sti, bank_lti, total_sti, total_lti,
            total_sti + bank_sti, total_lti + bank_lti,
        ).unwrap();
    }
}

fn main() {
    let output = std::env::args_os().nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("ecan-shifting-drifting.csv"));
    let mut writer = BufWriter::new(File::create(&output).unwrap());
    writeln!(writer, "round,snapshot_version,phase,input_atom,atom,category,sti,lti,in_af,af_rank,directly_stimulated,entered_via_diffusion,bank_sti,bank_lti,total_atom_sti,total_atom_lti,total_sti_plus_bank,total_lti_plus_bank").unwrap();

    let mut space = Space::new();
    space.add_all_sexpr(FIXTURE.as_bytes()).unwrap();
    let mut scheduler = CycleScheduler::new(SchedulePolicy::Fair {
        foreground_credits: 0,
        background_credits_per_process: 1,
    });
    scheduler.configure_process(ProcessId("ecan_af_diffusion".into()), 1);
    scheduler.configure_process(ProcessId("ecan_af_rent".into()), 1);
    let mut stimulated = HashSet::new();
    let mut round = 0;
    record(&mut writer, &space, round, "initial", "", &stimulated);

    for input in input_words() {
        round += 1;
        stimulated.insert(input.atom.clone());
        let request = format!("(Stimulate input-{round} {} {})", input.atom, input.stimulus);
        space.add_all_sexpr(request.as_bytes()).unwrap();
        space.add_all_sexpr(STIMULATION.as_bytes()).unwrap();
        assert_eq!(space.metta_calculus(10), 2);
        run_background(&mut space, &mut scheduler, "ecan_af_diffusion", DIFFUSION);
        run_background(&mut space, &mut scheduler, "ecan_af_rent", RENT);
        record(&mut writer, &space, round, &input.phase, &input.atom, &stimulated);
    }

    for _ in 0..SETTLING_ROUNDS {
        round += 1;
        run_background(&mut space, &mut scheduler, "ecan_af_diffusion", DIFFUSION);
        run_background(&mut space, &mut scheduler, "ecan_af_rent", RENT);
        record(&mut writer, &space, round, "settling", "", &stimulated);
    }
    writer.flush().unwrap();
    println!("{}", output.display());
}
