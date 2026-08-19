use std::collections::HashMap;
use log::debug;
use mork_expr::{destruct, Expr, ExprEnv, Tag, item_byte};
use weighted_atom_sweep::ProcessId;
use pathmap::zipper::{ZipperIteration, ZipperMoving, ZipperValues, ZipperWriting};
use crate::space::Space;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct NamespaceId(pub String);

pub struct ForegroundBatch {
    pub namespace: NamespaceId,
    pub operations: Vec<Expr>,
    pub atomic: bool,
}

pub struct BackgroundBatch {
    pub process_id: ProcessId,
    pub candidate_count: usize,
}

#[derive(Clone, Debug)]
pub enum SchedulePolicy {
    Fair {
        foreground_credits: usize,
        background_credits_per_process: usize,
    },
    ForegroundPriority {
        max_foreground_quantum: usize,
    },
}

pub struct CycleScheduler {
    pub policy: SchedulePolicy,
    pub background_credits: HashMap<ProcessId, usize>,
    pub background_rules: HashMap<ProcessId, Vec<u8>>,
    pub process_order: Vec<ProcessId>,
}

impl CycleScheduler {
    pub fn new(policy: SchedulePolicy) -> Self {
        Self {
            policy,
            background_credits: HashMap::new(),
            background_rules: HashMap::new(),
            process_order: Vec::new(),
        }
    }

    pub fn configure_process(&mut self, process_id: ProcessId, credits: usize) {
        if !self.process_order.contains(&process_id) {
            self.process_order.push(process_id.clone());
        }
        self.background_credits.insert(process_id, credits);
    }

    /// Find a WAS source structurally, so symbols containing the bytes `WAS`
    /// elsewhere in a rule are not misclassified as background execution.
    fn was_process(expr: Expr) -> Option<ProcessId> {
        destruct!(expr, ("WAS" {process_id: &str} pattern), {
            return Some(ProcessId(process_id.to_string()));
        }, _err => {});

        if expr.arity().is_some() {
            let mut args = Vec::new();
            ExprEnv::new(0, expr).args(&mut args);
            for arg in args.into_iter().skip(1) {
                if let Some(process_id) = Self::was_process(arg.subsexpr()) {
                    return Some(process_id);
                }
            }
        }
        None
    }

    /// Index each background rule once under the process named by its WAS source.
    fn index_background_rules(&mut self, space: &Space) {
        self.background_rules.clear();
        const PREFIX: [u8; 6] = const {
            [item_byte(Tag::Arity(4)), item_byte(Tag::SymbolSize(4)), b'e', b'x', b'e', b'c']
        };
        let mut rz = space.btm.read_zipper_at_borrowed_path(&PREFIX[..]);
        while rz.to_next_val() {
            let mut full_path = PREFIX.to_vec();
            full_path.extend_from_slice(rz.path());
            let expr = Expr { ptr: full_path.as_mut_ptr() };
            if let Some(process_id) = Self::was_process(expr) {
                self.background_rules.entry(process_id).or_insert(full_path);
            }
        }
    }

    /// Executes one candidate event through the registered rule for this process
    pub fn execute_background_batch(&self, space: &mut Space, pid: &ProcessId) -> usize {
        let Some(rule_path) = self.background_rules.get(pid) else { return 0 };
        let path = rule_path.clone();
        let removed_weight = space.btm.remove_val_at_w(&path[..], true);
        let Some(rule_weight) = removed_weight else { return 0 };

        let mut rule = path.clone();
        let expr = Expr { ptr: rule.as_mut_ptr() };
        if let Err(error) = space.interpret(expr) {
            debug!(target: "scheduler", "failed to interpret background rule: {}", error);
        }
        space.btm.write_zipper_at_path(&path[..]).set_val_w(rule_weight);
        1
    }

    /// Execute one deterministic background round in configured process order.
    pub fn run_background_round(&mut self, space: &mut Space) -> usize {
        self.index_background_rules(space);
        space.was.buffer_candidates();

        let process_order = if self.process_order.is_empty() {
            let mut discovered: Vec<_> = self.background_rules.keys().cloned().collect();
            discovered.sort_by(|a, b| a.0.cmp(&b.0));
            discovered
        } else {
            self.process_order.clone()
        };

        let default_credits = match self.policy {
            SchedulePolicy::Fair { background_credits_per_process, .. } => background_credits_per_process,
            SchedulePolicy::ForegroundPriority { .. } => 1,
        };
        let mut work_done = 0;
        for process_id in process_order {
            if !self.background_rules.contains_key(&process_id) {
                continue;
            }
            let credits = self.background_credits.get(&process_id).copied().unwrap_or(default_credits);
            for _ in 0..credits {
                if !space.was.select_existing_candidate(&process_id, &space.btm) {
                    break;
                }
                work_done += self.execute_background_batch(space, &process_id);
            }
        }

        if work_done > 0 {
            space.snapshot_version += 1;
            space.was.publish_snapshot(space.btm.clone(), space.snapshot_version);
        }
        work_done
    }

    /// Run one scheduler iteration/cycle, executing foreground and background steps according to policy
    pub fn schedule_step(&mut self, space: &mut Space) -> usize {
        let mut work_done = 0;

        match &self.policy {
            SchedulePolicy::Fair { foreground_credits, .. } => {
                // 1. Run foreground step(s)
                let fg_done = space.metta_calculus(*foreground_credits);
                work_done += fg_done;
                work_done += self.run_background_round(space);
            }
            SchedulePolicy::ForegroundPriority { max_foreground_quantum } => {
                // Prioritize foreground calculations, only run background if foreground has finished
                let fg_done = space.metta_calculus(*max_foreground_quantum);
                work_done += fg_done;

                if fg_done == 0 {
                    // Only execute background work if no foreground items were processed
                    work_done += self.run_background_round(space);
                }
            }
        }

        work_done
    }
}
