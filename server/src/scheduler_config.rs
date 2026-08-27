use std::collections::HashSet;
use std::path::Path;

use serde::Deserialize;
use weighted_atom_sweep::build_strategy;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchedulerConfig {
    #[serde(default = "default_foreground_transactions", alias = "foreground_cycles")]
    pub foreground_transactions_per_round: usize,
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,
    #[serde(default)]
    pub processes: Vec<ProcessConfig>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessConfig {
    pub id: String,
    pub engine: String,
    #[serde(default = "default_process_cycles")]
    pub cycles: usize,
}

fn default_foreground_transactions() -> usize { 10 }
fn default_process_cycles() -> usize { 1 }
fn default_poll_interval_ms() -> u64 { 10 }

impl SchedulerConfig {
    pub fn from_path(path: &Path) -> Result<Self, String> {
        let source = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
        Self::from_str(&source)
    }

    pub fn from_str(source: &str) -> Result<Self, String> {
        let config: Self = toml::from_str(source).map_err(|e| e.to_string())?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), String> {
        if self.foreground_transactions_per_round == 0 {
            return Err("foreground_transactions_per_round must be greater than zero".into());
        }
        if self.poll_interval_ms == 0 {
            return Err("poll_interval_ms must be greater than zero".into());
        }

        let mut ids = HashSet::new();
        for process in &self.processes {
            if process.id.is_empty() {
                return Err("process id must not be empty".into());
            }
            if !ids.insert(process.id.clone()) {
                return Err(format!("duplicate process id: {}", process.id));
            }
            if process.cycles == 0 {
                return Err(format!("cycles for process {} must be greater than zero", process.id));
            }
            if build_strategy(&process.engine).is_none() {
                return Err(format!("unknown engine for process {}: {}", process.id, process.engine));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_valid_config() {
        let config = SchedulerConfig::from_str(r#"
            foreground_transactions_per_round = 7
            poll_interval_ms = 5

            [[processes]]
            id = "walker"
            engine = "random_walk"
            cycles = 2
        "#).unwrap();
        assert_eq!(config.foreground_transactions_per_round, 7);
        assert_eq!(config.processes[0].cycles, 2);
    }

    #[test]
    fn rejects_duplicate_process_ids() {
        let error = SchedulerConfig::from_str(r#"
            [[processes]]
            id = "same"
            engine = "random_walk"
            [[processes]]
            id = "same"
            engine = "cpq"
        "#).unwrap_err();
        assert!(error.contains("duplicate process id"));
    }

    #[test]
    fn rejects_unknown_engine() {
        let error = SchedulerConfig::from_str(r#"
            [[processes]]
            id = "bad"
            engine = "missing"
        "#).unwrap_err();
        assert!(error.contains("unknown engine"));
    }

    #[test]
    fn ecan_example_orders_diffusion_before_rent() {
        let config = SchedulerConfig::from_str(include_str!(
            "../examples/ecan-shifting-drifting-scheduler.toml"
        )).unwrap();

        assert_eq!(config.foreground_transactions_per_round, 1);
        assert_eq!(config.processes.len(), 2);
        assert_eq!(config.processes[0].id, "ecan_af_diffusion");
        assert_eq!(config.processes[0].engine, "ecan_af_topk");
        assert_eq!(config.processes[0].cycles, 12);
        assert_eq!(config.processes[1].id, "ecan_af_rent");
        assert_eq!(config.processes[1].engine, "ecan_af_topk");
        assert_eq!(config.processes[1].cycles, 12);
    }
}
