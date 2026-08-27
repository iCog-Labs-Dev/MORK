use crate::ecan_sti::{SnapshotStiIndex, StiCandidate, read_max_af_size};
use crate::sweep::AtomPosition;
use crate::traversal::{TraversalEngine, TraversalError};
use pathmap::PathMap;
use std::collections::VecDeque;
use std::sync::Mutex;

/// Exact deterministic attentional-focus traversal for one immutable snapshot.
#[derive(Default)]
pub struct EcanAfTopK {
    index: SnapshotStiIndex,
    remaining: Mutex<Option<VecDeque<StiCandidate>>>,
}

impl EcanAfTopK {
    pub fn new() -> Self {
        Self::default()
    }

    fn rank_snapshot(&self, map: &PathMap<u64>) -> Result<VecDeque<StiCandidate>, TraversalError> {
        let max_af_size = read_max_af_size(map).map_err(|message| TraversalError { message })?;
        let mut candidates = self.index.get_or_build(map).candidates;
        candidates.sort_by(|left, right| {
            right
                .sti
                .total_cmp(&left.sti)
                .then(left.atom_key.cmp(&right.atom_key))
                .then(left.path.cmp(&right.path))
        });
        candidates.truncate(max_af_size);
        Ok(candidates.into())
    }
}

impl TraversalEngine for EcanAfTopK {
    fn name(&self) -> &str {
        "ecan_af_topk"
    }

    fn snapshot_changed(&self) {
        *self.remaining.lock().unwrap_or_else(|p| p.into_inner()) = None;
        self.index.snapshot_changed();
    }

    fn next_atom(&self, map: &PathMap<u64>) -> Result<AtomPosition, TraversalError> {
        let mut remaining = self.remaining.lock().unwrap_or_else(|p| p.into_inner());
        if remaining.is_none() {
            *remaining = Some(self.rank_snapshot(map)?);
        }
        remaining
            .as_mut()
            .and_then(VecDeque::pop_front)
            .map(|candidate| candidate.path)
            .ok_or_else(|| TraversalError {
                message: "attentional focus exhausted for current snapshot".to_string(),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mork_expr::{Tag, item_byte};

    fn symbol(value: &[u8]) -> Vec<u8> {
        let mut encoded = vec![item_byte(Tag::SymbolSize(value.len() as u8))];
        encoded.extend_from_slice(value);
        encoded
    }

    fn expression(items: &[Vec<u8>]) -> Vec<u8> {
        let mut encoded = vec![item_byte(Tag::Arity(items.len() as u8))];
        for item in items {
            encoded.extend_from_slice(item);
        }
        encoded
    }

    fn sti(atom: &[u8], value: &[u8]) -> Vec<u8> {
        expression(&[symbol(b"STI"), symbol(atom), symbol(value)])
    }

    fn snapshot(max_af_size: usize, facts: &[(&[u8], &[u8])]) -> PathMap<u64> {
        let mut map = PathMap::new();
        let param = expression(&[
            symbol(b"ECANParam"),
            symbol(b"MAX_AF_SIZE"),
            symbol(max_af_size.to_string().as_bytes()),
        ]);
        map.insert(&param, 1);
        for (atom, value) in facts {
            map.insert(&sti(atom, value), 1);
        }
        map
    }

    fn emitted(engine: &EcanAfTopK, map: &PathMap<u64>) -> Vec<Vec<u8>> {
        let mut paths = Vec::new();
        while let Ok(path) = engine.next_atom(map) {
            paths.push(path);
        }
        paths
    }

    #[test]
    fn emits_exact_top_k_once_and_never_emits_outside_af() {
        let map = snapshot(2, &[(b"low", b"1"), (b"highest", b"9"), (b"middle", b"5")]);
        let engine = EcanAfTopK::new();

        assert_eq!(
            emitted(&engine, &map),
            vec![sti(b"highest", b"9"), sti(b"middle", b"5")]
        );
        assert!(engine.next_atom(&map).is_err());
        assert!(engine.next_atom(&map).is_err());
    }

    #[test]
    fn boundary_ties_use_encoded_atom_identity() {
        let map = snapshot(2, &[(b"bbbb", b"7"), (b"a", b"7"), (b"cc", b"7")]);
        let engine = EcanAfTopK::new();

        // Encoded symbols sort by their size tag before payload bytes.
        assert_eq!(
            emitted(&engine, &map),
            vec![sti(b"a", b"7"), sti(b"cc", b"7")]
        );
    }

    #[test]
    fn snapshot_change_resets_exhausted_state() {
        let first = snapshot(1, &[(b"first", b"1")]);
        let second = snapshot(1, &[(b"second", b"2")]);
        let engine = EcanAfTopK::new();

        assert_eq!(emitted(&engine, &first), vec![sti(b"first", b"1")]);
        engine.snapshot_changed();
        assert_eq!(emitted(&engine, &second), vec![sti(b"second", b"2")]);
    }

    #[test]
    fn process_instances_have_independent_iteration_state() {
        let map = snapshot(2, &[(b"first", b"2"), (b"second", b"1")]);
        let diffusion = EcanAfTopK::new();
        let rent = EcanAfTopK::new();

        assert_eq!(diffusion.next_atom(&map).unwrap(), sti(b"first", b"2"));
        assert_eq!(rent.next_atom(&map).unwrap(), sti(b"first", b"2"));
        assert_eq!(diffusion.next_atom(&map).unwrap(), sti(b"second", b"1"));
        assert_eq!(rent.next_atom(&map).unwrap(), sti(b"second", b"1"));
    }
}
