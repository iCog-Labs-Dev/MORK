use mork_expr::{Tag, byte_item};
use pathmap::PathMap;
use pathmap::zipper::{ZipperIteration, ZipperMoving};
use std::sync::Mutex;

/// One valid semantic STI fact extracted from an immutable MORK snapshot.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct StiCandidate {
    /// Complete encoded `(STI atom value)` path, never a trie prefix.
    pub path: Vec<u8>,
    /// Complete encoded atom expression used for stable identity and tie ordering.
    pub atom_key: Vec<u8>,
    pub sti: f64,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct StiIndex {
    pub candidates: Vec<StiCandidate>,
    pub malformed_sti_facts: u64,
}

/// Lazily built snapshot-local STI index shared by ECAN traversal strategies.
#[derive(Default)]
pub(crate) struct SnapshotStiIndex {
    index: Mutex<Option<StiIndex>>,
    rebuilds: Mutex<u64>,
}

impl SnapshotStiIndex {
    pub fn snapshot_changed(&self) {
        *self.index.lock().unwrap_or_else(|p| p.into_inner()) = None;
    }

    pub fn get_or_build(&self, map: &PathMap<u64>) -> StiIndex {
        let mut cached = self.index.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(index) = cached.as_ref() {
            return index.clone();
        }

        let index = StiIndex::scan(map);
        *self.rebuilds.lock().unwrap_or_else(|p| p.into_inner()) += 1;
        *cached = Some(index.clone());
        index
    }

    #[cfg(test)]
    fn rebuild_count(&self) -> u64 {
        *self.rebuilds.lock().unwrap_or_else(|p| p.into_inner())
    }
}

impl StiIndex {
    fn scan(map: &PathMap<u64>) -> Self {
        let mut index = Self::default();
        let mut rz = map.read_zipper();
        while rz.to_next_val() {
            match parse_sti_fact(rz.path()) {
                StiFact::Valid(candidate) => index.candidates.push(candidate),
                StiFact::Malformed => index.malformed_sti_facts += 1,
                StiFact::Unrelated => {}
            }
        }
        index.candidates.sort_by(|left, right| {
            left.atom_key
                .cmp(&right.atom_key)
                .then(left.path.cmp(&right.path))
        });
        index
    }
}

pub(crate) fn read_max_af_size(map: &PathMap<u64>) -> Result<usize, String> {
    let mut found = None;
    let mut rz = map.read_zipper();
    while rz.to_next_val() {
        let path = rz.path();
        let Some(Tag::Arity(3)) = path.first().copied().map(byte_item) else {
            continue;
        };
        let Some((functor, offset)) = symbol_at(path, 1) else {
            continue;
        };
        if functor != b"ECANParam" {
            continue;
        }
        let Some((name, offset)) = symbol_at(path, offset) else {
            continue;
        };
        if name != b"MAX_AF_SIZE" {
            continue;
        }
        let Some((value, end)) = symbol_at(path, offset) else {
            return Err("MAX_AF_SIZE must be a numeric symbol".to_string());
        };
        if end != path.len() {
            return Err("MAX_AF_SIZE fact has trailing data".to_string());
        }
        let text =
            std::str::from_utf8(value).map_err(|_| "MAX_AF_SIZE is not UTF-8".to_string())?;
        let parsed = text
            .parse::<f64>()
            .map_err(|_| "MAX_AF_SIZE is not numeric".to_string())?;
        if !parsed.is_finite()
            || parsed < 0.0
            || parsed.fract() != 0.0
            || parsed > usize::MAX as f64
        {
            return Err("MAX_AF_SIZE must be a finite nonnegative integer".to_string());
        }
        if found.replace(parsed as usize).is_some() {
            return Err("multiple MAX_AF_SIZE facts found".to_string());
        }
    }
    found.ok_or_else(|| "MAX_AF_SIZE fact not found".to_string())
}

enum StiFact {
    Valid(StiCandidate),
    Malformed,
    Unrelated,
}

fn parse_sti_fact(path: &[u8]) -> StiFact {
    if !has_sti_functor(path) {
        return StiFact::Unrelated;
    }

    let Some(Tag::Arity(3)) = path.first().copied().map(byte_item) else {
        return StiFact::Malformed;
    };
    let Some((functor, mut offset)) = symbol_at(path, 1) else {
        return StiFact::Malformed;
    };
    if functor != b"STI" {
        return StiFact::Unrelated;
    }

    let atom_start = offset;
    let Some(atom_len) = expression_len(&path[atom_start..]) else {
        return StiFact::Malformed;
    };
    offset += atom_len;

    let Some((number, end)) = symbol_at(path, offset) else {
        return StiFact::Malformed;
    };
    if end != path.len() {
        return StiFact::Malformed;
    }
    let Ok(text) = std::str::from_utf8(number) else {
        return StiFact::Malformed;
    };
    let Ok(sti) = text.parse::<f64>() else {
        return StiFact::Malformed;
    };
    if !sti.is_finite() || sti < 0.0 {
        return StiFact::Malformed;
    }

    StiFact::Valid(StiCandidate {
        path: path.to_vec(),
        atom_key: path[atom_start..atom_start + atom_len].to_vec(),
        sti,
    })
}

fn has_sti_functor(path: &[u8]) -> bool {
    matches!(path.first().copied().map(byte_item), Some(Tag::Arity(_)))
        && symbol_at(path, 1).is_some_and(|(symbol, _)| symbol == b"STI")
}

fn symbol_at(path: &[u8], offset: usize) -> Option<(&[u8], usize)> {
    let Tag::SymbolSize(size) = byte_item(*path.get(offset)?) else {
        return None;
    };
    let start = offset + 1;
    let end = start.checked_add(size as usize)?;
    Some((path.get(start..end)?, end))
}

fn expression_len(path: &[u8]) -> Option<usize> {
    fn end_offset(path: &[u8], offset: usize) -> Option<usize> {
        match byte_item(*path.get(offset)?) {
            Tag::NewVar | Tag::VarRef(_) => Some(offset + 1),
            Tag::SymbolSize(size) => (offset + 1)
                .checked_add(size as usize)
                .filter(|end| *end <= path.len()),
            Tag::Arity(arity) => {
                let mut end = offset + 1;
                for _ in 0..arity {
                    end = end_offset(path, end)?;
                }
                Some(end)
            }
        }
    }

    end_offset(path, 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mork_expr::{Tag, item_byte};
    use pathmap::zipper::ZipperValues;

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

    fn fact(functor: &[u8], atom: Vec<u8>, value: &[u8]) -> Vec<u8> {
        expression(&[symbol(functor), atom, symbol(value)])
    }

    fn map_with(paths: &[Vec<u8>]) -> PathMap<u64> {
        let mut map = PathMap::new();
        for path in paths {
            map.insert(path, 1);
        }
        map
    }

    fn all_paths(map: &PathMap<u64>) -> Vec<Vec<u8>> {
        let mut paths = Vec::new();
        let mut rz = map.read_zipper();
        while rz.to_next_val() {
            paths.push(rz.path().to_vec());
        }
        paths
    }

    #[test]
    fn extracts_only_valid_exact_sti_facts_and_complete_paths() {
        let spider = fact(b"STI", symbol(b"spider"), b"12.5");
        let nested_atom = expression(&[symbol(b"Concept"), symbol(b"ant")]);
        let ant = fact(b"STI", nested_atom.clone(), b"0.0");
        let unrelated = fact(b"LTI", symbol(b"spider"), b"99.0");
        let map = map_with(&[spider.clone(), ant.clone(), unrelated]);

        let index = StiIndex::scan(&map);

        assert_eq!(index.malformed_sti_facts, 0);
        assert_eq!(index.candidates.len(), 2);
        assert!(
            index
                .candidates
                .iter()
                .any(|c| c.path == spider && c.sti == 12.5)
        );
        assert!(
            index
                .candidates
                .iter()
                .any(|c| c.path == ant && c.atom_key == nested_atom)
        );
        assert!(
            index
                .candidates
                .iter()
                .all(|c| map.read_zipper_at_path(&c.path).val().is_some())
        );
    }

    #[test]
    fn rejects_malformed_negative_nan_and_infinite_sti() {
        let paths = [
            fact(b"STI", symbol(b"negative"), b"-1"),
            fact(b"STI", symbol(b"nan"), b"NaN"),
            fact(b"STI", symbol(b"infinite"), b"inf"),
            fact(b"STI", symbol(b"text"), b"many"),
            expression(&[symbol(b"STI"), symbol(b"missing-value")]),
            expression(&[
                symbol(b"STI"),
                symbol(b"too"),
                symbol(b"1.0"),
                symbol(b"many"),
            ]),
        ];
        let map = map_with(&paths);

        let index = StiIndex::scan(&map);

        assert!(index.candidates.is_empty());
        assert_eq!(index.malformed_sti_facts, paths.len() as u64);
    }

    #[test]
    fn scan_is_deterministic_and_does_not_mutate_map() {
        let paths = [
            fact(b"STI", symbol(b"zeta"), b"1.0"),
            fact(b"STI", symbol(b"alpha"), b"2.0"),
        ];
        let map = map_with(&paths);
        let before = all_paths(&map);

        let first = StiIndex::scan(&map);
        let second = StiIndex::scan(&map);

        assert_eq!(first, second);
        // Ordering is by encoded atom identity, including the symbol-size tag.
        assert_eq!(first.candidates[0].atom_key, symbol(b"zeta"));
        assert_eq!(first.candidates[1].atom_key, symbol(b"alpha"));
        assert_eq!(all_paths(&map), before);
    }

    #[test]
    fn snapshot_cache_rebuilds_lazily_only_after_change() {
        let first_map = map_with(&[fact(b"STI", symbol(b"first"), b"1.0")]);
        let second_map = map_with(&[fact(b"STI", symbol(b"second"), b"2.0")]);
        let cache = SnapshotStiIndex::default();

        assert_eq!(cache.rebuild_count(), 0);
        assert_eq!(cache.get_or_build(&first_map).candidates[0].sti, 1.0);
        assert_eq!(cache.rebuild_count(), 1);
        assert_eq!(cache.get_or_build(&second_map).candidates[0].sti, 1.0);
        assert_eq!(cache.rebuild_count(), 1);

        cache.snapshot_changed();
        assert_eq!(cache.rebuild_count(), 1);
        assert_eq!(cache.get_or_build(&second_map).candidates[0].sti, 2.0);
        assert_eq!(cache.rebuild_count(), 2);
    }
}
