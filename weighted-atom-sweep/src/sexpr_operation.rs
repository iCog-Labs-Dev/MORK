use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use mork_expr::{byte_item, Expr, ExprZipper, Tag};
use pathmap::zipper::{WriteZipperTracked, Zipper, ZipperMoving, ZipperWriting};
use tracing::{debug, trace, warn};

use crate::operation::TransformOp;

/// The effect applied to a template after variable substitution.
#[derive(Clone, Debug)]
pub enum TemplateEffect {
    /// Insert the instantiated template as a trie path, setting a default value
    /// at the leaf.
    Add,

    /// Remove the instantiated template's trie path from the subtrie
    /// (with pruning of empty parent nodes).
    Remove,
}

/// An mm2 exec operation that pattern-matches against a subtrie and
/// instantiates templates with the matched variable bindings.
///
/// # Exec Model
///
/// An `SExprOperation` carries:
/// - A **pattern** — an mm2-encoded expression that may contain variables
///   (`NewVar`, `VarRef`). The pattern is walked against the trie structure
///   to find matching paths.
/// - A list of **templates** — mm2-encoded expressions paired with
///   [`TemplateEffect`]s. For each pattern match, variables are extracted
///   via mork-expr's `extract_data` and substituted into each template via
///   `substitute`. The resulting expression bytes are then applied to the
///   trie per the effect (Add or Remove).
///
/// # Path Prefixing
///
/// The write zipper passed to `apply` is already focused at the
/// traversal-returned atom path. Pattern matching and template writes
/// operate within this focused subtrie. The `atom_path` bytes are passed
/// for contextual metadata (logging, debugging).
///
/// # Thread Safety
///
/// `SExprOperation` is `Send + Sync` because all fields are owned values
/// with no interior mutability (except the `AtomicUsize` match counter).
///
/// # Example
/// ```ignore
/// use mork_expr::parse;
/// use weighted_atom_sweep::{SExprOperation, TemplateEffect};
///
/// let pattern = parse!("[3] = $ _1");
/// let template = parse!("[2] matched $");
///
/// // Match (= $x $x) in the subtrie, and for each match add (matched $x)
/// let op = SExprOperation::exec(
///     "match_and_add",
///     &pattern,
///     &[(&template[..], TemplateEffect::Add)],
/// );
/// ```
pub struct SExprOperation {
    name: String,
    /// The mm2-encoded pattern expression. May contain `NewVar`/`VarRef`.
    /// An empty pattern means "match unconditionally" — templates are applied
    /// directly without variable substitution.
    pattern: Vec<u8>,
    /// Template expressions paired with their effects.
    templates: Vec<(Vec<u8>, TemplateEffect)>,
    /// Cumulative match counter. Incremented for each successful pattern match.
    /// Observable via [`Self::match_count()`].
    match_counter: Arc<AtomicUsize>,
}

impl SExprOperation {
    /// Create an exec operation.
    ///
    /// # Arguments
    /// * `name` — Descriptive name for tracing and identification.
    /// * `pattern` — The mm2-encoded pattern bytes. May contain `NewVar`/`VarRef`
    ///   for wildcard and co-referential matching. Pass `&[]` for unconditional
    ///   template application (no pattern matching).
    /// * `templates` — Slice of `(template_bytes, effect)` pairs. For each
    ///   pattern match, each template is instantiated with the matched variable
    ///   bindings and the effect is applied.
    pub fn exec(
        name: impl Into<String>,
        pattern: &[u8],
        templates: &[(&[u8], TemplateEffect)],
    ) -> Self {
        let name = name.into();
        debug!(
            name = %name,
            pattern_len = pattern.len(),
            template_count = templates.len(),
            "creating SExprOperation::exec"
        );
        Self {
            name,
            pattern: pattern.to_vec(),
            templates: templates
                .iter()
                .map(|(t, e)| (t.to_vec(), e.clone()))
                .collect(),
            match_counter: Arc::new(AtomicUsize::new(0)),

        }
    }

    /// Returns a reference to the pattern bytes.
    pub fn pattern(&self) -> &[u8] {
        &self.pattern
    }

    /// Returns a reference to the templates.
    pub fn templates(&self) -> &[(Vec<u8>, TemplateEffect)] {
        &self.templates
    }

    /// Returns the cumulative number of pattern matches found.
    ///
    /// This counter is incremented each time `apply` finds a complete
    /// pattern match. It accumulates across multiple `apply` calls.
    pub fn match_count(&self) -> usize {
        self.match_counter.load(Ordering::Relaxed)
    }

    /// Resets the cumulative match counter to zero.
    pub fn reset_match_count(&self) {
        self.match_counter.store(0, Ordering::Relaxed);
    }

    // -----------------------------------------------------------------------
    // Template instantiation
    // -----------------------------------------------------------------------

    /// Apply all templates for a single matched path.
    ///
    /// Uses mork-expr's `extract_data` to extract variable bindings from
    /// the matched data, then `substitute` to instantiate each template.
    fn apply_templates(&self, wz: &mut WriteZipperTracked<u64>, matched_data: &[u8]) {
        if self.templates.is_empty() {
            return;
        }

        // extract_data needs Expr pointers into mutable memory
        let mut data_buf = matched_data.to_vec();
        let data_expr = Expr {
            ptr: data_buf.as_mut_ptr(),
        };

        let mut pattern_buf = self.pattern.clone();
        let pattern_expr = Expr {
            ptr: pattern_buf.as_mut_ptr(),
        };

        let bindings = match pattern_expr.extract_data(&mut ExprZipper::new(data_expr)) {
            Ok(b) => b,
            Err(e) => {
                trace!(
                    name = %self.name,
                    ?e,
                    "extract_data failed for matched path, skipping templates"
                );
                return;
            }
        };

        for (tmpl_bytes, effect) in &self.templates {
            let mut tmpl_buf = tmpl_bytes.clone();
            let tmpl_expr = Expr {
                ptr: tmpl_buf.as_mut_ptr(),
            };

            // Allocate output buffer — generous size
            let capacity = tmpl_bytes.len() + matched_data.len() * 2 + 64;
            let mut output_buf = vec![0u8; capacity];
            let out_expr = Expr {
                ptr: output_buf.as_mut_ptr(),
            };
            let mut oz = ExprZipper::new(out_expr);

            // substitute writes into oz; the return value is the span of the
            // *template* consumed, not the output.  The output lives in
            // output_buf[0..oz.loc] after the call.
            let _template_span = tmpl_expr.substitute(&bindings, &mut oz);
            let result_bytes = &output_buf[..oz.loc];

            if result_bytes.is_empty() {
                warn!(
                    name = %self.name,
                    "substitute produced empty output, skipping template"
                );
                continue;
            }

            trace!(
                name = %self.name,
                result_len = result_bytes.len(),
                ?effect,
                "instantiated template"
            );

            wz.reset();
            match effect {
                TemplateEffect::Add => {
                    wz.descend_to(result_bytes);
                    wz.set_val(1u64);
                    wz.reset();
                }
                TemplateEffect::Remove => {
                    wz.descend_to(result_bytes);
                    if wz.is_val() {
                        wz.remove_val(true);
                    }
                    wz.reset();
                }
            }
        }
    }

    /// Apply templates unconditionally (no pattern matching, no variable
    /// substitution). Used when the pattern is empty.
    fn apply_templates_direct(&self, wz: &mut WriteZipperTracked<u64>) {
        for (tmpl_bytes, effect) in &self.templates {
            if tmpl_bytes.is_empty() {
                continue;
            }
            trace!(
                name = %self.name,
                tmpl_len = tmpl_bytes.len(),
                ?effect,
                "applying template directly (no pattern)"
            );
            wz.reset();
            match effect {
                TemplateEffect::Add => {
                    wz.descend_to(tmpl_bytes);
                    wz.set_val(1u64);
                    wz.reset();
                }
                TemplateEffect::Remove => {
                    wz.descend_to(tmpl_bytes);
                    if wz.is_val() {
                        wz.remove_val(true);
                    }
                    wz.reset();
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Trie-walking pattern matcher
    //
    // Walks the trie structure matching against the pattern expression.
    // At each complete match (pattern fully consumed), collects the matched
    // trie path bytes into `matched_paths`.
    //
    // The logic mirrors MORK's `coreferential_transition`:
    //   - SymbolSize(n) + n bytes: exact match in trie
    //   - Arity(a): exact match, then recursively match a children
    //   - NewVar: wildcard, enumerate all children
    //   - VarRef(k): re-match the bytes captured at reference k
    // -----------------------------------------------------------------------

    /// Top-level: walk the pattern against the trie from the current
    /// zipper position, collecting all matched paths.
    fn walk_pattern(&self, wz: &mut WriteZipperTracked<u64>, matched_paths: &mut Vec<Vec<u8>>) {
        let mut references: Vec<usize> = Vec::new();
        self.walk_item(wz, 0, &mut references, matched_paths);
    }

    /// Match a single expression item starting at `expr_offset` against
    /// the current trie position.
    fn walk_item(
        &self,
        wz: &mut WriteZipperTracked<u64>,
        expr_offset: usize,
        references: &mut Vec<usize>,
        matched_paths: &mut Vec<Vec<u8>>,
    ) {
        let data = &self.pattern;
        if expr_offset >= data.len() {
            // Full pattern matched — record the matched trie path
            matched_paths.push(wz.path().to_vec());
            return;
        }

        let tag = byte_item(data[expr_offset]);
        match tag {
            Tag::NewVar => {
                let ref_idx = references.len();
                references.push(wz.path().len());

                let cm = wz.child_mask();
                let mut it = cm.iter();
                while let Some(b) = it.next() {
                    let child_tag = byte_item(b);
                    match child_tag {
                        Tag::SymbolSize(size) => {
                            wz.descend_to_byte(b);
                            if wz.path_exists() {
                                self.enumerate_k_paths(
                                    wz,
                                    size as usize,
                                    expr_offset + 1,
                                    references,
                                    matched_paths,
                                );
                            }
                            wz.ascend_byte();
                        }
                        Tag::Arity(_) => {
                            wz.descend_to_byte(b);
                            if wz.path_exists() {
                                self.walk_item(wz, expr_offset + 1, references, matched_paths);
                            }
                            wz.ascend_byte();
                        }
                        _ => {
                            wz.descend_to_byte(b);
                            if wz.path_exists() {
                                self.walk_item(wz, expr_offset + 1, references, matched_paths);
                            }
                            wz.ascend_byte();
                        }
                    }
                }

                references.truncate(ref_idx);
            }

            Tag::VarRef(k) => {
                if (k as usize) < references.len() {
                    let ref_path_start = references[k as usize];
                    let current_path = wz.path().to_vec();

                    if ref_path_start < current_path.len() {
                        let bound_copy = current_path[ref_path_start..].to_vec();
                        wz.descend_to(&bound_copy);
                        if wz.path_exists() {
                            self.walk_item(wz, expr_offset + 1, references, matched_paths);
                        }
                        wz.ascend(bound_copy.len());
                    }
                } else {
                    trace!(
                        name = %self.name,
                        var_ref = k,
                        num_refs = references.len(),
                        "VarRef out of bounds, skipping"
                    );
                }
            }

            Tag::SymbolSize(size) => {
                let symbol_byte = data[expr_offset];
                let symbol_data_start = expr_offset + 1;
                let symbol_data_end = symbol_data_start + size as usize;

                if symbol_data_end > data.len() {
                    return;
                }

                let symbol_bytes = &data[symbol_data_start..symbol_data_end];

                wz.descend_to_byte(symbol_byte);
                if wz.path_exists() {
                    wz.descend_to(symbol_bytes);
                    if wz.path_exists() {
                        self.walk_item(wz, symbol_data_end, references, matched_paths);
                    }
                    wz.ascend(size as usize);
                }
                wz.ascend_byte();
            }

            Tag::Arity(arity) => {
                let arity_byte = data[expr_offset];
                wz.descend_to_byte(arity_byte);
                if wz.path_exists() {
                    self.walk_arity_children(wz, expr_offset + 1, arity, references, matched_paths);
                }
                wz.ascend_byte();
            }
        }
    }

    /// Match `remaining` children of an arity node sequentially.
    fn walk_arity_children(
        &self,
        wz: &mut WriteZipperTracked<u64>,
        expr_offset: usize,
        remaining: u8,
        references: &mut Vec<usize>,
        matched_paths: &mut Vec<Vec<u8>>,
    ) {
        if remaining == 0 {
            self.walk_item(wz, expr_offset, references, matched_paths);
            return;
        }

        let data = &self.pattern;
        if expr_offset >= data.len() {
            return;
        }

        let child_size = expr_item_size(data, expr_offset);
        if child_size == 0 {
            return;
        }

        let next_child_offset = expr_offset + child_size;
        let tag = byte_item(data[expr_offset]);

        match tag {
            Tag::NewVar => {
                let ref_idx = references.len();
                references.push(wz.path().len());

                let cm = wz.child_mask();
                let mut it = cm.iter();
                while let Some(b) = it.next() {
                    let child_tag = byte_item(b);
                    match child_tag {
                        Tag::SymbolSize(size) => {
                            wz.descend_to_byte(b);
                            if wz.path_exists() {
                                self.enumerate_k_paths_then_siblings(
                                    wz,
                                    size as usize,
                                    next_child_offset,
                                    remaining - 1,
                                    references,
                                    matched_paths,
                                );
                            }
                            wz.ascend_byte();
                        }
                        Tag::Arity(_) => {
                            wz.descend_to_byte(b);
                            if wz.path_exists() {
                                self.walk_arity_children(
                                    wz,
                                    next_child_offset,
                                    remaining - 1,
                                    references,
                                    matched_paths,
                                );
                            }
                            wz.ascend_byte();
                        }
                        _ => {
                            wz.descend_to_byte(b);
                            if wz.path_exists() {
                                self.walk_arity_children(
                                    wz,
                                    next_child_offset,
                                    remaining - 1,
                                    references,
                                    matched_paths,
                                );
                            }
                            wz.ascend_byte();
                        }
                    }
                }

                references.truncate(ref_idx);
            }

            Tag::VarRef(k) => {
                if (k as usize) < references.len() {
                    let ref_path_start = references[k as usize];
                    let current_path = wz.path().to_vec();
                    if ref_path_start < current_path.len() {
                        let bound_copy = current_path[ref_path_start..].to_vec();
                        wz.descend_to(&bound_copy);
                        if wz.path_exists() {
                            self.walk_arity_children(
                                wz,
                                next_child_offset,
                                remaining - 1,
                                references,
                                matched_paths,
                            );
                        }
                        wz.ascend(bound_copy.len());
                    }
                }
            }

            Tag::SymbolSize(size) => {
                let symbol_byte = data[expr_offset];
                let symbol_data_start = expr_offset + 1;
                let symbol_data_end = symbol_data_start + size as usize;
                if symbol_data_end > data.len() {
                    return;
                }
                let symbol_bytes = &data[symbol_data_start..symbol_data_end];

                wz.descend_to_byte(symbol_byte);
                if wz.path_exists() {
                    wz.descend_to(symbol_bytes);
                    if wz.path_exists() {
                        self.walk_arity_children(
                            wz,
                            next_child_offset,
                            remaining - 1,
                            references,
                            matched_paths,
                        );
                    }
                    wz.ascend(size as usize);
                }
                wz.ascend_byte();
            }

            Tag::Arity(sub_arity) => {
                let arity_byte = data[expr_offset];
                wz.descend_to_byte(arity_byte);
                if wz.path_exists() {
                    self.walk_nested_then_siblings(
                        wz,
                        expr_offset + 1,
                        sub_arity,
                        next_child_offset,
                        remaining - 1,
                        references,
                        matched_paths,
                    );
                }
                wz.ascend_byte();
            }
        }
    }

    /// Match `inner_remaining` children of a nested arity, then continue
    /// with `outer_remaining` siblings starting at `siblings_offset`.
    fn walk_nested_then_siblings(
        &self,
        wz: &mut WriteZipperTracked<u64>,
        expr_offset: usize,
        inner_remaining: u8,
        siblings_offset: usize,
        outer_remaining: u8,
        references: &mut Vec<usize>,
        matched_paths: &mut Vec<Vec<u8>>,
    ) {
        if inner_remaining == 0 {
            self.walk_arity_children(
                wz,
                siblings_offset,
                outer_remaining,
                references,
                matched_paths,
            );
            return;
        }

        let data = &self.pattern;
        if expr_offset >= data.len() {
            return;
        }

        let child_size = expr_item_size(data, expr_offset);
        if child_size == 0 {
            return;
        }

        let next_offset = expr_offset + child_size;
        let tag = byte_item(data[expr_offset]);

        match tag {
            Tag::NewVar => {
                let ref_idx = references.len();
                references.push(wz.path().len());

                let cm = wz.child_mask();
                let mut it = cm.iter();
                while let Some(b) = it.next() {
                    let child_tag = byte_item(b);
                    match child_tag {
                        Tag::SymbolSize(size) => {
                            wz.descend_to_byte(b);
                            if wz.path_exists() {
                                self.enumerate_k_paths_then_nested(
                                    wz,
                                    size as usize,
                                    next_offset,
                                    inner_remaining - 1,
                                    siblings_offset,
                                    outer_remaining,
                                    references,
                                    matched_paths,
                                );
                            }
                            wz.ascend_byte();
                        }
                        Tag::Arity(_) | _ => {
                            wz.descend_to_byte(b);
                            if wz.path_exists() {
                                self.walk_nested_then_siblings(
                                    wz,
                                    next_offset,
                                    inner_remaining - 1,
                                    siblings_offset,
                                    outer_remaining,
                                    references,
                                    matched_paths,
                                );
                            }
                            wz.ascend_byte();
                        }
                    }
                }

                references.truncate(ref_idx);
            }

            Tag::VarRef(k) => {
                if (k as usize) < references.len() {
                    let ref_path_start = references[k as usize];
                    let current_path = wz.path().to_vec();
                    if ref_path_start < current_path.len() {
                        let bound_copy = current_path[ref_path_start..].to_vec();
                        wz.descend_to(&bound_copy);
                        if wz.path_exists() {
                            self.walk_nested_then_siblings(
                                wz,
                                next_offset,
                                inner_remaining - 1,
                                siblings_offset,
                                outer_remaining,
                                references,
                                matched_paths,
                            );
                        }
                        wz.ascend(bound_copy.len());
                    }
                }
            }

            Tag::SymbolSize(size) => {
                let symbol_byte = data[expr_offset];
                let start = expr_offset + 1;
                let end = start + size as usize;
                if end > data.len() {
                    return;
                }
                let symbol_bytes = &data[start..end];

                wz.descend_to_byte(symbol_byte);
                if wz.path_exists() {
                    wz.descend_to(symbol_bytes);
                    if wz.path_exists() {
                        self.walk_nested_then_siblings(
                            wz,
                            next_offset,
                            inner_remaining - 1,
                            siblings_offset,
                            outer_remaining,
                            references,
                            matched_paths,
                        );
                    }
                    wz.ascend(size as usize);
                }
                wz.ascend_byte();
            }

            Tag::Arity(sub_arity) => {
                let arity_byte = data[expr_offset];
                wz.descend_to_byte(arity_byte);
                if wz.path_exists() {
                    self.walk_nested_then_siblings(
                        wz,
                        expr_offset + 1,
                        sub_arity + (inner_remaining - 1),
                        siblings_offset,
                        outer_remaining,
                        references,
                        matched_paths,
                    );
                }
                wz.ascend_byte();
            }
        }
    }

    // -----------------------------------------------------------------------
    // k-path enumeration helpers
    // -----------------------------------------------------------------------

    /// Enumerate all k-byte paths at the current trie position, and for
    /// each one, continue matching at `continuation_offset`.
    fn enumerate_k_paths(
        &self,
        wz: &mut WriteZipperTracked<u64>,
        k: usize,
        continuation_offset: usize,
        references: &mut Vec<usize>,
        matched_paths: &mut Vec<Vec<u8>>,
    ) {
        if k == 0 {
            self.walk_item(wz, continuation_offset, references, matched_paths);
            return;
        }

        let cm = wz.child_mask();
        let mut it = cm.iter();
        while let Some(b) = it.next() {
            wz.descend_to_byte(b);
            if wz.path_exists() {
                self.enumerate_k_paths(wz, k - 1, continuation_offset, references, matched_paths);
            }
            wz.ascend_byte();
        }
    }

    /// Enumerate all k-byte paths then continue matching arity siblings.
    fn enumerate_k_paths_then_siblings(
        &self,
        wz: &mut WriteZipperTracked<u64>,
        k: usize,
        siblings_offset: usize,
        remaining_siblings: u8,
        references: &mut Vec<usize>,
        matched_paths: &mut Vec<Vec<u8>>,
    ) {
        if k == 0 {
            self.walk_arity_children(
                wz,
                siblings_offset,
                remaining_siblings,
                references,
                matched_paths,
            );
            return;
        }

        let cm = wz.child_mask();
        let mut it = cm.iter();
        while let Some(b) = it.next() {
            wz.descend_to_byte(b);
            if wz.path_exists() {
                self.enumerate_k_paths_then_siblings(
                    wz,
                    k - 1,
                    siblings_offset,
                    remaining_siblings,
                    references,
                    matched_paths,
                );
            }
            wz.ascend_byte();
        }
    }

    /// Enumerate all k-byte paths then continue matching nested arity
    /// children followed by outer siblings.
    fn enumerate_k_paths_then_nested(
        &self,
        wz: &mut WriteZipperTracked<u64>,
        k: usize,
        inner_offset: usize,
        inner_remaining: u8,
        siblings_offset: usize,
        outer_remaining: u8,
        references: &mut Vec<usize>,
        matched_paths: &mut Vec<Vec<u8>>,
    ) {
        if k == 0 {
            self.walk_nested_then_siblings(
                wz,
                inner_offset,
                inner_remaining,
                siblings_offset,
                outer_remaining,
                references,
                matched_paths,
            );
            return;
        }

        let cm = wz.child_mask();
        let mut it = cm.iter();
        while let Some(b) = it.next() {
            wz.descend_to_byte(b);
            if wz.path_exists() {
                self.enumerate_k_paths_then_nested(
                    wz,
                    k - 1,
                    inner_offset,
                    inner_remaining,
                    siblings_offset,
                    outer_remaining,
                    references,
                    matched_paths,
                );
            }
            wz.ascend_byte();
        }
    }
}

// ---------------------------------------------------------------------------
// Free function: compute the byte size of an expression item in a byte slice
// ---------------------------------------------------------------------------

/// Compute the byte size of the expression item at `offset` in `data`.
///
/// Returns the total number of bytes consumed by this item:
/// - `NewVar`: 1 byte
/// - `VarRef(_)`: 1 byte
/// - `SymbolSize(n)`: 1 + n bytes
/// - `Arity(a)`: 1 + sum of sizes of `a` children
fn expr_item_size(data: &[u8], offset: usize) -> usize {
    if offset >= data.len() {
        return 0;
    }
    match byte_item(data[offset]) {
        Tag::NewVar => 1,
        Tag::VarRef(_) => 1,
        Tag::SymbolSize(n) => 1 + n as usize,
        Tag::Arity(a) => {
            let mut size = 1;
            for _ in 0..a {
                let child_size = expr_item_size(data, offset + size);
                if child_size == 0 {
                    return 0;
                }
                size += child_size;
            }
            size
        }
    }
}

// Safety: all fields are owned (Vec<u8>, String, Arc<AtomicUsize>).
// No raw pointers are shared.
unsafe impl Send for SExprOperation {}
unsafe impl Sync for SExprOperation {}

impl TransformOp for SExprOperation {
    fn name(&self) -> &str {
        &self.name
    }

    fn apply(&self, wz: &mut WriteZipperTracked<u64>, atom_path: &[u8]) {
        trace!(
            name = %self.name,
            atom_path_len = atom_path.len(),
            pattern_len = self.pattern.len(),
            template_count = self.templates.len(),
            "SExprOperation::apply"
        );

        // Empty pattern: apply templates unconditionally (no matching)
        if self.pattern.is_empty() {
            self.match_counter.fetch_add(1, Ordering::Relaxed);
            self.apply_templates_direct(wz);
            return;
        }

        // Phase 1: Walk the trie to find all paths matching the pattern
        let mut matched_paths: Vec<Vec<u8>> = Vec::new();
        self.walk_pattern(wz, &mut matched_paths);
        wz.reset();

        let match_count = matched_paths.len();
        self.match_counter.fetch_add(match_count, Ordering::Relaxed);

        debug!(
            name = %self.name,
            match_count,
            total_matches = self.match_counter.load(Ordering::Relaxed),
            "pattern walk complete"
        );

        // Phase 2+3+4: For each match, extract bindings and instantiate templates
        for matched_data in &matched_paths {
            self.apply_templates(wz, matched_data);
        }
    }
}
