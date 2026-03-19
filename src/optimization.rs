//! Topology optimization (fusion, prefetch marking, builtin detection)

use pyo3::prelude::*;
use pyo3::types::{PyString, PyTuple};
use std::collections::{HashMap, HashSet};

use crate::{Stream, StreamPtr};
use crate::helpers::{
    get_analyze_lambda, get_builtin_bool, get_builtin_float, get_builtin_frozenset,
    get_builtin_int, get_builtin_isinstance, get_builtin_len, get_builtin_list,
    get_builtin_print, get_builtin_set, get_builtin_str, get_builtin_tuple, get_builtin_type,
    get_operator_attrgetter, get_operator_is, get_operator_is_not, get_operator_itemgetter,
    get_operator_not, get_operator_truth,
};
use crate::node::{BinaryOp, BuiltinOp, ChainNode, CompareOp, NodeLogic};

impl Stream {
    /// Optimize the topology on first emit.
    ///
    /// This method walks the stream graph and:
    /// - Fuses consecutive Map operations into a single composed Map
    /// - If all nodes have sync-only functions (`func_is_sync` = true), enables
    ///   `skip_async_check` for all nodes to avoid per-emit `is_awaitable` checks
    pub fn optimize_topology(&mut self, py: Python) {
        // Phase 1: Fuse consecutive maps and filters
        // DISABLED: Unsafe for dynamic topologies (where nodes are added after first emit)
        // Use optimize_topology_full() via compile() if you want fusion enabled.
        // self.fuse_linear_chains(py);

        // Phase 2: Collect all nodes and check sync status
        let all_nodes = self.collect_all_nodes(py);

        // If all nodes are sync and asynchronous flag wasn't explicitly set,
        // enable skip_async_check for all nodes
        // DISABLED: Unsafe if async nodes are added later to a "sync" optimized graph
        /*
        let all_sync = self.func_is_sync && all_nodes.iter().all(|n| n.borrow(py).func_is_sync);
        if all_sync && self.asynchronous.is_none() {
            self.skip_async_check = true;
            for node_py in &all_nodes {
                let mut node = node_py.borrow_mut(py);
                if node.asynchronous.is_none() {
                    node.skip_async_check = true;
                }
            }
        }
        */

        // Phase 3: Mark prefetch nodes inside par branches as needing locks
        // These nodes use internal threading which can conflict with par's threading
        self.mark_prefetch_in_par(py, &all_nodes);
    }

    /// Full optimization pass, safe because graph is being frozen via compile().
    ///
    /// This enables all optimizations including chain compilation, which is only safe
    /// when the topology is guaranteed not to change after optimization.
    pub fn optimize_topology_full(&mut self, py: Python) {
        // Phase 1: Eliminate source passthroughs
        self.eliminate_source_passthroughs(py);

        // Phase 2: Detect builtin fast-paths before chain building
        // (chain building moves builtin_op into ChainNode structs)
        self.detect_builtins(py);

        // Phase 3: Cache vectorcall function pointers before chain building
        // (chain building moves cached_vectorcall into ChainNode structs)
        self.cache_vectorcall_ptrs(py);

        // Phase 4: Build compiled chains for linear segments
        self.build_compiled_chains(py);

        // Phase 3: Collect all nodes and check sync status
        let all_nodes = self.collect_all_nodes(py);

        // If all nodes are sync and asynchronous flag wasn't explicitly set,
        // enable skip_async_check for all nodes
        let all_sync = self.func_is_sync && all_nodes.iter().all(|n| n.borrow(py).func_is_sync);
        if all_sync && self.asynchronous.is_none() {
            self.skip_async_check = true;
            for node_py in &all_nodes {
                let mut node = node_py.borrow_mut(py);
                if node.asynchronous.is_none() {
                    node.skip_async_check = true;
                }
            }
        }

        // Phase 3: Warn about par() nodes without splits (useless overhead)
        Self::warn_useless_par(py, &all_nodes);

        // Phase 4: Mark prefetch nodes inside par branches as needing locks
        self.mark_prefetch_in_par(py, &all_nodes);
    }

    /// Collect all nodes reachable from this node's downstreams.
    fn collect_all_nodes(&self, py: Python) -> Vec<Py<Self>> {
        let mut all_nodes: Vec<Py<Self>> = Vec::new();
        let mut to_visit: Vec<Py<Self>> =
            self.downstreams.iter().map(|d| d.clone_ref(py)).collect();

        // BFS traversal
        while let Some(node_py) = to_visit.pop() {
            let node = node_py.borrow(py);
            for downstream in &node.downstreams {
                to_visit.push(downstream.clone_ref(py));
            }
            drop(node);
            all_nodes.push(node_py);
        }

        all_nodes
    }

    /// Mark this node and all downstream nodes as frozen and compiled.
    ///
    /// This is called by compile() to prevent any further modifications
    /// to the stream graph after optimization.
    pub fn freeze_all(&mut self, py: Python) {
        self.frozen = true;
        self.compiled = true;
        // Cache chainability for the source node
        self.compiled_chainable = (self.logic.is_chainable()
            && !matches!(&self.logic, NodeLogic::Filter { .. }))
            || matches!(&self.logic, NodeLogic::Chain { .. });
        self.compiled_filter_fast = matches!(&self.logic, NodeLogic::Filter { .. })
            && (self.skip_async_check || self.builtin_op.is_some());
        // Cache raw pointers to downstream Streams (bypass PyCell borrow checks in propagate)
        self.compiled_downstream_ptrs = self.downstreams.iter().map(|d| {
            let mut guard = d.borrow_mut(py);
            StreamPtr(&mut *guard as *mut Stream)
        }).collect();

        // BFS to freeze all reachable nodes
        let mut to_visit: Vec<Py<Self>> =
            self.downstreams.iter().map(|d| d.clone_ref(py)).collect();

        while let Some(node_py) = to_visit.pop() {
            let mut node = node_py.borrow_mut(py);
            if !node.frozen {
                node.frozen = true;
                node.compiled = true;
                // Cache chainability so update() doesn't re-derive it per item
                node.compiled_chainable = (node.logic.is_chainable()
                    && !matches!(&node.logic, NodeLogic::Filter { .. }))
                    || matches!(&node.logic, NodeLogic::Chain { .. });
                node.compiled_filter_fast = matches!(&node.logic, NodeLogic::Filter { .. })
                    && (node.skip_async_check || node.builtin_op.is_some());
                // Cache raw pointers to downstream Streams
                let downstream_refs: Vec<Py<Stream>> = node.downstreams.iter().map(|d| d.clone_ref(py)).collect();
                drop(node);
                let ptrs: Vec<StreamPtr> = downstream_refs.iter().map(|d| {
                    let mut guard = d.borrow_mut(py);
                    StreamPtr(&mut *guard as *mut Stream)
                }).collect();
                let mut node = node_py.borrow_mut(py);
                node.compiled_downstream_ptrs = ptrs;
                for downstream in &node.downstreams {
                    to_visit.push(downstream.clone_ref(py));
                }
            }
        }
    }

    /// Warn about par() nodes that don't have multiple downstream branches.
    /// A par() without splits is useless overhead - it adds thread pool dispatch
    /// without any parallelism benefit.
    fn warn_useless_par(py: Python, all_nodes: &[Py<Self>]) {
        for node_py in all_nodes {
            let node = node_py.borrow(py);
            if matches!(node.logic, NodeLogic::Parallel) && node.downstreams.len() <= 1 {
                let msg = format!(
                    "par() node '{}' has {} downstream(s). \
                     par() is only useful with multiple branches for parallel execution.",
                    node.name,
                    node.downstreams.len()
                );
                drop(node);
                // Emit a Python UserWarning
                let warn_type = py.get_type::<pyo3::exceptions::PyUserWarning>();
                let c_msg = std::ffi::CString::new(msg).unwrap();
                let _ = PyErr::warn(py, warn_type.as_any(), &c_msg, 1);
            }
        }
    }

    /// Mark nodes that need locking due to being downstream of a prefetch within a par branch.
    /// When prefetch is inside par, its internal threading combined with par's threading
    /// can cause borrow conflicts at any downstream node (especially convergence points like union).
    /// We mark ALL nodes from the prefetch onwards as needing locks.
    #[allow(clippy::unused_self)] // Called as method for consistency with other topology methods
    fn mark_prefetch_in_par(&self, py: Python, all_nodes: &[Py<Self>]) {
        // Find all Par nodes
        let mut par_nodes: Vec<Py<Self>> = Vec::new();
        for node_py in all_nodes {
            let node = node_py.borrow(py);
            if matches!(node.logic, NodeLogic::Parallel) {
                drop(node);
                par_nodes.push(node_py.clone_ref(py));
            }
        }

        if par_nodes.is_empty() {
            return; // No parallel branches, no prefetch locking needed
        }

        for par_py in par_nodes {
            let par = par_py.borrow(py);
            let branches: Vec<Py<Self>> = par.downstreams.iter().map(|d| d.clone_ref(py)).collect();
            drop(par);

            if branches.len() < 2 {
                continue; // Need at least 2 branches for convergence
            }

            // Check if any branch contains a prefetch
            let mut has_prefetch = false;
            for branch_py in &branches {
                let mut visited: HashSet<usize> = HashSet::new();
                let mut stack: Vec<Py<Self>> = vec![branch_py.clone_ref(py)];

                while let Some(node_py) = stack.pop() {
                    let node_ptr = node_py.as_ptr() as usize;
                    if visited.contains(&node_ptr) {
                        continue;
                    }
                    visited.insert(node_ptr);

                    let node = node_py.borrow(py);
                    if matches!(
                        node.logic,
                        NodeLogic::PrefetchMap { .. }
                            | NodeLogic::PrefetchFilter { .. }
                            | NodeLogic::PrefetchBatchMap { .. }
                    ) {
                        has_prefetch = true;
                        break;
                    }
                    for downstream in &node.downstreams {
                        stack.push(downstream.clone_ref(py));
                    }
                }
                if has_prefetch {
                    break;
                }
            }

            if !has_prefetch {
                continue; // No prefetch in this par, no locking needed
            }

            // Find convergence points: nodes reachable from multiple branches
            // Track which branch indices can reach each node
            let mut node_branches: HashMap<usize, HashSet<usize>> = HashMap::new();
            let mut node_refs: HashMap<usize, Py<Self>> = HashMap::new();

            for (branch_idx, branch_py) in branches.iter().enumerate() {
                let mut visited: HashSet<usize> = HashSet::new();
                let mut stack: Vec<Py<Self>> = vec![branch_py.clone_ref(py)];

                while let Some(node_py) = stack.pop() {
                    let node_ptr = node_py.as_ptr() as usize;
                    if visited.contains(&node_ptr) {
                        continue;
                    }
                    visited.insert(node_ptr);

                    node_branches
                        .entry(node_ptr)
                        .or_default()
                        .insert(branch_idx);
                    node_refs
                        .entry(node_ptr)
                        .or_insert_with(|| node_py.clone_ref(py));

                    let node = node_py.borrow(py);
                    for downstream in &node.downstreams {
                        stack.push(downstream.clone_ref(py));
                    }
                }
            }

            // Find convergence points (nodes reachable from 2+ branches)
            let convergence_ids: HashSet<usize> = node_branches
                .iter()
                .filter(|(_, branch_set)| branch_set.len() >= 2)
                .map(|(id, _)| *id)
                .collect();

            if convergence_ids.is_empty() {
                continue; // No convergence points, no locking needed
            }

            // Only mark actual convergence points - nodes before convergence are in
            // separate branches (never accessed concurrently), and prefetch's internal
            // lock already protects its own state. Only the merge point needs locking.
            for conv_id in &convergence_ids {
                if let Some(conv_py) = node_refs.get(conv_id) {
                    let mut conv = conv_py.borrow_mut(py);
                    conv.needs_lock = true;
                }
            }
        }
    }

    /// Eliminate source passthrough nodes from the graph.
    /// A Source node with a single downstream that is itself a Source
    /// can be bypassed (union creates intermediate Source nodes).
    fn eliminate_source_passthroughs(&mut self, py: Python) {
        // Recurse into downstreams first (bottom-up)
        for downstream in &self.downstreams {
            let mut child = downstream.borrow_mut(py);
            child.eliminate_source_passthroughs(py);
        }

        if self.downstreams.len() != 1 {
            return;
        }

        let child_py = &self.downstreams[0];
        let child = child_py.borrow(py);

        if matches!(child.logic, NodeLogic::Source) {
            let new_downstreams: Vec<Py<Self>> =
                child.downstreams.iter().map(|d| d.clone_ref(py)).collect();
            drop(child);
            self.downstreams = new_downstreams;
        }
    }

    /// Build compiled chains for all linear segments reachable from this source.
    ///
    /// Walks the graph and extracts linear segments of chainable nodes
    /// (Map, Filter, Flatten, Accumulate, Sink, BatchMap) into
    /// `NodeLogic::Chain` variants. Eliminates per-node borrow_mut/drop/propagate
    /// overhead during execution.
    pub(crate) fn build_compiled_chains(&mut self, py: Python) {
        // Compute in-degree for all nodes so we don't absorb shared nodes into chains.
        // A node with in-degree > 1 (multiple parents) must not be consumed by a chain,
        // because other parents still need to reach it.
        let all_nodes = self.collect_all_nodes(py);
        let mut multi_parent: std::collections::HashSet<usize> = std::collections::HashSet::new();
        let mut seen_as_child: std::collections::HashSet<usize> = std::collections::HashSet::new();

        for node_py in &all_nodes {
            let node = node_py.borrow(py);
            for ds in &node.downstreams {
                let ptr = ds.as_ptr() as usize;
                if !seen_as_child.insert(ptr) {
                    multi_parent.insert(ptr);
                }
            }
        }
        // Also count source's own downstreams
        for ds in &self.downstreams {
            let ptr = ds.as_ptr() as usize;
            if !seen_as_child.insert(ptr) {
                multi_parent.insert(ptr);
            }
        }

        for downstream in &self.downstreams {
            Self::build_chain_at(py, downstream, &multi_parent);
        }
    }

    /// Try to build a compiled chain starting at the given node.
    /// Walks forward through single-downstream chainable nodes.
    /// Nodes in `multi_parent` have in-degree > 1 and must not be absorbed.
    fn build_chain_at(py: Python, start_py: &Py<Self>, multi_parent: &std::collections::HashSet<usize>) {
        // Check if this node is chainable
        {
            let node = start_py.borrow(py);
            if !node.logic.is_chainable() {
                // Not chainable — recurse into downstreams
                let downstreams: Vec<Py<Self>> = node.downstreams.iter().map(|d| d.clone_ref(py)).collect();
                drop(node);
                for ds in &downstreams {
                    Self::build_chain_at(py, ds, multi_parent);
                }
                return;
            }
        }

        // Walk forward collecting chainable nodes
        let mut chain_node_refs: Vec<Py<Self>> = vec![start_py.clone_ref(py)];

        loop {
            let last_py = chain_node_refs.last().unwrap();
            let last = last_py.borrow(py);

            // Terminal (Sink) or branching → stop
            if matches!(last.logic, NodeLogic::Sink { .. }) || last.downstreams.len() != 1 {
                break;
            }

            let next_py = last.downstreams[0].clone_ref(py);
            drop(last);

            // Don't absorb a node that has multiple parents
            if multi_parent.contains(&(next_py.as_ptr() as usize)) {
                break;
            }

            let next = next_py.borrow(py);
            let next_chainable = next.logic.is_chainable();
            drop(next);

            if !next_chainable {
                break;
            }

            chain_node_refs.push(next_py);
        }

        // Skip chains of length 1 — no benefit
        if chain_node_refs.len() < 2 {
            let node = start_py.borrow(py);
            let downstreams: Vec<Py<Self>> = node.downstreams.iter().map(|d| d.clone_ref(py)).collect();
            drop(node);
            for ds in &downstreams {
                Self::build_chain_at(py, ds, multi_parent);
            }
            return;
        }

        // Get tail downstreams before we start mutating
        let last_py = chain_node_refs.last().unwrap();
        let last = last_py.borrow(py);
        let tail_downstreams: Vec<Py<Self>> = last.downstreams.iter().map(|d| d.clone_ref(py)).collect();
        drop(last);

        // Extract logic from each node into ChainNode structs
        let mut steps: Vec<ChainNode> = Vec::new();
        let mut combined_func_is_sync = true;

        for node_py in &chain_node_refs {
            let mut node = node_py.borrow_mut(py);
            let node_id = std::ptr::from_ref(&*node) as usize;
            let logic = std::mem::replace(&mut node.logic, NodeLogic::ChainMember);
            let name = std::mem::take(&mut node.name);
            let source_location = std::mem::take(&mut node.source_location);
            let builtin_op = node.builtin_op.take();
            let cached_vectorcall = node.cached_vectorcall.take();
            combined_func_is_sync = combined_func_is_sync && node.func_is_sync;
            steps.push(ChainNode {
                logic,
                name,
                source_location,
                builtin_op,
                node_id,
                cached_vectorcall,
            });
        }

        // Set head node's logic to Chain, downstreams to tail's downstreams
        let mut head = start_py.borrow_mut(py);
        head.logic = NodeLogic::Chain { steps };
        head.downstreams = tail_downstreams.iter().map(|d| d.clone_ref(py)).collect();
        head.func_is_sync = combined_func_is_sync;
        drop(head);

        // Recurse into tail downstreams and any branches from branching nodes
        for ds in &tail_downstreams {
            Self::build_chain_at(py, ds, multi_parent);
        }

        // Also recurse into downstreams of intermediate nodes that had branches
        // (This handles the case where a chain stops at a branching node — we already
        // processed those above via the length-1 skip path)
    }

    /// Detect builtin fast-path hints for all nodes in the graph.
    ///
    /// Called during compile() to populate `builtin_op` on nodes that use
    /// known builtins (len, str, int, isinstance, etc.) for faster execution.
    fn detect_builtins(&mut self, py: Python) {
        let all_nodes = self.collect_all_nodes(py);
        for node_py in &all_nodes {
            let mut node = node_py.borrow_mut(py);
            node.builtin_op = detect_builtin_for_logic(py, &mut node.logic);
        }
    }

    /// Cache vectorcall function pointers for all nodes in the graph.
    ///
    /// Extracts the raw `vectorcallfunc` pointer from each node's callable
    /// so the hot loop can skip the per-call type flag check and offset dereference.
    fn cache_vectorcall_ptrs(&mut self, py: Python) {
        self.cached_vectorcall = extract_vectorcall_for_logic(py, &self.logic);

        let all_nodes = self.collect_all_nodes(py);
        for node_py in &all_nodes {
            let mut node = node_py.borrow_mut(py);
            node.cached_vectorcall = extract_vectorcall_for_logic(py, &node.logic);
        }
    }

}

/// Parse a comparison op string from the Python analyzer into a `CompareOp`.
fn parse_compare_op(s: &str) -> Option<CompareOp> {
    match s {
        "==" => Some(CompareOp::Eq),
        "!=" => Some(CompareOp::Ne),
        "<" => Some(CompareOp::Lt),
        ">" => Some(CompareOp::Gt),
        "<=" => Some(CompareOp::Le),
        ">=" => Some(CompareOp::Ge),
        _ => None,
    }
}

/// Parse a binary op string from the Python analyzer into a `BinaryOp`.
fn parse_binary_op(s: &str) -> Option<BinaryOp> {
    match s {
        "+" => Some(BinaryOp::Add),
        "-" => Some(BinaryOp::Sub),
        "*" => Some(BinaryOp::Mul),
        "/" => Some(BinaryOp::TrueDiv),
        "//" => Some(BinaryOp::FloorDiv),
        "%" => Some(BinaryOp::Mod),
        "**" => Some(BinaryOp::Pow),
        _ => None,
    }
}

/// Recursively parse the tuple returned by `_analyze_lambda()` into a `BuiltinOp`.
///
/// The Python analyzer returns nested tuples like `("chain", ("attrgetter", "name"), ("compare", "==", "John"))`.
#[allow(clippy::only_used_in_recursion)]
fn parse_analysis_tuple(py: Python, obj: &Bound<'_, PyAny>) -> Option<BuiltinOp> {
    let tuple = obj.cast::<PyTuple>().ok()?;
    let tag: String = tuple.get_item(0).ok()?.extract().ok()?;

    match tag.as_str() {
        "len" => Some(BuiltinOp::Len),
        "str" => Some(BuiltinOp::Str),
        "int" => Some(BuiltinOp::Int),
        "float" => Some(BuiltinOp::Float),
        "bool" => Some(BuiltinOp::Bool),
        "not" => Some(BuiltinOp::Not),
        "itemgetter" => {
            let key = tuple.get_item(1).ok()?.unbind();
            Some(BuiltinOp::ItemGetter(key))
        }
        "attrgetter" => {
            let name_str: Py<PyString> = tuple.get_item(1).ok()?.extract().ok()?;
            Some(BuiltinOp::AttrGetter(name_str))
        }
        "isinstance" => {
            let type_obj = tuple.get_item(1).ok()?.unbind();
            Some(BuiltinOp::IsInstance(type_obj))
        }
        "is" => {
            let val = tuple.get_item(1).ok()?.unbind();
            Some(BuiltinOp::Is(val))
        }
        "is_not" => {
            let val = tuple.get_item(1).ok()?.unbind();
            Some(BuiltinOp::IsNot(val))
        }
        "compare" => {
            let op_str: String = tuple.get_item(1).ok()?.extract().ok()?;
            let cmp_op = parse_compare_op(&op_str)?;
            let rhs = tuple.get_item(2).ok()?.unbind();
            Some(BuiltinOp::Compare(cmp_op, rhs))
        }
        "binop" => {
            let op_str: String = tuple.get_item(1).ok()?.extract().ok()?;
            let bin_op = parse_binary_op(&op_str)?;
            let rhs = tuple.get_item(2).ok()?.unbind();
            Some(BuiltinOp::BinOp(bin_op, rhs))
        }
        "contains" => {
            let coll = tuple.get_item(1).ok()?.unbind();
            Some(BuiltinOp::Contains(coll))
        }
        "not_contains" => {
            let coll = tuple.get_item(1).ok()?.unbind();
            Some(BuiltinOp::NotContains(coll))
        }
        "method0" => {
            let name_str: Py<PyString> = tuple.get_item(1).ok()?.extract().ok()?;
            Some(BuiltinOp::MethodCall(name_str))
        }
        "method1" => {
            let name_str: Py<PyString> = tuple.get_item(1).ok()?.extract().ok()?;
            let arg = tuple.get_item(2).ok()?.unbind();
            Some(BuiltinOp::MethodCall1(name_str, arg))
        }
        "method2" => {
            let name_str: Py<PyString> = tuple.get_item(1).ok()?.extract().ok()?;
            let arg1 = tuple.get_item(2).ok()?.unbind();
            let arg2 = tuple.get_item(3).ok()?.unbind();
            Some(BuiltinOp::MethodCall2(name_str, arg1, arg2))
        }
        "type" => Some(BuiltinOp::Type),
        "tuple" => Some(BuiltinOp::Tuple),
        "list" => Some(BuiltinOp::List),
        "set" => Some(BuiltinOp::Set),
        "frozenset" => Some(BuiltinOp::FrozenSet),
        "hasattr" => {
            let name_str: Py<PyString> = tuple.get_item(1).ok()?.extract().ok()?;
            Some(BuiltinOp::HasAttr(name_str))
        }
        "slice" => {
            let start = tuple.get_item(1).ok()?.unbind();
            let stop = tuple.get_item(2).ok()?.unbind();
            Some(BuiltinOp::Slice(start, stop))
        }
        "build_tuple" | "build_list" => {
            let mut ops = Vec::new();
            for i in 1..tuple.len() {
                let sub = parse_analysis_tuple(py, &tuple.get_item(i).ok()?)?;
                ops.push(sub);
            }
            if tag == "build_tuple" {
                Some(BuiltinOp::TupleConstruct(ops))
            } else {
                Some(BuiltinOp::ListConstruct(ops))
            }
        }
        "chain" => {
            let first = parse_analysis_tuple(py, &tuple.get_item(1).ok()?)?;
            let second = parse_analysis_tuple(py, &tuple.get_item(2).ok()?)?;
            Some(BuiltinOp::Chain(Box::new(first), Box::new(second)))
        }
        "negate" => {
            let inner = parse_analysis_tuple(py, &tuple.get_item(1).ok()?)?;
            Some(BuiltinOp::Negate(Box::new(inner)))
        }
        "and" => {
            let left = parse_analysis_tuple(py, &tuple.get_item(1).ok()?)?;
            let right = parse_analysis_tuple(py, &tuple.get_item(2).ok()?)?;
            Some(BuiltinOp::And(Box::new(left), Box::new(right)))
        }
        "or" => {
            let left = parse_analysis_tuple(py, &tuple.get_item(1).ok()?)?;
            let right = parse_analysis_tuple(py, &tuple.get_item(2).ok()?)?;
            Some(BuiltinOp::Or(Box::new(left), Box::new(right)))
        }
        _ => None,
    }
}

/// Analyze a Python callable's bytecode via `_analyze_lambda()` and convert to `BuiltinOp`.
fn analyze_lambda_fallback(py: Python, func: &Py<PyAny>) -> Option<BuiltinOp> {
    let analyzer = get_analyze_lambda().ok()?;
    let result = analyzer.call1(py, (func,)).ok()?;
    if result.is_none(py) {
        return None;
    }
    parse_analysis_tuple(py, result.bind(py))
}

/// Detect builtin fast-path for a Map function via identity checks only.
fn detect_map_builtin_identity(py: Python, func: &Py<PyAny>) -> Option<BuiltinOp> {
    // Simple identity checks: func is exactly the builtin
    macro_rules! check_identity {
        ($getter:ident, $op:expr) => {
            if let Ok(b) = $getter() { if func.is(b) { return Some($op); } }
        };
    }
    check_identity!(get_builtin_len, BuiltinOp::Len);
    check_identity!(get_builtin_str, BuiltinOp::Str);
    check_identity!(get_builtin_int, BuiltinOp::Int);
    check_identity!(get_builtin_float, BuiltinOp::Float);
    check_identity!(get_builtin_bool, BuiltinOp::Bool);
    check_identity!(get_operator_not, BuiltinOp::Not);
    check_identity!(get_operator_truth, BuiltinOp::Truth);
    check_identity!(get_builtin_type, BuiltinOp::Type);
    check_identity!(get_builtin_tuple, BuiltinOp::Tuple);
    check_identity!(get_builtin_list, BuiltinOp::List);
    check_identity!(get_builtin_set, BuiltinOp::Set);
    check_identity!(get_builtin_frozenset, BuiltinOp::FrozenSet);

    // Detect operator.itemgetter instances (e.g., operator.itemgetter("name"))
    if get_operator_itemgetter().is_ok_and(|ig| func.bind(py).is_instance(ig.bind(py)).unwrap_or(false)) {
        let key = func.call_method0(py, "__reduce__").ok()
            .and_then(|r| r.bind(py).get_item(1).ok().map(|a| a.unbind()))
            .and_then(|a| a.bind(py).cast::<PyTuple>().ok().map(|t| t.to_owned()))
            .filter(|t| t.len() == 1)
            .and_then(|t| t.get_item(0).ok().map(|k| k.unbind()));
        if let Some(k) = key {
            return Some(BuiltinOp::ItemGetter(k));
        }
    }
    // Detect operator.attrgetter instances (e.g., operator.attrgetter("name"))
    if get_operator_attrgetter().is_ok_and(|ag| func.bind(py).is_instance(ag.bind(py)).unwrap_or(false)) {
        let name = func.call_method0(py, "__reduce__").ok()
            .and_then(|r| r.bind(py).get_item(1).ok().map(|a| a.unbind()))
            .and_then(|a| a.bind(py).cast::<PyTuple>().ok().map(|t| t.to_owned()))
            .filter(|t| t.len() == 1)
            .and_then(|t| t.get_item(0).ok()?.cast_into::<pyo3::types::PyString>().ok());
        if let Some(name_str) = name {
            return Some(BuiltinOp::AttrGetter(name_str.unbind()));
        }
    }
    // Fallback: analyze lambda bytecode
    analyze_lambda_fallback(py, func)
}

/// Detect builtin fast-path for a Map function, also handling `("unwrap", callable)`.
///
/// When a lambda wraps a simple callable (e.g., `lambda x: MyClass(x)`), the analyzer
/// returns `("unwrap", resolved_callable)`. We replace the func in the Map logic directly
/// and return `None` — the callable gets cached vectorcall automatically.
fn detect_map_builtin_with_unwrap(py: Python, func: &mut Py<PyAny>) -> Option<BuiltinOp> {
    // First try the normal detection
    if let Some(op) = detect_map_builtin_identity(py, func) {
        return Some(op);
    }
    // Fallback: analyze lambda bytecode
    let analyzer = get_analyze_lambda().ok()?;
    let result = analyzer.call1(py, (func.clone_ref(py),)).ok()?;
    if result.is_none(py) {
        return None;
    }
    let bound = result.bind(py);
    let tuple = bound.cast::<PyTuple>().ok()?;
    let tag: String = tuple.get_item(0).ok()?.extract().ok()?;
    if tag == "unwrap" {
        // Replace func with the resolved callable — no BuiltinOp needed
        let resolved = tuple.get_item(1).ok()?.unbind();
        *func = resolved;
        return None;
    }
    parse_analysis_tuple(py, bound)
}

/// Detect builtin fast-path for a Filter predicate.
///
/// Handles wrapped predicates by checking `_akayu_inner` / `_akayu_args` attributes
/// set by `_build_map_func` when extra args were provided (e.g., `filter(isinstance, int)`).
fn detect_filter_builtin(py: Python, predicate: &Py<PyAny>) -> Option<BuiltinOp> {
    // Try to recover the original function and args from the wrapper
    let (func, args) = if let Ok(inner) = predicate.getattr(py, "_akayu_inner") {
        let args = predicate.getattr(py, "_akayu_args").ok();
        (inner, args)
    } else {
        // No wrapper — predicate is the original function (no extra args)
        // Fall through to lambda bytecode analysis
        return analyze_lambda_fallback(py, predicate);
    };

    // Filter builtins require exactly one extra arg
    let arg = if let Some(ref args_obj) = args {
        let args_bound = args_obj.bind(py);
        if let Ok(tuple) = args_bound.cast::<PyTuple>() {
            if tuple.len() == 1 {
                Some(tuple.get_item(0).ok()?.unbind())
            } else {
                None
            }
        } else {
            None
        }
    } else {
        None
    };
    let arg = arg?;

    if get_builtin_isinstance().is_ok_and(|b| func.is(b)) {
        return Some(BuiltinOp::IsInstance(arg));
    }
    if get_operator_is().is_ok_and(|b| func.is(b)) {
        return Some(BuiltinOp::Is(arg));
    }
    if get_operator_is_not().is_ok_and(|b| func.is(b)) {
        return Some(BuiltinOp::IsNot(arg));
    }
    None
}

/// Detect builtin fast-path for a Sink function (no extra args).
fn detect_sink_builtin(func: &Py<PyAny>) -> Option<BuiltinOp> {
    if get_builtin_print().is_ok_and(|b| func.is(b)) { return Some(BuiltinOp::Print); }
    None
}

/// Detect the appropriate builtin fast-path for a given NodeLogic variant.
///
/// For Map nodes, also handles `("unwrap", callable)` — replacing the func in-place.
fn detect_builtin_for_logic(py: Python, logic: &mut NodeLogic) -> Option<BuiltinOp> {
    match logic {
        NodeLogic::Map { func } => detect_map_builtin_with_unwrap(py, func),
        NodeLogic::Filter { predicate } => detect_filter_builtin(py, predicate),
        NodeLogic::Sink { func } => detect_sink_builtin(func),
        _ => None,
    }
}

/// Extract the vectorcall function pointer from a Python callable.
///
/// Returns `None` if the callable doesn't support vectorcall.
/// The pointer is valid as long as the callable object is alive.
unsafe fn extract_vectorcall(callable: *mut pyo3::ffi::PyObject) -> Option<pyo3::ffi::vectorcallfunc> {
    unsafe {
        let tp = pyo3::ffi::Py_TYPE(callable);
        if (*tp).tp_flags & pyo3::ffi::Py_TPFLAGS_HAVE_VECTORCALL == 0 {
            return None;
        }
        let offset = (*tp).tp_vectorcall_offset;
        if offset <= 0 {
            return None;
        }
        let ptr = (callable as *const u8).offset(offset as isize)
            as *const Option<pyo3::ffi::vectorcallfunc>;
        *ptr
    }
}

/// Extract the vectorcall function pointer for the callable in a NodeLogic.
fn extract_vectorcall_for_logic(_py: Python, logic: &NodeLogic) -> Option<pyo3::ffi::vectorcallfunc> {
    let callable_ptr = match logic {
        NodeLogic::Map { func } => func.as_ptr(),
        NodeLogic::Filter { predicate } => predicate.as_ptr(),
        NodeLogic::Sink { func } => func.as_ptr(),
        NodeLogic::Accumulate { func, .. } => func.as_ptr(),
        NodeLogic::BatchMap { func } => func.as_ptr(),
        _ => return None,
    };
    unsafe { extract_vectorcall(callable_ptr) }
}
