//! Topology optimization (fusion, prefetch marking, builtin detection)

use pyo3::prelude::*;
use pyo3::types::PyTuple;
use std::collections::{HashMap, HashSet};

use crate::Stream;
use crate::helpers::{
    get_builtin_bool, get_builtin_float, get_builtin_int, get_builtin_isinstance,
    get_builtin_len, get_builtin_print, get_builtin_str, get_operator_attrgetter,
    get_operator_is, get_operator_is_not, get_operator_itemgetter, get_operator_not,
    get_operator_truth,
};
use crate::node::{BuiltinOp, ChainNode, NodeLogic};

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

        // Phase 3: Build compiled chains for linear segments
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

        // BFS to freeze all reachable nodes
        let mut to_visit: Vec<Py<Self>> =
            self.downstreams.iter().map(|d| d.clone_ref(py)).collect();

        while let Some(node_py) = to_visit.pop() {
            let mut node = node_py.borrow_mut(py);
            if !node.frozen {
                node.frozen = true;
                node.compiled = true;
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
        for downstream in &self.downstreams {
            Self::build_chain_at(py, downstream);
        }
    }

    /// Try to build a compiled chain starting at the given node.
    /// Walks forward through single-downstream chainable nodes.
    fn build_chain_at(py: Python, start_py: &Py<Self>) {
        // Check if this node is chainable
        {
            let node = start_py.borrow(py);
            let is_chainable = matches!(node.logic,
                NodeLogic::Map { .. } | NodeLogic::Filter { .. } | NodeLogic::Flatten
                | NodeLogic::Accumulate { .. } | NodeLogic::Sink { .. } | NodeLogic::BatchMap { .. }
            );
            if !is_chainable {
                // Not chainable — recurse into downstreams
                let downstreams: Vec<Py<Self>> = node.downstreams.iter().map(|d| d.clone_ref(py)).collect();
                drop(node);
                for ds in &downstreams {
                    Self::build_chain_at(py, ds);
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

            let next = next_py.borrow(py);
            let next_chainable = matches!(next.logic,
                NodeLogic::Map { .. } | NodeLogic::Filter { .. } | NodeLogic::Flatten
                | NodeLogic::Accumulate { .. } | NodeLogic::Sink { .. } | NodeLogic::BatchMap { .. }
            );
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
                Self::build_chain_at(py, ds);
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
            combined_func_is_sync = combined_func_is_sync && node.func_is_sync;
            steps.push(ChainNode {
                logic,
                name,
                source_location,
                builtin_op,
                node_id,
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
            Self::build_chain_at(py, ds);
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
            node.builtin_op = detect_builtin_for_logic(py, &node.logic);
        }
    }

}

/// Detect builtin fast-path for a Map function (no extra args).
fn detect_map_builtin(py: Python, func: &Py<PyAny>) -> Option<BuiltinOp> {
    if let Ok(b) = get_builtin_len() { if func.is(b) { return Some(BuiltinOp::Len); } }
    if let Ok(b) = get_builtin_str() { if func.is(b) { return Some(BuiltinOp::Str); } }
    if let Ok(b) = get_builtin_int() { if func.is(b) { return Some(BuiltinOp::Int); } }
    if let Ok(b) = get_builtin_float() { if func.is(b) { return Some(BuiltinOp::Float); } }
    if let Ok(b) = get_builtin_bool() { if func.is(b) { return Some(BuiltinOp::Bool); } }
    if let Ok(b) = get_operator_not() { if func.is(b) { return Some(BuiltinOp::Not); } }
    if let Ok(b) = get_operator_truth() { if func.is(b) { return Some(BuiltinOp::Truth); } }
    // Detect operator.itemgetter instances (e.g., operator.itemgetter("name"))
    if let Ok(itemgetter_type) = get_operator_itemgetter() {
        if func.bind(py).is_instance(itemgetter_type.bind(py)).unwrap_or(false) {
            if let Ok(reduced) = func.call_method0(py, "__reduce__") {
                if let Ok(args) = reduced.bind(py).get_item(1) {
                    if let Ok(key_tuple) = args.cast::<PyTuple>() {
                        if key_tuple.len() == 1 {
                            return key_tuple.get_item(0).ok().map(|k| BuiltinOp::ItemGetter(k.unbind()));
                        }
                    }
                }
            }
        }
    }
    // Detect operator.attrgetter instances (e.g., operator.attrgetter("name"))
    if let Ok(attrgetter_type) = get_operator_attrgetter() {
        if func.bind(py).is_instance(attrgetter_type.bind(py)).unwrap_or(false) {
            if let Ok(reduced) = func.call_method0(py, "__reduce__") {
                if let Ok(args) = reduced.bind(py).get_item(1) {
                    if let Ok(key_tuple) = args.cast::<PyTuple>() {
                        if key_tuple.len() == 1 {
                            if let Some(Ok(name_str)) = key_tuple.get_item(0).ok().map(|k| k.cast_into::<pyo3::types::PyString>()) {
                                return Some(BuiltinOp::AttrGetter(name_str.unbind()));
                            }
                        }
                    }
                }
            }
        }
    }
    None
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
        return None;
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

    if let Ok(builtin_isinstance) = get_builtin_isinstance() {
        if func.is(&builtin_isinstance) {
            return Some(BuiltinOp::IsInstance(arg));
        }
    }
    if let Ok(op_is) = get_operator_is() {
        if func.is(&op_is) {
            return Some(BuiltinOp::Is(arg));
        }
    }
    if let Ok(op_is_not) = get_operator_is_not() {
        if func.is(&op_is_not) {
            return Some(BuiltinOp::IsNot(arg));
        }
    }
    None
}

/// Detect builtin fast-path for a Sink function (no extra args).
fn detect_sink_builtin(func: &Py<PyAny>) -> Option<BuiltinOp> {
    if let Ok(b) = get_builtin_print() { if func.is(b) { return Some(BuiltinOp::Print); } }
    None
}

/// Detect the appropriate builtin fast-path for a given NodeLogic variant.
fn detect_builtin_for_logic(py: Python, logic: &NodeLogic) -> Option<BuiltinOp> {
    match logic {
        NodeLogic::Map { func } => detect_map_builtin(py, func),
        NodeLogic::Filter { predicate } => detect_filter_builtin(py, predicate),
        NodeLogic::Sink { func } => detect_sink_builtin(func),
        _ => None,
    }
}
