//! Topology optimization (fusion, prefetch marking, builtin detection)

use pyo3::prelude::*;
use pyo3::types::{PyString, PyTuple};
use std::collections::{HashMap, HashSet};

use crate::helpers::{
    get_analyze_lambda, get_builtin_bool, get_builtin_float, get_builtin_frozenset,
    get_builtin_int, get_builtin_isinstance, get_builtin_len, get_builtin_list, get_builtin_print,
    get_builtin_set, get_builtin_str, get_builtin_tuple, get_builtin_type, get_operator_attrgetter,
    get_operator_is, get_operator_is_not, get_operator_itemgetter, get_operator_not,
    get_operator_truth,
};
use crate::node::{BinaryOp, BuiltinOp, ChainNode, CompareOp, NodeLogic};
use crate::{Stream, StreamPtr};

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
        self.mark_par_branch_nodes(py, &all_nodes);
    }

    /// Full optimization pass, safe because graph is being frozen via compile().
    ///
    /// This enables all optimizations including chain compilation, which is only safe
    /// when the topology is guaranteed not to change after optimization.
    pub fn optimize_topology_full(&mut self, py: Python, source_py_ptr: usize) {
        // Phase 1: Eliminate source passthroughs
        self.eliminate_source_passthroughs(py);

        // Phase 1.5: Fuse collect+filter→sink(flush) into ChunkBy
        self.fuse_chunk_by(py, source_py_ptr);

        // Phase 2: Detect flush targets (Sink → Collect direct Rust call)
        self.detect_flush_targets(py);

        // Phase 2.5: Detect emit targets (Sink → Source direct Rust call)
        self.detect_emit_targets(py);

        // Phase 3: Detect builtin fast-paths before chain building
        // (chain building moves builtin_op into ChainNode structs)
        self.detect_builtins(py);

        // Phase 4: Cache vectorcall function pointers before chain building
        // (chain building moves cached_vectorcall into ChainNode structs)
        self.cache_vectorcall_ptrs(py);

        // Phase 5: Build compiled chains for linear segments
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
        self.mark_par_branch_nodes(py, &all_nodes);

        // Phase 5: Mark convergence points and their downstreams with needs_lock
        self.mark_convergence_nodes(py, all_nodes);
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
        self.compiled_downstream_ptrs = self
            .downstreams
            .iter()
            .map(|d| {
                let mut guard = d.borrow_mut(py);
                StreamPtr(&mut *guard as *mut Stream)
            })
            .collect();

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
                let downstream_refs: Vec<Py<Stream>> =
                    node.downstreams.iter().map(|d| d.clone_ref(py)).collect();
                drop(node);
                let ptrs: Vec<StreamPtr> = downstream_refs
                    .iter()
                    .map(|d| {
                        let mut guard = d.borrow_mut(py);
                        StreamPtr(&mut *guard as *mut Stream)
                    })
                    .collect();
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

    /// Mark nodes that need per-node locking when inside par() branches.
    /// When par() dispatches to threads, multiple branches may converge on shared
    /// downstream nodes. We mark all nodes in par branches so their propagate()
    /// uses _safe_update (per-node Python locks), preventing concurrent borrow_mut
    /// on shared PyCells.
    #[allow(clippy::unused_self)]
    fn mark_par_branch_nodes(&self, py: Python, all_nodes: &[Py<Self>]) {
        let mut par_nodes: Vec<Py<Self>> = Vec::new();
        for node_py in all_nodes {
            let node = node_py.borrow(py);
            if matches!(node.logic, NodeLogic::Parallel) {
                drop(node);
                par_nodes.push(node_py.clone_ref(py));
            }
        }

        if par_nodes.is_empty() {
            return;
        }

        for par_py in par_nodes {
            let par = par_py.borrow(py);
            let branches: Vec<Py<Self>> = par.downstreams.iter().map(|d| d.clone_ref(py)).collect();
            drop(par);

            if branches.len() < 2 {
                continue;
            }

            // Check if branches actually converge anywhere
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

            let has_convergence = node_branches
                .values()
                .any(|branch_set| branch_set.len() >= 2);

            if !has_convergence {
                continue;
            }

            // Mark ALL nodes in par branches. Each node's propagate() checks
            // its own needs_lock flag to decide whether to use _safe_update.
            // We must mark branch nodes (not just convergence points) because
            // it's the branch node's propagate() that borrow_mut's the shared
            // downstream — the convergence point's own flag is checked too late.
            for node_py in node_refs.values() {
                let mut node = node_py.borrow_mut(py);
                node.needs_lock = true;
            }
        }
    }

    fn mark_convergence_nodes(&self, _py: Python, _all_nodes: Vec<Py<Self>>) {
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
            if child.parents.lock().unwrap().len() != 1 {
                return;
            }
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

    fn build_chain_at(py: Python, start_py: &Py<Self>) {
        // Check if this node is chainable
        {
            let node = start_py.borrow(py);
            if !node.logic.is_chainable() {
                let downstreams: Vec<Py<Self>> =
                    node.downstreams.iter().map(|d| d.clone_ref(py)).collect();
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

            if matches!(last.logic, NodeLogic::Sink { .. }) || last.downstreams.len() != 1 {
                break;
            }

            let next_py = last.downstreams[0].clone_ref(py);
            drop(last);

            let is_multi_parent = next_py.borrow(py).parents.lock().unwrap().len() > 1;
            if is_multi_parent {
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
            let downstreams: Vec<Py<Self>> =
                node.downstreams.iter().map(|d| d.clone_ref(py)).collect();
            drop(node);
            for ds in &downstreams {
                Self::build_chain_at(py, ds);
            }
            return;
        }

        // Get tail downstreams before we start mutating
        let last_py = chain_node_refs.last().unwrap();
        let last = last_py.borrow(py);
        let tail_downstreams: Vec<Py<Self>> =
            last.downstreams.iter().map(|d| d.clone_ref(py)).collect();
        drop(last);

        // Extract logic from each node into ChainNode structs
        let mut steps: Vec<ChainNode> = Vec::new();
        let mut combined_func_is_sync = true;

        for (i, node_py) in chain_node_refs.iter().enumerate() {
            let mut node = node_py.borrow_mut(py);
            let node_id = std::ptr::from_ref(&*node) as usize;
            let logic = std::mem::replace(&mut node.logic, NodeLogic::ChainMember);
            let name = std::mem::take(&mut node.name);
            let source_location = std::mem::take(&mut node.source_location);
            let builtin_op = node.builtin_op.take();
            let cached_vectorcall = node.cached_vectorcall.take();
            let flush_target = node.flush_target.take();
            let emit_target = node.emit_target.take();
            combined_func_is_sync = combined_func_is_sync && node.func_is_sync;
            node.downstreams.clear();
            steps.push(ChainNode {
                logic,
                name,
                source_location,
                builtin_op,
                node_id,
                cached_vectorcall,
                flush_target,
                emit_target,
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

    /// Detect Sink nodes whose function calls .flush() on a Collect node in the same graph.
    /// Sets `flush_target` on those Sink nodes so execution can bypass Python dispatch.
    fn detect_flush_targets(&mut self, py: Python) {
        let all_nodes = self.collect_all_nodes(py);
        for node_py in &all_nodes {
            let mut node = node_py.borrow_mut(py);
            if let NodeLogic::Sink { ref func } = node.logic {
                if let Some(target) = detect_flush_target_from_func(py, func) {
                    node.flush_target = Some(target);
                }
            }
        }
    }

    /// Detect Sink nodes whose function calls .emit() on a Source node in the same graph.
    /// Sets `emit_target` on those Sink nodes so execution can bypass Python dispatch.
    fn detect_emit_targets(&mut self, py: Python) {
        let all_nodes = self.collect_all_nodes(py);
        for node_py in &all_nodes {
            let mut node = node_py.borrow_mut(py);
            if let NodeLogic::Sink { ref func } = node.logic {
                if let Some(target) = detect_emit_target_from_func(py, func) {
                    node.emit_target = Some(target);
                }
            }
        }
    }

    /// Fuse `collect() + filter(pred).sink(flush)` patterns into a single `ChunkBy` node.
    ///
    /// Walks the graph to find Sink nodes that call `.flush()` on a Collect node.
    /// Then traces back to find the Filter providing the predicate and the branch point
    /// where both paths originate. Extracts any intermediate transforms between the
    /// branch point and the Collect into `pre_buffer_chain`.
    fn fuse_chunk_by(&mut self, py: Python, source_py_ptr: usize) {
        let all_nodes = self.collect_all_nodes(py);

        let source_ptr = source_py_ptr;

        let mut flush_pairs: Vec<(Py<Self>, Py<Self>)> = Vec::new();
        for node_py in &all_nodes {
            let node = node_py.borrow(py);
            if let NodeLogic::Sink { ref func } = node.logic {
                if let Some(target) = detect_flush_target_from_func(py, func) {
                    drop(node);
                    flush_pairs.push((node_py.clone_ref(py), target));
                }
            }
        }
        if flush_pairs.is_empty() {
        }

        // Phase 2: For each pair, try to fuse
        for (sink_py, collect_py) in flush_pairs {
            // Find Filter: walk up from Sink through single-parent linear path
            let filter_py = {
                let mut cur = sink_py.clone_ref(py);
                let mut found_filter: Option<Py<Self>> = None;
                loop {
                    let node = cur.borrow(py);
                    let parents = node.parents.lock().unwrap();
                    let parent_count = parents.len();
                    let single_parent = parents.first().map(|p| p.clone_ref(py));
                    drop(parents);
                    drop(node);

                    // Continue only if node has exactly one parent
                    if parent_count != 1 {
                        break;
                    }

                    let parent_py = single_parent.unwrap();

                    // Check if parent is source (node is direct child of source)
                    let is_source_child = parent_py.as_ptr() as usize == source_ptr;
                    if is_source_child {
                        break;
                    }

                    let parent = parent_py.borrow(py);
                    if matches!(parent.logic, NodeLogic::Filter { .. }) {
                        drop(parent);
                        found_filter = Some(parent_py);
                        break;
                    }
                    drop(parent);
                    cur = parent_py;
                }
                match found_filter {
                    Some(f) => f,
                    None => continue,
                }
            };

            // Find the branch point: the Filter's parent that is also an ancestor of Collect
            // Constraint: Filter must be a direct downstream of branch point
            let branch_point_py: Option<Py<Self>>;
            let branch_is_source: bool;
            {
                let filter_node = filter_py.borrow(py);
                let filter_parents = filter_node.parents.lock().unwrap();
                let filter_parent_count = filter_parents.len();
                let filter_single_parent = filter_parents.first().map(|p| p.clone_ref(py));
                drop(filter_parents);

                if filter_parent_count != 1 {
                    drop(filter_node);
                    continue;
                }

                let is_source_child = filter_single_parent.as_ref()
                    .map(|p| p.as_ptr() as usize == source_ptr)
                    .unwrap_or(false);
                branch_is_source = is_source_child;
                branch_point_py = if is_source_child {
                    None
                } else {
                    filter_single_parent
                };
                drop(filter_node);
            };

            // Find buffer path: from branch point to Collect
            let branch_downstreams = if branch_is_source {
                self.downstreams
                    .iter()
                    .map(|d| d.clone_ref(py))
                    .collect::<Vec<_>>()
            } else {
                let bp = branch_point_py.as_ref().unwrap().borrow(py);
                let ds = bp
                    .downstreams
                    .iter()
                    .map(|d| d.clone_ref(py))
                    .collect::<Vec<_>>();
                drop(bp);
                ds
            };

            let collect_ptr = collect_py.as_ptr() as usize;
            let filter_ptr = filter_py.as_ptr() as usize;

            let mut buffer_path: Option<Vec<Py<Self>>> = None;
            for start in &branch_downstreams {
                if start.as_ptr() as usize == filter_ptr {
                    continue;
                }
                let mut path = vec![start.clone_ref(py)];
                loop {
                    let cur = path.last().unwrap();
                    if cur.as_ptr() as usize == collect_ptr {
                        buffer_path = Some(path);
                        break;
                    }
                    let cur_node = cur.borrow(py);
                    if !cur_node.logic.is_chainable() || cur_node.downstreams.len() != 1 {
                        drop(cur_node);
                        break;
                    }
                    let next = cur_node.downstreams[0].clone_ref(py);
                    drop(cur_node);
                    path.push(next);
                }
                if buffer_path.is_some() {
                    break;
                }
            }

            let buffer_path = match buffer_path {
                Some(p) => p,
                None => continue,
            };

            // Verify all intermediate nodes in buffer_path (excluding Collect at end)
            let mut valid = true;
            for node_py_ref in &buffer_path[..buffer_path.len().saturating_sub(1)] {
                let node = node_py_ref.borrow(py);
                if node.downstreams.len() != 1 || !node.logic.is_chainable() {
                    valid = false;
                    break;
                }
                drop(node);
                // Check single parent
                let node = node_py_ref.borrow(py);
                let parents = node.parents.lock().unwrap();
                if parents.len() != 1 {
                    valid = false;
                    break;
                }
                drop(parents);
                drop(node);
            }
            if !valid {
                continue;
            }

            // Extract pre_buffer_chain from intermediate nodes
            let mut pre_buffer_chain: Vec<ChainNode> = Vec::new();
            for node_py_ref in &buffer_path[..buffer_path.len().saturating_sub(1)] {
                let mut node = node_py_ref.borrow_mut(py);
                let node_id = std::ptr::from_ref(&*node) as usize;
                let logic = std::mem::replace(&mut node.logic, NodeLogic::ChainMember);
                let name = std::mem::take(&mut node.name);
                let source_location = std::mem::take(&mut node.source_location);
                let builtin_op = node.builtin_op.take();
                let cached_vectorcall = node.cached_vectorcall.take();
                let flush_target = node.flush_target.take();
                let emit_target = node.emit_target.take();
                node.downstreams.clear();
                pre_buffer_chain.push(ChainNode {
                    logic,
                    name,
                    source_location,
                    builtin_op,
                    node_id,
                    cached_vectorcall,
                    flush_target,
                    emit_target,
                });
            }

            // Extract predicate from Filter
            let predicate = {
                let mut filter_node = filter_py.borrow_mut(py);
                let pred = match std::mem::replace(&mut filter_node.logic, NodeLogic::ChainMember) {
                    NodeLogic::Filter { predicate } => predicate,
                    _ => unreachable!(),
                };
                filter_node.downstreams.clear();
                pred
            };

            // Get Collect's downstreams
            let collect_downstreams = {
                let collect_node = collect_py.borrow(py);
                collect_node
                    .downstreams
                    .iter()
                    .map(|d| d.clone_ref(py))
                    .collect::<Vec<_>>()
            };

            let (filter_name, filter_source) = {
                let f = filter_py.borrow(py);
                (f.name.clone(), f.source_location.clone())
            };

            let chunk_node_py = if buffer_path.len() == 1 {
                collect_py.clone_ref(py)
            } else {
                buffer_path[0].clone_ref(py)
            };

            {
                let mut chunk_node = chunk_node_py.borrow_mut(py);
                chunk_node.logic = NodeLogic::ChunkBy {
                    buffer: Vec::new(),
                    predicate,
                    include_delimiter: true,
                    pre_buffer_chain,
                };
                chunk_node.name = format!("chunk_by[fused]({})", filter_name);
                chunk_node.source_location = filter_source;
                chunk_node.downstreams = collect_downstreams;
            }

            // If buffer_path had intermediate nodes, mark the Collect as orphaned
            if buffer_path.len() > 1 {
                let mut collect_node = collect_py.borrow_mut(py);
                collect_node.logic = NodeLogic::ChainMember;
                collect_node.downstreams.clear();
            }

            // Remove Filter from branch point's downstreams
            if branch_is_source {
                self.downstreams
                    .retain(|d| d.as_ptr() as usize != filter_ptr);
            } else {
                let bp = branch_point_py.as_ref().unwrap();
                let mut bp_node = bp.borrow_mut(py);
                bp_node
                    .downstreams
                    .retain(|d| d.as_ptr() as usize != filter_ptr);
            }
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
            if let Ok(b) = $getter() {
                if func.is(b) {
                    return Some($op);
                }
            }
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
    if get_operator_itemgetter()
        .is_ok_and(|ig| func.bind(py).is_instance(ig.bind(py)).unwrap_or(false))
    {
        let key = func
            .call_method0(py, "__reduce__")
            .ok()
            .and_then(|r| r.bind(py).get_item(1).ok().map(|a| a.unbind()))
            .and_then(|a| a.bind(py).cast::<PyTuple>().ok().map(|t| t.to_owned()))
            .filter(|t| t.len() == 1)
            .and_then(|t| t.get_item(0).ok().map(|k| k.unbind()));
        if let Some(k) = key {
            return Some(BuiltinOp::ItemGetter(k));
        }
    }
    // Detect operator.attrgetter instances (e.g., operator.attrgetter("name"))
    if get_operator_attrgetter()
        .is_ok_and(|ag| func.bind(py).is_instance(ag.bind(py)).unwrap_or(false))
    {
        let name = func
            .call_method0(py, "__reduce__")
            .ok()
            .and_then(|r| r.bind(py).get_item(1).ok().map(|a| a.unbind()))
            .and_then(|a| a.bind(py).cast::<PyTuple>().ok().map(|t| t.to_owned()))
            .filter(|t| t.len() == 1)
            .and_then(|t| {
                t.get_item(0)
                    .ok()?
                    .cast_into::<pyo3::types::PyString>()
                    .ok()
            });
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
    if get_builtin_print().is_ok_and(|b| func.is(b)) {
        return Some(BuiltinOp::Print);
    }
    None
}

/// Detect the appropriate builtin fast-path for a given NodeLogic variant.
///
/// For Map nodes, also handles `("unwrap", callable)` — replacing the func in-place.
fn detect_builtin_for_logic(py: Python, logic: &mut NodeLogic) -> Option<BuiltinOp> {
    match logic {
        NodeLogic::Map { func } => detect_map_builtin_with_unwrap(py, func),
        NodeLogic::Filter { predicate } => detect_filter_builtin(py, predicate),
        NodeLogic::ChunkBy { predicate, .. } => detect_filter_builtin(py, predicate),
        NodeLogic::Sink { func } => detect_sink_builtin(func),
        _ => None,
    }
}

/// Detect if a Sink's function calls .flush() on a Stream (Collect node).
///
/// Handles two forms:
/// - Bound method: `collected.flush` — check `__self__` is Stream, `__func__.__name__` is "flush"
/// - Lambda: `lambda _: collected.flush()` — bytecode analysis returns `("call_method0", obj, "flush")`
fn detect_flush_target_from_func(py: Python, func: &Py<PyAny>) -> Option<Py<Stream>> {
    // Method A: Bound method check (e.g., .sink(collected.flush))
    let bound = func.bind(py);
    if let (Ok(self_obj), Ok(func_obj)) = (bound.getattr("__self__"), bound.getattr("__func__")) {
        if let Ok(name) = func_obj.getattr("__name__") {
            if let Ok(name_str) = name.extract::<String>() {
                if name_str == "flush" && self_obj.is_instance_of::<Stream>() {
                    if let Ok(target) = self_obj.extract::<Py<Stream>>() {
                        // Verify target is a Collect node
                        let target_node = target.borrow(py);
                        if matches!(target_node.logic, NodeLogic::Collect { .. }) {
                            drop(target_node);
                            return Some(target);
                        }
                    }
                }
            }
        }
    }

    // Method B: Lambda bytecode analysis (e.g., .sink(lambda _: collected.flush()))
    let analyzer = get_analyze_lambda().ok()?;
    let result = analyzer.call1(py, (func,)).ok()?;
    if result.is_none(py) {
        return None;
    }
    let bound_result = result.bind(py);
    let tuple = bound_result.cast::<PyTuple>().ok()?;
    if tuple.len() != 3 {
        return None;
    }
    let tag: String = tuple.get_item(0).ok()?.extract().ok()?;
    if tag != "call_method0" {
        return None;
    }
    let method_name: String = tuple.get_item(2).ok()?.extract().ok()?;
    if method_name != "flush" {
        return None;
    }
    let obj = tuple.get_item(1).ok()?;
    if !obj.is_instance_of::<Stream>() {
        return None;
    }
    let target: Py<Stream> = obj.extract().ok()?;
    // Verify target is a Collect node
    let target_node = target.borrow(py);
    if matches!(target_node.logic, NodeLogic::Collect { .. }) {
        drop(target_node);
        return Some(target);
    }
    None
}

/// Detect if a Sink's function calls .emit() on a Stream (Source node).
///
/// Handles two forms:
/// - Bound method: `source.emit` — check `__self__` is Stream, `__func__.__name__` is "emit"
/// - Lambda: `lambda x: source.emit(x)` — bytecode analysis returns `("call_method1", obj, "emit")`
fn detect_emit_target_from_func(py: Python, func: &Py<PyAny>) -> Option<Py<Stream>> {
    // Method A: Bound method check (e.g., .sink(source.emit))
    let bound = func.bind(py);
    if let (Ok(self_obj), Ok(func_obj)) = (bound.getattr("__self__"), bound.getattr("__func__")) {
        if let Ok(name) = func_obj.getattr("__name__") {
            if let Ok(name_str) = name.extract::<String>() {
                if name_str == "emit" && self_obj.is_instance_of::<Stream>() {
                    if let Ok(target) = self_obj.extract::<Py<Stream>>() {
                        // Verify target is a Source node
                        let target_node = target.borrow(py);
                        if matches!(target_node.logic, NodeLogic::Source) {
                            drop(target_node);
                            return Some(target);
                        }
                    }
                }
            }
        }
    }

    // Method B: Lambda bytecode analysis (e.g., .sink(lambda x: source.emit(x)))
    let analyzer = get_analyze_lambda().ok()?;
    let result = analyzer.call1(py, (func,)).ok()?;
    if result.is_none(py) {
        return None;
    }
    let bound_result = result.bind(py);
    let tuple = bound_result.cast::<PyTuple>().ok()?;
    if tuple.len() != 3 {
        return None;
    }
    let tag: String = tuple.get_item(0).ok()?.extract().ok()?;
    if tag != "call_method1" {
        return None;
    }
    let method_name: String = tuple.get_item(2).ok()?.extract().ok()?;
    if method_name != "emit" {
        return None;
    }
    let obj = tuple.get_item(1).ok()?;
    if !obj.is_instance_of::<Stream>() {
        return None;
    }
    let target: Py<Stream> = obj.extract().ok()?;
    // Verify target is a Source node
    let target_node = target.borrow(py);
    if matches!(target_node.logic, NodeLogic::Source) {
        drop(target_node);
        return Some(target);
    }
    None
}

/// Extract the vectorcall function pointer from a Python callable.
///
/// Returns `None` if the callable doesn't support vectorcall.
/// The pointer is valid as long as the callable object is alive.
unsafe fn extract_vectorcall(
    callable: *mut pyo3::ffi::PyObject,
) -> Option<pyo3::ffi::vectorcallfunc> {
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
fn extract_vectorcall_for_logic(
    _py: Python,
    logic: &NodeLogic,
) -> Option<pyo3::ffi::vectorcallfunc> {
    let callable_ptr = match logic {
        NodeLogic::Map { func } => func.as_ptr(),
        NodeLogic::Filter { predicate } => predicate.as_ptr(),
        NodeLogic::ChunkBy { predicate, .. } => predicate.as_ptr(),
        NodeLogic::Sink { func } => func.as_ptr(),
        NodeLogic::Accumulate { func, .. } => func.as_ptr(),
        NodeLogic::BatchMap { func } => func.as_ptr(),
        _ => return None,
    };
    unsafe { extract_vectorcall(callable_ptr) }
}
