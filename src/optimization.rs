//! Topology optimization (fusion, prefetch marking, chain compilation)

use pyo3::prelude::*;
use std::collections::{HashMap, HashSet};

use crate::builtin::{
    cache_secondary_vectorcalls, detect_builtin_for_logic, detect_emit_target_from_func,
    detect_flush_target_from_func, extract_vectorcall_for_logic,
};
use crate::node::{ChainNode, NodeLogic};
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

        // Phase 3: Mark prefetch nodes inside par branches as needing locks
        // These nodes use internal threading which can conflict with par's threading
        self.mark_par_branch_nodes(py, &all_nodes);
    }

    /// Full optimization pass, safe because graph is being frozen via compile().
    ///
    /// This enables all optimizations including chain compilation, which is only safe
    /// when the topology is guaranteed not to change after optimization.
    pub fn optimize_topology_full(&mut self, py: Python, source_py_ptr: usize) -> Vec<Py<Self>> {
        self.eliminate_source_passthroughs(py);

        let all_nodes = self.collect_all_nodes(py);

        self.fuse_chunk_by(py, source_py_ptr, &all_nodes);
        self.detect_and_cache_all(py, &all_nodes);

        self.build_compiled_chains(py);

        Self::warn_useless_par(py, &all_nodes);
        self.mark_par_branch_nodes(py, &all_nodes);
        self.mark_convergence_nodes(py, &all_nodes);

        all_nodes
    }

    /// Collect all nodes reachable from this node's downstreams.
    fn collect_all_nodes(&self, py: Python) -> Vec<Py<Self>> {
        let mut all_nodes: Vec<Py<Self>> = Vec::new();
        let mut visited: HashSet<usize> = HashSet::new();
        let mut to_visit: Vec<Py<Self>> =
            self.downstreams.iter().map(|d| d.clone_ref(py)).collect();

        while let Some(node_py) = to_visit.pop() {
            let node_ptr = node_py.as_ptr() as usize;
            if !visited.insert(node_ptr) {
                continue;
            }
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
    pub fn freeze_all(&mut self, py: Python, all_nodes: &[Py<Self>]) {
        self.frozen = true;
        self.compiled = true;
        self.compiled_chainable = (self.logic.is_chainable()
            && !matches!(&self.logic, NodeLogic::Filter { .. }))
            || matches!(&self.logic, NodeLogic::Chain { .. });
        self.compiled_filter_fast = matches!(&self.logic, NodeLogic::Filter { .. })
            && self.builtin_op.is_some();
        self.compiled_downstream_ptrs = self
            .downstreams
            .iter()
            .map(|d| {
                let mut guard = d.borrow_mut(py);
                StreamPtr(&mut *guard as *mut Stream)
            })
            .collect();

        for node_py in all_nodes {
            let mut node = node_py.borrow_mut(py);
            if !node.frozen {
                node.frozen = true;
                node.compiled = true;
                node.compiled_chainable = (node.logic.is_chainable()
                    && !matches!(&node.logic, NodeLogic::Filter { .. }))
                    || matches!(&node.logic, NodeLogic::Chain { .. });
                node.compiled_filter_fast = matches!(&node.logic, NodeLogic::Filter { .. })
                    && node.builtin_op.is_some();
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

    fn mark_convergence_nodes(&self, _py: Python, _all_nodes: &[Py<Self>]) {
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

    /// Single-pass detection of builtins, flush/emit targets, and vectorcall caching.
    ///
    /// Merges what was previously four separate loops over `all_nodes` into one,
    /// reducing borrow_mut/drop overhead from 4N to N.
    ///
    /// Order within the loop body matters:
    /// - `detect_builtin_for_logic` must run before `extract_vectorcall_for_logic` because
    ///   the "unwrap" path can replace a lambda with a bare callable, and we want the
    ///   vectorcall pointer to come from the resolved callable.
    /// - flush_target and emit_target are independent of builtin_op (they match Sink
    ///   nodes exclusively, while builtins also match Map/Filter/ChunkBy).
    fn detect_and_cache_all(&mut self, py: Python, all_nodes: &[Py<Self>]) {
        // Cache vectorcall for the source node itself
        // (source isn't in all_nodes since collect_all_nodes starts from downstreams)
        self.cached_vectorcall = extract_vectorcall_for_logic(py, &self.logic);

        for node_py in all_nodes {
            let mut node = node_py.borrow_mut(py);

            // 1. Detect builtin fast-path (may mutate logic via "unwrap" on Map nodes)
            node.builtin_op = detect_builtin_for_logic(py, &mut node.logic);

            // 2. Detect flush targets (Sink nodes calling .flush() on Collect nodes)
            if let NodeLogic::Sink { ref func } = node.logic
                && let Some(target) = detect_flush_target_from_func(py, func)
            {
                node.flush_target = Some(target);
            }

            // 3. Detect emit targets (Sink nodes calling .emit() on Source nodes)
            if let NodeLogic::Sink { ref func } = node.logic
                && let Some(target) = detect_emit_target_from_func(py, func)
            {
                node.emit_target = Some(target);
            }

            // 4. Cache vectorcall pointer (must come after builtin detection for unwrap)
            node.cached_vectorcall = extract_vectorcall_for_logic(py, &node.logic);
            cache_secondary_vectorcalls(&mut node.logic);
        }
    }

    /// Fuse `collect() + filter(pred).sink(flush)` patterns into a single `ChunkBy` node.
    ///
    /// Walks the graph to find Sink nodes that call `.flush()` on a Collect node.
    /// Then traces back to find the Filter providing the predicate and the branch point
    /// where both paths originate. Extracts any intermediate transforms between the
    /// branch point and the Collect into `pre_buffer_chain`.
    fn fuse_chunk_by(&mut self, py: Python, source_py_ptr: usize, all_nodes: &[Py<Self>]) {
        let source_ptr = source_py_ptr;

        let mut flush_pairs: Vec<(Py<Self>, Py<Self>)> = Vec::new();
        for node_py in all_nodes {
            let node = node_py.borrow(py);
            if let NodeLogic::Sink { ref func } = node.logic
                && let Some(target) = detect_flush_target_from_func(py, func)
            {
                drop(node);
                flush_pairs.push((node_py.clone_ref(py), target));
            }
        }
        if flush_pairs.is_empty() {
            return;
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

}
