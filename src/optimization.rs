//! Topology optimization (fusion, prefetch marking, chain compilation)

use crate::helpers::{PtrMap as HashMap, PtrSet as HashSet};
use pyo3::prelude::*;
use std::sync::Arc;

use crate::builtin::{
    cache_secondary_vectorcalls, detect_builtin_for_logic, detect_emit_target_from_func,
    detect_flush_target_from_func, extract_vectorcall_for_logic,
};
use crate::lock::Downstream;
use crate::node::{BuiltinOp, ChainNode, ChunkByState, NodeLogic};
use crate::{Stream, StreamPtr};

impl Stream {
    /// Full optimization pass, safe because graph is being frozen via compile().
    ///
    /// This enables all optimizations including chain compilation, which is only safe
    /// when the topology is guaranteed not to change after optimization.
    pub fn optimize_topology_full(&mut self, py: Python, source_py_ptr: usize) -> Vec<Py<Self>> {
        // Before fusion, so the nodes it absorbs get the shared lock too.
        self.share_graph_lock(py, source_py_ptr);
        self.eliminate_source_passthroughs(py);

        let all_nodes = self.collect_all_nodes(py);

        self.fuse_chunk_by(py, source_py_ptr, &all_nodes);
        self.detect_and_cache_all(py, &all_nodes);

        self.build_compiled_chains(py);

        Self::warn_useless_par(py, &all_nodes);
        self.mark_par_branch_nodes(py, &all_nodes);

        all_nodes
    }

    /// Give every node of this connected graph one lock, when that is
    /// provably safe, and drop the per-edge locks it makes redundant.
    ///
    /// Every way into the graph (emit/emit_batch on any source, flush on any
    /// node) takes the lock of the node it starts at. Once all nodes share one
    /// lock, two threads can't be in the graph at once, so the `needs_lock`
    /// edges that guard merges of several sources (union, zip, combine_latest)
    /// lock nothing new: they are cleared, and the compiled raw-pointer path
    /// applies to them. Skipped when the graph has a par() node (its workers
    /// update branches while the emitting thread holds the lock), when any
    /// node is already frozen (another thread may be inside it through raw
    /// pointers), or when any node is borrowed right now (in use). The GIL is
    /// held throughout and no Python code runs, so nothing starts meanwhile.
    fn share_graph_lock(&mut self, py: Python, source_py_ptr: usize) {
        if matches!(self.logic, NodeLogic::Parallel) || self.frozen {
            return;
        }
        let mut seen = HashSet::from_iter([source_py_ptr]);
        let mut nodes: Vec<Py<Self>> = Vec::new();
        let mut stack: Vec<Py<Self>> = self
            .downstreams
            .iter()
            .map(|d| d.node.clone_ref(py))
            .collect();
        stack.extend(self.parents.lock().unwrap().iter().map(|p| p.clone_ref(py)));
        while let Some(node_py) = stack.pop() {
            if !seen.insert(node_py.as_ptr() as usize) {
                continue;
            }
            let Ok(node) = node_py.try_borrow_mut(py) else {
                return; // in use by another thread (or re-entrantly)
            };
            if node.frozen || matches!(node.logic, NodeLogic::Parallel) {
                return;
            }
            stack.extend(node.downstreams.iter().map(|d| d.node.clone_ref(py)));
            stack.extend(node.parents.lock().unwrap().iter().map(|p| p.clone_ref(py)));
            drop(node);
            nodes.push(node_py);
        }
        let lock = Arc::clone(&self.lock);
        self.needs_lock = false;
        for d in &mut self.downstreams {
            d.lock = Arc::clone(&lock);
        }
        for node_py in nodes {
            let mut node = node_py.borrow_mut(py);
            node.lock = Arc::clone(&lock);
            node.needs_lock = false;
            for d in &mut node.downstreams {
                d.lock = Arc::clone(&lock);
            }
        }
    }

    /// Mark branch points whose branches compile() can prove independent, so
    /// emit_batch can give each branch the whole batch instead of passing
    /// items through the branches one at a time.
    ///
    /// Branches are independent when every node in them is a native step (a
    /// recognised map/filter/accumulate operation, or a sink appending to a
    /// list): no Python function runs, so the only effects are the appends,
    /// and each list is appended to by one branch only. No node may have a
    /// second parent, a flush/emit shortcut, or a lock. The order of work
    /// across branches is then unobservable, except through the data's own
    /// dunder methods, or which items a failing batch had processed.
    pub(crate) fn mark_independent_fanouts(&mut self, py: Python, all_nodes: &[Py<Self>]) {
        self.batch_fanout = !self.needs_lock && independent_branches(py, &self.downstreams);
        for node_py in all_nodes {
            let Ok(node) = node_py.try_borrow(py) else {
                continue;
            };
            let independent = !node.needs_lock && independent_branches(py, &node.downstreams);
            drop(node);
            if let Ok(mut node) = node_py.try_borrow_mut(py) {
                node.batch_fanout = independent;
            }
        }
    }

    /// Collect all nodes reachable from this node's downstreams.
    fn collect_all_nodes(&self, py: Python) -> Vec<Py<Self>> {
        let mut all_nodes: Vec<Py<Self>> = Vec::new();
        let mut visited: HashSet<usize> = HashSet::default();
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
        self.compiled_filter_fast =
            matches!(&self.logic, NodeLogic::Filter { .. }) && self.builtin_op.is_some();
        for d in &mut self.downstreams {
            d.ptr = Some(StreamPtr::new(&mut d.node.borrow_mut(py)));
        }

        for node_py in all_nodes {
            let mut node = node_py.borrow_mut(py);
            if !node.frozen {
                node.frozen = true;
                node.compiled = true;
                node.compiled_chainable = (node.logic.is_chainable()
                    && !matches!(&node.logic, NodeLogic::Filter { .. }))
                    || matches!(&node.logic, NodeLogic::Chain { .. });
                node.compiled_filter_fast =
                    matches!(&node.logic, NodeLogic::Filter { .. }) && node.builtin_op.is_some();
                let downstream_refs: Vec<Py<Stream>> =
                    node.downstreams.iter().map(|d| d.clone_ref(py)).collect();
                drop(node);
                let ptrs: Vec<StreamPtr> = downstream_refs
                    .iter()
                    .map(|d| StreamPtr::new(&mut d.borrow_mut(py)))
                    .collect();
                let mut node = node_py.borrow_mut(py);
                for (d, ptr) in node.downstreams.iter_mut().zip(ptrs) {
                    d.ptr = Some(ptr);
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
            let mut node_branches: HashMap<usize, HashSet<usize>> = HashMap::default();
            let mut node_refs: HashMap<usize, Py<Self>> = HashMap::default();

            for (branch_idx, branch_py) in branches.iter().enumerate() {
                let mut visited: HashSet<usize> = HashSet::default();
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
            let new_downstreams: Vec<Downstream> =
                child.downstreams.iter().map(|d| d.clone_edge(py)).collect();
            drop(child);
            self.downstreams = new_downstreams.into();
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
        let tail_downstreams: Vec<Downstream> =
            last.downstreams.iter().map(|d| d.clone_edge(py)).collect();
        drop(last);

        // Extract logic from each node into ChainNode structs
        let mut steps: Vec<ChainNode> = Vec::new();

        for node_py in &chain_node_refs {
            let mut node = node_py.borrow_mut(py);
            let node_id = std::ptr::from_ref(&*node) as usize;
            let was_sink = matches!(node.logic, NodeLogic::Sink { .. });
            let logic = std::mem::replace(&mut node.logic, NodeLogic::ChainMember { was_sink });
            // Members keep their name and downstreams: data never reaches them
            // (the head propagates straight to the tail's downstreams), but a
            // flush() called on a member must still reach what lies below it.
            let name = node.name.clone();
            let source_location = node.source_location.clone();
            let builtin_op = node.builtin_op.take();
            let cached_vectorcall = node.cached_vectorcall.take();
            let flush_target = node.flush_target.take();
            let emit_target = node.emit_target.take();
            steps.push(ChainNode {
                logic,
                name,
                source_location,
                builtin_op,
                node_id,
                cached_vectorcall,
                flush_target,
                emit_target,
                numeric_run: None,
            });
        }
        annotate_numeric_runs(py, &mut steps);

        // Set head node's logic to Chain, downstreams to tail's downstreams,
        // and name it after its steps.
        let mut head = start_py.borrow_mut(py);
        head.name = steps
            .iter()
            .map(|step| &*step.name)
            .collect::<Vec<_>>()
            .join(" -> ")
            .into();
        head.logic = NodeLogic::Chain { steps };
        head.downstreams = tail_downstreams.iter().map(|d| d.clone_edge(py)).collect();
        drop(head);

        // The nodes after the chain now hang off the head. Their parent link
        // to the last member was the only thing keeping the members alive, so
        // point it at the head: members nobody else holds are then freed.
        // (Members the user holds keep their downstreams for flush().)
        let tail_py = chain_node_refs.last().expect("chain has members");
        for ds in &tail_downstreams {
            let child = ds.node.borrow(py);
            let mut parents = child.parents.lock().unwrap();
            for parent in parents.iter_mut() {
                if parent.is(tail_py) {
                    *parent = start_py.clone_ref(py);
                }
            }
        }

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

            // 1. Detect builtin fast-path (may mutate logic via "unwrap" on Map nodes).
            // A current eager op is what the analysis would find again: it
            // depends only on the code.
            node.builtin_op = match node.builtin_op.take().as_deref() {
                Some(BuiltinOp::Eager(eager)) if eager.current() => Some(Arc::clone(&eager.op)),
                _ => detect_builtin_for_logic(py, &mut node.logic),
            };

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

                let is_source_child = filter_single_parent
                    .as_ref()
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
                let was_sink = matches!(node.logic, NodeLogic::Sink { .. });
                let logic = std::mem::replace(&mut node.logic, NodeLogic::ChainMember { was_sink });
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
                    numeric_run: None,
                });
            }

            // Extract predicate from Filter
            let predicate = {
                let mut filter_node = filter_py.borrow_mut(py);
                let pred = match std::mem::replace(
                    &mut filter_node.logic,
                    NodeLogic::ChainMember { was_sink: false },
                ) {
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
                    .map(|d| d.clone_edge(py))
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
                chunk_node.logic = NodeLogic::ChunkBy(Box::new(ChunkByState {
                    buffer: Vec::new(),
                    predicate,
                    include_delimiter: true,
                    pre_buffer_chain,
                }));
                chunk_node.name = format!("chunk_by[fused]({})", filter_name).into();
                chunk_node.source_location = filter_source;
                chunk_node.downstreams = collect_downstreams.into();
            }

            // If buffer_path had intermediate nodes, mark the Collect as orphaned
            if buffer_path.len() > 1 {
                let mut collect_node = collect_py.borrow_mut(py);
                collect_node.logic = NodeLogic::ChainMember { was_sink: false };
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

/// See `Stream::mark_independent_fanouts`.
fn independent_branches(py: Python, edges: &[Downstream]) -> bool {
    let mut lists = HashSet::default();
    edges.len() >= 2
        && edges
            .iter()
            .all(|d| native_subtree(py, &d.node, &mut lists))
}

fn native_subtree(py: Python, node_py: &Py<Stream>, lists: &mut HashSet<usize>) -> bool {
    let Ok(node) = node_py.try_borrow(py) else {
        return false;
    };
    if node.needs_lock
        || node.flush_target.is_some()
        || node.emit_target.is_some()
        || node.parents.lock().unwrap().len() != 1
    {
        return false;
    }
    let native = match &node.logic {
        NodeLogic::Chain { steps } => steps.iter().all(|step| {
            step.flush_target.is_none()
                && step.emit_target.is_none()
                && native_step(&step.logic, step.builtin_op.as_deref(), lists)
        }),
        logic => native_step(logic, node.builtin_op.as_deref(), lists),
    };
    native
        && node
            .downstreams
            .iter()
            .all(|d| native_subtree(py, &d.node, lists))
}

/// A step compile() turned into a native operation; sinks must append to a
/// list no other sink in these branches appends to.
fn native_step(logic: &NodeLogic, op: Option<&BuiltinOp>, lists: &mut HashSet<usize>) -> bool {
    use BuiltinOp::{AccumBinOp, AccumBinOpConst, AccumMax, AccumMin, ListAppend, Print};
    match (logic, op) {
        (NodeLogic::Map { .. } | NodeLogic::Filter { .. }, Some(op)) => !matches!(
            op,
            Print | ListAppend(_) | AccumBinOp(_) | AccumBinOpConst(..) | AccumMax | AccumMin
        ),
        (
            NodeLogic::Accumulate {
                returns_state: false,
                ..
            },
            Some(AccumBinOp(_) | AccumBinOpConst(..) | AccumMax | AccumMin),
        ) => true,
        (NodeLogic::Sink { .. }, Some(ListAppend(list))) => lists.insert(list.as_ptr() as usize),
        _ => false,
    }
}

/// Mark runs of consecutive numeric map/filter steps in a fused chain, so they
/// execute together on unboxed values.
fn annotate_numeric_runs(py: Python, steps: &mut [ChainNode]) {
    let mut i = 0;
    while i < steps.len() {
        let candidates = steps[i..].iter().map_while(|step| {
            if step.flush_target.is_some() || step.emit_target.is_some() {
                return None;
            }
            let is_filter = match step.logic {
                NodeLogic::Map { .. } => false,
                NodeLogic::Filter { .. } => true,
                _ => return None,
            };
            Some((is_filter, step.builtin_op.as_ref()?))
        });
        match crate::numeric::NumRun::shared(py, candidates) {
            Some(run) => {
                let len = run.len();
                steps[i].numeric_run = Some(run);
                i += len;
            }
            None => i += 1,
        }
    }
}
