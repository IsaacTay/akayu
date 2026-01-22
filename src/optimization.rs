//! Topology optimization (fusion, prefetch marking)

use pyo3::prelude::*;
use std::collections::{HashMap, HashSet};

use crate::helpers::{get_compose_batch_maps, get_compose_filters, get_compose_maps};
use crate::node::NodeLogic;
use crate::Stream;

impl Stream {
    /// Optimize the topology on first emit.
    ///
    /// This method walks the stream graph and:
    /// - Fuses consecutive Map operations into a single composed Map
    /// - Fuses consecutive Filter operations into a single composed Filter
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
    /// This enables all optimizations including chain fusion, which is only safe
    /// when the topology is guaranteed not to change after optimization.
    pub fn optimize_topology_full(&mut self, py: Python) {
        // Phase 1: Fuse consecutive maps and filters (NOW ENABLED)
        self.fuse_linear_chains(py);

        // Phase 2: Collect all nodes
        let all_nodes = self.collect_all_nodes(py);

        // Phase 3: Mark prefetch nodes inside par branches as needing locks
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

                    node_branches.entry(node_ptr).or_default().insert(branch_idx);
                    node_refs.entry(node_ptr).or_insert_with(|| node_py.clone_ref(py));

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

    /// Recursively fuse linear chains of Map or Filter nodes.
    pub(crate) fn fuse_linear_chains(&mut self, py: Python) {
        // Process each downstream
        for downstream in &self.downstreams {
            let mut child = downstream.borrow_mut(py);
            child.fuse_linear_chains(py);
        }

        // Try to fuse this node with its single downstream if applicable
        if self.downstreams.len() != 1 {
            return;
        }

        let child_py = &self.downstreams[0];
        let child = child_py.borrow_mut(py);

        // Check if we can fuse Map -> Map
        if let NodeLogic::Map { func: ref my_func } = self.logic
            && let NodeLogic::Map {
                func: ref child_func,
            } = child.logic
            && let Ok(compose) = get_compose_maps()
            && let Ok(composed) = compose.call1(py, (my_func, child_func))
        {
            // Clone child's downstreams before modifying self
            let new_downstreams: Vec<Py<Self>> =
                child.downstreams.iter().map(|d| d.clone_ref(py)).collect();
            let child_name = child.name.clone();
            let child_func_is_sync = child.func_is_sync;
            drop(child); // Release borrow before modifying self

            // Update this node's function to the composed one
            self.logic = NodeLogic::Map { func: composed };
            // Update name to show fusion
            self.name = format!("{}+{}", self.name, child_name);
            // Bypass child: point to child's downstreams
            self.downstreams = new_downstreams;
            // Inherit func_is_sync (both must be sync for composed to be sync)
            self.func_is_sync = self.func_is_sync && child_func_is_sync;
            return;
        }

        // Check if we can fuse Filter -> Filter
        if let NodeLogic::Filter {
            predicate: ref my_pred,
        } = self.logic
            && let NodeLogic::Filter {
                predicate: ref child_pred,
            } = child.logic
            && let Ok(compose) = get_compose_filters()
            && let Ok(composed) = compose.call1(py, (my_pred, child_pred))
        {
            // Clone child's downstreams before modifying self
            let new_downstreams: Vec<Py<Self>> =
                child.downstreams.iter().map(|d| d.clone_ref(py)).collect();
            let child_name = child.name.clone();
            let child_func_is_sync = child.func_is_sync;
            drop(child); // Release borrow before modifying self

            // Update this node's predicate to the composed one
            self.logic = NodeLogic::Filter {
                predicate: composed,
            };
            // Update name to show fusion
            self.name = format!("{}+{}", self.name, child_name);
            // Bypass child: point to child's downstreams
            self.downstreams = new_downstreams;
            // Inherit func_is_sync
            self.func_is_sync = self.func_is_sync && child_func_is_sync;
            return;
        }

        // Note: Prefetch -> Map and Prefetch -> Filter fusion is done eagerly
        // in map() and filter() when the parent is a Prefetch node.

        // Check if we can fuse BatchMap -> BatchMap
        if let NodeLogic::BatchMap { func: ref my_func } = self.logic
            && let NodeLogic::BatchMap {
                func: ref child_func,
            } = child.logic
            && let Ok(compose) = get_compose_batch_maps()
            && let Ok(composed) = compose.call1(py, (my_func, child_func))
        {
            // Clone child's downstreams before modifying self
            let new_downstreams: Vec<Py<Self>> =
                child.downstreams.iter().map(|d| d.clone_ref(py)).collect();
            let child_name = child.name.clone();
            let child_func_is_sync = child.func_is_sync;
            drop(child); // Release borrow before modifying self

            // Update this node's function to the composed one
            self.logic = NodeLogic::BatchMap { func: composed };
            // Update name to show fusion
            self.name = format!("{}+{}", self.name, child_name);
            // Bypass child: point to child's downstreams
            self.downstreams = new_downstreams;
            // Inherit func_is_sync (both must be sync for composed to be sync)
            self.func_is_sync = self.func_is_sync && child_func_is_sync;
            return;
        }

        // Check if we can fuse Filter -> Map into FilterMap
        // This runs AFTER existing fusions so we're fusing already-optimized chains
        if let NodeLogic::Filter {
            predicate: ref my_pred,
        } = self.logic
            && let NodeLogic::Map {
                func: ref child_func,
            } = child.logic
        {
            // Clone child's downstreams before modifying self
            let new_downstreams: Vec<Py<Self>> =
                child.downstreams.iter().map(|d| d.clone_ref(py)).collect();
            let child_name = child.name.clone();
            let child_func_is_sync = child.func_is_sync;
            let pred = my_pred.clone_ref(py);
            let func = child_func.clone_ref(py);
            drop(child); // Release borrow before modifying self

            // Create FilterMap node
            self.logic = NodeLogic::FilterMap {
                predicate: pred,
                func,
            };
            // Update name to show fusion
            self.name = format!("{}+{}", self.name, child_name);
            // Bypass child: point to child's downstreams
            self.downstreams = new_downstreams;
            // Inherit func_is_sync
            self.func_is_sync = self.func_is_sync && child_func_is_sync;
            return;
        }

        // Check if we can fuse FilterMap -> Map (extend existing FilterMap)
        if let NodeLogic::FilterMap {
            predicate: ref my_pred,
            func: ref my_func,
        } = self.logic
            && let NodeLogic::Map {
                func: ref child_func,
            } = child.logic
            && let Ok(compose) = get_compose_maps()
            && let Ok(composed) = compose.call1(py, (my_func, child_func))
        {
            // Clone child's downstreams before modifying self
            let new_downstreams: Vec<Py<Self>> =
                child.downstreams.iter().map(|d| d.clone_ref(py)).collect();
            let child_name = child.name.clone();
            let child_func_is_sync = child.func_is_sync;
            let pred = my_pred.clone_ref(py);
            drop(child); // Release borrow before modifying self

            // Update FilterMap with composed map function
            self.logic = NodeLogic::FilterMap {
                predicate: pred,
                func: composed,
            };
            // Update name to show fusion
            self.name = format!("{}+{}", self.name, child_name);
            // Bypass child: point to child's downstreams
            self.downstreams = new_downstreams;
            // Inherit func_is_sync
            self.func_is_sync = self.func_is_sync && child_func_is_sync;
            return;
        }

        // Passthrough elimination: bypass Source nodes (seq() nodes) that only forward
        // Pattern: any_node -> Source -> downstream => any_node -> downstream
        // This reduces graph traversal overhead
        if let NodeLogic::Source = child.logic {
            // Source nodes just pass through, so bypass them
            let new_downstreams: Vec<Py<Self>> =
                child.downstreams.iter().map(|d| d.clone_ref(py)).collect();
            drop(child); // Release borrow before modifying self

            // Bypass the Source node: point directly to its downstreams
            self.downstreams = new_downstreams;
            // Note: Don't return here - we may be able to apply more fusions
            // with the new downstream after eliminating the passthrough
            return;
        }

        // Check if we can fuse Map -> Sink into MapSink
        if let NodeLogic::Map { func: ref map_func } = self.logic
            && let NodeLogic::Sink {
                func: ref sink_func,
            } = child.logic
        {
            let map_f = map_func.clone_ref(py);
            let sink_f = sink_func.clone_ref(py);
            let child_name = child.name.clone();
            let child_func_is_sync = child.func_is_sync;
            drop(child);

            self.logic = NodeLogic::MapSink {
                map_func: map_f,
                sink_func: sink_f,
            };
            self.name = format!("{}+{}", self.name, child_name);
            self.downstreams = Vec::new(); // Terminal node
            self.func_is_sync = self.func_is_sync && child_func_is_sync;
            return;
        }

        // Check if we can fuse Filter -> Sink into FilterSink
        if let NodeLogic::Filter {
            predicate: ref pred,
        } = self.logic
            && let NodeLogic::Sink {
                func: ref sink_func,
            } = child.logic
        {
            let p = pred.clone_ref(py);
            let sink_f = sink_func.clone_ref(py);
            let child_name = child.name.clone();
            let child_func_is_sync = child.func_is_sync;
            drop(child);

            self.logic = NodeLogic::FilterSink {
                predicate: p,
                sink_func: sink_f,
            };
            self.name = format!("{}+{}", self.name, child_name);
            self.downstreams = Vec::new(); // Terminal node
            self.func_is_sync = self.func_is_sync && child_func_is_sync;
            return;
        }

        // Check if we can fuse FilterMap -> Sink into FilterMapSink
        if let NodeLogic::FilterMap {
            predicate: ref pred,
            func: ref map_func,
        } = self.logic
            && let NodeLogic::Sink {
                func: ref sink_func,
            } = child.logic
        {
            let p = pred.clone_ref(py);
            let map_f = map_func.clone_ref(py);
            let sink_f = sink_func.clone_ref(py);
            let child_name = child.name.clone();
            let child_func_is_sync = child.func_is_sync;
            drop(child);

            self.logic = NodeLogic::FilterMapSink {
                predicate: p,
                map_func: map_f,
                sink_func: sink_f,
            };
            self.name = format!("{}+{}", self.name, child_name);
            self.downstreams = Vec::new(); // Terminal node
            self.func_is_sync = self.func_is_sync && child_func_is_sync;
        }
    }
}
