//! Pipeline profiling infrastructure.
//!
//! Collects per-node timing and item flow data during pipeline execution.
//! Uses thread-local storage for zero-cost when profiling is disabled.

use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Write as FmtWrite;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use crate::Stream;
use crate::node::NodeLogic;

// ---------------------------------------------------------------------------
// Per-node profile data (accumulated in thread-local during execution)
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
pub struct NodeProfile {
    pub func_time: Duration,
    pub items_in: u64,
    pub items_out: u64,
}

// ---------------------------------------------------------------------------
// Thread-local profiling state
// ---------------------------------------------------------------------------

struct ProfileState {
    active: bool,
    nodes: HashMap<usize, NodeProfile>,
    /// Start of the current emit call (for per-emit wall time accumulation).
    emit_start: Option<Instant>,
    /// Accumulated wall time across all emit calls.
    wall_total: Duration,
    emit_count: u64,
}

impl Default for ProfileState {
    fn default() -> Self {
        Self {
            active: false,
            nodes: HashMap::new(),
            emit_start: None,
            wall_total: Duration::ZERO,
            emit_count: 0,
        }
    }
}

impl Drop for ProfileState {
    // A thread that exits mid-session (start_profiling() without
    // stop_profiling()) must not leave every other thread checking for one.
    fn drop(&mut self) {
        if self.active {
            ACTIVE_SESSIONS.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

thread_local! {
    static PROFILE: RefCell<ProfileState> = RefCell::new(ProfileState::default());
}

/// Number of threads with an active session. Every node update asks
/// `is_profiling()`; reading this first keeps the common no-profiling case to
/// one relaxed load instead of a thread-local lookup (a `__tls_get_addr` call
/// in a shared library).
static ACTIVE_SESSIONS: AtomicUsize = AtomicUsize::new(0);

// ---------------------------------------------------------------------------
// Public API for controlling profiling sessions
// ---------------------------------------------------------------------------

/// Fast-path check: is profiling currently active?
#[inline(always)]
pub fn is_profiling() -> bool {
    ACTIVE_SESSIONS.load(Ordering::Relaxed) != 0 && PROFILE.with(|p| p.borrow().active)
}

/// Start a new profiling session. Clears any previous data.
/// Panics (returns Err) if profiling is already active.
pub fn start_session() -> PyResult<()> {
    PROFILE.with(|p| {
        let mut state = p.borrow_mut();
        if state.active {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Profiling is already active. Call stop_profiling() before starting a new session.",
            ));
        }
        state.active = true;
        ACTIVE_SESSIONS.fetch_add(1, Ordering::Relaxed);
        state.nodes.clear();
        state.emit_start = None;
        state.wall_total = Duration::ZERO;
        state.emit_count = 0;
        Ok(())
    })
}

/// Mark the beginning of an emit call (for wall time accumulation).
#[inline]
pub fn begin_emit() {
    PROFILE.with(|p| {
        p.borrow_mut().emit_start = Some(Instant::now());
    });
}

/// Mark the end of an emit call. Accumulates elapsed time into wall_total.
#[inline]
pub fn end_emit() {
    PROFILE.with(|p| {
        let mut state = p.borrow_mut();
        if let Some(start) = state.emit_start.take() {
            state.wall_total += start.elapsed();
            state.emit_count += 1;
        }
    });
}

/// Stop profiling and return the collected data.
pub fn stop_session() -> (HashMap<usize, NodeProfile>, Duration, u64) {
    PROFILE.with(|p| {
        let mut state = p.borrow_mut();
        if std::mem::replace(&mut state.active, false) {
            ACTIVE_SESSIONS.fetch_sub(1, Ordering::Relaxed);
        }
        // If an emit was in-flight (e.g., error path), finalize it
        if let Some(start) = state.emit_start.take() {
            state.wall_total += start.elapsed();
            state.emit_count += 1;
        }
        let nodes = std::mem::take(&mut state.nodes);
        (nodes, state.wall_total, state.emit_count)
    })
}

// ---------------------------------------------------------------------------
// Recording helpers (called from execution.rs hot path)
// ---------------------------------------------------------------------------

/// Record func_time + items for a node in a single thread-local access.
#[inline]
pub fn record_node(node_id: usize, func_time: Duration, items_in: u64, items_out: u64) {
    PROFILE.with(|p| {
        let mut state = p.borrow_mut();
        let entry = state.nodes.entry(node_id).or_default();
        entry.func_time += func_time;
        entry.items_in += items_in;
        entry.items_out += items_out;
    });
}

// ---------------------------------------------------------------------------
// Report data structures
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct ReportNode {
    name: String,
    source_location: String,
    func_time_ns: u64,
    items_in: u64,
    items_out: u64,
    is_terminal: bool,
    children: Vec<usize>, // indices into ProfileReport.nodes
}

/// Profiling report for a pipeline execution.
///
/// Contains per-node timing and item flow data, preserving the graph
/// structure for tree-view display.
#[pyclass(module = "akayu")]
#[derive(Clone)]
pub struct ProfileReport {
    nodes: Vec<ReportNode>,
    edges: Vec<(usize, usize)>, // (parent_idx, child_idx)
    wall_time_ns: u64,
    emit_count: u64,
}

// ---------------------------------------------------------------------------
// Building a report from collected data + graph walk
// ---------------------------------------------------------------------------

/// Build a `ProfileReport` by BFS-walking the stream graph and matching
/// each node to its collected profile data.
///
/// Uses Rust struct pointers (`std::ptr::from_ref`) for profile data lookups
/// to match the keys used in `execution.rs` instrumentation.
pub fn build_report(
    py: Python,
    source: &Stream,
    data: HashMap<usize, NodeProfile>,
    wall_time: Duration,
    emit_count: u64,
) -> PyResult<Py<ProfileReport>> {
    let mut nodes: Vec<ReportNode> = Vec::new();
    let mut edges: Vec<(usize, usize)> = Vec::new();
    // Map from Rust struct pointer → index in `nodes` vec (for dedup in DAGs)
    let mut ptr_to_idx: HashMap<usize, usize> = HashMap::new();

    // Add source node
    let source_ptr = std::ptr::from_ref(source) as usize;
    let source_profile = data.get(&source_ptr);
    let source_node = ReportNode {
        name: source.name.to_string(),
        source_location: source.source_location.to_string(),
        func_time_ns: source_profile.map_or(0, |p| p.func_time.as_nanos() as u64),
        items_in: source_profile.map_or(0, |p| p.items_in),
        items_out: source_profile.map_or(0, |p| p.items_out),
        is_terminal: source.downstreams.is_empty(),
        children: Vec::new(),
    };
    nodes.push(source_node);
    ptr_to_idx.insert(source_ptr, 0);

    // BFS queue: (Py<Stream>, parent_index)
    let mut queue: Vec<(Py<Stream>, Option<usize>)> = Vec::new();

    // Enqueue source's children
    for downstream in &source.downstreams {
        queue.push((downstream.clone_ref(py), Some(0)));
    }

    // Process queue
    let mut queue_idx = 0;
    while queue_idx < queue.len() {
        let child_py = queue[queue_idx].0.clone_ref(py);
        let parent_idx = queue[queue_idx].1;
        queue_idx += 1;

        // Borrow the Py<Stream> to get the Rust struct pointer
        let child = child_py
            .try_borrow(py)
            .map_err(|_| PyErr::from(crate::error::StreamError::BusyFlush))?;
        let child_ptr = std::ptr::from_ref(&*child) as usize;

        if let Some(&existing_idx) = ptr_to_idx.get(&child_ptr) {
            // Already visited (DAG convergence point) — just add edge
            if let Some(pidx) = parent_idx {
                edges.push((pidx, existing_idx));
                nodes[pidx].children.push(existing_idx);
            }
            continue;
        }

        // Chain nodes: expand individual steps into the report
        if let NodeLogic::Chain { ref steps } = child.logic {
            // Mark the chain head as visited (use first step's index)
            let first_step_idx = nodes.len();
            ptr_to_idx.insert(child_ptr, first_step_idx);

            let mut prev_idx = parent_idx;
            for step in steps {
                let step_profile = data.get(&step.node_id);
                let step_node = ReportNode {
                    name: step.name.to_string(),
                    source_location: step.source_location.to_string(),
                    func_time_ns: step_profile.map_or(0, |p| p.func_time.as_nanos() as u64),
                    items_in: step_profile.map_or(0, |p| p.items_in),
                    items_out: step_profile.map_or(0, |p| p.items_out),
                    is_terminal: false,
                    children: Vec::new(),
                };
                let step_idx = nodes.len();
                nodes.push(step_node);
                if let Some(pidx) = prev_idx {
                    edges.push((pidx, step_idx));
                    nodes[pidx].children.push(step_idx);
                }
                prev_idx = Some(step_idx);
            }
            // Last step: set terminal flag and enqueue chain head's downstreams
            if let Some(last_idx) = prev_idx {
                if child.downstreams.is_empty() {
                    nodes[last_idx].is_terminal = true;
                }
                for downstream in &child.downstreams {
                    queue.push((downstream.clone_ref(py), Some(last_idx)));
                }
            }
            continue;
        }

        let child_profile = data.get(&child_ptr);

        let child_node = ReportNode {
            name: child.name.to_string(),
            source_location: child.source_location.to_string(),
            func_time_ns: child_profile.map_or(0, |p| p.func_time.as_nanos() as u64),
            items_in: child_profile.map_or(0, |p| p.items_in),
            items_out: child_profile.map_or(0, |p| p.items_out),
            is_terminal: child.downstreams.is_empty(),
            children: Vec::new(),
        };
        let child_idx = nodes.len();
        nodes.push(child_node);
        ptr_to_idx.insert(child_ptr, child_idx);

        if let Some(pidx) = parent_idx {
            edges.push((pidx, child_idx));
            nodes[pidx].children.push(child_idx);
        }

        // Enqueue this node's children
        for downstream in &child.downstreams {
            queue.push((downstream.clone_ref(py), Some(child_idx)));
        }
    }

    let report = ProfileReport {
        nodes,
        edges,
        wall_time_ns: wall_time.as_nanos() as u64,
        emit_count,
    };
    Py::new(py, report)
}

// ---------------------------------------------------------------------------
// ProfileReport Python methods
// ---------------------------------------------------------------------------

#[pymethods]
impl ProfileReport {
    /// Print the profiling report.
    ///
    /// Args:
    ///     sort_by: If "time", print a flat list sorted by func_time descending.
    ///              If None (default), print a tree view showing graph structure.
    #[pyo3(signature = (sort_by=None))]
    pub fn print(&self, sort_by: Option<&str>) {
        let output = match sort_by {
            Some("time") => self.format_sorted(),
            _ => self.format_tree(),
        };
        println!("{output}");
    }

    /// Return profiling data as a dictionary.
    pub fn to_dict<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);

        let total_func_ns: u64 = self.nodes.iter().map(|n| n.func_time_ns).sum();

        dict.set_item("wall_time_ms", self.wall_time_ns as f64 / 1_000_000.0)?;
        dict.set_item("emit_count", self.emit_count)?;
        dict.set_item("total_func_time_ms", total_func_ns as f64 / 1_000_000.0)?;
        dict.set_item(
            "total_overhead_ms",
            (self.wall_time_ns.saturating_sub(total_func_ns)) as f64 / 1_000_000.0,
        )?;

        // Nodes
        let nodes_list = PyList::empty(py);
        for (i, node) in self.nodes.iter().enumerate() {
            let node_dict = PyDict::new(py);
            node_dict.set_item("id", i)?;
            node_dict.set_item("name", &node.name)?;
            node_dict.set_item("location", &node.source_location)?;
            node_dict.set_item("func_time_ms", node.func_time_ns as f64 / 1_000_000.0)?;
            node_dict.set_item("items_in", node.items_in)?;
            node_dict.set_item("items_out", node.items_out)?;
            nodes_list.append(node_dict)?;
        }
        dict.set_item("nodes", nodes_list)?;

        // Edges
        let edges_list = PyList::empty(py);
        for (from, to) in &self.edges {
            let edge_dict = PyDict::new(py);
            edge_dict.set_item("from", *from)?;
            edge_dict.set_item("to", *to)?;
            // Edge items = parent's items_out (approximation for non-branching)
            if *from < self.nodes.len() {
                edge_dict.set_item("items", self.nodes[*from].items_out)?;
            }
            edges_list.append(edge_dict)?;
        }
        dict.set_item("edges", edges_list)?;

        Ok(dict)
    }

    /// Return profiling data as a list of node dicts.
    pub fn to_list<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let nodes_list = PyList::empty(py);
        for node in &self.nodes {
            let node_dict = PyDict::new(py);
            node_dict.set_item("name", &node.name)?;
            node_dict.set_item("location", &node.source_location)?;
            node_dict.set_item("func_time_ms", node.func_time_ns as f64 / 1_000_000.0)?;
            node_dict.set_item("items_in", node.items_in)?;
            node_dict.set_item("items_out", node.items_out)?;
            nodes_list.append(node_dict)?;
        }
        Ok(nodes_list)
    }

    /// Total function time across all nodes in milliseconds.
    #[getter]
    pub fn total_func_time_ms(&self) -> f64 {
        let total_ns: u64 = self.nodes.iter().map(|n| n.func_time_ns).sum();
        total_ns as f64 / 1_000_000.0
    }

    /// Total framework overhead in milliseconds.
    #[getter]
    pub fn total_overhead_ms(&self) -> f64 {
        let total_func_ns: u64 = self.nodes.iter().map(|n| n.func_time_ns).sum();
        self.wall_time_ns.saturating_sub(total_func_ns) as f64 / 1_000_000.0
    }

    /// Total wall time in milliseconds.
    #[getter]
    pub fn total_time_ms(&self) -> f64 {
        self.wall_time_ns as f64 / 1_000_000.0
    }

    /// Number of emit calls profiled.
    #[getter]
    pub fn emit_count(&self) -> u64 {
        self.emit_count
    }

    fn __repr__(&self) -> String {
        self.format_tree()
    }

    fn __str__(&self) -> String {
        self.format_tree()
    }
}

// ---------------------------------------------------------------------------
// Formatting
// ---------------------------------------------------------------------------

impl ProfileReport {
    fn format_tree(&self) -> String {
        if self.nodes.is_empty() {
            return "Pipeline Profile (0 emits, 0.00ms wall time)\n(empty)\n".to_string();
        }

        let total_func_ns: u64 = self.nodes.iter().map(|n| n.func_time_ns).sum();
        let wall_ms = self.wall_time_ns as f64 / 1_000_000.0;
        let func_ms = total_func_ns as f64 / 1_000_000.0;
        let overhead_ns = self.wall_time_ns.saturating_sub(total_func_ns);
        let overhead_ms = overhead_ns as f64 / 1_000_000.0;

        let emit_label = if self.emit_count == 1 {
            "emit"
        } else {
            "emits"
        };

        let mut out = String::new();
        let _ = writeln!(
            out,
            "Pipeline Profile ({} {}, {:.2}ms wall time)\n",
            self.emit_count, emit_label, wall_ms
        );

        // DFS tree print starting from root (index 0)
        self.format_node(&mut out, 0, &[], true);

        // Summary line
        let _ = write!(out, "\nFunction: {:.2}ms", func_ms);
        if self.wall_time_ns > 0 {
            let _ = write!(
                out,
                " ({:.1}%)",
                total_func_ns as f64 / self.wall_time_ns as f64 * 100.0
            );
        }
        let _ = write!(out, "  Overhead: {:.2}ms", overhead_ms);
        if self.wall_time_ns > 0 {
            let _ = write!(
                out,
                " ({:.1}%)",
                overhead_ns as f64 / self.wall_time_ns as f64 * 100.0
            );
        }
        let _ = writeln!(out);

        out
    }

    fn format_node(
        &self,
        out: &mut String,
        node_idx: usize,
        prefix_parts: &[bool], // true = continuing branch, false = last branch
        is_root: bool,
    ) {
        let node = &self.nodes[node_idx];

        // Build prefix
        let mut prefix = String::new();
        if !is_root {
            for &continues in prefix_parts
                .iter()
                .take(prefix_parts.len().saturating_sub(1))
            {
                if continues {
                    prefix.push_str("\u{2502}   "); // │
                } else {
                    prefix.push_str("    ");
                }
            }
            if let Some(&continues) = prefix_parts.last() {
                if continues {
                    prefix.push_str("\u{251c}\u{2500}\u{2500} "); // ├──
                } else {
                    prefix.push_str("\u{2514}\u{2500}\u{2500} "); // └──
                }
            }
        }

        // Build items display
        let items_str = if node.is_terminal {
            format!("{}", node.items_in)
        } else {
            format!("{}\u{2192}{}", node.items_in, node.items_out) // →
        };

        // Build time display
        let func_ms = node.func_time_ns as f64 / 1_000_000.0;
        let pct = if self.wall_time_ns > 0 {
            node.func_time_ns as f64 / self.wall_time_ns as f64 * 100.0
        } else {
            0.0
        };

        // Calculate name padding — we want alignment
        let name_with_prefix = format!("{prefix}{}", node.name);
        let pad = 40_usize.saturating_sub(name_with_prefix.len());

        if pct >= 0.05 {
            let _ = writeln!(
                out,
                "{name_with_prefix}{:pad$} {:>10}    {:.2}ms  {:.1}%",
                "", items_str, func_ms, pct
            );
        } else {
            let _ = writeln!(
                out,
                "{name_with_prefix}{:pad$} {:>10}    {:.2}ms",
                "", items_str, func_ms
            );
        }

        // Recurse into children
        let n_children = node.children.len();
        for (i, &child_idx) in node.children.iter().enumerate() {
            let is_last = i == n_children - 1;
            let mut child_prefix = prefix_parts.to_vec();
            child_prefix.push(!is_last);
            self.format_node(out, child_idx, &child_prefix, false);
        }
    }

    fn format_sorted(&self) -> String {
        if self.nodes.is_empty() {
            return "Pipeline Profile (0 emits, 0.00ms wall time)\n(empty)\n".to_string();
        }

        let total_func_ns: u64 = self.nodes.iter().map(|n| n.func_time_ns).sum();
        let wall_ms = self.wall_time_ns as f64 / 1_000_000.0;
        let func_ms = total_func_ns as f64 / 1_000_000.0;
        let overhead_ns = self.wall_time_ns.saturating_sub(total_func_ns);
        let overhead_ms = overhead_ns as f64 / 1_000_000.0;

        let emit_label = if self.emit_count == 1 {
            "emit"
        } else {
            "emits"
        };

        let mut out = String::new();
        let _ = writeln!(
            out,
            "Pipeline Profile ({} {}, {:.2}ms wall time)\n",
            self.emit_count, emit_label, wall_ms
        );

        // Sort by func_time descending
        let mut sorted: Vec<usize> = (0..self.nodes.len()).collect();
        sorted.sort_by(|&a, &b| self.nodes[b].func_time_ns.cmp(&self.nodes[a].func_time_ns));

        let _ = writeln!(
            out,
            "  #  {:<30} {:<16} {:>10}  {:>10}  {:>5}",
            "Node", "Location", "In\u{2192}Out", "Func Time", "%"
        );
        let _ = writeln!(out, "{}", "\u{2500}".repeat(80));

        for (rank, &idx) in sorted.iter().enumerate() {
            let node = &self.nodes[idx];
            let func_ms_val = node.func_time_ns as f64 / 1_000_000.0;
            let pct = if self.wall_time_ns > 0 {
                node.func_time_ns as f64 / self.wall_time_ns as f64 * 100.0
            } else {
                0.0
            };

            let items_str = if node.is_terminal {
                format!("{}", node.items_in)
            } else {
                format!("{}\u{2192}{}", node.items_in, node.items_out)
            };

            // Truncate name/location if too long
            let name = if node.name.len() > 30 {
                format!("{}...", &node.name[..27])
            } else {
                node.name.clone()
            };
            let loc = if node.source_location.len() > 16 {
                format!("{}...", &node.source_location[..13])
            } else {
                node.source_location.clone()
            };

            let _ = writeln!(
                out,
                " {:>2}  {:<30} {:<16} {:>10}  {:>8.2}ms  {:>4.1}%",
                rank + 1,
                name,
                loc,
                items_str,
                func_ms_val,
                pct
            );
        }

        // Summary
        let _ = write!(out, "\nFunction: {:.2}ms", func_ms);
        if self.wall_time_ns > 0 {
            let _ = write!(
                out,
                " ({:.1}%)",
                total_func_ns as f64 / self.wall_time_ns as f64 * 100.0
            );
        }
        let _ = write!(out, "  Overhead: {:.2}ms", overhead_ms);
        if self.wall_time_ns > 0 {
            let _ = write!(
                out,
                " ({:.1}%)",
                overhead_ns as f64 / self.wall_time_ns as f64 * 100.0
            );
        }
        let _ = writeln!(out);

        out
    }
}
