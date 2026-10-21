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
            return Err(crate::error::exc::StreamError::new_err(
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

/// Count `items` emitted into `source` (by emit(), emit_batch() or a sink's
/// `target.emit` shortcut): a source passes on what it is given.
#[inline]
pub fn record_emitted(source: &Stream, items: usize) {
    if is_profiling() {
        let n = items as u64;
        record_node(std::ptr::from_ref(source) as usize, Duration::ZERO, n, n);
    }
}

/// Count `items` a node sent on when flushed: a collect's buffer, a
/// chunk_by's last chunk, a prefetch's results still in flight.
#[inline]
pub fn record_flushed(node: &Stream, items: usize) {
    if is_profiling() {
        record_node(
            std::ptr::from_ref(node) as usize,
            Duration::ZERO,
            0,
            items as u64,
        );
    }
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
    /// How its work runs (see `how`).
    how: &'static str,
    /// The compiled chain it was merged into, numbered from 1 in report order.
    chain: Option<usize>,
}

/// How a node's work runs, as the reports tag it: "Python" (its function is
/// called), "native" (a Rust operation: an analyzed lambda or builtin, or a
/// sink's emit/flush shortcut), "native, unboxed" (one of a run of numeric
/// steps that compile() runs together on machine numbers), "threads"
/// (par(), and prefetch()'s "Python, threads"), or nothing (no function:
/// union, collect, ...).
fn how(logic: &NodeLogic, native: bool, unboxed: bool) -> &'static str {
    match logic {
        NodeLogic::Map { .. }
        | NodeLogic::Filter { .. }
        | NodeLogic::StarMap { .. }
        | NodeLogic::Sink { .. }
        | NodeLogic::Accumulate { .. }
        | NodeLogic::BatchMap { .. }
        | NodeLogic::ChunkBy(_)
        | NodeLogic::ExtractBetween(_) => match (native, unboxed) {
            (_, true) => "native, unboxed",
            (true, false) => "native",
            (false, false) => "Python",
        },
        NodeLogic::Prefetched { .. } => "Python, threads",
        NodeLogic::Parallel => "threads",
        _ => "",
    }
}

/// Whether an uncompiled node's work is a Rust operation (see `how`).
fn native(node: &Stream) -> bool {
    node.builtin_op.is_some() || node.flush_target.is_some() || node.emit_target.is_some()
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
    /// The graph was compiled (for explain()'s header).
    compiled: bool,
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
    // Compiled chains seen so far: they are numbered in report order.
    let mut chains = 0;

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
        how: how(&source.logic, native(source), false),
        chain: None,
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
            chains += 1;
            // Steps left in the numeric run the last step with one started.
            let mut unboxed_left = 0;

            let mut prev_idx = parent_idx;
            for step in steps {
                if let Some(run) = &step.numeric_run {
                    unboxed_left = run.steps();
                }
                let unboxed = unboxed_left > 0;
                unboxed_left = unboxed_left.saturating_sub(1);
                let native = step.builtin_op.is_some()
                    || step.flush_target.is_some()
                    || step.emit_target.is_some();
                let step_profile = data.get(&step.node_id);
                let step_node = ReportNode {
                    name: step.name.to_string(),
                    source_location: step.source_location.to_string(),
                    func_time_ns: step_profile.map_or(0, |p| p.func_time.as_nanos() as u64),
                    items_in: step_profile.map_or(0, |p| p.items_in),
                    items_out: step_profile.map_or(0, |p| p.items_out),
                    is_terminal: false,
                    children: Vec::new(),
                    how: how(&step.logic, native, unboxed),
                    chain: Some(chains),
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
            how: how(&child.logic, native(&child), false),
            chain: None,
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
        compiled: source.compiled,
    };
    Py::new(py, report)
}

// ---------------------------------------------------------------------------
// ProfileReport Python methods
// ---------------------------------------------------------------------------

#[pymethods]
impl ProfileReport {
    /// Print the profiling report to `sys.stdout`, as `print()` does.
    ///
    /// Args:
    ///     sort_by: If "time", print a flat list sorted by func_time descending.
    ///              If None (default), print a tree view showing graph structure.
    #[pyo3(signature = (sort_by=None))]
    pub fn print(&self, py: Python, sort_by: Option<&str>) -> PyResult<()> {
        let output = match sort_by {
            Some("time") => self.format_sorted(),
            _ => self.format_tree(),
        };
        // Python's print(), not println!: that writes to the process's
        // stdout, which a Jupyter cell, pytest or redirect_stdout never sees.
        py.import("builtins")?.getattr("print")?.call1((output,))?;
        Ok(())
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
            node_dict.set_item("how", node.how)?;
            node_dict.set_item("chain", node.chain)?;
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
            node_dict.set_item("how", node.how)?;
            node_dict.set_item("chain", node.chain)?;
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

        self.format_node(&mut out, |node| {
            let items = if node.is_terminal {
                format!("{}", node.items_in)
            } else {
                format!("{}\u{2192}{}", node.items_in, node.items_out) // →
            };
            let ms = format!("{:.2}ms", node.func_time_ns as f64 / 1_000_000.0);
            let pct = if self.wall_time_ns > 0 {
                node.func_time_ns as f64 / self.wall_time_ns as f64 * 100.0
            } else {
                0.0
            };
            let pct = if pct >= 0.05 {
                format!("{pct:.1}%")
            } else {
                String::new()
            };
            format!("{items:>10}  {ms:>10}  {pct:>6}  {}", tag(node))
        });

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
        if self.nodes.iter().any(|n| n.how == "native, unboxed") {
            let _ = writeln!(
                out,
                "The native, unboxed steps run together on machine numbers, except while profiled:\n\
                 one at a time, so their times here are higher than in a run without profiling."
            );
        }

        out
    }

    /// How the graph runs, a line per node (see `Stream.explain()`).
    pub(crate) fn format_explain(&self) -> String {
        let root = &self.nodes[0];
        let chains = self.nodes.iter().filter_map(|n| n.chain).max().unwrap_or(0);
        let native = self
            .nodes
            .iter()
            .filter(|n| n.how.starts_with("native"))
            .count();
        let mut out = String::new();
        let _ = write!(
            out,
            "Graph below '{}' ({}), {}: {} nodes",
            root.name,
            file_and_line(&root.source_location),
            if self.compiled {
                "compiled"
            } else {
                "not compiled"
            },
            self.nodes.len()
        );
        if self.compiled {
            let plural = if chains == 1 { "" } else { "s" };
            let _ = write!(out, ", {chains} chain{plural}");
        }
        let _ = writeln!(out, ", {native} native\n");
        let location = |node: &ReportNode| keep_end(file_and_line(&node.source_location), 24);
        let width = self
            .nodes
            .iter()
            .map(|n| location(n).chars().count())
            .max()
            .unwrap_or(0);
        self.format_node(&mut out, |node| {
            format!("{:<width$}  {}", location(node), tag(node))
        });
        out
    }

    /// The tree below `root`, a row per node, depth first: (its branch lines
    /// and name, its index, whether it was listed above). Only branches
    /// indent: a node's only child goes in the same column, so a pipeline of
    /// a hundred steps is a hundred lines, not a staircase. Iterative (a graph
    /// tens of thousands of nodes deep would overflow the native stack in
    /// recursion); indentation stops growing after MAX_INDENT levels of
    /// branches (the text would grow with the square of the depth); and a
    /// node below a merge, reached along several paths, has its subtree
    /// listed once.
    fn tree_rows(&self, root: usize) -> Vec<(String, usize, bool)> {
        const MAX_INDENT: usize = 32;
        let mut rows = Vec::new();
        let mut shown = vec![false; self.nodes.len()];
        // (node, prefix parts: true = continuing branch, false = last; depth,
        // whether it continues its parent's column)
        let mut stack: Vec<(usize, Vec<bool>, usize, bool)> = vec![(root, Vec::new(), 0, false)];
        while let Some((node_idx, prefix_parts, depth, same_column)) = stack.pop() {
            let again = std::mem::replace(&mut shown[node_idx], true);
            let prefix = tree_prefix(&prefix_parts, depth > MAX_INDENT, same_column);
            rows.push((prefix + &self.nodes[node_idx].name, node_idx, again));
            if again {
                continue;
            }
            let children = &self.nodes[node_idx].children;
            if let [only] = children[..] {
                stack.push((only, prefix_parts, depth, true));
                continue;
            }
            for (i, &child_idx) in children.iter().enumerate().rev() {
                let mut child_prefix = prefix_parts.clone();
                child_prefix.push(i + 1 != children.len());
                if child_prefix.len() > MAX_INDENT {
                    child_prefix.remove(0);
                }
                stack.push((child_idx, child_prefix, depth + 1, false));
            }
        }
        rows
    }

    /// The tree, a line per node: the name column as wide as the widest name
    /// (up to 60 characters: a longer one pushes only its own line), then
    /// `columns` of the node.
    fn format_node(&self, out: &mut String, columns: impl Fn(&ReportNode) -> String) {
        let rows = self.tree_rows(0);
        let width = rows
            .iter()
            .map(|(left, ..)| left.chars().count())
            .max()
            .unwrap_or(0)
            .min(60);
        for (left, node_idx, again) in rows {
            if again {
                let _ = writeln!(out, "{left} (shown above)");
                continue;
            }
            let pad = width.saturating_sub(left.chars().count());
            let line = format!("{left}{:pad$}  {}", "", columns(&self.nodes[node_idx]));
            let _ = writeln!(out, "{}", line.trim_end());
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
            "  #  {:<30} {:<24} {:>10}  {:>10}  {:>5}",
            "Node", "Location", "In\u{2192}Out", "Func Time", "%"
        );
        let _ = writeln!(out, "{}", "\u{2500}".repeat(88));

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

            let name = keep_start(&node.name, 30);
            let loc = keep_end(file_and_line(&node.source_location), 24);

            let _ = writeln!(
                out,
                " {:>2}  {:<30} {:<24} {:>10}  {:>8.2}ms  {:>4.1}%",
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

/// `path/to/app.py:22` as `app.py:22`: the file and line, which tell nodes
/// apart, without the directories, which mostly don't.
fn file_and_line(location: &str) -> &str {
    location.rsplit(['/', '\\']).next().unwrap_or(location)
}

/// `s`, cut to `width` characters ending in "…" if it is longer.
fn keep_start(s: &str, width: usize) -> String {
    if s.chars().count() <= width {
        return s.to_string();
    }
    let kept: String = s.chars().take(width - 1).collect();
    kept + "\u{2026}"
}

/// `s`, cut to its last `width` characters after a "…" if it is longer.
fn keep_end(s: &str, width: usize) -> String {
    let n = s.chars().count();
    if n <= width {
        return s.to_string();
    }
    let kept: String = s.chars().skip(n - (width - 1)).collect();
    "\u{2026}".to_string() + &kept
}

/// A tree row's branch lines: `│   ` for each ancestor with siblings still
/// to come, then `├── ` or `└── `, or for a node in its parent's column
/// (`same_column`) the line below those; "…" first if levels were left out.
fn tree_prefix(parts: &[bool], deeper: bool, same_column: bool) -> String {
    let mut prefix = String::new();
    if deeper {
        prefix.push('\u{2026}'); // …: more levels than shown
    }
    for &continues in parts.iter().take(parts.len().saturating_sub(1)) {
        prefix.push_str(if continues { "\u{2502}   " } else { "    " }); // │
    }
    if let Some(&continues) = parts.last() {
        prefix.push_str(match (continues, same_column) {
            (true, false) => "\u{251c}\u{2500}\u{2500} ",  // ├──
            (false, false) => "\u{2514}\u{2500}\u{2500} ", // └──
            (true, true) => "\u{2502}   ",                 // │
            (false, true) => "    ",
        });
    }
    prefix
}

/// A node's tag in the reports: how it runs, then its chain.
fn tag(node: &ReportNode) -> String {
    match node.chain {
        Some(chain) => format!("{:<15}  chain {chain}", node.how),
        None => node.how.to_string(),
    }
}
