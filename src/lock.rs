//! The per-node lock that serializes concurrent use of a node across threads,
//! and how a thread waits for one without deadlocking.
//!
//! `emit()`, `emit_batch()` and `flush()` hold the lock of the node they were
//! called on, and edges marked `locks_children` (par() branches, merge inputs)
//! take the child's lock before updating it. It replaces a Python
//! `threading.RLock` per node, which cost ~3,600 instructions per emit through
//! PyO3 method calls.
//!
//! # Waiting
//!
//! A lock records the thread holding it, and a thread that waits, for a lock
//! or for pool tasks (`pool::wait_for`), is listed in `WAITING` with what it
//! waits for. Before it sleeps, and every `RECHECK` while it does, a thread
//! waiting for a lock follows the chain: the lock's holder, what the holder
//! waits for (a lock's holder, or the threads running its tasks), and so on.
//! A chain that comes back to this thread is a wait that would never end:
//! the thread raises instead of waiting (`DeadlockError`, or the re-entrancy
//! error of the source it waited for), and drops what it holds. It takes
//! itself off the list in the same step, under `WAITING`'s mutex, so of the
//! threads in a cycle exactly one gives up. A thread waiting for tasks never
//! gives up: the task's thread waiting for a lock does.
//!
//! # What refuses what
//!
//! | Flag | On | Set while | Refuses (error) |
//! |---|---|---|---|
//! | `Stream::emitting` | a source | its emit() or flush() | an emit/flush re-entering it (`ReentrantError`); a flush mid-emit (`BusyError`) |
//! | PyO3's borrow flag | a node | an update or flush of it (uncompiled) | an item re-entering it (`ReentrantError`); a change mid-emit (`BusyError`) |
//! | `Stream::busy` | a compiled node, if `guard_reentry` | a compiled update | as the borrow flag, which raw pointers bypass |
//! | `Stream::flushing` | a node | its flush() or drain() | a flush re-entering it (`ReentrantError`) |
//! | `NodeLock` | a node (one per compiled graph without par()/prefetch()) | an emit, flush or locked update | other threads: they wait, unless that would never end (`DeadlockError`) |

use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use lock_api::{RawMutex as _, RawMutexTimed as _};
use parking_lot::RawMutex;
use pyo3::prelude::*;

use crate::Stream;
use crate::error::StreamError;

/// Reentrant like `threading.RLock`: the owning thread may lock again, so a
/// sink that flushes or emits into its own pipeline reaches the re-entrancy
/// errors instead of deadlocking. Unlike `lock_api`'s reentrant mutex, it
/// says who holds it.
pub(crate) struct NodeLock {
    raw: RawMutex,
    /// The holder's `this_thread()`, 0 while free.
    owner: AtomicUsize,
    /// How many times the holder has locked it. Only the holder touches it.
    depth: AtomicUsize,
}

impl NodeLock {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            raw: RawMutex::INIT,
            owner: AtomicUsize::new(0),
            depth: AtomicUsize::new(0),
        })
    }

    /// Lock for the current thread (again, if it holds it already). Uncontended
    /// this is one atomic operation. Held by another thread, wait with the GIL
    /// released, so the holder can run Python code and finish; a wait that
    /// would never end raises `DeadlockError`, and signals (Ctrl+C) interrupt
    /// it with their error.
    #[inline]
    pub(crate) fn acquire(self: &Arc<Self>, py: Python) -> PyResult<NodeLockGuard<'_>> {
        self.acquire_or(py, |_| StreamError::WaitCycle.into())
    }

    /// `acquire`, raising `on_cycle(cycle)` for a wait that would never end.
    #[inline]
    pub(crate) fn acquire_or(
        self: &Arc<Self>,
        py: Python,
        on_cycle: impl Fn(Cycle) -> PyErr,
    ) -> PyResult<NodeLockGuard<'_>> {
        let me = this_thread();
        if self.owner.load(Relaxed) == me {
            self.depth.store(self.depth.load(Relaxed) + 1, Relaxed);
        } else {
            if !self.raw.try_lock() {
                self.wait(py, me, &on_cycle)?;
            }
            self.owner.store(me, Relaxed);
            self.depth.store(1, Relaxed);
        }
        Ok(NodeLockGuard(self, PhantomData))
    }

    #[cold]
    #[inline(never)]
    fn wait(
        self: &Arc<Self>,
        py: Python,
        me: usize,
        on_cycle: &dyn Fn(Cycle) -> PyErr,
    ) -> PyResult<()> {
        let waiting = Waiting::new(me, WaitingOn::Lock(Arc::clone(self)));
        loop {
            if let Some(cycle) = waiting.would_wait_forever() {
                return Err(on_cycle(cycle));
            }
            if py.detach(|| self.raw.try_lock_for(RECHECK)) {
                return Ok(());
            }
            py.check_signals()?;
        }
    }

    /// The holder's `this_thread()`, 0 while free.
    pub(crate) fn owner(&self) -> usize {
        self.owner.load(Relaxed)
    }

    /// Whether the current thread holds it.
    pub(crate) fn held_by_this_thread(&self) -> bool {
        self.owner() == this_thread()
    }
}

/// How often a thread waiting for a lock checks again whether its wait can
/// end, and for signals (Ctrl+C).
const RECHECK: Duration = Duration::from_millis(20);
/// How often a thread waiting for pool tasks checks for signals.
pub(crate) const SIGNAL_POLL: Duration = Duration::from_millis(50);
/// How long flush() of a node that isn't a source waits for a borrow it
/// can't find the holder of (another thread's item inside the node), before
/// giving up (`helpers::with_node_mut`).
pub(crate) const BUSY_WAIT: Duration = Duration::from_millis(500);

/// This thread, for the checks below: the address of a thread-local, so
/// unique among running threads, and never 0.
#[inline(always)]
pub(crate) fn this_thread() -> usize {
    thread_local! {
        static TOKEN: u8 = const { 0 };
    }
    TOKEN.with(|t| std::ptr::from_ref(t) as usize)
}

/// What a waiting thread waits for.
pub(crate) enum WaitingOn {
    /// A lock: its holder.
    Lock(Arc<NodeLock>),
    /// Pool tasks it submitted: the threads running them.
    Tasks(Vec<Arc<crate::pool::Task>>),
}

impl WaitingOn {
    /// The threads this waits for (0 for none).
    fn holders(&self) -> impl Iterator<Item = usize> + '_ {
        let (lock, tasks) = match self {
            Self::Lock(lock) => (Some(lock.owner()), &[][..]),
            Self::Tasks(tasks) => (None, &tasks[..]),
        };
        lock.into_iter()
            .chain(tasks.iter().map(|task| task.runner()))
    }
}

/// A thread listed in `WAITING`.
struct Waiter {
    /// Tells this wait from the same thread's others (a signal handler run
    /// while it waits may wait again).
    id: u64,
    thread: usize,
    on: WaitingOn,
}

/// Every thread waiting for a lock or for pool tasks.
static WAITING: Mutex<Vec<Waiter>> = Mutex::new(Vec::new());
static NEXT_WAIT: AtomicU64 = AtomicU64::new(0);

/// How a wait that would never end comes back to the waiting thread.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Cycle {
    /// The lock's holder waits for pool tasks this thread runs (directly, or
    /// through tasks those run): this thread is doing part of the holder's
    /// work, a par() branch or prefetched call of its emit.
    Tasks,
    /// Through another lock: independent emits (or flushes) waiting for each
    /// other.
    Locks,
}

/// Lists this thread in `WAITING` until dropped.
pub(crate) struct Waiting(u64);

impl Waiting {
    pub(crate) fn new(thread: usize, on: WaitingOn) -> Self {
        let id = NEXT_WAIT.fetch_add(1, Relaxed);
        lock_waiting().push(Waiter { id, thread, on });
        Self(id)
    }

    /// Whether the threads this one waits for wait, directly or through
    /// other waiting threads, for this one. If so, this wait is taken off the
    /// list right away: its thread gives up, and the others in the cycle,
    /// checking after it, see the cycle broken.
    fn would_wait_forever(&self) -> Option<Cycle> {
        let mut waiting = lock_waiting();
        let at = waiting.iter().position(|w| w.id == self.0)?;
        let me = waiting[at].thread;
        // (thread, whether the chain to it went through another lock)
        let mut todo: Vec<(usize, bool)> = waiting[at].on.holders().map(|t| (t, false)).collect();
        let mut seen = Vec::new();
        while let Some((thread, via_lock)) = todo.pop() {
            if thread == me {
                waiting.remove(at);
                return Some(if via_lock { Cycle::Locks } else { Cycle::Tasks });
            }
            if thread == 0 || seen.contains(&(thread, via_lock)) {
                continue;
            }
            seen.push((thread, via_lock));
            // What it waits for now: its innermost wait, listed last.
            if let Some(waiter) = waiting.iter().rev().find(|w| w.thread == thread) {
                let via_lock = via_lock || matches!(waiter.on, WaitingOn::Lock(_));
                todo.extend(waiter.on.holders().map(|t| (t, via_lock)));
            }
        }
        None
    }
}

impl Drop for Waiting {
    fn drop(&mut self) {
        let mut waiting = lock_waiting();
        if let Some(at) = waiting.iter().rposition(|w| w.id == self.0) {
            waiting.remove(at);
        }
    }
}

fn lock_waiting() -> std::sync::MutexGuard<'static, Vec<Waiter>> {
    WAITING.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Unlocks on drop, including on error paths. Not Send: it must unlock on the
/// thread that locked.
pub(crate) struct NodeLockGuard<'a>(&'a NodeLock, PhantomData<*const ()>);

impl Drop for NodeLockGuard<'_> {
    fn drop(&mut self) {
        let lock = self.0;
        let depth = lock.depth.load(Relaxed) - 1;
        lock.depth.store(depth, Relaxed);
        if depth == 0 {
            lock.owner.store(0, Relaxed);
            // SAFETY: this thread locked it (`acquire`), and the guard can't
            // leave the thread.
            unsafe { lock.raw.unlock() }
        }
    }
}

/// A downstream edge: the child plus its lock, so a parent can serialize
/// concurrent updates of a shared child without borrowing it first (the
/// borrow is what's contended). Derefs to the child.
pub(crate) struct Downstream {
    pub(crate) node: Py<Stream>,
    pub(crate) lock: Arc<NodeLock>,
    /// Set by compile(): a raw pointer to the child, bypassing PyCell's
    /// borrow check in the propagation hot path (see `StreamPtr`).
    pub(crate) ptr: Option<crate::StreamPtr>,
}

impl Downstream {
    pub(crate) fn new(py: Python, node: &Py<Stream>) -> Self {
        Self {
            node: node.clone_ref(py),
            lock: Arc::clone(&node.borrow(py).lock),
            ptr: None,
        }
    }

    pub(crate) fn clone_edge(&self, py: Python) -> Self {
        Self {
            node: self.node.clone_ref(py),
            lock: Arc::clone(&self.lock),
            ptr: self.ptr,
        }
    }

    /// Update the child while holding its lock. A same-thread re-entry passes
    /// the lock and fails the borrow, raising instead of deadlocking.
    pub(crate) fn locked_update(&self, py: Python, val: Py<PyAny>) -> PyResult<()> {
        let _guard = self.lock.acquire(py)?;
        crate::helpers::enter_node(self, py)?.update_compiled(py, val)
    }

    pub(crate) fn locked_update_batch(&self, py: Python, items: Vec<Py<PyAny>>) -> PyResult<()> {
        let _guard = self.lock.acquire(py)?;
        crate::helpers::enter_node(self, py)?.update_batch_compiled(py, items)
    }

    /// The child for a flush or drain reaching it: locked, then borrowed, in
    /// the order an update through a locked edge takes, so that a flush and
    /// another thread's emit take turns instead of meeting mid-way. (Below a
    /// merge, the emit holds the child's lock, not the flushed source's.)
    /// The borrow is released before the lock.
    pub(crate) fn locked_for_flush<'py>(
        &'py self,
        py: Python<'py>,
    ) -> PyResult<(PyRefMut<'py, Stream>, NodeLockGuard<'py>)> {
        let guard = self.lock.acquire(py)?;
        let node = self
            .node
            .try_borrow_mut(py)
            .map_err(|_| PyErr::from(StreamError::BusyFlush))?;
        Ok((node, guard))
    }
}

impl Deref for Downstream {
    type Target = Py<Stream>;

    fn deref(&self) -> &Py<Stream> {
        &self.node
    }
}

/// What a source is doing, in its `emitting` field.
pub(crate) const IDLE: u8 = 0;
const EMITTING: u8 = 1;
const FLUSHING: u8 = 2;

/// A source entered by emit()/emit_batch() (or flush()/drain()): its lock
/// held and its `emitting` state set, until dropped. The state is cleared
/// before the lock is released (fields drop in order).
pub(crate) struct EnteredSource<'a> {
    _emitting: Emitting<'a>,
    _guard: NodeLockGuard<'a>,
}

impl Stream {
    /// Enter this source to emit into it (`flush`: to flush or drain it).
    /// Every way into a source goes through here: lock, check the node is a
    /// source, mark it. A wait for the lock that would never end raises: as
    /// a re-entrant emit or flush when this thread is doing part of the
    /// source's own emit (a par() branch or prefetched call emitting into the
    /// source it runs under), as `DeadlockError` otherwise (threads emitting
    /// into each other's sources).
    #[inline(always)]
    pub(crate) fn enter_source(&self, py: Python, flush: bool) -> PyResult<EnteredSource<'_>> {
        let guard =
            self.lock
                .acquire_or(py, |cycle| match (cycle, self.emitting.load(Relaxed)) {
                    (Cycle::Locks, _) | (_, IDLE) => StreamError::WaitCycle.into(),
                    (Cycle::Tasks, state) if flush => refused_flush(state),
                    (Cycle::Tasks, state) => refused(state, &self.name),
                })?;
        if !matches!(self.logic, crate::node::NodeLogic::Source) {
            return Err(not_a_source(&self.name));
        }
        let emitting = Emitting::enter(&self.emitting, &guard, flush, &self.name)?;
        Ok(EnteredSource {
            _emitting: emitting,
            _guard: guard,
        })
    }
}

#[cold]
#[inline(never)]
fn not_a_source(name: &str) -> PyErr {
    StreamError::EmitOnNonSource(name.to_string()).into()
}

/// Marks a source as emitting (or flushing) until dropped, even when a panic
/// unwinds through it: a state left set would reject every later emit().
struct Emitting<'a>(&'a AtomicU8);

impl<'a> Emitting<'a> {
    /// Mark `state` emitting (`flush`: flushing), or fail if the source is
    /// already emitting or flushing: an emit() from inside its own graph, or
    /// from a callback of its own flush(). `_locked` is the source's lock,
    /// held, so a load and a store suffice.
    #[inline(always)]
    fn enter(
        state: &'a AtomicU8,
        _locked: &NodeLockGuard<'_>,
        flush: bool,
        name: &str,
    ) -> PyResult<Self> {
        let now = state.load(Relaxed);
        if now != IDLE {
            return Err(if flush {
                refused_flush(now)
            } else {
                refused(now, name)
            });
        }
        state.store(if flush { FLUSHING } else { EMITTING }, Relaxed);
        Ok(Self(state))
    }
}

impl Drop for Emitting<'_> {
    fn drop(&mut self) {
        self.0.store(IDLE, Relaxed);
    }
}

/// The error for an emit() into a source that is emitting or flushing.
#[cold]
#[inline(never)]
fn refused(state: u8, name: &str) -> PyErr {
    if state == FLUSHING {
        StreamError::EmitDuringFlush(name.to_string()).into()
    } else {
        StreamError::ReentrantEmit(name.to_string()).into()
    }
}

/// The error for a flush() of a source that is emitting or flushing.
#[cold]
#[inline(never)]
fn refused_flush(state: u8) -> PyErr {
    if state == FLUSHING {
        StreamError::ReentrantFlushCall.into()
    } else {
        StreamError::BusyFlush.into()
    }
}
