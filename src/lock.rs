//! The per-node lock that serializes concurrent use of a node across threads.
//!
//! `emit()`, `emit_batch()` and `flush()` hold the lock of the node they were
//! called on, and edges marked `locks_children` (par() branches, union inputs) take
//! the child's lock before updating it. It replaces a Python `threading.RLock`
//! per node, which cost ~3,600 instructions per emit through PyO3 method calls.

use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, PoisonError};
use std::time::Duration;

use lock_api::RawReentrantMutex;
use parking_lot::{RawMutex, RawThreadId};
use pyo3::prelude::*;

use crate::Stream;

/// Reentrant like `threading.RLock`: the owning thread may lock again, so a
/// sink that flushes or emits into its own pipeline reaches the re-entrancy
/// errors instead of deadlocking.
pub(crate) struct NodeLock(RawReentrantMutex<RawMutex, RawThreadId>);

impl NodeLock {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self(RawReentrantMutex::INIT))
    }

    /// Lock for the current thread. Uncontended this is one atomic operation.
    /// When another thread holds the lock, wait with the GIL released so the
    /// holder can run Python code and finish.
    #[inline]
    pub(crate) fn acquire(&self, py: Python) -> NodeLockGuard<'_> {
        if !self.0.try_lock() {
            self.wait(py);
        }
        NodeLockGuard(self, std::marker::PhantomData)
    }

    /// `acquire`, except that while another thread holds the lock, `check`
    /// decides whether waiting is still safe (see
    /// `Emitting::check_not_waited_on`), every `RECHECK`: the holder may only
    /// start waiting for this thread after this one has started waiting.
    /// Signals (Ctrl+C) interrupt the wait with their error.
    #[inline]
    pub(crate) fn acquire_checked(
        &self,
        py: Python,
        check: impl Fn() -> PyResult<()>,
    ) -> PyResult<NodeLockGuard<'_>> {
        if !self.0.try_lock() {
            self.wait_checked(py, &check)?;
        }
        Ok(NodeLockGuard(self, std::marker::PhantomData))
    }

    #[cold]
    #[inline(never)]
    fn wait(&self, py: Python) {
        let _blocked = Blocked::new(self);
        py.detach(|| self.0.lock());
    }

    #[cold]
    #[inline(never)]
    fn wait_checked(&self, py: Python, check: &dyn Fn() -> PyResult<()>) -> PyResult<()> {
        let _blocked = Blocked::new(self);
        loop {
            check()?;
            if py.detach(|| self.0.try_lock_for(RECHECK)) {
                return Ok(());
            }
            py.check_signals()?;
        }
    }

    /// Lock within `timeout`, waiting with the GIL released.
    pub(crate) fn acquire_within(
        &self,
        py: Python,
        timeout: std::time::Duration,
    ) -> Option<NodeLockGuard<'_>> {
        (self.0.try_lock() || py.detach(|| self.0.try_lock_for(timeout)))
            .then(|| NodeLockGuard(self, std::marker::PhantomData))
    }

    /// Whether the current thread holds it.
    pub(crate) fn held_by_this_thread(&self) -> bool {
        self.0.is_owned_by_current_thread()
    }
}

/// How often a thread waiting in `acquire_checked` checks again whether its
/// wait can end.
const RECHECK: Duration = Duration::from_millis(20);

/// This thread, for the checks below: the address of a thread-local, so
/// unique among running threads, and never 0.
pub(crate) fn this_thread() -> usize {
    thread_local! {
        static TOKEN: u8 = const { 0 };
    }
    TOKEN.with(|t| std::ptr::from_ref(t) as usize)
}

/// A thread waiting in `NodeLock::wait`: who, by `ThreadId`, by
/// `this_thread` and by its stack, and the lock it waits for.
struct BlockedOn {
    id: std::thread::ThreadId,
    thread: usize,
    stack: (usize, usize),
    lock: *const NodeLock,
}

// SAFETY: `lock` is only dereferenced under `BLOCKED`'s mutex, while the
// thread waiting for it is listed (see `holder_waits_for_this_thread`).
unsafe impl Send for BlockedOn {}

/// Threads waiting in `NodeLock::wait`. A thread that waits for another
/// thread's flush checks it here: a flush whose callback is blocked on a
/// lock may be blocked on one the waiting thread holds.
static BLOCKED: std::sync::Mutex<Vec<BlockedOn>> = std::sync::Mutex::new(Vec::new());

/// Lists this thread in `BLOCKED` until dropped.
struct Blocked(usize);

impl Blocked {
    fn new(lock: &NodeLock) -> Self {
        let thread = this_thread();
        BLOCKED
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(BlockedOn {
                id: std::thread::current().id(),
                thread,
                stack: crate::stack::range(),
                lock: std::ptr::from_ref(lock),
            });
        Self(thread)
    }
}

impl Drop for Blocked {
    fn drop(&mut self) {
        // A signal handler run while waiting may wait again on this thread:
        // the innermost wait ends first, and is listed last.
        let mut blocked = BLOCKED.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(i) = blocked.iter().rposition(|b| b.thread == self.0) {
            blocked.remove(i);
        }
    }
}

/// Whether `thread` is waiting for a node's lock.
pub(crate) fn is_blocked(thread: std::thread::ThreadId) -> bool {
    BLOCKED
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .any(|b| b.id == thread)
}

/// Whether the thread emitting into (or flushing) a source, found by `mark`,
/// an address on its stack, waits for this thread: asleep on a task this
/// thread runs, or blocked on a lock this thread holds, directly or through
/// other threads waiting likewise. This thread waiting for that source's lock
/// would never end. A thread blocked on a lock that this one doesn't hold
/// ends the search: who holds that lock isn't recorded.
pub(crate) fn holder_waits_for_this_thread(mark: usize) -> bool {
    #[derive(Clone, Copy)]
    enum Who {
        /// The thread whose stack holds this address.
        Address(usize),
        /// The thread with this `this_thread`.
        Thread(usize),
    }
    let me = this_thread();
    let blocked = BLOCKED.lock().unwrap_or_else(PoisonError::into_inner);
    let mut todo = vec![Who::Address(mark)];
    let mut seen = Vec::new();
    while let Some(who) = todo.pop() {
        let is = move |thread: usize, (low, high): (usize, usize)| match who {
            Who::Address(a) => low <= a && a < high,
            Who::Thread(t) => t == thread,
        };
        if let Some(b) = blocked.iter().rev().find(|b| is(b.thread, b.stack)) {
            // SAFETY: a thread is listed only while it waits for this lock,
            // which outlives the wait, and this holds BLOCKED's mutex, so the
            // thread can't stop waiting meanwhile.
            if unsafe { &*b.lock }.held_by_this_thread() {
                return true;
            }
            continue;
        }
        for runner in crate::pool::runners_awaited_by(&is) {
            if runner == me {
                return true;
            }
            if !seen.contains(&runner) {
                seen.push(runner);
                todo.push(Who::Thread(runner));
            }
        }
    }
    false
}

/// Unlocks on drop, including on error paths. Not Send: it must unlock on the
/// thread that locked.
pub(crate) struct NodeLockGuard<'a>(&'a NodeLock, std::marker::PhantomData<*const ()>);

impl Drop for NodeLockGuard<'_> {
    fn drop(&mut self) {
        // SAFETY: `acquire` locked it on this thread, and the guard can't
        // leave the thread.
        unsafe { self.0.0.unlock() }
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
        let _guard = self.lock.acquire(py);
        crate::helpers::enter_node(&self.node, py)?.update_compiled(py, val)
    }

    pub(crate) fn locked_update_batch(&self, py: Python, items: Vec<Py<PyAny>>) -> PyResult<()> {
        let _guard = self.lock.acquire(py);
        crate::helpers::enter_node(&self.node, py)?.update_batch_compiled(py, items)
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
        let guard = self.lock.acquire(py);
        let node = self
            .node
            .try_borrow_mut(py)
            .map_err(|_| PyErr::from(crate::error::StreamError::BusyFlush))?;
        Ok((node, guard))
    }
}

impl Deref for Downstream {
    type Target = Py<Stream>;

    fn deref(&self) -> &Py<Stream> {
        &self.node
    }
}

/// What a source is doing, in its `emitting` field: IDLE, or an address on
/// the stack of the thread emitting into it (which thread, for
/// `check_not_waited_on`), with the low bit set while it is flushing.
pub(crate) const IDLE: usize = 0;
const FLUSHING: usize = 1;

/// An address in the calling frame, standing for this thread: threads'
/// stacks don't overlap. Aligned, so the low bit is free for FLUSHING.
#[inline(always)]
fn this_thread_mark() -> usize {
    let marker = 0usize;
    std::ptr::addr_of!(marker) as usize
}

/// A source entered by emit()/emit_batch() (or flush()/drain()): its lock
/// held and its `emitting` mark set, until dropped. The mark is cleared
/// before the lock is released (fields drop in order).
pub(crate) struct EnteredSource<'a> {
    _emitting: Emitting<'a>,
    _guard: NodeLockGuard<'a>,
}

impl Stream {
    /// Enter this source to emit into it (`flush`: to flush or drain it).
    /// Every way into a source goes through here: lock, check the node is a
    /// source, mark it. Waiting for the lock is refused if the thread in it
    /// waits for this one (`Emitting::check_not_waited_on`).
    #[inline(always)]
    pub(crate) fn enter_source(&self, py: Python, flush: bool) -> PyResult<EnteredSource<'_>> {
        let guard = self.lock.acquire_checked(py, || {
            Emitting::check_not_waited_on(&self.emitting, |state| {
                if flush {
                    refused_flush(state)
                } else {
                    refused(state, &self.name)
                }
            })
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
    crate::error::StreamError::EmitOnNonSource(name.to_string()).into()
}

/// Marks a source as emitting (or flushing) until dropped, even when a panic
/// unwinds through it: a flag left set would reject every later emit().
pub(crate) struct Emitting<'a>(&'a AtomicUsize);

impl<'a> Emitting<'a> {
    /// Mark `flag` emitting (`flush`: flushing), or fail if the source is
    /// already emitting or flushing: an emit() from inside its own graph, or
    /// from a callback of its own flush(). `_locked` is the source's lock,
    /// held, so a load and a store suffice.
    #[inline(always)]
    fn enter(
        flag: &'a AtomicUsize,
        _locked: &NodeLockGuard<'_>,
        flush: bool,
        name: &str,
    ) -> pyo3::PyResult<Self> {
        let state = flag.load(Ordering::Acquire);
        if state != IDLE {
            return Err(if flush {
                refused_flush(state)
            } else {
                refused(state, name)
            });
        }
        flag.store(
            this_thread_mark() | if flush { FLUSHING } else { 0 },
            Ordering::Release,
        );
        Ok(Self(flag))
    }

    /// While waiting for a source's lock: fail, as an emit() into it from
    /// the emitting thread itself would, if the thread emitting into it (or
    /// flushing it) is waiting for this one, so the wait would never end:
    /// a par() branch or a prefetched call, on a pool thread, emitting into
    /// or flushing the source it runs under, or two threads emitting into
    /// each other's sources (see `holder_waits_for_this_thread`).
    #[cold]
    #[inline(never)]
    pub(crate) fn check_not_waited_on(
        flag: &AtomicUsize,
        refuse: impl FnOnce(usize) -> pyo3::PyErr,
    ) -> pyo3::PyResult<()> {
        let state = flag.load(Ordering::Acquire);
        if state != IDLE && holder_waits_for_this_thread(state & !FLUSHING) {
            return Err(refuse(state));
        }
        Ok(())
    }
}

/// The error for an emit() into a source that is emitting or flushing.
#[cold]
#[inline(never)]
fn refused(state: usize, name: &str) -> pyo3::PyErr {
    use crate::error::StreamError;
    if state & FLUSHING != 0 {
        StreamError::EmitDuringFlush(name.to_string()).into()
    } else {
        StreamError::ReentrantEmit(name.to_string()).into()
    }
}

/// The error for a flush() of a source that is emitting or flushing.
#[cold]
#[inline(never)]
fn refused_flush(state: usize) -> pyo3::PyErr {
    use crate::error::StreamError;
    if state & FLUSHING != 0 {
        StreamError::ReentrantFlushCall.into()
    } else {
        StreamError::BusyFlush.into()
    }
}

impl Drop for Emitting<'_> {
    fn drop(&mut self) {
        self.0.store(IDLE, Ordering::Release);
    }
}
