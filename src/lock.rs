//! The per-node lock that serializes concurrent use of a node across threads.
//!
//! `emit()`, `emit_batch()` and `flush()` hold the lock of the node they were
//! called on, and edges marked `needs_lock` (par() branches, union inputs) take
//! the child's lock before updating it. It replaces a Python `threading.RLock`
//! per node, which cost ~3,600 instructions per emit through PyO3 method calls.

use std::ops::Deref;
use std::sync::Arc;

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

    #[cold]
    #[inline(never)]
    fn wait(&self, py: Python) {
        let _blocked = Blocked::new();
        py.detach(|| self.0.lock());
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

/// Threads waiting in `NodeLock::wait`. A thread that waits for another
/// thread's flush checks it here: a flush whose callback is blocked on a
/// lock may be blocked on one the waiting thread holds.
static BLOCKED: std::sync::Mutex<Vec<std::thread::ThreadId>> = std::sync::Mutex::new(Vec::new());

/// Lists this thread in `BLOCKED` until dropped.
struct Blocked(std::thread::ThreadId);

impl Blocked {
    fn new() -> Self {
        let me = std::thread::current().id();
        BLOCKED
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(me);
        Self(me)
    }
}

impl Drop for Blocked {
    fn drop(&mut self) {
        let mut blocked = BLOCKED
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(i) = blocked.iter().position(|t| *t == self.0) {
            blocked.swap_remove(i);
        }
    }
}

/// Whether `thread` is waiting for a node's lock.
pub(crate) fn is_blocked(thread: std::thread::ThreadId) -> bool {
    BLOCKED
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .contains(&thread)
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
        crate::helpers::enter_node(&self.node, py)?.update(py, val)
    }

    pub(crate) fn locked_update_batch(&self, py: Python, items: Vec<Py<PyAny>>) -> PyResult<()> {
        let _guard = self.lock.acquire(py);
        crate::helpers::enter_node(&self.node, py)?.update_batch(py, items)
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
pub(crate) const EMITTING: u8 = 1;
pub(crate) const FLUSHING: u8 = 2;

/// Marks a source as emitting (or flushing) until dropped, even when a panic
/// unwinds through it: a flag left set would reject every later emit().
pub(crate) struct Emitting<'a>(&'a std::sync::atomic::AtomicU8);

impl<'a> Emitting<'a> {
    /// Mark `flag` emitting, or fail if the source is already emitting or
    /// flushing (an emit() from inside its own graph, or from a callback of
    /// its own flush()).
    /// The caller holds the source's lock, so a load and a store suffice.
    pub(crate) fn enter(flag: &'a std::sync::atomic::AtomicU8, name: &str) -> pyo3::PyResult<Self> {
        let state = flag.load(std::sync::atomic::Ordering::Acquire);
        if state != IDLE {
            return Err(refused(state, name));
        }
        flag.store(EMITTING, std::sync::atomic::Ordering::Release);
        Ok(Self(flag))
    }

    /// Mark `flag` flushing, for flush()/drain() of a source, or fail if a
    /// callback of the source's own emit() or flush() is calling it.
    pub(crate) fn enter_flush(flag: &'a std::sync::atomic::AtomicU8) -> pyo3::PyResult<Self> {
        use crate::error::StreamError;
        match flag.load(std::sync::atomic::Ordering::Acquire) {
            IDLE => {}
            EMITTING => return Err(StreamError::BusyFlush.into()),
            _ => return Err(StreamError::ReentrantFlushCall.into()),
        }
        flag.store(FLUSHING, std::sync::atomic::Ordering::Release);
        Ok(Self(flag))
    }
}

#[cold]
#[inline(never)]
fn refused(state: u8, name: &str) -> pyo3::PyErr {
    use crate::error::StreamError;
    if state == FLUSHING {
        StreamError::EmitDuringFlush(name.to_string()).into()
    } else {
        StreamError::ReentrantEmit(name.to_string()).into()
    }
}

impl Drop for Emitting<'_> {
    fn drop(&mut self) {
        self.0.store(IDLE, std::sync::atomic::Ordering::Release);
    }
}
