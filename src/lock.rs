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
    pub(crate) fn acquire(&self, py: Python) -> NodeLockGuard<'_> {
        if !self.0.try_lock() {
            py.detach(|| self.0.lock());
        }
        NodeLockGuard(self, std::marker::PhantomData)
    }
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
        self.node.try_borrow_mut(py)?.update(py, val)
    }

    pub(crate) fn locked_update_batch(&self, py: Python, items: Vec<Py<PyAny>>) -> PyResult<()> {
        let _guard = self.lock.acquire(py);
        self.node.try_borrow_mut(py)?.update_batch(py, items)
    }
}

impl Deref for Downstream {
    type Target = Py<Stream>;

    fn deref(&self) -> &Py<Stream> {
        &self.node
    }
}
