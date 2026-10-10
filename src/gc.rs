//! Cyclic garbage collection support for `Stream`.
//!
//! Every node holds strong references to its parents and its downstreams, so
//! any graph is a reference cycle that only Python's cyclic GC can free. That
//! needs `__traverse__` (report the references a node owns) and `__clear__`
//! (drop them to break the cycle).
//!
//! `__traverse__` only visits references that never change once the graph is
//! built or compiled: graph links, compile-time targets, callables, builtin
//! constants and prefetch state objects. It deliberately skips buffers and
//! accumulator state. The compiled fast path mutates downstream nodes through
//! raw pointers without setting PyO3's borrow flag, so a GC triggered by user
//! code mid-propagation can traverse a node while it is being mutated; reading
//! only fields that propagation never writes keeps that safe. Skipping a
//! reference is always safe for the GC: it only means a cycle running
//! *through* buffered data is not collected.

use std::sync::{PoisonError, TryLockError};

use pyo3::prelude::*;
use pyo3::{PyTraverseError, PyVisit};

use crate::Stream;
use crate::node::{BuiltinOp, ChainNode, NodeLogic};

fn visit_builtin(op: &BuiltinOp, visit: &PyVisit<'_>) -> Result<(), PyTraverseError> {
    match op {
        BuiltinOp::IsInstance(obj)
        | BuiltinOp::Is(obj)
        | BuiltinOp::IsNot(obj)
        | BuiltinOp::ItemGetter(obj)
        | BuiltinOp::BinOp(_, obj)
        | BuiltinOp::Compare(_, obj)
        | BuiltinOp::Contains(obj)
        | BuiltinOp::NotContains(obj) => visit.call(obj),
        BuiltinOp::AttrGetter(name) | BuiltinOp::HasAttr(name) | BuiltinOp::MethodCall(name) => {
            visit.call(name)
        }
        BuiltinOp::MethodCall1(name, arg) => {
            visit.call(name)?;
            visit.call(arg)
        }
        BuiltinOp::MethodCall2(name, arg1, arg2) => {
            visit.call(name)?;
            visit.call(arg1)?;
            visit.call(arg2)
        }
        BuiltinOp::Slice(start, stop) => {
            visit.call(start)?;
            visit.call(stop)
        }
        BuiltinOp::TupleConstruct(ops) | BuiltinOp::ListConstruct(ops) => {
            ops.iter().try_for_each(|op| visit_builtin(op, visit))
        }
        BuiltinOp::Chain(a, b) | BuiltinOp::And(a, b) | BuiltinOp::Or(a, b) => {
            visit_builtin(a, visit)?;
            visit_builtin(b, visit)
        }
        BuiltinOp::Negate(inner) => visit_builtin(inner, visit),
        BuiltinOp::Len
        | BuiltinOp::Str
        | BuiltinOp::Int
        | BuiltinOp::Float
        | BuiltinOp::Bool
        | BuiltinOp::Not
        | BuiltinOp::Truth
        | BuiltinOp::Print
        | BuiltinOp::Type
        | BuiltinOp::Tuple
        | BuiltinOp::List
        | BuiltinOp::Set
        | BuiltinOp::FrozenSet => Ok(()),
    }
}

fn visit_logic(logic: &NodeLogic, visit: &PyVisit<'_>) -> Result<(), PyTraverseError> {
    match logic {
        NodeLogic::Map { func } | NodeLogic::Sink { func } | NodeLogic::BatchMap { func } => {
            visit.call(func)
        }
        NodeLogic::Filter { predicate } => visit.call(predicate),
        // `state` is replaced on every item, so it is not visited.
        NodeLogic::Accumulate { func, .. } => visit.call(func),
        // Items in flight change on every update, so they are not visited.
        NodeLogic::Prefetched { prefetcher } => visit.call(&prefetcher.func),
        NodeLogic::Chain { steps } => steps
            .iter()
            .try_for_each(|step| visit_chain_node(step, visit)),
        NodeLogic::ChunkBy {
            predicate,
            pre_buffer_chain,
            ..
        } => {
            visit.call(predicate)?;
            pre_buffer_chain
                .iter()
                .try_for_each(|step| visit_chain_node(step, visit))
        }
        NodeLogic::ExtractBetween { open, close, .. } => {
            visit.call(open)?;
            visit.call(close)
        }
        // Nothing to visit, or only buffers/state that propagation mutates.
        NodeLogic::Source
        | NodeLogic::Flatten
        | NodeLogic::Collect { .. }
        | NodeLogic::Tag { .. }
        | NodeLogic::CombineLatest { .. }
        | NodeLogic::Zip { .. }
        | NodeLogic::Parallel
        | NodeLogic::Prefetch { .. }
        | NodeLogic::ChainMember { .. } => Ok(()),
    }
}

fn visit_chain_node(step: &ChainNode, visit: &PyVisit<'_>) -> Result<(), PyTraverseError> {
    visit_logic(&step.logic, visit)?;
    if let Some(op) = &step.builtin_op {
        visit_builtin(op, visit)?;
    }
    visit.call(&step.flush_target)?;
    visit.call(&step.emit_target)
}

#[pymethods]
impl Stream {
    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        for downstream in &self.downstreams {
            visit.call(&downstream.node)?;
        }
        // Never block inside the GC. If another thread holds the lock, skip
        // the parents: the GC then treats them as externally referenced, so
        // the cycle is just not collected this time.
        let parents = match self.parents.try_lock() {
            Ok(guard) => Some(guard),
            Err(TryLockError::Poisoned(poisoned)) => Some(poisoned.into_inner()),
            Err(TryLockError::WouldBlock) => None,
        };
        if let Some(parents) = parents {
            for parent in parents.iter() {
                visit.call(parent)?;
            }
        }
        visit.call(&self.flush_target)?;
        visit.call(&self.emit_target)?;
        visit_logic(&self.logic, &visit)?;
        if let Some(op) = &self.builtin_op {
            visit_builtin(op, &visit)?;
        }
        Ok(())
    }

    fn __clear__(&mut self) {
        // Leave an inert, empty source behind in case anything still holds
        // this node (e.g. a finalizer that resurrected it).
        self.compiled_downstream_ptrs.clear();
        self.downstreams.clear();
        self.parents
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
        self.flush_target = None;
        self.emit_target = None;
        self.logic = NodeLogic::Source;
        self.builtin_op = None;
        self.cached_vectorcall = None;
        self.frozen = false;
        self.compiled_chainable = false;
        self.compiled_filter_fast = false;
    }
}
