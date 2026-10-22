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
//!
//! Every struct visited here is destructured field by field, without `..`: a
//! new field holding a Python reference fails to compile until it is either
//! visited or listed as skipped.

use std::sync::{PoisonError, TryLockError};

use pyo3::prelude::*;
use pyo3::{PyTraverseError, PyVisit};

use crate::Stream;
use crate::node::{
    BuiltinOp, ChainNode, ChunkByState, EagerOp, ExtractBetweenState, MethodCall, NodeLogic,
};

fn visit_builtin(op: &BuiltinOp, visit: &PyVisit<'_>) -> Result<(), PyTraverseError> {
    match op {
        BuiltinOp::IsInstance(obj)
        | BuiltinOp::AccumBinOpConst(_, obj)
        | BuiltinOp::ListAppend(obj)
        | BuiltinOp::Is(obj)
        | BuiltinOp::IsNot(obj)
        | BuiltinOp::ItemGetter(obj)
        | BuiltinOp::BinOp(_, obj)
        | BuiltinOp::Compare(_, obj)
        | BuiltinOp::RBinOp(_, obj)
        | BuiltinOp::RCompare(_, obj)
        | BuiltinOp::GetItemFrom(obj)
        | BuiltinOp::Contains(obj)
        | BuiltinOp::NotContains(obj) => visit.call(obj),
        BuiltinOp::AttrGetter(name) | BuiltinOp::HasAttr(name) => visit.call(name),
        BuiltinOp::MethodCall(call) => {
            let MethodCall {
                name,
                args,
                direct,
                pure: _,
            } = &**call;
            visit.call(name)?;
            args.iter().try_for_each(|arg| visit.call(arg))?;
            direct.iter().try_for_each(|(tp, method)| {
                visit.call(tp)?;
                visit.call(method)
            })
        }
        BuiltinOp::Slice(start, stop) => {
            visit.call(start)?;
            visit.call(stop)
        }
        BuiltinOp::TupleConstruct(ops) | BuiltinOp::ListConstruct(ops) => {
            ops.iter().try_for_each(|op| visit_builtin(op, visit))
        }
        // The program holds only unboxed numbers.
        BuiltinOp::Numeric(_, generic) => visit_builtin(generic, visit),
        BuiltinOp::Arrays(op) => {
            op.objects().try_for_each(|obj| visit.call(obj))?;
            match &op.fallback {
                crate::arrays::Fallback::Op(op) => visit_builtin(op, visit),
                _ => Ok(()),
            }
        }
        BuiltinOp::Eager(eager) => {
            let EagerOp { func, code, op } = eager;
            visit.call(func)?;
            visit.call(code)?;
            visit_builtin(op, visit)
        }
        BuiltinOp::Chain(a, b)
        | BuiltinOp::And(a, b)
        | BuiltinOp::Or(a, b)
        | BuiltinOp::BinOpOf(_, a, b)
        | BuiltinOp::CompareOf(_, a, b) => {
            visit_builtin(a, visit)?;
            visit_builtin(b, visit)
        }
        BuiltinOp::Negate(inner) => visit_builtin(inner, visit),
        BuiltinOp::Len
        | BuiltinOp::Item
        | BuiltinOp::Unary(_)
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
        | BuiltinOp::FrozenSet
        | BuiltinOp::AccumBinOp(_)
        | BuiltinOp::AccumMax
        | BuiltinOp::AccumMin => Ok(()),
    }
}

fn visit_logic(logic: &NodeLogic, visit: &PyVisit<'_>) -> Result<(), PyTraverseError> {
    match logic {
        NodeLogic::Map { func }
        | NodeLogic::StarMap { func }
        | NodeLogic::Sink { func }
        | NodeLogic::BatchMap { func } => visit.call(func),
        NodeLogic::Filter { predicate } => visit.call(predicate),
        // `state` is replaced on every item, so it is not visited.
        NodeLogic::Accumulate {
            func,
            state: _,
            returns_state: _,
        } => visit.call(func),
        NodeLogic::Prefetched { prefetcher } => prefetcher.traverse(visit),
        NodeLogic::Chain { steps } => steps
            .iter()
            .try_for_each(|step| visit_chain_node(step, visit)),
        NodeLogic::ChunkBy(boxed) => {
            let ChunkByState {
                predicate,
                pre_buffer_chain,
                buffer: _,
                order: _,
            } = &**boxed;
            visit.call(predicate)?;
            pre_buffer_chain
                .iter()
                .try_for_each(|step| visit_chain_node(step, visit))
        }
        NodeLogic::ExtractBetween(boxed) => {
            let ExtractBetweenState {
                open,
                close,
                buffer: _,
                inside: _,
                close_vectorcall: _,
            } = &**boxed;
            visit.call(open)?;
            visit.call(close)
        }
        // Nothing to visit, or only buffers/state that propagation mutates.
        NodeLogic::Collect { buffer: _ }
        | NodeLogic::CombineLatest(_)
        | NodeLogic::Zip { buffers: _ }
        | NodeLogic::Source
        | NodeLogic::Flatten
        | NodeLogic::Tag { index: _ }
        | NodeLogic::Parallel
        | NodeLogic::Prefetch { size: _ }
        | NodeLogic::ChainMember { was_sink: _ } => Ok(()),
    }
}

fn visit_chain_node(step: &ChainNode, visit: &PyVisit<'_>) -> Result<(), PyTraverseError> {
    let ChainNode {
        logic,
        builtin_op,
        flush_target,
        emit_target,
        name: _,
        source_location: _,
        node_id: _,
        cached_vectorcall: _,
        numeric_run: _,
    } = step;
    visit_logic(logic, visit)?;
    if let Some(op) = builtin_op {
        visit_builtin(op, visit)?;
    }
    visit.call(flush_target)?;
    visit.call(emit_target)
}

#[pymethods]
impl Stream {
    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        let Stream {
            logic,
            downstreams,
            parents,
            flush_target,
            emit_target,
            builtin_op,
            name: _,
            locks_children: _,
            compiled: _,
            source_location: _,
            cached_vectorcall: _,
            chainable: _,
            batch_fanout: _,
            flushing: _,
            emitting: _,
            busy: _,
            guard_reentry: _,
            lock: _,
        } = self;
        for downstream in downstreams {
            visit.call(&downstream.node)?;
        }
        // Never block inside the GC. If another thread holds the lock, skip
        // the parents: the GC then treats them as externally referenced, so
        // the cycle is just not collected this time.
        let parents = match parents.try_lock() {
            Ok(guard) => Some(guard),
            Err(TryLockError::Poisoned(poisoned)) => Some(poisoned.into_inner()),
            Err(TryLockError::WouldBlock) => None,
        };
        if let Some(parents) = parents {
            for parent in parents.iter() {
                visit.call(parent)?;
            }
        }
        visit.call(flush_target)?;
        visit.call(emit_target)?;
        visit_logic(logic, &visit)?;
        if let Some(op) = builtin_op {
            visit_builtin(op, &visit)?;
        }
        Ok(())
    }

    fn __clear__(&mut self) {
        // Leave an inert, empty source behind in case anything still holds
        // this node (e.g. a finalizer that resurrected it).
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
        self.chainable = false;
    }
}
