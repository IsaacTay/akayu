//! Unboxed arithmetic for native op chains on exact ints and floats.
//!
//! A native op chain like `x * 2 + 1` or `x % 3 == 0` normally runs one
//! number-protocol call per step, allocating a Python int or float for every
//! intermediate result. When compile() finds a chain made only of arithmetic
//! and comparisons with numeric constants (on either side), `x * x`, `-x`,
//! `+x`, `~x` and `abs(x)`, it also builds a `NumProgram`: per item, an exact
//! int (fitting 64 bits) or float is unboxed once, every step runs on machine
//! numbers, and the result is boxed once.
//!
//! Every step is `v <op> c` or `v <op> c` compared: a constant on the left is
//! an op of its own (`c - v` is `RSub`) or, where the result is the same, the
//! op itself (`c + v`, `0 < v` as `v > 0`), `~v` is `v ^ -1`, and `v * v`,
//! `-v` and `abs(v)` are ops that leave the constant unused. Adding kinds of
//! step instead cost a long arithmetic chain 9-29% more instructions per
//! item: the loop's match on the step became a jump table before arith's own.
//!
//! Results must equal Python's exactly, so the program only handles what it
//! can do identically, and returns None otherwise. The caller then runs the
//! ordinary ops on the original item, which is safe because these steps have
//! no side effects on ints and floats. None is returned for: anything but an
//! exact int or float (bool included), int overflow, division by zero, `**`,
//! float `//` and `%`, bit operations on floats, negative shifts, `int / int`
//! or int-float comparisons beyond 2**53.

use pyo3::ffi;
use pyo3::prelude::*;
use std::sync::Arc;

use crate::node::{BinaryOp, BuiltinOp, CompareOp, UnaryOp};

#[derive(Clone, Copy)]
enum Num {
    Int(i64),
    Float(f64),
}

#[derive(Clone, Copy)]
enum Arith {
    Add,
    Sub,
    Mul,
    TrueDiv,
    FloorDiv,
    Mod,
    LShift,
    RShift,
    And,
    Or,
    Xor,
    // The constant on the left: v = c <op> v. (`c // v`, `c % v` and the
    // shifts are rare, and each op here costs every step a little.)
    RSub,
    RTrueDiv,
    // On v alone, the constant unused (~v is v ^ -1).
    /// v * v
    Square,
    /// -v
    Neg,
    /// abs(v)
    Abs,
}

/// The constant of a step on v alone.
const UNUSED: Num = Num::Int(0);

#[derive(Clone, Copy)]
enum Cmp {
    Eq,
    Ne,
    Lt,
    Gt,
    Le,
    Ge,
}

enum Step {
    Arith(Arith, Num),
    Cmp(Cmp, Num),
}

pub struct NumProgram(Vec<Step>);

/// A step's result on unboxed values.
enum Out {
    Num(Num),
    Bool(bool),
}

impl Out {
    /// Python truthiness: nonzero (a NaN is truthy).
    #[inline(always)]
    fn truth(&self) -> bool {
        match *self {
            Out::Bool(b) => b,
            Out::Num(Num::Int(i)) => i != 0,
            Out::Num(Num::Float(f)) => f != 0.0,
        }
    }
}

/// 2**53: integers up to this size convert to f64 exactly.
const EXACT: i64 = 1 << 53;

impl NumProgram {
    /// The program for a native op chain of numeric steps, if it is one.
    pub(crate) fn from_op(py: Python, op: &BuiltinOp) -> Option<Self> {
        let mut steps = Vec::new();
        collect(py, op, &mut steps)?;
        // A comparison produces a bool: only valid as the last step.
        let last = steps.len().checked_sub(1)?;
        if steps[..last].iter().any(|s| matches!(s, Step::Cmp(..))) {
            return None;
        }
        Some(Self(steps))
    }

    /// Worth running on its own: one arithmetic step saves nothing over the
    /// number protocol (it still boxes once), so it only joins runs; a
    /// comparison makes no number at all.
    pub(crate) fn worth_alone(&self) -> bool {
        self.0.len() >= 2 || self.ends_with_compare()
    }

    fn ends_with_compare(&self) -> bool {
        matches!(self.0.last(), Some(Step::Cmp(..)))
    }

    /// Run on `x`, or None to use the ordinary ops instead.
    pub(crate) fn run(&self, py: Python, x: &Py<PyAny>) -> Option<Py<PyAny>> {
        match self.eval(unbox(x)?)? {
            Out::Num(v) => boxed(py, v),
            Out::Bool(b) => Some(
                pyo3::types::PyBool::new(py, b)
                    .to_owned()
                    .into_any()
                    .unbind(),
            ),
        }
    }

    /// The truth of the result on `x`, as a filter sees it, or None to use
    /// the ordinary ops instead. No Python bool is made.
    pub(crate) fn truth(&self, x: &Py<PyAny>) -> Option<bool> {
        Some(self.eval(unbox(x)?)?.truth())
    }

    #[inline(always)]
    fn eval(&self, mut v: Num) -> Option<Out> {
        for step in &self.0 {
            match *step {
                Step::Arith(op, c) => v = arith(v, op, c)?,
                Step::Cmp(op, c) => return Some(Out::Bool(compare(v, op, c)?)),
            }
        }
        Some(Out::Num(v))
    }
}

fn boxed(py: Python, v: Num) -> Option<Py<PyAny>> {
    // SAFETY: GIL held; both constructors return a new reference, or NULL on
    // memory error: then None, and the ordinary path raises it again.
    unsafe {
        let ptr = match v {
            Num::Int(i) => ffi::PyLong_FromLongLong(i),
            Num::Float(f) => ffi::PyFloat_FromDouble(f),
        };
        if ptr.is_null() {
            ffi::PyErr_Clear();
        }
        Py::from_owned_ptr_or_opt(py, ptr)
    }
}

/// Consecutive map/filter steps of a fused chain that all have programs:
/// the item stays unboxed from the first step to the last. The map steps'
/// operations are laid out flat, one loop iteration each.
pub struct NumRun {
    ops: Vec<RunOp>,
    /// Chain steps the run covers.
    steps: usize,
}

/// One operation of a run on the unboxed item.
enum RunOp {
    /// item = item <op> c
    Arith(Arith, Num),
    /// item = (item <op> c): only a run's last operation.
    Cmp(Cmp, Num),
    /// A filter step: its program runs on a copy of the item, which is
    /// dropped unless the result is truthy.
    Filter(NumProgram),
}

/// What a run did with one item.
pub(crate) enum RunOutcome {
    Value(Py<PyAny>),
    Filtered,
    /// Not for the fast path: the untouched item, for the ordinary steps.
    Fallback(Py<PyAny>),
}

/// Runs (or None: no run) by the ops of the chain steps they were made
/// from, so graphs with the same steps share one: a run is a few allocations,
/// a third of a small compiled graph's memory. Entries hold their ops, so a
/// key's addresses can't be reused by other ops while it lives.
type RunKey = Box<[(bool, usize)]>;
type RunOps = Box<[Arc<BuiltinOp>]>;
static RUNS: crate::helpers::InternTable<RunKey, RunOps, Option<Arc<NumRun>>> =
    crate::helpers::InternTable::new(256, 1024);

impl NumRun {
    /// How many chain steps the run covers, from the one it is set on.
    pub(crate) fn steps(&self) -> usize {
        self.steps
    }

    /// The shared run starting at the first of `steps`, (is_filter, op) for
    /// consecutive chain steps (see `from_steps`).
    pub(crate) fn shared<'a>(
        py: Python,
        steps: impl Iterator<Item = (bool, &'a Arc<BuiltinOp>)>,
    ) -> Option<Arc<Self>> {
        let steps: Vec<_> = steps.collect();
        let key: Box<[_]> = steps
            .iter()
            .map(|(f, op)| (*f, Arc::as_ptr(op) as usize))
            .collect();
        if let Some(run) = RUNS.get(&key, |_| true) {
            return run;
        }
        let run = Self::from_steps(py, steps.iter().map(|(f, op)| (*f, &***op))).map(Arc::new);
        let ops = steps.iter().map(|(_, op)| Arc::clone(op)).collect();
        // In use: some graph still holds every op the run was made from.
        let in_use = |ops: &RunOps| ops.iter().all(|op| Arc::strong_count(op) > 1);
        RUNS.insert(key, ops, run, in_use)
    }

    /// `steps` are (is_filter, op) for consecutive chain steps; the run is the
    /// longest prefix with programs (a map yielding a bool ends it).
    fn from_steps<'a>(
        py: Python,
        steps: impl Iterator<Item = (bool, &'a BuiltinOp)>,
    ) -> Option<Self> {
        let (mut ops, mut covered) = (Vec::new(), 0);
        for (is_filter, op) in steps {
            let op = match op {
                BuiltinOp::Numeric(_, generic) => generic,
                op => op,
            };
            let Some(program) = NumProgram::from_op(py, op) else {
                break;
            };
            covered += 1;
            if is_filter {
                ops.push(RunOp::Filter(program));
                continue;
            }
            let yields_bool = program.ends_with_compare();
            ops.extend(program.0.into_iter().map(|step| match step {
                Step::Arith(op, c) => RunOp::Arith(op, c),
                Step::Cmp(op, c) => RunOp::Cmp(op, c),
            }));
            if yields_bool {
                break;
            }
        }
        (covered >= 2).then_some(Self {
            ops,
            steps: covered,
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.steps
    }

    /// Run `x` through every step. Out of line and taking the item, so that
    /// the caller's loop drops nothing: a `Py` drop there makes every chain
    /// call look up pyo3's thread-local GIL count, run or no run.
    #[inline(never)]
    pub(crate) fn run(&self, py: Python, x: Py<PyAny>) -> RunOutcome {
        match self.apply(py, &x) {
            Some(Some(v)) => RunOutcome::Value(v),
            Some(None) => RunOutcome::Filtered,
            None => RunOutcome::Fallback(x),
        }
    }

    /// Some(Some(value)), Some(None) if filtered out, None for the ordinary
    /// steps.
    #[inline(always)]
    fn apply(&self, py: Python, x: &Py<PyAny>) -> Option<Option<Py<PyAny>>> {
        let mut v = unbox(x)?;
        for op in &self.ops {
            match op {
                RunOp::Arith(op, c) => v = arith(v, *op, *c)?,
                RunOp::Cmp(op, c) => {
                    let b = compare(v, *op, *c)?;
                    return Some(Some(
                        pyo3::types::PyBool::new(py, b)
                            .to_owned()
                            .into_any()
                            .unbind(),
                    ));
                }
                RunOp::Filter(program) => {
                    if !program.eval(v)?.truth() {
                        return Some(None);
                    }
                }
            }
        }
        Some(Some(boxed(py, v)?))
    }

    /// Run every item through the steps, keeping the survivors in order. If
    /// any item can't use the fast path, Err gives back the untouched batch
    /// for the ordinary steps, so errors surface exactly as before.
    #[inline(never)]
    pub(crate) fn run_batch(
        &self,
        py: Python,
        items: Vec<Py<PyAny>>,
    ) -> Result<Vec<Py<PyAny>>, Vec<Py<PyAny>>> {
        let mut out = Vec::with_capacity(items.len());
        for x in &items {
            match self.apply(py, x) {
                Some(Some(v)) => out.push(v),
                Some(None) => {}
                None => return Err(items),
            }
        }
        Ok(out)
    }
}

fn collect(py: Python, op: &BuiltinOp, steps: &mut Vec<Step>) -> Option<()> {
    match op {
        BuiltinOp::Chain(a, b) => {
            collect(py, a, steps)?;
            collect(py, b, steps)
        }
        BuiltinOp::BinOp(op, c) => {
            steps.push(Step::Arith(arith_op(op)?, unbox(c)?));
            Some(())
        }
        BuiltinOp::RBinOp(op, c) => {
            let c = unbox(c)?;
            let op = match arith_op(op)? {
                // The same result, but for which NaN comes back when both
                // are NaN: only v can be.
                op @ (Arith::Add | Arith::Mul | Arith::And | Arith::Or | Arith::Xor) => {
                    if matches!(c, Num::Float(f) if f.is_nan()) {
                        return None;
                    }
                    op
                }
                Arith::Sub => Arith::RSub,
                Arith::TrueDiv => Arith::RTrueDiv,
                _ => return None,
            };
            steps.push(Step::Arith(op, c));
            Some(())
        }
        BuiltinOp::BinOpOf(BinaryOp::Mul, a, b)
            if matches!((&**a, &**b), (BuiltinOp::Item, BuiltinOp::Item)) =>
        {
            steps.push(Step::Arith(Arith::Square, UNUSED));
            Some(())
        }
        BuiltinOp::Unary(op) => {
            steps.push(match op {
                // The same number, but a comparison's bool becomes an int.
                UnaryOp::Pos if steps.last().is_some_and(|s| matches!(s, Step::Cmp(..))) => {
                    return None;
                }
                UnaryOp::Pos => return Some(()),
                UnaryOp::Neg => Step::Arith(Arith::Neg, UNUSED),
                // The same on ints, and floats raise TypeError either way.
                UnaryOp::Invert => Step::Arith(Arith::Xor, Num::Int(-1)),
                UnaryOp::Abs => Step::Arith(Arith::Abs, UNUSED),
            });
            Some(())
        }
        BuiltinOp::Compare(op, c) => {
            steps.push(Step::Cmp(cmp_op(op), unbox(c)?));
            let _ = py;
            Some(())
        }
        // c < v is v > c, NaN included.
        BuiltinOp::RCompare(op, c) => {
            let op = match cmp_op(op) {
                Cmp::Lt => Cmp::Gt,
                Cmp::Gt => Cmp::Lt,
                Cmp::Le => Cmp::Ge,
                Cmp::Ge => Cmp::Le,
                op @ (Cmp::Eq | Cmp::Ne) => op,
            };
            steps.push(Step::Cmp(op, unbox(c)?));
            Some(())
        }
        _ => None,
    }
}

fn arith_op(op: &BinaryOp) -> Option<Arith> {
    Some(match op {
        BinaryOp::Add => Arith::Add,
        BinaryOp::Sub => Arith::Sub,
        BinaryOp::Mul => Arith::Mul,
        BinaryOp::TrueDiv => Arith::TrueDiv,
        BinaryOp::FloorDiv => Arith::FloorDiv,
        BinaryOp::Mod => Arith::Mod,
        BinaryOp::LShift => Arith::LShift,
        BinaryOp::RShift => Arith::RShift,
        BinaryOp::And => Arith::And,
        BinaryOp::Or => Arith::Or,
        BinaryOp::Xor => Arith::Xor,
        BinaryOp::Pow => return None,
    })
}

fn cmp_op(op: &CompareOp) -> Cmp {
    match op {
        CompareOp::Eq => Cmp::Eq,
        CompareOp::Ne => Cmp::Ne,
        CompareOp::Lt => Cmp::Lt,
        CompareOp::Gt => Cmp::Gt,
        CompareOp::Le => Cmp::Le,
        CompareOp::Ge => Cmp::Ge,
    }
}

#[inline(always)]
fn unbox(x: &Py<PyAny>) -> Option<Num> {
    let p = x.as_ptr();
    // SAFETY: GIL held by every caller; exact type checks on a live object.
    unsafe {
        if ffi::PyLong_CheckExact(p) != 0 {
            let mut overflow = 0;
            let v = ffi::PyLong_AsLongLongAndOverflow(p, &raw mut overflow);
            return (overflow == 0).then_some(Num::Int(v));
        }
        if ffi::PyFloat_CheckExact(p) != 0 {
            return Some(Num::Float(ffi::PyFloat_AS_DOUBLE(p)));
        }
    }
    None
}

/// Python's int -> float conversion (correctly rounded), as `i64 as f64` is.
#[inline(always)]
fn float(v: Num) -> f64 {
    match v {
        #[allow(clippy::cast_precision_loss)]
        Num::Int(i) => i as f64,
        Num::Float(f) => f,
    }
}

/// `a <op> b` (`b <op> a` for the R ops) on unboxed numbers, as Python
/// computes it, or None.
#[inline(always)]
fn arith(a: Num, op: Arith, b: Num) -> Option<Num> {
    if let (Num::Int(a), Num::Int(b)) = (a, b) {
        return Some(Num::Int(match op {
            Arith::Add => a.checked_add(b)?,
            Arith::Sub => a.checked_sub(b)?,
            Arith::RSub => b.checked_sub(a)?,
            Arith::Mul => a.checked_mul(b)?,
            Arith::Square => a.checked_mul(a)?,
            Arith::FloorDiv => floor_div(a, b)?,
            Arith::Mod => modulo(a, b)?,
            Arith::TrueDiv => return true_div(a, b),
            Arith::RTrueDiv => return true_div(b, a),
            Arith::LShift => shl(a, b)?,
            Arith::RShift => shr(a, b)?,
            Arith::And => a & b,
            Arith::Or => a | b,
            Arith::Xor => a ^ b,
            Arith::Neg => a.checked_neg()?,
            Arith::Abs => a.checked_abs()?,
        }));
    }
    let (a, b) = (float(a), float(b));
    Some(Num::Float(match op {
        Arith::Add => a + b,
        Arith::Sub => a - b,
        Arith::RSub => b - a,
        Arith::Mul => a * b,
        Arith::Square => a * a,
        Arith::TrueDiv if b != 0.0 => a / b,
        Arith::RTrueDiv if a != 0.0 => b / a,
        Arith::Neg => -a,
        Arith::Abs => a.abs(),
        // Division by zero raises; float // and % have their own rules; bit
        // operations raise TypeError.
        Arith::TrueDiv
        | Arith::RTrueDiv
        | Arith::FloorDiv
        | Arith::Mod
        | Arith::LShift
        | Arith::RShift
        | Arith::And
        | Arith::Or
        | Arith::Xor => return None,
    }))
}

/// Python's `a // b` on ints (rounding down), or None for b == 0 and overflow.
#[inline(always)]
fn floor_div(a: i64, b: i64) -> Option<i64> {
    let q = a.checked_div(b)?;
    let r = a.checked_rem(b)?;
    Some(if r != 0 && (r < 0) != (b < 0) {
        q - 1
    } else {
        q
    })
}

/// Python's `a % b` on ints (the sign of b), or None for b == 0.
#[inline(always)]
fn modulo(a: i64, b: i64) -> Option<i64> {
    let r = a.checked_rem(b)?;
    Some(if r != 0 && (r < 0) != (b < 0) {
        r + b
    } else {
        r
    })
}

/// Python's `a / b` on ints, while both convert to f64 exactly.
#[inline(always)]
fn true_div(a: i64, b: i64) -> Option<Num> {
    if b == 0 || a.unsigned_abs() > EXACT as u64 || b.unsigned_abs() > EXACT as u64 {
        return None;
    }
    #[allow(clippy::cast_precision_loss)]
    Some(Num::Float(a as f64 / b as f64))
}

/// `a << b`, or None for a negative count (ValueError) and a result beyond
/// 64 bits (a bigger int).
#[inline(always)]
fn shl(a: i64, b: i64) -> Option<i64> {
    let b = u32::try_from(b).ok()?;
    if a == 0 {
        return Some(0);
    }
    let r = a.checked_shl(b)?;
    (r >> b == a).then_some(r)
}

/// `a >> b`, or None for a negative count (ValueError).
#[inline(always)]
fn shr(a: i64, b: i64) -> Option<i64> {
    (b >= 0).then(|| a >> b.min(63))
}

#[inline(always)]
fn compare(a: Num, op: Cmp, b: Num) -> Option<bool> {
    let ord = match (a, b) {
        (Num::Int(a), Num::Int(b)) => Some(a.cmp(&b)),
        (Num::Float(a), Num::Float(b)) => a.partial_cmp(&b),
        // Python compares int and float exactly: only safe while exact.
        (Num::Int(i), Num::Float(_)) | (Num::Float(_), Num::Int(i)) => {
            if i.unsigned_abs() > EXACT as u64 {
                return None;
            }
            float(a).partial_cmp(&float(b))
        }
    };
    use std::cmp::Ordering::{Equal, Greater, Less};
    // None: a NaN is involved; only != is true then.
    Some(match (op, ord) {
        (Cmp::Ne, None) => true,
        (_, None) => false,
        (Cmp::Eq, Some(o)) => o == Equal,
        (Cmp::Ne, Some(o)) => o != Equal,
        (Cmp::Lt, Some(o)) => o == Less,
        (Cmp::Gt, Some(o)) => o == Greater,
        (Cmp::Le, Some(o)) => o != Greater,
        (Cmp::Ge, Some(o)) => o != Less,
    })
}
