//! Unboxed arithmetic for native op chains on exact ints and floats.
//!
//! A native op chain like `x * 2 + 1` or `x % 3 == 0` normally runs one
//! number-protocol call per step, allocating a Python int or float for every
//! intermediate result. When compile() finds a chain made only of arithmetic
//! and comparisons with numeric constants, it also builds a `NumProgram`: per
//! item, an exact int (fitting 64 bits) or float is unboxed once, every step
//! runs on machine numbers, and the result is boxed once.
//!
//! Results must equal Python's exactly, so the program only handles what it
//! can do identically, and returns None otherwise. The caller then runs the
//! ordinary ops on the original item, which is safe because these steps have
//! no side effects on ints and floats. None is returned for: anything but an
//! exact int or float (bool included), int overflow, division by zero, `**`,
//! float `//` and `%`, `int / int` or int-float comparisons beyond 2**53.

use pyo3::ffi;
use pyo3::prelude::*;

use crate::node::{BinaryOp, BuiltinOp, CompareOp};

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
}

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

    /// Worth running on its own: one step saves nothing over the number
    /// protocol (it still boxes once), so single steps only join runs.
    pub(crate) fn worth_alone(&self) -> bool {
        self.0.len() >= 2
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
/// the item stays unboxed from the first step to the last.
pub struct NumRun(Vec<(bool, NumProgram)>);

/// What a run did with one item.
pub(crate) enum RunOutcome {
    Value(Py<PyAny>),
    Filtered,
    /// Not for the fast path: the untouched item, for the ordinary steps.
    Fallback(Py<PyAny>),
}

impl NumRun {
    /// `steps` are (is_filter, op) for consecutive chain steps; the run is the
    /// longest prefix with programs (a map yielding a bool ends it).
    pub(crate) fn from_steps<'a>(
        py: Python,
        steps: impl Iterator<Item = (bool, &'a BuiltinOp)>,
    ) -> Option<Self> {
        let mut run = Vec::new();
        for (is_filter, op) in steps {
            let op = match op {
                BuiltinOp::Numeric(_, generic) => generic,
                op => op,
            };
            let Some(program) = NumProgram::from_op(py, op) else {
                break;
            };
            let yields_bool = !is_filter && program.ends_with_compare();
            run.push((is_filter, program));
            if yields_bool {
                break;
            }
        }
        (run.len() >= 2).then_some(Self(run))
    }

    pub(crate) fn len(&self) -> usize {
        self.0.len()
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
    fn apply(&self, py: Python, x: &Py<PyAny>) -> Option<Option<Py<PyAny>>> {
        let mut v = unbox(x)?;
        for (is_filter, program) in &self.0 {
            match (*is_filter, program.eval(v)?) {
                (false, Out::Num(n)) => v = n,
                (false, Out::Bool(b)) => {
                    // Only a run's last step yields a bool (from_steps).
                    return Some(Some(
                        pyo3::types::PyBool::new(py, b)
                            .to_owned()
                            .into_any()
                            .unbind(),
                    ));
                }
                (true, out) => {
                    let keep = match out {
                        Out::Bool(b) => b,
                        // Truthiness: nonzero (a NaN is truthy).
                        Out::Num(Num::Int(i)) => i != 0,
                        Out::Num(Num::Float(f)) => f != 0.0,
                    };
                    if !keep {
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
            let op = match op {
                BinaryOp::Add => Arith::Add,
                BinaryOp::Sub => Arith::Sub,
                BinaryOp::Mul => Arith::Mul,
                BinaryOp::TrueDiv => Arith::TrueDiv,
                BinaryOp::FloorDiv => Arith::FloorDiv,
                BinaryOp::Mod => Arith::Mod,
                BinaryOp::Pow => return None,
            };
            steps.push(Step::Arith(op, unbox(c)?));
            Some(())
        }
        BuiltinOp::Compare(op, c) => {
            let op = match op {
                CompareOp::Eq => Cmp::Eq,
                CompareOp::Ne => Cmp::Ne,
                CompareOp::Lt => Cmp::Lt,
                CompareOp::Gt => Cmp::Gt,
                CompareOp::Le => Cmp::Le,
                CompareOp::Ge => Cmp::Ge,
            };
            steps.push(Step::Cmp(op, unbox(c)?));
            let _ = py;
            Some(())
        }
        _ => None,
    }
}

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
fn float(v: Num) -> f64 {
    match v {
        #[allow(clippy::cast_precision_loss)]
        Num::Int(i) => i as f64,
        Num::Float(f) => f,
    }
}

fn arith(a: Num, op: Arith, b: Num) -> Option<Num> {
    if let (Num::Int(a), Num::Int(b)) = (a, b) {
        return Some(match op {
            Arith::Add => Num::Int(a.checked_add(b)?),
            Arith::Sub => Num::Int(a.checked_sub(b)?),
            Arith::Mul => Num::Int(a.checked_mul(b)?),
            Arith::FloorDiv => {
                let q = a.checked_div(b)?;
                let r = a.checked_rem(b)?;
                Num::Int(if r != 0 && (r < 0) != (b < 0) {
                    q - 1
                } else {
                    q
                })
            }
            Arith::Mod => {
                let r = a.checked_rem(b)?;
                Num::Int(if r != 0 && (r < 0) != (b < 0) {
                    r + b
                } else {
                    r
                })
            }
            Arith::TrueDiv => {
                if b == 0 || a.unsigned_abs() > EXACT as u64 || b.unsigned_abs() > EXACT as u64 {
                    return None;
                }
                #[allow(clippy::cast_precision_loss)]
                Num::Float(a as f64 / b as f64)
            }
        });
    }
    let (a, b) = (float(a), float(b));
    Some(Num::Float(match op {
        Arith::Add => a + b,
        Arith::Sub => a - b,
        Arith::Mul => a * b,
        Arith::TrueDiv if b != 0.0 => a / b,
        // Division by zero raises; float // and % have their own rules.
        Arith::TrueDiv | Arith::FloorDiv | Arith::Mod => return None,
    }))
}

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
