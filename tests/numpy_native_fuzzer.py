"""The differential fuzzer for compile()'s numpy programs (src/arrays.rs; run
by test_numpy_native.py).

Random numpy expressions of one or two arguments (`lambda x:` for map,
`lambda x, y:` for starmap), over small float64 arrays of every layout and
awkward values, and over items the programs don't take, run by akayu
(compiled) and by plain Python. They must agree on everything a caller can
see: each result's type, dtype, shape, strides, flags, bytes (NaN payloads
included) and base, the warnings numpy gave, and every exception.

AKAYU_NP_FUZZ_SEEDS sets the number of seeds (default 30).
"""

import os
import random
import warnings

import numpy as np
from support import innermost

import akayu

SEEDS = int(os.environ.get("AKAYU_NP_FUZZ_SEEDS", "30"))
CHUNK = 10
EXPRS_PER_SEED = 20
ITEMS = 12

SPECIAL = (0.0, -0.0, 1.0, -1.0, np.inf, -np.inf, np.nan, 5e-324, 1e308, -1e308, 1e-300, 0.5, 2.0)
# A NaN with another payload, and a negative one: which comes back is visible.
ODD_NANS = (np.frombuffer(b"\x01\x00\x00\x00\x00\x00\xf8\x7f")[0], -np.float64(np.nan))

UFUNCS1 = ("sqrt", "degrees", "radians", "arccos", "arcsin", "arctan", "sin", "cos", "exp", "log", "abs", "negative")
UFUNCS2 = ("arctan2", "hypot", "maximum", "minimum", "add", "multiply", "divide")

C = np.random.default_rng(1).standard_normal((3, 3))
V = np.array([0.5, -1.5, 2.0])
CONSTS = {"C": C, "V": V, "K": 0.25, "N": np.float64(-2.0), "np": np}


def values(rng, shape):
    a = np.array([rng.gauss(0, 1) * 10.0 ** rng.randint(-4, 4) for _ in range(int(np.prod(shape)))])
    for i in range(a.size):
        r = rng.random()
        if r < 0.06:
            a.flat[i] = rng.choice(SPECIAL)
        elif r < 0.07:
            a.flat[i] = rng.choice(ODD_NANS)
    return a.reshape(shape)


def array(rng, shape):
    """A float64 array of `shape` in one of the layouts items come in."""
    kind = rng.randrange(9)
    if kind == 0:
        return np.asfortranarray(values(rng, shape))
    if kind == 1 and len(shape) == 2:
        return values(rng, shape[::-1]).T  # a transposed view
    if kind == 2:
        return values(rng, (3, *shape))[1]  # a view into a bigger array
    if kind == 3 and len(shape) == 1:
        return values(rng, (shape[0] * 2,))[::2]  # strided
    if kind == 4:
        a = values(rng, shape)
        a.flags.writeable = False
        return a
    if kind == 5:
        return np.frombuffer(values(rng, shape).tobytes()).reshape(shape)  # read-only, bytes-backed
    return values(rng, shape)


def foreign(rng):
    """An item the programs don't take (their function must run instead)."""
    return rng.choice(
        [
            lambda: values(rng, (3,)).astype(np.float32),
            lambda: np.arange(3),
            lambda: values(rng, (2, 2, 2)),
            lambda: values(rng, (5, 5)),
            lambda: np.asmatrix(values(rng, (3, 3))),
            lambda: values(rng, (3,))[::-1],
            lambda: np.frombuffer(b"\x00" + values(rng, (3,)).tobytes(), offset=1),  # unaligned
            lambda: np.array(rng.random()),
            lambda: rng.random(),
            lambda: np.float64(rng.random()),
            lambda: 3,
            lambda: None,
            lambda: [1.0, 2.0, 3.0],
            lambda: values(rng, (3,)).tobytes()[:20],
        ]
    )()


def item(rng):
    r = rng.random()
    if r < 0.08:
        return foreign(rng)
    if r < 0.16:
        return rng.choice((float(rng.choice(SPECIAL)), np.float64(rng.gauss(0, 2))))
    if r < 0.3:
        return values(rng, (3, 3)).tobytes()
    # Mostly the shapes of the constants, so that expressions combine.
    shape = rng.choice([(3,), (3,), (3, 3), (3, 3), (3, 3), (3, 3), (2,), (2, 3), (3, 1), (1, 3), (4, 4)])
    return array(rng, shape)


def expression(rng, args, depth=0):
    """Mostly numpy expressions the programs translate, some they don't."""
    leaf = depth >= 3 or rng.random() < 0.3
    if leaf:
        return rng.choice([*args, *args, "C", "V", "K", "N", "2", "0.5"])
    e = lambda: expression(rng, args, depth + 1)  # noqa: E731
    k = rng.randrange(16)
    if k < 4:
        return f"({e()} {rng.choice(('+', '-', '*', '/'))} {e()})"
    if k == 4:
        return f"({e()} @ {e()})"
    if k == 5:
        return f"(-{e()})"
    if k == 6:
        return f"({e()}).T"
    if k == 7:
        return f"np.{rng.choice(UFUNCS1)}({e()})"
    if k == 8:
        return f"np.{rng.choice(UFUNCS2)}({e()}, {e()})"
    if k == 9:
        return f"np.clip({e()}, {rng.choice(('-1.0', '-1', 'K'))}, {rng.choice(('1.0', '2', 'N'))})"
    if k == 10:
        return f"np.linalg.norm({e()})"
    if k == 11:
        return f"np.trace({e()})"
    if k == 12:
        key = rng.choice(("0", "-1", "1, 0", "0, 2", ":, 1", "2, :", ":", "3", "1, -1", "::2"))
        return f"({e()})[{key}]"
    if k == 13:
        shape = rng.choice(("3, 3", "9", "-1, 3", "(3, 3)", "1, -1", "2, 2"))
        return f"({e()}).reshape({shape})"
    if k == 14:
        return f"np.frombuffer({rng.choice(args)}).reshape(3, 3)"
    # Outside the programs: their function runs.
    return rng.choice(
        [f"({e()} ** 2)", f"({e()} // 2)", f"np.sum({e()})", f"({e()}).copy()", f"np.clip({e()}, a_min=-1, a_max=1)"]
    )


def describe(v, inputs):
    """Everything a caller can see of a result (see the module docstring)."""
    if isinstance(v, BaseException):
        return ("raise", type(v).__name__, str(v))
    if isinstance(v, np.ndarray):
        base = v.base
        if base is None:
            b = None
        elif any(base is i for i in inputs):
            b = ("input", next(k for k, i in enumerate(inputs) if base is i))
        elif isinstance(base, np.ndarray):
            b = ("array", describe(base, inputs))
        else:
            b = ("other", type(base).__name__)
        f = v.flags
        flags = (f.c_contiguous, f.f_contiguous, f.owndata, f.writeable, f.aligned)
        return ("ndarray", type(v).__name__, str(v.dtype), v.shape, v.strides, flags, v.tobytes(), b)
    if isinstance(v, (float, np.generic)):
        return (type(v).__name__, np.asarray(v).tobytes())
    return (type(v).__name__, repr(v))


def outcome(call, inputs):
    """(result described, warnings) of `call()`."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        try:
            result = call()
        except Exception as e:  # every exception is an outcome
            result = innermost(e)
    return describe(result, inputs), [(w.category.__name__, str(w.message)) for w in caught]


def _at(values, offset):
    """A copy of 1-D `values` starting `offset` bytes past a 64-byte boundary."""
    buf = np.empty(values.size + 16)
    start = ((offset - buf.ctypes.data) % 64) // 8
    a = buf[start : start + values.size]
    a[:] = values
    return a


def _blas_varies():
    """Whether this numpy's BLAS gives different bits for the same product of
    the same values at different addresses (Apple's Accelerate on Intel Macs
    does): numpy's own @ and norms then differ from call to call, as its
    temporaries land wherever the allocator puts them."""
    rng = np.random.default_rng(0)
    m = rng.standard_normal((3, 3))
    for _ in range(300):
        n = int(rng.integers(3, 17))
        v = rng.standard_normal(n) * 10.0 ** rng.integers(-8, 9, n)
        dots = {_at(v, off).dot(_at(v, off)).tobytes() for off in (0, 16, 32, 48)}
        products = {(m @ _at(v[:3], off)).tobytes() for off in (0, 16, 32, 48)}
        if len(dots) > 1 or len(products) > 1:
            return True
    return False


BLAS_VARIES = _blas_varies()


def _floats_near(a, b):
    """float64 bytes equal but for one ulp, or a NaN's sign and payload."""
    if len(a) != len(b) or len(a) % 8:
        return False
    x, y = np.frombuffer(a), np.frombuffer(b)
    nan = np.isnan(x)
    if not np.array_equal(nan, np.isnan(y)):
        return False
    x, y = x[~nan], y[~nan]
    ulps = np.abs(x.view(np.int64) - y.view(np.int64))
    return bool(np.all((np.signbit(x) == np.signbit(y)) & (ulps <= 1)))


def near(a, b):
    """Outcomes the same but for float bits numpy's BLAS itself varies in."""
    if type(a) is not type(b):
        return False
    if isinstance(a, (tuple, list)):
        return len(a) == len(b) and all(near(x, y) for x, y in zip(a, b, strict=True))
    if isinstance(a, bytes):
        return a == b or _floats_near(a, b)
    return a == b


def same_outcome(got, want, src):
    """Outcomes equal: exactly, but where numpy's own results vary (see
    `_blas_varies`) for BLAS's ops, @ and norms, within that variation."""
    return got == want or (BLAS_VARIES and ("@" in src or "norm" in src) and near(got, want))


def expr_mismatches(src, items, nargs):
    """Where compiled akayu and Python disagree on the expression `src`."""
    params = "x, y" if nargs == 2 else "x"
    found = []
    f = eval(f"lambda {params}: {src}", dict(CONSTS))
    g = eval(f"lambda {params}: {src}", dict(CONSTS))  # its own code: analysis caches by code
    s, out = akayu.Stream(), []
    (s.starmap(g) if nargs == 2 else s.map(g)).sink(out.append)
    s.compile()
    for x in items:
        args = x if nargs == 2 else (x,)
        want = outcome(lambda args=args: f(*args), args)
        out.clear()

        def run(x=x):
            s.emit(x)
            return out[0]

        got = outcome(run, args)
        if not same_outcome(got, want, src):
            found.append(f"lambda {params}: {src} on {args!r}:\n  python {want}\n  akayu  {got}")
    return found


def mismatches(seed):
    rng = random.Random(seed)
    found = []
    for _ in range(EXPRS_PER_SEED):
        nargs = rng.choice((1, 1, 2))
        args = ("x", "y") if nargs == 2 else ("x",)
        src = expression(rng, args)
        items = [item(rng) for _ in range(ITEMS)]
        if nargs == 2:
            items = [(a, b) for a, b in zip(items, [item(rng) for _ in range(ITEMS)], strict=True)]
            items.append([items[0][0], items[0][1]])  # a list: star-called, not a program's
        found += expr_mismatches(src, items, nargs)
    return found
