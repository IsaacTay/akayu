"""CPU and memory cost of akayu per workload, independent of machine load.

CPU is counted as user-space instructions per source item with `perf stat`,
restricted to the emit loop itself (graph building and input creation are
excluded). Instruction counts don't move with machine load or clock speed, so
two runs can be compared even on a busy machine. Without perf, wall-clock time
is reported instead, and is only meaningful on an idle machine.

    python scripts/bench_cost.py                       # everything
    python scripts/bench_cost.py -w simple -w zip3     # some workloads
    python scripts/bench_cost.py --save before.json
    python scripts/bench_cost.py --compare before.json # show % change
    python scripts/bench_cost.py -w deep -m batch --record prof
    perf report -i prof/deep-batch-opaque.data       # where the instructions go

`build` counts building one 4-node graph (`compiled`: and compiling it), the
cost of per-request pipelines; it has no batch modes. Its `lambda` kind makes
new lambdas for every graph, as a request handler would.

Memory: bytes per graph node (RSS over many small graphs) and the peak extra
RSS of pushing one million items through a 10-map pipeline.
"""

import argparse
import gc
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from akayu._helpers import _analyze_accumulate, _analyze_lambda

import akayu

MODES = ("eager", "compiled", "batch", "batch-eager")
KINDS = ("opaque", "lambda")
N = {"emit": 50_000, "batch": 200_000}


# "opaque": ordinary functions the bytecode optimizer can't replace (two
# statements: _check_opaque() fails if it ever learns them).
# "lambda": simple lambdas it turns into native operations.
def _inc(x):
    y = x + 1
    return y


def _even(x):
    y = x % 2
    return y == 0


def _dbl(x):
    y = x * 2
    return y


def _tri(x):
    y = x * 3
    return y


def _add(a, x):
    y = a + x
    return y


def _pair(x):
    return (x, x)


def _sum2(a, b):
    return a + b


def _delim(x):
    y = x % 64
    return y == 0


def _mod0(x):
    y = x % 3
    return y == 0


def _mod1(x):
    y = x % 3
    return y == 1


def _batch(xs):
    return [x * 1 + 1 for x in xs]


FUNCS = {
    "opaque": {
        "inc": _inc,
        "even": _even,
        "dbl": _dbl,
        "tri": _tri,
        "delim": _delim,
        "mod0": _mod0,
        "mod1": _mod1,
        "add": _add,
    },
    "lambda": {
        "inc": lambda x: x + 1,
        "even": lambda x: x % 2 == 0,
        "dbl": lambda x: x * 2,
        "tri": lambda x: x * 3,
        "delim": lambda x: x % 64 == 0,
        "mod0": lambda x: x % 3 == 0,
        "mod1": lambda x: x % 3 == 1,
        "add": lambda acc, x: acc + x,
    },
}


def _check_opaque():
    """The "opaque" kind must really call Python functions."""
    native = [k for k, f in FUNCS["opaque"].items() if _analyze_lambda(f) or _analyze_accumulate(f)]
    if native:
        sys.exit(f"bench_cost: the optimizer now recognizes opaque functions {native}; make them opaque again")


def _workloads(f, out):
    """name -> builder returning the source streams."""
    sink = out.append

    def simple():
        s = akayu.Stream()
        s.map(f["inc"]).filter(f["even"]).sink(sink)
        return [s]

    def deep():
        s = akayu.Stream()
        n = s
        for _ in range(10):
            n = n.map(f["inc"])
        n.sink(sink)
        return [s]

    def fanout():
        s = akayu.Stream()
        for _ in range(5):
            s.map(f["inc"]).filter(f["even"]).sink(sink)
        return [s]

    def diamond():
        s = akayu.Stream()
        akayu.union(s.map(f["dbl"]), s.map(f["tri"])).sink(sink)
        return [s]

    def partition():
        """Split into three lists: independent branches (see compile())."""
        s = akayu.Stream()
        lists = ([], [], [])
        s.filter(f["mod0"]).sink(lists[0].append)
        s.filter(f["mod1"]).sink(lists[1].append)
        s.map(f["dbl"]).sink(lists[2].append)
        sink(lists)  # keep them alive with the graph's output
        return [s]

    def zip3():
        ss = [akayu.Stream() for _ in range(3)]
        akayu.zip(*ss).sink(sink)
        return ss

    def combine():
        ss = [akayu.Stream() for _ in range(3)]
        akayu.combine_latest(*ss).sink(sink)
        return ss

    def accumulate():
        s = akayu.Stream()
        s.accumulate(f["add"], 0).sink(sink)
        return [s]

    def flatten():
        s = akayu.Stream()
        s.map(_pair).flatten().sink(sink)
        return [s]

    def starmap():
        s = akayu.Stream()
        s.map(_pair).starmap(_sum2).sink(sink)
        return [s]

    def chunk_by():
        s = akayu.Stream()
        s.chunk_by(f["delim"]).sink(sink)
        return [s]

    def batch_map():
        s = akayu.Stream()
        s.batch_map(_batch).sink(sink)
        return [s]

    def filtered():
        s = akayu.Stream()
        s.filter(f["delim"]).map(f["inc"]).sink(sink)
        return [s]

    def prefetch():
        s = akayu.Stream()
        s.prefetch(8).map(f["inc"]).sink(sink)
        return [s]

    def par():
        s = akayu.Stream()
        p = s.par()
        akayu.union(p.map(f["dbl"]), p.map(f["tri"])).seq().sink(sink)
        return [s]

    return {k: v for k, v in locals().items() if callable(v) and k != "sink"}


WORKLOADS = [*_workloads(FUNCS["opaque"], []), "build"]
N_BUILD = 5_000


class _Region:
    """Tells a parent `perf stat --control` when to count."""

    def __init__(self):
        ctl, ack = os.environ.get("BENCH_CTL"), os.environ.get("BENCH_ACK")
        self.ctl = Path(ctl).open("w") if ctl else None  # noqa: SIM115
        self.ack = Path(ack).open() if ack else None  # noqa: SIM115

    def _cmd(self, c):
        if self.ctl:
            self.ctl.write(c + "\n")
            self.ctl.flush()
            self.ack.readline()

    def __enter__(self):
        gc.collect()
        self.t = time.perf_counter()
        self._cmd("enable")

    def __exit__(self, *exc):
        self._cmd("disable")
        self.elapsed = time.perf_counter() - self.t


def _build_graph(f, out):
    s = akayu.Stream()
    s.map(f).filter(f).sink(out.append)
    return s


def _build_graph_inline(out):
    """Like a request handler: new lambdas every call."""
    s = akayu.Stream()
    s.map(lambda x: x + 1).filter(lambda x: x % 2 == 0).sink(out.append)
    return s


def _run_build(mode, kind, n):
    out, f = [], FUNCS["opaque"]["inc"]
    region = _Region()
    with region:
        for _ in range(n):
            s = _build_graph(f, out) if kind == "opaque" else _build_graph_inline(out)
            if mode == "compiled":
                s.compile()
    print(json.dumps({"elapsed": region.elapsed, "out": 0}))


def run_one(workload, mode, kind, n):
    """Child process: build, then run the measured region once."""
    if workload == "build":
        _run_build(mode, kind, n)
        return
    out = []
    sources = _workloads(FUNCS[kind], out)[workload]()
    if mode in ("compiled", "batch"):
        for s in sources:
            s.compile()
    data = list(range(n))
    region = _Region()
    if mode.startswith("batch"):
        emits = [s.emit_batch for s in sources]
        with region:
            for e in emits:
                e(data)
    else:
        emits = [s.emit for s in sources]
        if len(emits) == 1:
            (e,) = emits
            with region:
                for x in data:
                    e(x)
        else:
            with region:
                for x in data:
                    for e in emits:
                        e(x)
    for s in sources:
        s.flush()
    print(json.dumps({"elapsed": region.elapsed, "out": len(out)}))


def _perf():
    return os.environ.get("PERF") or shutil.which("perf")


EVENT = "instructions:u"


def measure(workload, mode, kind, event=EVENT, record=None):
    """`event` counts (or ns) per source item; None where it doesn't apply.

    With `record` (a directory), also `perf record` the measured region into
    record/WORKLOAD-MODE-KIND.data, with Python frames visible (-X perf).
    """
    if workload == "build":
        if mode not in ("eager", "compiled"):
            return None
        n = N_BUILD
    else:
        n = N["batch" if mode.startswith("batch") else "emit"]
    cmd = [sys.executable, __file__, "--one", workload, mode, kind, str(n)]
    perf = _perf()
    if not perf:
        best = min(json.loads(_run(cmd, os.environ))["elapsed"] for _ in range(3))
        return best / n * 1e9
    r = _perf_region([perf, "stat", "-x,", "-e", event], cmd, f"{workload}/{mode}/{kind}")
    if record:
        Path(record).mkdir(parents=True, exist_ok=True)
        out = Path(record, f"{workload}-{mode}-{kind}.data")
        pcmd = [perf, "record", "-q", "-g", "-e", event, "-o", str(out)]
        _perf_region(pcmd, [cmd[0], "-X", "perf", *cmd[1:]], f"{workload}/{mode}/{kind}")
    line = next(ln for ln in r.stderr.splitlines() if event in ln)
    return int(line.split(",")[0]) / n


def _perf_region(pcmd, cmd, what):
    """Run `pcmd -- cmd` with perf counting only inside the child's _Region."""
    with tempfile.TemporaryDirectory() as d:
        ctl, ack = Path(d, "ctl"), Path(d, "ack")
        os.mkfifo(ctl)
        os.mkfifo(ack)
        env = dict(os.environ, BENCH_CTL=ctl, BENCH_ACK=ack)
        r = subprocess.run(
            [*pcmd, "--delay=-1", "--control", f"fifo:{ctl},{ack}", "--", *cmd],
            capture_output=True,
            text=True,
            env=env,
            check=False,
        )
    if r.returncode:
        raise RuntimeError(f"{what} failed:\n{r.stderr}")
    return r


def _run(cmd, env):
    r = subprocess.run(cmd, capture_output=True, text=True, env=env, check=False)
    if r.returncode:
        raise RuntimeError(r.stderr)
    return r.stdout.strip().splitlines()[-1]


_MEM_CHILD = r"""
import gc, json, resource, sys
import akayu, psutil
what = sys.argv[1]
proc = psutil.Process()
def rss():
    gc.collect()
    return proc.memory_info().rss
f = lambda x: x * 2 + 0
if what in ("node", "node-compiled"):
    keep, before = [], rss()
    for _ in range(20_000):
        s = akayu.Stream()
        s.map(f).filter(f).sink(f)
        if what == "node-compiled":
            s.compile()
        keep.append(s)
    print(json.dumps((rss() - before) / 80_000))
else:
    out, data = [], list(range(1_000_000))
    s = akayu.Stream()
    n = s
    for _ in range(10):
        n = n.map(f)
    n.sink(out.append)
    s.compile()
    base = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if what == "batch":
        s.emit_batch(data)
    else:
        for x in data:
            s.emit(x)
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    print(json.dumps((peak - base) / 1024))
"""


def memory():
    m = {}
    m["bytes/node"] = json.loads(_run([sys.executable, "-c", _MEM_CHILD, "node"], os.environ))
    m["bytes/node, compiled"] = json.loads(_run([sys.executable, "-c", _MEM_CHILD, "node-compiled"], os.environ))
    m["sizeof(Stream)"] = sys.getsizeof(akayu.Stream())
    for what in ("emit", "batch"):
        mib = json.loads(_run([sys.executable, "-c", _MEM_CHILD, what], os.environ))
        m[f"peak MiB, 1M {what}"] = mib
    return m


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--one", nargs=4, help=argparse.SUPPRESS)
    ap.add_argument("-w", "--workload", action="append", choices=WORKLOADS)
    ap.add_argument("-m", "--mode", action="append", choices=MODES)
    ap.add_argument("-k", "--kind", action="append", choices=KINDS)
    ap.add_argument("--no-memory", action="store_true")
    ap.add_argument(
        "--event",
        default=EVENT,
        help="perf event to count (default instructions:u; cycles:u also sees atomics, "
        "cache misses and stalls, but varies with machine load)",
    )
    ap.add_argument("--save", help="write results to this JSON file")
    ap.add_argument("--record", metavar="DIR", help="also perf-record each cell's measured region into DIR")
    ap.add_argument("--compare", help="show %% change against this JSON file")
    ap.add_argument("--allow-debug", action="store_true", help="measure a debug build anyway")
    a = ap.parse_args()
    if akayu.akayu._build != "release" and not a.allow_debug:
        sys.exit(f"{akayu.__file__} is a debug build: run `just build-release` first (or pass --allow-debug)")
    if a.one:
        w, mode, kind, n = a.one
        run_one(w, mode, kind, int(n))
        return
    _check_opaque()

    unit = a.event if _perf() else "ns (wall clock: idle machine only)"
    old = json.loads(Path(a.compare).read_text()) if a.compare else {}
    res = {"unit": unit, "cpu": {}, "memory": {}}

    def show(key, val, fmt):
        if val is None:
            return "-"
        prev = old.get("cpu", {}).get(key) or old.get("memory", {}).get(key)
        delta = f"  {100 * (val - prev) / prev:+6.1f}%" if prev else ""
        return f"{val:{fmt}}{delta}"

    modes, kinds = a.mode or MODES, a.kind or KINDS
    for kind in kinds:
        print(f"\n## {kind} functions: {unit} per source item")
        print(f"{'workload':12}" + "".join(f"{m:>22}" for m in modes))
        for w in a.workload or WORKLOADS:
            cells = []
            for mode in modes:
                key = f"{w}/{mode}/{kind}"
                res["cpu"][key] = v = measure(w, mode, kind, a.event, a.record)
                cells.append(f"{show(key, v, '9.0f'):>22}")
            print(f"{w:12}" + "".join(cells), flush=True)
    if not a.no_memory:
        print("\n## memory")
        res["memory"] = memory()
        for k, v in res["memory"].items():
            print(f"  {k:22} {show(k, v, '9.1f')}")
    if a.save:
        Path(a.save).write_text(json.dumps(res, indent=1))


if __name__ == "__main__":
    main()
