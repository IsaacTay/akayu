"""Differential fuzzer for the native fast paths: random lambdas over awkward
values, run by akayu (uncompiled and compiled; map, filter and longer chains;
emit and emit_batch) and by plain Python, which must agree on every result
and every exception type.

compile() turns lambdas such as `x % 2 == 0`, `str(x).upper()` or
`acc + x` into native steps, with their own arithmetic, comparisons and
method calls; this checks them against what Python computes, including
overflow, NaN, -0.0, subclasses that override operators, and objects whose
operators return odd things.

AKAYU_OPS_FUZZ_SEEDS sets the number of seeds (default 30).
"""

import pytest
from native_ops_fuzzer import CHUNK, SEEDS, accumulate_mismatches, mismatches
from support import seed_chunks, seed_ids

# Random expressions trip Python's own warnings (`~True`, `x is 0`, ...).
pytestmark = pytest.mark.filterwarnings("ignore")


@pytest.mark.parametrize("seeds", seed_chunks(SEEDS, CHUNK), ids=seed_ids)
def test_native_ops_match_python(seeds):
    found = [m for seed in seeds for m in mismatches(seed)]
    assert not found, "\n".join(found[:10])


@pytest.mark.parametrize("seeds", seed_chunks(SEEDS * 4, CHUNK * 4), ids=seed_ids)
def test_native_accumulate_matches_python(seeds):
    found = [m for seed in seeds for m in accumulate_mismatches(seed)]
    assert not found, "\n".join(found[:10])
