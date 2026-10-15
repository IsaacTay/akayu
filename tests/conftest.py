import sys

import pytest
from hypothesis import HealthCheck, settings

# Property tests check correctness, not latency (the benchmark tests cover
# that). Hypothesis's default 200ms per-example deadline turns any stall on a
# busy machine (descheduling, GC, I/O) into a spurious DeadlineExceeded /
# FlakyFailure, so disable it, and the equivalent data-generation check,
# suite-wide.
settings.register_profile(
    "akayu",
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
settings.load_profile("akayu")


@pytest.fixture(params=[False, True], ids=["eager", "compiled"])
def compiled(request):
    """Run the test on an uncompiled graph, then on a compiled one (a test
    that parametrizes `compiled` itself overrides this)."""
    return request.param


@pytest.fixture
def fast_switching():
    """Switch threads every microsecond, so races show up in a short test."""
    old = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)
    yield
    sys.setswitchinterval(old)
