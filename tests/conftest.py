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
