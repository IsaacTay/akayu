"""Package metadata must stay honest."""

from importlib.metadata import metadata, requires


def test_no_runtime_or_extra_requirements():
    """numpy/scipy were once runtime dependencies (akayu never imports them),
    and dev tooling was published as a 'dev' extra. Neither belongs in the
    wheel: dev dependencies live in the dev dependency group."""
    assert not requires("akayu")


def test_requires_python_matches_what_works():
    """akayu claimed >=3.8 but could not even be imported below 3.13."""
    assert metadata("akayu")["Requires-Python"] == ">=3.13"
