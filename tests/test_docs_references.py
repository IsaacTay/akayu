"""Files and `just` recipes named in the docs must exist.

CONTRIBUTING.md listed test files that no longer existed, a test_async.py
for a removed feature, and machine-specific paths; justfile recipes pointed
at missing test files. This keeps the written guidance honest.
"""

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
DOCS = [ROOT / "README.md", ROOT / "CONTRIBUTING.md", *sorted((ROOT / "docs").rglob("*.md"))]
TOP_DIRS = ("src", "python", "tests", "examples", "docs", "scripts", "pipeline")

# `tests/test_io.py`, `src/gc.rs`, `docs/`, ...
PATH_RE = re.compile(rf"`((?:{'|'.join(TOP_DIRS)})/[\w./*-]*)`")
# `just test-quick`, just docs-check, ...
JUST_RE = re.compile(r"\bjust ([a-z][\w-]*)")
# indented bullet under `src/`: - `optimization.rs`, `builtin.rs`: ...
NESTED_RE = re.compile(r"^\s{4,}- (`.*?`):", re.MULTILINE)


def _recipes():
    text = (ROOT / "justfile").read_text(encoding="utf-8")
    # A recipe line: its name, any parameters (`py="3.13"`), then `:` (not `:=`).
    return set(re.findall(r"^([a-z][\w-]*)(?:\s[^\n:]*)?:(?!=)", text, re.MULTILINE))


@pytest.mark.parametrize("doc", DOCS, ids=lambda p: str(p.relative_to(ROOT)))
def test_referenced_paths_exist(doc):
    text = doc.read_text(encoding="utf-8")
    # Backticked paths, plus scripts named in python/pytest commands (code blocks).
    named = PATH_RE.findall(text) + [
        p
        for line in text.splitlines()
        if re.search(r"\b(?:pytest|python)\b", line)
        for p in re.findall(r"(?:tests|scripts|examples)/[\w./-]+\.py", line)
    ]
    missing = [p for p in named if "*" not in p and not (ROOT / p).exists()]
    assert not missing, f"{doc.name} references missing paths: {missing}"


@pytest.mark.parametrize("doc", DOCS, ids=lambda p: str(p.relative_to(ROOT)))
def test_referenced_recipes_exist(doc):
    missing = sorted(set(JUST_RE.findall(doc.read_text(encoding="utf-8"))) - _recipes())
    assert not missing, f"{doc.name} references missing just recipes: {missing}"


def test_contributing_file_map_matches_the_tree():
    """The nested src/ file list must name real files, and all of them."""
    text = (ROOT / "CONTRIBUTING.md").read_text(encoding="utf-8")
    section = text[text.index("## Project Structure") : text.index("### Python Package Structure")]
    named = [n for names in NESTED_RE.findall(section) for n in re.findall(r"`([\w.-]+\.(?:rs|py))`", names)]
    assert named, "file map not found"
    missing = [n for n in named if not (ROOT / "src" / n).exists()]
    assert not missing, f"CONTRIBUTING.md file map names missing src/ files: {missing}"
    unnamed = sorted(p.name for p in (ROOT / "src").glob("*.[rp][sy]") if p.name not in named)
    assert not unnamed, f"CONTRIBUTING.md file map leaves out src/ files: {unnamed}"


def test_api_reference_covers_the_public_api():
    """docs/api.md renders every public name, and mdformat hasn't flattened a
    mkdocstrings options block into text (it rendered as a stray paragraph)."""
    import akayu

    text = (ROOT / "docs" / "api.md").read_text(encoding="utf-8")
    rendered = re.findall(r"^::: akayu\.(\w+)$", text, re.MULTILINE)
    assert not set(akayu.__all__) - set(rendered), f"docs/api.md leaves out {set(akayu.__all__) - set(rendered)}"
    assert not re.search(r"^(options|members):", text, re.MULTILINE), "unindented mkdocstrings options in docs/api.md"


def test_no_machine_specific_paths():
    for doc in DOCS:
        assert "/mnt/" not in doc.read_text(encoding="utf-8"), f"{doc.name} contains a machine-specific path"
