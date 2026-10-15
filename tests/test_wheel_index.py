"""scripts/wheel_index.py writes the package index uv and pip install from.

GitHub can't be asked here, so the index is built from local files, served by
a local HTTP server, and uv and pip install from it as they would from the
docs site. Fake wheels stand in for akayu's. The uv tests skip where uv isn't
installed (the Wine and qemu wheel runs).
"""

import base64
import functools
import hashlib
import http.server
import importlib.util
import os
import shutil
import subprocess
import sys
import threading
import tomllib
import zipfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts" / "wheel_index.py"

needs_uv = pytest.mark.skipif(shutil.which("uv") is None, reason="uv isn't installed")


def load_script():
    spec = importlib.util.spec_from_file_location("wheel_index", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def fake_wheel(directory, version, extra=""):
    """A pure-Python akayu wheel; `extra` changes its contents but not its name."""
    info = f"akayu-{version}.dist-info"
    files = {
        "akayu/__init__.py": f"VERSION = {version!r}\n{extra}",
        f"{info}/METADATA": f"Metadata-Version: 2.1\nName: akayu\nVersion: {version}\n",
        f"{info}/WHEEL": "Wheel-Version: 1.0\nGenerator: test\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
    }

    def record(data):
        digest = base64.urlsafe_b64encode(hashlib.sha256(data.encode()).digest()).rstrip(b"=").decode()
        return f"sha256={digest},{len(data.encode())}"

    path = directory / f"akayu-{version}-py3-none-any.whl"
    with zipfile.ZipFile(path, "w") as z:
        for name, data in files.items():
            z.writestr(name, data)
        z.writestr(f"{info}/RECORD", "".join(f"{n},{record(d)}\n" for n, d in files.items()) + f"{info}/RECORD,,\n")
    return path


class QuietHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *args):
        pass


@pytest.fixture
def index(tmp_path):
    """Two releases' wheels and a file that isn't one, indexed and served:
    (the index's URL, the directory the files are in)."""
    files = tmp_path / "files"
    files.mkdir()
    fake_wheel(files, "9.0.0")
    fake_wheel(files, "9.1.0")
    (files / "notes.txt").write_text("not a distribution\n")
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), functools.partial(QuietHandler, directory=str(tmp_path)))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{server.server_port}"
    subprocess.run(
        [sys.executable, str(SCRIPT), str(tmp_path), "--files", str(files), "--base-url", f"{base}/files"],
        check=True,
        capture_output=True,
    )
    yield f"{base}/simple/", files
    server.shutdown()
    server.server_close()


def uv(*args, cwd):
    # Only this test's settings: no user config, its own cache, no downloads.
    env = {k: v for k, v in os.environ.items() if not k.startswith("UV_") and k not in ("VIRTUAL_ENV", "PYTHONPATH")}
    env |= {"UV_CACHE_DIR": str(Path(cwd).parent / "uv-cache"), "UV_NO_CONFIG": "1", "UV_PYTHON_DOWNLOADS": "never"}
    return subprocess.run(["uv", *args], cwd=cwd, env=env, capture_output=True, text=True, timeout=120, check=False)


def added_project(tmp_path, url):
    """A new uv project after the documented `uv add akayu --index akayu=URL`."""
    project = tmp_path / "project"
    project.mkdir()
    r = uv("init", "--bare", "--no-workspace", "--python", sys.executable, cwd=project)
    assert r.returncode == 0, r.stderr
    r = uv("add", "akayu", "--index", f"akayu={url}", cwd=project)
    assert r.returncode == 0, r.stderr
    return project


@needs_uv
def test_uv_add_pins_akayu_to_the_index_and_locks_the_newest_wheel(tmp_path, index):
    url, files = index
    project = added_project(tmp_path, url)
    config = tomllib.loads((project / "pyproject.toml").read_text())
    assert config["tool"]["uv"]["sources"]["akayu"] == {"index": "akayu"}
    assert config["tool"]["uv"]["index"] == [{"name": "akayu", "url": url}]
    [locked] = [p for p in tomllib.loads((project / "uv.lock").read_text())["package"] if p["name"] == "akayu"]
    assert locked["version"] == "9.1.0"
    assert locked["source"] == {"registry": url}
    sha = hashlib.sha256((files / "akayu-9.1.0-py3-none-any.whl").read_bytes()).hexdigest()
    assert [w["hash"] for w in locked["wheels"]] == [f"sha256:{sha}"]


@needs_uv
def test_uv_sync_refuses_a_wheel_that_changed_after_it_was_locked(tmp_path, index):
    url, files = index
    project = added_project(tmp_path, url)
    fake_wheel(files, "9.1.0", extra="changed = True\n")
    r = uv("sync", "--reinstall", "--no-cache", cwd=project)
    assert r.returncode != 0
    assert "Hash mismatch" in r.stderr


@pytest.mark.skipif(importlib.util.find_spec("ensurepip") is None, reason="this Python can't make a venv with pip")
def test_pip_refuses_a_wheel_that_changed_after_it_was_indexed(tmp_path, index):
    url, files = index
    subprocess.run([sys.executable, "-m", "venv", str(tmp_path / "venv")], check=True)
    bin_dir = tmp_path / "venv" / ("Scripts" if os.name == "nt" else "bin")
    pip = [str(bin_dir / "python"), "-m", "pip", "install", "-q", "--no-cache-dir", "--index-url", url]

    r = subprocess.run([*pip, "akayu==9.0.0"], capture_output=True, text=True, check=False)
    assert r.returncode == 0, r.stderr
    fake_wheel(files, "9.1.0", extra="changed = True\n")
    r = subprocess.run([*pip, "akayu==9.1.0"], capture_output=True, text=True, check=False)
    assert r.returncode != 0
    assert "THESE PACKAGES DO NOT MATCH THE HASHES" in r.stderr


def test_the_index_lists_published_releases_dists_only(tmp_path):
    """From the GitHub API's release list; files uploaded before mid-2025 have no digest."""
    script = load_script()

    def asset(name, url, digest=None):
        return {"name": name, "browser_download_url": url, "digest": digest}

    releases = [
        {
            "draft": False,
            "assets": [
                asset("akayu-2.0.0-cp313-cp313-win_amd64.whl", "https://x/a.whl", "sha256:ab"),
                asset("akayu-2.0.0.tar.gz", "https://x/a.tar.gz"),
                asset("akayu-2.0.0.sigstore.json", "https://x/s.json", "sha256:cd"),
            ],
        },
        {"draft": True, "assets": [asset("akayu-2.1.0.tar.gz", "https://x/b.tar.gz")]},
    ]
    assert script.write_index(tmp_path, script.assets_from_releases(releases)) == 2
    page = (tmp_path / "simple" / "akayu" / "index.html").read_text()
    assert '<a href="https://x/a.whl#sha256=ab">akayu-2.0.0-cp313-cp313-win_amd64.whl</a>' in page
    assert '<a href="https://x/a.tar.gz">akayu-2.0.0.tar.gz</a>' in page
    assert "sigstore" not in page
    assert "2.1.0" not in page
    assert '<a href="akayu/">akayu</a>' in (tmp_path / "simple" / "index.html").read_text()
