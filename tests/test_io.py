import threading
import time
from pathlib import Path

import pytest

from akayu import Stream, TextFileSource, from_text_file, to_text_file


def _write(tmp_path, text, name="input.txt"):
    path = tmp_path / name
    path.write_bytes(text.encode())
    return str(path)


def test_write_file_sink(tmp_path):
    path = str(tmp_path / "out.txt")
    s = Stream()
    s.sink(to_text_file, path)  # to_text_file is a sink function
    s.emit("Line 1")
    s.emit("Line 2")
    assert Path(path).read_text() == "Line 1\nLine 2\n"


def test_write_file_sink_append(tmp_path):
    """to_text_file appends, it doesn't overwrite."""
    path = str(tmp_path / "out.txt")
    s = Stream()
    s.sink(to_text_file, path=path)
    s.emit("Line 1")
    s.emit("Line 2")
    assert Path(path).read_text() == "Line 1\nLine 2\n"
    s.emit(3)  # non-strings are written via str()
    assert Path(path).read_text() == "Line 1\nLine 2\n3\n"


def test_concurrent_appends_keep_lines_whole(tmp_path):
    """to_text_file releases the GIL for its file I/O, so threads write
    concurrently; each line is one append, so lines never interleave."""
    path = str(tmp_path / "out.txt")
    lines = {f"thread{t}-" + "x" * 300 + f"-{i}" for t in range(8) for i in range(200)}

    def write(t):
        for i in range(200):
            to_text_file(f"thread{t}-" + "x" * 300 + f"-{i}", path)

    threads = [threading.Thread(target=write, args=(t,)) for t in range(8)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    written = Path(path).read_text().split("\n")
    assert written[-1] == ""
    assert sorted(written[:-1]) == sorted(lines)


def test_source_from_file(tmp_path):
    path = _write(tmp_path, "Line 1\nLine 2\nLine 3")
    s = from_text_file(path, interval=0.01)
    assert isinstance(s, TextFileSource)
    lines = []
    s.sink(lines.append)
    s.start().join(timeout=10)
    assert lines == ["Line 1", "Line 2", "Line 3"]


def test_nothing_is_read_before_start(tmp_path):
    """Regression: reading used to begin immediately, so lines emitted before
    the pipeline was attached were lost (0 of 5 with a 50 ms delay)."""
    path = _write(tmp_path, "".join(f"line{i}\n" for i in range(5)))
    s = from_text_file(path)
    time.sleep(0.05)  # e.g. setting up the rest of the program first
    out = []
    s.map(str.upper).sink(out.append)
    s.start().join(timeout=10)
    assert out == [f"LINE{i}" for i in range(5)]


def test_crlf_line_endings_are_stripped(tmp_path):
    path = _write(tmp_path, "a\r\nb\r\n")
    s = from_text_file(path)
    out = []
    s.sink(out.append)
    s.start().join(timeout=10)
    assert out == ["a", "b"]


def test_file_is_read_once(tmp_path):
    """interval only delays lines; appended lines are not picked up."""
    path = _write(tmp_path, "a\n")
    s = from_text_file(path, interval=0.001)
    out = []
    s.sink(out.append)
    s.start().join(timeout=10)
    with Path(path).open("a") as f:
        f.write("b\n")
    time.sleep(0.05)
    assert out == ["a"]


def test_missing_file_error_surfaces_on_join():
    """Used to be printed to stderr and otherwise ignored."""
    s = from_text_file("/nonexistent/path/file.txt")
    out = []
    s.sink(out.append)
    s.start()
    with pytest.raises(FileNotFoundError):
        s.join(timeout=10)
    assert out == []


def test_pipeline_error_stops_reading_and_surfaces_on_join(tmp_path):
    """Used to be silently swallowed by the reader thread."""
    path = _write(tmp_path, "1\n2\nboom\n4\n")
    s = from_text_file(path)
    out = []
    s.map(int).sink(out.append)
    s.start()
    with pytest.raises(RuntimeError) as excinfo:
        s.join(timeout=10)
    assert isinstance(excinfo.value.__cause__, ValueError)
    assert out == [1, 2]


def test_start_twice_and_join_before_start_are_errors(tmp_path):
    s = from_text_file(_write(tmp_path, "a\n"))
    with pytest.raises(RuntimeError, match="never started"):
        s.join()
    s.start().join(timeout=10)
    with pytest.raises(RuntimeError, match="already started"):
        s.start()
