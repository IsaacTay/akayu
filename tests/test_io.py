from rstreamz import Stream, to_text_file, from_text_file
import tempfile
import time
import os


def test_write_file_sink():
    # Create temp file
    with tempfile.NamedTemporaryFile(delete=False) as f:
        path = f.name

    try:
        s = Stream(asynchronous=False)
        # Using to_text_file as a utility sink
        s.sink(to_text_file, path)

        s.emit("Line 1")
        s.emit("Line 2")

        with open(path, "r") as f:
            content = f.read()
            assert content == "Line 1\nLine 2\n"

    finally:
        if os.path.exists(path):
            os.remove(path)


def test_source_from_file():
    # Create a source file
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
        f.write("Line 1\nLine 2\nLine 3")
        path = f.name

    try:
        # Read with a small delay to simulate streaming
        s = from_text_file(path, interval=0.01)
        L = []
        s.sink(L.append)

        # Wait for thread to process
        time.sleep(0.1)

        assert "Line 1" in L
        assert "Line 2" in L
        assert "Line 3" in L

    finally:
        if os.path.exists(path):
            os.remove(path)


def test_from_file_nonexistent():
    """Test from_text_file with non-existent file."""
    # Should handle gracefully (print error, not crash)
    s = from_text_file("/nonexistent/path/file.txt")
    L = []
    s.sink(L.append)
    time.sleep(0.1)
    assert L == []  # no data emitted


def test_write_file_sink_append():
    """Test that to_text_file appends, doesn't overwrite."""
    # Create temp file
    with tempfile.NamedTemporaryFile(delete=False) as f:
        path = f.name

    try:
        s = Stream(asynchronous=False)
        s.sink(to_text_file, path)

        # First batch of writes
        s.emit("Line 1")
        s.emit("Line 2")

        # Verify content after first batch
        with open(path, "r") as f:
            content = f.read()
            assert content == "Line 1\nLine 2\n"

        # Continue writing (should append)
        s.emit("Line 3")

        with open(path, "r") as f:
            content = f.read()
            assert content == "Line 1\nLine 2\nLine 3\n"

    finally:
        if os.path.exists(path):
            os.remove(path)
