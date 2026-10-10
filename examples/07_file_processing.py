"""
Example 07: File Processing

This example demonstrates:
- from_text_file() to create a stream from a text file
- start() and join() to read the file once the pipeline is built
- to_text_file() as a sink that appends each item to a file

from_text_file() reads nothing until start() is called, so build the whole
pipeline first. Lines are read in a background thread; join() waits for the
last one and re-raises any error from reading or from the pipeline.
"""

import tempfile
from pathlib import Path

import akayu


def main():
    print("=" * 60)
    print("AKAYU EXAMPLE 07: File Processing")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as tmp_name:
        tmp = Path(tmp_name)

        # ---------------------------------------------------------
        # SECTION: Reading Lines with from_text_file()
        # ---------------------------------------------------------
        print("\n--- Reading file with from_text_file() ---")

        test_file = tmp / "lines.txt"
        test_file.write_text(
            "Line 1: Hello World\n"
            "Line 2: The quick brown fox\n"
            "a line without the keyword\n"
            "Line 4: akayu streams are fast\n"
        )

        lines_read = []
        source = akayu.from_text_file(str(test_file))
        source.map(str.upper).filter(lambda line: line.startswith("LINE")).sink(lines_read.append)
        source.start().join()

        print(f"Lines starting with 'LINE': {lines_read}")
        assert lines_read == ["LINE 1: HELLO WORLD", "LINE 2: THE QUICK BROWN FOX", "LINE 4: AKAYU STREAMS ARE FAST"]

        # ---------------------------------------------------------
        # SECTION: Writing to File with to_text_file()
        # ---------------------------------------------------------
        # to_text_file(data, path) appends str(data) and a newline: use it as a sink
        print("\n--- Writing to file with to_text_file() ---")

        output_file = tmp / "processed.txt"
        source = akayu.Stream()
        source.map(lambda x: f"Processed: {x}").sink(akayu.to_text_file, path=str(output_file))
        source.emit_batch(["apple", "banana", "cherry"])

        written = output_file.read_text()
        print(f"Written to {output_file.name}:\n{written}")
        assert written == "Processed: apple\nProcessed: banana\nProcessed: cherry\n"

        # ---------------------------------------------------------
        # SECTION: Complete File Processing Pipeline
        # ---------------------------------------------------------
        # Read, transform, filter, and write, all in one pipeline
        print("\n--- Complete file processing pipeline ---")

        input_file = tmp / "scores.csv"
        input_file.write_text("name,score\nAlice,85\nBob,92\nCharlie,78\nDiana,95\nEve,88\n")
        passing_file = tmp / "passing.txt"

        def passing(line):
            name, score = line.split(",")
            return f"{name}: {score} (PASSED)" if score.isdigit() and int(score) >= 85 else None

        passing_lines = []
        source = akayu.from_text_file(str(input_file))
        passed = source.map(passing).filter(lambda x: x is not None)
        passed.sink(passing_lines.append)
        passed.sink(akayu.to_text_file, path=str(passing_file))
        source.start().join()

        print("Students with score >= 85:")
        for line in passing_lines:
            print(f"  {line}")
        assert passing_lines == ["Alice: 85 (PASSED)", "Bob: 92 (PASSED)", "Diana: 95 (PASSED)", "Eve: 88 (PASSED)"]
        assert passing_file.read_text().splitlines() == passing_lines

        # ---------------------------------------------------------
        # SECTION: Line-by-line Processing Pattern
        # ---------------------------------------------------------
        # Common pattern: read a log, categorize each line, aggregate counts
        print("\n--- Processing log file example ---")

        log_file = tmp / "server.log"
        log_file.write_text(
            "INFO: Server started on port 8080\n"
            "WARN: High memory usage detected\n"
            "ERROR: Connection timeout\n"
            "INFO: Retry successful\n"
            "ERROR: Authentication failed\n"
        )

        def categorize(line):
            return line.split(":", 1)[0].lower()

        categories = []
        source = akayu.from_text_file(str(log_file))
        source.map(categorize).sink(categories.append)
        source.start().join()

        counts = {c: categories.count(c) for c in ("error", "warn", "info")}
        print(f"Lines processed: {len(categories)}, counts: {counts}")
        assert counts == {"error": 2, "warn": 1, "info": 2}

    print("\n" + "=" * 60)
    print("KEY TAKEAWAYS:")
    print("1. from_text_file() reads nothing until start(): build the pipeline first")
    print("2. join() waits for the last line and re-raises any error")
    print("3. to_text_file(data, path) is a sink: .sink(akayu.to_text_file, path=...)")
    print("=" * 60)


if __name__ == "__main__":
    main()
