"""
Example 07: File Processing

This example demonstrates:
- from_text_file() to create a stream from a text file
- to_text_file() to write stream output to a file
- Processing lines through a pipeline

akayu provides file I/O helpers for reading lines from files
and writing data to files as part of your stream pipeline.
"""

import os
import tempfile
import akayu
from akayu import Stream


def main():
    print("=" * 60)
    print("AKAYU EXAMPLE 07: File Processing")
    print("=" * 60)

    # ---------------------------------------------------------
    # SECTION: Create a Test File
    # ---------------------------------------------------------
    # First, let's create a sample file to work with
    print("\n--- Creating test data file ---")

    test_content = """Line 1: Hello World
Line 2: The quick brown fox
Line 3: Jumps over the lazy dog
Line 4: Python is awesome
Line 5: akayu streams are fast
Line 6: Rust powers the backend
Line 7: Data flows through pipelines
Line 8: End of file
"""

    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
        f.write(test_content)
        test_file = f.name

    print(f"Created test file: {test_file}")
    print(f"Content preview (first 3 lines):")
    for i, line in enumerate(test_content.split('\n')[:3]):
        print(f"  {line}")

    # ---------------------------------------------------------
    # SECTION: Reading Lines with from_text_file()
    # ---------------------------------------------------------
    # from_text_file() creates a stream that emits lines from a file
    print("\n--- Reading file with from_text_file() ---")

    # Create a stream that reads from the file
    file_stream = akayu.from_text_file(test_file)

    lines_read = []

    # Process each line through the stream
    file_stream.map(lambda line: line.upper()).filter(lambda line: "LINE" in line).sink(lines_read.append)

    # Emit None to trigger the file reading (or let it run in background thread)
    # Note: from_text_file reads in a background thread
    import time
    time.sleep(0.1)  # Give time for background thread to read

    print(f"Lines containing 'LINE': {lines_read}")

    # ---------------------------------------------------------
    # SECTION: Writing to File with to_text_file()
    # ---------------------------------------------------------
    # to_text_file() writes each emitted item as a line in a file
    print("\n--- Writing to file with to_text_file() ---")

    output_file = test_file + ".out"

    source = Stream()
    akayu.to_text_file(source, output_file)

    source.map(lambda x: f"Processed: {x}").sink(lambda x: None)

    source.emit_batch(["apple", "banana", "cherry", "date"])

    # Read back what was written
    time.sleep(0.1)
    with open(output_file) as f:
        written_content = f.read()

    print(f"Written to {output_file}:")
    print(written_content)

    # ---------------------------------------------------------
    # SECTION: Complete File Processing Pipeline
    # ---------------------------------------------------------
    # A more complete example: read, transform, filter, write
    print("\n--- Complete file processing pipeline ---")

    # Create input file with some data
    input_data = """name,score
Alice,85
Bob,92
Charlie,78
Diana,95
Eve,88
"""

    input_file = test_file + ".in"
    with open(input_file, 'w') as f:
        f.write(input_data)

    # Create output file for passing students
    output_file = test_file + ".passing"

    source = akayu.from_text_file(input_file)

    passing_lines = []

    def process_line(line):
        if ',' in line and line != "name,score":
            name, score_str = line.strip().split(',')
            try:
                score = int(score_str)
                return f"{name}: {score} (PASSED)" if score >= 85 else None
            except ValueError:
                return None
        return None

    source.map(process_line).filter(lambda x: x is not None).sink(passing_lines.append)

    # Also write to output file
    writer_stream = Stream()
    akayu.to_text_file(writer_stream, output_file)
    source.sink(lambda x: writer_stream.emit(x))

    # Wait for background processing
    time.sleep(0.2)

    print(f"Input file: {input_file}")
    print(f"Students with score >= 85:")
    for line in passing_lines:
        print(f"  {line}")

    print(f"\nAlso written to: {output_file}")

    # ---------------------------------------------------------
    # SECTION: Line-by-line Processing Pattern
    # ---------------------------------------------------------
    # Common pattern: read file, process each line, aggregate results
    print("\n--- Processing log file example ---")

    # Create a sample log file
    log_content = """INFO: Server started on port 8080
DEBUG: Loading configuration
INFO: Database connected
WARN: High memory usage detected
ERROR: Connection timeout
INFO: Retry successful
DEBUG: Cache invalidated
WARN: Slow query detected
ERROR: Authentication failed
INFO: User logged in
"""

    log_file = test_file + ".log"
    with open(log_file, 'w') as f:
        f.write(log_content)

    # Process log file
    log_stream = akayu.from_text_file(log_file)

    error_count = 0
    warn_count = 0
    errors = []

    def categorize(line):
        if line.startswith("ERROR"):
            return "error"
        elif line.startswith("WARN"):
            return "warn"
        return "info"

    log_stream.map(categorize).sink(lambda cat: None)  # Process for side effects

    # Alternative: aggregate counts
    source = akayu.from_text_file(log_file)

    categories = []
    source.map(categorize).sink(categories.append)

    time.sleep(0.2)

    error_count = categories.count("error")
    warn_count = categories.count("warn")

    print(f"Log file: {log_file}")
    print(f"Lines processed: {len(categories)}")
    print(f"ERROR count: {error_count}")
    print(f"WARN count: {warn_count}")
    print(f"INFO count: {categories.count('info')}")

    # ---------------------------------------------------------
    # SECTION: Cleanup
    # ---------------------------------------------------------
    print("\n--- Cleanup ---")

    # Clean up temporary files
    for f in [test_file, output_file, input_file, log_file]:
        if os.path.exists(f):
            os.remove(f)

    print("Temporary files removed")

    print("\n" + "=" * 60)
    print("EXAMPLE 07 COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()