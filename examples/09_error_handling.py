"""
Example 09: Error Handling

This example demonstrates:
- How errors propagate through the stream graph
- Using stream_name for debugging
- Error message format and interpretation
- Error recovery patterns
- Graceful handling of failing operations

Understanding how akayu handles errors is important for building
robust pipelines that can recover from failures.
"""

import akayu


def main():
    print("=" * 60)
    print("AKAYU EXAMPLE 09: Error Handling")
    print("=" * 60)

    # ---------------------------------------------------------
    # SECTION: Basic Error Propagation
    # ---------------------------------------------------------
    # When an operation fails, the error propagates up through the graph
    print("\n--- Basic error propagation ---")

    source = akayu.Stream()
    source.map(lambda x: x / 0).sink(lambda _: None)  # Division by zero

    try:
        source.emit(10)
    except Exception as e:
        print(f"Caught error: {type(e).__name__}: {e}")
        print("\nError location points to the failing operation")

    # ---------------------------------------------------------
    # SECTION: Named Streams for Debugging
    # ---------------------------------------------------------
    # You can name your streams to make error messages clearer
    print("\n--- Named streams for better debugging ---")

    source = akayu.Stream(name="input_source")
    mapped = source.map(lambda x: x * 2, stream_name="double_operation")
    filtered = mapped.filter(lambda x: x > 5, stream_name="filter_greater_than_5")
    filtered.sink(lambda _: None, stream_name="final_sink")

    # The error message will include the stream name
    try:
        filtered.emit(10)
    except Exception as e:
        print(f"Error mentions stream name: {e}")

    # ---------------------------------------------------------
    # SECTION: Error Messages Show Source Location
    # ---------------------------------------------------------
    # akayu tries to show where in your code the error occurred
    print("\n--- Source location in error messages ---")

    source = akayu.Stream()

    # This will fail
    source.map(lambda _: int("not_a_number")).sink(lambda _: None)

    try:
        source.emit("hello")
    except Exception as e:
        print(f"Error type: {type(e).__name__}")
        print("Error message includes node name and location")

    # ---------------------------------------------------------
    # SECTION: Filter Errors
    # ---------------------------------------------------------
    # Errors in filter predicates are propagated
    print("\n--- Filter predicate errors ---")

    source = akayu.Stream()

    def may_fail(x):
        if x == 0:
            raise ValueError("Cannot filter zero")
        return x > 5

    source.filter(may_fail).sink(lambda _: None)

    # akayu wraps errors from your functions in a RuntimeError that names the
    # failing node; the original exception is available as __cause__.
    try:
        source.emit(0)
    except RuntimeError as e:
        print(f"Filter error: {type(e.__cause__).__name__}: {e.__cause__}")

    # ---------------------------------------------------------
    # SECTION: Recovery Patterns
    # ---------------------------------------------------------
    # How to handle errors and continue processing
    print("\n--- Error recovery patterns ---")

    processed_items = []
    failed_items = []

    source = akayu.Stream()

    def risky_operation(x):
        if x == 3:
            raise ValueError("3 is unlucky!")
        return x * 2

    def handle_error(x, error_list, error_counter):
        error_list.append({"item": x, "error": str(error_counter["count"])})
        error_counter["count"] += 1

    # Use a wrapper pattern for error recovery
    def safe_operation(x):
        try:
            return risky_operation(x)
        except ValueError as e:
            failed_items.append({"item": x, "error": str(e)})
            return None  # Signal that this item failed

    source.map(safe_operation).filter(lambda x: x is not None).sink(processed_items.append)

    for i in range(5):
        source.emit(i)  # errors are handled inside safe_operation

    print(f"Processed items: {processed_items}")
    print(f"Failed items: {failed_items}")

    # ---------------------------------------------------------
    # SECTION: Branch-Specific Error Handling
    # ---------------------------------------------------------
    # Different branches can have different error handling
    print("\n--- Branch-specific error handling ---")

    branch_results = {"safe": [], "risky": []}

    source = akayu.Stream()
    p = source.par()

    # Safe branch - no errors possible
    p.map(lambda x: x + 1).sink(lambda x: branch_results["safe"].append(x))

    # Risky branch - may fail
    def risky(x):
        if x > 5:
            raise ValueError("Too large!")
        return x * 10

    p.map(risky).sink(lambda x: branch_results["risky"].append(x))

    # Process and catch errors per emit
    for i in [1, 3, 6, 8]:
        try:
            source.emit(i)
        except RuntimeError as e:
            print(f"Error on emit({i}): {e.__cause__}")

    print(f"Safe branch results: {branch_results['safe']}")
    print(f"Risky branch results: {branch_results['risky']}")

    # ---------------------------------------------------------
    # SECTION: Error Context in Complex Pipelines
    # ---------------------------------------------------------
    # Understanding error context in multi-stage pipelines
    print("\n--- Multi-stage error context ---")

    source = akayu.Stream(name="data_source")

    stage1 = source.map(lambda x: x + " -> stage1", stream_name="add_stage1")
    stage2 = stage1.map(lambda x: int(x[0]) if x[0].isdigit() else 0, stream_name="parse_int")  # This might fail
    stage3 = stage2.map(lambda x: x * 2, stream_name="double_final")

    try:
        stage3.emit("hello")  # Will fail at parse_int
    except Exception as e:
        print("Error in multi-stage pipeline:")
        print("  Stage that failed: parse_int")
        print(f"  Error: {type(e).__name__}")

    # ---------------------------------------------------------
    # SECTION: Using accumulate with Error State
    # ---------------------------------------------------------
    # accumulate can track errors as part of processing state
    print("\n--- Error tracking with accumulate ---")

    def process_with_tracking(state, item):
        errors_so_far, processed = state
        try:
            if item < 0:
                raise ValueError("Negative not allowed")
            processed.append(item * 2)
            return (errors_so_far, processed)
        except ValueError as e:
            errors_so_far.append({"item": item, "error": str(e)})
            return (errors_so_far, processed)

    source = akayu.Stream()
    source.accumulate(process_with_tracking, start=([], [])).sink(lambda _: None)

    source.emit(1)
    source.emit(2)
    source.emit(-1)  # This will cause an error
    source.emit(4)

    # Note: accumulate emits state after each item
    # We'd need to check the final state for accumulated errors

    print("Accumulate with error tracking pattern demonstrated")

    # ---------------------------------------------------------
    # SECTION: Best Practices Summary
    # ---------------------------------------------------------
    print("\n--- Error handling best practices ---")
    print("1. Name your streams with stream_name= for clearer errors")
    print("2. Use try/except around emit() calls for recovery")
    print("3. Wrap risky operations in safe wrappers that return None")
    print("4. Use filter() to skip failed items after wrapping")
    print("5. Separate error handling logic into dedicated branches")
    print("6. Use accumulate() to track errors as part of processing state")

    print("\n" + "=" * 60)
    print("EXAMPLE 09 COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
