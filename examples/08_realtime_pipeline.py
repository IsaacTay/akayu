"""
Example 08: Real-time Pipeline

This example demonstrates a realistic data processing pipeline:
- Processing log entries with multiple stages
- Error handling and validation
- Multiple output sinks (console + file)
- Simulated real-time data ingestion

This example simulates processing sensor data or log entries
with validation, transformation, and multiple output destinations.
"""

import time
import random
import akayu
from akayu import Stream, union


def main():
    print("=" * 60)
    print("AKAYU EXAMPLE 08: Real-time Pipeline")
    print("=" * 60)

    # ---------------------------------------------------------
    # SECTION: Simulated Sensor Data
    # ---------------------------------------------------------
    # This simulates a data pipeline for IoT sensor readings
    print("\n--- Processing sensor data pipeline ---")

    # Sensor reading format: {"sensor_id": str, "temperature": float, "humidity": float, "timestamp": int}
    sensor_readings = [
        {"sensor_id": "A1", "temperature": 22.5, "humidity": 45.0, "timestamp": 1000},
        {"sensor_id": "A1", "temperature": 23.1, "humidity": 46.2, "timestamp": 1001},
        {"sensor_id": "B2", "temperature": 18.9, "humidity": 52.1, "timestamp": 1002},
        {"sensor_id": "A1", "temperature": 24.3, "humidity": 44.8, "timestamp": 1003},
        {"sensor_id": "C3", "temperature": 999.0, "humidity": 45.0, "timestamp": 1004},  # Invalid: temperature out of range
        {"sensor_id": "B2", "temperature": 19.2, "humidity": 51.5, "timestamp": 1005},
    ]

    valid_readings = []
    alerts = []
    processed_count = {"valid": 0, "invalid": 0}

    source = akayu.Stream()

    # Validation stage: filter out invalid readings
    def validate_reading(reading):
        temp = reading.get("temperature", 0)
        humidity = reading.get("humidity", 0)
        # Valid range: temperature -50 to 100, humidity 0 to 100
        if -50 <= temp <= 100 and 0 <= humidity <= 100:
            return reading
        return None

    validated = source.map(validate_reading).filter(lambda x: x is not None)

    # Branch 1: Store valid readings
    validated.map(lambda r: f"Sensor {r['sensor_id']}: {r['temperature']:.1f}°C, {r['humidity']:.1f}% humidity").sink(valid_readings.append)

    # Branch 2: Check for high temperature alerts
    def check_alerts(reading):
        if reading["temperature"] > 30:
            return f"ALERT: Sensor {reading['sensor_id']} high temperature: {reading['temperature']}°C"
        return None

    validated.map(check_alerts).filter(lambda x: x is not None).sink(alerts.append)

    # Process all readings
    for reading in sensor_readings:
        source.emit(reading)

    print(f"Valid readings: {len(valid_readings)}")
    for r in valid_readings:
        print(f"  {r}")

    print(f"\nAlerts: {len(alerts)}")
    for a in alerts:
        print(f"  {a}")

    # ---------------------------------------------------------
    # SECTION: Multi-stage Log Processing
    # ---------------------------------------------------------
    # More complex example: parse logs, extract info, aggregate
    print("\n--- Log processing pipeline ---")

    raw_logs = [
        "2024-01-15 10:30:45 INFO User login: user123",
        "2024-01-15 10:30:46 DEBUG Session created for user123",
        "2024-01-15 10:30:50 ERROR Database timeout",
        "2024-01-15 10:30:51 WARN Retry attempt 1/3",
        "2024-01-15 10:30:55 INFO User logout: user123",
        "2024-01-15 10:31:00 ERROR Payment failed for order 456",
        "2024-01-15 10:31:05 DEBUG Email sent to customer",
        "2024-01-15 10:31:10 ERROR Connection refused",
    ]

    parsed_logs = []
    error_summary = []
    warning_count = {"count": 0}

    source = akayu.Stream()

    # Stage 1: Parse log entries
    def parse_log(log):
        parts = log.split(" ", 3)
        if len(parts) >= 4:
            return {
                "timestamp": parts[0] + " " + parts[1],
                "level": parts[2],
                "message": parts[3]
            }
        return None

    parsed = source.map(parse_log).filter(lambda x: x is not None)

    # Branch to different processors based on log level
    p = parsed.par()

    # Errors
    p.filter(lambda log: log["level"] == "ERROR").map(lambda log: f"ERROR: {log['message']}").sink(error_summary.append)

    # Warnings
    p.filter(lambda log: log["level"] == "WARN").sink(lambda _: warning_count.update(count=warning_count["count"] + 1))

    # All logs (for debugging)
    p.map(lambda log: f"[{log['level']}] {log['message']}").sink(parsed_logs.append)

    # Process
    for log in raw_logs:
        source.emit(log)

    print(f"Parsed {len(parsed_logs)} logs")
    print(f"Errors ({len(error_summary)}):")
    for e in error_summary:
        print(f"  {e}")
    print(f"Warnings: {warning_count['count']}")

    # ---------------------------------------------------------
    # SECTION: Streaming Data Simulation
    # ---------------------------------------------------------
    # Simulate real-time data coming in and being processed
    print("\n--- Simulated real-time data processing ---")

    # Simulate sensor data being generated
    def generate_sensor_data(sensor_id, base_temp, base_humidity):
        return {
            "sensor_id": sensor_id,
            "temperature": base_temp + random.uniform(-2, 2),
            "humidity": base_humidity + random.uniform(-5, 5),
            "timestamp": time.time()
        }

    processed = []
    high_readings = []

    source = akayu.Stream()

    # Create parallel branches for different processing
    p = source.par()

    # Branch 1: Normalize and store
    p.map(lambda r: {
        "id": r["sensor_id"],
        "temp": round(r["temperature"], 1),
        "humidity": round(r["humidity"], 1)
    }).sink(processed.append)

    # Branch 2: Flag high readings
    p.map(lambda r: r["temperature"]).filter(lambda t: t > 25).map(lambda t: f"High temp: {t:.1f}°C").sink(high_readings.append)

    # Simulate data coming in
    sensors = [
        ("Temp_A", 20, 50),
        ("Temp_B", 22, 45),
        ("Temp_C", 18, 55),
    ]

    for _ in range(5):
        for sensor_id, base_temp, base_humidity in sensors:
            reading = generate_sensor_data(sensor_id, base_temp, base_humidity)
            source.emit(reading)
        time.sleep(0.01)  # Small delay to simulate real-time

    print(f"Processed {len(processed)} readings")
    print(f"Sample processed: {processed[:3]}")

    if high_readings:
        print(f"High readings detected: {high_readings}")
    else:
        print("No high readings detected")

    # ---------------------------------------------------------
    # SECTION: Complete ETL-style Pipeline
    # ---------------------------------------------------------
    # Extract -> Transform -> Load pattern
    print("\n--- ETL pipeline pattern ---")

    # Sample transaction data
    transactions = [
        {"id": 1, "amount": 100.50, "currency": "USD", "type": "purchase"},
        {"id": 2, "amount": 50.00, "currency": "EUR", "type": "refund"},
        {"id": 3, "amount": 200.00, "currency": "USD", "type": "purchase"},
        {"id": 4, "amount": -30.00, "currency": "GBP", "type": "refund"},
        {"id": 5, "amount": 75.25, "currency": "USD", "type": "purchase"},
    ]

    # Exchange rates
    rates = {"USD": 1.0, "EUR": 1.10, "GBP": 1.25}

    converted_amounts = []
    transaction_summary = []

    source = akayu.Stream()

    # Transform: convert to USD
    def to_usd(tx):
        rate = rates.get(tx["currency"], 1.0)
        return {
            **tx,
            "amount_usd": round(tx["amount"] * rate, 2)
        }

    transformed = source.map(to_usd)

    # Branch 1: Get all amounts in USD
    transformed.map(lambda tx: tx["amount_usd"]).sink(converted_amounts.append)

    # Branch 2: Create summary
    def summarize(tx):
        return f"{tx['type'].upper()}: ${tx['amount_usd']:.2f}"

    transformed.map(summarize).sink(transaction_summary.append)

    # Process
    for tx in transactions:
        source.emit(tx)

    print(f"Converted amounts (USD): {converted_amounts}")
    print(f"Transaction summary:")
    for s in transaction_summary:
        print(f"  {s}")

    print("\n" + "=" * 60)
    print("EXAMPLE 08 COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()