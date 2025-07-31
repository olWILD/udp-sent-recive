#!/bin/bash

# Test script to validate the receive-only mode fix
# This script tests that clock synchronization and latency calculations work properly in receive-only mode

set -e

echo "=== Testing UDP Monitor Receive-Only Mode Fix ==="

# Build the project
echo "Building project..."
cargo build --release

# Test 1: Standalone receive-only mode (should show no latency initially)
echo "Test 1: Starting receive-only mode without sender..."
timeout 3s ./target/release/udp_monitor --local 127.0.0.1:9001 --receive-only --csv test1.csv --json test1.json || true

# Check that initial export has empty latency values
echo "Checking initial CSV export (should have empty latency values)..."
if grep -q ",,,,," test1.csv; then
    echo "✓ Initial CSV correctly shows empty latency values"
else
    echo "✗ Initial CSV should have empty latency values"
    exit 1
fi

# Test 2: Receive-only mode with sender (should establish clock sync and show latency)
echo "Test 2: Testing receive-only mode with sender..."

# Start receiver in background
./target/release/udp_monitor --local 127.0.0.1:9002 --receive-only --csv test2.csv --json test2.json &
RECEIVER_PID=$!

# Give receiver time to start
sleep 1

# Start sender for a few seconds
timeout 8s ./target/release/udp_monitor --local 127.0.0.1:9003 --remote 127.0.0.1:9002 --interval 500 --count 15 || true

# Give receiver time to process and export
sleep 2

# Stop receiver
kill $RECEIVER_PID 2>/dev/null || true
wait $RECEIVER_PID 2>/dev/null || true

# Check that final export has latency values
echo "Checking final CSV export (should have latency values)..."
if grep -E "[0-9]+\.[0-9]+,[0-9]+\.[0-9]+,[0-9]+\.[0-9]+" test2.csv; then
    echo "✓ Final CSV correctly shows latency values (min,avg,max)"
else
    echo "✗ Final CSV should have latency values"
    echo "CSV content:"
    cat test2.csv
    exit 1
fi

# Check JSON has proper latency data
echo "Checking JSON export..."
if grep -q '"min_latency_ms": [0-9]' test2.json && grep -q '"max_latency_ms": [0-9]' test2.json; then
    echo "✓ JSON correctly shows latency values"
else
    echo "✗ JSON should have latency values"
    echo "JSON content:"
    cat test2.json
    exit 1
fi

# Clean up test files
rm -f test1.csv test1.json test2.csv test2.json

echo "=== All tests passed! ==="
echo "✓ Receive-only mode now properly establishes clock synchronization"
echo "✓ Latency calculations work correctly when data packets are received"
echo "✓ CSV and JSON exports contain proper min/max/avg values"