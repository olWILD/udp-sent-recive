# UDP Monitor

A high-precision UDP packet monitoring tool with timestamp-based latency measurement, clock synchronization, and comprehensive statistics export.

## Features

- **Latency Measurement**: Uses timestamp-based one-way latency calculation with clock synchronization
- **Real-time Statistics**: Live display of packet loss, latency metrics (min/avg/max/P95/P99), and jitter
- **Clock Synchronization**: NTP-like protocol for accurate time synchronization between endpoints
- **Flexible Operation Modes**: Send & receive, or receive-only mode
- **Data Export**: Export statistics to JSON and/or CSV formats with configurable intervals
- **Comprehensive Metrics**: Packet loss detection, latency distribution buckets, and delivery rate tracking
- **Interactive Display**: Real-time animated status display with activity tracking
- **Graceful Shutdown**: Proper cleanup and final statistics export on Ctrl+C

## Installation

### Prerequisites

- Rust (1.70 or later)
- Cargo package manager

### Build from Source

```bash
git clone <repository-url>
cd udp
cargo build --release
```

The compiled binary will be available at `target/release/udp.exe` (Windows) or `target/release/udp` (Linux/macOS).

## Usage

### Basic Syntax

```bash
udp -l <LOCAL_ADDR> -r <REMOTE_ADDR> [OPTIONS]
```

### Required Parameters

- `-l, --local <LOCAL_ADDR>`: Local address to bind to (e.g., `0.0.0.0:8080`)
- `-r, --remote <REMOTE_ADDR>`: Remote address to send packets to (e.g., `192.168.1.100:8080`)

### Optional Parameters

- `--receive-only`: Only receive packets without sending (remote address not required)
- `-i, --interval <MS>`: Interval between packets in milliseconds (default: 1000)
- `-c, --count <COUNT>`: Number of packets to send (0 for unlimited, default: 0)
- `--json <JSON_FILE>`: Export statistics to JSON file
- `--csv <CSV_FILE>`: Export statistics to CSV file
- `--stats-interval <SECONDS>`: Display update interval in seconds (default: 1)
- `--sync`: Enable initial synchronization before sending packets
- `--sync-timeout <SECONDS>`: Timeout for initial synchronization (default: 30)
- `--clock-sync-interval <SECONDS>`: Clock synchronization interval (default: 5)
- `--export-interval <SECONDS>`: Statistics export interval (default: 5)

## Examples

### Basic Packet Monitoring

Send packets from local port 8080 to remote host:
```bash
udp -l 0.0.0.0:8080 -r 192.168.1.100:8080
```

### High-Frequency Testing

Send packets every 100ms with statistics export:
```bash
udp -l 0.0.0.0:8080 -r 192.168.1.100:8080 -i 100 --json results.json --csv results.csv
```

### Receive-Only Mode

Monitor incoming packets without sending:
```bash
udp -l 0.0.0.0:8080 --receive-only
```

### Synchronized Start

Wait for connection before starting packet transmission:
```bash
udp -l 0.0.0.0:8080 -r 192.168.1.100:8080 --sync --sync-timeout 60
```

### Limited Packet Count

Send exactly 1000 packets:
```bash
udp -l 0.0.0.0:8080 -r 192.168.1.100:8080 -c 1000
```

## Output Format

### Real-time Display

The tool provides a live dashboard showing:

```
UDP Monitor Status at 14:30:25 ⠋
--------------------------------------------------------------------------------
Clock Sync: Offset: -2ms, Quality: Good, Samples: 12/15
Packets:  Sent: 145  Received: 143  Lost: 2 (1.4%)
Latency:  Min: 12.3 ms  Avg: 15.7 ms  Max: 23.1 ms  P95: 21.2 ms  P99: 22.8 ms  Jitter: 2.1 ms
Rate:     1.0 packets/sec     Running continuously
Sequence:  Next Expected: #146    Delivery Rate: 98.6%
--------------------------------------------------------------------------------
Last sent:     Packet #145    at 14:30:25.123
Last received: Packet #143    at 14:30:24.891 (Latency: 15.2 ms)
--------------------------------------------------------------------------------
Status:   JSON+CSV export enabled    Press Ctrl+C to exit
Send interval: 1000 ms  Clock sync interval: 5 sec  Export interval: 5 sec
```

### JSON Export Format

```json
[
  {
    "sent": 100,
    "received": 98,
    "lost": 2,
    "loss_percent": 2.04,
    "detected_lost_packets": 2,
    "timestamp": "2025-07-31T14:30:25.123Z",
    "min_latency_ms": 12.3,
    "avg_latency_ms": 15.7,
    "max_latency_ms": 23.1,
    "p95_latency_ms": 21.2,
    "p99_latency_ms": 22.8,
    "jitter_ms": 2.1,
    "latency_buckets": {
      "0-25ms": 95,
      "25-50ms": 3,
      "50-75ms": 0,
      ...
    }
  }
]
```

### CSV Export Format

The CSV export includes all metrics as separate columns:
- Packet counts (sent, received, lost, detected_lost_packets)
- Loss percentage
- Timestamp
- Latency statistics (min, avg, max, P95, P99, jitter)
- Latency distribution buckets (bucket_0_25ms, bucket_25_50ms, etc.)

## Clock Synchronization

The tool implements a simplified NTP-like protocol for clock synchronization:

1. **Time Sync Request**: Sender includes timestamp T1
2. **Time Sync Response**: Receiver adds timestamps T2 (receive time) and T3 (send time)
3. **Offset Calculation**: Uses all four timestamps (T1, T2, T3, T4) to calculate clock offset
4. **Quality Assessment**: Tracks standard deviation of offset measurements

### Clock Quality Levels

- **Excellent**: Standard deviation < 10ms
- **Good**: Standard deviation < 25ms
- **Fair**: Standard deviation < 50ms
- **Poor**: Standard deviation ≥ 50ms or insufficient samples
- **Stale**: No recent synchronization data

## Latency Measurement

### One-Way Latency Calculation

The tool measures true one-way latency by:
1. Synchronizing clocks between endpoints
2. Including send timestamp in each packet
3. Calculating latency as: `receive_time - adjusted_send_time`

### Metrics Provided

- **Min/Max/Average**: Basic latency statistics
- **Percentiles**: P95 and P99 latency values
- **Jitter**: Variation between consecutive measurements (exponential moving average)
- **Distribution**: Latency buckets from 0-25ms to >1000ms

## Network Requirements

- UDP connectivity between endpoints
- Symmetric network path (for accurate clock synchronization)
- Firewall rules allowing UDP traffic on specified ports

## Troubleshooting

### Common Issues

1. **"Address already in use"**: Choose a different local port or wait for the previous session to timeout
2. **"No route to host"**: Check network connectivity and firewall settings
3. **High latency variation**: Network congestion or asymmetric routing
4. **Clock sync quality "Poor"**: Network jitter or asymmetric paths affecting synchronization

### Debug Tips

- Use `--sync` to ensure proper connection establishment
- Monitor clock sync quality - "Good" or better recommended for accurate latency
- Check that both endpoints can reach each other (test with ping)
- Verify firewall settings allow UDP traffic

## Performance Notes

- Designed for monitoring intervals of 100ms or higher
- Memory usage grows with latency sample history (configurable limits)
- CPU usage is minimal during normal operation
- File I/O occurs only during statistics export intervals

## License

This project is available under standard open source terms. See the source code for specific