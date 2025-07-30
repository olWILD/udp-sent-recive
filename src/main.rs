use std::fs::{self, File, OpenOptions};
use std::io::{self, Write, Read, Seek};
use std::net::{UdpSocket, SocketAddr};
use std::str::FromStr;
use std::sync::{Arc, Mutex, atomic::{AtomicBool, Ordering}};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH, Instant};
use std::collections::{VecDeque, HashMap};
use clap::{App, Arg};
use serde::{Serialize, Deserialize};
use serde_json::{Value, json};
use chrono::{DateTime, Utc};

// Packet types
const PACKET_TYPE_SYNC: u8 = 1;
const PACKET_TYPE_SYNC_ACK: u8 = 2;
const PACKET_TYPE_DATA: u8 = 3;
const PACKET_TYPE_TERMINATE: u8 = 4;
const PACKET_TYPE_TIME_SYNC: u8 = 5;          // Packet for time synchronization
const PACKET_TYPE_TIME_SYNC_RESP: u8 = 6;     // Response to time sync packet

// Configuration constants
const CLOCK_SYNC_VALIDITY_DURATION_SECS: u64 = 30;
const CLOCK_SYNC_MIN_SAMPLES: u64 = 5;
const CLOCK_SYNC_RELIABLE_STDDEV: f64 = 50.0;
const CLOCK_SYNC_EXCELLENT_STDDEV: f64 = 10.0;
const CLOCK_SYNC_GOOD_STDDEV: f64 = 25.0;
const LATENCY_SAMPLE_HISTORY_SIZE: usize = 1000;
const CLOCK_OFFSET_HISTORY_SIZE: usize = 10;
const TERMINATE_PACKET_RETRY_COUNT: usize = 5;
const TERMINATE_PACKET_RETRY_DELAY_MS: u64 = 100;
const TIME_SYNC_REQUEST_TIMEOUT_SECS: u64 = 10;
const SOCKET_READ_TIMEOUT_MS: u64 = 100;
const THREAD_SLEEP_MS: u64 = 10;
const JITTER_EMA_ALPHA: f64 = 0.2; // Exponential moving average alpha for jitter calculation

// Long-running operation constants
const MAX_FILE_SIZE_MB: u64 = 100; // Maximum file size before rotation
const FILE_SAVE_RETRY_COUNT: usize = 3;
const FILE_SAVE_RETRY_DELAY_MS: u64 = 500;
const SHUTDOWN_TIMEOUT_MS: u64 = 5000; // Increased timeout for long-running cleanup

// Helper function to get latency bucket key efficiently
fn get_latency_bucket_key(latency_ms: f64) -> &'static str {
    match latency_ms as u64 {
        0..=24 => "0-25ms",
        25..=49 => "25-50ms",
        50..=74 => "50-75ms",
        75..=99 => "75-100ms",
        100..=124 => "100-125ms",
        125..=149 => "125-150ms",
        150..=174 => "150-175ms",
        175..=199 => "175-200ms",
        200..=249 => "200-250ms",
        250..=299 => "250-300ms",
        300..=349 => "300-350ms",
        350..=399 => "350-400ms",
        400..=449 => "400-450ms",
        450..=499 => "450-500ms",
        500..=999 => "500-999ms",
        _ => ">1000ms",
    }
}

// Structure for clock synchronization
#[derive(Clone, Debug)]
struct ClockSync {
    // Offset in milliseconds between local and remote clocks
    offset_ms: i64,
    // When was the offset last updated
    last_update: Instant,
    // Standard deviation of offset measurements (for quality assessment)
    offset_stddev: f64,
    // Recent offset samples for smoothing
    recent_offsets: VecDeque<i64>,
    // Number of sync packets sent
    sync_packets_sent: u64,
    // Number of sync responses received
    sync_responses_received: u64,
}

impl ClockSync {
    fn new() -> Self {
        ClockSync {
            offset_ms: 0,
            last_update: Instant::now(),
            offset_stddev: 0.0,
            recent_offsets: VecDeque::with_capacity(CLOCK_OFFSET_HISTORY_SIZE),
            sync_packets_sent: 0,
            sync_responses_received: 0,
        }
    }
    
    // Process a time sync response and update our offset
    fn process_sync_response(&mut self, t1: u64, t2: u64, t3: u64, t4: u64) {
        // Calculate round-trip time
        let rtt_ms = (t4 - t1) - (t3 - t2);
        
        // Calculate one-way delay (assuming symmetric path)
        let _one_way_delay_ms = rtt_ms / 2;
        
        // Calculate offset: how much the remote clock is ahead of local clock
        let offset = ((t2 as i64 - t1 as i64) + (t3 as i64 - t4 as i64)) / 2;
        
        // Add to recent offsets and keep last CLOCK_OFFSET_HISTORY_SIZE
        self.recent_offsets.push_back(offset);
        if self.recent_offsets.len() > CLOCK_OFFSET_HISTORY_SIZE {
            self.recent_offsets.pop_front();
        }
        
        // Calculate new smoothed offset (average of recent offsets)
        let mut sum = 0;
        for &o in &self.recent_offsets {
            sum += o;
        }
        
        // Calculate mean
        self.offset_ms = sum / self.recent_offsets.len() as i64;
        
        // Calculate standard deviation
        let mean = self.offset_ms as f64;
        let mut sum_squared_diff = 0.0;
        for &o in &self.recent_offsets {
            let diff = o as f64 - mean;
            sum_squared_diff += diff * diff;
        }
        self.offset_stddev = (sum_squared_diff / self.recent_offsets.len() as f64).sqrt();
        
        self.last_update = Instant::now();
        self.sync_responses_received += 1;
    }
    
    // Apply offset to convert remote timestamp to local time
    fn remote_to_local(&self, remote_timestamp: u64) -> u64 {
        // Subtract offset (if remote clock is ahead, we subtract; if behind, we add)
        if self.offset_ms >= 0 {
            // Remote clock is ahead, subtract offset
            remote_timestamp.saturating_sub(self.offset_ms as u64)
        } else {
            // Remote clock is behind, add offset
            remote_timestamp.saturating_add(-self.offset_ms as u64)
        }
    }
    
    // Check if our clock sync is fresh enough
    fn is_valid(&self) -> bool {
        self.last_update.elapsed() < Duration::from_secs(CLOCK_SYNC_VALIDITY_DURATION_SECS)
    }
    
    // Check if sync quality is good (low stddev)
    fn is_reliable(&self) -> bool {
        self.sync_responses_received >= CLOCK_SYNC_MIN_SAMPLES && self.offset_stddev < CLOCK_SYNC_RELIABLE_STDDEV
    }
    
    // Get synchronization quality as a string
    fn get_quality_string(&self) -> String {
        if !self.is_valid() {
            "Stale".to_string()
        } else if !self.is_reliable() {
            "Poor".to_string()
        } else if self.offset_stddev < CLOCK_SYNC_EXCELLENT_STDDEV {
            "Excellent".to_string()
        } else if self.offset_stddev < CLOCK_SYNC_GOOD_STDDEV {
            "Good".to_string()
        } else {
            "Fair".to_string()
        }
    }
}

// Structure for latency statistics
#[derive(Clone, Debug)]
struct LatencyStats {
    min_latency_ms: Option<f64>,
    max_latency_ms: Option<f64>,
    avg_latency_ms: Option<f64>,
    total_latency_ms: f64,
    samples_count: u64,
    // Jitter (variation in latency)
    jitter_ms: Option<f64>,
    prev_latency_ms: Option<f64>,
    // Recent latency measurements for percentiles
    recent_latencies: VecDeque<f64>,
    // Percentile values
    p95_latency_ms: Option<f64>,
    p99_latency_ms: Option<f64>,
    // Latency distribution buckets
    latency_buckets: HashMap<String, u64>,
    // Interval tracking - for calculating interval-based statistics
    interval_latencies: VecDeque<f64>,  // Latencies since last interval export
}

// Structure for interval latency statistics - calculated for each export interval
#[derive(Clone, Debug)]
struct IntervalLatencyStats {
    min_latency_ms: Option<f64>,
    max_latency_ms: Option<f64>,
    avg_latency_ms: Option<f64>,
    p95_latency_ms: Option<f64>,
    p99_latency_ms: Option<f64>,
    jitter_ms: Option<f64>,
}

impl LatencyStats {
    fn new() -> Self {
        // Initialize latency buckets
        let buckets = [
            "0-25ms", "25-50ms", "50-75ms", "75-100ms", "100-125ms", "125-150ms", "150-175ms", "175-200ms",
            "200-250ms", "250-300ms", "300-350ms", "350-400ms", "400-450ms", "450-500ms",
            "500-999ms", ">1000ms",
        ]
        .iter()
        .map(|s| (s.to_string(), 0))
        .collect();
        
        LatencyStats {
            min_latency_ms: None,
            max_latency_ms: None,
            avg_latency_ms: None,
            total_latency_ms: 0.0,
            samples_count: 0,
            jitter_ms: None,
            prev_latency_ms: None,
            recent_latencies: VecDeque::with_capacity(LATENCY_SAMPLE_HISTORY_SIZE),
            p95_latency_ms: None,
            p99_latency_ms: None,
            latency_buckets: buckets,
            interval_latencies: VecDeque::new(),
        }
    }
    
    // Process a new latency measurement
    fn add_sample(&mut self, latency_ms: f64) {
        // Update min/max
        if let Some(min) = self.min_latency_ms {
            if latency_ms < min {
                self.min_latency_ms = Some(latency_ms);
            }
        } else {
            self.min_latency_ms = Some(latency_ms);
        }
        
        if let Some(max) = self.max_latency_ms {
            if latency_ms > max {
                self.max_latency_ms = Some(latency_ms);
            }
        } else {
            self.max_latency_ms = Some(latency_ms);
        }
        
        // Update average
        self.total_latency_ms += latency_ms;
        self.samples_count += 1;
        self.avg_latency_ms = Some(self.total_latency_ms / self.samples_count as f64);
        
        // Update jitter (variation between consecutive latency measurements)
        if let Some(prev) = self.prev_latency_ms {
            let current_jitter = (latency_ms - prev).abs();
            
            if let Some(jitter) = self.jitter_ms {
                // Use exponential moving average for jitter
                self.jitter_ms = Some((1.0 - JITTER_EMA_ALPHA) * jitter + JITTER_EMA_ALPHA * current_jitter);
            } else {
                self.jitter_ms = Some(current_jitter);
            }
        }
        self.prev_latency_ms = Some(latency_ms);
        
        // Store recent latencies for percentiles
        self.recent_latencies.push_back(latency_ms);
        if self.recent_latencies.len() > LATENCY_SAMPLE_HISTORY_SIZE {
            self.recent_latencies.pop_front();
        }
        
        // Store latency for interval calculations
        self.interval_latencies.push_back(latency_ms);
        
        // Update latency buckets more efficiently
        let bucket_key = get_latency_bucket_key(latency_ms);
        *self.latency_buckets.entry(bucket_key.to_string()).or_insert(0) += 1;
        
        // Update percentiles
        self.p95_latency_ms = self.percentile(95.0);
        self.p99_latency_ms = self.percentile(99.0);
    }
    
    // Calculate percentile latency
    fn percentile(&self, p: f64) -> Option<f64> {
        if self.recent_latencies.is_empty() {
            return None;
        }
        
        let mut sorted = self.recent_latencies.iter().cloned().collect::<Vec<_>>();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        
        let idx = (p * sorted.len() as f64 / 100.0).floor() as usize;
        let idx = idx.min(sorted.len() - 1);
        Some(sorted[idx])
    }
    
    // Get latency stats string
    fn get_stats_string(&self) -> String {
        let min = self.min_latency_ms.map_or("N/A".to_string(), |v| format!("{v:.1}"));
        let avg = self.avg_latency_ms.map_or("N/A".to_string(), |v| format!("{v:.1}"));
        let max = self.max_latency_ms.map_or("N/A".to_string(), |v| format!("{v:.1}"));
        let p95 = self.p95_latency_ms.map_or("N/A".to_string(), |v| format!("{v:.1}"));
        let p99 = self.p99_latency_ms.map_or("N/A".to_string(), |v| format!("{v:.1}"));
        let jitter = self.jitter_ms.map_or("N/A".to_string(), |v| format!("{v:.1}"));
        
        format!("Min: {min} ms  Avg: {avg} ms  Max: {max} ms  P95: {p95} ms  P99: {p99} ms  Jitter: {jitter} ms")
    }
    
    // Calculate interval statistics from collected interval latencies
    fn calculate_interval_stats(&self) -> IntervalLatencyStats {
        if self.interval_latencies.is_empty() {
            return IntervalLatencyStats {
                min_latency_ms: None,
                max_latency_ms: None,
                avg_latency_ms: None,
                p95_latency_ms: None,
                p99_latency_ms: None,
                jitter_ms: None,
            };
        }
        
        let latencies: Vec<f64> = self.interval_latencies.iter().cloned().collect();
        
        // Calculate min/max
        let min_latency = latencies.iter().cloned().fold(f64::INFINITY, f64::min);
        let max_latency = latencies.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        
        // Calculate average
        let total: f64 = latencies.iter().sum();
        let avg_latency = total / latencies.len() as f64;
        
        // Calculate percentiles
        let mut sorted = latencies.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        
        let p95_idx = ((95.0 * sorted.len() as f64 / 100.0).floor() as usize).min(sorted.len() - 1);
        let p99_idx = ((99.0 * sorted.len() as f64 / 100.0).floor() as usize).min(sorted.len() - 1);
        
        let p95_latency = Some(sorted[p95_idx]);
        let p99_latency = Some(sorted[p99_idx]);
        
        // Calculate jitter for interval (average variation between consecutive measurements)
        let mut jitter_sum = 0.0;
        let mut jitter_count = 0;
        for i in 1..latencies.len() {
            jitter_sum += (latencies[i] - latencies[i-1]).abs();
            jitter_count += 1;
        }
        let jitter = if jitter_count > 0 {
            Some(jitter_sum / jitter_count as f64)
        } else {
            None
        };
        
        IntervalLatencyStats {
            min_latency_ms: Some(min_latency),
            max_latency_ms: Some(max_latency),
            avg_latency_ms: Some(avg_latency),
            p95_latency_ms: p95_latency,
            p99_latency_ms: p99_latency,
            jitter_ms: jitter,
        }
    }
    
    // Clear interval latencies after export (call this after calculating interval stats)
    fn clear_interval_latencies(&mut self) {
        self.interval_latencies.clear();
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct PacketStats {
    // Packet counting
    sent: u64,
    received: u64,
    lost: u64,
    loss_percent: f64,
    expected_next_seq: u64,
    detected_lost_packets: u64,
    
    // Timing and latency data
    timestamp: DateTime<Utc>,
    min_latency_ms: Option<f64>,
    avg_latency_ms: Option<f64>,
    max_latency_ms: Option<f64>,
    p95_latency_ms: Option<f64>,
    p99_latency_ms: Option<f64>,
    jitter_ms: Option<f64>,
    
    // Clock sync info
    clock_offset_ms: i64,
    clock_quality: String,
    
    // Latency distribution buckets
    latency_buckets: HashMap<String, u64>,
}

#[derive(Serialize, Debug)]
struct CsvExportStats {
    // Packet counting
    sent: u64,
    received: u64,
    lost: u64,
    #[serde(serialize_with = "round_f64")]
    loss_percent: f64,
    detected_lost_packets: u64,
    
    // Timing and latency data
    timestamp: DateTime<Utc>,
    #[serde(serialize_with = "round_option_f64")]
    min_latency_ms: Option<f64>,
    #[serde(serialize_with = "round_option_f64")]
    avg_latency_ms: Option<f64>,
    #[serde(serialize_with = "round_option_f64")]
    max_latency_ms: Option<f64>,
    #[serde(serialize_with = "round_option_f64")]
    p95_latency_ms: Option<f64>,
    #[serde(serialize_with = "round_option_f64")]
    p99_latency_ms: Option<f64>,
    #[serde(serialize_with = "round_option_f64")]
    jitter_ms: Option<f64>,
    
    // Latency distribution buckets as separate fields
    bucket_0_25ms: u64,
    bucket_25_50ms: u64,
    bucket_50_75ms: u64,
    bucket_75_100ms: u64,
    bucket_100_125ms: u64,
    bucket_125_150ms: u64,
    bucket_150_175ms: u64,
    bucket_175_200ms: u64,
    bucket_200_250ms: u64,
    bucket_250_300ms: u64,
    bucket_300_350ms: u64,
    bucket_350_400ms: u64,
    bucket_400_450ms: u64,
    bucket_450_500ms: u64,
    bucket_500_999ms: u64,
    bucket_gt_1000ms: u64,
}


#[derive(Serialize, Deserialize, Clone, Debug)]
struct JsonExportStats {
    // Packet counting
    sent: u64,
    received: u64,
    lost: u64,
    #[serde(serialize_with = "round_f64")]
    loss_percent: f64,
    detected_lost_packets: u64,
    
    // Timing and latency data
    timestamp: DateTime<Utc>,
    #[serde(serialize_with = "round_option_f64")]
    min_latency_ms: Option<f64>,
    #[serde(serialize_with = "round_option_f64")]
    avg_latency_ms: Option<f64>,
    #[serde(serialize_with = "round_option_f64")]
    max_latency_ms: Option<f64>,
    #[serde(serialize_with = "round_option_f64")]
    p95_latency_ms: Option<f64>,
    #[serde(serialize_with = "round_option_f64")]
    p99_latency_ms: Option<f64>,
    #[serde(serialize_with = "round_option_f64")]
    jitter_ms: Option<f64>,
    
    // Latency distribution buckets as HashMap
    latency_buckets: HashMap<String, u64>,
}

fn round_f64<S>(value: &f64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_f64((value * 100.0).round() / 100.0)
}

fn round_option_f64<S>(value: &Option<f64>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match value {
        Some(v) => serializer.serialize_some(&((v * 100.0).round() / 100.0)),
        None => serializer.serialize_none(),
    }
}

// Helper function to check if file needs rotation
fn should_rotate_file(file_path: &str) -> bool {
    if let Ok(metadata) = fs::metadata(file_path) {
        let size_mb = metadata.len() / (1024 * 1024);
        size_mb >= MAX_FILE_SIZE_MB
    } else {
        false
    }
}

// Helper function to rotate file by renaming it with timestamp
fn rotate_file(file_path: &str) -> std::io::Result<()> {
    if std::path::Path::new(file_path).exists() {
        let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S");
        let extension = std::path::Path::new(file_path)
            .extension()
            .and_then(|ext| ext.to_str())
            .unwrap_or("");
        let stem = std::path::Path::new(file_path)
            .file_stem()
            .and_then(|stem| stem.to_str())
            .unwrap_or("backup");
        
        let backup_path = if extension.is_empty() {
            format!("{}_{}", file_path, timestamp)
        } else {
            format!("{}_{}.{}", stem, timestamp, extension)
        };
        
        println!("Rotating file {} to {}", file_path, backup_path);
        fs::rename(file_path, backup_path)?;
    }
    Ok(())
}

// Retry wrapper for file operations
fn retry_operation<F, T>(mut operation: F, operation_name: &str) -> std::io::Result<T>
where
    F: FnMut() -> std::io::Result<T>,
{
    let mut last_error = None;
    
    for attempt in 1..=FILE_SAVE_RETRY_COUNT {
        match operation() {
            Ok(result) => return Ok(result),
            Err(e) => {
                eprintln!("Attempt {}/{} failed for {}: {}", attempt, FILE_SAVE_RETRY_COUNT, operation_name, e);
                last_error = Some(e);
                if attempt < FILE_SAVE_RETRY_COUNT {
                    thread::sleep(Duration::from_millis(FILE_SAVE_RETRY_DELAY_MS));
                }
            }
        }
    }
    
    Err(last_error.unwrap_or_else(|| std::io::Error::new(
        std::io::ErrorKind::Other,
        format!("All {} attempts failed for {}", FILE_SAVE_RETRY_COUNT, operation_name)
    )))
}

impl PacketStats {
    fn new() -> Self {
        // Initialize latency buckets (standardized to match LatencyStats)
        let buckets = [
            "0-25ms", "25-50ms", "50-75ms", "75-100ms", "100-125ms", "125-150ms", "150-175ms", "175-200ms",
            "200-250ms", "250-300ms", "300-350ms", "350-400ms", "400-450ms", "450-500ms",
            "500-999ms", ">1000ms",
        ]
        .iter()
        .map(|s| (s.to_string(), 0))
        .collect();
        
        PacketStats {
            sent: 0,
            received: 0,
            lost: 0,
            loss_percent: 0.0,
            expected_next_seq: 0,
            detected_lost_packets: 0,
            timestamp: Utc::now(),
            min_latency_ms: None,
            avg_latency_ms: None,
            max_latency_ms: None,
            p95_latency_ms: None,
            p99_latency_ms: None,
            jitter_ms: None,
            clock_offset_ms: 0,
            clock_quality: "Unknown".to_string(),
            latency_buckets: buckets,
        }
    }

    fn process_sequence(&mut self, seq_num: u64) {
        // If this is the first packet, initialize expected sequence
        if self.received == 0 {
            self.expected_next_seq = seq_num;
        }
        
        // Check if we received the expected sequence number
        if seq_num == self.expected_next_seq {
            // Perfect, got the expected packet
            self.expected_next_seq = seq_num + 1;
        } else if seq_num > self.expected_next_seq {
            // We missed some packets! Count them as lost
            let missed_packets = seq_num - self.expected_next_seq;
            self.detected_lost_packets += missed_packets;
            
            // Update expected next sequence
            self.expected_next_seq = seq_num + 1;
        } else {
            // Received an old packet (out of order delivery)
            // We just count it but don't adjust expected_next_seq
        }
        
        // Always count the received packet
        self.received += 1;
    }

    fn update_loss_stats(&mut self) {
        self.lost = self.detected_lost_packets;
        
        let total_expected = self.received + self.lost;
        if total_expected > 0 {
            self.loss_percent = self.lost as f64 / total_expected as f64 * 100.0;
        } else {
            self.loss_percent = 0.0;
        }
        
        self.timestamp = Utc::now();
    }
    
    // Update latency stats from LatencyStats object
    fn update_latency_stats(&mut self, latency: &LatencyStats) {
        self.min_latency_ms = latency.min_latency_ms;
        self.avg_latency_ms = latency.avg_latency_ms;
        self.max_latency_ms = latency.max_latency_ms;
        self.p95_latency_ms = latency.p95_latency_ms;
        self.p99_latency_ms = latency.p99_latency_ms;
        self.jitter_ms = latency.jitter_ms;
        self.latency_buckets = latency.latency_buckets.clone();
    }
    
    // Update clock sync info
    fn update_clock_sync(&mut self, clock_sync: &ClockSync) {
        self.clock_offset_ms = clock_sync.offset_ms;
        self.clock_quality = clock_sync.get_quality_string();
    }
    fn to_json_export_stats(&self) -> JsonExportStats {
        JsonExportStats {
            sent: self.sent,
            received: self.received,
            lost: self.lost,
            loss_percent: self.loss_percent,
            detected_lost_packets: self.detected_lost_packets,
            timestamp: self.timestamp,
            min_latency_ms: self.min_latency_ms,
            avg_latency_ms: self.avg_latency_ms,
            max_latency_ms: self.max_latency_ms,
            p95_latency_ms: self.p95_latency_ms,
            p99_latency_ms: self.p99_latency_ms,
            jitter_ms: self.jitter_ms,
            latency_buckets: self.latency_buckets.clone(),
        }
    }

    fn to_csv_export_stats(&self) -> CsvExportStats {
        CsvExportStats {
            sent: self.sent,
            received: self.received,
            lost: self.lost,
            loss_percent: self.loss_percent,
            detected_lost_packets: self.detected_lost_packets,
            timestamp: self.timestamp,
            min_latency_ms: self.min_latency_ms,
            avg_latency_ms: self.avg_latency_ms,
            max_latency_ms: self.max_latency_ms,
            p95_latency_ms: self.p95_latency_ms,
            p99_latency_ms: self.p99_latency_ms,
            jitter_ms: self.jitter_ms,
            bucket_0_25ms: *self.latency_buckets.get("0-25ms").unwrap_or(&0),
            bucket_25_50ms: *self.latency_buckets.get("25-50ms").unwrap_or(&0),
            bucket_50_75ms: *self.latency_buckets.get("50-75ms").unwrap_or(&0),
            bucket_75_100ms: *self.latency_buckets.get("75-100ms").unwrap_or(&0),
            bucket_100_125ms: *self.latency_buckets.get("100-125ms").unwrap_or(&0),
            bucket_125_150ms: *self.latency_buckets.get("125-150ms").unwrap_or(&0),
            bucket_150_175ms: *self.latency_buckets.get("150-175ms").unwrap_or(&0),
            bucket_175_200ms: *self.latency_buckets.get("175-200ms").unwrap_or(&0),
            bucket_200_250ms: *self.latency_buckets.get("200-250ms").unwrap_or(&0),
            bucket_250_300ms: *self.latency_buckets.get("250-300ms").unwrap_or(&0),
            bucket_300_350ms: *self.latency_buckets.get("300-350ms").unwrap_or(&0),
            bucket_350_400ms: *self.latency_buckets.get("350-400ms").unwrap_or(&0),
            bucket_400_450ms: *self.latency_buckets.get("400-450ms").unwrap_or(&0),
            bucket_450_500ms: *self.latency_buckets.get("450-500ms").unwrap_or(&0),
            bucket_500_999ms: *self.latency_buckets.get("500-999ms").unwrap_or(&0),
            bucket_gt_1000ms: *self.latency_buckets.get(">1000ms").unwrap_or(&0),
        }
    }
}

fn export_to_json(stats: &PacketStats, json_file: &str) -> std::io::Result<()> {
    retry_operation(|| {
        // Check if file needs rotation before writing
        if should_rotate_file(json_file) {
            rotate_file(json_file)?;
        }
        
        // Create the directory if it doesn't exist
        if let Some(parent) = std::path::Path::new(json_file).parent() {
            std::fs::create_dir_all(parent)?;
        }
        
        // Convert stats to JSON export format
        let export_stats = stats.to_json_export_stats();
        let stats_json = serde_json::to_value(export_stats)?;
        
        // For large files, use append-only approach to avoid reading entire file
        let file_exists = std::path::Path::new(json_file).exists();
        
        if !file_exists {
            // New file - create with array containing single entry
            let json_data = json!([stats_json]);
            let mut file = File::create(json_file)?;
            let json_str = serde_json::to_string_pretty(&json_data)?;
            file.write_all(json_str.as_bytes())?;
            file.flush()?;
        } else {
            // Existing file - check if it's small enough to read entirely
            let metadata = fs::metadata(json_file)?;
            let file_size_mb = metadata.len() / (1024 * 1024);
            
            if file_size_mb < 10 { // Small file - use existing logic
                let json_data = match fs::read_to_string(json_file) {
                    Ok(content) if !content.trim().is_empty() => {
                        match serde_json::from_str::<Value>(&content) {
                            Ok(Value::Array(mut arr)) => {
                                arr.push(stats_json);
                                Value::Array(arr)
                            },
                            _ => {
                                json!([stats_json])
                            }
                        }
                    },
                    _ => {
                        json!([stats_json])
                    }
                };
                
                let temp_file = format!("{json_file}.tmp");
                {
                    let mut file = File::create(&temp_file)?;
                    let json_str = serde_json::to_string_pretty(&json_data)?;
                    file.write_all(json_str.as_bytes())?;
                    file.flush()?;
                }
                fs::rename(temp_file, json_file)?;
            } else {
                // Large file - use append-only approach by modifying the end
                // Read last few bytes to check format and append
                let mut file = OpenOptions::new().read(true).write(true).open(json_file)?;
                
                // Seek to end and read last few characters
                file.seek(std::io::SeekFrom::End(-10))?;
                let mut buffer = [0; 10];
                file.read(&mut buffer)?;
                
                // Check if file ends with ']' (valid JSON array)
                let content = String::from_utf8_lossy(&buffer);
                if content.trim_end().ends_with(']') {
                    // File is a valid JSON array - replace ']' with ',new_entry]'
                    file.seek(std::io::SeekFrom::End(0))?;
                    let mut pos = file.seek(std::io::SeekFrom::Current(0))?;
                    
                    // Find the last ']'
                    while pos > 0 {
                        pos -= 1;
                        file.seek(std::io::SeekFrom::Start(pos))?;
                        let mut byte = [0; 1];
                        file.read_exact(&mut byte)?;
                        if byte[0] == b']' {
                            // Found the closing bracket - replace with comma and new entry
                            file.seek(std::io::SeekFrom::Start(pos))?;
                            let new_entry_str = format!(",\n{}\n]", serde_json::to_string_pretty(&stats_json)?);
                            file.write_all(new_entry_str.as_bytes())?;
                            file.flush()?;
                            break;
                        }
                    }
                } else {
                    // File format is unknown - rotate and start fresh
                    drop(file); // Close file before rotating
                    rotate_file(json_file)?;
                    let json_data = json!([stats_json]);
                    let mut new_file = File::create(json_file)?;
                    let json_str = serde_json::to_string_pretty(&json_data)?;
                    new_file.write_all(json_str.as_bytes())?;
                    new_file.flush()?;
                }
            }
        }
        
        Ok(())
    }, "JSON export")
}

fn export_to_csv(stats: &PacketStats, csv_file: &str, header_needed: bool) -> std::io::Result<()> {
    retry_operation(|| {
        // Check if file needs rotation before writing
        if should_rotate_file(csv_file) {
            rotate_file(csv_file)?;
        }
        
        // Create the directory if it doesn't exist
        if let Some(parent) = std::path::Path::new(csv_file).parent() {
            std::fs::create_dir_all(parent)?;
        }
        
        let file_exists = std::path::Path::new(csv_file).exists();
        
        // Always include headers if the file doesn't exist or headers are explicitly requested
        let include_headers = !file_exists || header_needed;
        
        let mut wtr = csv::WriterBuilder::new()
            .has_headers(include_headers)
            .from_writer(OpenOptions::new()
                .write(true)
                .create(true)
                .append(file_exists && !header_needed)
                .truncate(!file_exists || header_needed)
                .open(csv_file)?);

        // Convert to CSV export format
        let export_stats = stats.to_csv_export_stats();
        wtr.serialize(export_stats)?;
        wtr.flush()?;
        Ok(())
    }, "CSV export")
}

// Function to save interval statistics (only delta from last export)
fn save_interval_stats(
    current_stats: &PacketStats, 
    last_stats: &PacketStats, 
    latency_stats: &Arc<Mutex<LatencyStats>>,
    json_file: Option<&str>, 
    csv_file: Option<&str>
) -> std::io::Result<()> {
    // Calculate interval delta
    let mut interval_stats = current_stats.clone();
    interval_stats.sent = current_stats.sent.saturating_sub(last_stats.sent);
    interval_stats.received = current_stats.received.saturating_sub(last_stats.received);
    interval_stats.lost = current_stats.lost.saturating_sub(last_stats.lost);
    interval_stats.detected_lost_packets = current_stats.detected_lost_packets.saturating_sub(last_stats.detected_lost_packets);
    
    // Calculate interval latency buckets (delta)
    for (bucket, current_count) in &current_stats.latency_buckets {
        let last_count = last_stats.latency_buckets.get(bucket).unwrap_or(&0);
        let interval_count = current_count.saturating_sub(*last_count);
        interval_stats.latency_buckets.insert(bucket.clone(), interval_count);
    }
    
    // Calculate interval latency statistics
    if let Ok(mut latency_guard) = latency_stats.try_lock() {
        let interval_latency_stats = latency_guard.calculate_interval_stats();
        
        // Update interval_stats with interval-based latency values
        interval_stats.min_latency_ms = interval_latency_stats.min_latency_ms;
        interval_stats.avg_latency_ms = interval_latency_stats.avg_latency_ms;
        interval_stats.max_latency_ms = interval_latency_stats.max_latency_ms;
        interval_stats.p95_latency_ms = interval_latency_stats.p95_latency_ms;
        interval_stats.p99_latency_ms = interval_latency_stats.p99_latency_ms;
        interval_stats.jitter_ms = interval_latency_stats.jitter_ms;
        
        // Clear interval latencies for next period
        latency_guard.clear_interval_latencies();
    }
    
    // Recalculate loss percentage for interval
    let total_interval = interval_stats.received + interval_stats.lost;
    if total_interval > 0 {
        interval_stats.loss_percent = interval_stats.lost as f64 / total_interval as f64 * 100.0;
    } else {
        interval_stats.loss_percent = 0.0;
    }
    
    // Update timestamp to current time
    interval_stats.timestamp = Utc::now();
    
    if let Some(file) = json_file {
        if let Err(e) = export_to_json(&interval_stats, file) {
            eprintln!("Error saving interval JSON stats: {e}");
        }
    }
    
    if let Some(file) = csv_file {
        if let Err(e) = export_to_csv(&interval_stats, file, false) {
            eprintln!("Error saving interval CSV stats: {e}");
        }
    }
    
    Ok(())
}

// Function to save final statistics before exit
fn save_final_stats(stats: &PacketStats, json_file: Option<&str>, csv_file: Option<&str>) -> std::io::Result<()> {
    println!("=== SAVING FINAL STATISTICS BEFORE EXIT ===");
    
    let mut json_success = true;
    let mut csv_success = true;
    
    if let Some(file) = json_file {
        match retry_operation(|| export_to_json(stats, file), "final JSON save") {
            Ok(()) => {
                println!("Final statistics saved to JSON file: {file}");
            },
            Err(e) => {
                eprintln!("CRITICAL: Failed to save final JSON stats after all retries: {e}");
                json_success = false;
                
                // Try to save to backup location
                let backup_file = format!("{file}.emergency_backup");
                if let Ok(backup_json) = serde_json::to_string_pretty(&stats.to_json_export_stats()) {
                    if std::fs::write(&backup_file, backup_json).is_ok() {
                        println!("Emergency backup saved to: {backup_file}");
                    }
                }
            }
        }
    }
    
    if let Some(file) = csv_file {
        match retry_operation(|| export_to_csv(stats, file, !std::path::Path::new(file).exists()), "final CSV save") {
            Ok(()) => {
                println!("Final statistics saved to CSV file: {file}");
            },
            Err(e) => {
                eprintln!("CRITICAL: Failed to save final CSV stats after all retries: {e}");
                csv_success = false;
                
                // Try to save to backup location
                let backup_file = format!("{file}.emergency_backup");
                if let Ok(mut backup_writer) = csv::Writer::from_path(&backup_file) {
                    if backup_writer.serialize(stats.to_csv_export_stats()).is_ok() {
                        println!("Emergency backup saved to: {backup_file}");
                    }
                }
            }
        }
    }
    
    if json_success && csv_success {
        println!("=== ALL FINAL STATISTICS SAVED SUCCESSFULLY ===");
        Ok(())
    } else {
        println!("=== FINAL STATISTICS SAVE COMPLETED WITH SOME ERRORS ===");
        Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Some final statistics could not be saved"
        ))
    }
}

// Track the latest packet activity
#[derive(Clone)]
struct ActivityTracker {
    last_sent: Option<u64>,
    last_received: Option<u64>,
    last_sent_time: Option<DateTime<Utc>>,
    last_received_time: Option<DateTime<Utc>>,
    last_latency_ms: Option<f64>,
}

impl ActivityTracker {
    fn new() -> Self {
        ActivityTracker {
            last_sent: None,
            last_received: None,
            last_sent_time: None,
            last_received_time: None,
            last_latency_ms: None,
        }
    }
    
    fn update_sent(&mut self, seq: u64) {
        self.last_sent = Some(seq);
        self.last_sent_time = Some(Utc::now());
    }
    
    fn update_received(&mut self, seq: u64, latency_ms: Option<f64>) {
        self.last_received = Some(seq);
        self.last_received_time = Some(Utc::now());
        self.last_latency_ms = latency_ms;
    }
}

// Simple animated spinner for activity indication
struct Spinner {
    frames: Vec<char>,
    current: usize,
}

impl Spinner {
    fn new() -> Self {
        Spinner {
            frames: vec!['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'],
            current: 0,
        }
    }
    
    fn next(&mut self) -> char {
        let frame = self.frames[self.current];
        self.current = (self.current + 1) % self.frames.len();
        frame
    }
}

// Send a terminate packet to the remote endpoint
fn send_terminate_packet(socket: &UdpSocket, remote_addr: &SocketAddr) -> std::io::Result<()> {
    let mut packet = Vec::with_capacity(16);
    packet.push(PACKET_TYPE_TERMINATE);
    
    // Try to send the terminate packet multiple times for reliability
    for _ in 0..TERMINATE_PACKET_RETRY_COUNT {
        if let Err(e) = socket.send_to(&packet, remote_addr) {
            eprintln!("Failed to send terminate packet: {e}");
        }
        thread::sleep(Duration::from_millis(TERMINATE_PACKET_RETRY_DELAY_MS));
    }
    Ok(())
}

// Send a time sync packet to initiate clock synchronization
fn send_time_sync_packet(socket: &UdpSocket, remote_addr: &SocketAddr) -> std::io::Result<u64> {
    let mut packet = Vec::with_capacity(16);
    packet.push(PACKET_TYPE_TIME_SYNC);
    
    // Get current timestamp in milliseconds
    let t1 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as u64;
    
    // Include our timestamp (T1)
    packet.extend_from_slice(&t1.to_be_bytes());
    
    if let Err(e) = socket.send_to(&packet, remote_addr) {
        eprintln!("Failed to send time sync packet: {e}");
    }
    
    Ok(t1)
}

// Send time sync response packet
fn send_time_sync_response(socket: &UdpSocket, t1: u64, remote_addr: &SocketAddr) -> std::io::Result<()> {
    let mut packet = Vec::with_capacity(24);
    packet.push(PACKET_TYPE_TIME_SYNC_RESP);
    
    // Include original timestamp (T1)
    packet.extend_from_slice(&t1.to_be_bytes());
    
    // Add received timestamp (T2)
    let t2 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as u64;
    packet.extend_from_slice(&t2.to_be_bytes());
    
    // Add response timestamp (T3)
    let t3 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as u64;
    packet.extend_from_slice(&t3.to_be_bytes());
    
    socket.send_to(&packet, remote_addr)?;
    Ok(())
}

fn main() -> std::io::Result<()> {
    let matches = App::new("UDP Monitor")
        .version("1.5")
        .author("UDP Monitor Tool")
        .about("Sends and monitors UDP packets with accurate latency measurement")
        .arg(Arg::with_name("local")
            .short("l")
            .long("local")
            .value_name("LOCAL_ADDR")
            .help("Local address to bind to")
            .takes_value(true)
            .required(true))
        .arg(Arg::with_name("remote")
            .short("r")
            .long("remote")
            .value_name("REMOTE_ADDR")
            .help("Remote address to send packets to")
            .takes_value(true)
            .required(true))
        .arg(Arg::with_name("interval")
            .short("i")
            .long("interval")
            .value_name("MS")
            .help("Interval between packets in milliseconds")
            .takes_value(true)
            .default_value("1000"))
        .arg(Arg::with_name("count")
            .short("c")
            .long("count")
            .value_name("COUNT")
            .help("Number of packets to send (0 for unlimited)")
            .takes_value(true)
            .default_value("0"))
        .arg(Arg::with_name("json")
            .long("json")
            .value_name("JSON_FILE")
            .help("Export statistics to JSON file")
            .takes_value(true))
        .arg(Arg::with_name("csv")
            .long("csv")
            .value_name("CSV_FILE")
            .help("Export statistics to CSV file")
            .takes_value(true))
        .arg(Arg::with_name("stats_interval")
            .long("stats-interval")
            .value_name("SECONDS")
            .help("Interval for printing statistics in seconds")
            .takes_value(true)
            .default_value("1"))
        .arg(Arg::with_name("sync")
            .long("sync")
            .help("Enable initial synchronization before sending packets")
            .takes_value(false))
        .arg(Arg::with_name("sync_timeout")
            .long("sync-timeout")
            .value_name("SECONDS")
            .help("Timeout for initial synchronization in seconds")
            .takes_value(true)
            .default_value("30"))
        .arg(Arg::with_name("clock_sync_interval")
            .long("clock-sync-interval")
            .value_name("SECONDS")
            .help("Interval for clock synchronization in seconds")
            .takes_value(true)
            .default_value("5"))
        .arg(Arg::with_name("export_interval")
            .long("export-interval")
            .value_name("SECONDS")
            .help("Interval for exporting statistics to files in seconds")
            .takes_value(true)
            .default_value("5"))
        .get_matches();

    let local_addr = matches.value_of("local").unwrap();
    let remote_addr = matches.value_of("remote").unwrap();
    let interval = matches.value_of("interval").unwrap().parse::<u64>()
        .expect("Interval must be a valid number");
    let count = matches.value_of("count").unwrap().parse::<u64>()
        .expect("Count must be a valid number");
    let stats_interval = matches.value_of("stats_interval").unwrap().parse::<u64>()
        .expect("Stats interval must be a valid number");
    let enable_sync = matches.is_present("sync");
    let sync_timeout = matches.value_of("sync_timeout").unwrap().parse::<u64>()
        .expect("Sync timeout must be a valid number");
    let clock_sync_interval = matches.value_of("clock_sync_interval").unwrap().parse::<u64>()
        .expect("Clock sync interval must be a valid number");
    let export_interval = matches.value_of("export_interval").unwrap().parse::<u64>()
        .expect("Export interval must be a valid number");
    
    // Auto-append file extensions if missing
    let json_file = matches.value_of("json").map(|path| {
        if path.ends_with(".json") {
            path.to_string()
        } else {
            format!("{path}.json")
        }
    });
    
    let csv_file = matches.value_of("csv").map(|path| {
        if path.ends_with(".csv") {
            path.to_string()
        } else {
            format!("{path}.csv")
        }
    });

    // Initialize display
    println!("UDP Monitor v1.5 - Timestamp-based Latency Measurement");
    println!("Local: {local_addr}, Remote: {remote_addr}, Interval: {interval}ms");
    println!("Clock synchronization every {clock_sync_interval} seconds");
    println!("Data export every {export_interval} seconds");
    println!("Press Ctrl+C to exit and save statistics");
    println!();
    
    // Clear screen and move cursor to top
    print!("\x1B[2J\x1B[H");
    io::stdout().flush().unwrap();

    let socket = UdpSocket::bind(local_addr)?;
    socket.set_read_timeout(Some(Duration::from_millis(SOCKET_READ_TIMEOUT_MS)))?;
    
    // Initialize CSV file with headers if needed
    if let Some(ref file) = csv_file {
        if !std::path::Path::new(file).exists() {
            // Create a temporary stats object with headers
            let temp_stats = PacketStats::new();
            export_to_csv(&temp_stats, file, true)?;
            println!("Created CSV file with headers: {file}");
        }
    }
    
    // Initialize JSON file if needed
    if let Some(ref file) = json_file {
        if !std::path::Path::new(file).exists() {
            let mut f = File::create(file)?;
            f.write_all(b"[]")?;
            f.flush()?;
            println!("Created empty JSON array file: {file}");
        }
    }
    
    let remote_addr = SocketAddr::from_str(remote_addr).expect("Invalid remote address");
    
    // Create shared data structures
    let stats = Arc::new(Mutex::new(PacketStats::new()));
    let activity = Arc::new(Mutex::new(ActivityTracker::new()));
    let socket = Arc::clone(&Arc::new(socket));
    let latency_stats = Arc::new(Mutex::new(LatencyStats::new()));
    let clock_sync = Arc::new(Mutex::new(ClockSync::new()));
    
    // Map to track pending time sync requests
    let time_sync_requests = Arc::new(Mutex::new(std::collections::HashMap::<u64, Instant>::new()));
    
    // Flag to signal threads to terminate
    let running = Arc::new(AtomicBool::new(true));
    
    // Flag to signal synchronized start
    let synchronized = Arc::new(AtomicBool::new(!enable_sync));
    
    // Setup clean termination signal
    let terminate_socket = Arc::clone(&socket);
    let terminate_remote = remote_addr;
    let r = running.clone();
    let final_stats = stats.clone();
    let final_latency = latency_stats.clone();
    let final_clock = clock_sync.clone();
    let json_path = json_file.clone();
    let csv_path = csv_file.clone();
    
    ctrlc::set_handler(move || {
        println!("\nReceived Ctrl+C signal, initiating graceful shutdown...");
        r.store(false, Ordering::SeqCst);
        
        // Send termination packet to the remote side
        let _ = send_terminate_packet(&terminate_socket, &terminate_remote);
        
        // Give threads reasonable time to notice the termination signal and clean up
        let shutdown_timeout = Duration::from_millis(SHUTDOWN_TIMEOUT_MS);
        let start_time = Instant::now();
        
        println!("Waiting for threads to complete shutdown...");
        
        // Wait for threads with timeout and progress reporting
        while start_time.elapsed() < shutdown_timeout {
            thread::sleep(Duration::from_millis(200));
            
            // Show progress every 2 seconds
            if start_time.elapsed().as_millis() % 2000 < 200 {
                let elapsed = start_time.elapsed().as_secs();
                let total = shutdown_timeout.as_secs();
                println!("Shutdown progress: {}s/{}s", elapsed, total);
            }
        }
        
        println!("Timeout reached, proceeding with final statistics save...");
        
        // Save final statistics with all latency data included
        {
            match final_stats.try_lock() {
                Ok(mut stats_guard) => {
                    // Update with the latest latency stats
                    if let Ok(latency_guard) = final_latency.try_lock() {
                        stats_guard.update_latency_stats(&latency_guard);
                    }
                    
                    // Update with the latest clock sync info
                    if let Ok(clock_guard) = final_clock.try_lock() {
                        stats_guard.update_clock_sync(&clock_guard);
                    }
                    
                    match save_final_stats(
                        &stats_guard, 
                        json_path.as_deref(),
                        csv_path.as_deref()
                    ) {
                        Ok(()) => println!("Final statistics saved successfully"),
                        Err(e) => eprintln!("Final statistics save encountered errors: {e}"),
                    }
                },
                Err(e) => {
                    println!("Warning: Could not acquire stats lock for final save: {e}");
                    println!("Stats may have been saved by another thread or could be incomplete");
                }
            }
        }
        
       // Don't call exit here - let main thread handle cleanup
    }).expect("Error setting Ctrl+C handler");

    // Synchronization thread if enabled
    if enable_sync {
        let sync_socket = Arc::clone(&socket);
        let sync_running = Arc::clone(&running);
        let sync_status = Arc::clone(&synchronized);
        let sync_remote = remote_addr;
        
        let sync_handle = thread::spawn(move || {
            let start_time = Instant::now();
            let mut sync_received = false;
            
            // Display sync status
            let mut stdout = io::stdout();
            print!("\x1B[H"); // Move to top
            print!("⏳ Synchronizing: Waiting for connection...");
            let _ = stdout.flush();
            
            while !sync_received && start_time.elapsed() < Duration::from_secs(sync_timeout) && sync_running.load(Ordering::SeqCst) {
                // Send sync packet
                let mut sync_packet = Vec::with_capacity(16);
                sync_packet.push(PACKET_TYPE_SYNC);
                
                if let Err(e) = sync_socket.send_to(&sync_packet, sync_remote) {
                    eprintln!("Failed to send sync packet: {e}");
                }
                
                // Try to receive sync or sync_ack packet
                let mut buf = [0; 128];
                sync_socket.set_nonblocking(true).expect("Failed to set non-blocking mode");
                
                match sync_socket.recv_from(&mut buf) {
                    Ok((size, _)) if size > 0 => {
                        let packet_type = buf[0];
                        
                        if packet_type == PACKET_TYPE_SYNC {
                            // Received sync, send sync_ack
                            let mut ack_packet = Vec::with_capacity(16);
                            ack_packet.push(PACKET_TYPE_SYNC_ACK);
                            
                            if let Err(e) = sync_socket.send_to(&ack_packet, sync_remote) {
                                eprintln!("Failed to send sync ack packet: {e}");
                            }
                        } else if packet_type == PACKET_TYPE_SYNC_ACK {
                            // Received sync_ack, we're synchronized
                            sync_received = true;
                            break;
                        }
                    },
                    Err(e) => {
                        if e.kind() != std::io::ErrorKind::WouldBlock {
                            eprintln!("Failed to receive sync packet: {e}");
                        }
                    },
                    _ => {}
                }
                
                // Update sync status every second
                print!("\x1B[H"); // Move to top
                print!("⏳ Synchronizing: {:02}s/{:02}s... Waiting for connection", 
                       start_time.elapsed().as_secs(), sync_timeout);
                let _ = stdout.flush();
                
                thread::sleep(Duration::from_millis(500));
            }
            
            // Mark as synchronized or timed out
            if sync_received {
                print!("\x1B[H"); // Move to top
                print!("✓ Connection established! Starting...");
                let _ = stdout.flush();
                sync_status.store(true, Ordering::SeqCst);
                
                // Brief pause before starting
                thread::sleep(Duration::from_secs(1));
            } else if !sync_running.load(Ordering::SeqCst) {
                print!("\x1B[H"); // Move to top
                print!("✗ Synchronization aborted");
                let _ = stdout.flush();
            } else {
                print!("\x1B[H"); // Move to top
                print!("✗ Synchronization timed out - starting anyway");
                let _ = stdout.flush();
                sync_status.store(true, Ordering::SeqCst);
                
                // Brief pause before starting
                thread::sleep(Duration::from_secs(1));
            }
        });
        
        // Allow sync thread to run
        sync_handle.join().unwrap();
    }

    // Clear screen again after sync
    print!("\x1B[2J\x1B[H");
    io::stdout().flush().unwrap();

    // Clock synchronization thread
    let clock_sync_socket = Arc::clone(&socket);
    let clock_sync_running = Arc::clone(&running);
    let clock_sync_remote = remote_addr;
    let clock_sync_data = Arc::clone(&clock_sync);
    let time_sync_reqs = Arc::clone(&time_sync_requests);
    
    let clock_sync_handle = thread::spawn(move || {
        // Start right away with first sync
        let mut next_sync = Instant::now();
        let mut cleanup_counter = 0;
        
        while clock_sync_running.load(Ordering::SeqCst) {
            if Instant::now() >= next_sync {
                // Time to send a clock sync packet
                if let Ok(t1) = send_time_sync_packet(&clock_sync_socket, &clock_sync_remote) {
                    // Store the timestamp in pending requests
                    match time_sync_reqs.try_lock() {
                        Ok(mut requests) => {
                            requests.insert(t1, Instant::now());
                        },
                        Err(_) => {
                            // Continue without storing request if lock fails
                        }
                    }
                    
                    // Update sync counter
                    match clock_sync_data.try_lock() {
                        Ok(mut sync_data) => {
                            sync_data.sync_packets_sent += 1;
                        },
                        Err(_) => {
                            // Continue without updating counter if lock fails
                        }
                    }
                }
                
                // Set next sync time
                next_sync = Instant::now() + Duration::from_secs(clock_sync_interval);
                
                // Increment cleanup counter
                cleanup_counter += 1;
            }
            
            // Cleanup old pending requests more frequently in long-running scenarios
            if cleanup_counter >= 3 { // Clean up every 3 sync cycles
                cleanup_counter = 0;
                
                match time_sync_reqs.try_lock() {
                    Ok(mut requests) => {
                        let before_count = requests.len();
                        requests.retain(|_, time| {
                            time.elapsed() < Duration::from_secs(TIME_SYNC_REQUEST_TIMEOUT_SECS)
                        });
                        let after_count = requests.len();
                        
                        // Log cleanup if we removed items (helps with debugging long runs)
                        if before_count > after_count {
                            println!("Cleaned up {} expired time sync requests ({} -> {})", 
                                   before_count - after_count, before_count, after_count);
                        }
                        
                        // If we have too many pending requests, warn about potential issues
                        if requests.len() > 100 {
                            println!("Warning: Large number of pending time sync requests: {}", requests.len());
                        }
                    },
                    Err(_) => {
                        // Continue without cleanup if lock fails
                    }
                }
            }
            
            // Check running flag more frequently to ensure timely shutdown
            thread::sleep(Duration::from_millis(SOCKET_READ_TIMEOUT_MS));
            
            // Additional check for running flag to ensure prompt shutdown
            if !clock_sync_running.load(Ordering::SeqCst) {
                break;
            }
        }
        
        println!("Clock sync thread shutting down gracefully");
    });

    // Sender thread
    let sender_socket = Arc::clone(&socket);
    let sender_stats = Arc::clone(&stats);
    let sender_activity = Arc::clone(&activity);
    let sender_running = Arc::clone(&running);
    let sender_sync = Arc::clone(&synchronized);
    let sender_remote = remote_addr;
    
    let sender_handle = thread::spawn(move || {
        let mut seq_num: u64 = 0;
        let mut sent_count = 0;

        // Wait for synchronization if needed
        while !sender_sync.load(Ordering::SeqCst) && sender_running.load(Ordering::SeqCst) {
            thread::sleep(Duration::from_millis(100));
        }

        while (count == 0 || sent_count < count) && sender_running.load(Ordering::SeqCst) {
            // Check for termination signal before creating and sending each packet
            if !sender_running.load(Ordering::SeqCst) {
                break;
            }
            
            // Get current timestamp in milliseconds since UNIX epoch
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_millis() as u64;
            
            // Create packet: [type (1 byte)][seq_num (8 bytes)][timestamp (8 bytes)][data]
            let mut packet = Vec::with_capacity(1024);
            packet.push(PACKET_TYPE_DATA); // Packet type: DATA
            packet.extend_from_slice(&seq_num.to_be_bytes());
            packet.extend_from_slice(&timestamp.to_be_bytes());
            
            // Add lorem ipsum data
            let lorem = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.";
            let data_size = 500;
            let mut remaining = data_size;
            
            while remaining > 0 && remaining <= data_size {  // Add bounds check to prevent infinite loop
                let chunk_size = std::cmp::min(lorem.len(), remaining);
                packet.extend_from_slice(&lorem.as_bytes()[0..chunk_size]);
                remaining -= chunk_size;
            }

            match sender_socket.send_to(&packet, sender_remote) {
                Ok(_) => {
                    // Update activity tracker
                    if let Ok(mut activity) = sender_activity.lock() {
                        activity.update_sent(seq_num);
                    }
                    
                    if let Ok(mut stats) = sender_stats.lock() {
                        stats.sent += 1;
                    }
                    sent_count += 1;
                },
                Err(e) => {
                    eprintln!("Failed to send packet: {e}");
                    // Add small delay on send errors to prevent spinning
                    thread::sleep(Duration::from_millis(10));
                }
            }

            seq_num += 1;
            
            // Check if we've reached the packet count limit
            if count > 0 && sent_count >= count {
                println!("\nPacket count limit reached, notifying remote side...");
                sender_running.store(false, Ordering::SeqCst);
                let _ = send_terminate_packet(&sender_socket, &sender_remote);
                break;
            }
            
            // Check if we should exit before sleeping
            if !sender_running.load(Ordering::SeqCst) {
                break;
            }
            
            thread::sleep(Duration::from_millis(interval));
        }
        
    });

    // Receiver thread
    let receiver_socket = Arc::clone(&socket);
    let receiver_stats = Arc::clone(&stats);
    let receiver_activity = Arc::clone(&activity);
    let receiver_running = Arc::clone(&running);
    let receiver_latency = Arc::clone(&latency_stats);
    let receiver_clock_sync = Arc::clone(&clock_sync);
    let receiver_time_reqs = Arc::clone(&time_sync_requests);
    let json_path_recv = json_file.clone();
    let csv_path_recv = csv_file.clone();
    
    let receiver_handle = thread::spawn(move || {
        let mut buf = [0; 2048];

        // Set socket to non-blocking mode
        receiver_socket.set_nonblocking(true).expect("Failed to set non-blocking mode");

        while receiver_running.load(Ordering::SeqCst) {
            match receiver_socket.recv_from(&mut buf) {
                Ok((size, src)) => {
                    if size >= 1 {  // At least 1 byte for packet type
                        let packet_type = buf[0];
                        
                        match packet_type {
                            PACKET_TYPE_DATA if size >= 17 => { // type + seq + timestamp
                                // Extract seq_num and timestamp
                                let mut seq_bytes = [0u8; 8];
                                seq_bytes.copy_from_slice(&buf[1..9]);
                                let seq_num = u64::from_be_bytes(seq_bytes);
                                
                                let mut ts_bytes = [0u8; 8];
                                ts_bytes.copy_from_slice(&buf[9..17]);
                                let remote_send_time = u64::from_be_bytes(ts_bytes);
                                
                                // Get current time
                                let current_time = SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .expect("Time went backwards")
                                    .as_millis() as u64;
                                
                                // Calculate latency based on timestamps
                                let mut latency_ms = None;
                                
                                // Check if we have valid clock synchronization
                                let clock_sync_info = match receiver_clock_sync.lock() {
                                    Ok(clock_guard) => clock_guard.clone(),
                                    Err(_) => ClockSync::new(), // Use default if lock fails
                                };
                                if clock_sync_info.is_valid() {
                                    // Convert remote timestamp to local time using our offset
                                    let adjusted_remote_time = clock_sync_info.remote_to_local(remote_send_time);
                                    
                                    // Calculate one-way latency
                                    if current_time >= adjusted_remote_time {
                                        let one_way_latency = current_time - adjusted_remote_time;
                                        latency_ms = Some(one_way_latency as f64);
                                        
                                        // Update latency statistics with error handling
                                        match receiver_latency.lock() {
                                            Ok(mut latency_stats) => {
                                                latency_stats.add_sample(one_way_latency as f64);
                                            },
                                            Err(_) => {
                                                // Continue without updating latency stats if lock fails
                                            }
                                        }
                                    }
                                }
                                
                                // Update activity tracker with better error handling
                                match receiver_activity.lock() {
                                    Ok(mut activity) => {
                                        activity.update_received(seq_num, latency_ms);
                                    },
                                    Err(_) => {
                                        // Continue without updating activity if lock fails
                                    }
                                }
                                
                                // Process packet for sequence tracking with better error handling
                                match receiver_stats.lock() {
                                    Ok(mut stats) => {
                                        stats.process_sequence(seq_num);
                                        stats.update_loss_stats();
                                    },
                                    Err(_) => {
                                        // Continue processing other packets if stats lock fails
                                    }
                                }
                            },
                            
                            PACKET_TYPE_TIME_SYNC if size >= 9 => {
                                // Extract timestamp (T1)
                                let mut t1_bytes = [0u8; 8];
                                t1_bytes.copy_from_slice(&buf[1..9]);
                                let t1 = u64::from_be_bytes(t1_bytes);
                                
                                // Send time sync response with timestamps T1, T2, T3
                                if let Err(e) = send_time_sync_response(&receiver_socket, t1, &src) {
                                    eprintln!("Failed to send time sync response: {e}");
                                }
                            },
                            
                            PACKET_TYPE_TIME_SYNC_RESP if size >= 25 => {
                                // Extract timestamps (T1, T2, T3)
                                let mut t1_bytes = [0u8; 8];
                                t1_bytes.copy_from_slice(&buf[1..9]);
                                let t1 = u64::from_be_bytes(t1_bytes);
                                
                                let mut t2_bytes = [0u8; 8];
                                t2_bytes.copy_from_slice(&buf[9..17]);
                                let t2 = u64::from_be_bytes(t2_bytes);
                                
                                let mut t3_bytes = [0u8; 8];
                                t3_bytes.copy_from_slice(&buf[17..25]);
                                let t3 = u64::from_be_bytes(t3_bytes);
                                
                                // Get T4 (now)
                                let t4 = SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .expect("Time went backwards")
                                    .as_millis() as u64;
                                
                                // Check if this is a response to our request
                                let mut time_reqs = match receiver_time_reqs.try_lock() {
                                    Ok(reqs) => reqs,
                                    Err(_) => continue, // Skip this response if we can't access requests
                                };
                                if time_reqs.contains_key(&t1) {
                                    // Remove the request from pending
                                    time_reqs.remove(&t1);
                                    drop(time_reqs); // Release lock before processing
                                    
                                    // Process timestamps to calculate clock offset
                                    match receiver_clock_sync.try_lock() {
                                        Ok(mut clock_sync_data) => {
                                            clock_sync_data.process_sync_response(t1, t2, t3, t4);
                                        },
                                        Err(_) => {
                                            // Continue without updating clock sync if lock fails
                                        }
                                    }
                                }
                            },
                            
                            PACKET_TYPE_TERMINATE => {
                                // Received terminate packet, initiate clean shutdown
                                println!("\nReceived termination signal from remote side...");
                                receiver_running.store(false, Ordering::SeqCst);
                                
                                // Save stats before exit with improved error handling
                                {
                                    match receiver_stats.try_lock() {
                                        Ok(mut stats_guard) => {
                                            // Update with latest latency and clock sync data
                                            if let Ok(latency_guard) = receiver_latency.try_lock() {
                                                stats_guard.update_latency_stats(&latency_guard);
                                            }
                                            if let Ok(clock_guard) = receiver_clock_sync.try_lock() {
                                                stats_guard.update_clock_sync(&clock_guard);
                                            }
                                            
                                            match save_final_stats(
                                                &stats_guard, 
                                                json_path_recv.as_deref(),
                                                csv_path_recv.as_deref()
                                            ) {
                                                Ok(()) => println!("Final statistics saved successfully on termination"),
                                                Err(e) => eprintln!("Warning: Final statistics save had errors: {e}"),
                                            }
                                        },
                                        Err(e) => {
                                            eprintln!("Warning: Could not acquire stats lock for final save: {e}");
                                        }
                                    }
                                }
                                
                                println!("Receiver thread shutting down gracefully...");
                                return; // Exit receiver thread gracefully
                            },
                            
                            // Sync packets are handled in the sync thread
                            _ => {}
                        }
                    }
                },
                Err(e) => {
                    if e.kind() != std::io::ErrorKind::WouldBlock && e.kind() != std::io::ErrorKind::TimedOut {
                        eprintln!("Failed to receive: {e}");
                    }
                    // Small sleep to avoid CPU spinning on non-blocking socket
                    thread::sleep(Duration::from_millis(THREAD_SLEEP_MS));
                }
            }
        }
        
        //println!("Receiver thread shutting down gracefully");
    });

    // Statistics reporting and export
    let stats_clone = Arc::clone(&stats);
    let activity_clone = Arc::clone(&activity);
    let stats_running = Arc::clone(&running);
    let json_file_clone = json_file.clone();
    let csv_file_clone = csv_file.clone();
    let latency_data = Arc::clone(&latency_stats);
    let clock_data = Arc::clone(&clock_sync);
    let export_interval_clone = export_interval;
    
    let stats_handle = thread::spawn(move || {
        let mut spinner = Spinner::new();
        let mut activity_update_timer = 0;
        let mut last_activity_data = ActivityTracker::new();
        let mut last_exported_stats = PacketStats::new(); // Track last exported stats for interval calculation
        let start_time = Instant::now();
        let mut last_export_time = Instant::now(); // Track actual time for export interval
        
        while stats_running.load(Ordering::SeqCst) {
            // Get updated stats with timeout to avoid indefinite blocking
            let stats_snapshot = {
                match stats_clone.lock() {
                    Ok(mut stats_guard) => {
                        stats_guard.update_loss_stats();
                        
                        // Update latency and clock sync data for export
                        if let Ok(latency_guard) = latency_data.lock() {
                            stats_guard.update_latency_stats(&latency_guard);
                        }
                        if let Ok(clock_guard) = clock_data.lock() {
                            stats_guard.update_clock_sync(&clock_guard);
                        }
                        
                        stats_guard.clone()
                    },
                    Err(e) => {
                        eprintln!("Error acquiring stats lock: {e}");
                        // Sleep and continue to avoid spinning
                        thread::sleep(Duration::from_secs(1));
                        continue;
                    }
                }
            };
            
            // Get activity status - only update once per second and extract needed data
            activity_update_timer += 1;
            if activity_update_timer >= 1 {
                activity_update_timer = 0;
                match activity_clone.lock() {
                    Ok(activity_guard) => {
                        // Extract only needed fields to avoid full clone
                        last_activity_data.last_sent = activity_guard.last_sent;
                        last_activity_data.last_received = activity_guard.last_received;
                        last_activity_data.last_sent_time = activity_guard.last_sent_time;
                        last_activity_data.last_received_time = activity_guard.last_received_time;
                        last_activity_data.last_latency_ms = activity_guard.last_latency_ms;
                    },
                    Err(_) => {
                        // Keep using last known activity data if lock fails
                    }
                }
            }
            
            // Get latency stats with cached string to avoid frequent formatting
            let latency_stats_str = match latency_data.lock() {
                Ok(latency_guard) => {
                    // Only format stats string if we have new data
                    if latency_guard.samples_count > 0 {
                        latency_guard.get_stats_string()
                    } else {
                        "No latency data yet".to_string()
                    }
                },
                Err(_) => "Latency stats unavailable".to_string(),
            };
            
            // Get clock sync info with error handling
            let clock_sync_info = match clock_data.lock() {
                Ok(clock_guard) => clock_guard.clone(),
                Err(_) => ClockSync::new(),
            };
            
            // Update the display - now in row format with no borders
            {
                let mut stdout = io::stdout();
                
                // Move to top of screen and clear
                print!("\x1B[H\x1B[2J");
                
                // Title and status row
                let timestamp = stats_snapshot.timestamp.format("%H:%M:%S").to_string();
                println!("UDP Monitor Status at {} {}", timestamp, spinner.next());
                println!("{}", "-".repeat(80));
                
                // Clock sync status
                println!("Clock Sync: Offset: {}ms, Quality: {}, Samples: {}/{}", 
                        clock_sync_info.offset_ms,
                        clock_sync_info.get_quality_string(),
                        clock_sync_info.sync_responses_received,
                        clock_sync_info.sync_packets_sent);
                
                // Packets row
                println!("Packets:  Sent: {}  Received: {}  Lost: {} ({:.1}%)", 
                         stats_snapshot.sent, stats_snapshot.received, 
                         stats_snapshot.lost, stats_snapshot.loss_percent);
                
                // Latency stats
                println!("Latency:  {latency_stats_str}");
                
                // Rate and completion row
                let uptime_secs = start_time.elapsed().as_secs_f64().max(1.0);
                let packets_per_sec = stats_snapshot.sent as f64 / uptime_secs;
                
                let progress_info = if count > 0 {
                    let progress = (stats_snapshot.sent as f64 / count as f64) * 100.0;
                    format!("Progress: {progress:.1}% complete")
                } else {
                    "Running continuously".to_string()
                };
                
                println!("Rate:     {packets_per_sec:.1} packets/sec     {progress_info}");
                
                // Calculate delivery rate for progress bar
                let delivery_rate = if stats_snapshot.received + stats_snapshot.lost > 0 {
                    (stats_snapshot.received as f64 / (stats_snapshot.received + stats_snapshot.lost) as f64) * 100.0
                } else {
                    100.0
                };
                
                // Sequence tracking row
                println!("Sequence:  Next Expected: #{}    Delivery Rate: {:.1}%", 
                         stats_snapshot.expected_next_seq, delivery_rate);
                
                println!("{}", "-".repeat(80));
                
                // Activity rows
                if let (Some(seq), Some(time)) = (last_activity_data.last_sent, last_activity_data.last_sent_time) {
                    println!("Last sent:     Packet #{:<6} at {}", seq, time.format("%H:%M:%S.%3f"));
                } else {
                    println!("Last sent:     -");
                }
                
                if let (Some(seq), Some(time), Some(latency)) = 
                   (last_activity_data.last_received, last_activity_data.last_received_time, last_activity_data.last_latency_ms) {
                    println!("Last received: Packet #{:<6} at {} (Latency: {:.1} ms)", 
                             seq, time.format("%H:%M:%S.%3f"), latency);
                } else if let (Some(seq), Some(time)) = (last_activity_data.last_received, last_activity_data.last_received_time) {
                    println!("Last received: Packet #{:<6} at {} (Latency: not available)", 
                             seq, time.format("%H:%M:%S.%3f"));
                } else {
                    println!("Last received: -");
                }
                
                println!("{}", "-".repeat(80));
                
                // Export status
                let export_status = if let (Some(_), Some(_)) = (json_file_clone.as_ref(), csv_file_clone.as_ref()) {
                    "JSON+CSV export enabled"
                } else if json_file_clone.as_ref().is_some() {
                    "JSON export enabled"
                } else if csv_file_clone.as_ref().is_some() {
                    "CSV export enabled"
                } else {
                    "No file export"
                };
                
                println!("Status:   {export_status}    Press Ctrl+C to exit");
                println!("Send interval: {interval} ms  Clock sync interval: {clock_sync_interval} sec  Export interval: {export_interval_clone} sec");
                
                let _ = stdout.flush();
            }
            
            // Export data based on actual time elapsed, not iteration count
            if last_export_time.elapsed() >= Duration::from_secs(export_interval_clone) {
                last_export_time = Instant::now();
                
                // Save interval statistics (delta from last export)
                let _ = save_interval_stats(
                    &stats_snapshot, 
                    &last_exported_stats,
                    &latency_data,
                    json_file_clone.as_deref(),
                    csv_file_clone.as_deref()
                );
                
                // Update last exported stats for next interval calculation
                last_exported_stats = stats_snapshot.clone();
            }
            
            // Sleep for stats_interval
            thread::sleep(Duration::from_secs(stats_interval));
        }
        
       // println!("Stats thread shutting down gracefully");
    });

    // Wait for all threads to complete with proper error handling and timeouts
    println!("Waiting for threads to complete...");
    
    let mut all_joined = true;
    let individual_timeout = Duration::from_millis(SHUTDOWN_TIMEOUT_MS / 4); // Give each thread 1/4 of total timeout
    
    // Join sender thread with timeout using thread::spawn
    println!("Waiting for sender thread...");
    let sender_result = {
        let (tx, rx) = std::sync::mpsc::channel();
        let handle_tx = tx.clone();
        
        std::thread::spawn(move || {
            let result = sender_handle.join();
            let _ = handle_tx.send(result);
        });
        
        match rx.recv_timeout(individual_timeout) {
            Ok(Ok(_)) => {
                println!("Sender thread completed successfully");
                true
            },
            Ok(Err(e)) => {
                eprintln!("Sender thread panicked: {e:?}");
                false
            },
            Err(_) => {
                eprintln!("Sender thread join timed out");
                false
            }
        }
    };
    all_joined &= sender_result;
    
    // Join receiver thread with timeout
    println!("Waiting for receiver thread...");
    let receiver_result = {
        let (tx, rx) = std::sync::mpsc::channel();
        let handle_tx = tx.clone();
        
        std::thread::spawn(move || {
            let result = receiver_handle.join();
            let _ = handle_tx.send(result);
        });
        
        match rx.recv_timeout(individual_timeout) {
            Ok(Ok(_)) => {
                println!("Receiver thread completed successfully");
                true
            },
            Ok(Err(e)) => {
                eprintln!("Receiver thread panicked: {e:?}");
                false
            },
            Err(_) => {
                eprintln!("Receiver thread join timed out");
                false
            }
        }
    };
    all_joined &= receiver_result;
    
    // Join stats thread with timeout
    println!("Waiting for stats thread...");
    let stats_result = {
        let (tx, rx) = std::sync::mpsc::channel();
        let handle_tx = tx.clone();
        
        std::thread::spawn(move || {
            let result = stats_handle.join();
            let _ = handle_tx.send(result);
        });
        
        match rx.recv_timeout(individual_timeout) {
            Ok(Ok(_)) => {
                println!("Stats thread completed successfully");
                true
            },
            Ok(Err(e)) => {
                eprintln!("Stats thread panicked: {e:?}");
                false
            },
            Err(_) => {
                eprintln!("Stats thread join timed out");
                false
            }
        }
    };
    all_joined &= stats_result;
    
    // Join clock sync thread with timeout
    println!("Waiting for clock sync thread...");
    let clock_result = {
        let (tx, rx) = std::sync::mpsc::channel();
        let handle_tx = tx.clone();
        
        std::thread::spawn(move || {
            let result = clock_sync_handle.join();
            let _ = handle_tx.send(result);
        });
        
        match rx.recv_timeout(individual_timeout) {
            Ok(Ok(_)) => {
                println!("Clock sync thread completed successfully");
                true
            },
            Ok(Err(e)) => {
                eprintln!("Clock sync thread panicked: {e:?}");
                false
            },
            Err(_) => {
                eprintln!("Clock sync thread join timed out");
                false
            }
        }
    };
    all_joined &= clock_result;
    
    if all_joined {
        println!("All threads completed successfully");
    } else {
        println!("Some threads had issues or timed out, but shutdown completed");
        
        // Final attempt to save statistics if threads didn't complete properly
        println!("Attempting final statistics save as safety measure...");
        match stats.try_lock() {
            Ok(mut stats_guard) => {
                if let Ok(latency_guard) = latency_stats.try_lock() {
                    stats_guard.update_latency_stats(&latency_guard);
                }
                if let Ok(clock_guard) = clock_sync.try_lock() {
                    stats_guard.update_clock_sync(&clock_guard);
                }
                
                match save_final_stats(&stats_guard, json_file.as_deref(), csv_file.as_deref()) {
                    Ok(()) => println!("Final safety save completed successfully"),
                    Err(e) => eprintln!("Final safety save failed: {e}"),
                }
            },
            Err(_) => {
                println!("Could not acquire final stats lock for safety save");
            }
        }
    }
    
    // Final cleanup - close socket explicitly
    drop(socket);
    
    println!("UDP Monitor shutdown complete. Goodbye!");

    Ok(())
}
