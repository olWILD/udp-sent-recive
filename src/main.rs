use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::net::{UdpSocket, SocketAddr};
use std::str::FromStr;
use std::sync::{Arc, Mutex, atomic::{AtomicBool, Ordering}};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH, Instant};
use std::collections::{VecDeque, HashMap};
use rand::Rng;
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
            recent_offsets: VecDeque::with_capacity(10),
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
        
        // Add to recent offsets and keep last 10
        self.recent_offsets.push_back(offset);
        if self.recent_offsets.len() > 10 {
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
        self.last_update.elapsed() < Duration::from_secs(30)
    }
    
    // Check if sync quality is good (low stddev)
    fn is_reliable(&self) -> bool {
        self.sync_responses_received >= 5 && self.offset_stddev < 50.0
    }
    
    // Get synchronization quality as a string
    fn get_quality_string(&self) -> String {
        if !self.is_valid() {
            "Stale".to_string()
        } else if !self.is_reliable() {
            "Poor".to_string()
        } else if self.offset_stddev < 10.0 {
            "Excellent".to_string()
        } else if self.offset_stddev < 25.0 {
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
            recent_latencies: VecDeque::with_capacity(1000),
            p95_latency_ms: None,
            p99_latency_ms: None,
            latency_buckets: buckets,
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
                self.jitter_ms = Some(0.8 * jitter + 0.2 * current_jitter);
            } else {
                self.jitter_ms = Some(current_jitter);
            }
        }
        self.prev_latency_ms = Some(latency_ms);
        
        // Store recent latencies for percentiles
        self.recent_latencies.push_back(latency_ms);
        if self.recent_latencies.len() > 1000 {
            self.recent_latencies.pop_front();
        }
        
        // Update latency buckets
        let bucket = match latency_ms as u64 {
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
        };
        *self.latency_buckets.entry(bucket.to_string()).or_insert(0) += 1;
        
        // Update percentiles
        self.p95_latency_ms = self.percentile(95.0);
        self.p99_latency_ms = self.percentile(99.0);
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
        let min = self.min_latency_ms.map_or("N/A".to_string(), |v| format!("{:.1}", v));
        let avg = self.avg_latency_ms.map_or("N/A".to_string(), |v| format!("{:.1}", v));
        let max = self.max_latency_ms.map_or("N/A".to_string(), |v| format!("{:.1}", v));
        let p95 = self.p95_latency_ms.map_or("N/A".to_string(), |v| format!("{:.1}", v));
        let p99 = self.p99_latency_ms.map_or("N/A".to_string(), |v| format!("{:.1}", v));
        let jitter = self.jitter_ms.map_or("N/A".to_string(), |v| format!("{:.1}", v));
        
        format!("Min: {} ms  Avg: {} ms  Max: {} ms  P95: {} ms  P99: {} ms  Jitter: {} ms",
                min, avg, max, p95, p99, jitter)
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

// Структура для экспорта в CSV (с отдельными полями для каждого bucket)
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

// Структура для экспорта в JSON (с HashMap для buckets)
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

// Функции для округления при сериализации
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

impl PacketStats {
    fn new() -> Self {
        // Initialize latency buckets
        let buckets = [
            "0-50ms", "50-100ms", "100-150ms", "150-200ms", "200-250ms",
            "250-300ms", "300-350ms", "350-400ms", "400-450ms", "450-500ms",
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
    
    // Конвертация в JsonExportStats для экспорта в JSON
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
    
    // Конвертация в CsvExportStats для экспорта в CSV
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
    // Create the directory if it doesn't exist
    if let Some(parent) = std::path::Path::new(json_file).parent() {
        std::fs::create_dir_all(parent)?;
    }
    
    // Convert stats to JSON export format
    let export_stats = stats.to_json_export_stats();
    let stats_json = serde_json::to_value(export_stats)?;
    
    // Read existing JSON array or create a new one
    let json_data = if std::path::Path::new(json_file).exists() {
        match fs::read_to_string(json_file) {
            Ok(content) if !content.trim().is_empty() => {
                match serde_json::from_str::<Value>(&content) {
                    Ok(Value::Array(mut arr)) => {
                        // Add new stats to existing array
                        arr.push(stats_json);
                        Value::Array(arr)
                    },
                    _ => {
                        // If not a valid array, start a new one
                        json!([stats_json])
                    }
                }
            },
            _ => {
                // Empty or invalid file, start fresh
                json!([stats_json])
            }
        }
    } else {
        // File doesn't exist, create new array
        json!([stats_json])
    };
    
    // Write the updated JSON array to a temporary file
    let temp_file = format!("{}.tmp", json_file);
    {
        let mut file = File::create(&temp_file)?;
        let json_str = serde_json::to_string_pretty(&json_data)?;
        file.write_all(json_str.as_bytes())?;
        file.flush()?;
    }
    
    // Rename the temp file to the target file (atomic operation)
    fs::rename(temp_file, json_file)?;
    
    Ok(())
}

fn export_to_csv(stats: &PacketStats, csv_file: &str, header_needed: bool) -> std::io::Result<()> {
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
}

// Function to save interval statistics (only delta from last export)
fn save_interval_stats(current_stats: &PacketStats, last_stats: &PacketStats, json_file: Option<&str>, csv_file: Option<&str>) -> std::io::Result<()> {
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
            eprintln!("Error saving interval JSON stats: {}", e);
        }
    }
    
    if let Some(file) = csv_file {
        if let Err(e) = export_to_csv(&interval_stats, file, false) {
            eprintln!("Error saving interval CSV stats: {}", e);
        }
    }
    
    Ok(())
}

// Function to save final statistics before exit
fn save_final_stats(stats: &PacketStats, json_file: Option<&str>, csv_file: Option<&str>) -> std::io::Result<()> {
    println!("=== SAVING FINAL STATISTICS BEFORE EXIT ===");
    
    if let Some(file) = json_file {
        if let Err(e) = export_to_json(stats, file) {
            eprintln!("Error saving final JSON stats: {}", e);
        } else {
            println!("Final statistics saved to JSON file: {}", file);
        }
    }
    
    if let Some(file) = csv_file {
        // Use header_needed=true to ensure headers are present in a new file
        if let Err(e) = export_to_csv(stats, file, !std::path::Path::new(file).exists()) {
            eprintln!("Error saving final CSV stats: {}", e);
        } else {
            println!("Final statistics saved to CSV file: {}", file);
        }
    }
    
    println!("=== FINAL STATISTICS SAVED ===");
    Ok(())
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
    for _ in 0..5 {
        if let Err(e) = socket.send_to(&packet, remote_addr) {
            eprintln!("Failed to send terminate packet: {}", e);
        }
        thread::sleep(Duration::from_millis(100));
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
        eprintln!("Failed to send time sync packet: {}", e);
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
            format!("{}.json", path)
        }
    });
    
    let csv_file = matches.value_of("csv").map(|path| {
        if path.ends_with(".csv") {
            path.to_string()
        } else {
            format!("{}.csv", path)
        }
    });

    // Initialize display
    println!("UDP Monitor v1.5 - Timestamp-based Latency Measurement");
    println!("Local: {}, Remote: {}, Interval: {}ms", local_addr, remote_addr, interval);
    println!("Clock synchronization every {} seconds", clock_sync_interval);
    println!("Data export every {} seconds", export_interval);
    println!("Press Ctrl+C to exit and save statistics");
    println!();
    
    // Clear screen and move cursor to top
    print!("\x1B[2J\x1B[H");
    io::stdout().flush().unwrap();

    let socket = UdpSocket::bind(local_addr)?;
    socket.set_read_timeout(Some(Duration::from_millis(100)))?;
    
    // Initialize CSV file with headers if needed
    if let Some(ref file) = csv_file {
        if !std::path::Path::new(file).exists() {
            // Create a temporary stats object with headers
            let temp_stats = PacketStats::new();
            export_to_csv(&temp_stats, file, true)?;
            println!("Created CSV file with headers: {}", file);
        }
    }
    
    // Initialize JSON file if needed
    if let Some(ref file) = json_file {
        if !std::path::Path::new(file).exists() {
            let mut f = File::create(file)?;
            f.write_all(b"[]")?;
            f.flush()?;
            println!("Created empty JSON array file: {}", file);
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
    let terminate_remote = remote_addr.clone();
    let r = running.clone();
    let final_stats = stats.clone();
    let final_latency = latency_stats.clone();
    let final_clock = clock_sync.clone();
    let json_path = json_file.clone();
    let csv_path = csv_file.clone();
    
    ctrlc::set_handler(move || {
        println!("\nReceived Ctrl+C signal, initiating shutdown...");
        r.store(false, Ordering::SeqCst);
        
        // Send termination packet to the remote side
        let _ = send_terminate_packet(&terminate_socket, &terminate_remote);
        
        // Give some time for threads to notice the termination signal
        thread::sleep(Duration::from_millis(1000));
        
        // Save final statistics with all latency data included
        {
            if let Ok(mut stats_guard) = final_stats.try_lock() {
                // Update with the latest latency stats
                if let Ok(latency_guard) = final_latency.try_lock() {
                    stats_guard.update_latency_stats(&latency_guard);
                }
                
                // Update with the latest clock sync info
                if let Ok(clock_guard) = final_clock.try_lock() {
                    stats_guard.update_clock_sync(&clock_guard);
                }
                
                let _ = save_final_stats(
                    &stats_guard, 
                    json_path.as_deref(),
                    csv_path.as_deref()
                );
            } else {
                println!("Warning: Could not acquire lock for final stats save");
            }
        }
        
       // println!("Shutdown signal sent to all threads...");
        // Don't call exit here - let main thread handle cleanup
    }).expect("Error setting Ctrl+C handler");

    // Synchronization thread if enabled
    if enable_sync {
        let sync_socket = Arc::clone(&socket);
        let sync_running = Arc::clone(&running);
        let sync_status = Arc::clone(&synchronized);
        let sync_remote = remote_addr.clone();
        
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
                
                if let Err(e) = sync_socket.send_to(&sync_packet, &sync_remote) {
                    eprintln!("Failed to send sync packet: {}", e);
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
                            
                            if let Err(e) = sync_socket.send_to(&ack_packet, &sync_remote) {
                                eprintln!("Failed to send sync ack packet: {}", e);
                            }
                        } else if packet_type == PACKET_TYPE_SYNC_ACK {
                            // Received sync_ack, we're synchronized
                            sync_received = true;
                            break;
                        }
                    },
                    Err(e) => {
                        if e.kind() != std::io::ErrorKind::WouldBlock {
                            eprintln!("Failed to receive sync packet: {}", e);
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
    let clock_sync_remote = remote_addr.clone();
    let clock_sync_data = Arc::clone(&clock_sync);
    let time_sync_reqs = Arc::clone(&time_sync_requests);
    
    let clock_sync_handle = thread::spawn(move || {
        // Start right away with first sync
        let mut next_sync = Instant::now();
        
        while clock_sync_running.load(Ordering::SeqCst) {
            if Instant::now() >= next_sync {
                // Time to send a clock sync packet
                if let Ok(t1) = send_time_sync_packet(&clock_sync_socket, &clock_sync_remote) {
                    // Store the timestamp in pending requests
                    if let Ok(mut requests) = time_sync_reqs.try_lock() {
                        requests.insert(t1, Instant::now());
                    }
                    
                    // Update sync counter
                    if let Ok(mut sync_data) = clock_sync_data.try_lock() {
                        sync_data.sync_packets_sent += 1;
                    }
                }
                
                // Set next sync time
                next_sync = Instant::now() + Duration::from_secs(clock_sync_interval);
                
                // Cleanup old pending requests (older than 10 seconds)
                if let Ok(mut requests) = time_sync_reqs.try_lock() {
                    requests.retain(|_, time| {
                        time.elapsed() < Duration::from_secs(10)
                    });
                }
            }
            
            // Check running flag more frequently and don't spin the CPU
            thread::sleep(Duration::from_millis(100));
        }
        
        //println!("Clock sync thread shutting down gracefully");
    });

    // Sender thread
    let sender_socket = Arc::clone(&socket);
    let sender_stats = Arc::clone(&stats);
    let sender_activity = Arc::clone(&activity);
    let sender_running = Arc::clone(&running);
    let sender_sync = Arc::clone(&synchronized);
    let sender_remote = remote_addr.clone();
    
    let sender_handle = thread::spawn(move || {
        let mut rng = rand::thread_rng();
        let mut seq_num: u64 = 0;
        let mut sent_count = 0;

        // Wait for synchronization if needed
        while !sender_sync.load(Ordering::SeqCst) && sender_running.load(Ordering::SeqCst) {
            thread::sleep(Duration::from_millis(100));
        }

        while (count == 0 || sent_count < count) && sender_running.load(Ordering::SeqCst) {
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
            let lorem = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.";
            let data_size = rng.gen_range(50..500);
            let mut remaining = data_size;
            
            while remaining > 0 {
                let chunk_size = std::cmp::min(lorem.len(), remaining);
                packet.extend_from_slice(&lorem.as_bytes()[0..chunk_size]);
                remaining -= chunk_size;
            }

            if let Err(e) = sender_socket.send_to(&packet, &sender_remote) {
                eprintln!("Failed to send packet: {}", e);
            } else {
                // Update activity tracker
                sender_activity.lock().unwrap().update_sent(seq_num);
                
                let mut stats = sender_stats.lock().unwrap();
                stats.sent += 1;
                sent_count += 1;
            }

            seq_num += 1;
            
            // Check if we've reached the packet count limit
            if count > 0 && sent_count >= count {
                println!("\nPacket count limit reached, notifying remote side...");
                sender_running.store(false, Ordering::SeqCst);
                send_terminate_packet(&sender_socket, &sender_remote).unwrap();
                break;
            }
            
            // Check if we should exit before sleeping
            if !sender_running.load(Ordering::SeqCst) {
                break;
            }
            
            thread::sleep(Duration::from_millis(interval));
        }
        
        //println!("Sender thread shutting down gracefully");
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
                                let clock_sync_info = receiver_clock_sync.lock().unwrap();
                                if clock_sync_info.is_valid() {
                                    // Convert remote timestamp to local time using our offset
                                    let adjusted_remote_time = clock_sync_info.remote_to_local(remote_send_time);
                                    
                                    // Calculate one-way latency
                                    if current_time >= adjusted_remote_time {
                                        let one_way_latency = current_time - adjusted_remote_time;
                                        latency_ms = Some(one_way_latency as f64);
                                        
                                        // Update latency statistics
                                        receiver_latency.lock().unwrap().add_sample(one_way_latency as f64);
                                    }
                                }
                                
                                // Update activity tracker
                                receiver_activity.lock().unwrap().update_received(seq_num, latency_ms);
                                
                                // Process packet for sequence tracking
                                let mut stats = receiver_stats.lock().unwrap();
                                stats.process_sequence(seq_num);
                                stats.update_loss_stats();
                            },
                            
                            PACKET_TYPE_TIME_SYNC if size >= 9 => {
                                // Extract timestamp (T1)
                                let mut t1_bytes = [0u8; 8];
                                t1_bytes.copy_from_slice(&buf[1..9]);
                                let t1 = u64::from_be_bytes(t1_bytes);
                                
                                // Send time sync response with timestamps T1, T2, T3
                                if let Err(e) = send_time_sync_response(&receiver_socket, t1, &src) {
                                    eprintln!("Failed to send time sync response: {}", e);
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
                                let mut time_reqs = receiver_time_reqs.lock().unwrap();
                                if time_reqs.contains_key(&t1) {
                                    // Remove the request from pending
                                    time_reqs.remove(&t1);
                                    
                                    // Process timestamps to calculate clock offset
                                    let mut clock_sync_data = receiver_clock_sync.lock().unwrap();
                                    clock_sync_data.process_sync_response(t1, t2, t3, t4);
                                }
                            },
                            
                            PACKET_TYPE_TERMINATE => {
                                // Received terminate packet, initiate clean shutdown
                                println!("\nReceived termination signal from remote side...");
                                receiver_running.store(false, Ordering::SeqCst);
                                
                                // Save stats before exit
                                {
                                    if let Ok(mut stats_guard) = receiver_stats.try_lock() {
                                        // Update with latest latency and clock sync data
                                        if let Ok(latency_guard) = receiver_latency.try_lock() {
                                            stats_guard.update_latency_stats(&latency_guard);
                                        }
                                        if let Ok(clock_guard) = receiver_clock_sync.try_lock() {
                                            stats_guard.update_clock_sync(&clock_guard);
                                        }
                                        
                                        let _ = save_final_stats(
                                            &stats_guard, 
                                            json_path_recv.as_deref(),
                                            csv_path_recv.as_deref()
                                        );
                                    }
                                }
                                
                                //println!("Receiver thread shutting down gracefully...");
                                return; // Exit receiver thread gracefully
                            },
                            
                            // Sync packets are handled in the sync thread
                            _ => {}
                        }
                    }
                },
                Err(e) => {
                    if e.kind() != std::io::ErrorKind::WouldBlock && e.kind() != std::io::ErrorKind::TimedOut {
                        eprintln!("Failed to receive: {}", e);
                    }
                    // Small sleep to avoid CPU spinning on non-blocking socket
                    thread::sleep(Duration::from_millis(10));
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
            // Get updated stats
            let stats_snapshot = {
                if let Ok(mut stats_guard) = stats_clone.try_lock() {
                    stats_guard.update_loss_stats();
                    
                    // Update latency and clock sync data for export
                    if let Ok(latency_guard) = latency_data.try_lock() {
                        stats_guard.update_latency_stats(&latency_guard);
                    }
                    if let Ok(clock_guard) = clock_data.try_lock() {
                        stats_guard.update_clock_sync(&clock_guard);
                    }
                    
                    stats_guard.clone()
                } else {
                    // If we can't get lock, skip this iteration
                    thread::sleep(Duration::from_millis(100));
                    continue;
                }
            };
            
            // Get activity status - only update once per second
            activity_update_timer += 1;
            if activity_update_timer >= 1 {
                activity_update_timer = 0;
                if let Ok(activity_guard) = activity_clone.try_lock() {
                    last_activity_data = activity_guard.clone();
                }
            }
            
            // Get latency stats
            let latency_stats_str = if let Ok(latency_guard) = latency_data.try_lock() {
                latency_guard.get_stats_string()
            } else {
                "Stats unavailable".to_string()
            };
            
            // Get clock sync info
            let clock_sync_info = if let Ok(clock_guard) = clock_data.try_lock() {
                clock_guard.clone()
            } else {
                ClockSync::new()
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
                println!("Packets:  Sent: {}  Received: {}  Lost: {} ({}%)", 
                         stats_snapshot.sent, stats_snapshot.received, 
                         stats_snapshot.lost, format!("{:.1}", stats_snapshot.loss_percent));
                
                // Latency stats
                println!("Latency:  {}", latency_stats_str);
                
                // Rate and completion row
                let uptime_secs = start_time.elapsed().as_secs_f64().max(1.0);
                let packets_per_sec = stats_snapshot.sent as f64 / uptime_secs;
                
                let progress_info = if count > 0 {
                    let progress = (stats_snapshot.sent as f64 / count as f64) * 100.0;
                    format!("Progress: {:.1}% complete", progress)
                } else {
                    "Running continuously".to_string()
                };
                
                println!("Rate:     {:.1} packets/sec     {}", packets_per_sec, progress_info);
                
                // Calculate delivery rate for progress bar
                let delivery_rate = if stats_snapshot.received + stats_snapshot.lost > 0 {
                    (stats_snapshot.received as f64 / (stats_snapshot.received + stats_snapshot.lost) as f64) * 100.0
                } else {
                    100.0
                };
                
                // Sequence tracking row
                println!("Sequence:  Next Expected: #{}    Delivery Rate: {}", 
                         stats_snapshot.expected_next_seq,
                         format!("{:.1}%", delivery_rate));
                
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
                } else if let Some(_) = json_file_clone.as_ref() {
                    "JSON export enabled"
                } else if let Some(_) = csv_file_clone.as_ref() {
                    "CSV export enabled"
                } else {
                    "No file export"
                };
                
                println!("Status:   {}    Press Ctrl+C to exit", export_status);
                println!("Send interval: {} ms  Clock sync interval: {} sec  Export interval: {} sec", 
                        interval, clock_sync_interval, export_interval_clone);
                
                let _ = stdout.flush();
            }
            
            // Export data based on actual time elapsed, not iteration count
            if last_export_time.elapsed() >= Duration::from_secs(export_interval_clone) {
                last_export_time = Instant::now();
                
                // Save interval statistics (delta from last export)
                let _ = save_interval_stats(
                    &stats_snapshot, 
                    &last_exported_stats,
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

    // Wait for all threads to complete with timeout
    println!("Waiting for threads to complete...");
    
    let mut all_joined = true;
    
    // Try to join sender thread
    match sender_handle.join() {
        Ok(_) => println!(),
        Err(e) => {
            eprintln!("Sender thread panicked: {:?}", e);
            all_joined = false;
        }
    }
    
    // Try to join receiver thread
    match receiver_handle.join() {
        Ok(_) => println!(),
        Err(e) => {
            eprintln!("Receiver thread panicked: {:?}", e);
            all_joined = false;
        }
    }
    
    // Try to join stats thread
    match stats_handle.join() {
        Ok(_) => println!(),
        Err(e) => {
            eprintln!("Stats thread panicked: {:?}", e);
            all_joined = false;
        }
    }
    
    // Try to join clock sync thread
    match clock_sync_handle.join() {
        Ok(_) => println!(),
        Err(e) => {
            eprintln!("Clock sync thread panicked: {:?}", e);
            all_joined = false;
        }
    }
    
    if all_joined {
        println!("All threads completed successfully");
    } else {
        println!("Some threads had issues, but shutdown completed");
    }
    
    // Final cleanup - close socket explicitly
    drop(socket);
    
    println!("UDP Monitor shutdown complete. Goodbye!");

    Ok(())
}
