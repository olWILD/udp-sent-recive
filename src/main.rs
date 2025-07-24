use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::net::{UdpSocket, SocketAddr};
use std::str::FromStr;
use std::sync::{Arc, Mutex, atomic::{AtomicBool, Ordering}};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH, Instant};
use rand::Rng;
use clap::{App, Arg};
use serde::{Serialize, Deserialize};
use serde_json::{Value, json};
use chrono::{DateTime, Utc};

// Packet types
const PACKET_TYPE_SYNC: u8 = 1;
const PACKET_TYPE_SYNC_ACK: u8 = 2;
const PACKET_TYPE_DATA: u8 = 3;
const PACKET_TYPE_TERMINATE: u8 = 4; // New packet type for clean termination

#[derive(Serialize, Deserialize, Clone, Debug)]
struct PacketStats {
    sent: u64,
    received: u64,
    lost: u64,
    min_rtt_ms: Option<f64>,
    max_rtt_ms: Option<f64>,
    avg_rtt_ms: Option<f64>,
    total_rtt_ms: f64,
    timestamp: DateTime<Utc>,
    loss_percent: f64,
    expected_next_seq: u64,
    detected_lost_packets: u64,
    total_rtt_samples: u64, // Track number of RTT samples for better averaging
}

impl PacketStats {
    fn new() -> Self {
        PacketStats {
            sent: 0,
            received: 0,
            lost: 0,
            min_rtt_ms: None,
            max_rtt_ms: None,
            avg_rtt_ms: None,
            total_rtt_ms: 0.0,
            timestamp: Utc::now(),
            loss_percent: 0.0,
            expected_next_seq: 0,
            detected_lost_packets: 0,
            total_rtt_samples: 0,
        }
    }

    fn update_rtt(&mut self, rtt: Duration) {
        let rtt_ms = rtt.as_secs_f64() * 1000.0;
        
        if let Some(min) = self.min_rtt_ms {
            if rtt_ms < min {
                self.min_rtt_ms = Some(rtt_ms);
            }
        } else {
            self.min_rtt_ms = Some(rtt_ms);
        }

        if let Some(max) = self.max_rtt_ms {
            if rtt_ms > max {
                self.max_rtt_ms = Some(rtt_ms);
            }
        } else {
            self.max_rtt_ms = Some(rtt_ms);
        }

        self.total_rtt_ms += rtt_ms;
        self.total_rtt_samples += 1;
        self.update_avg_rtt();
    }

    fn update_avg_rtt(&mut self) {
        if self.total_rtt_samples > 0 {
            self.avg_rtt_ms = Some(self.total_rtt_ms / self.total_rtt_samples as f64);
        } else {
            self.avg_rtt_ms = None;
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
}

fn export_to_json(stats: &PacketStats, json_file: &str) -> std::io::Result<()> {
    // Create the directory if it doesn't exist
    if let Some(parent) = std::path::Path::new(json_file).parent() {
        std::fs::create_dir_all(parent)?;
    }
    
    // Convert stats to JSON
    let stats_json = serde_json::to_value(stats)?;
    
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

    wtr.serialize(stats)?;
    wtr.flush()?;
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
        // Use header_needed=true for final stats to ensure they're present
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
    last_rtt_ms: Option<u64>,
}

impl ActivityTracker {
    fn new() -> Self {
        ActivityTracker {
            last_sent: None,
            last_received: None,
            last_sent_time: None,
            last_received_time: None,
            last_rtt_ms: None,
        }
    }
    
    fn update_sent(&mut self, seq: u64) {
        self.last_sent = Some(seq);
        self.last_sent_time = Some(Utc::now());
    }
    
    fn update_received(&mut self, seq: u64, rtt_ms: u64) {
        self.last_received = Some(seq);
        self.last_received_time = Some(Utc::now());
        self.last_rtt_ms = Some(rtt_ms);
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

// Execute clean shutdown procedure
fn clean_shutdown(stats: &PacketStats, json_file: Option<&str>, csv_file: Option<&str>) -> std::io::Result<()> {
    println!("\nReceived termination signal, preparing to exit...");
    
    // Save final statistics
    save_final_stats(stats, json_file, csv_file)?;
    
    println!("Exiting UDP Monitor. Goodbye!");
    std::process::exit(0);
}

fn main() -> std::io::Result<()> {
    let matches = App::new("UDP Monitor")
        .version("1.2")
        .author("UDP Monitor Tool")
        .about("Sends and monitors UDP packets")
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
            .help("Interval for printing and exporting statistics in seconds")
            .takes_value(true)
            .default_value("1"))
        .arg(Arg::with_name("sync")
            .long("sync")
            .help("Enable synchronization before sending packets")
            .takes_value(false))
        .arg(Arg::with_name("sync_timeout")
            .long("sync-timeout")
            .value_name("SECONDS")
            .help("Timeout for synchronization in seconds")
            .takes_value(true)
            .default_value("30"))
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
    
    let json_file = matches.value_of("json");
    let csv_file = matches.value_of("csv");

    // Initialize display
    println!("UDP Monitor v1.2 - Starting...");
    println!("Local: {}, Remote: {}", local_addr, remote_addr);
    println!("Press Ctrl+C to exit and save statistics");
    println!();
    
    // Clear screen and move cursor to top
    print!("\x1B[2J\x1B[H");
    io::stdout().flush().unwrap();

    let socket = UdpSocket::bind(local_addr)?;
    socket.set_read_timeout(Some(Duration::from_millis(100)))?;
    
    if let Some(file) = json_file {
        // Initialize JSON file with an empty array if it doesn't exist
        if !std::path::Path::new(file).exists() {
            let mut f = File::create(file)?;
            f.write_all(b"[]")?;
            f.flush()?;
        }
    }
    
    if let Some(file) = csv_file {
        // Create CSV file with headers
        if !std::path::Path::new(file).exists() {
            export_to_csv(&PacketStats::new(), file, true)?;
        }
    }
    
    let remote_addr = SocketAddr::from_str(remote_addr).expect("Invalid remote address");
    
    // Create shared statistics and activity tracker
    let stats = Arc::new(Mutex::new(PacketStats::new()));
    let activity = Arc::new(Mutex::new(ActivityTracker::new()));
    let socket = Arc::clone(&Arc::new(socket));
    
    // Flag to signal threads to terminate
    let running = Arc::new(AtomicBool::new(true));
    
    // Flag to signal synchronized start
    let synchronized = Arc::new(AtomicBool::new(!enable_sync));
    
    // Setup clean termination signal
    let terminate_socket = Arc::clone(&socket);
    let terminate_remote = remote_addr.clone();
    let r = running.clone();
    let final_stats = stats.clone();
    let json_path = json_file.map(|s| s.to_string());
    let csv_path = csv_file.map(|s| s.to_string());
    
    ctrlc::set_handler(move || {
        println!("\nReceived Ctrl+C signal, notifying remote side...");
        r.store(false, Ordering::SeqCst);
        
        // Send termination packet to the remote side
        let _ = send_terminate_packet(&terminate_socket, &terminate_remote);
        
        // Give some time for threads to notice the termination signal
        thread::sleep(Duration::from_millis(500));
        
        // Save final statistics
        let stats_guard = final_stats.lock().unwrap();
        let _ = save_final_stats(
            &stats_guard, 
            json_path.as_deref(),
            csv_path.as_deref()
        );
        
        println!("Exiting UDP Monitor. Goodbye!");
        std::process::exit(0);
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
                sync_packet.push(PACKET_TYPE_SYNC); // Packet type: SYNC
                
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
                            ack_packet.push(PACKET_TYPE_SYNC_ACK); // Packet type: SYNC_ACK
                            
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
    });

    // Receiver thread
    let receiver_socket = Arc::clone(&socket);
    let receiver_stats = Arc::clone(&stats);
    let receiver_activity = Arc::clone(&activity);
    let receiver_running = Arc::clone(&running);
    let receiver_remote = remote_addr.clone();
    let json_path_recv = json_file.map(|s| s.to_string());
    let csv_path_recv = csv_file.map(|s| s.to_string());
    
    let receiver_handle = thread::spawn(move || {
        let mut buf = [0; 2048];

        // Set socket to non-blocking mode
        receiver_socket.set_nonblocking(true).expect("Failed to set non-blocking mode");

        while receiver_running.load(Ordering::SeqCst) {
            match receiver_socket.recv_from(&mut buf) {
                Ok((size, _src)) => {
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
                                let send_timestamp = u64::from_be_bytes(ts_bytes);
                                
                                // Calculate current time in milliseconds
                                let current_time = SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .expect("Time went backwards")
                                    .as_millis() as u64;
                                
                                // Improved RTT calculation
                                // On some systems, clock synchronization might be an issue
                                // Use a safer calculation that works across platforms
                                let rtt_ms = if current_time >= send_timestamp {
                                    current_time - send_timestamp
                                } else {
                                    // If timestamps are out of sync or there's system clock difference
                                    // Just use a reasonable default
                                    50 // 50ms minimum RTT - more realistic than 10ms
                                };
                                
                                let rtt = Duration::from_millis(rtt_ms);
                                
                                // Update activity tracker
                                receiver_activity.lock().unwrap().update_received(seq_num, rtt_ms);
                                
                                let mut stats = receiver_stats.lock().unwrap();
                                // Process sequence number to detect lost packets
                                stats.process_sequence(seq_num);
                                stats.update_rtt(rtt);
                                stats.update_loss_stats();
                            },
                            
                            PACKET_TYPE_TERMINATE => {
                                // Received terminate packet, initiate clean shutdown
                                println!("\nReceived termination signal from remote side...");
                                receiver_running.store(false, Ordering::SeqCst);
                                
                                // Save stats and exit
                                let stats_guard = receiver_stats.lock().unwrap();
                                let _ = save_final_stats(
                                    &stats_guard, 
                                    json_path_recv.as_deref(),
                                    csv_path_recv.as_deref()
                                );
                                
                                println!("Exiting UDP Monitor. Goodbye!");
                                std::process::exit(0);
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
    });

    // Statistics reporting and export
    let stats_clone = Arc::clone(&stats);
    let activity_clone = Arc::clone(&activity);
    let stats_running = Arc::clone(&running);
    let json_file_clone = json_file.map(|s| s.to_string());
    let csv_file_clone = csv_file.map(|s| s.to_string());
    
    let stats_handle = thread::spawn(move || {
        let mut spinner = Spinner::new();
        let mut export_timer = 0;
        let mut activity_update_timer = 0;
        let mut last_activity_data = ActivityTracker::new();
        let start_time = Instant::now();
        
        while stats_running.load(Ordering::SeqCst) {
            // Get updated stats
            let stats_snapshot = {
                let mut stats_guard = stats_clone.lock().unwrap();
                stats_guard.update_loss_stats();
                stats_guard.clone()
            };
            
            // Get activity status - only update once per second
            activity_update_timer += 1;
            if activity_update_timer >= 1 {
                activity_update_timer = 0;
                let activity_guard = activity_clone.lock().unwrap();
                last_activity_data = activity_guard.clone();
            }
            
            // Update the display - now in row format with no borders
            {
                let mut stdout = io::stdout();
                
                // Move to top of screen and clear
                print!("\x1B[H\x1B[2J");
                
                // Title and status row
                let timestamp = stats_snapshot.timestamp.format("%H:%M:%S").to_string();
                println!("UDP Monitor Status at {} {}", timestamp, spinner.next());
                println!("{}", "-".repeat(80));
                
                // Packets row
                println!("Packets:  Sent: {}  Received: {}  Lost: {} ({}%)", 
                         stats_snapshot.sent, stats_snapshot.received, 
                         stats_snapshot.lost, format!("{:.1}", stats_snapshot.loss_percent));
                
                // RTT row
                let min_rtt = stats_snapshot.min_rtt_ms.map_or("N/A".to_string(), |v| format!("{:.1} ms", v));
                let avg_rtt = stats_snapshot.avg_rtt_ms.map_or("N/A".to_string(), |v| format!("{:.1} ms", v));
                let max_rtt = stats_snapshot.max_rtt_ms.map_or("N/A".to_string(), |v| format!("{:.1} ms", v));
                
                println!("RTT:      Min: {}  Avg: {}  Max: {}", min_rtt, avg_rtt, max_rtt);
                
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
                
                if let (Some(seq), Some(time), Some(rtt)) = (last_activity_data.last_received, last_activity_data.last_received_time, last_activity_data.last_rtt_ms) {
                    println!("Last received: Packet #{:<6} at {} (RTT: {} ms)", 
                             seq, time.format("%H:%M:%S.%3f"), rtt);
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
                
                let _ = stdout.flush();
            }
            
            // Export data every 5 seconds to avoid excessive file writes
            export_timer += 1;
            if export_timer >= 5 {
                export_timer = 0;
                
                // Export to JSON if enabled
                if let Some(ref file) = json_file_clone {
                    if let Err(e) = export_to_json(&stats_snapshot, file) {
                        eprintln!("Failed to export to JSON: {}", e);
                    }
                }
                
                // Export to CSV if enabled
                if let Some(ref file) = csv_file_clone {
                    if let Err(e) = export_to_csv(&stats_snapshot, file, false) {
                        eprintln!("Failed to export to CSV: {}", e);
                    }
                }
            }
            
            // Sleep for stats_interval
            thread::sleep(Duration::from_secs(stats_interval));
        }
    });

    // Wait for all threads to complete
    sender_handle.join().unwrap();
    receiver_handle.join().unwrap();
    stats_handle.join().unwrap();

    Ok(())
}