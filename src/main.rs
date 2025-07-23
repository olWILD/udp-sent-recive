use std::env;
use std::fs::OpenOptions;
use std::io::Write;
use std::net::{UdpSocket, SocketAddr};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use rand::Rng;
use clap::{App, Arg};
use serde::{Serialize, Deserialize};
use chrono::{DateTime, Utc};

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
        self.update_avg_rtt();
    }

    fn update_avg_rtt(&mut self) {
        if self.received > 0 {
            self.avg_rtt_ms = Some(self.total_rtt_ms / self.received as f64);
        } else {
            self.avg_rtt_ms = None;
        }
    }

    fn update_loss_stats(&mut self) {
        self.lost = self.sent.saturating_sub(self.received);
        if self.sent > 0 {
            self.loss_percent = self.lost as f64 / self.sent as f64 * 100.0;
        } else {
            self.loss_percent = 0.0;
        }
        self.timestamp = Utc::now();
    }
}

fn export_to_json(stats: &PacketStats, json_file: &str) -> std::io::Result<()> {
    let json = serde_json::to_string_pretty(stats)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .append(true)
        .open(json_file)?;
    
    writeln!(file, "{}", json)?;
    println!("Statistics exported to JSON file: {}", json_file);
    Ok(())
}

fn export_to_csv(stats: &PacketStats, csv_file: &str, header_needed: bool) -> std::io::Result<()> {
    let file_exists = std::path::Path::new(csv_file).exists();
    
    let mut wtr = csv::WriterBuilder::new()
        .has_headers(!file_exists || header_needed)
        .append(file_exists)
        .from_path(csv_file)?;

    wtr.serialize(stats)?;
    wtr.flush()?;
    println!("Statistics exported to CSV file: {}", csv_file);
    Ok(())
}

fn main() -> std::io::Result<()> {
    let matches = App::new("UDP Monitor")
        .version("1.0")
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
    
    let json_file = matches.value_of("json");
    let csv_file = matches.value_of("csv");

    let socket = UdpSocket::bind(local_addr)?;
    println!("Bound to local address: {}", local_addr);
    println!("Remote target: {}", remote_addr);
    println!("Sending interval: {} ms", interval);
    
    if let Some(file) = json_file {
        println!("JSON export enabled: {}", file);
    }
    
    if let Some(file) = csv_file {
        println!("CSV export enabled: {}", file);
        
        // Create CSV file with headers if it doesn't exist
        if !std::path::Path::new(file).exists() {
            export_to_csv(&PacketStats::new(), file, true)?;
        }
    }
    
    let remote_addr = SocketAddr::from_str(remote_addr).expect("Invalid remote address");
    
    // Create shared statistics
    let stats = Arc::new(Mutex::new(PacketStats::new()));
    let socket = Arc::new(socket);

    // Sender thread
    let sender_socket = Arc::clone(&socket);
    let sender_stats = Arc::clone(&stats);
    let sender_handle = thread::spawn(move || {
        let mut rng = rand::thread_rng();
        let mut seq_num = 0;
        let mut sent_count = 0;

        while count == 0 || sent_count < count {
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_nanos();
            
            // Создаем пакет: [seq_num (8 байт)][timestamp (16 байт)][данные]
            let mut packet = Vec::with_capacity(1024);
            packet.extend_from_slice(&seq_num.to_be_bytes());
            packet.extend_from_slice(&timestamp.to_be_bytes());
            
            // Добавляем данные lorem ipsum
            let lorem = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.";
            let data_size = rng.gen_range(50..500);
            let mut remaining = data_size;
            
            while remaining > 0 {
                let chunk_size = std::cmp::min(lorem.len(), remaining);
                packet.extend_from_slice(&lorem.as_bytes()[0..chunk_size]);
                remaining -= chunk_size;
            }

            if let Err(e) = sender_socket.send_to(&packet, &remote_addr) {
                eprintln!("Failed to send packet: {}", e);
            } else {
                println!("Sent packet #{} to {}", seq_num, remote_addr);
                let mut stats = sender_stats.lock().unwrap();
                stats.sent += 1;
                stats.update_loss_stats();
                sent_count += 1;
            }

            seq_num += 1;
            thread::sleep(Duration::from_millis(interval));
        }
    });

    // Receiver thread
    let receiver_socket = Arc::clone(&socket);
    let receiver_stats = Arc::clone(&stats);
    let receiver_handle = thread::spawn(move || {
        let mut buf = [0; 2048];

        loop {
            match receiver_socket.recv_from(&mut buf) {
                Ok((size, src)) => {
                    if size >= 24 {  // Минимальный размер нашего пакета
                        // Извлекаем seq_num и timestamp
                        let mut seq_bytes = [0u8; 8];
                        seq_bytes.copy_from_slice(&buf[0..8]);
                        let seq_num = u64::from_be_bytes(seq_bytes);
                        
                        let mut ts_bytes = [0u8; 16];
                        ts_bytes.copy_from_slice(&buf[8..24]);
                        let send_timestamp = u128::from_be_bytes(ts_bytes);
                        
                        // Вычисляем текущее время в наносекундах
                        let current_time = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .expect("Time went backwards")
                            .as_nanos();
                        
                        // Вычисляем RTT
                        let rtt = Duration::from_nanos((current_time - send_timestamp) as u64);
                        
                        println!("Received packet #{} from {}, size: {} bytes, RTT: {:.2?}", 
                                seq_num, src, size, rtt);
                        
                        let mut stats = receiver_stats.lock().unwrap();
                        stats.received += 1;
                        stats.update_rtt(rtt);
                        stats.update_loss_stats();
                    }
                },
                Err(e) => {
                    eprintln!("Failed to receive: {}", e);
                }
            }
        }
    });

    // Статистический отчет и экспорт
    let stats_clone = Arc::clone(&stats);
    let json_file_clone = json_file.map(|s| s.to_string());
    let csv_file_clone = csv_file.map(|s| s.to_string());
    
    let stats_handle = thread::spawn(move || {
        loop {
            thread::sleep(Duration::from_secs(stats_interval));
            let mut stats_guard = stats_clone.lock().unwrap();
            stats_guard.update_loss_stats();
            let stats_snapshot = stats_guard.clone();
            drop(stats_guard); // Освобождаем блокировку перед операциями ввода-вывода
            
            println!("\n--- UDP Monitor Statistics at {} ---", stats_snapshot.timestamp);
            println!("Packets: Sent = {}, Received = {}, Lost = {} ({:.2}%)",
                     stats_snapshot.sent, stats_snapshot.received, 
                     stats_snapshot.lost, stats_snapshot.loss_percent);
            
            if let Some(min) = stats_snapshot.min_rtt_ms {
                println!("RTT min = {:.2} ms", min);
            }
            if let Some(avg) = stats_snapshot.avg_rtt_ms {
                println!("RTT avg = {:.2} ms", avg);
            }
            if let Some(max) = stats_snapshot.max_rtt_ms {
                println!("RTT max = {:.2} ms", max);
            }
            println!("-----------------------------\n");
            
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
    });

    sender_handle.join().unwrap();
    receiver_handle.join().unwrap();
    stats_handle.join().unwrap();

    Ok(())
}