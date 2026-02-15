use std::fs::File;
use std::io::{BufWriter, Write};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: generate-logs <size_mb> <output_file>");
        eprintln!("Example: generate-logs 1000 /tmp/test_1gb.log");
        std::process::exit(1);
    }

    let size_mb: u64 = args[1].parse().unwrap_or_else(|_| {
        eprintln!("Invalid size: '{}'", args[1]);
        std::process::exit(1);
    });
    let output_path = &args[2];
    let target_bytes = size_mb * 1024 * 1024;

    println!("Generating {} MB log file: {}", size_mb, output_path);

    let file = File::create(output_path).unwrap_or_else(|e| {
        eprintln!("Error creating '{}': {}", output_path, e);
        std::process::exit(1);
    });

    let mut writer = BufWriter::with_capacity(8 * 1024 * 1024, file);

    let levels = ["DEBUG", "INFO", "WARN", "ERROR", "FATAL"];
    let components = [
        "api-server",
        "auth-service",
        "database-pool",
        "cache-service",
        "payment-processor",
        "user-service",
        "notification-engine",
        "search-indexer",
        "load-balancer",
        "rate-limiter",
    ];
    let messages: Vec<Vec<(&str, &str)>> = vec![
        vec![
            ("hit_ratio=0.85", "evictions=1024"),
            ("cache_size=4096", "memory_mb=256"),
            ("query_plan=sequential", "index_used=false"),
            ("gc_pause_ms=12", "heap_mb=512"),
            ("pool_size=50", "active=23"),
        ],
        vec![
            ("request_id=abc123", "latency_ms=42 user_id=1001 status=200"),
            ("request_id=def456", "latency_ms=15 user_id=2002 status=200"),
            (
                "request_id=ghi789",
                "latency_ms=128 user_id=3003 status=201",
            ),
            ("session_created", "user_id=4004 ip=10.0.0.1"),
            ("batch_processed", "items=500 duration_ms=340"),
        ],
        vec![
            ("auth_failed", "user=john ip=192.168.1.1"),
            ("rate_limited", "client=api_key_42 requests=1001 limit=1000"),
            ("slow_query", "duration_ms=2500 table=orders"),
            ("connection_pool_low", "available=2 max=50"),
            (
                "certificate_expiring",
                "days_left=14 domain=api.example.com",
            ),
        ],
        vec![
            ("connection_timeout", "retries=3 queue_size=512"),
            ("disk_full", "partition=/data usage=99.2%"),
            ("replication_lag", "lag_seconds=45 primary=db-01"),
            ("oom_kill", "process=worker-7 memory_mb=8192"),
            ("ssl_handshake_failed", "peer=upstream-3 error=cert_expired"),
        ],
        vec![
            ("insufficient_funds", "amount=999.99 account=user123"),
            (
                "data_corruption",
                "table=transactions checksum_mismatch=true",
            ),
            ("split_brain", "nodes=3 quorum=false"),
            ("config_invalid", "key=max_connections value=-1"),
            ("panic", "thread=main message=index_out_of_bounds"),
        ],
    ];

    let mut bytes_written: u64 = 0;
    let mut line_count: u64 = 0;
    let mut rng_state: u64 = 0xDEAD_BEEF_CAFE_BABEu64;

    let base_year = 2025;
    let base_month = 2;
    let base_day = 12;
    let mut hour: u32 = 0;
    let mut minute: u32 = 0;
    let mut second: u32 = 0;

    while bytes_written < target_bytes {
        rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let rng = rng_state >> 32;

        let level_idx = match rng % 100 {
            0..=19 => 0,
            20..=69 => 1,
            70..=84 => 2,
            85..=94 => 3,
            _ => 4,
        };

        let comp_idx = ((rng >> 8) % components.len() as u64) as usize;
        let msg_idx = ((rng >> 16) % messages[level_idx].len() as u64) as usize;
        let (msg1, msg2) = messages[level_idx][msg_idx];

        if let Err(e) = writeln!(
            writer,
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z {} {} {} {}",
            base_year,
            base_month,
            base_day,
            hour,
            minute,
            second,
            levels[level_idx],
            components[comp_idx],
            msg1,
            msg2
        ) {
            eprintln!("Error writing to file: {}", e);
            std::process::exit(1);
        }

        bytes_written += 80;
        line_count += 1;

        second += 1;
        if second >= 60 {
            second = 0;
            minute += 1;
            if minute >= 60 {
                minute = 0;
                hour += 1;
                if hour >= 24 {
                    hour = 0;
                }
            }
        }
    }

    if let Err(e) = writer.flush() {
        eprintln!("Error flushing file: {}", e);
        std::process::exit(1);
    }

    println!(
        "Generated {} lines (~{:.2} MB) to {}",
        line_count,
        bytes_written as f64 / (1024.0 * 1024.0),
        output_path
    );
}
