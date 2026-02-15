mod data;
mod orchestrator;
mod parser;
mod simd_scan;

use data::ParseStats;
use memmap2::Mmap;
use std::borrow::Cow;
use std::fs::File;
use std::time::Instant;

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        eprintln!("╔══════════════════════════════════════════════╗");
        eprintln!("         PANDORA'S LOGS — SIMD Parser          ");
        eprintln!("╠══════════════════════════════════════════════╣");
        eprintln!("  Usage: pandoras-logs <file> [threads]        ");
        eprintln!("         [--mmap]                              ");
        eprintln!("                                               ");
        eprintln!("  Arguments:                                   ");
        eprintln!("    <file>     Path to log file                ");
        eprintln!("    [threads]  Number of parse threads         ");
        eprintln!("               (default: all CPU cores)        ");
        eprintln!("    --mmap     Use memory-map instead of       ");
        eprintln!("               streaming I/O (higher RSS)      ");
        eprintln!("╚══════════════════════════════════════════════╝");
        std::process::exit(1);
    }

    let default_threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);

    let mut file_path: Option<&str> = None;
    let mut num_threads = default_threads;
    let mut use_mmap = false;

    for arg in args.iter().skip(1) {
        if arg == "--mmap" {
            use_mmap = true;
            continue;
        }
        if file_path.is_none() {
            file_path = Some(arg);
            continue;
        }
        if let Ok(n) = arg.parse::<usize>() {
            num_threads = n;
        } else {
            eprintln!("Invalid thread count: '{}', using default", arg);
        }
    }

    let file_path = file_path.unwrap_or_else(|| {
        eprintln!("Missing <file> argument");
        std::process::exit(1);
    });

    let mode_str = if use_mmap { "mmap" } else { "streaming" };

    println!();
    println!("╔════════════════════════════════════════════════════╗");
    println!("       PANDORA'S LOGS — SIMD Log Parser             ");
    println!("╠════════════════════════════════════════════════════╣");
    println!("  SIMD:   {:<42} ", simd_scan::simd_capability());
    println!("  Threads:{:<42} ", num_threads);
    println!("  Mode:   {:<42} ", mode_str);
    println!("  File:   {:<42} ", file_path);
    println!("╚════════════════════════════════════════════════════╝");
    println!();

    let file = File::open(file_path).unwrap_or_else(|e| {
        eprintln!("Error opening '{}': {}", file_path, e);
        std::process::exit(1);
    });

    let file_size = file.metadata().unwrap().len() as usize;

    println!(
        "File size: {:.2} GB ({} bytes)",
        file_size as f64 / (1024.0 * 1024.0 * 1024.0),
        file_size
    );

    if file_size == 0 {
        println!("File is empty. Nothing to parse.");
        return;
    }

    let chunk_mb = std::env::var("PANDORA_CHUNK_MB")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v >= 1)
        .unwrap_or(64);

    println!(
        "\nFused Pipeline: Scan+Parse ({} threads, {} MB chunks, {})...",
        num_threads, chunk_mb, mode_str
    );

    let total_start = Instant::now();

    let result = if use_mmap {
        let mmap = unsafe { Mmap::map(&file) }.unwrap_or_else(|e| {
            eprintln!("Error memory-mapping '{}': {}", file_path, e);
            std::process::exit(1);
        });

        #[cfg(unix)]
        unsafe {
            libc::madvise(
                mmap.as_ptr() as *mut libc::c_void,
                mmap.len(),
                libc::MADV_SEQUENTIAL,
            );
        }

        orchestrator::parse_logs_pipelined(&mmap, num_threads)
    } else {
        let mut f = file;
        orchestrator::parse_logs_streamed(&mut f, file_size as u64, num_threads)
    };

    let total_elapsed = total_start.elapsed();
    let total_ms = total_elapsed.as_secs_f64() * 1000.0;

    let num_lines = result.total_lines;
    let throughput = (file_size as f64 / (1024.0 * 1024.0 * 1024.0)) / total_elapsed.as_secs_f64();
    println!(
        "  Processed {} lines in {:.1} ms ({:.2} GB/s)",
        num_lines, total_ms, throughput
    );

    println!();
    let stats = ParseStats {
        total_bytes: file_size as u64,
        total_lines: num_lines as u64,
        scan_time_ms: result.scan_time_ms,
        parse_time_ms: result.parse_time_ms,
        total_time_ms: total_ms,
        threads_used: num_threads,
    };
    print!("{}", stats);

    if let Some(first_batch) = result.batches.first() {
        let sample_count = first_batch.len.min(10);
        if sample_count > 0 {
            println!("\nSample log records:");
            println!("─────────────────────────────────────────────────────────────────────────");
            for i in 0..sample_count {
                unsafe {
                    println!(
                        "  [{:>4}] {} | {:>7} | {:>20} | {}",
                        i,
                        first_batch.timestamps[i],
                        first_batch.levels[i],
                        first_batch.component(i),
                        truncate_str(first_batch.message(i), 60)
                    );
                }
            }
            println!("─────────────────────────────────────────────────────────────────────────");
        }
    }

    println!(
        "\nParsed {} log records at {:.2} GB/s\n",
        num_lines,
        stats.throughput_gbps()
    );
}

fn truncate_str(s: &str, max_len: usize) -> Cow<'_, str> {
    if s.len() <= max_len {
        Cow::Borrowed(s)
    } else {
        let end = s.floor_char_boundary(max_len.saturating_sub(3));
        Cow::Owned(format!("{}...", &s[..end]))
    }
}
