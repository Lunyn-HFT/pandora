mod csv_parser;
mod data;
mod format;
mod json_parser;
mod logfmt_parser;
mod orchestrator;
mod parser;
mod simd_scan;
mod structured;
mod structured_orchestrator;

use data::ParseStats;
use format::LogFormat;
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
        eprintln!("         [--mmap] [--format <fmt>]             ");
        eprintln!("                                               ");
        eprintln!("  Arguments:                                   ");
        eprintln!("    <file>     Path to log file                ");
        eprintln!("    [threads]  Number of parse threads         ");
        eprintln!("               (default: all CPU cores)        ");
        eprintln!("    --mmap     Use memory-map instead of       ");
        eprintln!("               streaming I/O (higher RSS)      ");
        eprintln!("    --format   Force log format:               ");
        eprintln!("               auto, plain, json, logfmt, csv  ");
        eprintln!("               (default: auto-detect)          ");
        eprintln!("╚══════════════════════════════════════════════╝");
        std::process::exit(1);
    }

    let default_threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);

    let mut file_path: Option<&str> = None;
    let mut num_threads = default_threads;
    let mut use_mmap = false;
    let mut format_hint: Option<LogFormat> = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--mmap" => {
                use_mmap = true;
            }
            "--format" => {
                i += 1;
                if i < args.len() {
                    format_hint = match args[i].as_str() {
                        "json" | "ndjson" | "jsonl" => Some(LogFormat::Json),
                        "logfmt" => Some(LogFormat::Logfmt),
                        "csv" => Some(LogFormat::Csv),
                        "plain" | "text" | "plain-text" => Some(LogFormat::PlainText),
                        "auto" => None,
                        other => {
                            eprintln!("Unknown format '{}', using auto-detect", other);
                            None
                        }
                    };
                }
            }
            arg => {
                if file_path.is_none() {
                    file_path = Some(arg);
                } else if let Ok(n) = arg.parse::<usize>() {
                    num_threads = n;
                } else {
                    eprintln!("Invalid argument: '{}', ignoring", arg);
                }
            }
        }
        i += 1;
    }

    let file_path = file_path.unwrap_or_else(|| {
        eprintln!("Missing <file> argument");
        std::process::exit(1);
    });

    let mode_str = if use_mmap { "mmap" } else { "streaming" };

    let file = File::open(file_path).unwrap_or_else(|e| {
        eprintln!("Error opening '{}': {}", file_path, e);
        std::process::exit(1);
    });

    let file_size = file.metadata().unwrap().len() as usize;

    if file_size == 0 {
        println!("File is empty. Nothing to parse.");
        return;
    }

    let detected_format = if let Some(fmt) = format_hint {
        fmt
    } else {
        let mut peek_file = File::open(file_path).unwrap();
        let mut peek_buf = vec![0u8; 4096.min(file_size)];
        use std::io::Read;
        let _ = peek_file.read(&mut peek_buf);
        LogFormat::detect(&peek_buf)
    };

    let is_structured = detected_format != LogFormat::PlainText;

    println!();
    println!("╔════════════════════════════════════════════════════╗");
    println!("       PANDORA'S LOGS — SIMD Log Parser             ");
    println!("╠════════════════════════════════════════════════════╣");
    println!("  SIMD:   {:<42} ", simd_scan::simd_capability());
    println!("  Threads:{:<42} ", num_threads);
    println!("  Mode:   {:<42} ", mode_str);
    println!("  Format: {:<42} ", detected_format);
    println!("  File:   {:<42} ", file_path);
    println!("╚════════════════════════════════════════════════════╝");
    println!();
    println!(
        "File size: {:.2} GB ({} bytes)",
        file_size as f64 / (1024.0 * 1024.0 * 1024.0),
        file_size
    );

    let chunk_mb = std::env::var("PANDORA_CHUNK_MB")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v >= 1)
        .unwrap_or(64);

    println!(
        "\nFused Pipeline: Scan+Parse ({} threads, {} MB chunks, {}, {})...",
        num_threads, chunk_mb, mode_str, detected_format
    );

    let total_start = Instant::now();

    if is_structured {
        let mmap_holder;
        let result = if use_mmap {
            mmap_holder = Some(unsafe { Mmap::map(&file) }.unwrap_or_else(|e| {
                eprintln!("Error memory-mapping '{}': {}", file_path, e);
                std::process::exit(1);
            }));
            let mmap = mmap_holder.as_ref().unwrap();

            #[cfg(unix)]
            unsafe {
                libc::madvise(
                    mmap.as_ptr() as *mut libc::c_void,
                    mmap.len(),
                    libc::MADV_SEQUENTIAL,
                );
            }

            structured_orchestrator::parse_structured_mmap(mmap, num_threads, format_hint)
        } else {
            mmap_holder = None;
            let mut f = file;
            structured_orchestrator::parse_structured_streamed(
                &mut f,
                file_size as u64,
                num_threads,
                format_hint,
            )
        };
        let _ = &mmap_holder; // ensure mmap lives until here

        let total_elapsed = total_start.elapsed();
        let total_ms = total_elapsed.as_secs_f64() * 1000.0;
        let throughput =
            (file_size as f64 / (1024.0 * 1024.0 * 1024.0)) / total_elapsed.as_secs_f64();

        println!(
            "  Processed {} records ({} fields) in {:.1} ms ({:.2} GB/s)",
            result.total_records, result.total_fields, total_ms, throughput
        );

        println!();
        let stats = structured::StructuredParseStats {
            total_bytes: file_size as u64,
            total_records: result.total_records as u64,
            total_fields: result.total_fields as u64,
            scan_time_ms: result.scan_time_ms,
            parse_time_ms: result.parse_time_ms,
            total_time_ms: total_ms,
            threads_used: num_threads,
            format: detected_format.as_str(),
        };
        print!("{}", stats);

        if let Some(first_batch) = result.batches.first() {
            let sample_count = first_batch.len.min(10);
            if sample_count > 0 {
                println!("\nSample structured records:");
                println!(
                    "─────────────────────────────────────────────────────────────────────────"
                );
                for i in 0..sample_count {
                    unsafe {
                        let ts = first_batch.timestamp_value(i).unwrap_or("-");
                        let lvl = first_batch.level_value(i).unwrap_or("-");
                        let comp = first_batch.component_value(i).unwrap_or("-");
                        let msg = first_batch.message_value(i).unwrap_or("-");
                        let field_count = first_batch.field_count(i);

                        println!(
                            "  [{:>4}] {} | {:>7} | {:>20} | {} ({} fields)",
                            i,
                            truncate_str(ts, 24),
                            truncate_str(lvl, 7),
                            truncate_str(comp, 20),
                            truncate_str(msg, 40),
                            field_count
                        );
                    }
                }
                println!(
                    "─────────────────────────────────────────────────────────────────────────"
                );
            }
        }

        println!(
            "\nParsed {} structured records at {:.2} GB/s\n",
            result.total_records,
            stats.throughput_gbps()
        );
    } else {
        let mmap_holder;
        let result = if use_mmap {
            mmap_holder = Some(unsafe { Mmap::map(&file) }.unwrap_or_else(|e| {
                eprintln!("Error memory-mapping '{}': {}", file_path, e);
                std::process::exit(1);
            }));
            let mmap = mmap_holder.as_ref().unwrap();

            #[cfg(unix)]
            unsafe {
                libc::madvise(
                    mmap.as_ptr() as *mut libc::c_void,
                    mmap.len(),
                    libc::MADV_SEQUENTIAL,
                );
            }

            orchestrator::parse_logs_pipelined(mmap, num_threads)
        } else {
            mmap_holder = None;
            let mut f = file;
            orchestrator::parse_logs_streamed(&mut f, file_size as u64, num_threads)
        };
        let _ = &mmap_holder; // ensure mmap lives until here

        let total_elapsed = total_start.elapsed();
        let total_ms = total_elapsed.as_secs_f64() * 1000.0;

        let num_lines = result.total_lines;
        let throughput =
            (file_size as f64 / (1024.0 * 1024.0 * 1024.0)) / total_elapsed.as_secs_f64();
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
                println!(
                    "─────────────────────────────────────────────────────────────────────────"
                );
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
                println!(
                    "─────────────────────────────────────────────────────────────────────────"
                );
            }
        }

        println!(
            "\nParsed {} log records at {:.2} GB/s\n",
            num_lines,
            stats.throughput_gbps()
        );
    }
}

fn truncate_str(s: &str, max_len: usize) -> Cow<'_, str> {
    if s.len() <= max_len {
        Cow::Borrowed(s)
    } else {
        let end = s.floor_char_boundary(max_len.saturating_sub(3));
        Cow::Owned(format!("{}...", &s[..end]))
    }
}
