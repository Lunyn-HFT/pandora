use crate::data::LogBatch;
use crate::parser::parse_lines_range;
use crate::simd_scan;
use core_affinity::CoreId;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{self, Read};
use std::thread;
use std::time::Instant;

pub struct PipelineResult {
    pub batches: Vec<LogBatch>,
    pub total_lines: usize,
    pub scan_time_ms: f64,
    pub parse_time_ms: f64,

    pub _backing_data: Vec<Vec<u8>>,
}

#[allow(dead_code)]
pub struct StreamingResult {
    pub total_lines: usize,
    pub scan_time_ms: f64,
    pub parse_time_ms: f64,
}

#[derive(Clone, Copy, Debug)]
struct CpuTopoEntry {
    core: CoreId,
    package_id: Option<u32>,
    core_id: Option<u32>,
}

fn read_topology_u32(cpu_id: usize, leaf: &str) -> Option<u32> {
    let path = format!("/sys/devices/system/cpu/cpu{cpu_id}/topology/{leaf}");
    std::fs::read_to_string(path)
        .ok()?
        .trim()
        .parse::<u32>()
        .ok()
}

fn choose_pinned_cores(worker_threads: usize, core_ids: &[CoreId]) -> Vec<CoreId> {
    if worker_threads == 0 || core_ids.is_empty() {
        return Vec::new();
    }

    let topo: Vec<CpuTopoEntry> = core_ids
        .iter()
        .copied()
        .map(|core| CpuTopoEntry {
            core,
            package_id: read_topology_u32(core.id, "physical_package_id"),
            core_id: read_topology_u32(core.id, "core_id"),
        })
        .collect();

    let mut by_package: HashMap<Option<u32>, Vec<CpuTopoEntry>> = HashMap::new();
    for entry in topo {
        by_package.entry(entry.package_id).or_default().push(entry);
    }

    let mut packages: Vec<Vec<CpuTopoEntry>> = by_package.into_values().collect();
    packages.sort_by_key(|entries| std::cmp::Reverse(entries.len()));

    let mut selected = Vec::with_capacity(worker_threads);
    let mut used_core_ids: HashSet<(Option<u32>, Option<u32>)> = HashSet::new();

    for entries in &packages {
        for entry in entries {
            let key = (entry.package_id, entry.core_id);
            if used_core_ids.contains(&key) {
                continue;
            }
            used_core_ids.insert(key);
            selected.push(entry.core);
            if selected.len() >= worker_threads {
                return selected;
            }
        }
    }

    for entries in &packages {
        for entry in entries {
            if !selected.iter().any(|c| c.id == entry.core.id) {
                selected.push(entry.core);
                if selected.len() >= worker_threads {
                    return selected;
                }
            }
        }
    }

    selected
}

fn parse_chunk(data: &[u8], start: usize, end: usize, data_len: u64) -> (LogBatch, f64, f64) {
    let chunk = &data[start..end];
    let scan_start = Instant::now();
    let estimated = (chunk.len() / 80).max(16);
    let mut line_starts = Vec::with_capacity(estimated + 2);
    line_starts.push(start as u64);
    simd_scan::scan_region(chunk, start as u64, data_len, &mut line_starts);
    line_starts.push(end as u64);
    let scan_ms = scan_start.elapsed().as_secs_f64() * 1000.0;

    let num_lines = line_starts.len() - 1;
    let parse_start = Instant::now();
    let mut batch = LogBatch::new(num_lines, data.as_ptr());
    parse_lines_range(data, &line_starts, 0, num_lines, &mut batch);
    let parse_ms = parse_start.elapsed().as_secs_f64() * 1000.0;
    (batch, scan_ms, parse_ms)
}

#[allow(dead_code)]
fn parse_chunk_streaming(
    data: &[u8],
    start: usize,
    end: usize,
    data_len: u64,
) -> (usize, f64, f64) {
    let (batch, scan_ms, parse_ms) = parse_chunk(data, start, end, data_len);
    (batch.len, scan_ms, parse_ms)
}

pub fn parse_logs_pipelined(data: &[u8], _num_threads: usize) -> PipelineResult {
    if data.is_empty() {
        return PipelineResult {
            batches: vec![],
            total_lines: 0,
            scan_time_ms: 0.0,
            parse_time_ms: 0.0,
            _backing_data: vec![],
        };
    }

    let chunk_mb = std::env::var("PANDORA_CHUNK_MB")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v >= 1)
        .unwrap_or(64);
    let chunk_size = chunk_mb * 1024 * 1024;

    let mut boundaries = vec![0usize];
    let mut pos = chunk_size;
    while pos < data.len() {
        match memchr::memchr(b'\n', &data[pos..]) {
            Some(off) => {
                let boundary = pos + off + 1;
                boundaries.push(boundary);
                pos = boundary + chunk_size;
            }
            None => break,
        }
    }
    boundaries.push(data.len());

    let num_chunks = boundaries.len() - 1;
    let data_len = data.len() as u64;

    let requested_threads = _num_threads.max(1);
    let worker_threads = requested_threads.min(num_chunks.max(1));

    if worker_threads == 1 || num_chunks <= 1 {
        let mut batches = Vec::with_capacity(num_chunks);
        let mut scan_time_ms = 0.0_f64;
        let mut parse_time_ms = 0.0_f64;
        for i in 0..num_chunks {
            let start = boundaries[i];
            let end = boundaries[i + 1];
            let (batch, scan_ms, parse_ms) = parse_chunk(data, start, end, data_len);
            scan_time_ms += scan_ms;
            parse_time_ms += parse_ms;
            batches.push(batch);
        }
        let total_lines = batches.iter().map(|b| b.len).sum();
        return PipelineResult {
            batches,
            total_lines,
            scan_time_ms,
            parse_time_ms,
            _backing_data: vec![],
        };
    }

    let mut assignments: Vec<Vec<(usize, usize, usize)>> = vec![Vec::new(); worker_threads];
    for (worker_idx, assignment) in assignments.iter_mut().enumerate() {
        let start_chunk = (worker_idx * num_chunks) / worker_threads;
        let end_chunk = ((worker_idx + 1) * num_chunks) / worker_threads;
        for i in start_chunk..end_chunk {
            assignment.push((i, boundaries[i], boundaries[i + 1]));
        }
    }

    let core_ids = core_affinity::get_core_ids().unwrap_or_default();
    let enable_pinning = std::env::var("PANDORA_ENABLE_PINNING")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let pinned_cores = if enable_pinning {
        choose_pinned_cores(worker_threads, &core_ids)
    } else {
        Vec::new()
    };
    let mut ordered_batches: Vec<Option<LogBatch>> = (0..num_chunks).map(|_| None).collect();
    let mut scan_time_ms = 0.0_f64;
    let mut parse_time_ms = 0.0_f64;

    thread::scope(|scope| {
        let mut handles = Vec::with_capacity(worker_threads);
        for (worker_idx, worker_chunks) in assignments.into_iter().enumerate() {
            let worker_core = pinned_cores.get(worker_idx).copied();

            handles.push(scope.spawn(move || {
                if let Some(core) = worker_core {
                    let _ = core_affinity::set_for_current(core);
                }

                let mut local = Vec::with_capacity(worker_chunks.len());
                let mut worker_scan_ms = 0.0_f64;
                let mut worker_parse_ms = 0.0_f64;
                for (chunk_idx, start, end) in worker_chunks {
                    let (batch, chunk_scan_ms, chunk_parse_ms) =
                        parse_chunk(data, start, end, data_len);
                    worker_scan_ms += chunk_scan_ms;
                    worker_parse_ms += chunk_parse_ms;
                    local.push((chunk_idx, batch));
                }
                (local, worker_scan_ms, worker_parse_ms)
            }));
        }

        for handle in handles {
            let (worker_results, worker_scan_ms, worker_parse_ms) =
                handle.join().expect("worker thread panicked");
            scan_time_ms = scan_time_ms.max(worker_scan_ms);
            parse_time_ms = parse_time_ms.max(worker_parse_ms);
            for (chunk_idx, batch) in worker_results {
                ordered_batches[chunk_idx] = Some(batch);
            }
        }
    });

    let mut batches = Vec::with_capacity(num_chunks);
    for batch in ordered_batches.into_iter().flatten() {
        batches.push(batch);
    }

    let total_lines = batches.iter().map(|b| b.len).sum();
    PipelineResult {
        batches,
        total_lines,
        scan_time_ms,
        parse_time_ms,
        _backing_data: vec![],
    }
}

const STREAM_SEGMENT_SIZE: usize = 64 * 1024 * 1024;

fn read_full(reader: &mut impl Read, buf: &mut [u8]) -> io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        match reader.read(&mut buf[filled..])? {
            0 => break,
            n => filled += n,
        }
    }
    Ok(filled)
}

fn parse_owned_chunk(data: &[u8]) -> (LogBatch, f64, f64) {
    let data_len = data.len() as u64;

    let scan_start = Instant::now();
    let estimated = (data.len() / 80).max(16);
    let mut line_starts = Vec::with_capacity(estimated + 2);
    line_starts.push(0u64);
    simd_scan::scan_region(data, 0, data_len, &mut line_starts);
    line_starts.push(data_len);
    let scan_ms = scan_start.elapsed().as_secs_f64() * 1000.0;

    let num_lines = line_starts.len() - 1;
    let parse_start = Instant::now();
    let mut batch = LogBatch::new(num_lines, data.as_ptr());
    parse_lines_range(data, &line_starts, 0, num_lines, &mut batch);
    let parse_ms = parse_start.elapsed().as_secs_f64() * 1000.0;

    (batch, scan_ms, parse_ms)
}

pub fn parse_logs_streamed(file: &mut File, file_size: u64, _num_threads: usize) -> PipelineResult {
    if file_size == 0 {
        return PipelineResult {
            batches: vec![],
            total_lines: 0,
            scan_time_ms: 0.0,
            parse_time_ms: 0.0,
            _backing_data: vec![],
        };
    }

    let segment_size = std::env::var("PANDORA_CHUNK_MB")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v >= 1)
        .map(|mb| mb * 1024 * 1024)
        .unwrap_or(STREAM_SEGMENT_SIZE);

    #[cfg(unix)]
    unsafe {
        use std::os::unix::io::AsRawFd;
        libc::posix_fadvise(
            file.as_raw_fd(),
            0,
            file_size as i64,
            libc::POSIX_FADV_SEQUENTIAL,
        );
    }

    let mut read_buf = vec![0u8; segment_size];
    let mut leftover: Vec<u8> = Vec::new();

    let mut result_batches: Vec<LogBatch> = Vec::new();
    let mut backing_data: Vec<Vec<u8>> = Vec::new();
    let mut total_lines = 0usize;
    let mut total_scan_ms = 0.0_f64;
    let mut total_parse_ms = 0.0_f64;

    loop {
        let bytes_read = read_full(file, &mut read_buf).unwrap_or(0);
        let at_eof = bytes_read < segment_size;

        let mut work_buf: Vec<u8> = if leftover.is_empty() {
            if bytes_read == 0 {
                break;
            }
            read_buf[..bytes_read].to_vec()
        } else {
            let mut combined = std::mem::take(&mut leftover);
            combined.extend_from_slice(&read_buf[..bytes_read]);
            combined
        };

        if work_buf.is_empty() {
            break;
        }

        let complete_end = if at_eof {
            work_buf.len()
        } else {
            match memchr::memrchr(b'\n', &work_buf) {
                Some(pos) => pos + 1,
                None => {
                    leftover = work_buf;
                    continue;
                }
            }
        };

        if complete_end < work_buf.len() {
            leftover = work_buf[complete_end..].to_vec();
        }
        work_buf.truncate(complete_end);

        if work_buf.is_empty() {
            if at_eof {
                break;
            }
            continue;
        }

        let (batch, scan_ms, parse_ms) = parse_owned_chunk(&work_buf);
        total_lines += batch.len;
        total_scan_ms += scan_ms;
        total_parse_ms += parse_ms;

        if result_batches.is_empty() {
            result_batches.push(batch);
            backing_data.push(work_buf);
        }
    }

    PipelineResult {
        batches: result_batches,
        total_lines,
        scan_time_ms: total_scan_ms,
        parse_time_ms: total_parse_ms,
        _backing_data: backing_data,
    }
}

#[allow(dead_code)]
pub fn parse_logs_pipelined_streaming(data: &[u8], _num_threads: usize) -> StreamingResult {
    if data.is_empty() {
        return StreamingResult {
            total_lines: 0,
            scan_time_ms: 0.0,
            parse_time_ms: 0.0,
        };
    }

    let chunk_mb = std::env::var("PANDORA_CHUNK_MB")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v >= 1)
        .unwrap_or(64);
    let chunk_size = chunk_mb * 1024 * 1024;

    let mut boundaries = vec![0usize];
    let mut pos = chunk_size;
    while pos < data.len() {
        match memchr::memchr(b'\n', &data[pos..]) {
            Some(off) => {
                let boundary = pos + off + 1;
                boundaries.push(boundary);
                pos = boundary + chunk_size;
            }
            None => break,
        }
    }
    boundaries.push(data.len());

    let num_chunks = boundaries.len() - 1;
    let data_len = data.len() as u64;

    let requested_threads = _num_threads.max(1);
    let worker_threads = requested_threads.min(num_chunks.max(1));

    if worker_threads == 1 || num_chunks <= 1 {
        let mut total_lines = 0usize;
        let mut scan_time_ms = 0.0_f64;
        let mut parse_time_ms = 0.0_f64;
        for i in 0..num_chunks {
            let start = boundaries[i];
            let end = boundaries[i + 1];
            let (lines, scan_ms, parse_ms) = parse_chunk_streaming(data, start, end, data_len);
            total_lines += lines;
            scan_time_ms += scan_ms;
            parse_time_ms += parse_ms;
        }
        return StreamingResult {
            total_lines,
            scan_time_ms,
            parse_time_ms,
        };
    }

    let mut assignments: Vec<Vec<(usize, usize)>> = vec![Vec::new(); worker_threads];
    for (worker_idx, assignment) in assignments.iter_mut().enumerate() {
        let start_chunk = (worker_idx * num_chunks) / worker_threads;
        let end_chunk = ((worker_idx + 1) * num_chunks) / worker_threads;
        for i in start_chunk..end_chunk {
            assignment.push((boundaries[i], boundaries[i + 1]));
        }
    }

    let core_ids = core_affinity::get_core_ids().unwrap_or_default();
    let enable_pinning = std::env::var("PANDORA_ENABLE_PINNING")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let pinned_cores = if enable_pinning {
        choose_pinned_cores(worker_threads, &core_ids)
    } else {
        Vec::new()
    };

    let mut total_lines = 0usize;
    let mut scan_time_ms = 0.0_f64;
    let mut parse_time_ms = 0.0_f64;

    thread::scope(|scope| {
        let mut handles = Vec::with_capacity(worker_threads);
        for (worker_idx, worker_chunks) in assignments.into_iter().enumerate() {
            let worker_core = pinned_cores.get(worker_idx).copied();

            handles.push(scope.spawn(move || {
                if let Some(core) = worker_core {
                    let _ = core_affinity::set_for_current(core);
                }

                let mut worker_total = 0usize;
                let mut worker_scan_ms = 0.0_f64;
                let mut worker_parse_ms = 0.0_f64;

                for (start, end) in worker_chunks {
                    let (lines, chunk_scan_ms, chunk_parse_ms) =
                        parse_chunk_streaming(data, start, end, data_len);
                    worker_total += lines;
                    worker_scan_ms += chunk_scan_ms;
                    worker_parse_ms += chunk_parse_ms;
                }
                (worker_total, worker_scan_ms, worker_parse_ms)
            }));
        }

        for handle in handles {
            let (worker_total, worker_scan_ms, worker_parse_ms) =
                handle.join().expect("worker thread panicked");
            total_lines += worker_total;
            scan_time_ms = scan_time_ms.max(worker_scan_ms);
            parse_time_ms = parse_time_ms.max(worker_parse_ms);
        }
    });

    StreamingResult {
        total_lines,
        scan_time_ms,
        parse_time_ms,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::LogLevel;

    #[test]
    fn test_pipelined_parse_basic() {
        let data = b"2025-02-12T10:31:45Z INFO api-server request_id=abc123\n\
                     2025-02-12T10:31:46Z WARN auth-service auth_failed\n\
                     2025-02-12T10:31:47Z ERROR database-pool connection_timeout\n";

        let result = parse_logs_pipelined(data, 2);
        assert_eq!(result.total_lines, 3);

        let first = &result.batches[0];
        assert_eq!(first.levels[0], LogLevel::Info);
        assert_eq!(first.levels[1], LogLevel::Warn);
        assert_eq!(first.levels[2], LogLevel::Error);

        unsafe {
            assert_eq!(first.component(0), "api-server");
            assert_eq!(first.component(1), "auth-service");
            assert_eq!(first.component(2), "database-pool");
        }
    }

    #[test]
    fn test_pipelined_parse_single_line() {
        let data = b"2025-02-12T10:31:45Z DEBUG cache-service hit_ratio=0.85\n";
        let result = parse_logs_pipelined(data, 1);
        assert_eq!(result.total_lines, 1);
        assert_eq!(result.batches[0].levels[0], LogLevel::Debug);
    }

    #[test]
    fn test_pipelined_parse_many_lines() {
        let mut data = Vec::new();
        for _ in 0..100 {
            data.extend_from_slice(b"2025-02-12T10:31:45Z INFO api-server request_id=abc123\n");
        }

        let result = parse_logs_pipelined(&data, 8);
        assert_eq!(result.total_lines, 100);

        for batch in &result.batches {
            for i in 0..batch.len {
                assert_eq!(batch.levels[i], LogLevel::Info);
            }
        }
    }

    #[test]
    fn test_pipelined_parse_large() {
        let mut data = Vec::new();
        for _ in 0..1000 {
            data.extend_from_slice(b"2025-02-12T10:31:45Z INFO api-server request_id=abc123\n");
        }

        let result = parse_logs_pipelined(&data, 4);
        assert_eq!(result.total_lines, 1000);

        let first = &result.batches[0];
        assert_eq!(first.levels[0], LogLevel::Info);
        unsafe {
            assert_eq!(first.component(0), "api-server");
        }
    }
}
