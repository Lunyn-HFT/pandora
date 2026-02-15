use crate::csv_parser::{self, CsvHeader};
use crate::format::LogFormat;
use crate::json_parser;
use crate::logfmt_parser;
use crate::simd_scan;
use crate::structured::StructuredBatch;
use std::fs::File;
use std::io::Read;
use std::thread;
use std::time::Instant;

pub struct StructuredPipelineResult {
    pub batches: Vec<StructuredBatch>,
    pub total_records: usize,
    pub total_fields: usize,
    pub scan_time_ms: f64,
    pub parse_time_ms: f64,
    pub format: LogFormat,

    pub _backing_data: Vec<Vec<u8>>,
}

pub fn parse_structured_mmap(
    data: &[u8],
    num_threads: usize,
    format_hint: Option<LogFormat>,
) -> StructuredPipelineResult {
    if data.is_empty() {
        return StructuredPipelineResult {
            batches: vec![],
            total_records: 0,
            total_fields: 0,
            scan_time_ms: 0.0,
            parse_time_ms: 0.0,
            format: LogFormat::PlainText,
            _backing_data: vec![],
        };
    }

    let format = format_hint.unwrap_or_else(|| LogFormat::detect(data));

    match format {
        LogFormat::Json => parse_json_mmap(data, num_threads),
        LogFormat::Logfmt => parse_logfmt_mmap(data, num_threads),
        LogFormat::Csv => parse_csv_mmap(data, num_threads),
        LogFormat::PlainText => parse_logfmt_mmap(data, num_threads),
    }
}

pub fn parse_structured_streamed(
    file: &mut File,
    file_size: u64,
    num_threads: usize,
    format_hint: Option<LogFormat>,
) -> StructuredPipelineResult {
    if file_size == 0 {
        return StructuredPipelineResult {
            batches: vec![],
            total_records: 0,
            total_fields: 0,
            scan_time_ms: 0.0,
            parse_time_ms: 0.0,
            format: LogFormat::PlainText,
            _backing_data: vec![],
        };
    }

    let segment_size = std::env::var("PANDORA_CHUNK_MB")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v >= 1)
        .map(|mb| mb * 1024 * 1024)
        .unwrap_or(64 * 1024 * 1024);

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
    let mut result_batches: Vec<StructuredBatch> = Vec::new();
    let mut backing_data: Vec<Vec<u8>> = Vec::new();
    let mut total_records = 0usize;
    let mut total_fields = 0usize;
    let mut total_scan_ms = 0.0f64;
    let mut total_parse_ms = 0.0f64;
    let mut format: Option<LogFormat> = format_hint;
    let mut csv_header: Option<CsvHeader> = None;
    let mut first_chunk = true;

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

        if first_chunk {
            if format.is_none() {
                format = Some(LogFormat::detect(&work_buf));
            }
            first_chunk = false;
        }

        let detected_format = format.unwrap_or(LogFormat::PlainText);

        if detected_format == LogFormat::Csv && csv_header.is_none() {
            csv_header = CsvHeader::parse(&work_buf);
            if csv_header.is_some() {
                let header_end = csv_parser::header_end_offset(&work_buf);
                if header_end < work_buf.len() {
                    work_buf = work_buf[header_end..].to_vec();
                } else {
                    continue;
                }
            }
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

        let (batch, scan_ms, parse_ms) = parse_structured_chunk_owned(
            &work_buf,
            detected_format,
            csv_header.as_ref(),
            num_threads,
        );
        total_records += batch.len;
        total_fields += batch.fields.len();
        total_scan_ms += scan_ms;
        total_parse_ms += parse_ms;

        result_batches.push(batch);
        backing_data.push(work_buf);

        if at_eof {
            break;
        }
    }

    StructuredPipelineResult {
        batches: result_batches,
        total_records,
        total_fields,
        scan_time_ms: total_scan_ms,
        parse_time_ms: total_parse_ms,
        format: format.unwrap_or(LogFormat::PlainText),
        _backing_data: backing_data,
    }
}

fn read_full(reader: &mut impl Read, buf: &mut [u8]) -> std::io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        match reader.read(&mut buf[filled..])? {
            0 => break,
            n => filled += n,
        }
    }
    Ok(filled)
}

fn parse_json_mmap(data: &[u8], num_threads: usize) -> StructuredPipelineResult {
    parse_format_mmap(data, num_threads, LogFormat::Json, None)
}

fn parse_logfmt_mmap(data: &[u8], num_threads: usize) -> StructuredPipelineResult {
    parse_format_mmap(data, num_threads, LogFormat::Logfmt, None)
}

fn parse_csv_mmap(data: &[u8], num_threads: usize) -> StructuredPipelineResult {
    let csv_header = CsvHeader::parse(data);
    let data_start = csv_parser::header_end_offset(data);

    if data_start >= data.len() {
        return StructuredPipelineResult {
            batches: vec![],
            total_records: 0,
            total_fields: 0,
            scan_time_ms: 0.0,
            parse_time_ms: 0.0,
            format: LogFormat::Csv,
            _backing_data: vec![],
        };
    }

    let body = &data[data_start..];
    let mut result = parse_format_mmap(body, num_threads, LogFormat::Csv, csv_header.as_ref());
    result.format = LogFormat::Csv;
    result
}

fn parse_format_mmap(
    data: &[u8],
    num_threads: usize,
    format: LogFormat,
    csv_header: Option<&CsvHeader>,
) -> StructuredPipelineResult {
    if data.is_empty() {
        return StructuredPipelineResult {
            batches: vec![],
            total_records: 0,
            total_fields: 0,
            scan_time_ms: 0.0,
            parse_time_ms: 0.0,
            format,
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
    let worker_threads = num_threads.max(1).min(num_chunks.max(1));

    if worker_threads == 1 || num_chunks <= 1 {
        let mut batches = Vec::with_capacity(num_chunks);
        let mut total_scan_ms = 0.0f64;
        let mut total_parse_ms = 0.0f64;
        let mut total_records = 0;
        let mut total_fields = 0;

        for i in 0..num_chunks {
            let start = boundaries[i];
            let end = boundaries[i + 1];
            let (batch, scan_ms, parse_ms) =
                parse_structured_chunk(data, start, end, format, csv_header);
            total_records += batch.len;
            total_fields += batch.fields.len();
            total_scan_ms += scan_ms;
            total_parse_ms += parse_ms;
            batches.push(batch);
        }

        return StructuredPipelineResult {
            batches,
            total_records,
            total_fields,
            scan_time_ms: total_scan_ms,
            parse_time_ms: total_parse_ms,
            format,
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

    let mut ordered_batches: Vec<Option<StructuredBatch>> = (0..num_chunks).map(|_| None).collect();
    let mut scan_time_ms = 0.0f64;
    let mut parse_time_ms = 0.0f64;

    thread::scope(|scope| {
        let mut handles = Vec::with_capacity(worker_threads);
        for worker_chunks in assignments.into_iter() {
            handles.push(scope.spawn(move || {
                let mut local = Vec::with_capacity(worker_chunks.len());
                let mut worker_scan_ms = 0.0f64;
                let mut worker_parse_ms = 0.0f64;

                for (chunk_idx, start, end) in worker_chunks {
                    let (batch, s_ms, p_ms) =
                        parse_structured_chunk(data, start, end, format, csv_header);
                    worker_scan_ms += s_ms;
                    worker_parse_ms += p_ms;
                    local.push((chunk_idx, batch));
                }
                (local, worker_scan_ms, worker_parse_ms)
            }));
        }

        for handle in handles {
            let (worker_results, w_scan, w_parse) =
                handle.join().expect("structured worker panicked");
            scan_time_ms = scan_time_ms.max(w_scan);
            parse_time_ms = parse_time_ms.max(w_parse);
            for (chunk_idx, batch) in worker_results {
                ordered_batches[chunk_idx] = Some(batch);
            }
        }
    });

    let mut batches = Vec::with_capacity(num_chunks);
    let mut total_records = 0;
    let mut total_fields = 0;
    for batch in ordered_batches.into_iter().flatten() {
        total_records += batch.len;
        total_fields += batch.fields.len();
        batches.push(batch);
    }

    StructuredPipelineResult {
        batches,
        total_records,
        total_fields,
        scan_time_ms,
        parse_time_ms,
        format,
        _backing_data: vec![],
    }
}

fn parse_structured_chunk(
    data: &[u8],
    start: usize,
    end: usize,
    format: LogFormat,
    csv_header: Option<&CsvHeader>,
) -> (StructuredBatch, f64, f64) {
    let chunk = &data[start..end];
    let data_len = data.len() as u64;

    let scan_start = Instant::now();
    let estimated = (chunk.len() / 80).max(16);
    let mut line_starts = Vec::with_capacity(estimated + 2);
    line_starts.push(start as u64);
    simd_scan::scan_region(chunk, start as u64, data_len, &mut line_starts);
    line_starts.push(end as u64);
    let scan_ms = scan_start.elapsed().as_secs_f64() * 1000.0;

    let num_lines = line_starts.len() - 1;

    let parse_start = Instant::now();
    let avg_fields = match format {
        LogFormat::Json => 8,
        LogFormat::Logfmt => 6,
        LogFormat::Csv => csv_header.map(|h| h.num_columns()).unwrap_or(4),
        LogFormat::PlainText => 4,
    };
    let mut batch =
        StructuredBatch::with_capacity(num_lines, num_lines * avg_fields, data.as_ptr());

    match format {
        LogFormat::Json => {
            json_parser::parse_json_lines_range(data, &line_starts, 0, num_lines, &mut batch);
        }
        LogFormat::Logfmt | LogFormat::PlainText => {
            logfmt_parser::parse_logfmt_lines_range(data, &line_starts, 0, num_lines, &mut batch);
        }
        LogFormat::Csv => {
            if let Some(header) = csv_header {
                csv_parser::parse_csv_lines_range(
                    data,
                    &line_starts,
                    0,
                    num_lines,
                    header,
                    &mut batch,
                );
            }
        }
    }

    let parse_ms = parse_start.elapsed().as_secs_f64() * 1000.0;

    (batch, scan_ms, parse_ms)
}

fn parse_structured_chunk_owned(
    data: &[u8],
    format: LogFormat,
    csv_header: Option<&CsvHeader>,
    _num_threads: usize,
) -> (StructuredBatch, f64, f64) {
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
    let avg_fields = match format {
        LogFormat::Json => 8,
        LogFormat::Logfmt => 6,
        LogFormat::Csv => csv_header.map(|h| h.num_columns()).unwrap_or(4),
        LogFormat::PlainText => 4,
    };
    let mut batch =
        StructuredBatch::with_capacity(num_lines, num_lines * avg_fields, data.as_ptr());

    match format {
        LogFormat::Json => {
            json_parser::parse_json_lines_range(data, &line_starts, 0, num_lines, &mut batch);
        }
        LogFormat::Logfmt | LogFormat::PlainText => {
            logfmt_parser::parse_logfmt_lines_range(data, &line_starts, 0, num_lines, &mut batch);
        }
        LogFormat::Csv => {
            if let Some(header) = csv_header {
                csv_parser::parse_csv_lines_range(
                    data,
                    &line_starts,
                    0,
                    num_lines,
                    header,
                    &mut batch,
                );
            }
        }
    }

    let parse_ms = parse_start.elapsed().as_secs_f64() * 1000.0;

    (batch, scan_ms, parse_ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_structured_json_mmap() {
        let data = br#"{"level":"info","msg":"started","ts":"2025-02-12T10:31:45Z"}
{"level":"warn","msg":"slow","ts":"2025-02-12T10:31:46Z"}
{"level":"error","msg":"failed","ts":"2025-02-12T10:31:47Z"}
"#;
        let result = parse_structured_mmap(data, 1, Some(LogFormat::Json));
        assert_eq!(result.format, LogFormat::Json);
        assert_eq!(result.total_records, 3);
        assert!(result.total_fields >= 9);

        unsafe {
            let batch = &result.batches[0];
            assert_eq!(batch.level_value(0), Some("info"));
            assert_eq!(batch.level_value(1), Some("warn"));
            assert_eq!(batch.level_value(2), Some("error"));
        }
    }

    #[test]
    fn test_structured_logfmt_mmap() {
        let data = b"level=info msg=started ts=2025-02-12\nlevel=warn msg=slow ts=2025-02-13\n";
        let result = parse_structured_mmap(data, 1, Some(LogFormat::Logfmt));
        assert_eq!(result.format, LogFormat::Logfmt);
        assert_eq!(result.total_records, 2);

        unsafe {
            let batch = &result.batches[0];
            assert_eq!(batch.level_value(0), Some("info"));
            assert_eq!(batch.level_value(1), Some("warn"));
        }
    }

    #[test]
    fn test_structured_auto_detect_json() {
        let data = br#"{"level":"info","msg":"auto-detected"}
"#;
        let result = parse_structured_mmap(data, 1, None);
        assert_eq!(result.format, LogFormat::Json);
        assert_eq!(result.total_records, 1);
    }

    #[test]
    fn test_structured_auto_detect_logfmt() {
        let data = b"level=info msg=\"auto-detected\" ts=2025\n";
        let result = parse_structured_mmap(data, 1, None);
        assert_eq!(result.format, LogFormat::Logfmt);
        assert_eq!(result.total_records, 1);
    }

    #[test]
    fn test_structured_empty() {
        let result = parse_structured_mmap(b"", 1, None);
        assert_eq!(result.total_records, 0);
    }

    #[test]
    fn test_structured_json_multithreaded() {
        let mut data = Vec::new();
        for i in 0..100 {
            data.extend_from_slice(
                format!(
                    "{{\"level\":\"info\",\"msg\":\"request {}\",\"ts\":\"2025-02-12T10:31:{}Z\"}}\n",
                    i,
                    i % 60
                )
                .as_bytes(),
            );
        }
        let result = parse_structured_mmap(&data, 4, Some(LogFormat::Json));
        assert_eq!(result.total_records, 100);
    }
}
