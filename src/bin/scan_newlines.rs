use pandoraslogs::simd_scan;
use std::fs::File;
use std::io::{self, Read};
use std::thread;
use std::time::Instant;

const WINDOW_SIZE: usize = 2 * 1024 * 1024;

const STREAM_BUF_SIZE: usize = 2 * 1024 * 1024;

#[derive(Clone, Copy, PartialEq)]
enum IoMode {
    SlidingMmap,

    Streaming,

    FullMmap,
}

fn parse_args() -> (String, usize, bool, IoMode) {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: scan-newlines <file> [threads] [--quiet] [--mmap] [--streaming]");
        std::process::exit(1);
    }

    let mut file_path: Option<String> = None;
    let mut threads: Option<usize> = None;
    let mut quiet = false;
    let mut mode = IoMode::SlidingMmap;

    for arg in args.iter().skip(1) {
        match arg.as_str() {
            "--quiet" | "-q" => {
                quiet = true;
                continue;
            }
            "--mmap" => {
                mode = IoMode::FullMmap;
                continue;
            }
            "--streaming" => {
                mode = IoMode::Streaming;
                continue;
            }
            _ => {}
        }

        if file_path.is_none() {
            file_path = Some(arg.clone());
            continue;
        }
        if threads.is_none() {
            threads = arg.parse::<usize>().ok();
            continue;
        }
    }

    let file_path = file_path.unwrap_or_else(|| {
        eprintln!("Missing <file> argument");
        std::process::exit(1);
    });

    let default_threads = thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);

    let threads = threads.unwrap_or(default_threads).max(1);
    (file_path, threads, quiet, mode)
}

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

#[cfg(unix)]
fn pread_full(fd: i32, buf: &mut [u8], mut offset: i64) -> io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        let ret = unsafe {
            libc::pread(
                fd,
                buf[filled..].as_mut_ptr() as *mut libc::c_void,
                buf.len() - filled,
                offset,
            )
        };
        if ret < 0 {
            return Err(io::Error::last_os_error());
        }
        if ret == 0 {
            break;
        }
        filled += ret as usize;
        offset += ret as i64;
    }
    Ok(filled)
}

fn count_lines_sliding_mmap_parallel(file: &File, num_threads: usize) -> u64 {
    use memmap2::Mmap;

    let mmap = unsafe { Mmap::map(file) }.unwrap_or_else(|e| {
        eprintln!("Error memory-mapping: {}", e);
        std::process::exit(1);
    });

    if mmap.is_empty() {
        return 0;
    }

    if num_threads <= 1 || mmap.len() < 1_000_000 {
        #[cfg(unix)]
        unsafe {
            libc::madvise(
                mmap.as_ptr() as *mut libc::c_void,
                mmap.len(),
                libc::MADV_SEQUENTIAL,
            );
        }

        let mut total_newlines = 0u64;
        let mut last_byte = 0u8;
        let mut offset = 0usize;

        while offset < mmap.len() {
            let end = (offset + WINDOW_SIZE).min(mmap.len());
            let chunk = &mmap[offset..end];
            total_newlines += simd_scan::count_newlines_in_region(chunk);
            last_byte = chunk[chunk.len() - 1];

            #[cfg(unix)]
            unsafe {
                libc::madvise(
                    mmap.as_ptr().add(offset) as *mut libc::c_void,
                    end - offset,
                    libc::MADV_DONTNEED,
                );
            }
            offset = end;
        }

        return if last_byte == b'\n' {
            total_newlines
        } else {
            total_newlines + 1
        };
    }

    let data_len = mmap.len();
    let segment_size = data_len.div_ceil(num_threads);
    let last_byte = mmap[data_len - 1];

    #[derive(Clone, Copy)]
    struct SendPtr(*const u8);
    unsafe impl Send for SendPtr {}
    unsafe impl Sync for SendPtr {}

    impl SendPtr {
        unsafe fn slice(&self, start: usize, end: usize) -> &[u8] {
            unsafe { std::slice::from_raw_parts(self.0.add(start), end - start) }
        }
    }

    let send = SendPtr(mmap.as_ptr());

    let counts: Vec<u64> = thread::scope(|scope| {
        let handles: Vec<_> = (0..num_threads)
            .map(|i| {
                let seg_start = i * segment_size;
                let seg_end = ((i + 1) * segment_size).min(data_len);
                let s = send;
                scope.spawn(move || {
                    let mut count = 0u64;
                    let mut offset = seg_start;
                    while offset < seg_end {
                        let end = (offset + WINDOW_SIZE).min(seg_end);
                        let chunk = unsafe { s.slice(offset, end) };
                        count += simd_scan::count_newlines_in_region(chunk);

                        #[cfg(unix)]
                        unsafe {
                            libc::madvise(
                                s.0.add(offset) as *mut libc::c_void,
                                end - offset,
                                libc::MADV_DONTNEED,
                            );
                        }
                        offset = end;
                    }
                    count
                })
            })
            .collect();

        handles
            .into_iter()
            .map(|h| h.join().expect("sliding-mmap worker panicked"))
            .collect()
    });

    let total_newlines: u64 = counts.iter().sum();

    if last_byte == b'\n' {
        total_newlines
    } else {
        total_newlines + 1
    }
}

fn count_lines_streaming_single(file: &mut File, file_size: u64) -> u64 {
    if file_size == 0 {
        return 0;
    }

    let buf_size = STREAM_BUF_SIZE.min(file_size as usize);
    let mut buf = vec![0u8; buf_size];
    let mut total_newlines = 0u64;
    let mut last_byte = 0u8;

    loop {
        let n = read_full(file, &mut buf).unwrap_or(0);
        if n == 0 {
            break;
        }
        total_newlines += simd_scan::count_newlines_in_region(&buf[..n]);
        last_byte = buf[n - 1];
    }

    if last_byte == b'\n' {
        total_newlines
    } else {
        total_newlines + 1
    }
}

#[cfg(unix)]
fn count_lines_streaming_parallel(file: &File, file_size: u64, num_threads: usize) -> u64 {
    use std::os::unix::io::AsRawFd;

    if file_size == 0 {
        return 0;
    }
    if num_threads <= 1 || file_size < 1_000_000 {
        let mut f = file.try_clone().expect("failed to clone file handle");
        return count_lines_streaming_single(&mut f, file_size);
    }

    let fd = file.as_raw_fd();
    let segment_size = (file_size as usize).div_ceil(num_threads);

    let counts: Vec<u64> = thread::scope(|scope| {
        let handles: Vec<_> = (0..num_threads)
            .map(|i| {
                let seg_start = (i * segment_size) as i64;
                let seg_end = (((i + 1) * segment_size) as u64).min(file_size) as i64;
                scope.spawn(move || {
                    let buf_size = STREAM_BUF_SIZE.min((seg_end - seg_start) as usize);
                    let mut buf = vec![0u8; buf_size];
                    let mut offset = seg_start;
                    let mut count = 0u64;
                    while offset < seg_end {
                        let to_read = buf_size.min((seg_end - offset) as usize);
                        let n = pread_full(fd, &mut buf[..to_read], offset).unwrap_or(0);
                        if n == 0 {
                            break;
                        }
                        count += simd_scan::count_newlines_in_region(&buf[..n]);
                        offset += n as i64;
                    }
                    count
                })
            })
            .collect();

        handles
            .into_iter()
            .map(|h| h.join().expect("streaming worker panicked"))
            .collect()
    });

    let total_newlines: u64 = counts.iter().sum();

    let mut last = [0u8; 1];
    let n = pread_full(fd, &mut last, file_size as i64 - 1).unwrap_or(0);
    let last_byte = if n > 0 { last[0] } else { 0 };

    if last_byte == b'\n' {
        total_newlines
    } else {
        total_newlines + 1
    }
}

fn count_lines_mmap(file: &File, num_threads: usize) -> u64 {
    use memmap2::Mmap;

    let mmap = unsafe { Mmap::map(file) }.unwrap_or_else(|e| {
        eprintln!("Error memory-mapping: {}", e);
        std::process::exit(1);
    });

    #[cfg(unix)]
    unsafe {
        libc::madvise(
            mmap.as_ptr() as *mut libc::c_void,
            mmap.len(),
            libc::MADV_SEQUENTIAL,
        );
        libc::madvise(
            mmap.as_ptr() as *mut libc::c_void,
            mmap.len(),
            libc::MADV_WILLNEED,
        );
    }

    count_lines_mmap_inner(&mmap, num_threads)
}

fn count_lines_mmap_inner(data: &[u8], num_threads: usize) -> u64 {
    if data.is_empty() {
        return 0;
    }
    if num_threads <= 1 || data.len() < 1_000_000 {
        let mut line_starts = Vec::with_capacity((data.len() / 80).max(64));
        line_starts.push(0);
        simd_scan::scan_region(data, 0, data.len() as u64, &mut line_starts);
        return line_starts.len() as u64;
    }

    let chunk_size = data.len().div_ceil(num_threads);
    let data_len = data.len() as u64;

    let chunks: Vec<(usize, usize, bool)> = (0..num_threads)
        .map(|i| {
            let start = i * chunk_size;
            let end = ((i + 1) * chunk_size).min(data.len());
            (start, end, i == 0)
        })
        .filter(|(start, end, _)| start < end)
        .collect();

    thread::scope(|scope| {
        let mut handles = Vec::with_capacity(chunks.len());
        for (start, end, is_first_chunk) in chunks {
            handles.push(scope.spawn(move || {
                let mut local = Vec::with_capacity(((end - start) / 80).max(16) + 1);
                if is_first_chunk {
                    local.push(0);
                }
                simd_scan::scan_region(&data[start..end], start as u64, data_len, &mut local);
                local.len() as u64
            }));
        }

        let mut total_lines = 0u64;
        for handle in handles {
            total_lines += handle.join().expect("scan worker panicked");
        }
        total_lines
    })
}

fn main() {
    let (file_path, num_threads, quiet, mode) = parse_args();

    let file = File::open(&file_path).unwrap_or_else(|e| {
        eprintln!("Error opening '{}': {}", file_path, e);
        std::process::exit(1);
    });

    let file_size = file.metadata().unwrap().len();

    #[cfg(unix)]
    if mode == IoMode::Streaming {
        unsafe {
            use std::os::unix::io::AsRawFd;
            libc::posix_fadvise(
                file.as_raw_fd(),
                0,
                file_size as i64,
                libc::POSIX_FADV_SEQUENTIAL,
            );
        }
    }

    let start = Instant::now();

    let line_count = match mode {
        IoMode::SlidingMmap => count_lines_sliding_mmap_parallel(&file, num_threads),
        IoMode::FullMmap => count_lines_mmap(&file, num_threads),
        IoMode::Streaming => {
            #[cfg(unix)]
            {
                count_lines_streaming_parallel(&file, file_size, num_threads)
            }
            #[cfg(not(unix))]
            {
                let mut f = file.try_clone().expect("clone");
                count_lines_streaming_single(&mut f, file_size)
            }
        }
    };

    let elapsed = start.elapsed().as_secs_f64();

    if quiet {
        println!("{}", line_count);
        return;
    }

    let mode_str = match mode {
        IoMode::SlidingMmap => "sliding-mmap",
        IoMode::FullMmap => "mmap",
        IoMode::Streaming => "streaming",
    };

    let file_size_gib = file_size as f64 / (1024.0 * 1024.0 * 1024.0);
    let throughput = if elapsed > 0.0 {
        file_size_gib / elapsed
    } else {
        0.0
    };

    println!("file={}", file_path);
    println!("bytes={}", file_size);
    println!("threads={}", num_threads);
    println!("mode={}", mode_str);
    println!("simd={}", simd_scan::simd_capability());
    println!("line_count={}", line_count);
    println!("elapsed_ms={:.3}", elapsed * 1000.0);
    println!("throughput_gib_s={:.3}", throughput);
}
