#[cfg(test)]
use std::thread;

#[cfg(test)]
pub fn scan_newlines(data: &[u8]) -> Vec<u64> {
    if data.is_empty() {
        return vec![0];
    }

    let estimated_lines = (data.len() / 80).max(64);
    let mut line_starts: Vec<u64> = Vec::with_capacity(estimated_lines);
    line_starts.push(0);

    let data_len = data.len() as u64;
    scan_region(data, 0, data_len, &mut line_starts);
    line_starts
}

#[cfg(test)]
pub fn scan_newlines_parallel(data: &[u8], num_threads: usize) -> Vec<u64> {
    if data.is_empty() {
        return vec![0];
    }
    if num_threads <= 1 || data.len() < 1_000_000 {
        return scan_newlines(data);
    }

    let chunk_size = data.len().div_ceil(num_threads);
    let data_len = data.len() as u64;

    let chunks: Vec<(usize, usize, bool)> = (0..num_threads)
        .map(|i| {
            let start = i * chunk_size;
            let end = ((i + 1) * chunk_size).min(data.len());
            (start, end, i == 0)
        })
        .filter(|(s, e, _)| s < e)
        .collect();

    let mut results: Vec<(usize, Vec<u64>)> = Vec::with_capacity(chunks.len());
    thread::scope(|scope| {
        let mut handles = Vec::with_capacity(chunks.len());
        for (idx, (start, end, is_first)) in chunks.iter().copied().enumerate() {
            handles.push(scope.spawn(move || {
                let chunk_data = &data[start..end];
                let estimated = (chunk_data.len() / 80).max(16);
                let mut local = Vec::with_capacity(estimated + 1);
                if is_first {
                    local.push(0);
                }
                scan_region(chunk_data, start as u64, data_len, &mut local);
                (idx, local)
            }));
        }

        for handle in handles {
            let (idx, local) = handle.join().expect("scan thread panicked");
            results.push((idx, local));
        }
    });

    results.sort_unstable_by_key(|(idx, _)| *idx);

    let total: usize = results.iter().map(|(_, v)| v.len()).sum();
    let mut merged = Vec::with_capacity(total);
    for (_, r) in results {
        merged.extend(r);
    }
    merged
}

pub fn scan_region(data: &[u8], global_base: u64, data_total_len: u64, line_starts: &mut Vec<u64>) {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx512f") && is_x86_feature_detected!("avx512bw") {
            unsafe {
                scan_region_avx512(data, global_base, data_total_len, line_starts);
            }
            return;
        }
        if is_x86_feature_detected!("avx2") {
            unsafe {
                scan_region_avx2(data, global_base, data_total_len, line_starts);
            }
            return;
        }
    }

    scan_region_scalar(data, global_base, data_total_len, line_starts);
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f,avx512bw")]
unsafe fn scan_region_avx512(
    data: &[u8],
    global_base: u64,
    data_total_len: u64,
    line_starts: &mut Vec<u64>,
) {
    unsafe {
        use std::arch::x86_64::*;

        let newline = _mm512_set1_epi8(0x0Au8 as i8);
        let len = data.len();
        let ptr = data.as_ptr();

        let mut offset = 0usize;
        let unrolled_end = if len >= 256 { len - 255 } else { 0 };

        while offset < unrolled_end {
            let zmm0 = _mm512_loadu_si512(ptr.add(offset) as *const _);
            let zmm1 = _mm512_loadu_si512(ptr.add(offset + 64) as *const _);
            let zmm2 = _mm512_loadu_si512(ptr.add(offset + 128) as *const _);
            let zmm3 = _mm512_loadu_si512(ptr.add(offset + 192) as *const _);

            let mask0 = _mm512_cmpeq_epi8_mask(zmm0, newline);
            let mask1 = _mm512_cmpeq_epi8_mask(zmm1, newline);
            let mask2 = _mm512_cmpeq_epi8_mask(zmm2, newline);
            let mask3 = _mm512_cmpeq_epi8_mask(zmm3, newline);

            extract_positions_from_mask(
                mask0,
                global_base + offset as u64,
                data_total_len,
                line_starts,
            );
            extract_positions_from_mask(
                mask1,
                global_base + (offset + 64) as u64,
                data_total_len,
                line_starts,
            );
            extract_positions_from_mask(
                mask2,
                global_base + (offset + 128) as u64,
                data_total_len,
                line_starts,
            );
            extract_positions_from_mask(
                mask3,
                global_base + (offset + 192) as u64,
                data_total_len,
                line_starts,
            );

            offset += 256;
        }

        let single_end = if len >= 64 { len - 63 } else { 0 };
        while offset < single_end {
            let zmm = _mm512_loadu_si512(ptr.add(offset) as *const _);
            let mask = _mm512_cmpeq_epi8_mask(zmm, newline);
            extract_positions_from_mask(
                mask,
                global_base + offset as u64,
                data_total_len,
                line_starts,
            );
            offset += 64;
        }

        while offset < len {
            if *ptr.add(offset) == b'\n' {
                let next_line = global_base + (offset as u64) + 1;
                if next_line < data_total_len {
                    line_starts.push(next_line);
                }
            }
            offset += 1;
        }
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn scan_region_avx2(
    data: &[u8],
    global_base: u64,
    data_total_len: u64,
    line_starts: &mut Vec<u64>,
) {
    unsafe {
        use std::arch::x86_64::*;

        let newline = _mm256_set1_epi8(0x0Au8 as i8);
        let len = data.len();
        let ptr = data.as_ptr();

        let mut offset = 0usize;
        let unrolled_end = if len >= 256 { len - 255 } else { 0 };

        while offset < unrolled_end {
            let mask0 = avx2_cmp_64(ptr.add(offset), newline);
            let mask1 = avx2_cmp_64(ptr.add(offset + 64), newline);
            let mask2 = avx2_cmp_64(ptr.add(offset + 128), newline);
            let mask3 = avx2_cmp_64(ptr.add(offset + 192), newline);

            extract_positions_from_mask(
                mask0,
                global_base + offset as u64,
                data_total_len,
                line_starts,
            );
            extract_positions_from_mask(
                mask1,
                global_base + (offset + 64) as u64,
                data_total_len,
                line_starts,
            );
            extract_positions_from_mask(
                mask2,
                global_base + (offset + 128) as u64,
                data_total_len,
                line_starts,
            );
            extract_positions_from_mask(
                mask3,
                global_base + (offset + 192) as u64,
                data_total_len,
                line_starts,
            );

            offset += 256;
        }

        let single_end = if len >= 64 { len - 63 } else { 0 };
        while offset < single_end {
            let mask = avx2_cmp_64(ptr.add(offset), newline);
            extract_positions_from_mask(
                mask,
                global_base + offset as u64,
                data_total_len,
                line_starts,
            );
            offset += 64;
        }

        let ymm_end = if len >= 32 { len - 31 } else { 0 };
        while offset < ymm_end {
            let ymm = _mm256_loadu_si256(ptr.add(offset) as *const __m256i);
            let cmp = _mm256_cmpeq_epi8(ymm, newline);
            let mask = _mm256_movemask_epi8(cmp) as u32 as u64;
            extract_positions_from_mask(
                mask,
                global_base + offset as u64,
                data_total_len,
                line_starts,
            );
            offset += 32;
        }

        while offset < len {
            if *ptr.add(offset) == b'\n' {
                let next_line = global_base + (offset as u64) + 1;
                if next_line < data_total_len {
                    line_starts.push(next_line);
                }
            }
            offset += 1;
        }
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[inline]
unsafe fn avx2_cmp_64(ptr: *const u8, target: std::arch::x86_64::__m256i) -> u64 {
    unsafe {
        use std::arch::x86_64::*;
        let lo = _mm256_loadu_si256(ptr as *const __m256i);
        let hi = _mm256_loadu_si256(ptr.add(32) as *const __m256i);
        let cmp_lo = _mm256_cmpeq_epi8(lo, target);
        let cmp_hi = _mm256_cmpeq_epi8(hi, target);
        let mask_lo = _mm256_movemask_epi8(cmp_lo) as u32 as u64;
        let mask_hi = _mm256_movemask_epi8(cmp_hi) as u32 as u64;
        mask_lo | (mask_hi << 32)
    }
}

#[inline(always)]
fn extract_positions_from_mask(
    mask: u64,
    base_offset: u64,
    data_total_len: u64,
    line_starts: &mut Vec<u64>,
) {
    let mut m = mask;
    while m != 0 {
        let pos = m.trailing_zeros() as u64;
        let next_line_start = base_offset + pos + 1;
        if next_line_start < data_total_len {
            line_starts.push(next_line_start);
        }
        m &= m.wrapping_sub(1);
    }
}

fn scan_region_scalar(
    data: &[u8],
    global_base: u64,
    data_total_len: u64,
    line_starts: &mut Vec<u64>,
) {
    for (i, &byte) in data.iter().enumerate() {
        if byte == b'\n' {
            let next_line = global_base + (i as u64) + 1;
            if next_line < data_total_len {
                line_starts.push(next_line);
            }
        }
    }
}

#[allow(dead_code)]
pub fn count_newlines_in_region(data: &[u8]) -> u64 {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx512f") && is_x86_feature_detected!("avx512bw") {
            return unsafe { count_newlines_avx512(data) };
        }
        if is_x86_feature_detected!("avx2") {
            return unsafe { count_newlines_avx2(data) };
        }
    }
    count_newlines_scalar(data)
}

#[allow(dead_code)]
fn count_newlines_scalar(data: &[u8]) -> u64 {
    data.iter().filter(|&&b| b == b'\n').count() as u64
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f,avx512bw")]
#[allow(dead_code)]
unsafe fn count_newlines_avx512(data: &[u8]) -> u64 {
    unsafe {
        use std::arch::x86_64::*;

        let newline = _mm512_set1_epi8(0x0Au8 as i8);
        let ptr = data.as_ptr();
        let len = data.len();
        let mut count = 0u64;
        let mut offset = 0usize;

        let unrolled_end = if len >= 256 { len - 255 } else { 0 };
        while offset < unrolled_end {
            let m0 =
                _mm512_cmpeq_epi8_mask(_mm512_loadu_si512(ptr.add(offset) as *const _), newline);
            let m1 = _mm512_cmpeq_epi8_mask(
                _mm512_loadu_si512(ptr.add(offset + 64) as *const _),
                newline,
            );
            let m2 = _mm512_cmpeq_epi8_mask(
                _mm512_loadu_si512(ptr.add(offset + 128) as *const _),
                newline,
            );
            let m3 = _mm512_cmpeq_epi8_mask(
                _mm512_loadu_si512(ptr.add(offset + 192) as *const _),
                newline,
            );
            count += m0.count_ones() as u64
                + m1.count_ones() as u64
                + m2.count_ones() as u64
                + m3.count_ones() as u64;
            offset += 256;
        }

        let single_end = if len >= 64 { len - 63 } else { 0 };
        while offset < single_end {
            let m =
                _mm512_cmpeq_epi8_mask(_mm512_loadu_si512(ptr.add(offset) as *const _), newline);
            count += m.count_ones() as u64;
            offset += 64;
        }

        while offset < len {
            if *ptr.add(offset) == b'\n' {
                count += 1;
            }
            offset += 1;
        }

        count
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[allow(dead_code)]
unsafe fn count_newlines_avx2(data: &[u8]) -> u64 {
    unsafe {
        use std::arch::x86_64::*;

        let newline = _mm256_set1_epi8(0x0Au8 as i8);
        let ptr = data.as_ptr();
        let len = data.len();
        let mut count = 0u64;
        let mut offset = 0usize;

        let unrolled_end = if len >= 256 { len - 255 } else { 0 };
        while offset < unrolled_end {
            let m0 = avx2_cmp_64(ptr.add(offset), newline);
            let m1 = avx2_cmp_64(ptr.add(offset + 64), newline);
            let m2 = avx2_cmp_64(ptr.add(offset + 128), newline);
            let m3 = avx2_cmp_64(ptr.add(offset + 192), newline);
            count += m0.count_ones() as u64
                + m1.count_ones() as u64
                + m2.count_ones() as u64
                + m3.count_ones() as u64;
            offset += 256;
        }

        let single_end = if len >= 64 { len - 63 } else { 0 };
        while offset < single_end {
            let m = avx2_cmp_64(ptr.add(offset), newline);
            count += m.count_ones() as u64;
            offset += 64;
        }

        let ymm_end = if len >= 32 { len - 31 } else { 0 };
        while offset < ymm_end {
            let ymm = _mm256_loadu_si256(ptr.add(offset) as *const __m256i);
            let cmp = _mm256_cmpeq_epi8(ymm, newline);
            let mask = _mm256_movemask_epi8(cmp) as u32;
            count += mask.count_ones() as u64;
            offset += 32;
        }

        while offset < len {
            if *ptr.add(offset) == b'\n' {
                count += 1;
            }
            offset += 1;
        }

        count
    }
}

pub fn simd_capability() -> &'static str {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx512f") && is_x86_feature_detected!("avx512bw") {
            return "AVX-512 (512-bit, 64 bytes/compare)";
        }
        if is_x86_feature_detected!("avx2") {
            return "AVX2 (256-bit, 32 bytes/compare)";
        }
    }
    "Scalar (no SIMD)"
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scan_newlines_reference(data: &[u8]) -> Vec<u64> {
        let mut result = vec![0u64];
        for (i, &b) in data.iter().enumerate() {
            if b == b'\n' && (i + 1) < data.len() {
                result.push((i + 1) as u64);
            }
        }
        result
    }

    #[test]
    fn test_scan_empty() {
        let result = scan_newlines(b"");
        assert_eq!(result, vec![0]);
    }

    #[test]
    fn test_scan_no_newlines() {
        let result = scan_newlines(b"hello world");
        assert_eq!(result, vec![0]);
    }

    #[test]
    fn test_scan_single_newline() {
        let result = scan_newlines(b"hello\nworld");
        assert_eq!(result, vec![0, 6]);
    }

    #[test]
    fn test_scan_multiple_newlines() {
        let data = b"line1\nline2\nline3\n";
        let result = scan_newlines(data);
        let expected = scan_newlines_reference(data);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_scan_large_block() {
        let mut data = Vec::new();
        for i in 0..100 {
            data.extend_from_slice(format!("log line number {} with some content\n", i).as_bytes());
        }
        let result = scan_newlines(&data);
        let expected = scan_newlines_reference(&data);
        assert_eq!(result, expected, "SIMD scan mismatch vs reference");
    }

    #[test]
    fn test_scan_consecutive_newlines() {
        let data = b"a\n\n\nb\n";
        let result = scan_newlines(data);
        let expected = scan_newlines_reference(data);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_scan_boundary_alignment() {
        let mut data = vec![b'x'; 64];
        data[63] = b'\n';
        data.extend_from_slice(b"next line\n");
        let result = scan_newlines(&data);
        let expected = scan_newlines_reference(&data);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_scan_parallel_matches_sequential() {
        let mut data = Vec::new();
        for i in 0..1000 {
            data.extend_from_slice(
                format!(
                    "2025-02-12T10:31:45Z INFO api-server request_id=test{}\n",
                    i
                )
                .as_bytes(),
            );
        }
        let seq = scan_newlines(&data);
        let par = scan_newlines_parallel(&data, 4);
        assert_eq!(seq, par, "Parallel scan must match sequential scan");
    }

    #[test]
    fn test_count_newlines_empty() {
        assert_eq!(count_newlines_in_region(b""), 0);
    }

    #[test]
    fn test_count_newlines_none() {
        assert_eq!(count_newlines_in_region(b"hello world"), 0);
    }

    #[test]
    fn test_count_newlines_single() {
        assert_eq!(count_newlines_in_region(b"hello\nworld"), 1);
    }

    #[test]
    fn test_count_newlines_multiple() {
        assert_eq!(count_newlines_in_region(b"a\nb\nc\n"), 3);
    }

    #[test]
    fn test_count_newlines_consecutive() {
        assert_eq!(count_newlines_in_region(b"a\n\n\nb\n"), 4);
    }

    #[test]
    fn test_count_newlines_large_block() {
        let mut data = Vec::new();
        for i in 0..1000 {
            data.extend_from_slice(format!("log line number {}\n", i).as_bytes());
        }
        assert_eq!(count_newlines_in_region(&data), 1000);
    }

    #[test]
    fn test_count_newlines_boundary_256() {
        let mut data = vec![b'x'; 256];
        data[63] = b'\n';
        data[127] = b'\n';
        data[191] = b'\n';
        data[255] = b'\n';
        assert_eq!(count_newlines_in_region(&data), 4);
    }

    #[test]
    fn test_count_matches_scan_positions() {
        let mut data = Vec::new();
        for i in 0..500 {
            data.extend_from_slice(
                format!(
                    "2025-02-12T10:31:45Z INFO api-server request_id=test{}\n",
                    i
                )
                .as_bytes(),
            );
        }
        let scan_result = scan_newlines(&data);
        let newline_count = count_newlines_in_region(&data);

        assert_eq!(scan_result.len() as u64, newline_count);
    }
}
