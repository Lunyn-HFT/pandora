use crate::structured::{FieldRef, StructuredBatch, well_known};

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[inline]
unsafe fn find_quote_mask_avx2(data: *const u8, len: usize) -> (u64, u64) {
    unsafe {
        use std::arch::x86_64::*;

        let quote = _mm256_set1_epi8(b'"' as i8);
        let backslash = _mm256_set1_epi8(b'\\' as i8);

        if len >= 64 {
            let lo = _mm256_loadu_si256(data as *const __m256i);
            let hi = _mm256_loadu_si256(data.add(32) as *const __m256i);

            let q_lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(lo, quote)) as u32 as u64;
            let q_hi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(hi, quote)) as u32 as u64;
            let quote_mask = q_lo | (q_hi << 32);

            let b_lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(lo, backslash)) as u32 as u64;
            let b_hi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(hi, backslash)) as u32 as u64;
            let bs_mask = b_lo | (b_hi << 32);

            (quote_mask, bs_mask)
        } else if len >= 32 {
            let lo = _mm256_loadu_si256(data as *const __m256i);
            let q_lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(lo, quote)) as u32 as u64;
            let b_lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(lo, backslash)) as u32 as u64;
            (q_lo, b_lo)
        } else {
            (0, 0)
        }
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f,avx512bw")]
#[inline]
unsafe fn find_quote_mask_avx512(data: *const u8, len: usize) -> (u64, u64) {
    unsafe {
        use std::arch::x86_64::*;
        if len >= 64 {
            let chunk = _mm512_loadu_si512(data as *const _);
            let quote = _mm512_set1_epi8(b'"' as i8);
            let backslash = _mm512_set1_epi8(b'\\' as i8);

            let q_mask = _mm512_cmpeq_epi8_mask(chunk, quote);
            let b_mask = _mm512_cmpeq_epi8_mask(chunk, backslash);
            (q_mask, b_mask)
        } else {
            (0, 0)
        }
    }
}

#[inline(always)]
fn resolve_escapes(quote_mask: u64, bs_mask: u64) -> u64 {
    if bs_mask == 0 {
        return quote_mask;
    }

    let mut result = quote_mask;
    let mut m = quote_mask;
    while m != 0 {
        let pos = m.trailing_zeros();
        if pos > 0 {
            let mut bs_count = 0u32;
            let mut check_pos = pos - 1;
            loop {
                if (bs_mask >> check_pos) & 1 == 1 {
                    bs_count += 1;
                    if check_pos == 0 {
                        break;
                    }
                    check_pos -= 1;
                } else {
                    break;
                }
            }
            if bs_count & 1 == 1 {
                result &= !(1u64 << pos);
            }
        }
        m &= m.wrapping_sub(1);
    }
    result
}

#[inline]
pub fn parse_json_line(line: &[u8], base_offset: u64, batch: &mut StructuredBatch) {
    let len = line.len();
    if len < 2 {
        return;
    }

    let mut i = 0;
    while i < len && line[i] != b'{' {
        i += 1;
    }
    if i >= len {
        return;
    }
    i += 1; // skip '{'

    batch.begin_record(base_offset, len as u32);
    let record_field_base = batch.fields.len() as u32;

    loop {
        while i < len && is_json_whitespace(line[i]) {
            i += 1;
        }

        if i >= len || line[i] == b'}' {
            break;
        }

        if line[i] == b',' {
            i += 1;
            continue;
        }

        if line[i] != b'"' {
            while i < len && line[i] != b',' && line[i] != b'}' {
                i += 1;
            }
            continue;
        }

        let key_start = i + 1; // after opening quote
        i += 1;
        while i < len && line[i] != b'"' {
            if line[i] == b'\\' {
                i += 1; // skip escaped char
            }
            i += 1;
        }
        let key_end = i;
        if i < len {
            i += 1; // skip closing quote
        }

        while i < len && is_json_whitespace(line[i]) {
            i += 1;
        }
        if i < len && line[i] == b':' {
            i += 1;
        }
        while i < len && is_json_whitespace(line[i]) {
            i += 1;
        }

        let (val_start, val_end) = parse_json_value(line, &mut i);

        let field_idx = batch.fields.len() as u32;

        let field = FieldRef {
            key_offset: base_offset + key_start as u64,
            key_len: (key_end - key_start) as u32,
            val_offset: base_offset + val_start as u64,
            val_len: (val_end - val_start) as u32,
        };

        batch.push_field(field);

        let key_bytes = &line[key_start..key_end];
        match well_known::classify_key(key_bytes) {
            well_known::WellKnownKind::Timestamp => {
                batch.set_well_known_timestamp(field_idx);
            }
            well_known::WellKnownKind::Level => {
                batch.set_well_known_level(field_idx);
            }
            well_known::WellKnownKind::Message => {
                batch.set_well_known_message(field_idx);
            }
            well_known::WellKnownKind::Component => {
                batch.set_well_known_component(field_idx);
            }
            well_known::WellKnownKind::Other => {}
        }

        while i < len && is_json_whitespace(line[i]) {
            i += 1;
        }
        if i < len && line[i] == b',' {
            i += 1;
        }
    }

    let total_fields = batch.fields.len() as u32 - record_field_base;
    if total_fields > 0 {
        let _ = total_fields; // already set above
    }

    batch.end_record();
}

#[inline]
fn parse_json_value(line: &[u8], i: &mut usize) -> (usize, usize) {
    let len = line.len();
    if *i >= len {
        return (*i, *i);
    }

    match line[*i] {
        b'"' => {
            let val_start = *i + 1;
            *i += 1;
            while *i < len && line[*i] != b'"' {
                if line[*i] == b'\\' {
                    *i += 1; // skip escaped char
                }
                *i += 1;
            }
            let val_end = *i;
            if *i < len {
                *i += 1; // skip closing quote
            }
            (val_start, val_end)
        }
        b'{' => {
            let val_start = *i;
            let mut depth = 1i32;
            *i += 1;
            while *i < len && depth > 0 {
                match line[*i] {
                    b'{' => depth += 1,
                    b'}' => depth -= 1,
                    b'"' => {
                        *i += 1;
                        while *i < len && line[*i] != b'"' {
                            if line[*i] == b'\\' {
                                *i += 1;
                            }
                            *i += 1;
                        }
                    }
                    _ => {}
                }
                *i += 1;
            }
            (val_start, *i)
        }
        b'[' => {
            let val_start = *i;
            let mut depth = 1i32;
            *i += 1;
            while *i < len && depth > 0 {
                match line[*i] {
                    b'[' => depth += 1,
                    b']' => depth -= 1,
                    b'"' => {
                        *i += 1;
                        while *i < len && line[*i] != b'"' {
                            if line[*i] == b'\\' {
                                *i += 1;
                            }
                            *i += 1;
                        }
                    }
                    _ => {}
                }
                *i += 1;
            }
            (val_start, *i)
        }
        _ => {
            let val_start = *i;
            while *i < len && !is_json_value_terminator(line[*i]) {
                *i += 1;
            }
            let mut val_end = *i;
            while val_end > val_start && is_json_whitespace(line[val_end - 1]) {
                val_end -= 1;
            }
            (val_start, val_end)
        }
    }
}

#[inline(always)]
fn is_json_whitespace(b: u8) -> bool {
    b == b' ' || b == b'\t' || b == b'\r' || b == b'\n'
}

#[inline(always)]
fn is_json_value_terminator(b: u8) -> bool {
    b == b',' || b == b'}' || b == b']' || is_json_whitespace(b)
}

#[allow(dead_code)]
#[inline]
pub fn find_string_end_simd(data: &[u8], start: usize) -> usize {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx512f") && is_x86_feature_detected!("avx512bw") {
            return unsafe { find_string_end_avx512(data, start) };
        }
        if is_x86_feature_detected!("avx2") {
            return unsafe { find_string_end_avx2(data, start) };
        }
    }
    find_string_end_scalar(data, start)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn find_string_end_avx2(data: &[u8], start: usize) -> usize {
    let mut pos = start;
    let len = data.len();

    while pos + 64 <= len {
        let (q_mask, bs_mask) = unsafe { find_quote_mask_avx2(data.as_ptr().add(pos), 64) };
        let real_quotes = resolve_escapes(q_mask, bs_mask);
        if real_quotes != 0 {
            return pos + real_quotes.trailing_zeros() as usize;
        }
        pos += 64;
    }

    find_string_end_scalar(data, pos)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f,avx512bw")]
unsafe fn find_string_end_avx512(data: &[u8], start: usize) -> usize {
    let mut pos = start;
    let len = data.len();

    while pos + 64 <= len {
        let (q_mask, bs_mask) = unsafe { find_quote_mask_avx512(data.as_ptr().add(pos), 64) };
        let real_quotes = resolve_escapes(q_mask, bs_mask);
        if real_quotes != 0 {
            return pos + real_quotes.trailing_zeros() as usize;
        }
        pos += 64;
    }

    find_string_end_scalar(data, pos)
}

fn find_string_end_scalar(data: &[u8], start: usize) -> usize {
    let mut pos = start;
    while pos < data.len() {
        match data[pos] {
            b'"' => return pos,
            b'\\' => pos += 2, // skip escaped char
            _ => pos += 1,
        }
    }
    data.len()
}

pub fn parse_json_lines_range(
    data: &[u8],
    line_starts: &[u64],
    start_idx: usize,
    end_idx: usize,
    batch: &mut StructuredBatch,
) {
    let num_lines = line_starts.len();

    for i in start_idx..end_idx {
        let line_start = line_starts[i] as usize;
        let line_end = if i + 1 < num_lines {
            let next = line_starts[i + 1] as usize;
            if next > 0 && next <= data.len() && data[next - 1] == b'\n' {
                if next > 1 && data[next - 2] == b'\r' {
                    next - 2
                } else {
                    next - 1
                }
            } else {
                next
            }
        } else {
            data.len()
        };

        if line_start >= data.len() || line_start >= line_end {
            continue;
        }

        let line = &data[line_start..line_end];

        if line.iter().all(|&b| is_json_whitespace(b)) {
            continue;
        }

        parse_json_line(line, line_start as u64, batch);
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[allow(dead_code)]
/// # Safety
/// This function uses AVX2 intrinsics and requires the data pointer to be valid for the given length.
/// The caller must ensure AVX2 is available on the target CPU.
pub unsafe fn structural_scan_avx2(
    data: &[u8],
    global_base: u64,
    line_starts: &mut Vec<u64>,
    brace_positions: &mut Vec<u64>,
) {
    unsafe {
        use std::arch::x86_64::*;

        let newline = _mm256_set1_epi8(b'\n' as i8);
        let open_brace = _mm256_set1_epi8(b'{' as i8);
        let ptr = data.as_ptr();
        let len = data.len();
        let mut offset = 0usize;

        while offset + 64 <= len {
            let lo = _mm256_loadu_si256(ptr.add(offset) as *const __m256i);
            let hi = _mm256_loadu_si256(ptr.add(offset + 32) as *const __m256i);

            let nl_lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(lo, newline)) as u32 as u64;
            let nl_hi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(hi, newline)) as u32 as u64;
            let nl_mask = nl_lo | (nl_hi << 32);

            let ob_lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(lo, open_brace)) as u32 as u64;
            let ob_hi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(hi, open_brace)) as u32 as u64;
            let ob_mask = ob_lo | (ob_hi << 32);

            extract_mask_positions(nl_mask, global_base + offset as u64, line_starts);
            extract_mask_positions_raw(ob_mask, global_base + offset as u64, brace_positions);

            offset += 64;
        }

        while offset < len {
            let b = *ptr.add(offset);
            if b == b'\n' {
                line_starts.push(global_base + offset as u64 + 1);
            } else if b == b'{' {
                brace_positions.push(global_base + offset as u64);
            }
            offset += 1;
        }
    }
}

#[inline(always)]
#[allow(dead_code)]
fn extract_mask_positions(mask: u64, base: u64, output: &mut Vec<u64>) {
    let mut m = mask;
    while m != 0 {
        let pos = m.trailing_zeros() as u64;
        output.push(base + pos + 1); // position AFTER the newline
        m &= m.wrapping_sub(1);
    }
}

#[inline(always)]
#[allow(dead_code)]
fn extract_mask_positions_raw(mask: u64, base: u64, output: &mut Vec<u64>) {
    let mut m = mask;
    while m != 0 {
        let pos = m.trailing_zeros() as u64;
        output.push(base + pos);
        m &= m.wrapping_sub(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_batch(data: &[u8]) -> StructuredBatch {
        StructuredBatch::with_capacity(16, 64, data.as_ptr())
    }

    #[test]
    fn test_parse_simple_json_line() {
        let line = br#"{"level":"info","msg":"hello world","ts":"2025-02-12T10:31:45Z"}"#;
        let mut batch = make_batch(line);

        parse_json_line(line, 0, &mut batch);

        assert_eq!(batch.len, 1);
        assert_eq!(batch.field_count(0), 3);

        unsafe {
            let fields = batch.record_fields(0);
            assert_eq!(batch.field_key(&fields[0]), "level");
            assert_eq!(batch.field_value(&fields[0]), "info");
            assert_eq!(batch.field_key(&fields[1]), "msg");
            assert_eq!(batch.field_value(&fields[1]), "hello world");
            assert_eq!(batch.field_key(&fields[2]), "ts");
            assert_eq!(batch.field_value(&fields[2]), "2025-02-12T10:31:45Z");
        }
    }

    #[test]
    fn test_well_known_detection() {
        let line = br#"{"timestamp":"2025-02-12T10:31:45Z","level":"error","message":"disk full","component":"storage"}"#;
        let mut batch = make_batch(line);

        parse_json_line(line, 0, &mut batch);

        assert_eq!(batch.len, 1);
        unsafe {
            assert_eq!(batch.timestamp_value(0), Some("2025-02-12T10:31:45Z"));
            assert_eq!(batch.level_value(0), Some("error"));
            assert_eq!(batch.message_value(0), Some("disk full"));
            assert_eq!(batch.component_value(0), Some("storage"));
        }
    }

    #[test]
    fn test_parse_json_with_numbers() {
        let line = br#"{"latency_ms":42,"status":200,"success":true}"#;
        let mut batch = make_batch(line);

        parse_json_line(line, 0, &mut batch);

        assert_eq!(batch.len, 1);
        assert_eq!(batch.field_count(0), 3);

        unsafe {
            let fields = batch.record_fields(0);
            assert_eq!(batch.field_key(&fields[0]), "latency_ms");
            assert_eq!(batch.field_value(&fields[0]), "42");
            assert_eq!(batch.field_key(&fields[1]), "status");
            assert_eq!(batch.field_value(&fields[1]), "200");
            assert_eq!(batch.field_key(&fields[2]), "success");
            assert_eq!(batch.field_value(&fields[2]), "true");
        }
    }

    #[test]
    fn test_parse_json_with_nested_object() {
        let line = br#"{"msg":"hello","context":{"user":"john","ip":"10.0.0.1"},"level":"info"}"#;
        let mut batch = make_batch(line);

        parse_json_line(line, 0, &mut batch);

        assert_eq!(batch.len, 1);
        assert_eq!(batch.field_count(0), 3);

        unsafe {
            let fields = batch.record_fields(0);
            assert_eq!(batch.field_key(&fields[0]), "msg");
            assert_eq!(batch.field_value(&fields[0]), "hello");
            assert_eq!(batch.field_key(&fields[1]), "context");
            let ctx = batch.field_value(&fields[1]);
            assert!(ctx.starts_with('{'));
            assert!(ctx.contains("john"));
            assert_eq!(batch.field_key(&fields[2]), "level");
            assert_eq!(batch.field_value(&fields[2]), "info");
        }
    }

    #[test]
    fn test_parse_json_with_array() {
        let line = br#"{"tags":["web","prod"],"msg":"deploy"}"#;
        let mut batch = make_batch(line);

        parse_json_line(line, 0, &mut batch);

        assert_eq!(batch.len, 1);
        assert_eq!(batch.field_count(0), 2);

        unsafe {
            let fields = batch.record_fields(0);
            assert_eq!(batch.field_key(&fields[0]), "tags");
            let tags = batch.field_value(&fields[0]);
            assert!(tags.starts_with('['));
            assert!(tags.contains("web"));
        }
    }

    #[test]
    fn test_parse_json_with_escaped_quotes() {
        let line = br#"{"msg":"said \"hello\"","level":"info"}"#;
        let mut batch = make_batch(line);

        parse_json_line(line, 0, &mut batch);

        assert_eq!(batch.len, 1);
        assert_eq!(batch.field_count(0), 2);

        unsafe {
            let fields = batch.record_fields(0);
            assert_eq!(batch.field_key(&fields[0]), "msg");
            let msg = batch.field_value(&fields[0]);
            assert!(msg.contains("hello"));
        }
    }

    #[test]
    fn test_parse_json_null_value() {
        let line = br#"{"msg":"test","extra":null}"#;
        let mut batch = make_batch(line);

        parse_json_line(line, 0, &mut batch);

        assert_eq!(batch.len, 1);
        assert_eq!(batch.field_count(0), 2);

        unsafe {
            let fields = batch.record_fields(0);
            assert_eq!(batch.field_value(&fields[1]), "null");
        }
    }

    #[test]
    fn test_parse_ndjson_multiple_lines() {
        let data = br#"{"level":"info","msg":"request started"}
{"level":"warn","msg":"slow query"}
{"level":"error","msg":"connection lost"}
"#;
        let mut batch = make_batch(data);
        let line_starts: Vec<u64> = vec![0, 41, 77, data.len() as u64];

        parse_json_lines_range(data, &line_starts, 0, 3, &mut batch);

        assert_eq!(batch.len, 3);

        unsafe {
            assert_eq!(batch.level_value(0), Some("info"));
            assert_eq!(batch.level_value(1), Some("warn"));
            assert_eq!(batch.level_value(2), Some("error"));
            assert_eq!(batch.message_value(0), Some("request started"));
            assert_eq!(batch.message_value(1), Some("slow query"));
            assert_eq!(batch.message_value(2), Some("connection lost"));
        }
    }

    #[test]
    fn test_parse_json_empty_object() {
        let line = b"{}";
        let mut batch = make_batch(line);

        parse_json_line(line, 0, &mut batch);

        assert_eq!(batch.len, 1);
        assert_eq!(batch.field_count(0), 0);
    }

    #[test]
    fn test_parse_json_with_whitespace() {
        let line = br#"{ "level" : "info" , "msg" : "hello" }"#;
        let mut batch = make_batch(line);

        parse_json_line(line, 0, &mut batch);

        assert_eq!(batch.len, 1);
        assert_eq!(batch.field_count(0), 2);

        unsafe {
            let fields = batch.record_fields(0);
            assert_eq!(batch.field_key(&fields[0]), "level");
            assert_eq!(batch.field_value(&fields[0]), "info");
        }
    }

    #[test]
    fn test_find_string_end_scalar() {
        let data = br#"hello world" rest"#;
        let end = find_string_end_scalar(data, 0);
        assert_eq!(end, 11); // position of closing quote
    }

    #[test]
    fn test_find_string_end_with_escape() {
        let data = br#"hello \"world\"" rest"#;
        let end = find_string_end_scalar(data, 0);
        assert_eq!(data[end], b'"');
    }

    #[test]
    fn test_resolve_escapes_no_backslash() {
        let result = resolve_escapes(0b1010, 0);
        assert_eq!(result, 0b1010);
    }

    #[test]
    fn test_resolve_escapes_with_backslash() {
        let result = resolve_escapes(0b10, 0b01);
        assert_eq!(result, 0); // quote at position 1 is escaped by backslash at position 0
    }

    #[test]
    fn test_base_offset_propagation() {
        let line = br#"{"key":"value"}"#;
        let base = 1000u64;
        let mut batch = make_batch(line);

        parse_json_line(line, base, &mut batch);

        assert_eq!(batch.len, 1);
        let fields = batch.record_fields(0);
        assert_eq!(fields[0].key_offset, base + 2); // "key" starts at offset 2 in line
        assert_eq!(fields[0].val_offset, base + 8); // "value" starts at offset 8
    }
}
