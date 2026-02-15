use crate::structured::{FieldRef, StructuredBatch, well_known};

#[inline]
pub fn parse_logfmt_line(line: &[u8], base_offset: u64, batch: &mut StructuredBatch) {
    let len = line.len();
    if len == 0 {
        return;
    }

    batch.begin_record(base_offset, len as u32);

    let mut i = 0;

    loop {
        while i < len && line[i] == b' ' {
            i += 1;
        }
        if i >= len {
            break;
        }

        let key_start = i;
        while i < len && line[i] != b'=' && line[i] != b' ' {
            i += 1;
        }
        let key_end = i;

        if i >= len || line[i] != b'=' {
            if key_end > key_start {
                let field_idx = batch.fields.len() as u32;
                batch.push_field(FieldRef {
                    key_offset: base_offset + key_start as u64,
                    key_len: (key_end - key_start) as u32,
                    val_offset: base_offset + key_end as u64,
                    val_len: 0,
                });

                let key_bytes = &line[key_start..key_end];
                classify_and_set(key_bytes, field_idx, batch);
            }
            continue;
        }

        i += 1;

        let (val_start, val_end) = if i < len && line[i] == b'"' {
            i += 1; // skip opening quote
            let vs = i;
            while i < len && line[i] != b'"' {
                if line[i] == b'\\' {
                    i += 1; // skip escaped char
                }
                i += 1;
            }
            let ve = i;
            if i < len {
                i += 1; // skip closing quote
            }
            (vs, ve)
        } else {
            let vs = i;
            while i < len && line[i] != b' ' {
                i += 1;
            }
            (vs, i)
        };

        let field_idx = batch.fields.len() as u32;
        batch.push_field(FieldRef {
            key_offset: base_offset + key_start as u64,
            key_len: (key_end - key_start) as u32,
            val_offset: base_offset + val_start as u64,
            val_len: (val_end - val_start) as u32,
        });

        let key_bytes = &line[key_start..key_end];
        classify_and_set(key_bytes, field_idx, batch);
    }

    batch.end_record();
}

#[inline]
fn classify_and_set(key_bytes: &[u8], field_idx: u32, batch: &mut StructuredBatch) {
    match well_known::classify_key(key_bytes) {
        well_known::WellKnownKind::Timestamp => batch.set_well_known_timestamp(field_idx),
        well_known::WellKnownKind::Level => batch.set_well_known_level(field_idx),
        well_known::WellKnownKind::Message => batch.set_well_known_message(field_idx),
        well_known::WellKnownKind::Component => batch.set_well_known_component(field_idx),
        well_known::WellKnownKind::Other => {}
    }
}

pub fn parse_logfmt_lines_range(
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

        if line.iter().all(|&b| b == b' ' || b == b'\t') {
            continue;
        }

        parse_logfmt_line(line, line_start as u64, batch);
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[allow(dead_code)]
/// # Safety
/// This function uses AVX2 intrinsics and requires the data pointer to be valid.
/// The caller must ensure AVX2 is available on the target CPU.
pub unsafe fn find_equals_avx2(data: &[u8]) -> Vec<usize> {
    unsafe {
        use std::arch::x86_64::*;

        let equals = _mm256_set1_epi8(b'=' as i8);
        let ptr = data.as_ptr();
        let len = data.len();
        let mut positions = Vec::with_capacity(len / 10);
        let mut offset = 0;

        while offset + 32 <= len {
            let chunk = _mm256_loadu_si256(ptr.add(offset) as *const __m256i);
            let cmp = _mm256_cmpeq_epi8(chunk, equals);
            let mut mask = _mm256_movemask_epi8(cmp) as u32;

            while mask != 0 {
                let pos = mask.trailing_zeros() as usize;
                positions.push(offset + pos);
                mask &= mask - 1;
            }
            offset += 32;
        }

        while offset < len {
            if *ptr.add(offset) == b'=' {
                positions.push(offset);
            }
            offset += 1;
        }

        positions
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_batch(data: &[u8]) -> StructuredBatch {
        StructuredBatch::with_capacity(16, 64, data.as_ptr())
    }

    #[test]
    fn test_parse_simple_logfmt() {
        let line = b"level=info msg=hello ts=2025-02-12T10:31:45Z";
        let mut batch = make_batch(line);

        parse_logfmt_line(line, 0, &mut batch);

        assert_eq!(batch.len, 1);
        assert_eq!(batch.field_count(0), 3);

        unsafe {
            let fields = batch.record_fields(0);
            assert_eq!(batch.field_key(&fields[0]), "level");
            assert_eq!(batch.field_value(&fields[0]), "info");
            assert_eq!(batch.field_key(&fields[1]), "msg");
            assert_eq!(batch.field_value(&fields[1]), "hello");
            assert_eq!(batch.field_key(&fields[2]), "ts");
            assert_eq!(batch.field_value(&fields[2]), "2025-02-12T10:31:45Z");
        }
    }

    #[test]
    fn test_parse_logfmt_quoted_value() {
        let line = br#"level=info msg="hello world" latency_ms=42"#;
        let mut batch = make_batch(line);

        parse_logfmt_line(line, 0, &mut batch);

        assert_eq!(batch.len, 1);
        assert_eq!(batch.field_count(0), 3);

        unsafe {
            let fields = batch.record_fields(0);
            assert_eq!(batch.field_value(&fields[1]), "hello world");
        }
    }

    #[test]
    fn test_parse_logfmt_well_known() {
        let line = b"ts=2025-02-12 level=error message=fail component=db";
        let mut batch = make_batch(line);

        parse_logfmt_line(line, 0, &mut batch);

        unsafe {
            assert_eq!(batch.timestamp_value(0), Some("2025-02-12"));
            assert_eq!(batch.level_value(0), Some("error"));
            assert_eq!(batch.message_value(0), Some("fail"));
            assert_eq!(batch.component_value(0), Some("db"));
        }
    }

    #[test]
    fn test_parse_logfmt_escaped_quote() {
        let line = br#"msg="said \"hello\"" level=info"#;
        let mut batch = make_batch(line);

        parse_logfmt_line(line, 0, &mut batch);

        assert_eq!(batch.len, 1);
        assert_eq!(batch.field_count(0), 2);

        unsafe {
            let fields = batch.record_fields(0);
            let msg = batch.field_value(&fields[0]);
            assert!(msg.contains("hello"));
        }
    }

    #[test]
    fn test_parse_logfmt_empty_value() {
        let line = b"key= other=value";
        let mut batch = make_batch(line);

        parse_logfmt_line(line, 0, &mut batch);

        assert_eq!(batch.len, 1);
        assert_eq!(batch.field_count(0), 2);

        unsafe {
            let fields = batch.record_fields(0);
            assert_eq!(batch.field_key(&fields[0]), "key");
            assert_eq!(batch.field_value(&fields[0]), "");
            assert_eq!(batch.field_key(&fields[1]), "other");
            assert_eq!(batch.field_value(&fields[1]), "value");
        }
    }

    #[test]
    fn test_parse_logfmt_multiple_lines() {
        let data = b"level=info msg=start\nlevel=warn msg=slow\nlevel=error msg=fail\n";
        let mut batch = make_batch(data);
        let line_starts: Vec<u64> = vec![0, 21, 41];

        parse_logfmt_lines_range(data, &line_starts, 0, 3, &mut batch);

        assert_eq!(batch.len, 3);

        unsafe {
            assert_eq!(batch.level_value(0), Some("info"));
            assert_eq!(batch.level_value(1), Some("warn"));
            assert_eq!(batch.level_value(2), Some("error"));
        }
    }

    #[test]
    fn test_parse_logfmt_base_offset() {
        let line = b"key=value";
        let base = 500u64;
        let mut batch = make_batch(line);

        parse_logfmt_line(line, base, &mut batch);

        let fields = batch.record_fields(0);
        assert_eq!(fields[0].key_offset, base);
        assert_eq!(fields[0].val_offset, base + 4);
    }
}
