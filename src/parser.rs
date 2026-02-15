use crate::data::{LogBatch, LogLevel};

#[inline(always)]
fn parse_timestamp_fast(b: &[u8]) -> u64 {
    if b.len() < 20 {
        return 0;
    }

    let year = swar_parse_4(b, 0) as i64;
    let month = swar_parse_2(b, 5);
    let day = swar_parse_2(b, 8);
    let hms = swar_parse_hms(b, 11);
    let hour = hms / 10000;
    let min = (hms / 100) % 100;
    let sec = hms % 100;

    let mut days = 0i64;
    let y = year - 1970;
    days += y * 365;

    if year > 1970 {
        days += (year - 1969) / 4;
        days -= (year - 1901) / 100;
        days += (year - 1601) / 400;
    }

    const MONTH_DAYS: [u32; 12] = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];
    if (1..=12).contains(&month) {
        days += MONTH_DAYS[(month - 1) as usize] as i64;
        if month > 2 && is_leap_year(year) {
            days += 1;
        }
    }

    days += (day as i64) - 1;

    let total_secs = days * 86400 + hour as i64 * 3600 + min as i64 * 60 + sec as i64;
    if total_secs < 0 { 0 } else { total_secs as u64 }
}

#[inline(always)]
fn swar_parse_4(b: &[u8], off: usize) -> u32 {
    let chunk = u32::from_le_bytes([b[off], b[off + 1], b[off + 2], b[off + 3]]);
    let digits = chunk.wrapping_sub(0x30303030);
    let d0 = digits & 0xFF;
    let d1 = (digits >> 8) & 0xFF;
    let d2 = (digits >> 16) & 0xFF;
    let d3 = (digits >> 24) & 0xFF;
    d0 * 1000 + d1 * 100 + d2 * 10 + d3
}

#[inline(always)]
fn swar_parse_2(b: &[u8], off: usize) -> u32 {
    let chunk = u16::from_le_bytes([b[off], b[off + 1]]);
    let digits = chunk.wrapping_sub(0x3030);
    let d0 = (digits & 0xFF) as u32;
    let d1 = ((digits >> 8) & 0xFF) as u32;
    d0 * 10 + d1
}

#[inline(always)]
fn swar_parse_hms(b: &[u8], off: usize) -> u32 {
    let hh = swar_parse_2(b, off);
    let mm = swar_parse_2(b, off + 3);
    let ss = swar_parse_2(b, off + 6);
    hh * 10000 + mm * 100 + ss
}

#[inline(always)]
fn is_leap_year(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0)
}

#[inline(always)]
fn find_first_3_spaces(line: &[u8]) -> [usize; 3] {
    let mut result = [usize::MAX; 3];
    let mut start = 0;
    for slot in &mut result {
        match memchr::memchr(b' ', &line[start..]) {
            Some(pos) => {
                *slot = start + pos;
                start = start + pos + 1;
            }
            None => break,
        }
    }
    result
}

#[inline]
pub fn parse_line(line: &[u8], index: usize, batch: &mut LogBatch, base_offset: u64) {
    let spaces = find_first_3_spaces(line);
    let space1 = spaces[0];

    if space1 == usize::MAX {
        batch.timestamps[index] = 0;
        batch.levels[index] = LogLevel::Unknown;
        batch.component_offsets[index] = base_offset;
        batch.component_lens[index] = 0;
        batch.message_offsets[index] = base_offset;
        batch.message_lens[index] = line.len() as u32;
        return;
    }

    batch.timestamps[index] = parse_timestamp_fast(&line[..space1]);

    let after_ts = space1 + 1;
    let space2 = spaces[1];

    if space2 == usize::MAX {
        batch.levels[index] = LogLevel::from_bytes(&line[after_ts..]);
        batch.component_offsets[index] = base_offset + line.len() as u64;
        batch.component_lens[index] = 0;
        batch.message_offsets[index] = base_offset + line.len() as u64;
        batch.message_lens[index] = 0;
        return;
    }

    batch.levels[index] = LogLevel::from_bytes(&line[after_ts..space2]);

    let after_level = space2 + 1;
    let space3 = spaces[2];

    if space3 == usize::MAX {
        batch.component_offsets[index] = base_offset + after_level as u64;
        batch.component_lens[index] = (line.len() - after_level) as u32;
        batch.message_offsets[index] = base_offset + line.len() as u64;
        batch.message_lens[index] = 0;
        return;
    }

    batch.component_offsets[index] = base_offset + after_level as u64;
    batch.component_lens[index] = (space3 - after_level) as u32;

    let after_component = space3 + 1;
    let msg_len = if after_component < line.len() {
        line.len() - after_component
    } else {
        0
    };
    batch.message_offsets[index] = base_offset + after_component as u64;
    batch.message_lens[index] = msg_len as u32;
}

pub fn parse_lines_range(
    data: &[u8],
    line_starts: &[u64],
    start_idx: usize,
    end_idx: usize,
    batch: &mut LogBatch,
) {
    let num_lines = line_starts.len();
    for i in start_idx..end_idx {
        let line_start = line_starts[i] as usize;
        let line_end = if i + 1 < num_lines {
            let next = line_starts[i + 1] as usize;

            if next > 0 && next <= data.len() && data[next - 1] == b'\n' {
                next - 1
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
        parse_line(line, i, batch, line_start as u64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_timestamp() {
        let ts = parse_timestamp_fast(b"2025-02-12T10:31:45Z");

        assert_eq!(ts, 1739356305);
    }

    #[test]
    fn test_parse_timestamp_epoch() {
        let ts = parse_timestamp_fast(b"1970-01-01T00:00:00Z");
        assert_eq!(ts, 0);
    }

    #[test]
    fn test_parse_timestamp_short() {
        let ts = parse_timestamp_fast(b"short");
        assert_eq!(ts, 0);
    }

    #[test]
    fn test_parse_line_full() {
        let line = b"2025-02-12T10:31:45Z INFO api-server request_id=abc123 latency_ms=42";
        let data_ptr = line.as_ptr();
        let mut batch = crate::data::LogBatch::new(1, data_ptr);

        parse_line(line, 0, &mut batch, 0);

        assert_eq!(batch.timestamps[0], 1739356305);
        assert_eq!(batch.levels[0], LogLevel::Info);
        unsafe {
            assert_eq!(batch.component(0), "api-server");
            assert_eq!(batch.message(0), "request_id=abc123 latency_ms=42");
        }
    }

    #[test]
    fn test_parse_line_warn() {
        let line = b"2025-02-12T10:31:46Z WARN auth-service auth_failed user=john ip=192.168.1.1";
        let data_ptr = line.as_ptr();
        let mut batch = crate::data::LogBatch::new(1, data_ptr);

        parse_line(line, 0, &mut batch, 0);

        assert_eq!(batch.levels[0], LogLevel::Warn);
        unsafe {
            assert_eq!(batch.component(0), "auth-service");
        }
    }

    #[test]
    fn test_parse_line_error() {
        let line =
            b"2025-02-12T10:31:47Z ERROR database-pool connection_timeout retries=3 queue_size=512";
        let data_ptr = line.as_ptr();
        let mut batch = crate::data::LogBatch::new(1, data_ptr);

        parse_line(line, 0, &mut batch, 0);

        assert_eq!(batch.levels[0], LogLevel::Error);
        unsafe {
            assert_eq!(batch.component(0), "database-pool");
            assert_eq!(
                batch.message(0),
                "connection_timeout retries=3 queue_size=512"
            );
        }
    }

    #[test]
    fn test_find_first_3_spaces() {
        let result = find_first_3_spaces(b"a b c d");
        assert_eq!(result, [1, 3, 5]);

        let result = find_first_3_spaces(b"nospaces");
        assert_eq!(result, [usize::MAX, usize::MAX, usize::MAX]);

        let result = find_first_3_spaces(b"one space");
        assert_eq!(result[0], 3);
        assert_eq!(result[1], usize::MAX);
    }
}
