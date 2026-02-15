use crate::structured::{FieldRef, StructuredBatch, well_known};

pub struct CsvHeader {
    pub columns: Vec<(u64, u32)>,
    pub well_known: Vec<well_known::WellKnownKind>,
}

impl CsvHeader {
    pub fn parse(data: &[u8]) -> Option<CsvHeader> {
        let line_end = memchr::memchr(b'\n', data).unwrap_or(data.len());
        let header_line = &data[..line_end];

        let header_line = if header_line.last() == Some(&b'\r') {
            &header_line[..header_line.len() - 1]
        } else {
            header_line
        };

        if header_line.is_empty() {
            return None;
        }

        let mut columns = Vec::new();
        let mut well_known_kinds = Vec::new();
        let mut pos = 0;

        for field in header_line.split(|&b| b == b',') {
            let field = trim_csv_field(field);
            let offset = unsafe { field.as_ptr().offset_from(data.as_ptr()) as u64 };
            let len = field.len() as u32;
            columns.push((offset, len));
            well_known_kinds.push(well_known::classify_key(field));
            pos += 1;
        }

        let _ = pos;

        Some(CsvHeader {
            columns,
            well_known: well_known_kinds,
        })
    }

    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }
}

#[inline]
pub fn parse_csv_line(
    line: &[u8],
    base_offset: u64,
    header: &CsvHeader,
    batch: &mut StructuredBatch,
) {
    if line.is_empty() {
        return;
    }

    batch.begin_record(base_offset, line.len() as u32);

    let mut col_idx = 0;
    let mut i = 0;
    let len = line.len();

    while i < len && col_idx < header.num_columns() {
        let (val_start, val_end) = parse_csv_field(line, &mut i);

        let (key_offset, key_len) = header.columns[col_idx];
        let field_idx = batch.fields.len() as u32;

        batch.push_field(FieldRef {
            key_offset,
            key_len,
            val_offset: base_offset + val_start as u64,
            val_len: (val_end - val_start) as u32,
        });

        match header.well_known[col_idx] {
            well_known::WellKnownKind::Timestamp => batch.set_well_known_timestamp(field_idx),
            well_known::WellKnownKind::Level => batch.set_well_known_level(field_idx),
            well_known::WellKnownKind::Message => batch.set_well_known_message(field_idx),
            well_known::WellKnownKind::Component => batch.set_well_known_component(field_idx),
            well_known::WellKnownKind::Other => {}
        }

        col_idx += 1;

        if i < len && line[i] == b',' {
            i += 1;
        }
    }

    batch.end_record();
}

#[inline]
fn parse_csv_field(line: &[u8], i: &mut usize) -> (usize, usize) {
    let len = line.len();

    if *i >= len {
        return (*i, *i);
    }

    if line[*i] == b'"' {
        *i += 1;
        let start = *i;
        while *i < len {
            if line[*i] == b'"' {
                if *i + 1 < len && line[*i + 1] == b'"' {
                    *i += 2;
                } else {
                    let end = *i;
                    *i += 1; // skip closing quote
                    return (start, end);
                }
            } else {
                *i += 1;
            }
        }
        (start, *i)
    } else {
        let start = *i;
        while *i < len && line[*i] != b',' && line[*i] != b'\n' && line[*i] != b'\r' {
            *i += 1;
        }
        (start, *i)
    }
}

pub fn parse_csv_lines_range(
    data: &[u8],
    line_starts: &[u64],
    start_idx: usize,
    end_idx: usize,
    header: &CsvHeader,
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
            let mut end = data.len();
            if end > 0 && data[end - 1] == b'\n' {
                end -= 1;
            }
            if end > 0 && data[end - 1] == b'\r' {
                end -= 1;
            }
            end
        };

        if line_start >= data.len() || line_start >= line_end {
            continue;
        }

        let line = &data[line_start..line_end];
        parse_csv_line(line, line_start as u64, header, batch);
    }
}

#[inline]
fn trim_csv_field(field: &[u8]) -> &[u8] {
    let mut start = 0;
    let mut end = field.len();
    while start < end && (field[start] == b' ' || field[start] == b'\t') {
        start += 1;
    }
    while end > start && (field[end - 1] == b' ' || field[end - 1] == b'\t') {
        end -= 1;
    }
    if end - start >= 2 && field[start] == b'"' && field[end - 1] == b'"' {
        start += 1;
        end -= 1;
    }
    &field[start..end]
}

pub fn header_end_offset(data: &[u8]) -> usize {
    match memchr::memchr(b'\n', data) {
        Some(pos) => pos + 1,
        None => data.len(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_batch(data: &[u8]) -> StructuredBatch {
        StructuredBatch::with_capacity(16, 64, data.as_ptr())
    }

    #[test]
    fn test_parse_csv_header() {
        let data = b"timestamp,level,component,message\n2025-02-12,INFO,api,hello\n";
        let header = CsvHeader::parse(data).unwrap();

        assert_eq!(header.num_columns(), 4);
        assert_eq!(header.well_known[0], well_known::WellKnownKind::Timestamp);
        assert_eq!(header.well_known[1], well_known::WellKnownKind::Level);
        assert_eq!(header.well_known[2], well_known::WellKnownKind::Component);
        assert_eq!(header.well_known[3], well_known::WellKnownKind::Message);
    }

    #[test]
    fn test_parse_csv_line() {
        let data =
            b"timestamp,level,component,message\n2025-02-12,INFO,api-server,request handled\n";
        let header = CsvHeader::parse(data).unwrap();
        let data_start = header_end_offset(data);
        let line = &data[data_start..data.len() - 1]; // strip trailing newline

        let mut batch = make_batch(data);
        parse_csv_line(line, data_start as u64, &header, &mut batch);

        assert_eq!(batch.len, 1);
        assert_eq!(batch.field_count(0), 4);

        unsafe {
            assert_eq!(batch.timestamp_value(0), Some("2025-02-12"));
            assert_eq!(batch.level_value(0), Some("INFO"));
            assert_eq!(batch.component_value(0), Some("api-server"));
            assert_eq!(batch.message_value(0), Some("request handled"));
        }
    }

    #[test]
    fn test_parse_csv_quoted_field() {
        let data = b"msg,level\n\"hello, world\",INFO\n";
        let header = CsvHeader::parse(data).unwrap();
        let data_start = header_end_offset(data);
        let line_end = data.len() - 1;
        let line = &data[data_start..line_end];

        let mut batch = make_batch(data);
        parse_csv_line(line, data_start as u64, &header, &mut batch);

        assert_eq!(batch.len, 1);
        unsafe {
            assert_eq!(batch.message_value(0), Some("hello, world"));
        }
    }

    #[test]
    fn test_header_end_offset() {
        assert_eq!(header_end_offset(b"a,b,c\ndata\n"), 6);
        assert_eq!(header_end_offset(b"no newline"), 10);
    }

    #[test]
    fn test_parse_csv_multiple_lines() {
        let data = b"timestamp,level,message\n2025-01-01,INFO,first\n2025-01-02,WARN,second\n2025-01-03,ERROR,third\n";
        let header = CsvHeader::parse(data).unwrap();
        let data_start = header_end_offset(data);
        let remainder = &data[data_start..];

        let mut line_starts: Vec<u64> = vec![data_start as u64];
        for (i, &b) in remainder.iter().enumerate() {
            if b == b'\n' && data_start + i + 1 < data.len() {
                line_starts.push((data_start + i + 1) as u64);
            }
        }

        let mut batch = make_batch(data);
        parse_csv_lines_range(data, &line_starts, 0, 3, &header, &mut batch);

        assert_eq!(batch.len, 3);
        unsafe {
            assert_eq!(batch.level_value(0), Some("INFO"));
            assert_eq!(batch.level_value(1), Some("WARN"));
            assert_eq!(batch.level_value(2), Some("ERROR"));
            assert_eq!(batch.message_value(0), Some("first"));
            assert_eq!(batch.message_value(1), Some("second"));
            assert_eq!(batch.message_value(2), Some("third"));
        }
    }
}
