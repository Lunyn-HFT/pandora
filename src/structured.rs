use std::fmt;

#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub struct FieldRef {
    pub key_offset: u64,
    pub key_len: u32,
    pub val_offset: u64,
    pub val_len: u32,
}

#[derive(Debug, Clone, Copy)]
pub struct WellKnownFields {
    pub timestamp: u32,
    pub level: u32,
    pub message: u32,
    pub component: u32,
}

impl Default for WellKnownFields {
    fn default() -> Self {
        WellKnownFields {
            timestamp: u32::MAX,
            level: u32::MAX,
            message: u32::MAX,
            component: u32::MAX,
        }
    }
}

#[repr(C, align(64))]
pub struct StructuredBatch {
    pub fields: Vec<FieldRef>,

    pub field_starts: Vec<u32>,

    pub well_known: Vec<WellKnownFields>,

    pub line_offsets: Vec<u64>,

    pub line_lens: Vec<u32>,

    pub data_ptr: *const u8,

    pub len: usize,
}

unsafe impl Send for StructuredBatch {}
unsafe impl Sync for StructuredBatch {}

impl StructuredBatch {
    pub fn with_capacity(
        record_capacity: usize,
        field_capacity: usize,
        data_ptr: *const u8,
    ) -> Self {
        let mut field_starts = Vec::with_capacity(record_capacity + 1);
        field_starts.push(0);

        StructuredBatch {
            fields: Vec::with_capacity(field_capacity),
            field_starts,
            well_known: Vec::with_capacity(record_capacity),
            line_offsets: Vec::with_capacity(record_capacity),
            line_lens: Vec::with_capacity(record_capacity),
            data_ptr,
            len: 0,
        }
    }

    #[inline]
    pub fn begin_record(&mut self, line_offset: u64, line_len: u32) {
        self.line_offsets.push(line_offset);
        self.line_lens.push(line_len);
        self.well_known.push(WellKnownFields::default());
        self.len += 1;
    }

    #[inline]
    pub fn push_field(&mut self, field: FieldRef) {
        self.fields.push(field);
    }

    #[inline]
    pub fn end_record(&mut self) {
        self.field_starts.push(self.fields.len() as u32);
    }

    #[inline]
    pub fn set_well_known_timestamp(&mut self, field_idx: u32) {
        if let Some(wk) = self.well_known.last_mut() {
            wk.timestamp = field_idx;
        }
    }

    #[inline]
    pub fn set_well_known_level(&mut self, field_idx: u32) {
        if let Some(wk) = self.well_known.last_mut() {
            wk.level = field_idx;
        }
    }

    #[inline]
    pub fn set_well_known_message(&mut self, field_idx: u32) {
        if let Some(wk) = self.well_known.last_mut() {
            wk.message = field_idx;
        }
    }

    #[inline]
    pub fn set_well_known_component(&mut self, field_idx: u32) {
        if let Some(wk) = self.well_known.last_mut() {
            wk.component = field_idx;
        }
    }

    #[inline]
    pub fn field_count(&self, i: usize) -> usize {
        (self.field_starts[i + 1] - self.field_starts[i]) as usize
    }

    #[inline]
    #[allow(dead_code)]
    pub fn record_fields(&self, i: usize) -> &[FieldRef] {
        let start = self.field_starts[i] as usize;
        let end = self.field_starts[i + 1] as usize;
        &self.fields[start..end]
    }

    #[inline]
    #[allow(dead_code)]
    /// # Safety
    /// The field reference must be valid and point to valid UTF-8 data within the log data.
    pub unsafe fn field_key(&self, field: &FieldRef) -> &str {
        unsafe {
            let ptr = self.data_ptr.add(field.key_offset as usize);
            let slice = std::slice::from_raw_parts(ptr, field.key_len as usize);
            std::str::from_utf8_unchecked(slice)
        }
    }

    #[inline]
    /// # Safety
    /// The field reference must be valid and point to valid UTF-8 data within the log data.
    pub unsafe fn field_value(&self, field: &FieldRef) -> &str {
        unsafe {
            let ptr = self.data_ptr.add(field.val_offset as usize);
            let slice = std::slice::from_raw_parts(ptr, field.val_len as usize);
            std::str::from_utf8_unchecked(slice)
        }
    }

    #[inline]
    #[allow(dead_code)]
    /// # Safety
    /// The index must be within bounds and the line data must be valid UTF-8.
    pub unsafe fn raw_line(&self, i: usize) -> &str {
        unsafe {
            let ptr = self.data_ptr.add(self.line_offsets[i] as usize);
            let slice = std::slice::from_raw_parts(ptr, self.line_lens[i] as usize);
            std::str::from_utf8_unchecked(slice)
        }
    }

    #[inline]
    /// # Safety
    /// The index must be within bounds and the well-known field must be valid.
    pub unsafe fn timestamp_value(&self, i: usize) -> Option<&str> {
        let wk = &self.well_known[i];
        if wk.timestamp == u32::MAX {
            return None;
        }
        let field = &self.fields[wk.timestamp as usize];
        Some(unsafe { self.field_value(field) })
    }

    #[inline]
    /// # Safety
    /// The index must be within bounds and the well-known field must be valid.
    pub unsafe fn level_value(&self, i: usize) -> Option<&str> {
        let wk = &self.well_known[i];
        if wk.level == u32::MAX {
            return None;
        }
        let field = &self.fields[wk.level as usize];
        Some(unsafe { self.field_value(field) })
    }

    #[inline]
    /// # Safety
    /// The index must be within bounds and the well-known field must be valid.
    pub unsafe fn message_value(&self, i: usize) -> Option<&str> {
        let wk = &self.well_known[i];
        if wk.message == u32::MAX {
            return None;
        }
        let field = &self.fields[wk.message as usize];
        Some(unsafe { self.field_value(field) })
    }

    #[inline]
    /// # Safety
    /// The index must be within bounds and the well-known field must be valid.
    pub unsafe fn component_value(&self, i: usize) -> Option<&str> {
        let wk = &self.well_known[i];
        if wk.component == u32::MAX {
            return None;
        }
        let field = &self.fields[wk.component as usize];
        Some(unsafe { self.field_value(field) })
    }
}

impl fmt::Debug for StructuredBatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StructuredBatch")
            .field("len", &self.len)
            .field("total_fields", &self.fields.len())
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct StructuredParseStats {
    pub total_bytes: u64,
    pub total_records: u64,
    pub total_fields: u64,
    pub scan_time_ms: f64,
    pub parse_time_ms: f64,
    pub total_time_ms: f64,
    pub threads_used: usize,
    pub format: &'static str,
}

impl StructuredParseStats {
    pub fn throughput_gbps(&self) -> f64 {
        if self.total_time_ms <= 0.0 {
            return 0.0;
        }
        (self.total_bytes as f64 / (1024.0 * 1024.0 * 1024.0)) / (self.total_time_ms / 1000.0)
    }
}

impl fmt::Display for StructuredParseStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "╔══════════════════════════════════════════╗")?;
        writeln!(f, "   PANDORA'S LOGS — STRUCTURED PARSE STATS ")?;
        writeln!(f, "╠══════════════════════════════════════════╣")?;
        writeln!(f, "  Format:        {:<24}    ", self.format)?;
        writeln!(
            f,
            "  Total bytes:   {:>10.2} GB              ",
            self.total_bytes as f64 / (1024.0 * 1024.0 * 1024.0)
        )?;
        writeln!(
            f,
            "  Total records: {:>10}                 ",
            self.total_records
        )?;
        writeln!(
            f,
            "  Total fields:  {:>10}                 ",
            self.total_fields
        )?;
        writeln!(
            f,
            "  Threads used:  {:>10}                 ",
            self.threads_used
        )?;
        writeln!(f, "╠══════════════════════════════════════════╣")?;
        writeln!(
            f,
            "  Scan time:     {:>8.1} ms               ",
            self.scan_time_ms
        )?;
        writeln!(
            f,
            "  Parse time:    {:>8.1} ms               ",
            self.parse_time_ms
        )?;
        writeln!(
            f,
            "  Total time:    {:>8.1} ms               ",
            self.total_time_ms
        )?;
        writeln!(
            f,
            "  Throughput:    {:>8.2} GB/s             ",
            self.throughput_gbps()
        )?;
        writeln!(f, "╚══════════════════════════════════════════╝")?;
        Ok(())
    }
}

pub mod well_known {
    const TIMESTAMP_NAMES: &[&[u8]] = &[
        b"timestamp",
        b"time",
        b"ts",
        b"@timestamp",
        b"datetime",
        b"date",
        b"t",
        b"created_at",
        b"logged_at",
        b"event_time",
    ];

    const LEVEL_NAMES: &[&[u8]] = &[
        b"level",
        b"severity",
        b"lvl",
        b"log_level",
        b"loglevel",
        b"log.level",
        b"priority",
        b"sev",
    ];

    const MESSAGE_NAMES: &[&[u8]] = &[
        b"message",
        b"msg",
        b"text",
        b"body",
        b"log",
        b"description",
        b"content",
    ];

    const COMPONENT_NAMES: &[&[u8]] = &[
        b"component",
        b"source",
        b"logger",
        b"module",
        b"service",
        b"caller",
        b"name",
        b"logger_name",
        b"subsystem",
        b"tag",
    ];

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum WellKnownKind {
        Timestamp,
        Level,
        Message,
        Component,
        Other,
    }

    #[inline]
    pub fn classify_key(key: &[u8]) -> WellKnownKind {
        let mut buf = [0u8; 64];
        let len = key.len().min(64);
        buf[..len].copy_from_slice(&key[..len]);
        for b in &mut buf[..len] {
            *b = b.to_ascii_lowercase();
        }
        let lower = &buf[..len];

        match lower.first() {
            Some(b't') | Some(b'@') | Some(b'd') | Some(b'c') | Some(b'e') | Some(b'l') => {}
            Some(b'm') => {
                for name in MESSAGE_NAMES {
                    if lower == *name {
                        return WellKnownKind::Message;
                    }
                }
                for name in COMPONENT_NAMES {
                    if lower == *name {
                        return WellKnownKind::Component;
                    }
                }
                return WellKnownKind::Other;
            }
            Some(b's') => {
                for name in LEVEL_NAMES {
                    if lower == *name {
                        return WellKnownKind::Level;
                    }
                }
                for name in COMPONENT_NAMES {
                    if lower == *name {
                        return WellKnownKind::Component;
                    }
                }
                return WellKnownKind::Other;
            }
            Some(b'p') => {
                for name in LEVEL_NAMES {
                    if lower == *name {
                        return WellKnownKind::Level;
                    }
                }
                return WellKnownKind::Other;
            }
            Some(b'b') | Some(b'n') => {
                for name in MESSAGE_NAMES {
                    if lower == *name {
                        return WellKnownKind::Message;
                    }
                }
                for name in COMPONENT_NAMES {
                    if lower == *name {
                        return WellKnownKind::Component;
                    }
                }
                return WellKnownKind::Other;
            }
            _ => return WellKnownKind::Other,
        }

        for name in TIMESTAMP_NAMES {
            if lower == *name {
                return WellKnownKind::Timestamp;
            }
        }
        for name in LEVEL_NAMES {
            if lower == *name {
                return WellKnownKind::Level;
            }
        }
        for name in MESSAGE_NAMES {
            if lower == *name {
                return WellKnownKind::Message;
            }
        }
        for name in COMPONENT_NAMES {
            if lower == *name {
                return WellKnownKind::Component;
            }
        }

        WellKnownKind::Other
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_structured_batch_basic() {
        let data = b"{\"level\":\"info\",\"msg\":\"hello\"}";
        let mut batch = StructuredBatch::with_capacity(1, 4, data.as_ptr());

        batch.begin_record(0, data.len() as u32);
        batch.push_field(FieldRef {
            key_offset: 2,
            key_len: 5,
            val_offset: 10,
            val_len: 4,
        });
        batch.push_field(FieldRef {
            key_offset: 17,
            key_len: 3,
            val_offset: 23,
            val_len: 5,
        });
        batch.set_well_known_level(0);
        batch.set_well_known_message(1);
        batch.end_record();

        assert_eq!(batch.len, 1);
        assert_eq!(batch.field_count(0), 2);

        unsafe {
            assert_eq!(batch.level_value(0), Some("info"));
            assert_eq!(batch.message_value(0), Some("hello"));
            assert_eq!(batch.timestamp_value(0), None);
        }
    }

    #[test]
    fn test_well_known_classification() {
        use well_known::*;
        assert_eq!(classify_key(b"timestamp"), WellKnownKind::Timestamp);
        assert_eq!(classify_key(b"time"), WellKnownKind::Timestamp);
        assert_eq!(classify_key(b"ts"), WellKnownKind::Timestamp);
        assert_eq!(classify_key(b"@timestamp"), WellKnownKind::Timestamp);
        assert_eq!(classify_key(b"level"), WellKnownKind::Level);
        assert_eq!(classify_key(b"severity"), WellKnownKind::Level);
        assert_eq!(classify_key(b"msg"), WellKnownKind::Message);
        assert_eq!(classify_key(b"message"), WellKnownKind::Message);
        assert_eq!(classify_key(b"component"), WellKnownKind::Component);
        assert_eq!(classify_key(b"source"), WellKnownKind::Component);
        assert_eq!(classify_key(b"logger"), WellKnownKind::Component);
        assert_eq!(classify_key(b"foobar"), WellKnownKind::Other);
        assert_eq!(classify_key(b"LEVEL"), WellKnownKind::Level);
        assert_eq!(classify_key(b"Timestamp"), WellKnownKind::Timestamp);
        assert_eq!(classify_key(b"MSG"), WellKnownKind::Message);
    }
}
