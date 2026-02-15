use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum LogLevel {
    Debug = 0,
    Info = 1,
    Warn = 2,
    Error = 3,
    Fatal = 4,
    Unknown = 255,
}

impl LogLevel {
    #[inline(always)]
    pub fn from_bytes(b: &[u8]) -> LogLevel {
        if b.is_empty() {
            return LogLevel::Unknown;
        }
        match (b[0], b.len()) {
            (b'D', 5) => LogLevel::Debug,
            (b'I', 4) => LogLevel::Info,
            (b'W', 4) => LogLevel::Warn,
            (b'E', 5) => LogLevel::Error,
            (b'F', 5) => LogLevel::Fatal,
            _ => LogLevel::Unknown,
        }
    }

    #[inline]
    pub fn as_str(self) -> &'static str {
        match self {
            LogLevel::Debug => "Debug",
            LogLevel::Info => "Info",
            LogLevel::Warn => "Warn",
            LogLevel::Error => "Error",
            LogLevel::Fatal => "Fatal",
            LogLevel::Unknown => "Unknown",
        }
    }
}

impl fmt::Display for LogLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[repr(C, align(64))]
pub struct LogBatch {
    pub timestamps: Vec<u64>,

    pub levels: Vec<LogLevel>,

    pub component_offsets: Vec<u64>,

    pub component_lens: Vec<u32>,

    pub message_offsets: Vec<u64>,

    pub message_lens: Vec<u32>,

    pub data_ptr: *const u8,

    pub len: usize,
}

unsafe impl Send for LogBatch {}
unsafe impl Sync for LogBatch {}

impl LogBatch {
    pub fn new(capacity: usize, data_ptr: *const u8) -> Self {
        LogBatch {
            timestamps: vec![0u64; capacity],
            levels: vec![LogLevel::Unknown; capacity],
            component_offsets: vec![0u64; capacity],
            component_lens: vec![0u32; capacity],
            message_offsets: vec![0u64; capacity],
            message_lens: vec![0u32; capacity],
            data_ptr,
            len: capacity,
        }
    }

    /// # Safety
    ///
    /// - `i` must be less than `self.len`.
    /// - `self.data_ptr` must point to valid, readable memory for the lifetime of the returned reference.
    /// - The offset and length at index `i` in `self.component_offsets` and `self.component_lens` must be valid and point to a valid UTF-8 sequence within the data.
    #[inline]
    pub unsafe fn component(&self, i: usize) -> &str {
        unsafe {
            let ptr = self.data_ptr.add(self.component_offsets[i] as usize);
            let slice = std::slice::from_raw_parts(ptr, self.component_lens[i] as usize);
            std::str::from_utf8_unchecked(slice)
        }
    }

    /// # Safety
    ///
    /// - `i` must be less than `self.len`.
    /// - `self.data_ptr` must point to valid, readable memory for the lifetime of the returned reference.
    /// - The offset and length at index `i` in `self.message_offsets` and `self.message_lens` must be valid and point to a valid UTF-8 sequence within the data.
    #[inline]
    pub unsafe fn message(&self, i: usize) -> &str {
        unsafe {
            let ptr = self.data_ptr.add(self.message_offsets[i] as usize);
            let slice = std::slice::from_raw_parts(ptr, self.message_lens[i] as usize);
            std::str::from_utf8_unchecked(slice)
        }
    }
}

#[derive(Debug, Clone)]
pub struct ParseStats {
    pub total_bytes: u64,
    pub total_lines: u64,
    pub scan_time_ms: f64,
    pub parse_time_ms: f64,
    pub total_time_ms: f64,
    pub threads_used: usize,
}

impl ParseStats {
    pub fn throughput_gbps(&self) -> f64 {
        if self.total_time_ms <= 0.0 {
            return 0.0;
        }
        (self.total_bytes as f64 / (1024.0 * 1024.0 * 1024.0)) / (self.total_time_ms / 1000.0)
    }

    pub fn scan_throughput_gbps(&self) -> f64 {
        if self.scan_time_ms <= 0.0 {
            return 0.0;
        }
        (self.total_bytes as f64 / (1024.0 * 1024.0 * 1024.0)) / (self.scan_time_ms / 1000.0)
    }

    pub fn parse_throughput_gbps(&self) -> f64 {
        if self.parse_time_ms <= 0.0 {
            return 0.0;
        }
        (self.total_bytes as f64 / (1024.0 * 1024.0 * 1024.0)) / (self.parse_time_ms / 1000.0)
    }
}

impl fmt::Display for ParseStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "╔══════════════════════════════════════╗")?;
        writeln!(f, "     PANDORA'S LOGS — PARSE STATS      ")?;
        writeln!(f, "╠══════════════════════════════════════╣")?;
        writeln!(
            f,
            "  Total bytes:     {:>10.2} GB        ",
            self.total_bytes as f64 / (1024.0 * 1024.0 * 1024.0)
        )?;
        writeln!(f, "  Total lines:     {:>10}           ", self.total_lines)?;
        writeln!(f, "  Threads used:    {:>10}           ", self.threads_used)?;
        writeln!(f, "╠══════════════════════════════════════╣")?;
        writeln!(
            f,
            "  Stage 1 (scan):  {:>8.1} ms         ",
            self.scan_time_ms
        )?;
        writeln!(
            f,
            "    └─ throughput: {:>8.2} GB/s       ",
            self.scan_throughput_gbps()
        )?;
        writeln!(
            f,
            "  Stage 2 (parse): {:>8.1} ms         ",
            self.parse_time_ms
        )?;
        writeln!(
            f,
            "    └─ throughput: {:>8.2} GB/s       ",
            self.parse_throughput_gbps()
        )?;
        writeln!(f, "╠══════════════════════════════════════╣")?;
        writeln!(
            f,
            "  Total time:      {:>8.1} ms         ",
            self.total_time_ms
        )?;
        writeln!(
            f,
            "     Throughput:   {:>8.2} GB/s       ",
            self.throughput_gbps()
        )?;
        writeln!(f, "╚══════════════════════════════════════╝")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_level_from_bytes() {
        assert_eq!(LogLevel::from_bytes(b"DEBUG"), LogLevel::Debug);
        assert_eq!(LogLevel::from_bytes(b"INFO"), LogLevel::Info);
        assert_eq!(LogLevel::from_bytes(b"WARN"), LogLevel::Warn);
        assert_eq!(LogLevel::from_bytes(b"ERROR"), LogLevel::Error);
        assert_eq!(LogLevel::from_bytes(b"FATAL"), LogLevel::Fatal);
        assert_eq!(LogLevel::from_bytes(b""), LogLevel::Unknown);
        assert_eq!(LogLevel::from_bytes(b"TRACE"), LogLevel::Unknown);
    }

    #[test]
    fn test_log_batch_creation() {
        let data = [0u8; 100];
        let batch = LogBatch::new(10, data.as_ptr());
        assert_eq!(batch.len, 10);
        assert_eq!(batch.timestamps.len(), 10);
        assert_eq!(batch.levels.len(), 10);
    }

    #[test]
    fn test_parse_stats_display() {
        let stats = ParseStats {
            total_bytes: 1_073_741_824,
            total_lines: 4_000_000,
            scan_time_ms: 200.0,
            parse_time_ms: 300.0,
            total_time_ms: 500.0,
            threads_used: 8,
        };
        assert!((stats.throughput_gbps() - 2.0).abs() < 0.01);
        let display = format!("{}", stats);
        assert!(display.contains("PANDORA'S LOGS"));
    }
}
