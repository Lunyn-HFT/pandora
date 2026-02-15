#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LogFormat {
    PlainText,

    Json,

    Logfmt,

    Csv,
}

impl LogFormat {
    pub fn detect(data: &[u8]) -> LogFormat {
        let trimmed = skip_whitespace_and_bom(data);

        if trimmed.is_empty() {
            return LogFormat::PlainText;
        }

        if trimmed[0] == b'{' {
            return LogFormat::Json;
        }

        if trimmed[0] == b'[' && trimmed.len() > 1 {
            let inner = skip_whitespace_and_bom(&trimmed[1..]);
            if !inner.is_empty() && inner[0] == b'{' {
                return LogFormat::Json;
            }
        }

        let first_line_end = memchr::memchr(b'\n', trimmed).unwrap_or(trimmed.len());
        let first_line = &trimmed[..first_line_end];

        if detect_logfmt(first_line) {
            return LogFormat::Logfmt;
        }

        if detect_csv(first_line, trimmed) {
            return LogFormat::Csv;
        }

        LogFormat::PlainText
    }

    pub fn as_str(self) -> &'static str {
        match self {
            LogFormat::PlainText => "plain-text",
            LogFormat::Json => "json",
            LogFormat::Logfmt => "logfmt",
            LogFormat::Csv => "csv",
        }
    }
}

impl std::fmt::Display for LogFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[inline]
fn skip_whitespace_and_bom(data: &[u8]) -> &[u8] {
    let mut i = 0;
    if data.len() >= 3 && data[0] == 0xEF && data[1] == 0xBB && data[2] == 0xBF {
        i = 3;
    }
    while i < data.len()
        && (data[i] == b' ' || data[i] == b'\t' || data[i] == b'\r' || data[i] == b'\n')
    {
        i += 1;
    }
    &data[i..]
}

fn detect_logfmt(line: &[u8]) -> bool {
    let mut i = 0;
    let mut kv_count = 0;

    while i < line.len() {
        while i < line.len() && line[i] == b' ' {
            i += 1;
        }

        let key_start = i;
        while i < line.len() && is_logfmt_key_char(line[i]) {
            i += 1;
        }

        if i == key_start || i >= line.len() || line[i] != b'=' {
            while i < line.len() && line[i] != b' ' {
                i += 1;
            }
            continue;
        }

        i += 1; // skip '='

        if i < line.len() && line[i] == b'"' {
            i += 1;
            while i < line.len() && line[i] != b'"' {
                if line[i] == b'\\' {
                    i += 1; // skip escaped char
                }
                i += 1;
            }
            if i < line.len() {
                i += 1; // skip closing quote
            }
        } else {
            while i < line.len() && line[i] != b' ' {
                i += 1;
            }
        }

        kv_count += 1;
        if kv_count >= 2 {
            return true;
        }
    }

    false
}

#[inline(always)]
fn is_logfmt_key_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'-' || b == b'.'
}

fn detect_csv(first_line: &[u8], all_data: &[u8]) -> bool {
    let comma_count = first_line.iter().filter(|&&b| b == b',').count();
    if comma_count < 2 {
        return false;
    }

    let alpha_count = first_line
        .iter()
        .filter(|&&b| b.is_ascii_alphabetic() || b == b'_')
        .count();
    if alpha_count < first_line.len() / 3 {
        return false;
    }

    let after_first = if first_line.len() < all_data.len() {
        let rest = &all_data[first_line.len()..];
        let start = if !rest.is_empty() && rest[0] == b'\n' {
            1
        } else if rest.len() >= 2 && rest[0] == b'\r' && rest[1] == b'\n' {
            2
        } else {
            return false;
        };
        &rest[start..]
    } else {
        return false;
    };

    let second_line_end = memchr::memchr(b'\n', after_first).unwrap_or(after_first.len());
    let second_line = &after_first[..second_line_end];
    let second_comma_count = second_line.iter().filter(|&&b| b == b',').count();

    second_comma_count == comma_count
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_json() {
        assert_eq!(LogFormat::detect(b"{\"level\":\"info\"}"), LogFormat::Json);
        assert_eq!(
            LogFormat::detect(b"  {\"level\":\"info\"}"),
            LogFormat::Json
        );
        assert_eq!(
            LogFormat::detect(b"\n{\"level\":\"info\"}"),
            LogFormat::Json
        );
    }

    #[test]
    fn test_detect_json_with_bom() {
        let mut data = vec![0xEF, 0xBB, 0xBF];
        data.extend_from_slice(b"{\"level\":\"info\"}");
        assert_eq!(LogFormat::detect(&data), LogFormat::Json);
    }

    #[test]
    fn test_detect_logfmt() {
        assert_eq!(
            LogFormat::detect(b"level=info msg=\"hello world\" duration=1.5ms"),
            LogFormat::Logfmt
        );
        assert_eq!(
            LogFormat::detect(b"ts=2025-02-12T10:31:45Z level=info component=api-server"),
            LogFormat::Logfmt
        );
    }

    #[test]
    fn test_detect_csv() {
        let csv =
            b"timestamp,level,component,message\n2025-02-12T10:31:45Z,INFO,api-server,hello\n";
        assert_eq!(LogFormat::detect(csv), LogFormat::Csv);
    }

    #[test]
    fn test_detect_plain_text() {
        assert_eq!(
            LogFormat::detect(b"2025-02-12T10:31:45Z INFO api-server request_id=abc123"),
            LogFormat::PlainText
        );
    }

    #[test]
    fn test_detect_empty() {
        assert_eq!(LogFormat::detect(b""), LogFormat::PlainText);
        assert_eq!(LogFormat::detect(b"   "), LogFormat::PlainText);
    }
}
