use std::io::Write as IoWrite;

use anyhow::Result;
use tokio::io::AsyncWriteExt;
use tokio::signal::unix::{signal, SignalKind};

use termd::proto::{
    terminal_response::Response,
    StreamMetadataReason,
};

// ── VtFilter ──────────────────────────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq)]
enum CsiMode { Normal, Private }

#[derive(Clone, Copy, PartialEq)]
enum FilterState {
    Normal,
    AfterEsc,
    InCsi(CsiMode),
}

struct VtFilter {
    state:          FilterState,
    buf:            Vec<u8>,
    server_rows:    u32,
    server_cols:    u32,
    client_rows:    u32,
    client_cols:    u32,
    declrmm_active: bool,
    in_alt_screen:  bool,
}

impl VtFilter {
    fn new(server_rows: u32, server_cols: u32, client_rows: u32, client_cols: u32) -> Self {
        Self {
            state: FilterState::Normal,
            buf: Vec::new(),
            server_rows,
            server_cols,
            client_rows,
            client_cols,
            declrmm_active: false,
            in_alt_screen: false,
        }
    }

    fn effective_rows(&self) -> u32 { self.server_rows.min(self.client_rows) }
    fn effective_cols(&self) -> u32 { self.server_cols.min(self.client_cols) }

    fn update_region(&mut self, server_rows: u32, server_cols: u32) {
        self.server_rows = server_rows;
        self.server_cols = server_cols;
    }

    fn update_client_size(&mut self, client_rows: u32, client_cols: u32) {
        self.client_rows = client_rows;
        self.client_cols = client_cols;
    }

    fn emit_region_setup(&mut self, out: &mut Vec<u8>) {
        write!(out, "\x1b[1;{}r", self.effective_rows()).ok();
        if self.client_cols > self.server_cols {
            out.extend_from_slice(b"\x1b[?69h");
            write!(out, "\x1b[1;{}s", self.effective_cols()).ok();
            self.declrmm_active = true;
        }
    }

    fn filter(&mut self, input: &[u8], out: &mut Vec<u8>) {
        for &byte in input {
            match self.state {
                FilterState::Normal => {
                    if byte == 0x1b {
                        self.buf.push(byte);
                        self.state = FilterState::AfterEsc;
                    } else {
                        out.push(byte);
                    }
                }
                FilterState::AfterEsc => {
                    match byte {
                        b'[' => {
                            self.buf.push(byte);
                            self.state = FilterState::InCsi(CsiMode::Normal);
                        }
                        b'c' => {
                            // RIS: pass through, then re-emit region setup
                            out.extend_from_slice(b"\x1bc");
                            self.emit_region_setup(out);
                            self.buf.clear();
                            self.state = FilterState::Normal;
                        }
                        // Two-char sequences with large content (OSC/DCS/APC/PM/SOS)
                        // or no content (ST = ESC \): flush ESC + byte immediately
                        b']' | b'P' | b'_' | b'^' | b'X' | b'\\' => {
                            out.extend_from_slice(&self.buf);
                            out.push(byte);
                            self.buf.clear();
                            self.state = FilterState::Normal;
                        }
                        _ => {
                            // Unknown two-char ESC sequence: pass both bytes through
                            out.extend_from_slice(&self.buf);
                            out.push(byte);
                            self.buf.clear();
                            self.state = FilterState::Normal;
                        }
                    }
                }
                FilterState::InCsi(mode) => {
                    if byte == 0x1b {
                        // Nested ESC: flush incomplete sequence, start new escape
                        out.extend_from_slice(&self.buf);
                        self.buf.clear();
                        self.buf.push(byte);
                        self.state = FilterState::AfterEsc;
                    } else if (0x40..=0x7e).contains(&byte) {
                        // Final byte (0x40–0x7E): sequence complete, dispatch
                        self.buf.push(byte);
                        self.dispatch_csi(mode, byte, out);
                        self.buf.clear();
                        self.state = FilterState::Normal;
                    } else if (0x20..=0x3f).contains(&byte) {
                        // Parameter bytes (0x30–0x3F) and intermediate bytes (0x20–0x2F)
                        if byte == b'?' && self.buf.len() == 2 {
                            // '?' immediately after '\x1b[' marks a private CSI
                            self.state = FilterState::InCsi(CsiMode::Private);
                        }
                        self.buf.push(byte);
                        if self.buf.len() > 30 {
                            // Safety: too many bytes accumulated — give up on this sequence
                            out.extend_from_slice(&self.buf);
                            self.buf.clear();
                            self.state = FilterState::Normal;
                        }
                    } else {
                        // Unexpected byte: flush and pass through
                        out.extend_from_slice(&self.buf);
                        self.buf.clear();
                        out.push(byte);
                        self.state = FilterState::Normal;
                    }
                }
            }
        }
    }

    fn dispatch_csi(&mut self, mode: CsiMode, final_byte: u8, out: &mut Vec<u8>) {
        // Stub: pass everything through — rewrite rules added in Tasks 3 and 4
        let _ = (mode, final_byte);
        out.extend_from_slice(&self.buf);
    }

    fn parse_csi_params(&self) -> Vec<u32> {
        // buf = "\x1b[" [maybe "?"] <params> <final_byte>
        // skip the leading "\x1b[" (2 bytes), skip "?" if private (1 more byte)
        let start = if self.buf.get(2) == Some(&b'?') { 3 } else { 2 };
        let end = self.buf.len().saturating_sub(1); // exclude final byte
        if start >= end {
            return vec![];
        }
        self.buf[start..end]
            .split(|&b| b == b';')
            .filter_map(|s| {
                let s = std::str::from_utf8(s).ok()?;
                Some(s.parse::<u32>().unwrap_or(0))
            })
            .collect()
    }
}

// ── run() placeholder — implemented in Task 5 ─────────────────────────────────

pub(super) async fn run(_ctx: super::RunContext) -> Result<bool> {
    todo!("region::run")
}

// ── tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn filter_all(f: &mut VtFilter, input: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        f.filter(input, &mut out);
        out
    }

    #[test]
    fn plain_bytes_pass_through() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        assert_eq!(filter_all(&mut f, b"hello world"), b"hello world");
    }

    #[test]
    fn esc_unknown_char_passes_through() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        // ESC A is an unknown two-char sequence — both bytes must pass through
        assert_eq!(filter_all(&mut f, b"\x1bA"), b"\x1bA");
    }

    #[test]
    fn esc_string_opener_passes_immediately() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        // ESC ] is OSC opener — ESC + ] flushed immediately; rest flows through Normal
        assert_eq!(
            filter_all(&mut f, b"\x1b]0;window title\x07"),
            b"\x1b]0;window title\x07",
        );
    }

    #[test]
    fn esc_ris_emits_region() {
        let mut f = VtFilter::new(24, 80, 40, 80);
        // ESC c = RIS — pass RIS through, then re-emit DECSTBM
        let out = filter_all(&mut f, b"\x1bc");
        let s = String::from_utf8(out).unwrap();
        assert!(s.starts_with("\x1bc"), "RIS must be passed through first");
        assert!(s.contains("\x1b[1;24r"), "DECSTBM region setup must follow RIS");
    }

    #[test]
    fn csi_unknown_passes_through() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        // ESC [ 5 A = cursor up 5 — unrecognized CSI, passes through unchanged
        assert_eq!(filter_all(&mut f, b"\x1b[5A"), b"\x1b[5A");
    }

    #[test]
    fn csi_safety_limit_flushes() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        // A CSI sequence longer than 32 bytes must be flushed as-is, not buffered forever
        let long: Vec<u8> = b"\x1b[1;2;3;4;5;6;7;8;9;10;11;12;13".to_vec();
        let out = filter_all(&mut f, &long);
        assert!(out.starts_with(b"\x1b["), "safety flush must emit the accumulated bytes");
    }

    #[test]
    fn nested_esc_in_csi_flushes_buf() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        // ESC [ 5 ESC [ 2 J — nested ESC flushes incomplete first CSI
        let out = filter_all(&mut f, b"\x1b[5\x1b[2J");
        assert!(out.starts_with(b"\x1b[5"), "incomplete CSI must be flushed");
        assert!(out.ends_with(b"\x1b[2J"), "subsequent CSI must pass through");
    }

    #[test]
    fn csi_private_marker_detected() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        // ESC [ ? 25 h = show cursor — private CSI not in our rewrite table, passes through
        assert_eq!(filter_all(&mut f, b"\x1b[?25h"), b"\x1b[?25h");
    }
}
