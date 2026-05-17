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
                FilterState::InCsi(_) => {
                    // Placeholder — implemented in Task 2
                    out.extend_from_slice(&self.buf);
                    out.push(byte);
                    self.buf.clear();
                    self.state = FilterState::Normal;
                }
            }
        }
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
}
