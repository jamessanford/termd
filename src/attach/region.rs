use std::io::Write as IoWrite;

use anyhow::Result;

// ── VtFilter ──────────────────────────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq)]
enum CsiMode { Normal, Private }

#[derive(Clone, Copy, PartialEq)]
enum FilterState {
    Normal,
    AfterEsc,
    InCsi(CsiMode),
}

/// Streaming VT escape-sequence filter for region mode.
///
/// Region mode forwards raw server PTY bytes to the client terminal, confined to
/// a DECSTBM scroll region sized to the server PTY dimensions. The problem is that
/// programs running on the server (vim, less, htop, tmux) emit their own DECSTBM
/// and DECLRMM/DECSLRM sequences, which would clobber the client-side region setup.
/// `VtFilter` sits between the gRPC byte stream and stdout, intercepting those
/// sequences byte-by-byte and rewriting or suppressing them so they stay within the
/// server's bounds. The state machine carries partial-sequence state across `filter()`
/// calls, handling escape sequences that span buffer boundaries.
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
        } else if self.declrmm_active {
            out.extend_from_slice(b"\x1b[?69l");
            self.declrmm_active = false;
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
                    if self.buf.len() > 32 {
                        // Safety: too many bytes accumulated — give up on this sequence
                        out.extend_from_slice(&self.buf);
                        self.buf.clear();
                        self.state = FilterState::Normal;
                        if byte == 0x1b {
                            self.buf.push(byte);
                            self.state = FilterState::AfterEsc;
                        } else {
                            out.push(byte);
                        }
                        continue;
                    }
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
        match (mode, final_byte) {
            (CsiMode::Normal, b'p') if self.buf.get(2) == Some(&b'!') => {
                // DECSTR (Soft Terminal Reset, ESC [ ! p): pass through, then re-establish
                // our margins — DECSTR disables DECLRMM and resets DECSLRM to full width.
                out.extend_from_slice(&self.buf);
                self.emit_region_setup(out);
            }
            (CsiMode::Normal, b'r') => {
                // DECSTBM: clamp bottom margin to server_rows
                let params = self.parse_csi_params();
                let top = params.first().copied().filter(|&p| p != 0).unwrap_or(1);
                let bottom = params.get(1).copied().unwrap_or(0);
                let effective_bottom = if bottom == 0 || bottom > self.effective_rows() {
                    self.effective_rows()
                } else {
                    bottom
                };
                let effective_top = if top >= effective_bottom { 1 } else { top };
                write!(out, "\x1b[{};{}r", effective_top, effective_bottom).ok();
                // Re-establish horizontal margins: DECSTBM resets them on some terminals.
                if self.declrmm_active {
                    write!(out, "\x1b[1;{}s", self.effective_cols()).ok();
                }
            }
            (CsiMode::Normal, b's') if self.declrmm_active => {
                // DECSLRM: clamp right margin to server_cols
                // Only intercept when we've enabled DECLRMM; otherwise ESC[s = cursor save
                let params = self.parse_csi_params();
                let left = params.first().copied().filter(|&p| p != 0).unwrap_or(1);
                let right = params.get(1).copied().unwrap_or(0);
                let effective_right = if right == 0 || right > self.effective_cols() {
                    self.effective_cols()
                } else {
                    right
                };
                let effective_left = if left >= effective_right { 1 } else { left };
                write!(out, "\x1b[{};{}s", effective_left, effective_right).ok();
            }
            (CsiMode::Private, b'h') => {
                // Only the first param is inspected; combined sequences (e.g. ?1049;2004h)
                // drop the non-intercepted params. Multi-param private sequences are rare in practice.
                let params = self.parse_csi_params();
                match params.first().copied() {
                    Some(69) => {
                        // DECLRMM enable: suppress — we manage this ourselves
                    }
                    Some(1049) => {
                        // Alt-screen enter: pass through
                        out.extend_from_slice(&self.buf);
                        self.in_alt_screen = true;
                    }
                    _ => out.extend_from_slice(&self.buf),
                }
            }
            (CsiMode::Private, b'l') => {
                // Only the first param is inspected; combined sequences drop non-intercepted params.
                let params = self.parse_csi_params();
                match params.first().copied() {
                    Some(69) => {
                        // DECLRMM disable: suppress
                    }
                    Some(1049) => {
                        // Alt-screen exit: pass through, then re-emit region setup
                        out.extend_from_slice(&self.buf);
                        self.in_alt_screen = false;
                        self.emit_region_setup(out);
                    }
                    _ => out.extend_from_slice(&self.buf),
                }
            }
            _ => out.extend_from_slice(&self.buf),
        }
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

// ── RegionHandler ────────────────────────────────────────────────────────────

pub(super) struct RegionHandler {
    filter: VtFilter,
}

impl RegionHandler {
    pub(super) fn new(server_rows: u32, server_cols: u32, client_rows: u32, client_cols: u32) -> Self {
        Self {
            filter: VtFilter::new(server_rows, server_cols, client_rows, client_cols),
        }
    }
}

impl super::RenderModeHandler for RegionHandler {
    fn init(&mut self, refresh_data: &[u8], buffered: &[(u64, Vec<u8>)], out: &mut Vec<u8>) -> Result<super::EventResult> {
        if !super::server_fits_client(self.filter.server_cols, self.filter.server_rows, self.filter.client_cols, self.filter.client_rows) {
            return Ok(super::EventResult::ChangeRenderMode(super::RenderMode::Cell));
        }
        self.filter.emit_region_setup(out);
        self.filter.filter(refresh_data, out);
        for (_gen, data) in buffered {
            self.filter.filter(data, out);
        }
        Ok(super::EventResult::Continue)
    }

    fn on_pty_event(&mut self, event: super::PtyEvent, out: &mut Vec<u8>) -> Result<super::EventResult> {
        match event {
            super::PtyEvent::Stream { data, .. } => {
                self.filter.filter(data, out);
            }
            super::PtyEvent::Refresh { cols, rows, data, .. } => {
                if !super::server_fits_client(cols, rows, self.filter.client_cols, self.filter.client_rows) {
                    return Ok(super::EventResult::ChangeRenderMode(super::RenderMode::Cell));
                }
                self.filter.update_region(rows, cols);
                self.filter.emit_region_setup(out);
                self.filter.filter(data, out);
            }
            super::PtyEvent::Resize { cols, rows } => {
                if !super::server_fits_client(cols, rows, self.filter.client_cols, self.filter.client_rows) {
                    return Ok(super::EventResult::ChangeRenderMode(super::RenderMode::Cell));
                }
                self.filter.update_region(rows, cols);
                self.filter.emit_region_setup(out);
            }
            super::PtyEvent::Closed => {}
        }
        Ok(super::EventResult::Continue)
    }

    fn on_sigwinch(&mut self, cols: u32, rows: u32, out: &mut Vec<u8>) -> Result<super::EventResult> {
        if !super::server_fits_client(self.filter.server_cols, self.filter.server_rows, cols, rows) {
            return Ok(super::EventResult::ChangeRenderMode(super::RenderMode::Cell));
        }
        self.filter.update_client_size(rows, cols);
        self.filter.emit_region_setup(out);
        Ok(super::EventResult::Continue)
    }

    fn cleanup(&mut self, out: &mut Vec<u8>) {
        out.extend_from_slice(b"\x1b[r");
        if self.filter.declrmm_active {
            out.extend_from_slice(b"\x1b[?69l");
        }
    }
}

// ── tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::{PtyEvent, EventResult, RenderMode, RenderModeHandler};

    fn filter_all(f: &mut VtFilter, input: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        f.filter(input, &mut out);
        out
    }

    // ── VtFilter tests ──

    #[test]
    fn plain_bytes_pass_through() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        assert_eq!(filter_all(&mut f, b"hello world"), b"hello world");
    }

    #[test]
    fn esc_unknown_char_passes_through() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        assert_eq!(filter_all(&mut f, b"\x1bA"), b"\x1bA");
    }

    #[test]
    fn esc_string_opener_passes_immediately() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        assert_eq!(
            filter_all(&mut f, b"\x1b]0;window title\x07"),
            b"\x1b]0;window title\x07",
        );
    }

    #[test]
    fn esc_ris_emits_region() {
        let mut f = VtFilter::new(24, 80, 40, 80);
        let out = filter_all(&mut f, b"\x1bc");
        let s = String::from_utf8(out).unwrap();
        assert!(s.starts_with("\x1bc"), "RIS must be passed through first");
        assert!(s.contains("\x1b[1;24r"), "DECSTBM region setup must follow RIS");
    }

    #[test]
    fn csi_unknown_passes_through() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        assert_eq!(filter_all(&mut f, b"\x1b[5A"), b"\x1b[5A");
    }

    #[test]
    fn csi_safety_limit_flushes() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        let long: Vec<u8> = b"\x1b[1;2;3;4;5;6;7;8;9;10;11;12;13;14".to_vec();
        let out = filter_all(&mut f, &long);
        assert!(out.starts_with(b"\x1b["), "safety flush must emit the accumulated bytes");
    }

    #[test]
    fn nested_esc_in_csi_flushes_buf() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        let out = filter_all(&mut f, b"\x1b[5\x1b[2J");
        assert!(out.starts_with(b"\x1b[5"), "incomplete CSI must be flushed");
        assert!(out.ends_with(b"\x1b[2J"), "subsequent CSI must pass through");
    }

    #[test]
    fn csi_private_marker_detected() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        assert_eq!(filter_all(&mut f, b"\x1b[?25h"), b"\x1b[?25h");
    }

    #[test]
    fn decstbm_bare_reset_rewritten() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        assert_eq!(filter_all(&mut f, b"\x1b[r"), b"\x1b[1;24r");
    }

    #[test]
    fn decstbm_bottom_clamped_to_server_rows() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        assert_eq!(filter_all(&mut f, b"\x1b[1;30r"), b"\x1b[1;24r");
    }

    #[test]
    fn decstbm_within_bounds_unchanged() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        assert_eq!(filter_all(&mut f, b"\x1b[5;20r"), b"\x1b[5;20r");
    }

    #[test]
    fn decstbm_top_exceeds_server_rows_collapses() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        assert_eq!(filter_all(&mut f, b"\x1b[25;30r"), b"\x1b[1;24r");
    }

    #[test]
    fn decstbm_cross_buffer() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        let mut out = Vec::new();
        f.filter(b"\x1b[1;30", &mut out);
        f.filter(b"r", &mut out);
        assert_eq!(out, b"\x1b[1;24r");
    }

    #[test]
    fn decslrm_right_clamped_to_server_cols() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        let mut out = Vec::new();
        f.emit_region_setup(&mut out);
        out.clear();
        f.filter(b"\x1b[1;100s", &mut out);
        assert_eq!(out, b"\x1b[1;80s");
    }

    #[test]
    fn declrmm_enable_suppressed() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        assert_eq!(filter_all(&mut f, b"\x1b[?69h"), b"");
    }

    #[test]
    fn declrmm_disable_suppressed() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        assert_eq!(filter_all(&mut f, b"\x1b[?69l"), b"");
    }

    #[test]
    fn alt_screen_enter_passes_through() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        let out = filter_all(&mut f, b"\x1b[?1049h");
        assert_eq!(out, b"\x1b[?1049h");
        assert!(f.in_alt_screen);
    }

    #[test]
    fn alt_screen_exit_reemits_region() {
        let mut f = VtFilter::new(24, 80, 40, 80);
        filter_all(&mut f, b"\x1b[?1049h");
        let out = filter_all(&mut f, b"\x1b[?1049l");
        let s = String::from_utf8(out).unwrap();
        assert!(s.starts_with("\x1b[?1049l"), "alt-screen exit byte must pass through");
        assert!(s.contains("\x1b[1;24r"), "region setup must be re-emitted after exit");
        assert!(!f.in_alt_screen);
    }

    #[test]
    fn other_private_mode_passes_through() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        assert_eq!(filter_all(&mut f, b"\x1b[?2004h"), b"\x1b[?2004h");
    }

    #[test]
    fn decslrm_left_exceeds_server_cols_collapses() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        let mut out = Vec::new();
        f.emit_region_setup(&mut out);
        out.clear();
        f.filter(b"\x1b[90;100s", &mut out);
        assert_eq!(out, b"\x1b[1;80s");
    }

    #[test]
    fn emit_region_setup_disables_declrmm_when_client_shrinks() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        let mut out = Vec::new();
        f.emit_region_setup(&mut out);
        assert!(f.declrmm_active);
        out.clear();
        f.update_client_size(40, 80);
        f.emit_region_setup(&mut out);
        assert!(!f.declrmm_active);
        let s = String::from_utf8(out).unwrap();
        assert!(s.contains("\x1b[?69l"), "DECLRMM disable sequence must be emitted");
    }

    #[test]
    fn decstr_re_emits_region_setup() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        let mut out = Vec::new();
        f.emit_region_setup(&mut out);
        out.clear();
        f.filter(b"\x1b[!p", &mut out);
        let s = String::from_utf8(out).unwrap();
        assert!(s.starts_with("\x1b[!p"), "DECSTR must pass through first");
        assert!(s.contains("\x1b[?69h"), "DECLRMM must be re-enabled after DECSTR");
        assert!(s.contains("\x1b[1;80s"), "DECSLRM must be re-established after DECSTR");
    }

    #[test]
    fn decstbm_rewrite_re_emits_decslrm() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        let mut out = Vec::new();
        f.emit_region_setup(&mut out);
        out.clear();
        f.filter(b"\x1b[r", &mut out);
        assert_eq!(out, b"\x1b[1;24r\x1b[1;80s");
    }

    // ── RegionHandler tests ──

    #[test]
    fn region_init_emits_setup_and_data() {
        let mut h = RegionHandler::new(24, 80, 40, 120);
        let mut out = Vec::new();
        let result = h.init(b"hello", &[], &mut out).unwrap();
        assert!(matches!(result, EventResult::Continue));
        let s = String::from_utf8_lossy(&out);
        assert!(s.contains("\x1b[1;24r"), "should emit DECSTBM");
        assert!(s.contains("hello"), "should include refresh data");
    }

    #[test]
    fn region_init_too_small_returns_change_mode() {
        let mut h = RegionHandler::new(24, 80, 20, 60);
        let mut out = Vec::new();
        let result = h.init(b"hello", &[], &mut out).unwrap();
        assert!(matches!(result, EventResult::ChangeRenderMode(RenderMode::Cell)));
    }

    #[test]
    fn region_init_replays_buffered() {
        let mut h = RegionHandler::new(24, 80, 40, 120);
        let mut out = Vec::new();
        let buffered = vec![(2, b"world".to_vec())];
        h.init(b"hello", &buffered, &mut out).unwrap();
        let s = String::from_utf8_lossy(&out);
        assert!(s.contains("hello"));
        assert!(s.contains("world"));
    }

    #[test]
    fn region_stream_filters_data() {
        let mut h = RegionHandler::new(24, 80, 40, 120);
        let mut out = Vec::new();
        h.init(b"", &[], &mut out).unwrap();
        out.clear();

        let result = h.on_pty_event(PtyEvent::Stream { gen: 1, data: b"test" }, &mut out).unwrap();
        assert!(matches!(result, EventResult::Continue));
        assert_eq!(out, b"test");
    }

    #[test]
    fn region_refresh_updates_filter_and_re_emits_setup() {
        let mut h = RegionHandler::new(24, 80, 40, 120);
        let mut out = Vec::new();
        h.init(b"", &[], &mut out).unwrap();
        out.clear();

        let result = h.on_pty_event(
            PtyEvent::Refresh { gen: 2, cols: 80, rows: 30, data: b"new" },
            &mut out,
        ).unwrap();
        assert!(matches!(result, EventResult::Continue));
        let s = String::from_utf8_lossy(&out);
        assert!(s.contains("\x1b[1;30r"), "should emit updated DECSTBM");
        assert!(s.contains("new"));
    }

    #[test]
    fn region_refresh_too_large_switches_to_cell() {
        let mut h = RegionHandler::new(24, 80, 40, 120);
        let mut out = Vec::new();
        h.init(b"", &[], &mut out).unwrap();
        out.clear();

        let result = h.on_pty_event(
            PtyEvent::Refresh { gen: 2, cols: 200, rows: 50, data: b"big" },
            &mut out,
        ).unwrap();
        assert!(matches!(result, EventResult::ChangeRenderMode(RenderMode::Cell)));
    }

    #[test]
    fn region_resize_too_large_switches_to_cell() {
        let mut h = RegionHandler::new(24, 80, 40, 120);
        let mut out = Vec::new();
        h.init(b"", &[], &mut out).unwrap();
        out.clear();

        let result = h.on_pty_event(
            PtyEvent::Resize { cols: 200, rows: 50 },
            &mut out,
        ).unwrap();
        assert!(matches!(result, EventResult::ChangeRenderMode(RenderMode::Cell)));
    }

    #[test]
    fn region_sigwinch_too_small_switches_to_cell() {
        let mut h = RegionHandler::new(24, 80, 40, 120);
        let mut out = Vec::new();
        h.init(b"", &[], &mut out).unwrap();
        out.clear();

        let result = h.on_sigwinch(60, 20, &mut out).unwrap();
        assert!(matches!(result, EventResult::ChangeRenderMode(RenderMode::Cell)));
    }

    #[test]
    fn region_sigwinch_ok_updates_client_size() {
        let mut h = RegionHandler::new(24, 80, 40, 120);
        let mut out = Vec::new();
        h.init(b"", &[], &mut out).unwrap();
        out.clear();

        let result = h.on_sigwinch(200, 50, &mut out).unwrap();
        assert!(matches!(result, EventResult::Continue));
        let s = String::from_utf8_lossy(&out);
        assert!(s.contains("\x1b[1;24r"), "should re-emit DECSTBM on resize");
    }

    #[test]
    fn region_cleanup_resets_margins() {
        let mut h = RegionHandler::new(24, 80, 40, 120);
        let mut out = Vec::new();
        h.init(b"", &[], &mut out).unwrap();
        out.clear();

        h.cleanup(&mut out);
        let s = String::from_utf8_lossy(&out);
        assert!(s.contains("\x1b[r"), "should reset DECSTBM");
    }

    #[test]
    fn region_cleanup_disables_declrmm_when_active() {
        let mut h = RegionHandler::new(24, 80, 40, 120);
        let mut out = Vec::new();
        h.init(b"", &[], &mut out).unwrap();
        out.clear();

        h.cleanup(&mut out);
        let s = String::from_utf8_lossy(&out);
        assert!(s.contains("\x1b[?69l"), "should disable DECLRMM");
    }
}
