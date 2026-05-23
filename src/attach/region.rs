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

// ── run() ────────────────────────────────────────────────────────────────────

enum LoopExit {
    ChangeRenderMode,
    Action(super::InputAction),
}

pub(super) async fn run(ctx: super::RunContext) -> Result<super::RunOutcome> {
    let (mut client_cols, mut client_rows) = super::get_terminal_size();
    let server_rows = ctx.item.rows;
    let server_cols = ctx.item.cols;

    if !super::server_fits_client(server_cols, server_rows, client_cols, client_rows) {
        eprintln!(
            "[region: client ({client_cols}x{client_rows}) smaller than \
             server ({server_cols}x{server_rows}); switching to cell mode]"
        );
        return Ok(super::RunOutcome::ChangeRenderMode(super::RenderMode::Cell, ctx));
    }

    let super::RunContext {
        mut resp_rx, cmd_tx, pty_id, mut item,
        refresh_gen, refresh_bytes, buffered, mut action_rx,
    } = ctx;

    let mut filter = VtFilter::new(server_rows, server_cols, client_rows, client_cols);
    let mut stdout = tokio::io::stdout();
    let mut out = Vec::new();
    let mut current_refresh_gen = refresh_gen;

    filter.emit_region_setup(&mut out);
    filter.filter(&refresh_bytes, &mut out);
    stdout.write_all(&out).await?;

    for (gen, data) in &buffered {
        if *gen > current_refresh_gen {
            out.clear();
            filter.filter(data, &mut out);
            stdout.write_all(&out).await?;
        }
    }
    stdout.flush().await?;

    let mut sigwinch = signal(SignalKind::window_change())?;
    let mut loop_exit: Option<LoopExit> = None;
    let mut pty_closed = false;

    loop {
        out.clear();
        tokio::select! {
            msg = resp_rx.message() => {
                match msg {
                    Ok(Some(r)) => match r.response {
                        Some(Response::Stream(s)) if s.pty_id == pty_id && s.generation > current_refresh_gen => {
                            filter.filter(&s.data, &mut out);
                        }
                        Some(Response::Refresh(rf)) if rf.pty_id == pty_id => {
                            current_refresh_gen = rf.generation;
                            item.cols = rf.cols;
                            item.rows = rf.rows;
                            if !super::server_fits_client(rf.cols, rf.rows, client_cols, client_rows) {
                                eprintln!(
                                    "[region: server refreshed at ({}x{}), larger than \
                                     client ({}x{}); switching to cell mode]",
                                    rf.cols, rf.rows, client_cols, client_rows
                                );
                                loop_exit = Some(LoopExit::ChangeRenderMode);
                                break;
                            }
                            filter.update_region(rf.rows, rf.cols);
                            filter.emit_region_setup(&mut out);
                            filter.filter(&rf.data, &mut out);
                        }
                        Some(Response::Metadata(m)) if m.pty_id == pty_id => {
                            if m.reason == StreamMetadataReason::Resize as i32 {
                                if let Some(ref mi) = m.item {
                                    if mi.cols > 0 && mi.rows > 0 {
                                        item.cols = mi.cols;
                                        item.rows = mi.rows;
                                        if !super::server_fits_client(mi.cols, mi.rows, client_cols, client_rows) {
                                            eprintln!(
                                                "[region: server resized to ({}x{}), larger than \
                                                 client ({}x{}); switching to cell mode]",
                                                mi.cols, mi.rows, client_cols, client_rows
                                            );
                                            loop_exit = Some(LoopExit::ChangeRenderMode);
                                            break;
                                        }
                                        filter.update_region(mi.rows, mi.cols);
                                        filter.emit_region_setup(&mut out);
                                        // A full refresh is almost certainly arriving next,
                                        // don't output any other codes.
                                        // Consider ignoring StreamMetadataReason::Resize entirely.
                                    }
                                }
                            } else if m.reason == StreamMetadataReason::Closed as i32 {
                                if !pty_closed {
                                    pty_closed = true;
                                    super::move_terminal_end();
                                    eprint!("\r\n[PTY closed]\r\n");
                                }
                            }
                        }
                        _ => {}
                    },
                    _ => { break; }
                }
            }
            action = action_rx.recv() => {
                loop_exit = Some(LoopExit::Action(action.unwrap_or(super::InputAction::Detach)));
                break;
            }
            _ = sigwinch.recv() => {
                let (new_cols, new_rows) = super::get_terminal_size();
                if !super::server_fits_client(filter.server_cols, filter.server_rows, new_cols, new_rows) {
                    eprintln!(
                        "[region: client shrank to ({}x{}), smaller than server ({}x{}); \
                         switching to cell mode]",
                        new_cols, new_rows, filter.server_cols, filter.server_rows
                    );
                    loop_exit = Some(LoopExit::ChangeRenderMode);
                    break;
                }
                client_cols = new_cols;
                client_rows = new_rows;
                // TODO: Trigger debounced SubscribeUpdate RPC here to inform new_rows new_cols
                filter.update_client_size(new_rows, new_cols);
                filter.emit_region_setup(&mut out);
            }
        }
        if !out.is_empty() {
            if stdout.write_all(&out).await.is_err() { break; }
            let _ = stdout.flush().await;
        }
    }

    // Cleanup: restore client terminal margins
    let _ = stdout.write_all(b"\x1b[r").await;
    if filter.declrmm_active {
        let _ = stdout.write_all(b"\x1b[?69l").await;
    }
    let _ = stdout.flush().await;

    match loop_exit {
        None => Ok(super::RunOutcome::ServerClosed),
        Some(LoopExit::ChangeRenderMode) => Ok(super::RunOutcome::ChangeRenderMode(
            super::RenderMode::Cell,
            super::RunContext {
                resp_rx, cmd_tx, pty_id, item,
                refresh_gen: 0,
                refresh_bytes: vec![],
                buffered: vec![],
                action_rx,
            },
        )),
        Some(LoopExit::Action(action)) => Ok(super::RunOutcome::Action(
            action,
            super::RunContext {
                resp_rx, cmd_tx, pty_id, item,
                refresh_gen: current_refresh_gen,
                refresh_bytes: vec![], buffered: vec![],
                action_rx,
            },
        )),
    }
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
        // A CSI sequence longer than 32 bytes must be flushed as-is, not buffered forever.
        // Input is 34 bytes: buf reaches 33 before the final byte, triggering the > 32 limit.
        let long: Vec<u8> = b"\x1b[1;2;3;4;5;6;7;8;9;10;11;12;13;14".to_vec();
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

    #[test]
    fn decstbm_bare_reset_rewritten() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        // ESC [ r with no params = full-screen reset → rewritten to server bounds
        assert_eq!(filter_all(&mut f, b"\x1b[r"), b"\x1b[1;24r");
    }

    #[test]
    fn decstbm_bottom_clamped_to_server_rows() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        // Bottom margin (30) exceeds server_rows (24) → clamped
        assert_eq!(filter_all(&mut f, b"\x1b[1;30r"), b"\x1b[1;24r");
    }

    #[test]
    fn decstbm_within_bounds_unchanged() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        // Both margins within server bounds → rewritten to same values
        assert_eq!(filter_all(&mut f, b"\x1b[5;20r"), b"\x1b[5;20r");
    }

    #[test]
    fn decstbm_top_exceeds_server_rows_collapses() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        // top (25) > server_rows (24) → collapse to full region
        assert_eq!(filter_all(&mut f, b"\x1b[25;30r"), b"\x1b[1;24r");
    }

    #[test]
    fn decstbm_cross_buffer() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        let mut out = Vec::new();
        // Sequence split across two filter() calls — state must be preserved
        f.filter(b"\x1b[1;30", &mut out);
        f.filter(b"r", &mut out);
        assert_eq!(out, b"\x1b[1;24r");
    }

    #[test]
    fn decslrm_right_clamped_to_server_cols() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        // client_cols (120) > server_cols (80) → emit_region_setup sets declrmm_active
        let mut out = Vec::new();
        f.emit_region_setup(&mut out);
        out.clear();
        // Right margin (100) exceeds server_cols (80) → clamped
        f.filter(b"\x1b[1;100s", &mut out);
        assert_eq!(out, b"\x1b[1;80s");
    }

    #[test]
    fn declrmm_enable_suppressed() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        // Server app tries to enable DECLRMM — we own this mode, suppress it
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
        filter_all(&mut f, b"\x1b[?1049h"); // enter alt screen first
        let out = filter_all(&mut f, b"\x1b[?1049l");
        let s = String::from_utf8(out).unwrap();
        assert!(s.starts_with("\x1b[?1049l"), "alt-screen exit byte must pass through");
        assert!(s.contains("\x1b[1;24r"), "region setup must be re-emitted after exit");
        assert!(!f.in_alt_screen);
    }

    #[test]
    fn other_private_mode_passes_through() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        // ESC [ ? 2004 h = bracketed-paste mode — not intercepted
        assert_eq!(filter_all(&mut f, b"\x1b[?2004h"), b"\x1b[?2004h");
    }

    #[test]
    fn decslrm_left_exceeds_server_cols_collapses() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        let mut out = Vec::new();
        f.emit_region_setup(&mut out);
        out.clear();
        // left (90) > effective_right (80) → collapse to full region
        f.filter(b"\x1b[90;100s", &mut out);
        assert_eq!(out, b"\x1b[1;80s");
    }

    #[test]
    fn emit_region_setup_disables_declrmm_when_client_shrinks() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        // Initial setup: client_cols (120) > server_cols (80) → DECLRMM enabled
        let mut out = Vec::new();
        f.emit_region_setup(&mut out);
        assert!(f.declrmm_active);
        out.clear();
        // Client shrinks to match server width → DECLRMM must be disabled
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
        // DECSTR resets DECLRMM/DECSLRM — filter must re-establish them afterward
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
        // Server sends DECSTBM; some terminals reset horizontal margins on DECSTBM —
        // filter must re-emit DECSLRM immediately after the rewritten DECSTBM.
        f.filter(b"\x1b[r", &mut out);
        assert_eq!(out, b"\x1b[1;24r\x1b[1;80s");
    }
}
