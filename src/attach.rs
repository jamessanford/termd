use std::io::Write as IoWrite;

use anyhow::Result;
use libghostty_vt::{Terminal, TerminalOptions, RenderState};
use libghostty_vt::render::{Dirty, RowIterator, CellIterator};
use libghostty_vt::style::Underline;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;

use termd::proto::{
    terminal_command::Command, terminal_response::Response,
    PtyItem, RefreshRequest, SubscribeRequest, TerminalCommand,
    StreamMetadataReason,
    terminal_service_client::TerminalServiceClient,
};

type AuthedClient = TerminalServiceClient<
    tonic::service::interceptor::InterceptedService<
        tonic::transport::Channel,
        fn(Request<()>) -> Result<Request<()>, tonic::Status>,
    >,
>;

struct LocalTerminal {
    terminal: Terminal<'static, 'static>,
    render_state: RenderState<'static>,
    row_iter: RowIterator<'static>,
    cell_iter: CellIterator<'static>,
}

impl LocalTerminal {
    fn new(cols: u32, rows: u32) -> Result<Self> {
        Ok(Self {
            terminal: Terminal::new(TerminalOptions {
                cols: cols as u16,
                rows: rows as u16,
                max_scrollback: 0,
            })?,
            render_state: RenderState::new()?,
            row_iter: RowIterator::new()?,
            cell_iter: CellIterator::new()?,
        })
    }

    fn resize(&mut self, cols: u32, rows: u32) -> Result<()> {
        Ok(self.terminal.resize(cols as u16, rows as u16, 0, 0)?)
    }
}

/// Renders only dirty rows from the terminal state into `out` as VT sequences.
/// Uses absolute cursor positioning so output is correct regardless of client terminal size.
/// If `force_full` is true, all rows are rendered (used for SIGWINCH repaints).
/// Returns true if any output was produced.
fn render_dirty(
    terminal: &Terminal<'static, 'static>,
    render_state: &mut RenderState<'static>,
    row_iter: &mut RowIterator<'static>,
    cell_iter: &mut CellIterator<'static>,
    force_full: bool,
    out: &mut Vec<u8>,
) -> Result<bool> {
    let snapshot = render_state.update(terminal)?;

    let global_dirty = if force_full {
        snapshot.set_dirty(Dirty::Full)?;
        Dirty::Full
    } else {
        snapshot.dirty()?
    };

    if global_dirty == Dirty::Clean {
        return Ok(false);
    }

    let cursor_visible = snapshot.cursor_visible().unwrap_or(true);
    let (cursor_x, cursor_y) = match snapshot.cursor_viewport().ok().flatten() {
        Some(cv) => (cv.x as u32, cv.y as u32),
        None => (
            terminal.cursor_x().unwrap_or(0) as u32,
            terminal.cursor_y().unwrap_or(0) as u32,
        ),
    };

    let mut row_iter_active = row_iter.update(&snapshot)?;
    let mut row_idx: u32 = 0;
    let mut char_enc = [0u8; 4];
    let mut rendered_any = false;

    while let Some(row) = row_iter_active.next() {
        if global_dirty != Dirty::Full && !row.dirty()? {
            row_idx += 1;
            continue;
        }
        rendered_any = true;
        write!(out, "\x1b[{};1H", row_idx + 1).ok();

        let mut cell_iter_active = cell_iter.update(row)?;
        while let Some(cell) = cell_iter_active.next() {
            let style = cell.style()?;
            let fg = cell.fg_color().ok().flatten();
            let bg = cell.bg_color().ok().flatten();
            let graphemes = cell.graphemes()?;

            out.extend_from_slice(b"\x1b[0");
            if style.bold          { out.extend_from_slice(b";1"); }
            if style.faint         { out.extend_from_slice(b";2"); }
            if style.italic        { out.extend_from_slice(b";3"); }
            match style.underline {
                Underline::None   => {}
                Underline::Double => out.extend_from_slice(b";21"),
                _                 => out.extend_from_slice(b";4"),
            }
            if style.blink         { out.extend_from_slice(b";5"); }
            if style.inverse       { out.extend_from_slice(b";7"); }
            if style.invisible     { out.extend_from_slice(b";8"); }
            if style.strikethrough { out.extend_from_slice(b";9"); }
            if style.overline      { out.extend_from_slice(b";53"); }
            if let Some(c) = fg {
                write!(out, ";38;2;{};{};{}", c.r, c.g, c.b).ok();
            }
            if let Some(c) = bg {
                write!(out, ";48;2;{};{};{}", c.r, c.g, c.b).ok();
            }
            out.push(b'm');

            if graphemes.is_empty() {
                out.push(b' ');
            } else {
                for ch in &graphemes {
                    out.extend_from_slice(ch.encode_utf8(&mut char_enc).as_bytes());
                }
            }
        }
        row.set_dirty(false)?;
        row_idx += 1;
    }

    if rendered_any {
        out.extend_from_slice(b"\x1b[0m");
        if cursor_visible {
            out.extend_from_slice(b"\x1b[?25h");
        } else {
            out.extend_from_slice(b"\x1b[?25l");
        }
        write!(out, "\x1b[{};{}H", cursor_y + 1, cursor_x + 1).ok();
    }

    snapshot.set_dirty(Dirty::Clean)?;
    Ok(rendered_any)
}

pub async fn run(
    client: &mut AuthedClient,
    item: PtyItem,
    debug: bool,
) -> Result<()> {
    if debug {
        run_debug(client, item.pty_id).await
    } else {
        unimplemented!("normal attach mode not yet implemented")
    }
}

async fn run_debug(client: &mut AuthedClient, pty_id: String) -> Result<()> {
    let (cmd_tx, cmd_rx) = mpsc::channel::<TerminalCommand>(64);
    let mut resp_rx = client
        .stream(ReceiverStream::new(cmd_rx))
        .await?
        .into_inner();

    // Subscribe
    cmd_tx.send(TerminalCommand {
        command: Some(Command::Subscribe(SubscribeRequest { pty_id: pty_id.clone() })),
    }).await?;

    loop {
        match resp_rx.message().await? {
            None => { eprintln!("server disconnected during subscribe"); return Ok(()); }
            Some(r) => match r.response {
                Some(Response::Command(c)) => {
                    if !c.success {
                        eprintln!("subscribe failed: {}", c.error.unwrap_or_default());
                        return Ok(());
                    }
                    break;
                }
                _ => {}
            }
        }
    }

    // NOTE: no resize sent — server PTY owns its dimensions

    // Request refresh
    cmd_tx.send(TerminalCommand {
        command: Some(Command::Refresh(RefreshRequest { pty_id: pty_id.clone() })),
    }).await?;

    // Main debug receive loop — print events to stderr, no rendering
    loop {
        match resp_rx.message().await {
            Ok(Some(r)) => match r.response {
                Some(Response::Stream(s)) => {
                    eprintln!("[Stream gen={} len={}]", s.generation, s.data.len());
                }
                Some(Response::Refresh(rf)) => {
                    eprintln!("[Refresh gen={} len={}]", rf.generation, rf.data.len());
                }
                Some(Response::Metadata(m)) => {
                    eprintln!("[Metadata reason={} gen={} pty_id={}]", m.reason, m.generation, m.pty_id);
                    if m.reason == StreamMetadataReason::Closed as i32 {
                        eprintln!("[Connection closed]");
                        break;
                    }
                }
                _ => {}
            },
            _ => { eprintln!("[Connection closed]"); break; }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_dirty_produces_no_output_when_clean() {
        let mut lt = LocalTerminal::new(80, 24).unwrap();
        // Initial render to clear the startup-dirty state
        let mut out = Vec::new();
        render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, false, &mut out).unwrap();
        out.clear();
        // No changes — second render should produce nothing
        let changed = render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, false, &mut out).unwrap();
        assert!(!changed, "clean terminal should produce no render output");
        assert!(out.is_empty());
    }

    #[test]
    fn render_dirty_emits_only_changed_rows() {
        let mut lt = LocalTerminal::new(80, 24).unwrap();
        // Clear initial dirty state
        let mut out = Vec::new();
        render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, false, &mut out).unwrap();
        out.clear();

        // Write text — lands on row 0 (ANSI row 1)
        lt.terminal.vt_write(b"Hello");

        let changed = render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, false, &mut out).unwrap();
        assert!(changed);
        let s = String::from_utf8_lossy(&out);
        assert!(s.contains("\x1b[1;1H"), "row 1 cursor position expected");
        assert!(!s.contains("\x1b[2;1H"), "row 2 should not be re-rendered");
    }

    #[test]
    fn force_full_renders_all_rows() {
        let mut lt = LocalTerminal::new(80, 24).unwrap();
        // Clear initial dirty state
        let mut out = Vec::new();
        render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, false, &mut out).unwrap();
        out.clear();

        // Force full repaint with no terminal changes
        let changed = render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, true, &mut out).unwrap();
        assert!(changed, "force_full should produce output even with no changes");
        let s = String::from_utf8_lossy(&out);
        // All 24 rows should appear
        assert!(s.contains("\x1b[1;1H"));
        assert!(s.contains("\x1b[24;1H"));
    }
}
