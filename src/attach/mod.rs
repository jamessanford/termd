use std::io::Write;

use anyhow::Result;
use tokio::io::AsyncReadExt;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

mod input;

pub(super) enum InputAction {
    Detach,
    Destroy,
    Create,
    ForceResize,
    ForceRefresh,
    SwitchNext,
    SwitchPrevious,
    SwitchRecent,
    SwitchIndex(u8),
    ShowList,
    ShowInfo,
    ShowScrollback,
    ShowHelp,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum RenderMode {
    /// Cell-by-cell render state for all dirty states
    Cell,
    /// Raw PTY byte passthrough
    Raw,
    /// Raw passthrough within a DECSTBM scroll region
    Region,
    /// Raw passthrough with libghostty-driven explicit wrap injection
    Autowrap,
}

pub(super) enum PtyEvent<'a> {
    Stream { data: &'a [u8] },
    Refresh { cols: u32, rows: u32, data: &'a [u8] },
    Resize { cols: u32, rows: u32 },
    Closed,
}

pub(super) enum EventResult {
    Continue,
    ChangeRenderMode(RenderMode),
    RequestRefresh,
}

pub(super) trait RenderModeHandler {
    fn init(&mut self, refresh_data: &[u8], buffered: &[(u64, Vec<u8>)], out: &mut Vec<u8>) -> anyhow::Result<EventResult>;
    fn on_pty_event(&mut self, event: PtyEvent, out: &mut Vec<u8>) -> anyhow::Result<EventResult>;
    fn on_sigwinch(&mut self, cols: u32, rows: u32, out: &mut Vec<u8>) -> anyhow::Result<EventResult>;
    fn cleanup(&mut self, _out: &mut Vec<u8>) {}
}

fn create_handler(
    mode: RenderMode,
    server_cols: u32,
    server_rows: u32,
    upgrade_to: Option<RenderMode>,
) -> anyhow::Result<Box<dyn RenderModeHandler>> {
    Ok(match mode {
        RenderMode::Cell => Box::new(cell::CellHandler::new(server_cols, server_rows, upgrade_to)?),
        RenderMode::Raw => Box::new(raw::RawHandler::new()),
        RenderMode::Region => {
            let (client_cols, client_rows) = get_terminal_size();
            Box::new(region::RegionHandler::new(server_rows, server_cols, client_rows, client_cols))
        }
        RenderMode::Autowrap => Box::new(autowrap::AutowrapHandler::new(server_cols, server_rows)?),
    })
}

enum RunOutcome {
    ServerClosed,
    OutputClosed,
    PtyClosed,
    Action(InputAction),
}

use termd::proto::{
    terminal_command::Command, terminal_response::Response,
    CreateRequest, DestroyRequest, ListRequest, PtyItem, RefreshRequest, ResizeRequest,
    SubscribeRequest, UnsubscribeRequest, WriteRequest,
    TerminalCommand, StreamMetadataReason,
};

mod autowrap;
mod cell;
mod help;
mod raw;
mod region;
mod scrollback;

use crate::AuthedClient;

struct TerminalGuard {
    original: nix::sys::termios::Termios,
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        use nix::sys::termios::{tcsetattr, SetArg};
        let fd = unsafe { std::os::fd::BorrowedFd::borrow_raw(libc::STDIN_FILENO) };
        let _ = tcsetattr(fd, SetArg::TCSAFLUSH, &self.original);
    }
}

fn setup_raw_mode() -> Result<TerminalGuard> {
    use nix::sys::termios::{tcgetattr, tcsetattr, SetArg, LocalFlags, InputFlags};
    let fd = unsafe { std::os::fd::BorrowedFd::borrow_raw(libc::STDIN_FILENO) };
    let original = tcgetattr(fd)?;
    let mut raw = original.clone();
    raw.local_flags.remove(
        LocalFlags::ICANON | LocalFlags::ECHO | LocalFlags::ISIG | LocalFlags::IEXTEN,
    );
    raw.input_flags.remove(
        InputFlags::IXON | InputFlags::ICRNL | InputFlags::BRKINT
            | InputFlags::INPCK | InputFlags::ISTRIP,
    );
    raw.control_chars[libc::VMIN] = 1;
    raw.control_chars[libc::VTIME] = 0;
    tcsetattr(fd, SetArg::TCSAFLUSH, &raw)?;
    Ok(TerminalGuard { original })
}


pub(super) async fn show_info(msg: &str) {
    clear_screen();
    use std::io::Write;
    let _ = std::io::stderr().write_all(format!("\r\n[{msg}]\r\n").as_bytes());
    let _ = std::io::stderr().flush();
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
}

pub(super) async fn show_error(msg: &str) {
    show_info(&format!("Error: {}", msg)).await;
}

pub fn get_terminal_size() -> (u32, u32) {
    let mut ws = libc::winsize { ws_row: 0, ws_col: 0, ws_xpixel: 0, ws_ypixel: 0 };
    unsafe { libc::ioctl(libc::STDOUT_FILENO, libc::TIOCGWINSZ, &mut ws); }
    (ws.ws_col as u32, ws.ws_row as u32)
}

pub(super) fn server_fits_client(server_cols: u32, server_rows: u32, client_cols: u32, client_rows: u32) -> bool {
    client_cols >= server_cols && client_rows >= server_rows
}

// Escape sequences written to the client terminal to undo any PTY-set modes.
// Consider using a full "RIS" reset to initial state; keeping it specific like
// this does help us understand the missing gaps.
const RESET_TERMINAL_MODES: &str = concat!(
    "\x1b[?1049l",  // leave alternate screen mode
    "\x1b[?1000l",  // disable X10 mouse reporting
    "\x1b[?1002l",  // disable button-event mouse tracking
    "\x1b[?1003l",  // disable all-motion mouse tracking
    "\x1b[?1006l",  // disable SGR mouse extension
    "\x1b[?1016l",  // disable SGR-pixels mouse extension
    "\x1b[?1015l",  // disable urxvt mouse extension
    "\x1b[?1004l",  // disable focus event reporting
    "\x1b[?2004l",  // disable bracketed paste
    "\x1b[?6l",     // disable origin mode (DECOM)
    "\x1b[?5l",     // disable reverse video (DECSCNM)
    "\x1b[4l",      // disable insert mode (IRM)
    "\x1b[r",       // reset DECSTBM scroll region to full screen
    "\x1b[?69l",    // disable DECLRMM (horizontal margins)
    "\x1b[?7h",     // re-enable auto-wrap (DECAWM)
    "\x1b[0m",      // reset SGR (colors, attributes)
    // Both kitty keyboard and xterm modifyOtherKeys make the terminal emit CSI-u-style key
    // codes; clear both so the client UI (list/help/scrollback) gets normal keys.  CSI = 0 ; 1 u
    // resets the current kitty stack entry's flags to 0 (depth-independent — matches how
    // pty.rs:do_refresh restores via CSI = flags ; 1 u, where a plain pop would not); the pop
    // additionally drops a stack level pushed straight through from the server PTY.
    "\x1b[<1u",     // pop one kitty keyboard stack level (undo a passed-through push)
    "\x1b[=0;1u",   // reset current kitty keyboard flags to 0
    "\x1b[>4;0m",   // disable xterm modifyOtherKeys
    "\x1b[0 q",     // DECSCUSR: reset cursor shape to default (CSI Ps SP q — the space is required)
    "\x1b[?25h",    // show cursor
    "\x1b[?1l",     // DECCKM - normal cursor keys
    "\x1b>",        // DECNKM - normal keypad mode
    "\x1b(B",       // reset G0 character set to ASCII
    "\x1b)B",       // reset G1 character set to ASCII
    "\x1b*B",       // reset G2 character set to ASCII
    "\x1b+B",       // reset G3 character set to ASCII
    "\x0f",         // SI - shift in, invoke G0 into GL
);

// Disable any PTY-set terminal modes so client-side UI and new-PTY refreshes start clean.
// Called on every renderer exit (before ShowList, PTY switch, etc.) and also at session exit
// (where the caller appends the cursor-to-last-row tail).
fn reset_terminal_modes() {
    use std::io::Write;
    let _ = std::io::stdout().write_all(RESET_TERMINAL_MODES.as_bytes());
    let _ = std::io::stdout().flush();
}

fn move_terminal_end() {
    use std::io::Write;
    let (_, rows) = get_terminal_size();
    let _ = std::io::stdout().write_all(
        format!("\x1b[{rows};1H\r\n").as_bytes() // move cursor to last row
    );
    let _ = std::io::stdout().flush();
}

async fn subscribe(
    cmd_tx:  &mpsc::Sender<TerminalCommand>,
    resp_rx: &mut tonic::Streaming<termd::proto::TerminalResponse>,
    pty_id:  u64,
) -> anyhow::Result<bool> {
    let (cols, rows) = get_terminal_size();
    cmd_tx.send(TerminalCommand {
        command: Some(Command::Subscribe(SubscribeRequest {
            pty_id,
            hostname: hostname::get().unwrap_or_default().to_string_lossy().into_owned(),
            cols,
            rows,
        })),
    }).await?;
    loop {
        match resp_rx.message().await? {
            None => anyhow::bail!("server disconnected during subscribe"),
            Some(r) => match r.response {
                Some(Response::Subscribe(c)) if c.pty_id == pty_id && c.success => return Ok(true),
                Some(Response::Subscribe(c)) if c.pty_id == pty_id => {
                    show_error(&format!("subscribe failed: {}", c.error.unwrap_or_default())).await;
                    return Ok(false);
                }
                _ => {}
            }
        }
    }
}

/// A PTY event buffered after the refresh snapshot: its generation and raw bytes.
type BufferedEvent = (u64, Vec<u8>);

/// Result of a refresh: the refresh generation, the rendered screen bytes, and
/// any events that arrived (and were buffered) after that snapshot.
type RefreshSnapshot = (u64, Vec<u8>, Vec<BufferedEvent>);

// Returns Ok(None) if the server returned an error for this PTY (e.g. reader thread dead).
// Returns Ok(Some(...)) on success. Returns Err on transport failure.
async fn request_refresh(
    cmd_tx:  &mpsc::Sender<TerminalCommand>,
    resp_rx: &mut tonic::Streaming<termd::proto::TerminalResponse>,
    pty_id:  u64,
) -> anyhow::Result<Option<RefreshSnapshot>> {
    cmd_tx.send(TerminalCommand {
        command: Some(Command::Refresh(RefreshRequest { pty_id })),
    }).await?;
    let mut buffered: Vec<BufferedEvent> = Vec::new();
    loop {
        match resp_rx.message().await? {
            None => anyhow::bail!("server disconnected during refresh"),
            Some(r) => match r.response {
                Some(Response::Refresh(rf)) if rf.pty_id == pty_id => return Ok(Some((rf.generation, rf.data, buffered))),
                Some(Response::Stream(s))   if s.pty_id  == pty_id => buffered.push((s.generation, s.data)),
                Some(Response::Command(c)) if c.pty_id == pty_id && !c.success => return Ok(None),
                _ => {}
            }
        }
    }
}

async fn fetch_list(
    cmd_tx:   &mpsc::Sender<TerminalCommand>,
    resp_rx:  &mut tonic::Streaming<termd::proto::TerminalResponse>,
    pty_list: &mut Vec<PtyItem>,
) -> anyhow::Result<()> {
    cmd_tx.send(TerminalCommand {
        command: Some(Command::List(ListRequest {})),
    }).await?;
    loop {
        match resp_rx.message().await? {
            None => anyhow::bail!("server disconnected during list"),
            Some(r) => if let Some(Response::List(lr)) = r.response {
                *pty_list = lr.items;
                pty_list.sort_by_key(|p| p.sort_order);
                return Ok(());
            }
        }
    }
}

// Fetches updated pty_list. Returns false and shows an error message
// if the fetch fails; the caller should continue 'session.
async fn ensure_list(
    cmd_tx:   &mpsc::Sender<TerminalCommand>,
    resp_rx:  &mut tonic::Streaming<termd::proto::TerminalResponse>,
    pty_list: &mut Vec<PtyItem>,
) -> bool {
    if let Err(e) = fetch_list(cmd_tx, resp_rx, pty_list).await {
        show_error(&e.to_string()).await;
        return false;
    }
    true
}

async fn destroy_and_drain(
    cmd_tx:  &mpsc::Sender<TerminalCommand>,
    resp_rx: &mut tonic::Streaming<termd::proto::TerminalResponse>,
    pty_id:  u64,
) -> anyhow::Result<()> {
    cmd_tx.send(TerminalCommand {
        command: Some(Command::Destroy(DestroyRequest { pty_id })),
    }).await?;
    loop {
        match resp_rx.message().await? {
            None => anyhow::bail!("server disconnected"),
            Some(r) => if let Some(Response::Command(c)) = r.response {
                if c.pty_id != pty_id { continue; }
                if !c.success {
                    anyhow::bail!("Failed to destroy PTY: {}", c.error.unwrap_or_default());
                }
                break;
            }
        }
    }
    Ok(())
}

async fn recent_pty<'a>(list: &'a [PtyItem], previous_pty_id: &Option<u64>, current_pty_id: u64) -> Option<&'a PtyItem> {
    let prev_id = (*previous_pty_id)?;
    if let Some(item) = list.iter().find(|p| p.pty_id == prev_id) {
        Some(item)
    } else {
        let best = list.iter()
            .filter(|p| p.pty_id != current_pty_id)
            .max_by_key(|p| {
                let ts = p.last_subscribed_at.as_ref().or(p.created_at.as_ref());
                ts.map(|t| (t.seconds, t.nanos)).unwrap_or((0, 0))
            });
        if best.is_none() {
            show_info("No other PTYs").await;
        }
        best.or_else(|| list.first())
    }
}


fn next_pty(list: &[PtyItem], current_id: u64) -> Option<&PtyItem> {
    if list.is_empty() { return None; }
    let pos = list.iter().position(|p| p.pty_id == current_id).unwrap_or(0);
    Some(&list[(pos + 1) % list.len()])
}

fn prev_pty(list: &[PtyItem], current_id: u64) -> Option<&PtyItem> {
    if list.is_empty() { return None; }
    let pos = list.iter().position(|p| p.pty_id == current_id).unwrap_or(0);
    Some(&list[(pos + list.len() - 1) % list.len()])
}

// Unsubscribes from the current PTY, updates session state.
async fn switch_pty(
    cmd_tx:          &mpsc::Sender<TerminalCommand>,
    current_pty_id:  &mut u64,
    current_item:    &mut PtyItem,
    previous_pty_id: &mut Option<u64>,
    new_item:        PtyItem,
) {
    let _ = cmd_tx.send(TerminalCommand {
        command: Some(Command::Unsubscribe(UnsubscribeRequest {
            pty_id: *current_pty_id,
        })),
    }).await;
    *previous_pty_id = Some(std::mem::replace(current_pty_id, new_item.pty_id));
    *current_item = new_item;
}

fn clear_screen() {
    use std::io::Write;
    let _ = std::io::stdout().write_all(b"\x1b[2J\x1b[H");
    let _ = std::io::stdout().flush();
}

fn draw_list(items: &[PtyItem], selected: usize) {
    use std::io::Write;
    let mut out = Vec::new();
    out.extend_from_slice(b"\x1b[2J\x1b[H");
    for (i, item) in items.iter().enumerate() {
        if i == selected { out.extend_from_slice(b"\x1b[7m"); }
        let title = if item.title.is_empty() { &item.pts_name } else { &item.title };
        let pty_id_hex = format!("{:016x}", item.pty_id);
        let title_trunc: String = title.chars().take(32).collect();
        let line = format!(
            " {:>3}  {:<16}  {:<32}  {}x{}\r\n",
            item.sort_order,
            pty_id_hex,
            title_trunc,
            item.cols, item.rows,
        );
        out.extend_from_slice(line.as_bytes());
        if i == selected { out.extend_from_slice(b"\x1b[0m"); }
    }
    let _ = std::io::stdout().write_all(&out);
    let _ = std::io::stdout().flush();
}

async fn show_list(
    cmd_tx:          &mpsc::Sender<TerminalCommand>,
    resp_rx:         &mut tonic::Streaming<termd::proto::TerminalResponse>,
    pty_list:        &mut Vec<PtyItem>,
    current_pty_id:  u64,
    stdin:           &mut tokio::io::Stdin,
) -> anyhow::Result<Option<u64>> {
    // Returns Some(new_pty_id) on selection, None on cancel.
    if let Err(e) = fetch_list(cmd_tx, resp_rx, pty_list).await {
        show_error(&e.to_string()).await;
        return Ok(None);
    }
    if pty_list.is_empty() {
        show_info("No PTYs in this session").await;
        return Ok(None);
    }

    let mut selected = pty_list
        .iter()
        .position(|p| p.pty_id == current_pty_id)
        .unwrap_or_else(|| {
            pty_list
                .iter()
                .enumerate()
                .max_by_key(|(_, p)| {
                    let ts = p.last_subscribed_at.as_ref().or(p.created_at.as_ref());
                    ts.map(|t| (t.seconds, t.nanos)).unwrap_or((0, 0))
                })
                .map(|(i, _)| i)
                .unwrap_or(0)
        });

    draw_list(pty_list, selected);

    let mut buf = [0u8; 8];

    loop {
        let n = match stdin.read(&mut buf).await {
            Ok(0) | Err(_) => return Ok(None),
            Ok(n) => n,
        };

        match &buf[..n] {
            // Enter — select
            [b'\r'] | [b'\n'] => {
                clear_screen();
                return Ok(Some(pty_list[selected].pty_id));
            }
            // Arrow keys arrive as 3-byte ESC sequences; match the whole read
            [0x1b, b'[', b'A', ..] => {
                selected = selected.saturating_sub(1);
                draw_list(pty_list, selected);
            }
            [0x1b, b'[', b'B', ..] => {
                if selected + 1 < pty_list.len() { selected += 1; }
                draw_list(pty_list, selected);
            }
            // Bare ESC: try to read 2 more bytes within 50 ms to rule out
            // a split arrow-key sequence. Timeout means it really was bare ESC.
            [0x1b] => {
                let mut rest = [0u8; 2];
                let is_arrow = tokio::time::timeout(
                    std::time::Duration::from_millis(50),
                    stdin.read(&mut rest),
                ).await
                .ok()
                .and_then(|r| r.ok())
                .map(|n2| &rest[..n2] == b"[A" || &rest[..n2] == b"[B")
                .unwrap_or(false);

                if is_arrow {
                    if rest[1] == b'A' {
                        selected = selected.saturating_sub(1);
                    } else {
                        if selected + 1 < pty_list.len() { selected += 1; }
                    }
                    draw_list(pty_list, selected);
                } else {
                    // Bare escape — cancel
                    clear_screen();
                    return Ok(None);
                }
            }
            _ => {}
        }
    }
}

fn draw_idle() {
    use std::io::Write;
    let mut out = Vec::new();
    out.extend_from_slice(b"\x1b[2J\x1b[H");
    out.extend_from_slice(b"Not attached to a PTY.\r\n\r\n");
    out.extend_from_slice(b"  C-a \"      show list of PTYs\r\n");
    out.extend_from_slice(b"  C-a c      create new PTY\r\n");
    out.extend_from_slice(b"  C-a ?      show keybindings\r\n");
    out.extend_from_slice(b"  C-a d      detach from termd\r\n");
    let _ = std::io::stdout().write_all(&out);
    let _ = std::io::stdout().flush();
}

// Wait state: no PTY is attached. Draw an idle screen and wait for the user to
// act. Input runs through the same InputProcessor as the render loop, so C-a
// bindings (create, list, switch, detach, …) work here too — unlike the modal
// screens (show_list/help/scrollback), which read stdin raw and can't trigger
// them. Returns a RunOutcome for the shared dispatch in `run`.
async fn run_idle(
    resp_rx: &mut tonic::Streaming<termd::proto::TerminalResponse>,
    stdin:   &mut tokio::io::Stdin,
    input:   &mut input::InputProcessor,
) -> anyhow::Result<RunOutcome> {
    draw_idle();
    let mut sigwinch = signal(SignalKind::window_change())?;
    let mut input_buf = [0u8; 256];
    loop {
        tokio::select! {
            msg = resp_rx.message() => {
                // No subscription here, so any PTY traffic is stale — ignore it.
                // Only a closed/errored stream matters.
                if !matches!(msg, Ok(Some(_))) {
                    return Ok(RunOutcome::ServerClosed);
                }
            }
            result = stdin.read(&mut input_buf) => {
                let n = match result {
                    Ok(0) | Err(_) => return Ok(RunOutcome::Action(InputAction::Detach)),
                    Ok(n) => n,
                };
                // Drop r.write: with no PTY there's nowhere to send keystrokes.
                if let Some(a) = input.process(&input_buf[..n]).action {
                    return Ok(RunOutcome::Action(a));
                }
            }
            _ = sigwinch.recv() => {
                draw_idle();
            }
        }
    }
}

/// Status line shown on the client's terminal while reconnecting. The caller
/// has already reset the terminal (left the alt screen, cleared modes), so we
/// just clear and print at the top of the main screen.
fn reconnect_status(msg: &str) {
    use std::io::Write;
    let mut out = std::io::stdout();
    let _ = out.write_all(format!("\x1b[2J\x1b[H[{msg}]\r\n").as_bytes());
    let _ = out.flush();
}

/// Re-establish the bidirectional command/response stream after a transport
/// failure, retrying with capped exponential backoff until it succeeds. A
/// freshly opened stream can report success before the underlying connection is
/// actually usable (lazily-reconnecting tonic `Channel`s do this), so we confirm
/// liveness with a `List` round-trip before handing the stream back — otherwise
/// a dead-but-"Ok" stream would spin the caller's reconnect path with no delay.
///
/// While waiting between attempts the user can press Ctrl-C or `q` to give up.
/// Returns the fresh `(cmd_tx, resp_rx)` once connected, or `None` if the user
/// aborted (or stdin closed). Re-subscribing and repainting is the caller's job.
async fn reconnect(
    client: &mut AuthedClient,
    stdin:  &mut tokio::io::Stdin,
) -> Option<(
    mpsc::Sender<TerminalCommand>,
    tonic::Streaming<termd::proto::TerminalResponse>,
)> {
    let mut backoff = std::time::Duration::from_millis(500);
    let max_backoff = std::time::Duration::from_secs(5);
    let mut buf = [0u8; 8];
    let mut attempt: u32 = 0;
    loop {
        attempt += 1;
        reconnect_status(&format!(
            "Connection lost — reconnecting (attempt {attempt})…  Ctrl-C/q to quit"
        ));
        let (cmd_tx, cmd_rx) = mpsc::channel::<TerminalCommand>(64);
        let probe = match client.stream(ReceiverStream::new(cmd_rx)).await {
            Ok(resp) => {
                let mut resp_rx = resp.into_inner();
                let mut throwaway = Vec::new();
                // A real round-trip also re-runs the per-stream auth handshake,
                // so a successful List confirms the link is genuinely back.
                fetch_list(&cmd_tx, &mut resp_rx, &mut throwaway)
                    .await
                    .map(|()| (cmd_tx, resp_rx))
            }
            Err(status) => Err(anyhow::anyhow!("{}", status.message())),
        };
        match probe {
            Ok(conn) => return Some(conn),
            Err(e) => reconnect_status(&format!(
                "Reconnect failed: {e} — retrying in {:.1}s  (Ctrl-C/q to quit)",
                backoff.as_secs_f32(),
            )),
        }
        // Wait out the backoff, but let the user abort with Ctrl-C / q.
        tokio::select! {
            _ = tokio::time::sleep(backoff) => {}
            r = stdin.read(&mut buf) => match r {
                Ok(0) | Err(_) => return None,
                Ok(n) if buf[..n].iter().any(|&b| b == 0x03 || b == b'q') => return None,
                _ => {}
            }
        }
        backoff = (backoff * 2).min(max_backoff);
    }
}

pub async fn run(
    client: &mut AuthedClient,
    item: PtyItem,
    debug: bool,
    mode: RenderMode,
) -> Result<()> {
    if debug {
        return run_debug(client, item.pty_id).await;
    }

    let (mut cmd_tx, cmd_rx) = mpsc::channel::<TerminalCommand>(64);
    let mut resp_rx = client
        .stream(ReceiverStream::new(cmd_rx))
        .await?
        .into_inner();

    let _guard = setup_raw_mode()?;

    let mut current_pty_id = item.pty_id;
    let mut current_item = item;
    let mut pty_list: Vec<PtyItem> = Vec::new();
    let mut previous_pty_id: Option<u64> = None;
    let mut subscribed_pty_id: Option<u64> = None;

    let upgrade_to = match mode {
        RenderMode::Region => Some(RenderMode::Region),
        RenderMode::Autowrap => Some(RenderMode::Autowrap),
        _ => None,
    };
    let mut dispatch_mode = mode;
    let mut stdout = std::io::stdout();
    let mut stdin = tokio::io::stdin();
    let mut input = input::InputProcessor::new();
    let mut out = Vec::new();
    let mut skip_subscribe = false;

    // On a transport failure, re-establish the stream (retrying with backoff)
    // and restart the session loop so we re-subscribe and repaint the current
    // PTY. If the user aborts the reconnect, leave the session cleanly.
    // The loop label is passed in because `macro_rules!` labels are hygienic and
    // can't otherwise see the caller's `'session`.
    macro_rules! do_reconnect {
        ($lt:lifetime) => {
            match reconnect(client, &mut stdin).await {
                Some((tx, rx)) => {
                    cmd_tx = tx;
                    resp_rx = rx;
                    subscribed_pty_id = None;
                    continue $lt;
                }
                None => break $lt,
            }
        };
    }
    // Unwrap a command-helper Result, treating any Err as a transport failure
    // that triggers a reconnect. (Helpers signal logical failures as Ok values,
    // so Err is always the stream going away.)
    macro_rules! reconnect_or_break {
        ($lt:lifetime, $e:expr) => {
            match $e {
                Ok(v) => v,
                Err(_) => do_reconnect!($lt),
            }
        };
    }

    'session: loop {
        let (refresh_gen, refresh_bytes, buffered): RefreshSnapshot = 'refresh: {
        if skip_subscribe {
            skip_subscribe = false;
            break 'refresh (0, vec![], vec![]);
        }

        let subscribe_ok = if subscribed_pty_id != Some(current_pty_id) {
            let ok = reconnect_or_break!('session, subscribe(&cmd_tx, &mut resp_rx, current_pty_id).await);
            if ok { subscribed_pty_id = Some(current_pty_id); }
            ok
        } else {
            true
        };

        let refresh_result = if subscribe_ok {
            reconnect_or_break!('session, request_refresh(&cmd_tx, &mut resp_rx, current_pty_id).await)
        } else {
            None
        };

        match refresh_result {
            Some(triple) => triple,
            None => {
                subscribed_pty_id = None;
                pty_list.clear();
                let _ = destroy_and_drain(&cmd_tx, &mut resp_rx, current_pty_id).await;
                match show_list(&cmd_tx, &mut resp_rx, &mut pty_list, current_pty_id, &mut stdin).await? {
                    Some(new_id) => {
                        if let Some(target) = pty_list.iter().find(|p| p.pty_id == new_id).cloned() {
                            switch_pty(&cmd_tx, &mut current_pty_id, &mut current_item, &mut previous_pty_id, target).await;
                            pty_list.clear();
                        }
                        continue 'session;
                    }
                    None => (0, vec![], vec![]),  // Wait for user to tell us what to do next.
                }
            }
        }
        };
        // Without a live subscription there's no PTY to render: show the idle
        // screen and wait for the user to act. run_idle routes input through the
        // same InputProcessor/InputAction path as the render loop, so the shared
        // dispatch below handles either outcome identically.
        let outcome: RunOutcome = if subscribed_pty_id != Some(current_pty_id) {
            run_idle(&mut resp_rx, &mut stdin, &mut input).await?
        } else {
            let mut current_refresh_gen = refresh_gen;

            let buffered: Vec<_> = buffered.into_iter()
                .filter(|(gen, _)| *gen > current_refresh_gen)
                .collect();

            let mut handler: Box<dyn RenderModeHandler> = create_handler(
                dispatch_mode, current_item.cols, current_item.rows, upgrade_to,
            )?;

            out.clear();
            if let EventResult::ChangeRenderMode(new_mode) = handler.init(&refresh_bytes, &buffered, &mut out)? {
                handler.cleanup(&mut out);
                if !out.is_empty() {
                    stdout.write_all(&out)?;
                    out.clear();
                }
                dispatch_mode = new_mode;
                handler = create_handler(dispatch_mode, current_item.cols, current_item.rows, upgrade_to)?;
                handler.init(&refresh_bytes, &buffered, &mut out)?;
            }
            if !out.is_empty() {
                stdout.write_all(&out)?;
                stdout.flush()?;
            }

            let mut sigwinch = signal(SignalKind::window_change())?;
            let mut refresh_debounce = Box::pin(tokio::time::sleep(std::time::Duration::from_secs(86400)));
            let mut debounce_active = false;
            let mut input_buf = [0u8; 256];

            loop {
                out.clear();
                let mut change_mode: Option<(RenderMode, Vec<u8>)> = None;

                tokio::select! {
                    msg = resp_rx.message() => {
                        match msg {
                            Ok(Some(r)) => match r.response {
                                Some(Response::Stream(s)) if s.pty_id == current_pty_id && s.generation > current_refresh_gen => {
                                    let result = handler.on_pty_event(PtyEvent::Stream { data: &s.data }, &mut out)?;
                                    if let EventResult::ChangeRenderMode(m) = result {
                                        change_mode = Some((m, vec![]));
                                    }
                                }
                                Some(Response::Refresh(rf)) if rf.pty_id == current_pty_id => {
                                    current_refresh_gen = rf.generation;
                                    current_item.cols = rf.cols;
                                    current_item.rows = rf.rows;
                                    let result = handler.on_pty_event(
                                        PtyEvent::Refresh { cols: rf.cols, rows: rf.rows, data: &rf.data },
                                        &mut out,
                                    )?;
                                    if let EventResult::ChangeRenderMode(m) = result {
                                        change_mode = Some((m, rf.data));
                                    }
                                }
                                Some(Response::Metadata(m)) if m.pty_id == current_pty_id => {
                                    if m.reason == StreamMetadataReason::Resize as i32 {
                                        if let Some(ref mi) = m.item {
                                            if mi.cols > 0 && mi.rows > 0 {
                                                current_item.cols = mi.cols;
                                                current_item.rows = mi.rows;
                                                let result = handler.on_pty_event(
                                                    PtyEvent::Resize { cols: mi.cols, rows: mi.rows },
                                                    &mut out,
                                                )?;
                                                if let EventResult::ChangeRenderMode(m) = result {
                                                    change_mode = Some((m, vec![]));
                                                }
                                            }
                                        }
                                    } else if m.reason == StreamMetadataReason::Closed as i32 {
                                        handler.on_pty_event(PtyEvent::Closed, &mut out)?;
                                        input.reset();
                                        break RunOutcome::PtyClosed;
                                    }
                                }
                                _ => {}
                            },
                            _ => { break RunOutcome::ServerClosed; }
                        }
                    }
                    result = stdin.read(&mut input_buf) => {
                        let n = match result {
                            Ok(0) | Err(_) => break RunOutcome::Action(InputAction::Detach),
                            Ok(n) => n,
                        };
                        let r = input.process(&input_buf[..n]);
                        if !r.write.is_empty() {
                            let _ = cmd_tx.send(TerminalCommand {
                                command: Some(Command::Write(WriteRequest {
                                    pty_id: current_pty_id,
                                    data: r.write,
                                })),
                            }).await;
                        }
                        if let Some(a) = r.action {
                            break RunOutcome::Action(a);
                        }
                    }
                    _ = sigwinch.recv() => {
                        let (cols, rows) = get_terminal_size();
                        match handler.on_sigwinch(cols, rows, &mut out)? {
                            EventResult::ChangeRenderMode(m) => {
                                change_mode = Some((m, vec![]));
                            }
                            EventResult::RequestRefresh => {
                                refresh_debounce.as_mut().reset(
                                    tokio::time::Instant::now() + std::time::Duration::from_secs(1)
                                );
                                debounce_active = true;
                            }
                            EventResult::Continue => {}
                        }
                    }
                    _ = &mut refresh_debounce, if debounce_active => {
                        debounce_active = false;
                        // Re-subscribe with the current size so the server can refit the PTY
                        // to all subscribers. handle_subscribe upserts (it's idempotent for an
                        // already-subscribed client) and recomputes best-fit; if it resizes, the
                        // resulting Resize broadcast re-renders us.
                        let (cols, rows) = get_terminal_size();
                        let _ = cmd_tx.send(TerminalCommand {
                            command: Some(Command::Subscribe(SubscribeRequest {
                                pty_id: current_pty_id,
                                hostname: hostname::get().unwrap_or_default().to_string_lossy().into_owned(),
                                cols,
                                rows,
                            })),
                        }).await;
                        // Always refresh regardless of whether the size changed: a SIGWINCH storm
                        // can leave the user's terminal visually garbled even when it settles back
                        // to the same dimensions, so we repaint unconditionally.
                        let _ = cmd_tx.send(TerminalCommand {
                            command: Some(Command::Refresh(RefreshRequest {
                                pty_id: current_pty_id,
                            })),
                        }).await;
                    }
                }

                if let Some((new_mode, refresh_data)) = change_mode {
                    handler.cleanup(&mut out);
                    dispatch_mode = new_mode;
                    handler = create_handler(dispatch_mode, current_item.cols, current_item.rows, upgrade_to)?;
                    let init_result = handler.init(&refresh_data, &[], &mut out)?;
                    if let EventResult::ChangeRenderMode(fallback) = init_result {
                        handler.cleanup(&mut out);
                        dispatch_mode = fallback;
                        handler = create_handler(dispatch_mode, current_item.cols, current_item.rows, upgrade_to)?;
                        handler.init(&refresh_data, &[], &mut out)?;
                    }
                    // A SIGWINCH-driven switch hands off empty data and doesn't resize the
                    // server, so no Refresh follows on its own — the new handler would paint
                    // a stale/blank screen. Request one so a full repaint comes down the pipe.
                    // (Resize-driven switches are also empty but already get a server Refresh;
                    // an extra request there is a harmless idempotent repaint.) We only reach
                    // this branch with a live subscription, so the PTY is always there to ask.
                    if refresh_data.is_empty() {
                        let _ = cmd_tx.send(TerminalCommand {
                            command: Some(Command::Refresh(RefreshRequest { pty_id: current_pty_id })),
                        }).await;
                    }
                }

                if !out.is_empty() {
                    if stdout.write_all(&out).is_err() { break RunOutcome::OutputClosed; }
                    let _ = stdout.flush();
                }
            }
        };

        match outcome {
            RunOutcome::ServerClosed => {
                // Leave the alt screen / clear PTY modes so the reconnect status
                // shows on a clean main screen, then retry the transport.
                reset_terminal_modes();
                do_reconnect!('session);
            }
            RunOutcome::OutputClosed => {
                // Our own stdout died (the client terminal went away). Reconnecting
                // the server stream wouldn't help — just exit cleanly.
                reset_terminal_modes();
                break 'session;
            }
            RunOutcome::PtyClosed => {
                reset_terminal_modes();
                subscribed_pty_id = None;
                pty_list.clear();
                let _ = destroy_and_drain(&cmd_tx, &mut resp_rx, current_pty_id).await;
                match show_list(&cmd_tx, &mut resp_rx, &mut pty_list, current_pty_id, &mut stdin).await? {
                    Some(new_id) => {
                        if let Some(target) = pty_list.iter().find(|p| p.pty_id == new_id).cloned() {
                            switch_pty(&cmd_tx, &mut current_pty_id, &mut current_item, &mut previous_pty_id, target).await;
                            pty_list.clear();
                        }
                    }
                    None => { skip_subscribe = true; }
                }
                continue 'session;
            }
            RunOutcome::Action(action) => {
                reset_terminal_modes();
                match action {
                    InputAction::Detach => break 'session,

                    InputAction::Destroy => {
                        if let Err(e) = destroy_and_drain(&cmd_tx, &mut resp_rx, current_pty_id).await {
                            show_error(&e.to_string()).await;
                            continue 'session;
                        }
                        subscribed_pty_id = None;
                        pty_list.clear();
                        let auto_target = if ensure_list(&cmd_tx, &mut resp_rx, &mut pty_list).await {
                            recent_pty(&pty_list, &previous_pty_id, current_pty_id).await.cloned()
                        } else {
                            None
                        };
                        match auto_target {
                            Some(target) if target.pty_id != current_pty_id => {
                                switch_pty(&cmd_tx, &mut current_pty_id, &mut current_item, &mut previous_pty_id, target).await;
                                pty_list.clear();
                            }
                            _ => { skip_subscribe = true; }
                        }
                    }

                    InputAction::ForceResize => {
                        let (cols, rows) = get_terminal_size();
                        let _ = cmd_tx.send(TerminalCommand {
                            command: Some(Command::Resize(ResizeRequest {
                                pty_id: current_pty_id, cols, rows,
                            })),
                        }).await;
                    }

                    InputAction::ForceRefresh => {
                        let _ = cmd_tx.send(TerminalCommand {
                            command: Some(Command::Refresh(RefreshRequest {
                                pty_id: current_pty_id,
                            })),
                        }).await;
                    }

                    InputAction::Create => {
                        let (cols, rows) = get_terminal_size();
                        reconnect_or_break!('session, cmd_tx.send(TerminalCommand {
                            command: Some(Command::Create(CreateRequest {
                                cols, rows, command: None,
                            })),
                        }).await);
                        'create: loop {
                            match resp_rx.message().await {
                                Ok(Some(r)) => if let Some(Response::Create(cr)) = r.response {
                                    match cr.item {
                                        Some(new_item) => {
                                            switch_pty(&cmd_tx, &mut current_pty_id, &mut current_item, &mut previous_pty_id, new_item).await;
                                            break 'create;
                                        }
                                        None => {
                                            show_error("Failed to create new PTY").await;
                                            pty_list.clear();
                                            continue 'session;
                                        }
                                    }
                                },
                                // Stream closed (None) or errored: reconnect and restart the session.
                                _ => do_reconnect!('session),
                            }
                        }
                        pty_list.clear();
                    }

                    InputAction::SwitchNext => {
                        if !ensure_list(&cmd_tx, &mut resp_rx, &mut pty_list).await {
                            continue 'session;
                        }
                        if let Some(target) = next_pty(&pty_list, current_pty_id).cloned() {
                            if target.pty_id != current_pty_id {
                                switch_pty(&cmd_tx, &mut current_pty_id, &mut current_item, &mut previous_pty_id, target).await;
                            }
                        }
                    }

                    InputAction::SwitchPrevious => {
                        if !ensure_list(&cmd_tx, &mut resp_rx, &mut pty_list).await {
                            continue 'session;
                        }
                        if let Some(target) = prev_pty(&pty_list, current_pty_id).cloned() {
                            if target.pty_id != current_pty_id {
                                switch_pty(&cmd_tx, &mut current_pty_id, &mut current_item, &mut previous_pty_id, target).await;
                            }
                        }
                    }

                    InputAction::SwitchRecent => {
                        if ensure_list(&cmd_tx, &mut resp_rx, &mut pty_list).await {
                            if let Some(target) = recent_pty(&pty_list, &previous_pty_id, current_pty_id).await.cloned() {
                                if target.pty_id != current_pty_id {
                                    switch_pty(&cmd_tx, &mut current_pty_id, &mut current_item, &mut previous_pty_id, target).await;
                                }
                            }
                        }
                    }

                    InputAction::SwitchIndex(n) => {
                        if !ensure_list(&cmd_tx, &mut resp_rx, &mut pty_list).await {
                            continue 'session;
                        }
                        if let Some(target) = pty_list.get(n as usize).cloned() {
                            if target.pty_id != current_pty_id {
                                switch_pty(&cmd_tx, &mut current_pty_id, &mut current_item, &mut previous_pty_id, target).await;
                            }
                        }
                    }

                    InputAction::ShowList => {
                        match show_list(&cmd_tx, &mut resp_rx, &mut pty_list, current_pty_id, &mut stdin).await? {
                            Some(new_id) if new_id != current_pty_id => {
                                if let Some(target) = pty_list.iter().find(|p| p.pty_id == new_id).cloned() {
                                    switch_pty(&cmd_tx, &mut current_pty_id, &mut current_item, &mut previous_pty_id, target).await;
                                    pty_list.clear();
                                }
                            }
                            _ => {}
                        }
                    }

                    InputAction::ShowInfo => {
                        let (client_cols, client_rows) = get_terminal_size();
                        show_info(&format!(
                            "requested={mode:?} actual={dispatch_mode:?} pty={current_pty_id:016x} \
                             server={server_cols}x{server_rows} client={client_cols}x{client_rows}",
                            server_cols = current_item.cols,
                            server_rows = current_item.rows,
                        )).await;
                    }

                    InputAction::ShowScrollback => {
                        scrollback::show_scrollback(
                            &cmd_tx,
                            &mut resp_rx,
                            current_pty_id,
                            current_item.rows,
                            &mut stdin,
                        ).await?;
                    }

                    InputAction::ShowHelp => {
                        help::show_help(&mut stdin).await;
                    }
                }
            }
        }
    }

    move_terminal_end();
    drop(_guard);
    Ok(())
}

async fn run_debug(client: &mut AuthedClient, pty_id: u64) -> Result<()> {
    let (cmd_tx, cmd_rx) = mpsc::channel::<TerminalCommand>(64);
    let mut resp_rx = client
        .stream(ReceiverStream::new(cmd_rx))
        .await?
        .into_inner();

    // Subscribe
    let (cols, rows) = get_terminal_size();
    cmd_tx.send(TerminalCommand {
        command: Some(Command::Subscribe(SubscribeRequest {
            pty_id,
            hostname: hostname::get().unwrap_or_default().to_string_lossy().into_owned(),
            cols,
            rows,
        })),
    }).await?;

    loop {
        match resp_rx.message().await? {
            None => { eprintln!("server disconnected during subscribe"); return Ok(()); }
            Some(r) => if let Some(Response::Subscribe(s)) = r.response {
                if s.success {
                    eprintln!("[Subscribe pty_id={:016x} subscriber_id={}]", s.pty_id, s.subscriber_id);
                } else {
                    eprintln!("subscribe failed: {}", s.error.unwrap_or_default());
                    return Ok(());
                }
                break;
            }
        }
    }

    // Request refresh
    cmd_tx.send(TerminalCommand {
        command: Some(Command::Refresh(RefreshRequest { pty_id })),
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
                    eprintln!("[Metadata reason={} gen={} pty_id={:016x}]", m.reason, m.generation, m.pty_id);
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
    fn reset_clears_both_csi_u_keyboard_protocols() {
        let bytes = RESET_TERMINAL_MODES.as_bytes();
        let has = |needle: &[u8]| bytes.windows(needle.len()).any(|w| w == needle);
        // kitty keyboard: depth-independent absolute clear of the current entry, plus a pop
        assert!(has(b"\x1b[=0;1u"), "reset must clear current kitty keyboard flags to 0");
        assert!(has(b"\x1b[<1u"), "reset must pop a passed-through kitty keyboard stack level");
        // xterm modifyOtherKeys
        assert!(has(b"\x1b[>4;0m"), "reset must disable xterm modifyOtherKeys");
    }

    #[test]
    fn reset_resets_cursor_shape_with_decscusr() {
        let bytes = RESET_TERMINAL_MODES.as_bytes();
        let has = |needle: &[u8]| bytes.windows(needle.len()).any(|w| w == needle);
        // DECSCUSR is CSI Ps SP q — the space intermediate is required. CSI 0 q (no space)
        // is DECLL (load LEDs), which does not touch the cursor shape.
        assert!(has(b"\x1b[0 q"), "reset must reset cursor shape via DECSCUSR (CSI 0 SP q)");
        assert!(!has(b"\x1b[0q"), "CSI 0 q without the space is DECLL, not a cursor-shape reset");
    }
}
