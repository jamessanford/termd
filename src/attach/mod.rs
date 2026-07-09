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
    fn init(&mut self, refresh_data: &[u8], out: &mut Vec<u8>) -> anyhow::Result<EventResult>;
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
    /// Transport error on the Subscribe stream: the connection is suspect, so
    /// retry it with the reconnect banner/backoff.
    ServerClosed,
    OutputClosed,
    PtyClosed,
    /// Drop and reopen the Subscribe stream for the current PTY (SIGWINCH refit
    /// / DataLost resync), or a graceful per-PTY stream close with the transport
    /// still healthy. Reopens + repaints quietly, with no reconnect banner.
    Resubscribe,
    Action(InputAction),
}

use termd::proto::{
    subscribe_event::Event, subscribe_frame::Frame, stream_metadata,
    CreateRequest, DestroyRequest, ListRequest, PtyItem, RefreshRequest, ResizeRequest,
    Size, StreamMetadata, SubscribeEvent, SubscribeFrame, SubscribeStart, WriteData,
};

/// Columns/rows for a `PtyItem`, treating a missing `size` as 0x0.
pub(super) fn item_size(item: &PtyItem) -> (u32, u32) {
    let s = item.size.unwrap_or_default();
    (s.cols, s.rows)
}

/// A live Subscribe stream for the current PTY: the up-channel sender (first send
/// is `Start`, then `Write`), the down-channel event stream, and the server-assigned
/// `subscriber_id` (needed by `refresh`/`scrollback`).
pub(super) struct Subscription {
    pub frame_tx: mpsc::Sender<SubscribeFrame>,
    pub event_rx: tonic::Streaming<SubscribeEvent>,
    pub subscriber_id: String,
}

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

/// Open a fresh Subscribe stream for `pty_id`, send the `Start` frame with the
/// client's current viewport size, and read the first event — which must be the
/// server's `Ready` carrying the `subscriber_id`. Returns the live `Subscription`.
/// Transport/protocol failures surface as `Err`.
async fn subscribe(
    client: &mut AuthedClient,
    pty_id: u64,
) -> anyhow::Result<Subscription> {
    let (cols, rows) = get_terminal_size();
    let (frame_tx, frame_rx) = mpsc::channel::<SubscribeFrame>(64);
    frame_tx.send(SubscribeFrame {
        frame: Some(Frame::Start(SubscribeStart {
            pty_id,
            hostname: hostname::get().unwrap_or_default().to_string_lossy().into_owned(),
            size: Some(Size { cols, rows }),
        })),
    }).await?;
    let mut event_rx = client
        .subscribe(ReceiverStream::new(frame_rx))
        .await?
        .into_inner();
    let subscriber_id = match event_rx.message().await? {
        Some(SubscribeEvent { event: Some(Event::Ready(ready)) }) => ready.subscriber_id,
        Some(_) => anyhow::bail!("subscribe: first event was not Ready"),
        None => anyhow::bail!("server disconnected during subscribe"),
    };
    Ok(Subscription { frame_tx, event_rx, subscriber_id })
}

/// Result of a refresh: the rendered screen bytes plus its (cols, rows). The
/// snapshot is the authoritative base; the server sequences it after the data it
/// subsumes, so the client renders only the snapshot and resumes on the data that
/// follows it.
type RefreshSnapshot = (u32, u32, Vec<u8>);

// Returns Ok(None) if the server reported this subscription is gone (the Subscribe
// stream ended before a Refresh arrived). Returns Ok(Some(...)) on success.
// Returns Err on transport failure.
async fn request_refresh(
    client: &mut AuthedClient,
    sub:    &mut Subscription,
    pty_id: u64,
) -> anyhow::Result<Option<RefreshSnapshot>> {
    client.refresh(RefreshRequest {
        pty_id,
        subscriber_id: sub.subscriber_id.clone(),
    }).await?;
    loop {
        match sub.event_rx.message().await? {
            None => return Ok(None),
            Some(SubscribeEvent { event: Some(Event::Refresh(rf)) }) => {
                let s = rf.size.unwrap_or_default();
                return Ok(Some((s.cols, s.rows, rf.data)));
            }
            // Discard everything that precedes the snapshot. The server emits the
            // snapshot inline, ordered after the data it subsumes, so any Data here
            // is already reflected in the snapshot — replaying it would double-apply.
            // Metadata (incl Exited) is likewise consumed up to the Refresh; if the
            // PTY exited, the stream then closes and the render loop's next
            // message() returns None, driving teardown.
            Some(_) => {}
        }
    }
}

async fn fetch_list(
    client:   &mut AuthedClient,
    pty_list: &mut Vec<PtyItem>,
) -> anyhow::Result<()> {
    let resp = client.list(ListRequest {}).await?;
    *pty_list = resp.into_inner().items;
    pty_list.sort_by_key(|p| p.sort_order);
    Ok(())
}

// Fetches updated pty_list. Returns false and shows an error message
// if the fetch fails; the caller should continue 'session.
async fn ensure_list(
    client:   &mut AuthedClient,
    pty_list: &mut Vec<PtyItem>,
) -> bool {
    if let Err(e) = fetch_list(client, pty_list).await {
        show_error(&e.to_string()).await;
        return false;
    }
    true
}

/// True if `err` is a gRPC `NotFound` Status (the PTY no longer exists), as
/// opposed to a transport failure. `subscribe`/control errors carry the
/// underlying `tonic::Status` through `anyhow`, so we downcast to inspect it.
fn is_pty_gone(err: &anyhow::Error) -> bool {
    err.downcast_ref::<tonic::Status>()
        .is_some_and(|s| s.code() == tonic::Code::NotFound)
}

async fn destroy_and_drain(
    client: &mut AuthedClient,
    pty_id: u64,
) -> anyhow::Result<()> {
    match client.destroy(DestroyRequest { pty_id }).await {
        Ok(_) => Ok(()),
        // Already gone is success.
        Err(status) if status.code() == tonic::Code::NotFound => Ok(()),
        Err(status) => anyhow::bail!("Failed to destroy PTY: {}", status.message()),
    }
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

// Updates session state to point at `new_item`. The caller is responsible for
// dropping the current Subscribe stream (which unsubscribes server-side) and
// opening a new one for the target PTY.
fn switch_pty(
    current_pty_id:  &mut u64,
    current_item:    &mut PtyItem,
    previous_pty_id: &mut Option<u64>,
    new_item:        PtyItem,
) {
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
        let (cols, rows) = item_size(item);
        let line = format!(
            " {:>3}  {:<16}  {:<32}  {}x{}\r\n",
            item.sort_order,
            pty_id_hex,
            title_trunc,
            cols, rows,
        );
        out.extend_from_slice(line.as_bytes());
        if i == selected { out.extend_from_slice(b"\x1b[0m"); }
    }
    let _ = std::io::stdout().write_all(&out);
    let _ = std::io::stdout().flush();
}

async fn show_list(
    client:          &mut AuthedClient,
    pty_list:        &mut Vec<PtyItem>,
    current_pty_id:  u64,
    stdin:           &mut tokio::io::Stdin,
) -> anyhow::Result<Option<u64>> {
    // Returns Some(new_pty_id) on selection, None on cancel.
    if let Err(e) = fetch_list(client, pty_list).await {
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
    stdin:   &mut tokio::io::Stdin,
    input:   &mut input::InputProcessor,
) -> anyhow::Result<RunOutcome> {
    draw_idle();
    let mut sigwinch = signal(SignalKind::window_change())?;
    let mut input_buf = [0u8; 256];
    loop {
        tokio::select! {
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

/// Re-establish a working link to the server after a transport failure, retrying
/// with capped exponential backoff until a request succeeds. The unary client owns
/// a lazily-reconnecting tonic `Channel`, which can report success before the
/// underlying connection is actually usable, so we confirm liveness with a real
/// `List` round-trip (which also re-runs the per-stream auth handshake) before
/// returning — otherwise a dead-but-"Ok" link would spin the caller's reconnect
/// path with no delay.
///
/// While waiting between attempts the user can press Ctrl-C or `q` to give up.
/// Returns `Some(())` once the link is back, or `None` if the user aborted (or
/// stdin closed). Re-subscribing and repainting is the caller's job.
async fn reconnect(
    client: &mut AuthedClient,
    stdin:  &mut tokio::io::Stdin,
) -> Option<()> {
    let mut backoff = std::time::Duration::from_millis(500);
    let max_backoff = std::time::Duration::from_secs(5);
    let mut buf = [0u8; 8];
    let mut attempt: u32 = 0;
    loop {
        attempt += 1;
        reconnect_status(&format!(
            "Connection lost — reconnecting (attempt {attempt})…  Ctrl-C/q to quit"
        ));
        let mut throwaway = Vec::new();
        let probe = fetch_list(client, &mut throwaway).await;
        match probe {
            Ok(()) => return Some(()),
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

    let _guard = setup_raw_mode()?;

    let mut current_pty_id = item.pty_id;
    let mut current_item = item;
    let mut pty_list: Vec<PtyItem> = Vec::new();
    let mut previous_pty_id: Option<u64> = None;
    // The single active Subscribe stream for the current PTY (None = idle/unsubscribed).
    let mut sub: Option<Subscription> = None;

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

    // On a transport failure, confirm the link is back (retrying with backoff) and
    // restart the session loop so we re-subscribe and repaint the current PTY. If
    // the user aborts the reconnect, leave the session cleanly. The loop label is
    // passed in because `macro_rules!` labels are hygienic and can't otherwise see
    // the caller's `'session`.
    macro_rules! do_reconnect {
        ($lt:lifetime) => {
            match reconnect(client, &mut stdin).await {
                Some(()) => {
                    sub = None;
                    continue $lt;
                }
                None => break $lt,
            }
        };
    }
    // Unwrap a control-helper Result, treating any Err as a transport failure that
    // triggers a reconnect.
    macro_rules! reconnect_or_break {
        ($lt:lifetime, $e:expr) => {
            match $e {
                Ok(v) => v,
                Err(_) => do_reconnect!($lt),
            }
        };
    }

    'session: loop {
        let (refresh_cols, refresh_rows, refresh_bytes): (u32, u32, Vec<u8>) = 'refresh: {
        if skip_subscribe {
            skip_subscribe = false;
            sub = None;
            break 'refresh (0, 0, vec![]);
        }

        // (Re)open the Subscribe stream for the current PTY if needed. Switching
        // PTYs and resubscribes set `sub = None` so we always open a fresh stream
        // for the current PTY here.
        if sub.is_none() {
            match subscribe(client, current_pty_id).await {
                Ok(s) => sub = Some(s),
                // PTY is gone: leave `sub = None` so the gone-PTY branch below
                // routes to the list (no reconnect banner — the transport is up).
                Err(e) if is_pty_gone(&e) => {}
                // Transport failure: retry the connection with backoff.
                Err(_) => do_reconnect!('session),
            }
        }

        let refresh_result = if let Some(s) = sub.as_mut() {
            reconnect_or_break!('session, request_refresh(client, s, current_pty_id).await)
        } else {
            // subscribe() reported the PTY is gone; fall into the gone branch.
            None
        };

        match refresh_result {
            Some((cols, rows, bytes)) => (cols, rows, bytes),
            None => {
                // Subscription is gone (the PTY closed, or subscribe found it
                // already gone). Tear it down and let the user pick a new one.
                sub = None;
                pty_list.clear();
                let _ = destroy_and_drain(client, current_pty_id).await;
                match show_list(client, &mut pty_list, current_pty_id, &mut stdin).await? {
                    Some(new_id) => {
                        if let Some(target) = pty_list.iter().find(|p| p.pty_id == new_id).cloned() {
                            switch_pty(&mut current_pty_id, &mut current_item, &mut previous_pty_id, target);
                            pty_list.clear();
                        }
                        continue 'session;
                    }
                    None => { skip_subscribe = true; (0, 0, vec![]) }
                }
            }
        }
        };
        // Without a live subscription there's no PTY to render: show the idle
        // screen and wait for the user to act. run_idle routes input through the
        // same InputProcessor/InputAction path as the render loop, so the shared
        // dispatch below handles either outcome identically.
        let outcome: RunOutcome = if let Some(sub) = sub.as_mut() {
            // `sub` here is the live `&mut Subscription` for the render loop; its
            // fields are borrowed directly. The idle branch below runs when there
            // is no subscription.
            // The refresh carries the authoritative server size for this PTY.
            if refresh_cols > 0 && refresh_rows > 0 {
                current_item.size = Some(Size { cols: refresh_cols, rows: refresh_rows });
            }
            let (item_cols, item_rows) = item_size(&current_item);

            let mut handler: Box<dyn RenderModeHandler> = create_handler(
                dispatch_mode, item_cols, item_rows, upgrade_to,
            )?;

            out.clear();
            if let EventResult::ChangeRenderMode(new_mode) = handler.init(&refresh_bytes, &mut out)? {
                handler.cleanup(&mut out);
                if !out.is_empty() {
                    stdout.write_all(&out)?;
                    out.clear();
                }
                dispatch_mode = new_mode;
                let (c, r) = item_size(&current_item);
                handler = create_handler(dispatch_mode, c, r, upgrade_to)?;
                handler.init(&refresh_bytes, &mut out)?;
            }
            if !out.is_empty() {
                stdout.write_all(&out)?;
                stdout.flush()?;
            }

            let mut sigwinch = signal(SignalKind::window_change())?;
            let mut refresh_debounce = Box::pin(tokio::time::sleep(std::time::Duration::from_secs(86400)));
            let mut debounce_active = false;
            let mut input_buf = [0u8; 256];
            // True while a lag-recovery Refresh request is in flight, so a
            // persistently slow link can't amplify lag into a refresh storm.
            let mut refresh_pending = false;

            loop {
                out.clear();
                let mut change_mode: Option<(RenderMode, Vec<u8>)> = None;

                tokio::select! {
                    msg = sub.event_rx.message() => {
                        match msg {
                            Ok(Some(SubscribeEvent { event: Some(ev) })) => match ev {
                                Event::Data(s) => {
                                    let result = handler.on_pty_event(PtyEvent::Stream { data: &s.data }, &mut out)?;
                                    if let EventResult::ChangeRenderMode(m) = result {
                                        change_mode = Some((m, vec![]));
                                    }
                                }
                                Event::Refresh(rf) => {
                                    refresh_pending = false;
                                    let s = rf.size.unwrap_or_default();
                                    if s.cols > 0 && s.rows > 0 {
                                        current_item.size = Some(s);
                                    }
                                    let result = handler.on_pty_event(
                                        PtyEvent::Refresh { cols: s.cols, rows: s.rows, data: &rf.data },
                                        &mut out,
                                    )?;
                                    if let EventResult::ChangeRenderMode(m) = result {
                                        change_mode = Some((m, rf.data));
                                    }
                                }
                                Event::Metadata(StreamMetadata { event: Some(me) }) => match me {
                                    stream_metadata::Event::Resized(r) => {
                                        let s = r.size.unwrap_or_default();
                                        if s.cols > 0 && s.rows > 0 {
                                            current_item.size = Some(s);
                                            let result = handler.on_pty_event(
                                                PtyEvent::Resize { cols: s.cols, rows: s.rows },
                                                &mut out,
                                            )?;
                                            if let EventResult::ChangeRenderMode(m) = result {
                                                change_mode = Some((m, vec![]));
                                            }
                                        }
                                    }
                                    stream_metadata::Event::TitleChanged(t) => {
                                        current_item.title = t.title;
                                    }
                                    stream_metadata::Event::Exited(_) => {
                                        handler.on_pty_event(PtyEvent::Closed, &mut out)?;
                                        input.reset();
                                        break RunOutcome::PtyClosed;
                                    }
                                    stream_metadata::Event::SubscribersChanged(_) => {}
                                    // We lagged the server's broadcast and lost stream
                                    // bytes; the screen may be corrupt. Ask for a full
                                    // snapshot — every render mode recovers completely
                                    // from a Refresh, which the server sequences inline
                                    // on this stream after the data it subsumes.
                                    stream_metadata::Event::DataLost(_) => {
                                        if !refresh_pending {
                                            refresh_pending = true;
                                            let _ = client.refresh(RefreshRequest {
                                                pty_id: current_pty_id,
                                                subscriber_id: sub.subscriber_id.clone(),
                                            }).await;
                                        }
                                    }
                                },
                                _ => {}
                            },
                            // Ready never reappears mid-stream; ignore empty events.
                            Ok(Some(_)) => {}
                            // Graceful per-PTY stream close (transport still up,
                            // e.g. the server reaped this stream): quietly reopen
                            // + repaint rather than flashing the reconnect banner.
                            Ok(None) => { break RunOutcome::Resubscribe; }
                            // Transport error: the connection is suspect → reconnect.
                            Err(_) => { break RunOutcome::ServerClosed; }
                        }
                    }
                    result = stdin.read(&mut input_buf) => {
                        let n = match result {
                            Ok(0) | Err(_) => break RunOutcome::Action(InputAction::Detach),
                            Ok(n) => n,
                        };
                        let r = input.process(&input_buf[..n]);
                        if !r.write.is_empty() {
                            let _ = sub.frame_tx.send(SubscribeFrame {
                                frame: Some(Frame::Write(WriteData { data: r.write })),
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
                        // Reopen the Subscribe stream with the current viewport size so the
                        // server can refit the PTY to all subscribers (apply_subscribe keys
                        // off the SubscribeStart size). The 'session loop will re-subscribe
                        // and repaint. Even if the size didn't change, a SIGWINCH storm can
                        // garble the terminal, so the unconditional repaint is desirable.
                        break RunOutcome::Resubscribe;
                    }
                }

                if let Some((new_mode, refresh_data)) = change_mode {
                    handler.cleanup(&mut out);
                    dispatch_mode = new_mode;
                    let (c, r) = item_size(&current_item);
                    handler = create_handler(dispatch_mode, c, r, upgrade_to)?;
                    let init_result = handler.init(&refresh_data, &mut out)?;
                    if let EventResult::ChangeRenderMode(fallback) = init_result {
                        handler.cleanup(&mut out);
                        dispatch_mode = fallback;
                        let (c, r) = item_size(&current_item);
                        handler = create_handler(dispatch_mode, c, r, upgrade_to)?;
                        handler.init(&refresh_data, &mut out)?;
                    }
                    // A SIGWINCH-driven switch hands off empty data and doesn't resize the
                    // server, so no Refresh follows on its own — the new handler would paint
                    // a stale/blank screen. Request one so a full repaint comes down the pipe.
                    // We only reach this branch with a live subscription, so the PTY is always
                    // there to ask.
                    if refresh_data.is_empty() {
                        let _ = client.refresh(RefreshRequest {
                            pty_id: current_pty_id,
                            subscriber_id: sub.subscriber_id.clone(),
                        }).await;
                    }
                }

                if !out.is_empty() {
                    if stdout.write_all(&out).is_err() { break RunOutcome::OutputClosed; }
                    let _ = stdout.flush();
                }
            }
        } else {
            run_idle(&mut stdin, &mut input).await?
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
                // wouldn't help — just exit cleanly.
                reset_terminal_modes();
                break 'session;
            }
            RunOutcome::Resubscribe => {
                // Drop the current Subscribe stream (server reaps it) and reopen it
                // for the same PTY with the current size on the next 'session pass.
                // Reached by SIGWINCH refit, DataLost resync, or a graceful stream
                // close while the transport is healthy — none of which warrant the
                // reconnect banner. If the PTY is actually gone, the reopen's
                // refresh returns None and routes to the list.
                reset_terminal_modes();
                sub = None;
                continue 'session;
            }
            RunOutcome::PtyClosed => {
                reset_terminal_modes();
                sub = None;
                pty_list.clear();
                let _ = destroy_and_drain(client, current_pty_id).await;
                match show_list(client, &mut pty_list, current_pty_id, &mut stdin).await? {
                    Some(new_id) => {
                        if let Some(target) = pty_list.iter().find(|p| p.pty_id == new_id).cloned() {
                            switch_pty(&mut current_pty_id, &mut current_item, &mut previous_pty_id, target);
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
                        if let Err(e) = destroy_and_drain(client, current_pty_id).await {
                            show_error(&e.to_string()).await;
                            continue 'session;
                        }
                        sub = None;
                        pty_list.clear();
                        let auto_target = if ensure_list(client, &mut pty_list).await {
                            recent_pty(&pty_list, &previous_pty_id, current_pty_id).await.cloned()
                        } else {
                            None
                        };
                        match auto_target {
                            Some(target) if target.pty_id != current_pty_id => {
                                switch_pty(&mut current_pty_id, &mut current_item, &mut previous_pty_id, target);
                                pty_list.clear();
                            }
                            _ => { skip_subscribe = true; }
                        }
                    }

                    InputAction::ForceResize => {
                        let (cols, rows) = get_terminal_size();
                        let _ = client.resize(ResizeRequest {
                            pty_id: current_pty_id,
                            size: Some(Size { cols, rows }),
                        }).await;
                    }

                    InputAction::ForceRefresh => {
                        if let Some(s) = sub.as_ref() {
                            let _ = client.refresh(RefreshRequest {
                                pty_id: current_pty_id,
                                subscriber_id: s.subscriber_id.clone(),
                            }).await;
                        }
                    }

                    InputAction::Create => {
                        let (cols, rows) = get_terminal_size();
                        let created = reconnect_or_break!('session, client.create(CreateRequest {
                            size: Some(Size { cols, rows }),
                            command: None,
                        }).await);
                        let new_item = created.into_inner();
                        sub = None;
                        switch_pty(&mut current_pty_id, &mut current_item, &mut previous_pty_id, new_item);
                        pty_list.clear();
                    }

                    InputAction::SwitchNext => {
                        if !ensure_list(client, &mut pty_list).await {
                            continue 'session;
                        }
                        if let Some(target) = next_pty(&pty_list, current_pty_id).cloned() {
                            if target.pty_id != current_pty_id {
                                sub = None;
                                switch_pty(&mut current_pty_id, &mut current_item, &mut previous_pty_id, target);
                            }
                        }
                    }

                    InputAction::SwitchPrevious => {
                        if !ensure_list(client, &mut pty_list).await {
                            continue 'session;
                        }
                        if let Some(target) = prev_pty(&pty_list, current_pty_id).cloned() {
                            if target.pty_id != current_pty_id {
                                sub = None;
                                switch_pty(&mut current_pty_id, &mut current_item, &mut previous_pty_id, target);
                            }
                        }
                    }

                    InputAction::SwitchRecent => {
                        if ensure_list(client, &mut pty_list).await {
                            if let Some(target) = recent_pty(&pty_list, &previous_pty_id, current_pty_id).await.cloned() {
                                if target.pty_id != current_pty_id {
                                    sub = None;
                                    switch_pty(&mut current_pty_id, &mut current_item, &mut previous_pty_id, target);
                                }
                            }
                        }
                    }

                    InputAction::SwitchIndex(n) => {
                        if !ensure_list(client, &mut pty_list).await {
                            continue 'session;
                        }
                        if let Some(target) = pty_list.get(n as usize).cloned() {
                            if target.pty_id != current_pty_id {
                                sub = None;
                                switch_pty(&mut current_pty_id, &mut current_item, &mut previous_pty_id, target);
                            }
                        }
                    }

                    InputAction::ShowList => {
                        match show_list(client, &mut pty_list, current_pty_id, &mut stdin).await? {
                            Some(new_id) if new_id != current_pty_id => {
                                if let Some(target) = pty_list.iter().find(|p| p.pty_id == new_id).cloned() {
                                    sub = None;
                                    switch_pty(&mut current_pty_id, &mut current_item, &mut previous_pty_id, target);
                                    pty_list.clear();
                                }
                            }
                            _ => {}
                        }
                    }

                    InputAction::ShowInfo => {
                        let (client_cols, client_rows) = get_terminal_size();
                        let (server_cols, server_rows) = item_size(&current_item);
                        show_info(&format!(
                            "requested={mode:?} actual={dispatch_mode:?} pty={current_pty_id:016x} \
                             server={server_cols}x{server_rows} client={client_cols}x{client_rows}",
                        )).await;
                    }

                    InputAction::ShowScrollback => {
                        if let Some(s) = sub.as_ref() {
                            let (_, rows) = item_size(&current_item);
                            scrollback::show_scrollback(
                                client,
                                current_pty_id,
                                s.subscriber_id.clone(),
                                rows,
                                &mut stdin,
                            ).await?;
                        }
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
    // Open the Subscribe stream and read the Ready event for the subscriber_id.
    let mut sub = subscribe(client, pty_id).await?;
    eprintln!("[Subscribe pty_id={:016x} subscriber_id={}]", pty_id, sub.subscriber_id);

    // Request refresh (the snapshot arrives in-order on the Subscribe stream).
    client.refresh(RefreshRequest {
        pty_id,
        subscriber_id: sub.subscriber_id.clone(),
    }).await?;

    // Main debug receive loop — print events to stderr, no rendering.
    loop {
        match sub.event_rx.message().await {
            Ok(Some(SubscribeEvent { event: Some(ev) })) => match ev {
                Event::Ready(r) => eprintln!("[Ready subscriber_id={}]", r.subscriber_id),
                Event::Data(s) => eprintln!("[Data len={}]", s.data.len()),
                Event::Refresh(rf) => eprintln!("[Refresh len={} degraded={}]", rf.data.len(), rf.degraded),
                Event::Metadata(StreamMetadata { event }) => {
                    eprintln!("[Metadata {event:?}]");
                    if matches!(event, Some(stream_metadata::Event::Exited(_))) {
                        eprintln!("[PTY exited]");
                        break;
                    }
                }
            },
            Ok(Some(_)) => {}
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
