use std::collections::HashSet;
use std::io::Write;

use anyhow::Result;
use tokio::io::AsyncReadExt;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

mod input;
mod mru;

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
    ToggleKeep,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum RenderMode {
    /// Cell-by-cell render state for all dirty states
    Cell,
    /// Raw PTY byte passthrough
    Raw,
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
        RenderMode::Autowrap => Box::new(autowrap::AutowrapHandler::new(server_cols, server_rows)?),
    })
}

/// How long a possible escape-sequence start (e.g. a bare ESC) is held waiting
/// for the rest before being treated as the bare key. A sequence from the
/// terminal arrives in one burst, so this only needs to cover a split read.
const ESC_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(50);

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
    /// Chosen in the picker.
    Select(u64),
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

/// Half-close-and-drain teardown for a Subscribe stream. Bare-dropping the
/// stream RSTs it, and h2 >= 0.4.15 discards buffered DATA once a reset is
/// scheduled — losing any still-queued Write (e.g. keystrokes read in the same
/// stdin chunk as a chord). Dropping only `frame_tx` half-closes gracefully;
/// the server processes inbound frames in order and ends the response stream
/// on half-close, so draining to stream end confirms every Write reached the
/// PTY. Timeout-capped so a wedged server can't hang teardown; on a broken
/// transport the drain errors out immediately.
async fn close_subscription(sub: Subscription) {
    let Subscription { frame_tx, mut event_rx, .. } = sub;
    drop(frame_tx);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while let Ok(Some(_)) = event_rx.message().await {}
    })
    .await;
}

/// Fire-and-forget `close_subscription` for mid-session teardowns (PTY switch,
/// create, resubscribe): the drain runs in the background so the switch never
/// waits on a server round-trip.
fn close_subscription_bg(sub: &mut Option<Subscription>) {
    if let Some(s) = sub.take() {
        tokio::spawn(close_subscription(s));
    }
}

mod autowrap;
mod cell;
mod help;
mod raw;
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
    "\x1b[?2026l",    // end synchronized output (a cut stream mid-sync freezes rendering)
    "\x1b]8;;\x1b\\", // close any unterminated OSC 8 hyperlink
    "\x1b]104\x1b\\", // reset OSC 4 palette redefinitions to the terminal's configured defaults
    "\x1b]110\x1b\\", // reset dynamic default foreground (undo OSC 10)
    "\x1b]111\x1b\\", // reset dynamic default background (undo OSC 11)
    "\x1b]112\x1b\\", // reset dynamic cursor color (undo OSC 12)
    "\x1b]0;\x1b\\",  // clear window title (undo OSC 0/2; session pop in run() restores the original)
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
/// `keep_on_exit`: Some sets the PTY's flag as part of subscribing; None leaves it.
async fn subscribe(
    client: &mut AuthedClient,
    pty_id: u64,
    keep_on_exit: Option<bool>,
) -> anyhow::Result<Subscription> {
    let (cols, rows) = get_terminal_size();
    let (frame_tx, frame_rx) = mpsc::channel::<SubscribeFrame>(64);
    frame_tx.send(SubscribeFrame {
        frame: Some(Frame::Start(SubscribeStart {
            pty_id,
            hostname: hostname::get().unwrap_or_default().to_string_lossy().into_owned(),
            size: Some(Size { cols, rows }),
            keep_on_exit,
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
type RefreshSnapshot = (u32, u32, Vec<u8>, bool);

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
                return Ok(Some((s.cols, s.rows, rf.data, rf.exited)));
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

/// True if the server answered but failed to serve this PTY (Internal — e.g. an
/// older server whose reader died when the command exited). The transport is
/// fine, so reconnecting would just loop; but the PTY may be perfectly alive
/// behind a server bug, so it must not be destroyed either.
fn is_pty_broken(err: &anyhow::Error) -> bool {
    err.downcast_ref::<tonic::Status>()
        .is_some_and(|s| s.code() == tonic::Code::Internal)
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
    sub:             &mut Option<Subscription>,
    current_pty_id:  &mut u64,
    current_item:    &mut PtyItem,
    mru:             &mut mru::Mru,
    new_item:        PtyItem,
) {
    // Tear down the old PTY's stream gracefully (in the background, so the
    // switch never waits on the server) rather than letting callers bare-drop it.
    close_subscription_bg(sub);
    *current_pty_id = new_item.pty_id;
    mru.touch(new_item.pty_id);
    *current_item = new_item;
}

fn clear_screen() {
    use std::io::Write;
    let _ = std::io::stdout().write_all(b"\x1b[2J\x1b[H");
    let _ = std::io::stdout().flush();
}

fn draw_list(items: &[PtyItem], selected: usize, can_cancel: bool) {
    use std::io::Write;
    let mut out = Vec::new();
    out.extend_from_slice(b"\x1b[2J\x1b[H");
    if items.is_empty() {
        out.extend_from_slice(b" No PTYs in this session.\r\n");
    }
    for (i, item) in items.iter().enumerate() {
        if i == selected { out.extend_from_slice(b"\x1b[7m"); }
        let title = if item.title.is_empty() { &item.pts_name } else { &item.title };
        let pty_id_hex = format!("{:016x}", item.pty_id);
        let title_trunc: String = title.chars().take(32).collect();
        let (cols, rows) = item_size(item);
        let state = match (&item.exited, item.keep_on_exit) {
            (Some(e), _) => format!("  [exited {}]", e.exit_code),
            (None, true) => "  [keep]".to_string(),
            (None, false) => String::new(),
        };
        let line = format!(
            " {:>3}  {:<16}  {:<32}  {}x{}{}\r\n",
            item.sort_order,
            pty_id_hex,
            title_trunc,
            cols, rows, state,
        );
        out.extend_from_slice(line.as_bytes());
        if i == selected { out.extend_from_slice(b"\x1b[0m"); }
    }
    out.extend_from_slice(b"\r\n ");
    if !items.is_empty() {
        out.extend_from_slice("\u{2191}\u{2193}/jk select  Enter switch  ".as_bytes());
    }
    if can_cancel {
        out.extend_from_slice(b"Esc/q back  ");
    }
    out.extend_from_slice(b"C-a c create  C-a d detach  C-a ? help\r\n");
    let _ = std::io::stdout().write_all(&out);
    let _ = std::io::stdout().flush();
}

/// The most recently viewed PTY other than `current`, skipping `broken` ones.
fn pick_recent(
    mru: &mut mru::Mru, list: &[PtyItem], current: u64, broken: &HashSet<u64>,
) -> Option<PtyItem> {
    let candidates: Vec<PtyItem> =
        list.iter().filter(|p| !broken.contains(&p.pty_id)).cloned().collect();
    mru.pick(&candidates, current).cloned()
}

/// We can't stay on `gone_id` (exited, destroyed, or broken): pick where to go.
/// The most recently viewed usable PTY wins, never one in `broken` — so a
/// server failing every PTY can't bounce us around forever. None = nothing
/// usable: the caller drops to the picker, which lists broken PTYs too, where
/// choosing one retries it (one try per keypress).
async fn next_after_gone(
    client:   &mut AuthedClient,
    pty_list: &mut Vec<PtyItem>,
    mru:      &mut mru::Mru,
    broken:   &HashSet<u64>,
    gone_id:  u64,
) -> Option<PtyItem> {
    mru.remove(gone_id);
    if !ensure_list(client, pty_list).await { return None; }
    pick_recent(mru, pty_list, gone_id, broken)
}

/// A key the picker understands, decoded from InputProcessor's pass-through bytes.
#[derive(Debug, PartialEq, Eq)]
enum PickerKey { Up, Down, Enter, Cancel }

/// Decode picker keys from bytes InputProcessor passed through (C-a bindings
/// never get here). A lone ESC is only ever a whole-buffer `[0x1b]` — the
/// processor holds it until more arrives or the caller's timeout flushes it.
fn picker_keys(b: &[u8]) -> Vec<PickerKey> {
    use PickerKey::*;
    let mut keys = Vec::new();
    let mut i = 0;
    while i < b.len() {
        match &b[i..] {
            [b'\r' | b'\n', ..] => { keys.push(Enter); i += 1; }
            [b'k', ..] => { keys.push(Up); i += 1; }
            [b'j', ..] => { keys.push(Down); i += 1; }
            [b'q', ..] => { keys.push(Cancel); i += 1; }
            [0x1b, b'[' | b'O', b'A', ..] => { keys.push(Up); i += 3; }
            [0x1b, b'[' | b'O', b'B', ..] => { keys.push(Down); i += 3; }
            [0x1b, b'[', rest @ ..] => {
                // Some other CSI: skip to its final byte. CSI-u ESC / Enter
                // (`27u` / `13u`, optionally with a no-modifier `;1`) count as
                // Cancel / Enter.
                let Some(end) = rest.iter().position(|c| (0x40..=0x7e).contains(c)) else { break };
                let body = &rest[..end];
                if rest[end] == b'u' {
                    if body == b"27" || body == b"27;1" {
                        keys.push(Cancel);
                    } else if body == b"13" || body == b"13;1" {
                        keys.push(Enter);
                    }
                }
                i += 2 + end + 1;
            }
            [0x1b] => { keys.push(Cancel); i += 1; }
            [0x1b, _, ..] => i += 2, // Alt-<key>: ignore
            _ => i += 1,
        }
    }
    keys
}

enum PickerOutcome {
    Select(u64),
    /// Back to the PTY we came from (only offered when there is one).
    Cancel,
    /// A C-a binding pressed in the picker, for the shared dispatch.
    Action(InputAction),
}

/// The PTY picker, also the "not viewing any PTY" screen (`current` None: no
/// Cancel, and an empty list is fine). Input runs through the same
/// InputProcessor as the render loop, so C-a bindings (create, detach, help,
/// …) work here and come back as `Action` for the shared dispatch.
async fn run_picker(
    client:   &mut AuthedClient,
    pty_list: &mut Vec<PtyItem>,
    current:  Option<u64>,
    stdin:    &mut tokio::io::Stdin,
    input:    &mut input::InputProcessor,
) -> anyhow::Result<PickerOutcome> {
    if let Err(e) = fetch_list(client, pty_list).await {
        show_error(&e.to_string()).await;
        if current.is_some() { return Ok(PickerOutcome::Cancel); }
    }
    let mut selected = current
        .and_then(|id| pty_list.iter().position(|p| p.pty_id == id))
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
    let can_cancel = current.is_some();
    draw_list(pty_list, selected, can_cancel);

    input.reset();
    let mut sigwinch = signal(SignalKind::window_change())?;
    let mut buf = [0u8; 256];
    let mut esc_flush = Box::pin(tokio::time::sleep(std::time::Duration::from_secs(86400)));
    let mut esc_armed = false;
    loop {
        let bytes = tokio::select! {
            r = stdin.read(&mut buf) => {
                let n = match r {
                    Ok(0) | Err(_) => return Ok(PickerOutcome::Action(InputAction::Detach)),
                    Ok(n) => n,
                };
                let r = input.process(&buf[..n]);
                if let Some(a) = r.action {
                    clear_screen();
                    return Ok(PickerOutcome::Action(a));
                }
                esc_armed = input.has_pending_escape();
                if esc_armed {
                    esc_flush.as_mut().reset(tokio::time::Instant::now() + ESC_TIMEOUT);
                }
                r.write
            }
            _ = &mut esc_flush, if esc_armed => {
                esc_armed = false;
                input.flush_pending_escape()
            }
            _ = sigwinch.recv() => {
                draw_list(pty_list, selected, can_cancel);
                continue;
            }
        };
        for key in picker_keys(&bytes) {
            match key {
                PickerKey::Up => selected = selected.saturating_sub(1),
                PickerKey::Down => if selected + 1 < pty_list.len() { selected += 1 },
                PickerKey::Enter => if let Some(p) = pty_list.get(selected) {
                    clear_screen();
                    return Ok(PickerOutcome::Select(p.pty_id));
                },
                PickerKey::Cancel => if can_cancel {
                    clear_screen();
                    return Ok(PickerOutcome::Cancel);
                },
            }
        }
        draw_list(pty_list, selected, can_cancel);
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

    // Save the client terminal's title on the xterm title stack; PTY output and the
    // reset-path title clears overwrite it freely during the session, and the matching
    // pop at the bottom restores it. Terminals without a title stack ignore both.
    {
        use std::io::Write;
        let _ = std::io::stdout().write_all(b"\x1b[22;0t");
        let _ = std::io::stdout().flush();
    }

    let mut current_pty_id = item.pty_id;
    let mut current_item = item;
    let mut detached_msg: Option<String> = None;
    let mut pty_list: Vec<PtyItem> = Vec::new();
    let mut mru = mru::Mru::default();
    // C-a o's new keep_on_exit for the current PTY, sent on the next subscribe.
    let mut pending_keep: Option<bool> = None;
    // PTYs the server failed to serve (Internal) this session: automatic picks
    // skip them. Cleared per PTY once one displays again.
    let mut broken: HashSet<u64> = HashSet::new();
    mru.touch(current_pty_id);
    // The single active Subscribe stream for the current PTY (None = idle/unsubscribed).
    let mut sub: Option<Subscription> = None;

    let upgrade_to = match mode {
        RenderMode::Autowrap => Some(RenderMode::Autowrap),
        _ => None,
    };
    let mut dispatch_mode = mode;
    let mut stdout = std::io::stdout();
    let mut stdin = tokio::io::stdin();
    let mut input = input::InputProcessor::new();
    let mut out = Vec::new();
    let mut skip_subscribe = false;
    // C-a " while viewing a PTY: show the picker (keeping the subscription)
    // instead of rendering, on the next pass.
    let mut picker_open = false;

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
        if picker_open {
            break 'refresh (0, 0, vec![]);
        }

        // (Re)open the Subscribe stream for the current PTY if needed. Switching
        // PTYs and resubscribes set `sub = None` so we always open a fresh stream
        // for the current PTY here.
        if sub.is_none() {
            match subscribe(client, current_pty_id, pending_keep.take()).await {
                Ok(s) => sub = Some(s),
                // PTY is gone: leave `sub = None` so the gone-PTY branch below
                // routes to the list (no reconnect banner — the transport is up).
                Err(e) if is_pty_gone(&e) => {}
                // Transport failure: retry the connection with backoff.
                Err(_) => do_reconnect!('session),
            }
        }

        let refresh_result = if let Some(s) = sub.as_mut() {
            match request_refresh(client, s, current_pty_id).await {
                Ok(r) => r,
                // Not a transport problem: skip this PTY (without destroying
                // it) via the gone branch below.
                Err(e) if is_pty_broken(&e) => {
                    show_error(&format!("can't display PTY {current_pty_id:016x}: {e}")).await;
                    broken.insert(current_pty_id);
                    None
                }
                Err(_) => do_reconnect!('session),
            }
        } else {
            // subscribe() reported the PTY is gone; fall into the gone branch.
            None
        };

        match refresh_result {
            // An exited PTY without keep_on_exit is being reaped (C-a o just
            // cleared the flag); treat it as gone rather than show it.
            Some((cols, rows, bytes, exited)) if !exited || current_item.keep_on_exit => {
                broken.remove(&current_pty_id);
                (cols, rows, bytes)
            }
            _ => {
                // The PTY is gone (stream ended, or subscribe found it already
                // gone), being reaped, or broken. Move on; destroy it unless
                // broken (it may be a live shell behind a server bug).
                close_subscription_bg(&mut sub);
                pty_list.clear();
                if !broken.contains(&current_pty_id) {
                    let _ = destroy_and_drain(client, current_pty_id).await;
                }
                match next_after_gone(client, &mut pty_list, &mut mru, &broken, current_pty_id).await {
                    Some(target) => {
                        switch_pty(&mut sub, &mut current_pty_id, &mut current_item, &mut mru, target);
                        pty_list.clear();
                        continue 'session;
                    }
                    // Nothing usable: `sub` is None, so this pass shows the
                    // picker. (Not skip_subscribe — that would also throw away
                    // whatever the picker then switches to.)
                    None => (0, 0, vec![]),
                }
            }
        }
        };
        // Without a live subscription there's no PTY to render, so show the
        // picker (nothing to go back to); likewise when C-a " asked for it. It
        // routes C-a bindings through the same InputProcessor/InputAction path
        // as the render loop, so the shared dispatch below handles both alike.
        let outcome: RunOutcome = if picker_open || sub.is_none() {
            picker_open = false;
            let back_to = sub.is_some().then_some(current_pty_id);
            match run_picker(client, &mut pty_list, back_to, &mut stdin, &mut input).await? {
                PickerOutcome::Select(id) => RunOutcome::Select(id),
                PickerOutcome::Action(a) => RunOutcome::Action(a),
                PickerOutcome::Cancel => continue 'session,
            }
        } else if let Some(sub) = sub.as_mut() {
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
            // Armed while the InputProcessor holds a possible escape-sequence
            // start (e.g. a bare ESC); on expiry the held bytes go to the PTY.
            let mut esc_flush = Box::pin(tokio::time::sleep(std::time::Duration::from_secs(86400)));
            let mut esc_armed = false;

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
                                        // keep_on_exit: stay on the final screen. The
                                        // stream stays open until the PTY is destroyed
                                        // (or its flag cleared), then ends and routes
                                        // through the gone path.
                                        if !current_item.keep_on_exit {
                                            input.reset();
                                            break RunOutcome::PtyClosed;
                                        }
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
                        esc_armed = input.has_pending_escape();
                        if esc_armed {
                            esc_flush.as_mut().reset(tokio::time::Instant::now() + ESC_TIMEOUT);
                        }
                    }
                    _ = &mut esc_flush, if esc_armed => {
                        // Nothing followed: it was a bare ESC (or a truncated
                        // sequence), not the start of one. Pass it through.
                        esc_armed = false;
                        let data = input.flush_pending_escape();
                        if !data.is_empty() {
                            let _ = sub.frame_tx.send(SubscribeFrame {
                                frame: Some(Frame::Write(WriteData { data })),
                            }).await;
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
            unreachable!("no subscription is handled by the picker branch")
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
                // Close the current Subscribe stream (drained in the background)
                // and reopen it for the same PTY with the current size on the next
                // 'session pass. Reached by SIGWINCH refit, DataLost resync, or a
                // graceful stream close while the transport is healthy — none of
                // which warrant the reconnect banner. If the PTY is actually gone,
                // the reopen's refresh returns None and routes to the list.
                reset_terminal_modes();
                close_subscription_bg(&mut sub);
                continue 'session;
            }
            RunOutcome::PtyClosed => {
                reset_terminal_modes();
                sub = None;
                pty_list.clear();
                let _ = destroy_and_drain(client, current_pty_id).await;
                match next_after_gone(client, &mut pty_list, &mut mru, &broken, current_pty_id).await {
                    Some(target) => {
                        switch_pty(&mut sub, &mut current_pty_id, &mut current_item, &mut mru, target);
                        pty_list.clear();
                    }
                    None => { skip_subscribe = true; }
                }
                continue 'session;
            }
            RunOutcome::Select(id) => {
                // Re-choosing the PTY we're already on just returns to it.
                if id != current_pty_id || sub.is_none() {
                    if let Some(target) = pty_list.iter().find(|p| p.pty_id == id).cloned() {
                        switch_pty(&mut sub, &mut current_pty_id, &mut current_item, &mut mru, target);
                    }
                }
                pty_list.clear();
            }
            RunOutcome::Action(action) => {
                reset_terminal_modes();
                match action {
                    InputAction::Detach => {
                        let title = if current_item.title.is_empty() { &current_item.pts_name } else { &current_item.title };
                        detached_msg = Some(format!("[Detached from {title} ({:016x})]\r\n", current_pty_id));
                        break 'session;
                    }

                    InputAction::Destroy => {
                        if let Err(e) = destroy_and_drain(client, current_pty_id).await {
                            show_error(&e.to_string()).await;
                            continue 'session;
                        }
                        sub = None;
                        pty_list.clear();
                        match next_after_gone(client, &mut pty_list, &mut mru, &broken, current_pty_id).await {
                            Some(target) => {
                                switch_pty(&mut sub, &mut current_pty_id, &mut current_item, &mut mru, target);
                                pty_list.clear();
                            }
                            None => { skip_subscribe = true; }
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
                        switch_pty(&mut sub,&mut current_pty_id, &mut current_item, &mut mru, new_item);
                        pty_list.clear();
                    }

                    InputAction::SwitchNext => {
                        if !ensure_list(client, &mut pty_list).await {
                            continue 'session;
                        }
                        if let Some(target) = next_pty(&pty_list, current_pty_id).cloned() {
                            if target.pty_id != current_pty_id {
                                switch_pty(&mut sub, &mut current_pty_id, &mut current_item, &mut mru, target);
                            }
                        }
                    }

                    InputAction::SwitchPrevious => {
                        if !ensure_list(client, &mut pty_list).await {
                            continue 'session;
                        }
                        if let Some(target) = prev_pty(&pty_list, current_pty_id).cloned() {
                            if target.pty_id != current_pty_id {
                                switch_pty(&mut sub, &mut current_pty_id, &mut current_item, &mut mru, target);
                            }
                        }
                    }

                    InputAction::SwitchRecent => {
                        if ensure_list(client, &mut pty_list).await {
                            match pick_recent(&mut mru, &pty_list, current_pty_id, &broken) {
                                Some(target) => switch_pty(&mut sub, &mut current_pty_id, &mut current_item, &mut mru, target),
                                None => show_info("No other PTYs").await,
                            }
                        }
                    }

                    InputAction::SwitchIndex(n) => {
                        if !ensure_list(client, &mut pty_list).await {
                            continue 'session;
                        }
                        if let Some(target) = pty_list.get(n as usize).cloned() {
                            if target.pty_id != current_pty_id {
                                switch_pty(&mut sub, &mut current_pty_id, &mut current_item, &mut mru, target);
                            }
                        }
                    }

                    InputAction::ShowList => picker_open = true,

                    InputAction::ShowInfo => {
                        let (client_cols, client_rows) = get_terminal_size();
                        let (server_cols, server_rows) = item_size(&current_item);
                        show_info(&format!(
                            "requested={mode:?} actual={dispatch_mode:?} pty={current_pty_id:016x} \
                             server={server_cols}x{server_rows} client={client_cols}x{client_rows} \
                             keep_on_exit={keep}",
                            keep = current_item.keep_on_exit,
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

                    InputAction::ToggleKeep => {
                        // The flag rides SubscribeStart: resubscribe with it now. If
                        // the PTY is already dead and we just cleared it, the server
                        // reaps it and the next pass moves on to the recent PTY.
                        let keep = !current_item.keep_on_exit;
                        close_subscription_bg(&mut sub);
                        match subscribe(client, current_pty_id, Some(keep)).await {
                            Ok(s) => sub = Some(s),
                            // Let the next pass's subscribe sort it out (gone /
                            // reconnect), still carrying the flag.
                            Err(_) => {
                                current_item.keep_on_exit = keep;
                                pending_keep = Some(keep);
                                continue 'session;
                            }
                        }
                        // Believe the server, not ourselves: older servers ignore the
                        // flag, and treating such a PTY as kept means not destroying
                        // it on exit, leaving a dead entry they can't serve.
                        let applied = ensure_list(client, &mut pty_list).await
                            && pty_list.iter().find(|p| p.pty_id == current_pty_id)
                                .is_some_and(|p| p.keep_on_exit == keep);
                        pty_list.clear();
                        if applied || !keep {
                            current_item.keep_on_exit = keep;
                        }
                        show_info(match (applied, keep) {
                            (true, true) => "This PTY will be kept after it exits (C-a k to destroy)",
                            (true, false) => "This PTY will be removed when it exits",
                            (false, _) => "This server doesn't support keeping exited PTYs",
                        }).await;
                    }
                }
            }
        }
    }

    // Flush any Write still queued on the stream (e.g. keystrokes read in the
    // same stdin chunk as the detach chord) before tearing it down. On a broken
    // transport this fails fast rather than waiting out the timeout.
    if let Some(s) = sub.take() {
        close_subscription(s).await;
    }

    // Restore the client terminal's original title (matches the push at session start).
    {
        use std::io::Write;
        let _ = std::io::stdout().write_all(b"\x1b[23;0t");
        let _ = std::io::stdout().flush();
    }
    move_terminal_end();
    if let Some(msg) = detached_msg {
        use std::io::Write;
        let _ = std::io::stdout().write_all(msg.as_bytes());
        let _ = std::io::stdout().flush();
    }
    drop(_guard);
    Ok(())
}

async fn run_debug(client: &mut AuthedClient, pty_id: u64) -> Result<()> {
    // Open the Subscribe stream and read the Ready event for the subscriber_id.
    let mut sub = subscribe(client, pty_id, None).await?;
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
    fn picker_keys_decodes_navigation() {
        use PickerKey::*;
        assert_eq!(picker_keys(b"\x1b[A\x1b[B\x1bOA\x1bOBkj"), vec![Up, Down, Up, Down, Up, Down]);
        assert_eq!(picker_keys(b"\r"), vec![Enter]);
        assert_eq!(picker_keys(b"\x1b[13u"), vec![Enter]);
        assert_eq!(picker_keys(b"q"), vec![Cancel]);
        assert_eq!(picker_keys(b"\x1b"), vec![Cancel]);
        assert_eq!(picker_keys(b"\x1b[27u"), vec![Cancel]);
        assert_eq!(picker_keys(b"\x1b[27;1u"), vec![Cancel]);
    }

    #[test]
    fn picker_keys_ignores_everything_else() {
        // Other CSI (incl. modified CSI-u ESC), Alt-keys, stray letters.
        assert!(picker_keys(b"\x1b[27;5u\x1b[1;5C\x1bxz").is_empty());
        // A truncated CSI doesn't panic or loop.
        assert!(picker_keys(b"\x1b[1;").is_empty());
    }

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
