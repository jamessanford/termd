use anyhow::Result;
use libghostty_vt::{Terminal, TerminalOptions, RenderState};
use libghostty_vt::render::{RowIterator, CellIterator};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;

mod input;

pub(super) enum InputAction {
    Detach,
    Create,
    SwitchNext,
    SwitchIndex(u8),
    ShowList,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum RenderMode {
    /// Cell-by-cell render state for all dirty states (default)
    Cell,
    /// VT formatter for full repaints, cell-by-cell for partial repaints
    Formatter,
    /// Raw PTY byte passthrough — no libghostty on the client render path
    Raw,
    /// Raw passthrough within a DECSTBM scroll region; rewrites conflicting sequences
    Region,
}

pub(crate) struct RunContext {
    pub resp_rx:       tonic::Streaming<termd::proto::TerminalResponse>,
    pub cmd_tx:        tokio::sync::mpsc::Sender<TerminalCommand>,
    pub pty_id:        String,
    pub item:          PtyItem,
    pub refresh_gen:   u64,
    pub refresh_bytes: Vec<u8>,
    pub buffered:      Vec<(u64, Vec<u8>)>,
    pub shutdown_rx:   tokio::sync::oneshot::Receiver<()>,
}

/// Outcome returned by every render-mode runner.
pub(super) enum RunOutcome {
    /// The server PTY exited or closed.
    ServerClosed,
    /// The client disconnected (stdin EOF or `~.` escape).
    ClientDisconnected,
    /// Region mode detected it can no longer handle current dimensions.
    /// `refresh_bytes` is empty — this relies on the server sending a
    /// refresh following a resize event. That holds for resize-triggered
    /// fallbacks but would not hold for arbitrary render-mode changes.
    FallbackToCell(RunContext),
}

use termd::proto::{
    terminal_command::Command, terminal_response::Response,
    PtyItem, RefreshRequest, SubscribeRequest, TerminalCommand,
    StreamMetadataReason,
    terminal_service_client::TerminalServiceClient,
};

mod cell;
mod formatter;
mod raw;
mod region;

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
    raw.control_chars[libc::VMIN as usize] = 1;
    raw.control_chars[libc::VTIME as usize] = 0;
    tcsetattr(fd, SetArg::TCSAFLUSH, &raw)?;
    Ok(TerminalGuard { original })
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

    let pty_id = item.pty_id.clone();

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

    // Request refresh; buffer any Stream chunks that arrive before the response
    cmd_tx.send(TerminalCommand {
        command: Some(Command::Refresh(RefreshRequest { pty_id: pty_id.clone() })),
    }).await?;
    let mut buffered: Vec<(u64, Vec<u8>)> = Vec::new();
    let (refresh_gen, refresh_bytes) = loop {
        match resp_rx.message().await? {
            None => { eprintln!("server disconnected during refresh"); return Ok(()); }
            Some(r) => match r.response {
                Some(Response::Refresh(rf)) => break (rf.generation, rf.data),
                Some(Response::Stream(s))   => buffered.push((s.generation, s.data)),
                _ => {}
            }
        }
    };

    // Enter raw terminal mode; guard restores settings on any exit path
    let _guard = setup_raw_mode()?;

    // Spawn stdin forwarder; action_rx receives InputAction when an escape sequence fires.
    // _action_rx dropped here; render modes still use the old shutdown_rx oneshot
    // until Task 2 replaces RunContext.shutdown_rx with action_rx.
    let (action_tx, _action_rx) = mpsc::channel::<InputAction>(4);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let _ = shutdown_tx; // will be removed in Task 2
    let stdin_task = tokio::spawn(input::run_stdin(cmd_tx.clone(), action_tx, pty_id.clone()));

    let ctx = RunContext {
        resp_rx,
        cmd_tx,
        pty_id,
        item,
        refresh_gen,
        refresh_bytes,
        buffered,
        shutdown_rx,
    };

    let mut dispatch_mode = mode;
    let mut dispatch_ctx = ctx;
    let outcome = loop {
        let result = match dispatch_mode {
            RenderMode::Cell      => cell::run(dispatch_ctx).await?,
            RenderMode::Formatter => formatter::run(dispatch_ctx).await?,
            RenderMode::Raw       => raw::run(dispatch_ctx).await?,
            RenderMode::Region    => region::run(dispatch_ctx).await?,
        };
        match result {
            RunOutcome::FallbackToCell(fallback_ctx) => {
                dispatch_mode = RenderMode::Cell;
                dispatch_ctx = fallback_ctx;
            }
            other => break other,
        }
    };

    stdin_task.abort();
    drop(_guard);
    if matches!(outcome, RunOutcome::ServerClosed) {
        eprintln!("[Connection closed]");
    }
    Ok(())
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

