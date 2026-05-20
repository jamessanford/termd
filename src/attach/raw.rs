// Raw passthrough mode — no libghostty on the render path.

use anyhow::Result;
use tokio::io::AsyncWriteExt;
use tokio::signal::unix::{signal, SignalKind};

use termd::proto::{
    terminal_command::Command, terminal_response::Response,
    RefreshRequest, StreamMetadataReason, TerminalCommand,
};

pub(super) async fn run(ctx: super::RunContext) -> Result<super::RunOutcome> {
    let super::RunContext { mut resp_rx, cmd_tx, pty_id, item, mut refresh_gen, refresh_bytes, buffered, mut action_rx } = ctx;

    let mut stdout = tokio::io::stdout();

    // Write initial state directly — no local terminal model
    stdout.write_all(&refresh_bytes).await?;
    for (gen, data) in &buffered {
        if *gen > refresh_gen {
            stdout.write_all(data).await?;
        }
    }
    stdout.flush().await?;

    let mut sigwinch = signal(SignalKind::window_change())?;

    let mut pty_closed = false;
    loop {
        tokio::select! {
            msg = resp_rx.message() => {
                match msg {
                    Ok(Some(r)) => match r.response {
                        Some(Response::Stream(s)) => {
                            if s.generation > refresh_gen {
                                if stdout.write_all(&s.data).await.is_err() { break; }
                                let _ = stdout.flush().await;
                            }
                        }
                        Some(Response::Refresh(rf)) => {
                            // Response to a SIGWINCH-triggered refresh request
                            refresh_gen = rf.generation;
                            if stdout.write_all(&rf.data).await.is_err() { break; }
                            let _ = stdout.flush().await;
                        }
                        Some(Response::Metadata(m)) => {
                            if m.reason == StreamMetadataReason::Resize as i32 {
                                // Clear stale content; server will broadcast a Refresh next
                                let _ = stdout.write_all(b"\x1b[2J").await;
                                let _ = stdout.flush().await;
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
                let action = action.unwrap_or(super::InputAction::Detach);
                return Ok(super::RunOutcome::Action(action, super::RunContext {
                    resp_rx, cmd_tx, pty_id, item,
                    refresh_gen, refresh_bytes: vec![], buffered: vec![],
                    action_rx,
                }));
            }
            _ = sigwinch.recv() => {
                // Request a fresh screen dump from the server; response arrives as Response::Refresh above
                let _ = cmd_tx.send(TerminalCommand {
                    command: Some(Command::Refresh(RefreshRequest { pty_id: pty_id.clone() })),
                }).await;
            }
        }
    }

    Ok(super::RunOutcome::ServerClosed)
}
