use anyhow::Result;
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
