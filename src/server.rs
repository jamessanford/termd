use std::sync::Arc;

use tonic::{Request, Response, Status, Streaming};
use tonic::service::interceptor::InterceptedService;

use crate::proto;
use crate::pty::{MetadataReason, PtyEvent, PtyMetadata, PtyRegistry};
use crate::commands;

pub use crate::proto::terminal_service_server::{TerminalService, TerminalServiceServer};

/// Generate a fresh random auth token for a daemon instance, formatted as
/// 16 hex digits like a PTY id.
pub fn generate_token() -> String {
    format!("{:016x}", uuid::Uuid::new_v4().as_u64_pair().0)
}

/// Build an auth interceptor that accepts requests carrying the given token.
pub fn auth_interceptor(
    token: String,
) -> impl Fn(Request<()>) -> Result<Request<()>, Status> + Clone {
    move |req: Request<()>| match req.metadata().get("x-auth-token") {
        Some(v) if v.as_bytes() == token.as_bytes() => Ok(req),
        _ => Err(Status::unauthenticated("invalid or missing x-auth-token")),
    }
}

#[derive(Clone)]
pub struct TerminalServiceImpl {
    pub(crate) registry: Arc<PtyRegistry>,
    pub(crate) log_grpc: bool,
}

impl TerminalServiceImpl {
    pub fn new(registry: Arc<PtyRegistry>, log_grpc: bool) -> Self {
        Self { registry, log_grpc }
    }
}

/// Build a token-authenticated service (used for the TCP listener and tests).
pub fn make_service(
    registry: Arc<PtyRegistry>,
    log_grpc: bool,
    token: String,
) -> InterceptedService<
    TerminalServiceServer<TerminalServiceImpl>,
    impl tonic::service::Interceptor + Clone,
> {
    TerminalServiceServer::with_interceptor(
        TerminalServiceImpl::new(registry, log_grpc),
        auth_interceptor(token),
    )
}

async fn dispatch_command(
    cmd:            proto::TerminalCommand,
    registry:       &Arc<PtyRegistry>,
    subscriber_id:  &str,
    subscribed_ids: &mut std::collections::HashSet<u64>,
    sub_tasks:      &mut std::collections::HashMap<u64, tokio::task::JoinHandle<()>>,
    sub_tx:         &tokio::sync::mpsc::Sender<(u64, PtyEvent)>,
) -> proto::TerminalResponse {
    use proto::terminal_command::Command;
    match cmd.command {
        None => proto::TerminalResponse {
            response: Some(proto::terminal_response::Response::Command(
                proto::CommandResponse { pty_id: 0, success: false, error: Some("unknown command".into()) }
            )),
        },
        Some(Command::List(_r))        => commands::handle_list(registry),
        Some(Command::Create(r))       => commands::handle_create(registry, r),
        Some(Command::Destroy(r))      => commands::handle_destroy(registry, r, subscriber_id, subscribed_ids, sub_tasks),
        Some(Command::Subscribe(r))    => commands::handle_subscribe(registry, r, subscriber_id, subscribed_ids, sub_tasks, sub_tx),
        Some(Command::Unsubscribe(r))  => commands::handle_unsubscribe(registry, r, subscriber_id, subscribed_ids, sub_tasks),
        Some(Command::Write(r))        => commands::handle_write(registry, r),
        Some(Command::Resize(r))       => commands::handle_resize(registry, r).await,
        Some(Command::SetTitle(r))     => commands::handle_set_title(registry, r),
        Some(Command::Refresh(r))      => commands::handle_refresh(registry, r).await,
        Some(Command::Scrollback(r))   => commands::handle_scrollback(registry, r, subscriber_id).await,
    }
}

/// Owns one connection's subscription bookkeeping and reaps it on drop — whether
/// the stream task ends cleanly or unwinds (panic). Doing the cleanup from `Drop`
/// rather than as straight-line code after the loop means a panicking handler
/// can't strand this client's subscriber entries on their PTYs. See docs/REAP.md
/// for the bug this hardens against.
struct ConnReaper {
    registry:       Arc<PtyRegistry>,
    subscriber_id:  String,
    subscribed_ids: std::collections::HashSet<u64>,
    sub_tasks:      std::collections::HashMap<u64, tokio::task::JoinHandle<()>>,
}

impl Drop for ConnReaper {
    fn drop(&mut self) {
        tracing::debug!(
            subscriber_id = %self.subscriber_id,
            ptys = ?self.subscribed_ids,
            "stream ended, reaping subscriber"
        );
        for &pty_id in &self.subscribed_ids {
            if let Some(handle) = self.registry.get(pty_id) {
                handle.close_scrollback(&self.subscriber_id);
                handle.remove_subscriber(&self.subscriber_id);
                handle.broadcast_metadata(Arc::new(PtyMetadata {
                    reason:     MetadataReason::SubscribersChanged,
                    exit_code:  None,
                    generation: handle.current_generation(),
                    info:       handle.info(),
                }));
            }
        }
        for (_, t) in self.sub_tasks.drain() {
            t.abort();
        }
    }
}

#[tonic::async_trait]
impl TerminalService for TerminalServiceImpl {
    type StreamStream = tokio_stream::wrappers::ReceiverStream<Result<proto::TerminalResponse, Status>>;

    async fn stream(
        &self,
        request: Request<Streaming<proto::TerminalCommand>>,
    ) -> Result<Response<Self::StreamStream>, Status> {
        use std::collections::{HashMap, HashSet};
        use tokio::sync::mpsc;
        use tokio_stream::StreamExt;

        let registry = self.registry.clone();
        let log_grpc = self.log_grpc;
        let mut inbound = request.into_inner();

        let (resp_tx, resp_rx) = mpsc::channel::<Result<proto::TerminalResponse, Status>>(256);
        let (sub_tx, mut sub_rx) = mpsc::channel::<(u64, PtyEvent)>(1024);

        tokio::spawn(async move {
            // All subscription bookkeeping lives in the reaper so it's cleaned up on
            // drop — covering both a clean loop exit and an unwind. Nothing below
            // needs a manual cleanup block.
            let mut reaper = ConnReaper {
                registry:       registry.clone(),
                subscriber_id:  uuid::Uuid::new_v4().to_string(),
                subscribed_ids: HashSet::new(),
                sub_tasks:      HashMap::new(),
            };

            loop {
                tokio::select! {
                    cmd = inbound.next() => {
                        match cmd {
                            None => break,
                            Some(Err(e)) => {
                                tracing::warn!("stream read error: {e}");
                                break;
                            }
                            Some(Ok(cmd)) => {
                                if log_grpc { tracing::debug!(command = ?cmd, "gRPC request"); }
                                let resp = dispatch_command(
                                    cmd, &registry, &reaper.subscriber_id,
                                    &mut reaper.subscribed_ids, &mut reaper.sub_tasks, &sub_tx,
                                ).await;
                                if log_grpc { tracing::debug!(response = ?resp, "gRPC response"); }
                                if resp_tx.send(Ok(resp)).await.is_err() { break; }
                            }
                        }
                    }
                    Some((pty_id, event)) = sub_rx.recv() => {
                        let resp = match event {
                            PtyEvent::Data(chunk) => proto::TerminalResponse {
                                response: Some(proto::terminal_response::Response::Stream(
                                    proto::StreamData { pty_id, generation: chunk.generation, data: chunk.data.to_vec() }
                                )),
                            },
                            PtyEvent::Refresh(rd) => proto::TerminalResponse {
                                response: Some(proto::terminal_response::Response::Refresh(
                                    proto::RefreshResponse {
                                        pty_id,
                                        generation: rd.generation,
                                        data: rd.data.to_vec(),
                                        cols: rd.cols,
                                        rows: rd.rows,
                                        degraded: rd.degraded,
                                    }
                                )),
                            },
                            PtyEvent::Metadata(meta) => {
                                use proto::StreamMetadataReason;
                                let reason = match meta.reason {
                                    MetadataReason::Resize             => StreamMetadataReason::Resize,
                                    MetadataReason::Closed             => StreamMetadataReason::Closed,
                                    MetadataReason::TitleChanged       => StreamMetadataReason::TitleChanged,
                                    MetadataReason::SubscribersChanged => StreamMetadataReason::SubscribersChanged,
                                };
                                proto::TerminalResponse {
                                    response: Some(proto::terminal_response::Response::Metadata(
                                        proto::StreamMetadata {
                                            pty_id,
                                            item:       Some(commands::pty_info_to_item(meta.info.clone())),
                                            reason:     reason as i32,
                                            exit_code:  meta.exit_code,
                                            generation: meta.generation,
                                        }
                                    )),
                                }
                            }
                        };
                        if resp_tx.send(Ok(resp)).await.is_err() { break; }
                    }
                }
            }

            // `reaper` drops here — on a clean exit or an unwind — reaping this
            // client's subscriptions and aborting its forwarding tasks.
            drop(reaper);
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(resp_rx)))
    }
}

pub async fn serve(
    registry: Arc<PtyRegistry>,
    unix_path: &std::path::Path,
    tcp_addr: std::net::SocketAddr,
    token: String,
    log_grpc: bool,
) -> anyhow::Result<()> {
    use tokio::net::UnixListener;
    use tonic::transport::Server;
    use tokio_stream::wrappers::{TcpListenerStream, UnixListenerStream};
    use tokio::signal::unix::{signal, SignalKind};
    use tokio::sync::broadcast;

    // Remove stale socket file if present
    let _ = std::fs::remove_file(unix_path);

    let unix_listener = UnixListener::bind(unix_path)?;
    let tcp_listener = match tokio::net::TcpListener::bind(tcp_addr).await {
        Ok(l) => l,
        Err(e) => {
            let _ = std::fs::remove_file(unix_path);
            return Err(e.into());
        }
    };

    tracing::info!(unix = ?unix_path, tcp = %tcp_addr, "termd listening");

    let (shutdown_tx, _) = broadcast::channel::<()>(1);
    let mut shutdown_rx1 = shutdown_tx.subscribe();
    let mut shutdown_rx2 = shutdown_tx.subscribe();

    // drain_tx fires 5 s after the shutdown signal — only then does the deadline start.
    let (drain_tx, drain_rx) = tokio::sync::oneshot::channel::<()>();

    let registry_for_shutdown = registry.clone();
    tokio::spawn(async move {
        let mut sigterm = signal(SignalKind::terminate()).expect("SIGTERM handler");
        let mut sigint  = signal(SignalKind::interrupt()).expect("SIGINT handler");
        tokio::select! {
            _ = sigterm.recv() => tracing::info!("SIGTERM received, shutting down"),
            _ = sigint.recv()  => tracing::info!("SIGINT received, shutting down"),
        }
        registry_for_shutdown.destroy_all();
        let _ = shutdown_tx.send(());
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        let _ = drain_tx.send(());
    });

    // Domain-socket clients are trusted by virtue of filesystem access, so the
    // unix listener runs without auth. The TCP listener requires the token.
    let svc_unix = TerminalServiceServer::new(TerminalServiceImpl::new(registry.clone(), log_grpc));
    let svc_tcp  = make_service(registry, log_grpc, token);

    let servers = async move {
        let _ = tokio::try_join!(
            Server::builder()
                .add_service(svc_unix)
                .serve_with_incoming_shutdown(
                    UnixListenerStream::new(unix_listener),
                    async move { let _ = shutdown_rx1.recv().await; },
                ),
            // Make sure we eventually timeout and remove TCP clients that have vanished.
            Server::builder()
                .http2_keepalive_interval(Some(std::time::Duration::from_secs(60)))
                .http2_keepalive_timeout(Some(std::time::Duration::from_secs(20)))
                .tcp_keepalive(Some(std::time::Duration::from_secs(60)))
                .add_service(svc_tcp)
                .serve_with_incoming_shutdown(
                    TcpListenerStream::new(tcp_listener),
                    async move { let _ = shutdown_rx2.recv().await; },
                ),
        );
    };

    tokio::select! {
        _ = servers => {},
        Ok(()) = drain_rx => {
            tracing::warn!("shutdown drain timed out after 5s, forcing exit");
        }
    }

    crate::utmp::remove_all_records();
    Ok(())
}
