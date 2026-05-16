use std::sync::Arc;

use tonic::{Request, Response, Status, Streaming};
use tonic::service::interceptor::InterceptedService;

use crate::proto;
use crate::pty::{MetadataReason, PtyEvent, PtyRegistry};
use crate::commands;

pub use crate::proto::terminal_service_server::{TerminalService, TerminalServiceServer};

pub const AUTH_TOKEN: &str = "termd-dev-secret";

pub fn auth_interceptor(req: Request<()>) -> Result<Request<()>, Status> {
    match req.metadata().get("x-auth-token") {
        Some(v) if v.as_bytes() == AUTH_TOKEN.as_bytes() => Ok(req),
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

type AuthedTerminalService = InterceptedService<
    TerminalServiceServer<TerminalServiceImpl>,
    fn(Request<()>) -> Result<Request<()>, Status>,
>;

pub fn make_service(
    registry: Arc<PtyRegistry>,
    log_grpc: bool,
) -> AuthedTerminalService {
    TerminalServiceServer::with_interceptor(
        TerminalServiceImpl::new(registry, log_grpc),
        auth_interceptor as fn(Request<()>) -> Result<Request<()>, Status>,
    )
}

async fn dispatch_command(
    cmd: proto::TerminalCommand,
    registry: &Arc<PtyRegistry>,
    subscribed_ids: &mut std::collections::HashSet<String>,
    sub_tasks: &mut std::collections::HashMap<String, tokio::task::JoinHandle<()>>,
    sub_tx: &tokio::sync::mpsc::Sender<(String, PtyEvent)>,
) -> proto::TerminalResponse {
    use proto::terminal_command::Command;

    match cmd.command {
        None => proto::TerminalResponse {
            response: Some(proto::terminal_response::Response::Command(
                proto::CommandResponse {
                    pty_id: String::new(),
                    success: false,
                    error: Some("empty command".into()),
                }
            )),
        },
        Some(Command::List(_r))        => commands::handle_list(registry, subscribed_ids),
        Some(Command::Create(r))      => commands::handle_create(registry, r),
        Some(Command::Destroy(r))     => commands::handle_destroy(registry, r, subscribed_ids, sub_tasks),
        Some(Command::Subscribe(r))   => commands::handle_subscribe(registry, r, subscribed_ids, sub_tasks, sub_tx),
        Some(Command::Unsubscribe(r)) => commands::handle_unsubscribe(registry, r, subscribed_ids, sub_tasks),
        Some(Command::Write(r))       => commands::handle_write(registry, r),
        Some(Command::Resize(r))      => commands::handle_resize(registry, r),
        Some(Command::SetTitle(r))    => commands::handle_set_title(registry, r),
        Some(Command::Refresh(r))     => commands::handle_refresh(registry, r).await,
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
        let (sub_tx, mut sub_rx) = mpsc::channel::<(String, PtyEvent)>(1024);

        tokio::spawn(async move {
            let mut sub_tasks: HashMap<String, tokio::task::JoinHandle<()>> = HashMap::new();
            let mut subscribed_ids: HashSet<String> = HashSet::new();

            loop {
                tokio::select! {
                    cmd = inbound.next() => {
                        match cmd {
                            None => break,
                            Some(Err(e)) => {
                                // NOTE: When clients disappear, e.code() == tonic::Code::Unknown happens,
                                // check if this is expected or if we are missing something.
                                tracing::warn!("stream read error: {e}");
                                break;
                            }
                            Some(Ok(cmd)) => {
                                if log_grpc {
                                    tracing::debug!(command = ?cmd, "gRPC request");
                                }
                                let resp = dispatch_command(
                                    cmd, &registry, &mut subscribed_ids, &mut sub_tasks, &sub_tx
                                ).await;
                                if log_grpc {
                                    tracing::debug!(response = ?resp, "gRPC response");
                                }
                                if resp_tx.send(Ok(resp)).await.is_err() { break; }
                            }
                        }
                    }
                    Some((pty_id, event)) = sub_rx.recv() => {
                        let resp = match event {
                            PtyEvent::Data(chunk) => proto::TerminalResponse {
                                response: Some(proto::terminal_response::Response::Stream(
                                    proto::StreamData {
                                        pty_id,
                                        generation: chunk.generation,
                                        data: chunk.data.to_vec(),
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
                                            item: Some(commands::pty_info_to_item(meta.info.clone(), true)),
                                            reason: reason as i32,
                                            exit_code: meta.exit_code,
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
            for (_, t) in sub_tasks { t.abort(); }
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(resp_rx)))
    }
}

pub async fn serve(
    registry: Arc<PtyRegistry>,
    unix_path: &std::path::Path,
    tcp_addr: std::net::SocketAddr,
    log_grpc: bool,
) -> anyhow::Result<()> {
    use tokio::net::UnixListener;
    use tonic::transport::Server;
    use tokio_stream::wrappers::{TcpListenerStream, UnixListenerStream};

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

    let svc_unix = make_service(registry.clone(), log_grpc);
    let svc_tcp  = make_service(registry, log_grpc);

    tokio::try_join!(
        Server::builder()
            .add_service(svc_unix)
            .serve_with_incoming(UnixListenerStream::new(unix_listener)),
        Server::builder()
            .add_service(svc_tcp)
            .serve_with_incoming(TcpListenerStream::new(tcp_listener)),
    )?;

    Ok(())
}
