use std::sync::Arc;

use tonic::{Request, Response, Status, Streaming};
use tonic::service::interceptor::InterceptedService;

use crate::proto;
use crate::pty::{PtyChunk, PtyRegistry};

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
    sub_tx: &tokio::sync::mpsc::Sender<(String, Arc<PtyChunk>)>,
) -> proto::TerminalResponse {
    use proto::terminal_command::Command;
    use crate::commands;

    match cmd.command {
        None => proto::TerminalResponse { response: None },
        Some(Command::List(_r))        => commands::handle_list(registry, subscribed_ids),
        Some(Command::Create(r))      => commands::handle_create(registry, r),
        Some(Command::Destroy(r))     => commands::handle_destroy(registry, r),
        Some(Command::Subscribe(r))   => commands::handle_subscribe(registry, r, subscribed_ids, sub_tasks, sub_tx),
        Some(Command::Unsubscribe(r)) => commands::handle_unsubscribe(r, subscribed_ids, sub_tasks),
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
        let (sub_tx, mut sub_rx) = mpsc::channel::<(String, Arc<PtyChunk>)>(1024);

        tokio::spawn(async move {
            let mut sub_tasks: HashMap<String, tokio::task::JoinHandle<()>> = HashMap::new();
            let mut subscribed_ids: HashSet<String> = HashSet::new();

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
                    Some((pty_id, chunk)) = sub_rx.recv() => {
                        let resp = proto::TerminalResponse {
                            response: Some(proto::terminal_response::Response::Stream(
                                proto::StreamData {
                                    pty_id,
                                    generation: chunk.generation,
                                    data: chunk.data.to_vec(),
                                }
                            )),
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
