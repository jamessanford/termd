use std::sync::Arc;

use tonic::{Request, Response, Status, Streaming};
use tonic::service::interceptor::InterceptedService;

use crate::proto;
use crate::pty::PtyRegistry;

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
    pub(crate) log_grpc: bool, // TODO Task 5: used in stream handler
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

#[tonic::async_trait]
impl TerminalService for TerminalServiceImpl {
    type StreamStream = tokio_stream::wrappers::ReceiverStream<Result<proto::TerminalResponse, Status>>;

    async fn stream(
        &self,
        _request: Request<Streaming<proto::TerminalCommand>>,
    ) -> Result<Response<Self::StreamStream>, Status> {
        Err(Status::unimplemented("stream handler not yet implemented"))
    }
}
