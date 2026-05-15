use std::sync::Arc;

use tonic::{Request, Response, Status, Streaming};
use tonic::service::interceptor::InterceptedService;

use crate::pty::PtyRegistry;

pub mod proto {
    tonic::include_proto!("terminal");
}

pub use proto::terminal_service_server::{TerminalService, TerminalServiceServer};

pub const AUTH_TOKEN: &str = "termd-dev-secret";

pub fn auth_interceptor(req: Request<()>) -> Result<Request<()>, Status> {
    match req.metadata().get("x-auth-token") {
        Some(v) if v.as_bytes() == AUTH_TOKEN.as_bytes() => Ok(req),
        _ => Err(Status::unauthenticated("invalid or missing x-auth-token")),
    }
}

#[derive(Clone)]
pub struct TerminalServiceImpl {
    pub registry: Arc<PtyRegistry>,
    pub log_grpc: bool,
}

impl TerminalServiceImpl {
    pub fn new(registry: Arc<PtyRegistry>, log_grpc: bool) -> Self {
        Self { registry, log_grpc }
    }
}

pub fn make_service(
    registry: Arc<PtyRegistry>,
    log_grpc: bool,
) -> InterceptedService<TerminalServiceServer<TerminalServiceImpl>, fn(Request<()>) -> Result<Request<()>, Status>> {
    TerminalServiceServer::with_interceptor(
        TerminalServiceImpl::new(registry, log_grpc),
        auth_interceptor,
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
