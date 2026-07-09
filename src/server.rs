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

/// Owns exactly one Subscribe stream's bookkeeping and reaps it on drop — whether
/// the forwarding task ends cleanly or unwinds (panic). Doing the cleanup from
/// `Drop` rather than as straight-line code after the loop means a panicking
/// handler can't strand this subscriber's entry on its PTY. See docs/REAP.md for
/// the bug this hardens against.
struct StreamReaper {
    registry:      Arc<PtyRegistry>,
    pty_id:        u64,
    subscriber_id: String,
}

impl Drop for StreamReaper {
    fn drop(&mut self) {
        tracing::debug!(
            subscriber_id = %self.subscriber_id,
            pty_id = self.pty_id,
            "subscribe stream ended, reaping subscriber"
        );
        if let Some(handle) = self.registry.get(self.pty_id) {
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
}

/// Map one internal `PtyEvent` to its wire `SubscribeEvent`. `generation` never
/// appears on the wire (the per-PTY stream + ordered refresh make it unneeded).
fn refresh_to_proto(rd: &crate::pty::RefreshData) -> proto::RefreshData {
    proto::RefreshData {
        data:     rd.data.to_vec(),
        size:     Some(proto::Size { cols: rd.cols, rows: rd.rows }),
        degraded: rd.degraded,
    }
}

fn event_to_subscribe(event: PtyEvent) -> proto::SubscribeEvent {
    use proto::subscribe_event::Event;
    let inner = match event {
        PtyEvent::Data(chunk) => Event::Data(proto::StreamData { data: chunk.data.to_vec() }),
        PtyEvent::Refresh(rd) => Event::Refresh(refresh_to_proto(&rd)),
        // Per-subscriber snapshot — the caller has already confirmed it's ours
        // (the subscriber_id filter lives in the forwarding loop).
        PtyEvent::RefreshFor { data, .. } => Event::Refresh(refresh_to_proto(&data)),
        PtyEvent::Metadata(meta) => {
            use proto::stream_metadata::Event as MetaEvent;
            let inner = match meta.reason {
                MetadataReason::Resize => MetaEvent::Resized(proto::Resized {
                    size: Some(proto::Size { cols: meta.info.cols, rows: meta.info.rows }),
                }),
                MetadataReason::TitleChanged => MetaEvent::TitleChanged(proto::TitleChanged {
                    title: meta.info.title.clone(),
                }),
                MetadataReason::Closed => MetaEvent::Exited(proto::Exited {
                    exit_code: meta.exit_code.unwrap_or_default(),
                }),
                MetadataReason::SubscribersChanged => MetaEvent::SubscribersChanged(
                    proto::SubscribersChanged {
                        subscribers: commands::pty_info_to_item(meta.info.clone()).subscribers,
                    },
                ),
            };
            Event::Metadata(proto::StreamMetadata { event: Some(inner) })
        }
    };
    proto::SubscribeEvent { event: Some(inner) }
}

#[tonic::async_trait]
impl TerminalService for TerminalServiceImpl {
    async fn list(
        &self,
        request: Request<proto::ListRequest>,
    ) -> Result<Response<proto::ListResponse>, Status> {
        if self.log_grpc { tracing::debug!(request = ?request.get_ref(), "gRPC list"); }
        Ok(Response::new(commands::handle_list(&self.registry)))
    }

    async fn create(
        &self,
        request: Request<proto::CreateRequest>,
    ) -> Result<Response<proto::PtyItem>, Status> {
        if self.log_grpc { tracing::debug!(request = ?request.get_ref(), "gRPC create"); }
        commands::handle_create(&self.registry, request.into_inner()).map(Response::new)
    }

    async fn destroy(
        &self,
        request: Request<proto::DestroyRequest>,
    ) -> Result<Response<()>, Status> {
        if self.log_grpc { tracing::debug!(request = ?request.get_ref(), "gRPC destroy"); }
        commands::handle_destroy(&self.registry, request.into_inner()).map(Response::new)
    }

    async fn resize(
        &self,
        request: Request<proto::ResizeRequest>,
    ) -> Result<Response<proto::PtyItem>, Status> {
        if self.log_grpc { tracing::debug!(request = ?request.get_ref(), "gRPC resize"); }
        commands::handle_resize(&self.registry, request.into_inner()).await.map(Response::new)
    }

    async fn set_title(
        &self,
        request: Request<proto::SetTitleRequest>,
    ) -> Result<Response<proto::PtyItem>, Status> {
        if self.log_grpc { tracing::debug!(request = ?request.get_ref(), "gRPC set_title"); }
        commands::handle_set_title(&self.registry, request.into_inner()).map(Response::new)
    }

    async fn refresh(
        &self,
        request: Request<proto::RefreshRequest>,
    ) -> Result<Response<()>, Status> {
        if self.log_grpc { tracing::debug!(request = ?request.get_ref(), "gRPC refresh"); }
        commands::handle_refresh(&self.registry, request.into_inner()).await.map(Response::new)
    }

    async fn scrollback(
        &self,
        request: Request<proto::ScrollbackRequest>,
    ) -> Result<Response<proto::ScrollbackResponse>, Status> {
        if self.log_grpc { tracing::debug!(request = ?request.get_ref(), "gRPC scrollback"); }
        commands::handle_scrollback(&self.registry, request.into_inner()).await.map(Response::new)
    }

    type SubscribeStream =
        tokio_stream::wrappers::ReceiverStream<Result<proto::SubscribeEvent, Status>>;

    async fn subscribe(
        &self,
        request: Request<Streaming<proto::SubscribeFrame>>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        use proto::subscribe_frame::Frame;
        use proto::subscribe_event::Event;
        use tokio::sync::mpsc;
        use tokio::sync::broadcast::error::RecvError;
        use tokio_stream::StreamExt;
        use tokio_stream::wrappers::{BroadcastStream, ReceiverStream};

        let registry = self.registry.clone();
        let log_grpc = self.log_grpc;
        let mut inbound = request.into_inner();

        // First frame must be SubscribeStart, naming the PTY to attach to.
        let start = match inbound.message().await? {
            Some(proto::SubscribeFrame { frame: Some(Frame::Start(start)) }) => start,
            _ => return Err(Status::invalid_argument("expected SubscribeStart as first frame")),
        };

        let handle = registry
            .get(start.pty_id)
            .ok_or_else(|| Status::not_found("PTY not found"))?;
        let pty_id = start.pty_id;

        // Server-assigned id — the client learns it via SubscribeReady.
        let subscriber_id = uuid::Uuid::new_v4().to_string();

        let (resp_tx, resp_rx) =
            mpsc::channel::<Result<proto::SubscribeEvent, Status>>(256);

        // First event: hand the client its subscriber_id.
        if resp_tx
            .send(Ok(proto::SubscribeEvent {
                event: Some(Event::Ready(proto::SubscribeReady {
                    subscriber_id: subscriber_id.clone(),
                })),
            }))
            .await
            .is_err()
        {
            // Client already gone; nothing registered yet, so nothing to reap.
            return Ok(Response::new(ReceiverStream::new(resp_rx)));
        }

        // Subscribe to the broadcast channels BEFORE apply_subscribe so we don't
        // miss the Resize this subscriber's own refit (inside apply_subscribe)
        // may broadcast — the client wants to see itself refit the PTY.
        let mut data_rx = handle.subscribe();
        let meta_rx = handle.meta_subscribe();

        let size = start.size.unwrap_or_default();
        let info = crate::pty::SubscriberInfo {
            hostname:   start.hostname,
            cols:       size.cols,
            rows:       size.rows,
            created_at: std::time::SystemTime::now(),
        };
        commands::apply_subscribe(&handle, &subscriber_id, info).await;

        tokio::spawn(async move {
            // Reaper drops on clean exit OR unwind, reaping the subscriber.
            // Owned by this task so the cleanup is panic-safe (see docs/REAP.md).
            let _reaper = StreamReaper {
                registry:      registry.clone(),
                pty_id,
                subscriber_id: subscriber_id.clone(),
            };

            let mut meta_stream = BroadcastStream::new(meta_rx);
            // Once the meta stream is exhausted (sender dropped) it yields `None`
            // immediately and forever; gate its select arm so we stop polling it
            // and don't busy-spin while the data arm is still serving.
            let mut meta_done = false;

            // Build a DataLost event (broadcast lag → client should Refresh).
            let data_lost = || proto::SubscribeEvent {
                event: Some(Event::Metadata(proto::StreamMetadata {
                    event: Some(proto::stream_metadata::Event::DataLost(proto::DataLost {})),
                })),
            };

            loop {
                tokio::select! {
                    item = data_rx.recv() => {
                        match item {
                            // A per-subscriber refresh snapshot rides the data
                            // broadcast addressed to one subscriber. Skip it unless
                            // it's ours — ordering relative to data is guaranteed
                            // because the reader sequences it inline on this channel.
                            Ok(PtyEvent::RefreshFor { subscriber_id: ref sid, .. })
                                if *sid != subscriber_id => {}
                            Ok(event) => {
                                let ev = event_to_subscribe(event);
                                if resp_tx.send(Ok(ev)).await.is_err() { break; }
                            }
                            Err(RecvError::Lagged(n)) => {
                                tracing::debug!(subscriber_id = %subscriber_id, lagged = n, "data stream lagged");
                                if resp_tx.send(Ok(data_lost())).await.is_err() { break; }
                            }
                            Err(RecvError::Closed) => break,
                        }
                    }
                    item = meta_stream.next(), if !meta_done => {
                        match item {
                            Some(Ok(meta)) => {
                                let ev = event_to_subscribe(PtyEvent::Metadata(meta));
                                if resp_tx.send(Ok(ev)).await.is_err() { break; }
                            }
                            Some(Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(n))) => {
                                tracing::debug!(subscriber_id = %subscriber_id, lagged = n, "meta stream lagged");
                                // A lagged-away Resize/TitleChanged is also repaired
                                // by a refresh (RefreshData carries the size).
                                if resp_tx.send(Ok(data_lost())).await.is_err() { break; }
                            }
                            // Meta channel closed: park this arm, keep serving data.
                            None => meta_done = true,
                        }
                    }
                    msg = inbound.message() => {
                        match msg {
                            Ok(Some(proto::SubscribeFrame { frame: Some(Frame::Write(w)) })) => {
                                if let Err(e) = handle.write(&w.data) {
                                    tracing::debug!(subscriber_id = %subscriber_id, "pty write error: {e}");
                                }
                            }
                            Ok(Some(proto::SubscribeFrame { frame: Some(Frame::Start(_)) })) => {
                                tracing::warn!(subscriber_id = %subscriber_id, "unexpected SubscribeStart after first frame; ignoring");
                            }
                            Ok(Some(proto::SubscribeFrame { frame: None })) => {
                                tracing::debug!(subscriber_id = %subscriber_id, "empty SubscribeFrame; ignoring");
                            }
                            Ok(None) => break,        // client closed the up-stream
                            Err(e) => {
                                if log_grpc { tracing::debug!(subscriber_id = %subscriber_id, "subscribe read error: {e}"); }
                                break;
                            }
                        }
                    }
                }
            }
            // `_reaper` drops here — clean exit or unwind — reaping this subscriber
            // from its PTY.
        });

        Ok(Response::new(ReceiverStream::new(resp_rx)))
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
