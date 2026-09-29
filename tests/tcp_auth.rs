//! End-to-end check of the TCP listener's auth: pinned TLS, the token, and
//! AdminService being reachable only over the unix socket.

use std::sync::Arc;
use std::time::Duration;

use tonic::{Code, Request};

use termd::auth::{load_or_create_cert, TokenStore};
use termd::client::{socket_channel, tcp_channel};
use termd::proto::admin_service_client::AdminServiceClient;
use termd::proto::terminal_service_client::TerminalServiceClient;
use termd::proto::{ListRequest, TokenRequest, TokenResponse};
use termd::pty::PtyRegistry;

fn with_token(token: &str) -> impl FnMut(Request<()>) -> Result<Request<()>, tonic::Status> {
    let value: tonic::metadata::MetadataValue<_> = token.parse().unwrap();
    move |mut req| {
        req.metadata_mut().insert("x-auth-token", value.clone());
        Ok(req)
    }
}

#[tokio::test]
async fn tcp_auth_end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    let sock = dir.path().join("termd.sock");
    let cert = load_or_create_cert(dir.path()).unwrap();
    let tokens = TokenStore::fixed("initial-token");
    let registry = Arc::new(PtyRegistry::new());
    let addr = "127.0.0.1:0".parse().unwrap();
    tokio::spawn({
        let sock = sock.clone();
        async move { termd::server::serve(registry, &sock, addr, tokens, Some(cert), false).await }
    });
    for _ in 0..100 {
        if sock.exists() { break; }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Local credentials come from the socket.
    let mut admin = AdminServiceClient::new(socket_channel(sock.clone()).await.unwrap());
    let TokenResponse { token, fingerprint, listen } =
        admin.token(TokenRequest { refresh: false }).await.unwrap().into_inner();
    assert_eq!(token, "initial-token");
    assert!(fingerprint.starts_with("sha256:"));
    let endpoint = format!("https://{listen}#{fingerprint}");

    // Pinned TLS + token: accepted.
    let ch = tcp_channel(&endpoint, None).await.unwrap();
    TerminalServiceClient::with_interceptor(ch.clone(), with_token(&token))
        .list(ListRequest {}).await.unwrap();

    // Wrong token: rejected.
    let err = TerminalServiceClient::with_interceptor(ch.clone(), with_token("wrong"))
        .list(ListRequest {}).await.unwrap_err();
    assert_eq!(err.code(), Code::Unauthenticated);

    // Wrong pin, or no pin against a self-signed cert: TLS fails.
    let bad_pin = format!("https://{listen}#sha256:{}", "0".repeat(64));
    assert!(tcp_channel(&bad_pin, None).await.is_err());
    assert!(tcp_channel(&format!("https://{listen}"), None).await.is_err());

    // AdminService is not mounted on TCP, even with a valid token.
    let err = AdminServiceClient::with_interceptor(ch.clone(), with_token(&token))
        .token(TokenRequest { refresh: true }).await.unwrap_err();
    assert_eq!(err.code(), Code::Unimplemented);

    // Refresh over the socket: old token stops working, new one works.
    let new = admin.token(TokenRequest { refresh: true }).await.unwrap().into_inner().token;
    assert_ne!(new, token);
    let err = TerminalServiceClient::with_interceptor(ch.clone(), with_token(&token))
        .list(ListRequest {}).await.unwrap_err();
    assert_eq!(err.code(), Code::Unauthenticated);
    TerminalServiceClient::with_interceptor(ch, with_token(&new))
        .list(ListRequest {}).await.unwrap();
}
