//! Channel setup shared by the CLI and tests.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};

pub async fn socket_channel(path: PathBuf) -> Result<tonic::transport::Channel> {
    use hyper_util::rt::TokioIo;
    use tonic::transport::Endpoint;
    use tower::service_fn;
    Ok(Endpoint::try_from("http://[::]:1")?
        .connect_with_connector(service_fn(move |_| {
            let path = path.clone();
            async move { tokio::net::UnixStream::connect(path).await.map(TokioIo::new) }
        }))
        .await?)
}

/// Connect over TCP, doing TLS ourselves for https so we can pin a
/// self-signed cert by fingerprint (tonic's TLS config can't).
pub async fn tcp_channel(uri: &str, fingerprint: Option<&str>) -> Result<tonic::transport::Channel> {
    use hyper_util::rt::TokioIo;
    use tonic::transport::{Endpoint, Uri};
    use tower::service_fn;

    // `https://host:port#sha256:<hex>` carries the pin in the fragment.
    let (uri, fingerprint) = match uri.split_once('#') {
        Some((base, frag)) => {
            if let Some(fp) = fingerprint {
                anyhow::ensure!(
                    crate::auth::normalize_fingerprint(fp)? == crate::auth::normalize_fingerprint(frag)?,
                    "--fingerprint conflicts with the endpoint's #fingerprint"
                );
            }
            (base, Some(frag))
        }
        None => (uri, fingerprint),
    };
    let parsed: Uri = uri.parse()?;
    match parsed.scheme_str() {
        Some("https") => {}
        Some("http") => {
            anyhow::ensure!(fingerprint.is_none(), "--fingerprint needs an https:// endpoint");
            return Ok(Endpoint::from_shared(uri.to_string())?.connect().await?);
        }
        _ => anyhow::bail!("endpoint must be http:// or https://"),
    }
    let host = parsed.host().context("endpoint has no host")?.to_string();
    let port = parsed.port_u16().unwrap_or(443);
    let bare_host = host.trim_start_matches('[').trim_end_matches(']').to_string();
    let server_name = rustls::pki_types::ServerName::try_from(bare_host.clone())?;
    let tls = tokio_rustls::TlsConnector::from(Arc::new(crate::auth::client_tls_config(fingerprint)?));

    // tonic only sees plaintext h2 from the connector, so hand it an http URI.
    let authority = parsed.authority().context("endpoint has no authority")?.as_str();
    Ok(Endpoint::from_shared(format!("http://{authority}"))?
        .connect_with_connector(service_fn(move |_| {
            let (tls, server_name, bare_host) = (tls.clone(), server_name.clone(), bare_host.clone());
            async move {
                let tcp = tokio::net::TcpStream::connect((bare_host.as_str(), port)).await?;
                let stream = tls.connect(server_name, tcp).await?;
                Ok::<_, std::io::Error>(TokioIo::new(stream))
            }
        }))
        .await?)
}
