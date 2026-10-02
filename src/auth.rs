//! TCP listener credentials: the auth token and the self-signed TLS cert.
//!
//! Both live in `$XDG_STATE_HOME/termd` (default `~/.local/state/termd`) so
//! they survive daemon restarts. Clients pin the cert by its SHA-256
//! fingerprint instead of validating a hostname, which is what makes bare-IP
//! access work without a CA.

use std::fs;
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use anyhow::{Context, Result};
use ring::digest::{SHA256, digest};

pub fn state_dir() -> PathBuf {
    std::env::var_os("XDG_STATE_HOME")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".local/state")))
        .unwrap_or_else(|| PathBuf::from("/tmp"))
        .join("termd")
}

fn ensure_dir(dir: &Path) -> Result<()> {
    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)
        .with_context(|| format!("creating {}", dir.display()))?;
    // The dir may predate us (e.g. the /tmp/termd fallback, made by another
    // user), and mode only applies on creation, so check what we got.
    use std::os::unix::fs::MetadataExt;
    let meta = fs::symlink_metadata(dir)?;
    anyhow::ensure!(meta.is_dir(), "{} is not a directory", dir.display());
    anyhow::ensure!(
        meta.uid() == unsafe { libc::geteuid() },
        "{} is not owned by the current user",
        dir.display()
    );
    anyhow::ensure!(
        meta.mode() & 0o077 == 0,
        "{} is accessible by other users (mode {:o}); chmod 700 it",
        dir.display(),
        meta.mode() & 0o777
    );
    Ok(())
}

/// Write `contents` to `path` with mode 0600, atomically via rename.
fn write_private(path: &Path, contents: &[u8]) -> Result<()> {
    let tmp = path.with_extension("tmp");
    let mut f = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&tmp)
        .with_context(|| format!("writing {}", tmp.display()))?;
    f.write_all(contents)?;
    f.sync_all()?;
    fs::rename(&tmp, path)?;
    Ok(())
}

/// Generate a fresh random auth token: 32 hex digits (128 bits).
pub fn generate_token() -> String {
    format!("{:032x}", uuid::Uuid::new_v4().as_u128())
}

/// Compare without short-circuiting, so response timing doesn't leak how many
/// leading bytes of a guess were right.
pub fn token_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// The single live TCP token, shared by the auth interceptor and the admin
/// service. When backed by a file, rotations are persisted there.
#[derive(Clone)]
pub struct TokenStore {
    token: Arc<RwLock<String>>,
    path: Option<PathBuf>,
}

impl TokenStore {
    /// An in-memory store (tests).
    pub fn fixed(token: impl Into<String>) -> Self {
        Self { token: Arc::new(RwLock::new(token.into())), path: None }
    }

    /// Load the token from `dir/token`, creating one if absent.
    pub fn load_or_create(dir: &Path) -> Result<Self> {
        ensure_dir(dir)?;
        let path = dir.join("token");
        let token = match fs::read_to_string(&path) {
            Ok(s) if !s.trim().is_empty() => s.trim().to_string(),
            Ok(_) => {
                let t = generate_token();
                write_private(&path, t.as_bytes())?;
                t
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let t = generate_token();
                write_private(&path, t.as_bytes())?;
                t
            }
            Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
        };
        Ok(Self { token: Arc::new(RwLock::new(token)), path: Some(path) })
    }

    pub fn get(&self) -> String {
        self.token.read().unwrap().clone()
    }

    pub fn check(&self, presented: &[u8]) -> bool {
        token_eq(self.token.read().unwrap().as_bytes(), presented)
    }

    /// Switch to a new token (persisting it first) and return it.
    pub fn refresh(&self) -> Result<String> {
        let t = generate_token();
        if let Some(path) = &self.path {
            write_private(path, t.as_bytes())?;
        }
        *self.token.write().unwrap() = t.clone();
        Ok(t)
    }
}

/// The daemon's TLS identity, PEM-encoded.
pub struct ServerCert {
    pub cert_pem: String,
    pub key_pem: String,
    pub fingerprint: String,
}

/// Load `dir/cert.pem` + `dir/key.pem`, generating a self-signed pair if absent.
pub fn load_or_create_cert(dir: &Path) -> Result<ServerCert> {
    ensure_dir(dir)?;
    let cert_path = dir.join("cert.pem");
    let key_path = dir.join("key.pem");
    let (cert_pem, key_pem) = if cert_path.exists() && key_path.exists() {
        (fs::read_to_string(&cert_path)?, fs::read_to_string(&key_path)?)
    } else {
        let host = hostname::get()
            .ok()
            .and_then(|h| h.into_string().ok())
            .unwrap_or_else(|| "termd".into());
        let mut params = rcgen::CertificateParams::new(vec![host.clone(), "localhost".into()])
            .context("building certificate params")?;
        params.distinguished_name = rcgen::DistinguishedName::new();
        params.distinguished_name.push(rcgen::DnType::CommonName, format!("termd@{host}"));
        let key = rcgen::KeyPair::generate().context("generating key pair")?;
        let cert = params.self_signed(&key).context("self-signing certificate")?;
        let (c, k) = (cert.pem(), key.serialize_pem());
        write_private(&key_path, k.as_bytes())?;
        write_private(&cert_path, c.as_bytes())?;
        (c, k)
    };
    let der = first_cert_der(&cert_pem)?;
    Ok(ServerCert { fingerprint: fingerprint(&der), cert_pem, key_pem })
}

fn first_cert_der(pem: &str) -> Result<Vec<u8>> {
    use rustls::pki_types::{pem::PemObject, CertificateDer};
    let cert = CertificateDer::pem_slice_iter(pem.as_bytes())
        .next()
        .context("no certificate in cert.pem")??;
    Ok(cert.to_vec())
}

/// `sha256:<hex>` over a certificate's DER encoding.
pub fn fingerprint(der: &[u8]) -> String {
    format!("sha256:{}", hex::encode(digest(&SHA256, der)))
}

/// Normalize a user-supplied fingerprint: optional `sha256:` prefix, colons
/// and case ignored (so `openssl x509 -fingerprint` output pastes in).
pub fn normalize_fingerprint(s: &str) -> Result<String> {
    let hex: String = s
        .trim()
        .trim_start_matches("sha256:")
        .trim_start_matches("SHA256:")
        .chars()
        .filter(|c| *c != ':')
        .collect::<String>()
        .to_ascii_lowercase();
    anyhow::ensure!(
        hex.len() == 64 && hex.chars().all(|c| c.is_ascii_hexdigit()),
        "fingerprint must be 64 hex digits (optionally prefixed with sha256:)"
    );
    Ok(format!("sha256:{hex}"))
}

/// Client-side rustls config. With a fingerprint, trust exactly the cert that
/// hashes to it (any name, any chain); without, verify normally against the
/// system roots (e.g. termd behind caddy with a real certificate).
pub fn client_tls_config(pinned: Option<&str>) -> Result<rustls::ClientConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()?;
    let mut cfg = match pinned {
        Some(fp) => builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(PinnedVerifier {
                fingerprint: normalize_fingerprint(fp)?,
                provider,
            }))
            .with_no_client_auth(),
        None => {
            let mut roots = rustls::RootCertStore::empty();
            let native = rustls_native_certs::load_native_certs();
            roots.add_parsable_certificates(native.certs);
            anyhow::ensure!(!roots.is_empty(), "no system root certificates found");
            builder.with_root_certificates(roots).with_no_client_auth()
        }
    };
    cfg.alpn_protocols = vec![b"h2".to_vec()];
    Ok(cfg)
}

#[derive(Debug)]
struct PinnedVerifier {
    fingerprint: String,
    provider: Arc<rustls::crypto::CryptoProvider>,
}

impl rustls::client::danger::ServerCertVerifier for PinnedVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        let got = fingerprint(end_entity);
        if token_eq(got.as_bytes(), self.fingerprint.as_bytes()) {
            Ok(rustls::client::danger::ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::General(format!(
                "server certificate fingerprint {got} does not match pinned {}",
                self.fingerprint
            )))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message, cert, dss, &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message, cert, dss, &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.provider.signature_verification_algorithms.supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprint_is_sha256_hex() {
        // FIPS 180-2 test vectors; pins the format users paste as `#sha256:<hex>`.
        assert_eq!(
            fingerprint(b""),
            "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            fingerprint(b"abc"),
            "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn token_persists_and_refreshes() {
        let dir = tempfile::tempdir().unwrap();
        let a = TokenStore::load_or_create(dir.path()).unwrap();
        let t1 = a.get();
        assert_eq!(t1.len(), 32);
        assert_eq!(TokenStore::load_or_create(dir.path()).unwrap().get(), t1);
        let t2 = a.refresh().unwrap();
        assert_ne!(t1, t2);
        assert!(a.check(t2.as_bytes()) && !a.check(t1.as_bytes()));
        assert_eq!(TokenStore::load_or_create(dir.path()).unwrap().get(), t2);
        let mode = fs::metadata(dir.path().join("token")).unwrap().permissions();
        assert_eq!(std::os::unix::fs::PermissionsExt::mode(&mode) & 0o777, 0o600);
    }

    #[test]
    fn rejects_loose_state_dir() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("termd");
        fs::create_dir(&sub).unwrap();
        fs::set_permissions(&sub, fs::Permissions::from_mode(0o755)).unwrap();
        assert!(TokenStore::load_or_create(&sub).is_err());
    }

    #[test]
    fn cert_is_stable_across_loads() {
        let dir = tempfile::tempdir().unwrap();
        let a = load_or_create_cert(dir.path()).unwrap();
        let b = load_or_create_cert(dir.path()).unwrap();
        assert_eq!(a.fingerprint, b.fingerprint);
        assert_eq!(normalize_fingerprint(&a.fingerprint.to_uppercase().replace("SHA256:", "")).unwrap(), a.fingerprint);
    }
}
