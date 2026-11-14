//! Lane TLS (RealityEngine_CI#668, this repo's #72).
//!
//! The runtime contract is RealityEngine_CI `docs/TRANSPORT_SECURITY.md`:
//! `RE_TLS_CERT`, `RE_TLS_KEY`, `RE_TLS_CA`, `RE_TLS_CRL`. With `RE_TLS_CERT`
//! set the RE and PE serve TLS 1.3 only on their own port, with the profile's
//! suites and groups and a client certificate from the lane CA, and call
//! internal peers over https with the lane CA and their own certificate. With
//! it unset nothing here runs and the engine serves and calls as before.
//!
//! The voting engines make the same choices (C++ `src/http.cpp`, LSP
//! `src/tls.lisp`, Scala `LaneTls`): the internal-peer rule, the PE's device
//! routes and the 403 body. A difference would read as a divergence.
//!
//! The crypto provider is rustls' `ring`, chosen so the build needs no C
//! toolchain beyond `cc`. It has no X25519MLKEM768, so this follower uses the
//! profile's fallback groups and says so at startup.

use axum::Router;
use axum::body::Body;
use axum::extract::Request;
use axum::http::{StatusCode, header};
use axum::middleware::Next;
use axum::response::Response;
use rustls::crypto::{CryptoProvider, ring};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, CertificateRevocationListDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use std::net::IpAddr;
use std::sync::Arc;

/// The body every engine answers when a route needs a client certificate.
pub const CLIENT_CERTIFICATE_REQUIRED: &str = r#"{"error":"client certificate required"}"#;

/// The PE's device routes: the HealthKit bridge on a phone reaches them with
/// server TLS and its bearer token (localHealthkitBridge
/// `docs/INGEST_CONTRACT.md`). `resync` is called by a service and is not one.
pub const PE_DEVICE_ROUTES: [&str; 3] = [
    "/api/integrations/healthkit/ingest",
    "/api/integrations/healthkit/status",
    "/api/integrations/healthkit/scope",
];

/// The groups this build negotiates: the profile's fallback, since `ring` has
/// no X25519MLKEM768.
pub const GROUPS: &str = "X25519:secp384r1";

#[derive(Clone, Debug)]
pub struct Settings {
    pub cert: String,
    pub key: String,
    pub ca: String,
    pub crl: Option<String>,
}

fn env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|v| v.trim().to_owned())
        .filter(|v| !v.is_empty())
}

impl Settings {
    /// `RE_TLS_CERT` set: serve TLS. A partial configuration is an error, never
    /// a plaintext start.
    pub fn serving() -> Result<Option<Settings>, String> {
        let Some(cert) = env("RE_TLS_CERT") else {
            return Ok(None);
        };
        let key = env("RE_TLS_KEY").ok_or("TLS: RE_TLS_CERT is set, so RE_TLS_KEY must be too")?;
        let ca = env("RE_TLS_CA").ok_or("TLS: RE_TLS_CERT is set, so RE_TLS_CA must be too")?;
        Ok(Some(Settings {
            cert,
            key,
            ca,
            crl: env("RE_TLS_CRL"),
        }))
    }
}

/// `RE_TLS_CA` set: the lane is on for calls too.
pub fn lane_on() -> bool {
    env("RE_TLS_CA").is_some()
}

/// `ring` held to the profile: TLS 1.3 suites AES-256-GCM and ChaCha20, groups
/// X25519 and secp384r1.
pub fn provider() -> Arc<CryptoProvider> {
    let base = ring::default_provider();
    let suites = base
        .cipher_suites
        .iter()
        .copied()
        .filter(|s| {
            matches!(
                s.suite(),
                rustls::CipherSuite::TLS13_AES_256_GCM_SHA384
                    | rustls::CipherSuite::TLS13_CHACHA20_POLY1305_SHA256
            )
        })
        .collect();
    let groups = base
        .kx_groups
        .iter()
        .copied()
        .filter(|g| {
            matches!(
                g.name(),
                rustls::NamedGroup::X25519 | rustls::NamedGroup::secp384r1
            )
        })
        .collect();
    Arc::new(CryptoProvider {
        cipher_suites: suites,
        kx_groups: groups,
        ..base
    })
}

fn certificates(path: &str, what: &str) -> Result<Vec<CertificateDer<'static>>, String> {
    let certs: Vec<_> = CertificateDer::pem_file_iter(path)
        .and_then(|it| it.collect::<Result<Vec<_>, _>>())
        .map_err(|e| format!("TLS: cannot load {what} {path}: {e}"))?;
    if certs.is_empty() {
        return Err(format!("TLS: no certificate in {what} {path}"));
    }
    Ok(certs)
}

fn private_key(path: &str) -> Result<PrivateKeyDer<'static>, String> {
    PrivateKeyDer::from_pem_file(path)
        .map_err(|e| format!("TLS: cannot load RE_TLS_KEY {path}: {e}"))
}

fn crls(path: &Option<String>) -> Result<Vec<CertificateRevocationListDer<'static>>, String> {
    let Some(path) = path else {
        return Ok(Vec::new());
    };
    let list: Vec<_> = CertificateRevocationListDer::pem_file_iter(path)
        .and_then(|it| it.collect::<Result<Vec<_>, _>>())
        .map_err(|e| format!("TLS: cannot load RE_TLS_CRL {path}: {e}"))?;
    if list.is_empty() {
        return Err(format!("TLS: no CRL in RE_TLS_CRL {path}"));
    }
    Ok(list)
}

fn lane_roots(ca: &str) -> Result<Arc<RootCertStore>, String> {
    let mut roots = RootCertStore::empty();
    for cert in certificates(ca, "RE_TLS_CA")? {
        roots
            .add(cert)
            .map_err(|e| format!("TLS: RE_TLS_CA {ca}: {e}"))?;
    }
    Ok(Arc::new(roots))
}

/// The server side. With `require_at_handshake` a client without a lane
/// certificate is refused at the handshake; without it one is requested, any
/// presented is verified, and [`require_client_certificate`] decides per route.
pub fn server_config(
    s: &Settings,
    require_at_handshake: bool,
) -> Result<Arc<ServerConfig>, String> {
    let provider = provider();
    let builder = WebPkiClientVerifier::builder_with_provider(lane_roots(&s.ca)?, provider.clone())
        .with_crls(crls(&s.crl)?);
    let builder = if require_at_handshake {
        builder
    } else {
        builder.allow_unauthenticated()
    };
    let verifier = builder
        .build()
        .map_err(|e| format!("TLS: client verifier: {e}"))?;
    let config = ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|e| format!("TLS: {e}"))?
        .with_client_cert_verifier(verifier)
        .with_single_cert(certificates(&s.cert, "RE_TLS_CERT")?, private_key(&s.key)?)
        .map_err(|e| format!("TLS: RE_TLS_KEY does not match RE_TLS_CERT: {e}"))?;
    Ok(Arc::new(config))
}

/// The client side for an internal peer: the lane CA alone (and its CRL), this
/// service's certificate where it has one.
pub fn internal_client_config(
    ca: &str,
    crl: &Option<String>,
    own: Option<&Settings>,
) -> Result<ClientConfig, String> {
    let provider = provider();
    let verifier = rustls::client::WebPkiServerVerifier::builder_with_provider(
        lane_roots(ca)?,
        provider.clone(),
    )
    .with_crls(crls(crl)?)
    .build()
    .map_err(|e| format!("TLS: server verifier: {e}"))?;
    let builder = ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|e| format!("TLS: {e}"))?
        .with_webpki_verifier(verifier);
    match own {
        Some(s) => builder
            .with_client_auth_cert(certificates(&s.cert, "RE_TLS_CERT")?, private_key(&s.key)?)
            .map_err(|e| format!("TLS: client certificate: {e}")),
        None => Ok(builder.with_no_client_auth()),
    }
}

/// The client side for an external party with the lane on: the system's roots,
/// held to the profile.
pub fn external_client_config() -> Result<ClientConfig, String> {
    let mut roots = RootCertStore::empty();
    for cert in rustls_native_certs().into_iter().flatten() {
        let _ = roots.add(cert);
    }
    Ok(ClientConfig::builder_with_provider(provider())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|e| format!("TLS: {e}"))?
        .with_root_certificates(roots)
        .with_no_client_auth())
}

fn rustls_native_certs() -> Option<Vec<CertificateDer<'static>>> {
    // hyper-rustls' native-tokio feature brings rustls-native-certs.
    Some(rustls_native_certs::load_native_certs().certs)
}

/// An internal peer: loopback, a private or link-local address, a single-label
/// name (a compose service), or a name under `.localhost`, `.internal` or
/// `.local`. Internal links trust the lane CA alone and present this instance's
/// certificate; every other host is an external party.
pub fn is_internal_host(raw: &str) -> bool {
    let h = raw
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_ascii_lowercase();
    if h.is_empty() {
        return false;
    }
    if h == "localhost"
        || h.ends_with(".localhost")
        || h.ends_with(".internal")
        || h.ends_with(".local")
    {
        return true;
    }
    match h.parse::<IpAddr>() {
        Ok(IpAddr::V4(a)) => {
            let [a0, a1, ..] = a.octets();
            a.is_loopback()
                || a0 == 10
                || (a0 == 172 && (16..=31).contains(&a1))
                || (a0 == 192 && a1 == 168)
                || (a0 == 169 && a1 == 254)
        }
        Ok(IpAddr::V6(a)) => {
            let first = a.segments()[0];
            a.is_loopback() || (first & 0xfe00) == 0xfc00 || (first & 0xffc0) == 0xfe80
        }
        Err(_) => !h.contains('.'),
    }
}

/// Did this connection's client present a certificate the lane CA verified?
#[derive(Clone, Copy, Debug)]
pub struct PeerVerified(pub bool);

/// HEALTHKIT_CLIENT_CERTIFICATE_REQUIRED (owner, 2026-11-12): the sources that
/// must present a client certificate on the device routes too (PIM's scope
/// pushes), comma-separated. C++, LSP, Scala and the TypeScript PE read the same.
pub fn certificate_sources(raw: Option<&str>) -> Vec<String> {
    raw.unwrap_or("")
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect()
}

/// A device route's source: the query's `bridgeId` (status), else the body's
/// `source`, else its `bridgeId` (scope, ingest), else the configured bridge.
fn device_source(query: Option<&str>, body: &[u8]) -> String {
    let named = |v: Option<&serde_json::Value>| {
        v.and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(String::from)
    };
    let from_query = query.and_then(|q| {
        q.split('&')
            .filter_map(|kv| kv.split_once('='))
            .find(|(k, _)| *k == "bridgeId")
            .map(|(_, v)| v.to_string())
            .filter(|v| !v.is_empty())
    });
    let json: serde_json::Value = serde_json::from_slice(body).unwrap_or(serde_json::Value::Null);
    from_query
        .or_else(|| named(json.get("source")))
        .or_else(|| named(json.get("bridgeId")))
        .unwrap_or_else(|| {
            std::env::var("HEALTHKIT_BRIDGE_ID").unwrap_or_else(|_| "healthkit-ios-bridge".into())
        })
}

fn forbidden() -> Response {
    Response::builder()
        .status(StatusCode::FORBIDDEN)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(CLIENT_CERTIFICATE_REQUIRED))
        .expect("a static response")
}

/// For a port with device routes: 403 without a verified certificate, except
/// on [`PE_DEVICE_ROUTES`] for a source that need not present one.
pub async fn require_client_certificate(request: Request, next: Next) -> Response {
    let verified = request
        .extensions()
        .get::<PeerVerified>()
        .is_some_and(|p| p.0);
    if verified {
        return next.run(request).await;
    }
    if !PE_DEVICE_ROUTES.contains(&request.uri().path()) {
        return forbidden();
    }
    let required = certificate_sources(
        std::env::var("HEALTHKIT_CLIENT_CERTIFICATE_REQUIRED")
            .ok()
            .as_deref(),
    );
    if required.is_empty() {
        return next.run(request).await;
    }
    // Read the body for its source, then hand the request on whole.
    let (parts, body) = request.into_parts();
    let Ok(bytes) = axum::body::to_bytes(body, 16 * 1024 * 1024).await else {
        return forbidden();
    };
    let source = device_source(parts.uri.query(), &bytes);
    if required.contains(&source) {
        return forbidden();
    }
    next.run(Request::from_parts(parts, Body::from(bytes)))
        .await
}

pub fn startup_line(port: u16, device_routes: bool) -> String {
    let req = if device_routes {
        "client certificate required except device routes"
    } else {
        "client certificate required"
    };
    format!(
        "listening on {port} (TLS 1.3, {req}, groups {GROUPS} - this build's ring provider has no X25519MLKEM768)"
    )
}

/// Serve `router` over TLS on `listener`: no plaintext listener beside it. A
/// refused handshake (no or untrusted certificate, TLS 1.2, plaintext) closes
/// that connection only.
pub async fn serve(
    listener: tokio::net::TcpListener,
    router: Router,
    config: Arc<ServerConfig>,
    device_routes: bool,
) -> std::io::Result<()> {
    let router = if device_routes {
        router.layer(axum::middleware::from_fn(require_client_certificate))
    } else {
        router
    };
    let acceptor = tokio_rustls::TlsAcceptor::from(config);
    loop {
        let (tcp, _) = listener.accept().await?;
        let acceptor = acceptor.clone();
        let router = router.clone();
        tokio::spawn(async move {
            let Ok(Ok(tls)) =
                tokio::time::timeout(std::time::Duration::from_secs(10), acceptor.accept(tcp))
                    .await
            else {
                return;
            };
            let verified = tls
                .get_ref()
                .1
                .peer_certificates()
                .is_some_and(|c| !c.is_empty());
            let service = hyper_util::service::TowerToHyperService::new(
                router.layer(axum::Extension(PeerVerified(verified))),
            );
            let _ = hyper::server::conn::http1::Builder::new()
                .serve_connection(hyper_util::rt::TokioIo::new(tls), service)
                .with_upgrades()
                .await;
        });
    }
}

#[cfg(test)]
mod tests {
    //! The checks the voting engines make (C++ `tests/http_tls_tests.cpp`, LSP
    //! `tests/tls-tests.lisp`, Scala `LaneTlsSpec`): a throwaway PKI from the
    //! openssl CLI in a temporary directory, an RE-like and a PE-like server on
    //! loopback, this crate's client, and tokio-rustls clients for refusals.
    use super::*;
    use axum::routing::get;
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn certificate_sources_are_a_trimmed_comma_list() {
        assert_eq!(
            certificate_sources(Some(" pim , other,")),
            vec!["pim", "other"]
        );
        assert!(certificate_sources(None).is_empty());
    }

    #[test]
    fn a_device_requests_source_is_the_query_then_the_body_then_the_bridge() {
        assert_eq!(device_source(Some("bridgeId=phone-2"), b""), "phone-2");
        assert_eq!(
            device_source(None, br#"{"source":"pim","bridgeId":"phone"}"#),
            "pim"
        );
        assert_eq!(
            device_source(None, br#"{"source":"","bridgeId":"phone"}"#),
            "phone"
        );
        assert_eq!(device_source(None, b"not json"), "healthkit-ios-bridge");
    }

    fn openssl() -> String {
        [
            "/opt/homebrew/opt/openssl@3/bin/openssl",
            "/usr/local/opt/openssl@3/bin/openssl",
        ]
        .iter()
        .find(|p| Path::new(p).exists())
        .map_or_else(|| "openssl".to_owned(), |p| (*p).to_owned())
    }

    fn sh(dir: &Path, cmd: &str) {
        let out = Command::new("/bin/sh")
            .arg("-c")
            .arg(cmd)
            .current_dir(dir)
            .output()
            .expect("sh");
        assert!(
            out.status.success(),
            "test PKI: {cmd}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    /// A CA in the profile's shape, one instance leaf (server and client,
    /// localhost and 127.0.0.1), a revoked client and a client of another CA.
    fn make_pki() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "re-rust-tls-{}-{}",
            std::process::id(),
            rand_suffix()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let o = openssl();
        let d = dir.display();
        std::fs::write(
            dir.join("ca.cnf"),
            format!("[ca]\ndefault_ca=d\n[d]\ndir={d}\ndatabase=$dir/index.txt\ncrlnumber=$dir/crlnumber\ndefault_md=sha384\ndefault_crl_days=2\n"),
        )
        .unwrap();
        std::fs::write(dir.join("index.txt"), "").unwrap();
        std::fs::write(dir.join("crlnumber"), "01\n").unwrap();
        std::fs::write(
            dir.join("leaf.ext"),
            "subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth,clientAuth\nkeyUsage=critical,digitalSignature\n",
        )
        .unwrap();
        std::fs::write(
            dir.join("client.ext"),
            "extendedKeyUsage=clientAuth\nkeyUsage=critical,digitalSignature\n",
        )
        .unwrap();
        for ca in ["ca", "rogue-ca"] {
            sh(
                &dir,
                &format!(
                    "{o} req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-384 -nodes -sha384 -days 2 -subj /CN={ca} -addext basicConstraints=critical,CA:TRUE -addext keyUsage=critical,keyCertSign,cRLSign -keyout {ca}.key -out {ca}.crt"
                ),
            );
        }
        for (name, issuer, ext) in [
            ("rust-1", "ca", "leaf.ext"),
            ("revoked", "ca", "client.ext"),
            ("rogue", "rogue-ca", "client.ext"),
        ] {
            sh(
                &dir,
                &format!(
                    "{o} req -newkey ec -pkeyopt ec_paramgen_curve:P-384 -nodes -subj /CN={name} -keyout {name}.key -out {name}.csr && {o} x509 -req -sha384 -days 2 -in {name}.csr -CA {issuer}.crt -CAkey {issuer}.key -CAcreateserial -extfile {ext} -out {name}.crt && cat {name}.crt {issuer}.crt > {name}.chain"
                ),
            );
        }
        sh(
            &dir,
            &format!(
                "{o} ca -config ca.cnf -keyfile ca.key -cert ca.crt -revoke revoked.crt && {o} ca -config ca.cnf -keyfile ca.key -cert ca.crt -gencrl -out ca.crl"
            ),
        );
        dir
    }

    fn rand_suffix() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    }

    fn settings(dir: &Path, name: &str) -> Settings {
        let f = |n: &str| dir.join(n).display().to_string();
        Settings {
            cert: f(&format!("{name}.chain")),
            key: f(&format!("{name}.key")),
            ca: f("ca.crt"),
            crl: Some(f("ca.crl")),
        }
    }

    async fn start(config: Arc<ServerConfig>, device_routes: bool) -> u16 {
        let router = Router::new()
            .route("/health", get(|| async { r#"{"status":"ok"}"# }))
            .route(
                "/api/integrations/healthkit/status",
                get(|| async { r#"{"device":true}"# }),
            )
            .route("/service", get(|| async { r#"{"service":true}"# }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(serve(listener, router, config, device_routes));
        port
    }

    /// One GET over a tokio-rustls client: the status line, or why it failed.
    async fn raw_get(port: u16, path: &str, config: ClientConfig) -> String {
        let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
        let tcp = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .unwrap();
        let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
        let attempt = async {
            let mut tls = connector
                .connect(name, tcp)
                .await
                .map_err(|e| format!("handshake: {e}"))?;
            tls.write_all(
                format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                    .as_bytes(),
            )
            .await
            .map_err(|e| format!("write: {e}"))?;
            let mut out = Vec::new();
            let _ = tls.read_to_end(&mut out).await;
            Ok::<_, String>(String::from_utf8_lossy(&out).into_owned())
        };
        tokio::time::timeout(std::time::Duration::from_secs(5), attempt)
            .await
            .unwrap_or_else(|_| Err("timeout".to_owned()))
            .unwrap_or_else(|e| e)
    }

    fn client_with(
        dir: &Path,
        provider: Arc<CryptoProvider>,
        versions: &[&'static rustls::SupportedProtocolVersion],
        cert: Option<&str>,
    ) -> ClientConfig {
        let mut roots = RootCertStore::empty();
        for c in certificates(&dir.join("ca.crt").display().to_string(), "ca").unwrap() {
            roots.add(c).unwrap();
        }
        let b = ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(versions)
            .unwrap()
            .with_root_certificates(roots);
        match cert {
            Some(n) => {
                let s = settings(dir, n);
                b.with_client_auth_cert(
                    certificates(&s.cert, "c").unwrap(),
                    private_key(&s.key).unwrap(),
                )
                .unwrap()
            }
            None => b.with_no_client_auth(),
        }
    }

    #[test]
    fn the_internal_peer_rule_matches_the_voting_engines() {
        for h in [
            "127.0.0.1",
            "localhost",
            "engine-cpp-re",
            "cpp-1.localhost",
            "host.docker.internal",
            "10.0.0.5",
            "172.16.0.1",
            "172.31.255.1",
            "192.168.1.20",
            "169.254.1.1",
            "::1",
            "fd00::1",
            "fe80::1",
            "printer.local",
        ] {
            assert!(is_internal_host(h), "{h} is internal");
        }
        for h in [
            "api.openai.com",
            "8.8.8.8",
            "172.32.0.1",
            "172.15.0.1",
            "2001:db8::1",
            "",
        ] {
            assert!(!is_internal_host(h), "{h} is external");
        }
    }

    #[tokio::test]
    async fn lane_tls_serves_refuses_and_calls_as_the_contract_says() {
        let dir = make_pki();
        let own = settings(&dir, "rust-1");
        let re = start(server_config(&own, true).unwrap(), false).await;
        let pe = start(server_config(&own, false).unwrap(), true).await;
        let lane = || internal_client_config(&own.ca, &own.crl, Some(&own)).unwrap();
        let tls13 = &[&rustls::version::TLS13];

        // The client: an IP SAN and a DNS SAN, through hyper-rustls as client.rs uses it.
        for host in ["127.0.0.1", "localhost"] {
            let connector = hyper_rustls::HttpsConnectorBuilder::new()
                .with_tls_config(lane())
                .https_only()
                .enable_http1()
                .build();
            let client =
                hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
                    .build::<_, http_body_util::Full<axum::body::Bytes>>(connector);
            let response = client
                .get(format!("https://{host}:{re}/health").parse().unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "client to {host}");
        }

        // An RE-like port.
        assert!(
            raw_get(re, "/health", lane())
                .await
                .contains("HTTP/1.1 200"),
            "a lane client is served"
        );
        assert!(
            !raw_get(re, "/health", client_with(&dir, provider(), tls13, None))
                .await
                .contains("200"),
            "no certificate"
        );
        let tls12 = client_with(
            &dir,
            Arc::new(ring::default_provider()),
            &[&rustls::version::TLS12],
            Some("rust-1"),
        );
        assert!(
            !raw_get(re, "/health", tls12).await.contains("200"),
            "TLS 1.2"
        );
        let mut aes128 = ring::default_provider();
        aes128
            .cipher_suites
            .retain(|s| s.suite() == rustls::CipherSuite::TLS13_AES_128_GCM_SHA256);
        assert!(
            !raw_get(
                re,
                "/health",
                client_with(&dir, Arc::new(aes128), tls13, Some("rust-1"))
            )
            .await
            .contains("200"),
            "a suite outside the profile"
        );
        assert!(
            !raw_get(
                re,
                "/health",
                client_with(&dir, provider(), tls13, Some("rogue"))
            )
            .await
            .contains("200"),
            "another CA"
        );
        assert!(
            !raw_get(
                re,
                "/health",
                client_with(&dir, provider(), tls13, Some("revoked"))
            )
            .await
            .contains("200"),
            "a revoked certificate"
        );

        // A PE-like port: the device routes.
        assert!(
            raw_get(
                pe,
                "/api/integrations/healthkit/status",
                client_with(&dir, provider(), tls13, None)
            )
            .await
            .contains("HTTP/1.1 200"),
            "a device route without a certificate"
        );
        let out = raw_get(pe, "/service", client_with(&dir, provider(), tls13, None)).await;
        assert!(
            out.contains("HTTP/1.1 403") && out.contains(CLIENT_CERTIFICATE_REQUIRED),
            "403 elsewhere: {out}"
        );
        assert!(
            raw_get(pe, "/service", lane())
                .await
                .contains("HTTP/1.1 200"),
            "a lane client elsewhere"
        );
        assert!(
            !raw_get(
                pe,
                "/api/integrations/healthkit/status",
                client_with(&dir, provider(), tls13, Some("rogue"))
            )
            .await
            .contains("200"),
            "a presented untrusted certificate"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}
