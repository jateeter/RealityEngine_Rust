//! The PE's outbound calls: to its Reality Engine and to the integrations,
//! over HTTP/1.1, plain or TLS by the URL's scheme. HTTPS verifies the peer
//! against the system's roots, as C++ verifies against OpenSSL's defaults.

use axum::body::Bytes;
use http_body_util::{BodyExt, Full};
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use std::sync::OnceLock;
use std::time::Duration;

/// Long enough for the full corpus's `GET /api/machines`.
const TIMEOUT: Duration = Duration::from_secs(120);

/// The connector, built once: loading the system's roots is not free. Each
/// call builds its own client over it, so no pooled connection outlives the
/// runtime that opened it. With no roots to verify a peer with, the store is
/// empty: plain HTTP still works, and HTTPS fails at its handshake.
fn connector() -> HttpsConnector<HttpConnector> {
    static CONNECTOR: OnceLock<HttpsConnector<HttpConnector>> = OnceLock::new();
    CONNECTOR
        .get_or_init(|| {
            let builder = hyper_rustls::HttpsConnectorBuilder::new();
            let builder = match builder.with_native_roots() {
                Ok(builder) => builder,
                Err(_) => hyper_rustls::HttpsConnectorBuilder::new().with_tls_config(
                    rustls::ClientConfig::builder()
                        .with_root_certificates(rustls::RootCertStore::empty())
                        .with_no_client_auth(),
                ),
            };
            builder.https_or_http().enable_http1().build()
        })
        .clone()
}

/// Send `request`: the status and the body, or why nothing answered.
async fn send(
    request: axum::http::Request<Full<Bytes>>,
) -> Result<(axum::http::StatusCode, Bytes), String> {
    let what = format!("{} {}", request.method(), request.uri());
    let client = Client::builder(TokioExecutor::new()).build::<_, Full<Bytes>>(connector());
    let response = tokio::time::timeout(TIMEOUT, client.request(request))
        .await
        .map_err(|_| format!("{what}: timed out"))?
        .map_err(|error| format!("{what}: {error}"))?;
    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .map_err(|error| format!("{what}: {error}"))?
        .to_bytes();
    Ok((status, body))
}

/// `GET url`: the body of a 2xx answer, or why there is none.
pub(crate) async fn get(url: &str) -> Result<Bytes, String> {
    let request = axum::http::Request::get(url)
        .body(Full::default())
        .map_err(|error| format!("GET {url}: {error}"))?;
    let (status, body) = send(request).await?;
    if !status.is_success() {
        return Err(format!("GET {url}: HTTP {status}"));
    }
    Ok(body)
}

/// `POST url` with a JSON body: the body of a 2xx answer, or why there is none.
pub(crate) async fn post_json(url: &str, body: &serde_json::Value) -> Result<Bytes, String> {
    let request = axum::http::Request::post(url)
        .header(axum::http::header::CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from(body.to_string())))
        .map_err(|error| format!("POST {url}: {error}"))?;
    let (status, body) = send(request).await?;
    if !status.is_success() {
        return Err(format!(
            "POST {url}: HTTP {status}: {}",
            String::from_utf8_lossy(&body)
        ));
    }
    Ok(body)
}

/// A request as C++ `http::request_json` makes it: the body whatever the
/// status, failing only when nothing answers. For the integrations, whose
/// services answer their own errors in JSON.
pub(crate) async fn request(
    method: axum::http::Method,
    url: &str,
    headers: &[(&str, String)],
    body: Option<String>,
) -> Result<Bytes, String> {
    let mut builder = axum::http::Request::builder()
        .method(method.clone())
        .uri(url);
    for (name, value) in headers {
        builder = builder.header(*name, value);
    }
    if body.is_some() {
        builder = builder.header(axum::http::header::CONTENT_TYPE, "application/json");
    }
    let request = builder
        .body(Full::new(Bytes::from(body.unwrap_or_default())))
        .map_err(|error| format!("{method} {url}: {error}"))?;
    Ok(send(request).await?.1)
}
