//! The PE's calls to its Reality Engine, over plain HTTP/1.1.

use axum::body::Bytes;
use axum::http::Uri;
use http_body_util::{BodyExt, Empty};
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use std::time::Duration;

/// Long enough for the full corpus's `GET /api/machines`.
const TIMEOUT: Duration = Duration::from_secs(120);

/// `GET url`: the body of a 2xx answer, or why there is none.
pub(crate) async fn get(url: &str) -> Result<Bytes, String> {
    let uri: Uri = url.parse().map_err(|error| format!("{url}: {error}"))?;
    let client = Client::builder(TokioExecutor::new()).build_http::<Empty<Bytes>>();
    let response = tokio::time::timeout(TIMEOUT, client.get(uri))
        .await
        .map_err(|_| format!("GET {url}: timed out"))?
        .map_err(|error| format!("GET {url}: {error}"))?;
    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .map_err(|error| format!("GET {url}: {error}"))?
        .to_bytes();
    if !status.is_success() {
        return Err(format!("GET {url}: HTTP {status}"));
    }
    Ok(body)
}
