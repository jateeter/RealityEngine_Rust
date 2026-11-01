//! The API surface, driven by SURFACE_SPEC.md: every route the specification
//! defines exists in this file's report with a known implementation status.
//!
//! Part of tier R1 (`RealityEngine_CI/docs/RUST_VALIDATION_WORKFLOW.md`). The
//! route list is `conformance/surface/spec-routes.json`, parsed from the spec's
//! route tables by `scripts/refresh-surface.sh` and pinned to the spec's commit.
//! Each route is probed through the HTTP router in process and classified:
//!
//! - `implemented`: the router serves the method and path, answers without a
//!   5xx, and any 4xx carries the `{"error": "..."}` envelope.
//! - `implemented-bad-envelope`: served, but a 4xx without that envelope.
//! - `implemented-error`: served, but the probe got a 5xx.
//! - `declared-unsupported`: declares the participation state `unsupported` in
//!   the shape SURFACE_SPEC.md rules ("An unimplemented route answers
//!   `unsupported`"): `501` with `error`, `participation: "unsupported"` and
//!   `surface` naming this route as the spec writes it. A declaration in any
//!   other shape is `implemented-bad-envelope`.
//! - `missing`: nothing answers. The router's own 404/405 with an empty body,
//!   or no service at all (this repo has no Perception Engine). The spec calls
//!   this silence, and it is the state every route must leave.
//!
//! The probe sends no meaningful input: `{}` to a route with a body, `probe`
//! for each path parameter. It asks whether the route exists, not whether it
//! behaves; behaviour is R1's corpus replay and R2.
//!
//! The record is `conformance/surface-status.json`, one entry per route with
//! its status and, for anything not `implemented`, the issue tracking it.
//! The test fails when an observed status differs from the record, a spec route
//! has no entry, an entry names no spec route, a non-implemented route names no
//! issue, or `conformance/SURFACE_STATUS.md` is not what the record generates.
//! `SURFACE_RECORD=1` rewrites both files from the observation, keeping each
//! entry's tracking and notes; new entries get `UNCLASSIFIED`, which fails
//! until each names its issue.

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use reality_engine::pe::{PerceptionEngine, pe_app};
use reality_engine::{Engine, app};
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::time::Duration;
use tower::ServiceExt;

const UNCLASSIFIED: &str = "UNCLASSIFIED";
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

fn conformance() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("conformance")
}

fn read_json(path: &PathBuf) -> Value {
    let text = fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("parse {}: {e}", path.display()))
}

/// The router a route's layer is served by, or `None` when this repo has no
/// such service.
fn router_for(layer: &str) -> Option<axum::Router> {
    match layer {
        "RE" | "RE-phase2" => Some(app(Engine::new(7680))),
        "PE" => Some(pe_app(PerceptionEngine::new(7680))),
        _ => None,
    }
}

fn concrete_path(template: &str) -> String {
    template
        .split('/')
        .map(|segment| {
            if segment.starts_with(':') {
                "probe"
            } else {
                segment
            }
        })
        .collect::<Vec<_>>()
        .join("/")
}

struct Observation {
    status: &'static str,
    detail: String,
}

async fn probe(route: &Value) -> Observation {
    let layer = route["layer"].as_str().unwrap_or_default();
    let Some(router) = router_for(layer) else {
        return Observation {
            status: "missing",
            detail: "no Perception Engine in this repo".to_owned(),
        };
    };
    probe_router(router, route).await
}

async fn probe_router(router: axum::Router, route: &Value) -> Observation {
    let method = route["method"].as_str().unwrap_or_default();
    let path = concrete_path(route["path"].as_str().unwrap_or_default());

    let mut request = Request::builder().uri(&path);
    let mut body = Body::empty();
    match method {
        "SSE" => {
            request = request
                .method("GET")
                .header(header::ACCEPT, "text/event-stream")
        }
        "WS" => {
            request = request
                .method("GET")
                .header(header::CONNECTION, "upgrade")
                .header(header::UPGRADE, "websocket")
                .header("sec-websocket-version", "13")
                .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        }
        "POST" | "PUT" | "PATCH" => {
            request = request
                .method(method)
                .header(header::CONTENT_TYPE, "application/json");
            body = Body::from("{}");
        }
        _ => request = request.method(method),
    }
    let request = request.body(body).expect("request");

    let response = match tokio::time::timeout(PROBE_TIMEOUT, router.oneshot(request)).await {
        Ok(Ok(response)) => response,
        Ok(Err(error)) => {
            return Observation {
                status: "implemented-error",
                detail: format!("router error: {error}"),
            };
        }
        Err(_) => {
            return Observation {
                status: "implemented-error",
                detail: format!("no response head within {PROBE_TIMEOUT:?}"),
            };
        }
    };
    let status = response.status();
    let streaming = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("text/event-stream"));
    if streaming || status == StatusCode::SWITCHING_PROTOCOLS {
        return Observation {
            status: "implemented",
            detail: format!("{status}, stream opened"),
        };
    }
    let bytes =
        match tokio::time::timeout(PROBE_TIMEOUT, to_bytes(response.into_body(), 1 << 20)).await {
            Ok(Ok(bytes)) => bytes,
            _ => {
                return Observation {
                    status: "implemented-error",
                    detail: format!("{status}, body unreadable"),
                };
            }
        };
    let parsed: Option<Value> = serde_json::from_slice(&bytes).ok();

    // The router's own answer for a path or method it does not serve.
    if bytes.is_empty()
        && matches!(
            status,
            StatusCode::NOT_FOUND | StatusCode::METHOD_NOT_ALLOWED
        )
    {
        return Observation {
            status: "missing",
            detail: format!("{status}, empty body"),
        };
    }
    let surface = format!("{method} {}", route["path"].as_str().unwrap_or_default());
    let declared = status == StatusCode::NOT_IMPLEMENTED
        && parsed.as_ref().is_some_and(|v| {
            v.get("participation").and_then(Value::as_str) == Some("unsupported")
                && v.get("surface").and_then(Value::as_str) == Some(surface.as_str())
                && v.get("error").and_then(Value::as_str).is_some()
        });
    if declared {
        return Observation {
            status: "declared-unsupported",
            detail: format!("{status}, participation state unsupported"),
        };
    }
    if parsed
        .as_ref()
        .and_then(|v| v.get("participation"))
        .and_then(Value::as_str)
        == Some("unsupported")
    {
        return Observation {
            status: "implemented-bad-envelope",
            detail: format!("{status} declares unsupported outside the ruled 501 shape"),
        };
    }
    if status.is_server_error() {
        return Observation {
            status: "implemented-error",
            detail: format!("{status} on the probe"),
        };
    }
    if status.is_client_error()
        && parsed
            .as_ref()
            .and_then(|v| v.get("error"))
            .and_then(Value::as_str)
            .is_none()
    {
        return Observation {
            status: "implemented-bad-envelope",
            detail: format!("{status} without {{\"error\": ...}}"),
        };
    }
    Observation {
        status: "implemented",
        detail: status.to_string(),
    }
}

fn render_markdown(spec: &Value, routes: &[Value], record: &Map<String, Value>) -> String {
    let mut by_layer: BTreeMap<&str, (usize, usize)> = BTreeMap::new();
    for route in routes {
        let layer = route["layer"].as_str().unwrap_or_default();
        let id = route["id"].as_str().unwrap_or_default();
        let entry = by_layer.entry(layer).or_default();
        entry.1 += 1;
        if record.get(id).and_then(|e| e["status"].as_str()) == Some("implemented") {
            entry.0 += 1;
        }
    }
    let mut out = String::new();
    out.push_str("# RealityEngine_Rust API surface status\n\n");
    out.push_str(
        "Generated by `tests/surface.rs` from `conformance/surface-status.json`; \
         regenerate with `SURFACE_RECORD=1 cargo test --test surface`, never by hand. \
         Every route the specification defines is listed with its implementation status \
         in this repo.\n\n",
    );
    let source = &spec["source"];
    out.push_str(&format!(
        "Specification: `{}` `{}` at `{}`.\n\n",
        source["repository"].as_str().unwrap_or_default(),
        source["path"].as_str().unwrap_or_default(),
        &source["commit"].as_str().unwrap_or_default()
            [..12.min(source["commit"].as_str().unwrap_or_default().len())],
    ));
    out.push_str("| layer | implemented | routes |\n|---|---|---|\n");
    for layer in ["RE", "PE", "RE-phase2"] {
        if let Some((implemented, total)) = by_layer.get(layer) {
            out.push_str(&format!("| {layer} | {implemented} | {total} |\n"));
        }
    }
    out.push_str(
        "\n`RE` and `PE` are the specification's implemented-contract tables; \
         `RE-phase2` is the K-line snapshot routes it specifies outside them. \
         A route this engine does not implement is `declared-unsupported`: \
         it answers `501` with the participation state, as the specification \
         rules (\"An unimplemented route answers `unsupported`\"). A `missing` \
         route answers silently, which the specification forbids. The RE's \
         routes are probed on `app`, the PE's on `pe_app`. Status says a route \
         exists, not that it conforms: the notes name known shape defects.\n",
    );
    let mut current = (String::new(), String::new());
    for route in routes {
        let layer = route["layer"].as_str().unwrap_or_default().to_owned();
        let group = route["group"].as_str().unwrap_or_default().to_owned();
        if (layer.clone(), group.clone()) != current {
            out.push_str(&format!(
                "\n## {layer}: {group}\n\n| method | path | status | tracking | notes |\n|---|---|---|---|---|\n"
            ));
            current = (layer, group);
        }
        let id = route["id"].as_str().unwrap_or_default();
        let entry = record.get(id).cloned().unwrap_or(Value::Null);
        out.push_str(&format!(
            "| {} | `{}` | {} | {} | {} |\n",
            route["method"].as_str().unwrap_or_default(),
            route["path"].as_str().unwrap_or_default(),
            entry["status"].as_str().unwrap_or("**no entry**"),
            entry["tracking"].as_str().unwrap_or(""),
            entry["notes"].as_str().unwrap_or(""),
        ));
    }
    out
}

#[tokio::test]
async fn surface_status_is_known_for_every_specified_route() {
    let spec = read_json(&conformance().join("surface").join("spec-routes.json"));
    let routes = spec["routes"]
        .as_array()
        .expect("spec-routes.json lists routes")
        .clone();
    assert!(!routes.is_empty(), "spec-routes.json lists no routes");

    let mut observed: BTreeMap<String, Observation> = BTreeMap::new();
    for route in &routes {
        let id = route["id"].as_str().expect("route id").to_owned();
        observed.insert(id, probe(route).await);
    }

    let status_path = conformance().join("surface-status.json");
    let markdown_path = conformance().join("SURFACE_STATUS.md");
    let previous: Map<String, Value> = if status_path.exists() {
        read_json(&status_path)["routes"]
            .as_object()
            .cloned()
            .unwrap_or_default()
    } else {
        Map::new()
    };

    if std::env::var("SURFACE_RECORD").as_deref() == Ok("1") {
        let mut record = Map::new();
        for (id, observation) in &observed {
            let old = previous.get(id);
            let mut entry = Map::new();
            entry.insert("status".to_owned(), json!(observation.status));
            if observation.status != "implemented" {
                let tracking = old
                    .and_then(|e| e["tracking"].as_str())
                    .unwrap_or(UNCLASSIFIED);
                entry.insert("tracking".to_owned(), json!(tracking));
            }
            if let Some(notes) = old.and_then(|e| e["notes"].as_str()) {
                entry.insert("notes".to_owned(), json!(notes));
            }
            record.insert(id.clone(), Value::Object(entry));
        }
        let body = json!({
            "schema": 1,
            "purpose": "Implementation status of every SURFACE_SPEC route in this repo. Rewrite with SURFACE_RECORD=1 cargo test --test surface; then give every UNCLASSIFIED entry its issue.",
            "spec": spec["source"],
            "routes": record,
        });
        fs::write(
            &status_path,
            serde_json::to_string_pretty(&body).unwrap() + "\n",
        )
        .expect("write status");
        fs::write(&markdown_path, render_markdown(&spec, &routes, &record))
            .expect("write markdown");
    }

    let record: Map<String, Value> = read_json(&status_path)["routes"]
        .as_object()
        .cloned()
        .unwrap_or_default();
    let mut problems = Vec::new();
    let mut tally: BTreeMap<(String, &str), usize> = BTreeMap::new();
    for route in &routes {
        let id = route["id"].as_str().unwrap_or_default();
        let layer = route["layer"].as_str().unwrap_or_default().to_owned();
        let observation = &observed[id];
        *tally.entry((layer, observation.status)).or_default() += 1;
        match record.get(id) {
            None => problems.push(format!("{id}: no entry in surface-status.json")),
            Some(entry) => {
                let recorded = entry["status"].as_str().unwrap_or_default();
                if recorded != observation.status {
                    problems.push(format!(
                        "{id}: recorded {recorded}, observed {} ({}); re-record",
                        observation.status, observation.detail
                    ));
                }
                if observation.status != "implemented"
                    && entry["tracking"].as_str().is_none_or(|t| t == UNCLASSIFIED)
                {
                    problems.push(format!(
                        "{id}: {} names no tracking issue",
                        observation.status
                    ));
                }
            }
        }
    }
    for id in record.keys() {
        if !observed.contains_key(id) {
            problems.push(format!("{id}: in surface-status.json but not in the spec"));
        }
    }
    let expected_markdown = render_markdown(&spec, &routes, &record);
    if fs::read_to_string(&markdown_path).ok().as_deref() != Some(expected_markdown.as_str()) {
        problems.push(
            "SURFACE_STATUS.md is not what surface-status.json generates; re-record".to_owned(),
        );
    }

    let report: BTreeMap<_, _> = observed
        .iter()
        .map(|(id, o)| {
            (
                id.clone(),
                json!({ "status": o.status, "detail": o.detail }),
            )
        })
        .collect();
    let report_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("conformance");
    fs::create_dir_all(&report_dir).expect("report dir");
    fs::write(
        report_dir.join("surface-report.json"),
        serde_json::to_string_pretty(&json!({ "spec": spec["source"], "routes": report })).unwrap(),
    )
    .expect("write report");

    let mut line = String::from("surface:");
    for ((layer, status), count) in &tally {
        line.push_str(&format!(" {layer} {status} {count};"));
    }
    println!("{line} report: target/conformance/surface-report.json");
    for problem in &problems {
        println!("  {problem}");
    }
    assert!(
        problems.is_empty(),
        "surface: {} problem(s)",
        problems.len()
    );
}

/// The classifier itself, against a router built to answer each way: the
/// states Rust's own router cannot show yet must still be told apart.
#[tokio::test]
async fn classifier_distinguishes_every_state() {
    use axum::Json;
    use axum::response::IntoResponse;
    use axum::routing::get;

    let router = axum::Router::new()
        .route(
            "/ok",
            get(|| async { Json(json!({ "status": "healthy" })) }),
        )
        .route(
            "/items/{id}",
            get(|| async {
                (
                    StatusCode::NOT_FOUND,
                    Json(json!({ "error": "no such item" })),
                )
            }),
        )
        .route(
            "/bad",
            get(|| async { (StatusCode::BAD_REQUEST, "plain text") }),
        )
        .route("/boom", get(|| async { StatusCode::INTERNAL_SERVER_ERROR }))
        .route(
            "/declared",
            get(|| async {
                (
                    StatusCode::NOT_IMPLEMENTED,
                    Json(json!({
                        "error": "GET /declared is not implemented by this runtime",
                        "participation": "unsupported",
                        "surface": "GET /declared",
                    })),
                )
            }),
        )
        .route(
            "/misdeclared",
            get(|| async {
                (
                    StatusCode::NOT_IMPLEMENTED,
                    Json(json!({ "error": "no", "participation": "unsupported", "surface": "GET /elsewhere" })),
                )
            }),
        )
        .route(
            "/declared-as-404",
            get(|| async {
                (
                    StatusCode::NOT_FOUND,
                    Json(json!({ "error": "no", "participation": "unsupported", "surface": "GET /declared-as-404" })),
                )
            }),
        )
        .route(
            "/stream",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "text/event-stream")],
                    "data: {}\n\n",
                )
                    .into_response()
            }),
        );

    let cases = [
        ("GET", "/ok", "implemented"),
        ("GET", "/items/:id", "implemented"),
        ("GET", "/bad", "implemented-bad-envelope"),
        ("GET", "/boom", "implemented-error"),
        ("GET", "/declared", "declared-unsupported"),
        ("GET", "/misdeclared", "implemented-bad-envelope"),
        ("GET", "/declared-as-404", "implemented-bad-envelope"),
        ("SSE", "/stream", "implemented"),
        ("GET", "/nothing", "missing"),
        ("POST", "/ok", "missing"),
    ];
    for (method, path, expected) in cases {
        let route = json!({ "layer": "RE", "method": method, "path": path });
        let observation = probe_router(router.clone(), &route).await;
        assert_eq!(
            observation.status, expected,
            "{method} {path}: {}",
            observation.detail
        );
    }
}
