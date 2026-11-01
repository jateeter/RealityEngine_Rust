//! Integrations (#20 workstream 4), slice D: localAI. `GET .../catalog`,
//! `POST .../bootstrap`, `POST .../invoke` and `GET .../ledger` under
//! `/api/integrations/localai`.
//!
//! SURFACE_SPEC.md, "localAI invoke contract" (settled 2026-09-23, 3-of-3), is
//! the contract; C++ `localai_catalog`, `bootstrap_localai`, `invoke_localai`
//! and `localai_ledger` are the concrete form.
//!
//! - **The allow-list is configuration:** `allowedOperations` on the `localai`
//!   integration of `INTEGRATIONS_CONFIG`, verbatim. None configured allows
//!   nothing.
//! - **Invoke** matches `(method, path)` exactly, the query string ignored.
//!   `method` defaults to the allowed operation's own; the body sent is
//!   `payload`, else `body`. A refusal is 403, a target that is missing 400, a
//!   provider failure 502, and every attempt, refused ones included, is
//!   recorded in the ledger under a correlation id (the caller's, or minted).
//!   A record summarises the response (`evidence`), never stores it.
//! - **Bootstrap** registers the three bridge sensors (inactive until a value
//!   arrives) and loads localAIStack's five bridge machines into this PE's RE,
//!   interning each as a test source; one already held, by name, is skipped.
//! - **The ledger** keeps the newest 256 records.

use super::*;
use axum::http::Method;
use std::collections::VecDeque;

const LEDGER: usize = 256;

/// localAIStack's bridge machines (C++ `localai_machine_files`).
const MACHINE_FILES: [&str; 5] = [
    "rag_corrective_cycle.json",
    "session_rag_context.json",
    "session_agent_context.json",
    "ai_load_bridge.json",
    "agent_activity_classifier.json",
];

type Ledger = std::sync::Mutex<VecDeque<Value>>;

pub(crate) fn ledger() -> Arc<Ledger> {
    Arc::default()
}

fn mint(prefix: &str) -> String {
    format!(
        "{prefix}-{}",
        new_machine_id().trim_start_matches("machine-")
    )
}

fn config(pe: &SharedPe) -> Result<Arc<integrations::Integrations>, (StatusCode, Json<Value>)> {
    Ok(Arc::clone(&read(pe)?.integrations))
}

/// `GET url` as JSON, the body whatever the status; the error as `{error}`.
async fn fetch_or_error(url: &str) -> Value {
    match client::request(Method::GET, url, &[], None).await {
        Ok(bytes) => {
            serde_json::from_slice(&bytes).unwrap_or_else(|e| json!({ "error": e.to_string() }))
        }
        Err(error) => json!({ "error": error }),
    }
}

pub(crate) async fn catalog(State(pe): State<SharedPe>) -> Answer {
    let config = config(&pe)?;
    let base = &config.localai_base_url;
    let Json(status) = integrations::localai_status(State(Arc::clone(&pe))).await?;
    let graph_schema = fetch_or_error(&format!("{base}/graph/schema")).await;
    let events = fetch_or_error(&format!("{base}/graphql/events")).await;
    let (allowed, source) = config.localai_allowed_operations();
    read(&pe)?.broadcast_state();
    Ok(Json(json!({
        "success": true,
        "status": status,
        "graphSchema": graph_schema,
        "recentGraphQLEvents": events,
        "invokeEndpoint": "/api/integrations/localai/invoke",
        "allowedEndpoints": allowed,
        "allowedEndpointsSource": source,
        "realityBridge": {
            "sensors": integrations::LOCALAI_SENSORS.iter().map(|s| s.0).collect::<Vec<_>>(),
            "bootstrapEndpoint": "/api/integrations/localai/bootstrap",
            "signalEndpoint": "/api/signals",
        },
    })))
}

pub(crate) async fn ledger_route(State(pe): State<SharedPe>) -> Answer {
    let (base, ledger) = {
        let state = read(&pe)?;
        (
            state.integrations.localai_base_url.clone(),
            Arc::clone(&state.localai_ledger),
        )
    };
    let records: Vec<Value> = ledger
        .lock()
        .map_err(|_| error_response(lock_error()))?
        .iter()
        .cloned()
        .collect();
    Ok(Json(
        json!({ "provider": "localai", "endpoint": base, "records": records }),
    ))
}

pub(crate) async fn invoke(State(pe): State<SharedPe>, body: Bytes) -> Response {
    let body = match body_of(&body) {
        Ok(body) => body,
        Err(error) => return error.into_response(),
    };
    let (config, ledger) = match read(&pe) {
        Ok(state) => (
            Arc::clone(&state.integrations),
            Arc::clone(&state.localai_ledger),
        ),
        Err(error) => return error.into_response(),
    };
    let Some(target) = body["endpoint"].as_str().or_else(|| body["path"].as_str()) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "success": false, "error": "localAI invocation requires endpoint or path" })),
        )
            .into_response();
    };
    let endpoint = if target.starts_with('/') {
        target.to_owned()
    } else {
        format!("/{target}")
    };
    let route_path = endpoint.split('?').next().unwrap_or_default().to_owned();
    let (allowed, _) = config.localai_allowed_operations();
    let operations = allowed.as_array().cloned().unwrap_or_default();
    let method = body["method"]
        .as_str()
        .filter(|m| !m.is_empty())
        .map(str::to_owned)
        .or_else(|| {
            operations
                .iter()
                .find(|op| op["path"].as_str() == Some(&route_path))
                .and_then(|op| op["method"].as_str().map(str::to_owned))
                .filter(|m| !m.is_empty())
        })
        .unwrap_or_else(|| "POST".to_owned())
        .to_uppercase();
    let correlation = body["correlationId"]
        .as_str()
        .map_or_else(|| mint("localai-invocation"), str::to_owned);
    let invocation = mint("localai-inv");
    let started = now_ms();
    // The catalogue's own id for this (method, path): exact, no prefixes.
    let operation_id = operations
        .iter()
        .find(|op| {
            op["method"].as_str().map(str::to_uppercase).as_deref() == Some(method.as_str())
                && op["path"].as_str() == Some(&route_path)
        })
        .and_then(|op| op["id"].as_str())
        .unwrap_or_default()
        .to_owned();
    let record = |success: bool, operation: &str, response: Option<&Value>, error: &str| {
        let mut out = json!({
            "id": invocation,
            "correlationId": correlation,
            "provider": "localai",
            "endpoint": endpoint,
            "method": method,
            "startedAt": started,
            "completedAt": now_ms(),
            "success": success,
        });
        if !operation.is_empty() {
            out["operationId"] = json!(operation);
        }
        for key in ["machineName", "sequenceId", "requestClass", "resultClass"] {
            if let Some(value) = body[key].as_str().filter(|v| !v.is_empty()) {
                out[key] = json!(value);
            }
        }
        if !error.is_empty() {
            out["error"] = json!(error);
        }
        if let Some(response) = response.filter(|r| !r.is_null()) {
            let shape = if response.is_object() {
                "object"
            } else if response.is_array() {
                "array"
            } else {
                "scalar"
            };
            out["evidence"] =
                json!({ "uri": format!("{}{endpoint}", config.localai_base_url), "shape": shape });
            if let Some(confidence) = response["confidence"].as_f64() {
                out["confidence"] = json!(confidence);
            }
        }
        if let Ok(mut ledger) = ledger.lock() {
            ledger.push_back(out);
            while ledger.len() > LEDGER {
                ledger.pop_front();
            }
        }
    };
    let refusal = |status: StatusCode, error: &str| {
        (
            status,
            Json(json!({
                "success": false, "endpoint": endpoint, "method": method,
                "correlationId": correlation, "invocationId": invocation, "error": error,
            })),
        )
            .into_response()
    };
    // C++ `normalize_endpoint`: no `..` or `//` in the path.
    let safe = !route_path.contains("..") && !route_path.contains("//");
    if !safe || operation_id.is_empty() {
        record(false, "", None, "endpoint is not allowed");
        return refusal(StatusCode::FORBIDDEN, "localAI endpoint is not allowed");
    }
    let url = format!("{}{endpoint}", config.localai_base_url);
    let answered = if method == "GET" {
        client::request(Method::GET, &url, &[], None).await
    } else {
        let payload = [&body["payload"], &body["body"]]
            .into_iter()
            .find(|v| !v.is_null())
            .cloned()
            .unwrap_or_else(|| json!({}));
        let method = Method::from_bytes(method.as_bytes()).unwrap_or(Method::POST);
        client::request(method, &url, &[], Some(payload.to_string())).await
    };
    let parsed = answered.and_then(|bytes| {
        serde_json::from_slice::<Value>(&bytes)
            .map_err(|error| format!("{} {url}: {error}", method))
    });
    match parsed {
        Ok(response) => {
            record(true, &operation_id, Some(&response), "");
            Json(json!({
                "success": true, "endpoint": endpoint, "method": method,
                "correlationId": correlation, "invocationId": invocation, "response": response,
            }))
            .into_response()
        }
        Err(error) => {
            record(false, &operation_id, None, &error);
            refusal(StatusCode::BAD_GATEWAY, &error)
        }
    }
}

/// The machine's name from a corpus document, either form (C++
/// `load_machine_from_json_string` reads it so).
fn machine_name(document: &Value) -> Option<String> {
    let machine = if document["machine"].is_object() {
        &document["machine"]
    } else {
        document
    };
    machine["name"].as_str().map(str::to_owned)
}

pub(crate) async fn bootstrap(State(pe): State<SharedPe>) -> Answer {
    let (config, re_url) = {
        let state = read(&pe)?;
        (Arc::clone(&state.integrations), state.re_url.clone())
    };
    let (mut registered, mut skipped_sensors) = (Vec::new(), Vec::new());
    {
        let mut state = write(&pe)?;
        let now = now_ms() as i64;
        for (sensor_id, name, offset, length) in integrations::LOCALAI_SENSORS {
            if state.has_sensor(sensor_id) {
                skipped_sensors.push(json!(sensor_id));
                continue;
            }
            let source = state.add_source(sources::localai_sensor(sensor_id, name, offset, length));
            registered.push(source.to_json(now));
        }
    }
    let mut names: std::collections::BTreeSet<String> =
        match client::get(&format!("{re_url}/api/machines")).await {
            Ok(bytes) => serde_json::from_slice::<Value>(&bytes)
                .ok()
                .and_then(|v| v["machines"].as_array().cloned())
                .into_iter()
                .flatten()
                .filter_map(|m| m["name"].as_str().map(str::to_owned))
                .collect(),
            Err(_) => Default::default(),
        };
    let (mut imported, mut skipped_machines, mut failed) = (Vec::new(), Vec::new(), Vec::new());
    for file in MACHINE_FILES {
        let path = std::path::Path::new(&config.localai_machines_dir).join(file);
        let Ok(raw) = std::fs::read_to_string(&path) else {
            failed.push(json!({ "file": file, "error": "not found" }));
            continue;
        };
        let document: Value = match serde_json::from_str(&raw) {
            Ok(document) => document,
            Err(error) => {
                failed.push(json!({ "file": file, "error": error.to_string() }));
                continue;
            }
        };
        let name = machine_name(&document).unwrap_or_else(|| "unnamed".to_owned());
        if names.contains(&name) {
            skipped_machines.push(json!(name));
            continue;
        }
        let answer = match client::request(
            Method::POST,
            &format!("{re_url}/api/machines"),
            &[],
            Some(raw),
        )
        .await
        {
            Ok(bytes) => serde_json::from_slice::<Value>(&bytes).map_err(|e| e.to_string()),
            Err(error) => Err(error),
        };
        match answer {
            Ok(answer) => {
                let machine = if answer["machine"].is_object() {
                    answer["machine"].clone()
                } else {
                    document.get("machine").cloned().unwrap_or(document)
                };
                write(&pe)?.intern(&machine);
                imported.push(machine);
                names.insert(name);
            }
            Err(error) => failed.push(json!({ "file": file, "error": error })),
        }
    }
    Ok(Json(json!({
        "success": true,
        "registeredSensors": registered,
        "skippedSensors": skipped_sensors,
        "importedMachines": imported,
        "skippedMachines": skipped_machines,
        "failedMachines": failed,
        "localAIBaseUrl": config.localai_base_url,
        "machineDirectory": config.localai_machines_dir,
    })))
}
