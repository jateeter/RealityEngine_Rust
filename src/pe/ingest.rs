//! Integrations (#20 workstream 4), slice B: the ingest routes
//! `POST /api/integrations/completions`, `/healthkit/ingest` and
//! `/carekit/ingest`.
//!
//! C++ `ingest_completion`, `ingest_healthkit` and `ingest_carekit` are the
//! concrete form this follows; `localHealthkitBridge/docs/INGEST_CONTRACT.md`
//! is HealthKit's contract. Each resolves a source mapping from the
//! integrations configuration, derives a `sensorId` and a signal from it, and
//! sends that signal through `POST /api/signals`' own path, so a value lands,
//! registers and earns activity exactly as a signal does.
//!
//! - **Completions:** an external agent's finished result. The mapping is
//!   `sourceMappingId` (404 when unknown) with `sourceMapping` over it; the
//!   envelope's own region, values and TTL win over the mapping's. The reply
//!   carries the completion's identity and the signal's reply.
//! - **HealthKit:** one sample or `samples[]`, each resolved or unmapped on its
//!   own: 200 when all resolved, 207 when some, 400 when none. A sample's
//!   mapping is `healthkit:<type>:<sourceName>`, else `healthkit:<type>`. With
//!   `HEALTHKIT_BRIDGE_TOKEN` set, the body's token or the bearer must match
//!   it (401).
//! - **CareKit:** one sample or `samples[]` merged over the body, each through
//!   its mapping (`CAREKIT_DEFAULT_SOURCE_MAPPING_ID` by default): 200 when all
//!   succeeded, else 207. With `CAREKIT_BRIDGE_TOKEN` set, the body's token
//!   must match it (401).
//!
//! Each broadcasts its own message on the event streams as C++ does:
//! `agent.completion.received`, `healthkit.ingest`, `carekit.ingest`.

use super::*;
use sources::{id_part, signal_reply};

fn text<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value[key].as_str()
}

/// C++ `merge_objects`: `overlay`'s keys over `base`, shallow.
fn merge(base: Value, overlay: &Value) -> Value {
    let mut base = if base.is_object() { base } else { json!({}) };
    if let Some(overlay) = overlay.as_object() {
        for (key, value) in overlay {
            base[key] = value.clone();
        }
    }
    base
}

fn template(mut id: String, tokens: &[(&str, String)]) -> String {
    for (token, value) in tokens {
        id = id.replace(token, value);
    }
    id
}

/// The `Authorization: Bearer <token>` token, or empty.
fn bearer(headers: &axum::http::HeaderMap) -> String {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .filter(|v| v.len() > 7 && v[..7].eq_ignore_ascii_case("bearer "))
        .map(|v| v[7..].trim_start().to_owned())
        .unwrap_or_default()
}

fn config(pe: &SharedPe) -> Result<Arc<integrations::Integrations>, (StatusCode, Json<Value>)> {
    Ok(Arc::clone(&read(pe)?.integrations))
}

fn body_object(bytes: &Bytes, what: &str) -> Result<Value, (StatusCode, Json<Value>)> {
    let body = body_of(bytes)?;
    if !body.is_object() {
        return Err(error_response(EngineError::bad_request(format!(
            "{what} body must be a JSON object"
        ))));
    }
    Ok(body)
}

fn announce(pe: &SharedPe, message: Value) {
    if let Ok(state) = pe.read() {
        state.broadcast(&message);
    }
}

pub(crate) async fn completion(State(pe): State<SharedPe>, body: Bytes) -> Response {
    let body = match body_object(&body, "completion") {
        Ok(body) => body,
        Err(error) => return error.into_response(),
    };
    let (status, reply) = ingest_completion(&pe, &body).await;
    (status, Json(reply)).into_response()
}

/// C++ `ingest_completion`: the route's body once it is an object, and the
/// path an integration's dispatch commits its result through.
pub(crate) async fn ingest_completion(pe: &SharedPe, body: &Value) -> (StatusCode, Value) {
    let config = match config(pe) {
        Ok(config) => config,
        Err((status, Json(reply))) => return (status, reply),
    };
    let mapping_id = text(body, "sourceMappingId")
        .or_else(|| text(body, "mappingId"))
        .unwrap_or_default()
        .to_owned();
    let mut mapping = json!({});
    if !mapping_id.is_empty() {
        match config.source_mappings.get(&mapping_id) {
            Some(found) => mapping = found.clone(),
            None => {
                let (status, Json(reply)) = error_response(EngineError::not_found(format!(
                    "Unknown sourceMappingId \"{mapping_id}\""
                )));
                return (status, reply);
            }
        }
    }
    let mapping = merge(mapping, &body["sourceMapping"]);
    let provider = text(body, "provider").unwrap_or("external").to_owned();
    let agent = text(body, "agent")
        .or_else(|| text(body, "agentId"))
        .unwrap_or("agent")
        .to_owned();
    let correlation = text(body, "correlationId").unwrap_or_default().to_owned();
    let envelope = text(body, "envelopeId").unwrap_or_default().to_owned();
    let mut sensor_id = text(body, "sensorId")
        .or_else(|| text(&mapping, "sensorId"))
        .unwrap_or_default()
        .to_owned();
    if sensor_id.is_empty()
        && let Some(pattern) = text(&mapping, "sensorIdTemplate")
    {
        sensor_id = template(
            pattern.to_owned(),
            &[
                ("{provider}", id_part(&provider)),
                ("{agent}", id_part(&agent)),
                ("{correlationId}", id_part(&correlation)),
                ("{envelopeId}", id_part(&envelope)),
            ],
        );
    }
    if sensor_id.is_empty() {
        sensor_id = format!("agent.{}.completion", id_part(&agent));
    }
    let mut signal = json!({
        "sensorId": sensor_id,
        "name": text(body, "name").or_else(|| text(&mapping, "name")).map_or_else(
            || format!("agent:{provider}/{agent}/completion"),
            str::to_owned,
        ),
        "values": if body["values"].is_array() { body["values"].clone() } else { mapping["values"].clone() },
        "ttlMs": if body["ttlMs"].is_number() {
            body["ttlMs"].clone()
        } else if mapping["ttlMs"].is_number() {
            mapping["ttlMs"].clone()
        } else {
            json!(300000)
        },
        "triggerPush": body["triggerPush"].as_bool().unwrap_or(false),
        "compactPush": body["compactPush"].as_bool().unwrap_or(true),
        "origin": provider,
    });
    // The envelope's own region wins, then the mapping's (C++ #24).
    if body["region"].is_object() {
        signal["region"] = body["region"].clone();
    } else if mapping["region"].is_object() {
        signal["region"] = mapping["region"].clone();
    }
    let (status, reply) = signal_reply(pe, &signal).await;
    if !status.is_success() {
        return (status, reply);
    }
    let mut completion = json!({
        "provider": provider,
        "agent": agent,
        "sensorId": sensor_id,
        "sourceMappingId": mapping_id,
        "correlationId": correlation,
        "envelopeId": envelope,
        "completionId": text(body, "completionId").or_else(|| text(body, "id")).unwrap_or_default(),
        "receivedAt": now_ms(),
    });
    if body["metadata"].is_object() {
        completion["metadata"] = body["metadata"].clone();
    }
    announce(
        pe,
        json!({
            "type": "agent.completion.received",
            "provider": provider,
            "agent": agent,
            "sensorId": sensor_id,
            "sourceMappingId": mapping_id,
            "correlationId": correlation,
            "envelopeId": envelope,
            "timestamp": now_ms(),
        }),
    );
    (
        StatusCode::OK,
        json!({ "success": true, "completion": completion, "signal": reply }),
    )
}

/// `HKQuantityTypeIdentifierHeartRate` -> `heartrate` (C++
/// `compact_hk_identifier`).
fn compact_hk_identifier(kind: &str) -> String {
    const PREFIXES: [&str; 8] = [
        "HKQuantityTypeIdentifier",
        "HKCategoryTypeIdentifier",
        "HKWorkoutTypeIdentifier",
        "HKCorrelationTypeIdentifier",
        "HKDocumentTypeIdentifier",
        "HKClinicalTypeIdentifier",
        "HKSeriesTypeIdentifier",
        "HKTypeIdentifier",
    ];
    let stripped = PREFIXES
        .iter()
        .find_map(|p| kind.strip_prefix(p))
        .unwrap_or(kind);
    let out: String = stripped
        .bytes()
        .filter(u8::is_ascii_alphanumeric)
        .map(|b| b.to_ascii_lowercase() as char)
        .collect();
    if out.is_empty() {
        "unknown".to_owned()
    } else {
        out
    }
}

/// The mapping's `sensorId`, else its template, else `hk.<type>[.<source>]`.
fn hk_sensor_id(kind: &str, source: &str, mapping: &Value) -> String {
    if let Some(fixed) = text(mapping, "sensorId").filter(|s| !s.is_empty()) {
        return fixed.to_owned();
    }
    if let Some(pattern) = text(mapping, "sensorIdTemplate").filter(|s| !s.is_empty()) {
        return template(
            pattern.to_owned(),
            &[
                ("{type}", id_part(kind)),
                ("{sampleType}", id_part(kind)),
                ("{source}", id_part(source)),
                ("{provider}", "healthkit".to_owned()),
                ("{agent}", id_part(source)),
            ],
        );
    }
    let suffix = if source.is_empty() {
        String::new()
    } else {
        format!(".{}", id_part(source))
    };
    format!("hk.{}{suffix}", compact_hk_identifier(kind))
}

fn unmapped(kind: &str, source: &str, reason: String) -> Value {
    json!({ "unmapped": true, "type": kind, "sourceName": source, "reason": reason })
}

/// C++ `ingest_healthkit_one`: one sample, resolved or unmapped.
async fn healthkit_one(
    pe: &SharedPe,
    config: &integrations::Integrations,
    sample: &Value,
) -> Value {
    let kind = text(sample, "type")
        .or_else(|| text(sample, "sampleType"))
        .unwrap_or_default();
    let source = text(sample, "sourceName").unwrap_or_default();
    if kind.is_empty() {
        return unmapped(kind, source, "sample.type is required".to_owned());
    }
    let values: Vec<f64> = if sample["values"].is_array() {
        machine_routes::numbers(&sample["values"])
    } else {
        sample["value"].as_f64().into_iter().collect()
    };
    if values.is_empty() {
        return unmapped(
            kind,
            source,
            "sample.value must be a finite number".to_owned(),
        );
    }
    let mapping_id = text(sample, "sourceMappingId")
        .or_else(|| text(sample, "mappingId"))
        .unwrap_or_default();
    let mut mapping = if mapping_id.is_empty() {
        let specific = (!source.is_empty())
            .then(|| {
                config
                    .source_mappings
                    .get(&format!("healthkit:{kind}:{source}"))
            })
            .flatten();
        specific
            .or_else(|| config.source_mappings.get(&format!("healthkit:{kind}")))
            .cloned()
            .unwrap_or(Value::Null)
    } else {
        let Some(found) = config.source_mappings.get(mapping_id) else {
            let mut out = unmapped(
                kind,
                source,
                format!("unknown sourceMappingId \"{mapping_id}\""),
            );
            out["sourceMappingId"] = json!(mapping_id);
            return out;
        };
        let mut found = found.clone();
        found["id"] = json!(mapping_id);
        found
    };
    if sample["sourceMapping"].is_object() {
        mapping = merge(mapping, &sample["sourceMapping"]);
    }
    if !mapping.is_object() {
        return unmapped(
            kind,
            source,
            format!("no registry mapping (declare healthkit:{kind}[:<sourceName>])"),
        );
    }
    if !mapping["region"].is_object() {
        return unmapped(
            kind,
            source,
            "mapping is missing region.offset/region.length".to_owned(),
        );
    }
    let sensor_id = hk_sensor_id(kind, source, &mapping);
    let ttl = mapping["ttlMs"].as_f64().map_or(3_600_000, |t| t as i64);
    let name = text(&mapping, "name").map_or_else(|| format!("healthkit:{kind}"), str::to_owned);
    let signal = json!({
        "sensorId": sensor_id,
        "name": name,
        "region": mapping["region"],
        "values": if sample["values"].is_array() { sample["values"].clone() } else { json!([sample["value"]]) },
        "ttlMs": ttl,
        "origin": "healthkit",
    });
    let (status, reply) = signal_reply(pe, &signal).await;
    if !status.is_success() {
        let reason = text(&reply, "error")
            .unwrap_or("ingest_signal failed")
            .to_owned();
        return unmapped(kind, source, reason);
    }
    json!({
        "resolved": true,
        "sensorId": sensor_id,
        "name": name,
        "type": kind,
        "sourceName": source,
        "sourceMappingId": text(&mapping, "id").unwrap_or_default(),
        "region": mapping["region"],
        "values": values,
        "ttlMs": ttl,
        "source": reply["source"],
    })
}

pub(crate) async fn healthkit(
    State(pe): State<SharedPe>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> Response {
    let body = match body_object(&body, "HealthKit ingest") {
        Ok(body) => body,
        Err(error) => return error.into_response(),
    };
    let config = match config(&pe) {
        Ok(config) => config,
        Err(error) => return error.into_response(),
    };
    let token = text(&body, "bridgeToken")
        .or_else(|| text(&body, "token"))
        .unwrap_or_default();
    if !config.healthkit_token.is_empty()
        && token != config.healthkit_token
        && bearer(&headers) != config.healthkit_token
    {
        return error_response(EngineError {
            status: StatusCode::UNAUTHORIZED,
            message: "HealthKit bridge token rejected".to_owned(),
        })
        .into_response();
    }
    let bridge_id = text(&body, "bridgeId")
        .unwrap_or(&config.healthkit_bridge_id)
        .to_owned();
    let samples: Vec<Value> = match body["samples"].as_array() {
        Some(samples) => samples.clone(),
        None => vec![body.clone()],
    };
    let (mut resolved, mut missed) = (Vec::new(), Vec::new());
    // No scope is ever declared here (the scope route is not specified), so
    // every type is in scope.
    for sample in &samples {
        let result = healthkit_one(&pe, &config, sample).await;
        if result["resolved"] == json!(true) {
            resolved.push(result);
        } else {
            missed.push(result);
        }
    }
    let status = if missed.is_empty() {
        StatusCode::OK
    } else if resolved.is_empty() {
        StatusCode::BAD_REQUEST
    } else {
        StatusCode::MULTI_STATUS
    };
    announce(
        &pe,
        json!({
            "type": "healthkit.ingest",
            "bridgeId": bridge_id,
            "samples": resolved.len() + missed.len(),
            "resolved": resolved.len(),
            "unmapped": missed.len(),
            "timestamp": now_ms(),
        }),
    );
    let mut out = json!({ "success": missed.is_empty(), "bridgeId": bridge_id, "resolved": resolved, "unmapped": missed });
    if let Some(resync) = text(&body, "resyncId").filter(|r| !r.is_empty()) {
        out["resyncId"] = json!(resync);
    }
    (status, Json(out)).into_response()
}

/// C++ `ingest_carekit_one`: one sample through its mapping.
async fn carekit_one(
    pe: &SharedPe,
    config: &integrations::Integrations,
    body: &Value,
) -> Result<Value, EngineError> {
    let mapping_id = text(body, "sourceMappingId")
        .or_else(|| text(body, "mappingId"))
        .unwrap_or(&config.carekit_mapping)
        .to_owned();
    let mut mapping = json!({});
    if !mapping_id.is_empty() {
        mapping = config
            .source_mappings
            .get(&mapping_id)
            .cloned()
            .ok_or_else(|| {
                EngineError::bad_request(format!("Unknown sourceMappingId \"{mapping_id}\""))
            })?;
    }
    mapping = merge(mapping, &body["sourceMapping"]);
    if !mapping_id.is_empty() {
        mapping["id"] = json!(mapping_id);
    }
    let bridge_id = text(body, "bridgeId")
        .unwrap_or(&config.carekit_bridge_id)
        .to_owned();
    let sample_type = text(body, "sampleType")
        .or_else(|| text(body, "type"))
        .unwrap_or("task-event")
        .to_owned();
    let mut sensor_id = text(body, "sensorId")
        .or_else(|| text(&mapping, "sensorId"))
        .unwrap_or_default()
        .to_owned();
    if sensor_id.is_empty()
        && let Some(pattern) = text(&mapping, "sensorIdTemplate")
    {
        sensor_id = template(
            pattern.to_owned(),
            &[
                ("{bridgeId}", id_part(&bridge_id)),
                ("{sampleType}", id_part(&sample_type)),
                ("{type}", id_part(&sample_type)),
                (
                    "{taskId}",
                    id_part(text(body, "taskId").unwrap_or(&sample_type)),
                ),
                (
                    "{carePlanId}",
                    id_part(text(body, "carePlanId").unwrap_or("care-plan")),
                ),
            ],
        );
    }
    if sensor_id.is_empty() {
        sensor_id = format!("carekit.{}", id_part(&sample_type));
    }
    let mut signal = json!({
        "sensorId": sensor_id,
        "name": text(body, "name").or_else(|| text(&mapping, "name")).map_or_else(
            || format!("carekit:{sample_type}"),
            str::to_owned,
        ),
        "values": if body["values"].is_array() {
            body["values"].clone()
        } else if body["value"].is_number() {
            json!([body["value"]])
        } else {
            json!([])
        },
        "ttlMs": if body["ttlMs"].is_number() {
            body["ttlMs"].clone()
        } else if mapping["ttlMs"].is_number() {
            mapping["ttlMs"].clone()
        } else {
            json!(300000)
        },
        "triggerPush": body["triggerPush"].as_bool().unwrap_or(false),
        "compactPush": body["compactPush"].as_bool().unwrap_or(true),
        "origin": "carekit",
    });
    if mapping["region"].is_object() {
        signal["region"] = mapping["region"].clone();
    }
    if body["region"].is_object() {
        signal["region"] = body["region"].clone();
    }
    let (status, reply) = signal_reply(pe, &signal).await;
    Ok(json!({
        "success": status.is_success(),
        "status": status.as_u16(),
        "sampleType": text(body, "sampleType").or_else(|| text(body, "type")).unwrap_or_default(),
        "taskId": text(body, "taskId").unwrap_or_default(),
        "carePlanId": text(body, "carePlanId").unwrap_or_default(),
        "sourceMappingId": text(&mapping, "id").unwrap_or_default(),
        "sensorId": sensor_id,
        "result": reply,
    }))
}

pub(crate) async fn carekit(State(pe): State<SharedPe>, body: Bytes) -> Response {
    let body = match body_object(&body, "CareKit ingest") {
        Ok(body) => body,
        Err(error) => return error.into_response(),
    };
    let config = match config(&pe) {
        Ok(config) => config,
        Err(error) => return error.into_response(),
    };
    let token = text(&body, "bridgeToken")
        .or_else(|| text(&body, "token"))
        .unwrap_or_default();
    if !config.carekit_token.is_empty() && token != config.carekit_token {
        return error_response(EngineError {
            status: StatusCode::UNAUTHORIZED,
            message: "CareKit bridge token rejected".to_owned(),
        })
        .into_response();
    }
    let samples: Vec<Value> = match body["samples"].as_array() {
        Some(samples) => samples
            .iter()
            .map(|sample| {
                let mut merged = merge(body.clone(), sample);
                if let Some(object) = merged.as_object_mut() {
                    object.remove("samples");
                    object.remove("bridgeToken");
                }
                merged
            })
            .collect(),
        None => vec![body.clone()],
    };
    let mut results = Vec::new();
    let mut all_ok = true;
    for sample in &samples {
        match carekit_one(&pe, &config, sample).await {
            Ok(result) => {
                all_ok = all_ok && result["success"] == json!(true);
                results.push(result);
            }
            // A mapping that does not resolve refuses the whole request (C++).
            Err(error) => return error_response(error).into_response(),
        }
    }
    let bridge_id = text(&body, "bridgeId")
        .unwrap_or(&config.carekit_bridge_id)
        .to_owned();
    announce(
        &pe,
        json!({
            "type": "carekit.ingest",
            "bridgeId": bridge_id,
            "samples": results.len(),
            "success": all_ok,
            "timestamp": now_ms(),
        }),
    );
    let status = if all_ok {
        StatusCode::OK
    } else {
        StatusCode::MULTI_STATUS
    };
    (
        status,
        Json(json!({ "success": all_ok, "bridgeId": bridge_id, "results": results })),
    )
        .into_response()
}
