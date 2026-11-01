//! The rest of the RE surface (#13, slice 4): the sampler, the perception
//! routes, `GET /api/governance/route` and the demos.
//!
//! C++'s routes in `reality_engine_server.cpp` are the concrete form this
//! follows.
//!
//! - The sampler is bookkeeping: `start` and `stop` set its state, `sample`
//!   counts a sample and echoes its `data`, and nothing samples on its own. An
//!   engine reset leaves it as it is.
//! - `perception/observe` echoes `data` with no transformations;
//!   `perception/diagnostic` resolves a universal input space into each mapped
//!   machine's input, in id order.
//! - `governance/route` is the paging contract resolver the step path already
//!   uses, asked directly.
//! - A demo route answers the resident machine of that name, or 404.

use super::*;
use axum::extract::Query;
use std::collections::HashMap;

/// The sampler's state (C++ `sampler_stats`).
pub(crate) struct Sampler {
    running: bool,
    strategy: String,
    interval_ms: i64,
    sample_count: u64,
}

impl Default for Sampler {
    fn default() -> Self {
        Self {
            running: false,
            strategy: "MANUAL".to_owned(),
            interval_ms: 0,
            sample_count: 0,
        }
    }
}

impl Sampler {
    fn stats(&self) -> Value {
        json!({
            "isRunning": self.running,
            "sampleCount": self.sample_count,
            "bufferSize": 0,
            "strategy": self.strategy,
            "intervalMs": self.interval_ms,
        })
    }
}

type Answer = Result<Json<Value>, (StatusCode, Json<Value>)>;

fn body_of(bytes: &Bytes) -> Result<Value, (StatusCode, Json<Value>)> {
    if bytes.is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_slice(bytes).map_err(|error| {
        error_response(EngineError::bad_request(format!(
            "invalid JSON body: {error}"
        )))
    })
}

fn write(
    engine: &SharedEngine,
) -> Result<std::sync::RwLockWriteGuard<'_, Engine>, (StatusCode, Json<Value>)> {
    engine.write().map_err(|_| error_response(lock_error()))
}

fn read(
    engine: &SharedEngine,
) -> Result<std::sync::RwLockReadGuard<'_, Engine>, (StatusCode, Json<Value>)> {
    engine.read().map_err(|_| error_response(lock_error()))
}

pub(crate) async fn sampler_start(State(engine): State<SharedEngine>, body: Bytes) -> Answer {
    let body = body_of(&body)?;
    let mut state = write(&engine)?;
    let sampler = &mut state.sampler;
    sampler.running = true;
    sampler.strategy = body["strategy"].as_str().unwrap_or("manual").to_owned();
    sampler.interval_ms = body["intervalMs"].as_f64().unwrap_or(0.0) as i64;
    Ok(Json(json!({ "success": true, "stats": sampler.stats() })))
}

pub(crate) async fn sampler_stop(State(engine): State<SharedEngine>) -> Answer {
    write(&engine)?.sampler.running = false;
    Ok(Json(json!({ "success": true })))
}

pub(crate) async fn sampler_sample(State(engine): State<SharedEngine>, body: Bytes) -> Answer {
    let body = body_of(&body)?;
    write(&engine)?.sampler.sample_count += 1;
    Ok(Json(json!({
        "success": true,
        "result": {
            "inputEvent": machine_routes::numbers(&body["data"]),
            "processingTimestamp": now_ms(),
        },
    })))
}

pub(crate) async fn sampler_stats(State(engine): State<SharedEngine>) -> Answer {
    Ok(Json(json!({ "stats": read(&engine)?.sampler.stats() })))
}

pub(crate) async fn observe(body: Bytes) -> Answer {
    let body = body_of(&body)?;
    Ok(Json(json!({
        "success": true,
        "inputEvent": machine_routes::numbers(&body["data"]),
        "transformations": [],
        "processingTimestamp": now_ms(),
    })))
}

impl Engine {
    /// C++ `PerceptionMapper::diagnostic_mapping`: the space is the engine's
    /// dimension, widened to the posted vector; a region past it is an error.
    fn diagnostic(&self, universal: &[f64]) -> Result<Value, EngineError> {
        let dimension = self.dimension.max(universal.len());
        let non_zero: Vec<Value> = universal
            .iter()
            .enumerate()
            .filter(|(_, v)| **v != 0.0)
            .map(|(index, value)| json!({ "index": index, "value": value }))
            .collect();
        let mut mappings = Vec::new();
        for machine in self.machines.values() {
            let Some((offset, length)) = machine.input_region() else {
                continue;
            };
            if offset + length > dimension {
                return Err(EngineError::internal(
                    "Input mapping outside perceptual space",
                ));
            }
            let resolved: Vec<f64> = (offset..offset + length)
                .map(|i| universal.get(i).copied().unwrap_or(0.0))
                .collect();
            mappings.push(json!({
                "machineId": machine.id,
                "machineName": machine.name,
                "inputMapping": { "offset": offset, "length": length },
                "resolvedInput": resolved,
            }));
        }
        Ok(json!({
            "universalSpace": { "dimension": dimension, "nonZeroValues": non_zero },
            "machineMappings": mappings,
        }))
    }

    /// C++ `demo_machine_response`: the first resident machine (in id order)
    /// named `name`, with its metadata summarised.
    fn demo(&self, name: &str, file: &str) -> Result<Value, EngineError> {
        let machine = self
            .machines
            .values()
            .find(|m| m.name == name)
            .ok_or_else(|| {
                EngineError::not_found(format!(
                    "{name} machine not found. Please ensure {file} is loaded."
                ))
            })?;
        let sequences = machine.sequences_in_order();
        let mut metadata = machine.metadata();
        let input_vectors = metadata["inputSequences"]
            .as_array()
            .and_then(|sequences| sequences.first())
            .and_then(|first| first["events"].as_array())
            .map_or(0, Vec::len);
        metadata["name"] = json!(machine.name);
        metadata["description"] = json!(machine.description);
        metadata["machineId"] = json!(machine.id);
        metadata["totalSequences"] = json!(sequences.len());
        metadata["sequenceNames"] = json!(sequences.iter().map(|s| &s.name).collect::<Vec<_>>());
        metadata["totalInputVectors"] = json!(input_vectors);
        Ok(json!({
            "success": true,
            "machine": machine.to_json(true),
            "metadata": metadata,
            "sequencesLoaded": sequences.len(),
            "inputVectorsLoaded": input_vectors,
        }))
    }
}

pub(crate) async fn diagnostic(State(engine): State<SharedEngine>, body: Bytes) -> Answer {
    let body = body_of(&body)?;
    let universal = machine_routes::numbers(&body["universalInputSpace"]);
    read(&engine)?
        .diagnostic(&universal)
        .map(Json)
        .map_err(error_response)
}

/// `strtod`'s reading of a token: leading whitespace skipped, then the longest
/// prefix that is a number; `None` when there is none (C++ `std::stod`).
pub(crate) fn leading_number(token: &str) -> Option<f64> {
    let token = token.trim_start();
    (1..=token.len())
        .rev()
        .filter(|end| token.is_char_boundary(*end))
        .find_map(|end| token[..end].parse::<f64>().ok())
}

pub(crate) async fn governance_route(
    State(engine): State<SharedEngine>,
    Query(query): Query<HashMap<String, String>>,
) -> Answer {
    let (Some(machine_id), Some(sequence_id), Some(raw)) = (
        query.get("machineId"),
        query.get("sequenceId"),
        query.get("values"),
    ) else {
        return Err(error_response(EngineError::bad_request(
            "machineId, sequenceId, and values query parameters are required",
        )));
    };
    // `getline` on ',': a trailing separator ends the list, it opens no token.
    let values: Vec<f64> = raw
        .strip_suffix(',')
        .unwrap_or(raw)
        .split(',')
        .filter(|_| !raw.is_empty())
        .filter_map(leading_number)
        .collect();
    let state = read(&engine)?;
    let machine = state.machines.get(machine_id).ok_or_else(|| {
        error_response(EngineError::not_found(format!(
            "Machine not found: {machine_id}"
        )))
    })?;
    let decision = step::resolve_governance(machine, sequence_id, &values).ok_or_else(|| {
        error_response(EngineError::not_found(format!(
            "No triggerConfig rule matches (sequenceId={sequence_id}, values={raw})"
        )))
    })?;
    Ok(Json(json!({ "success": true, "decision": decision })))
}

pub(crate) async fn demo_multi_step(State(engine): State<SharedEngine>) -> Answer {
    read(&engine)?
        .demo("Multi-Step State Machine", "MultiStep.json")
        .map(Json)
        .map_err(error_response)
}

pub(crate) async fn demo_data_center(State(engine): State<SharedEngine>) -> Answer {
    read(&engine)?
        .demo("Data Center Monitoring", "DataCenterMonitoring.json")
        .map(Json)
        .map_err(error_response)
}

pub(crate) async fn demo_kleene_star(State(engine): State<SharedEngine>) -> Answer {
    read(&engine)?
        .demo("Kleene Star Operator", "KleeneStar.json")
        .map(Json)
        .map_err(error_response)
}
