//! The engine's own sequence and vector stores (#13, slice 3): `/api/sequences`
//! and `/api/vectors`.
//!
//! Both are engine-scoped scratch stores, separate from the resident machines:
//! an id posted here exists on this engine and no other, so a 404 says only
//! that this engine does not hold it (RealityEngine_CI#397). C++'s routes and
//! its `sequence_from_json` / `vector_from_json` are the concrete form this
//! follows.
//!
//! - A sequence is validated before it is stored: at least one initial event,
//!   and at least one event with an output. The 400 carries every failure, as
//!   an array.
//! - A stored vector is the posted document, with an `id` minted when it has
//!   none. Search scores each by cosine against `vector`, or the values of
//!   `elements`, in id order, and stops at `limit` results.
//! - `POST /api/sequences/persist` has nothing to persist to and answers success,
//!   as on C++.

use super::*;

/// `<prefix>-<uuid-shaped id>`, as C++ `make_id` mints.
fn mint(prefix: &str) -> String {
    format!(
        "{prefix}-{}",
        new_machine_id().trim_start_matches("machine-")
    )
}

/// C++ `vector_from_json`: a Reality Event from a request body. Its metadata
/// is not read; its matchAlgorithm is the default.
fn event_from_json(body: &Value) -> Result<Event, EngineError> {
    let mut elements = Vec::new();
    for element in body["elements"].as_array().into_iter().flatten() {
        let comparator = match element["comparatorType"].as_str() {
            None => String::new(),
            Some(name) => step::comparator(name)?.to_owned(),
        };
        elements.push(Element {
            value: element["value"].as_f64().unwrap_or(0.0),
            comparator,
            threshold: element["threshold"].as_f64(),
        });
    }
    let initial = body["isInitial"].as_bool().unwrap_or(false);
    let loaded_at = now_ms();
    Ok(Event {
        id: body["id"]
            .as_str()
            .map_or_else(|| mint("vector"), str::to_owned),
        initial,
        active: initial,
        elements,
        outputs: body["outputEvents"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|output| Output {
                id: output["id"]
                    .as_str()
                    .map_or_else(|| mint("output"), str::to_owned),
                vector: output["vector"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|v| v.as_f64().unwrap_or(0.0))
                    .collect(),
                metadata: output
                    .get("metadata")
                    .filter(|m| m.is_object())
                    .cloned()
                    .unwrap_or_else(|| json!({})),
                timestamp: loaded_at,
            })
            .collect(),
        next_ids: body["nextEventIds"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect(),
        metadata: json!({}),
        was_just_matched: false,
        predecessor_chain: Vec::new(),
    })
}

/// C++ `sequence_from_json`.
fn sequence_from_json(body: &Value) -> Result<Sequence, EngineError> {
    let mut events = BTreeMap::new();
    for event in body["events"].as_array().into_iter().flatten() {
        let event = event_from_json(event)?;
        events.insert(event.id.clone(), event);
    }
    Ok(Sequence {
        id: body["id"]
            .as_str()
            .map_or_else(|| mint("sequence"), str::to_owned),
        name: body["name"].as_str().unwrap_or("unnamed").to_owned(),
        metadata: body
            .get("metadata")
            .filter(|m| m.is_object())
            .cloned()
            .unwrap_or_else(|| json!({})),
        schema_version: String::new(),
        events,
        deprecated_at: String::new(),
        replaced_by: String::new(),
    })
}

/// The events' default comparator, as a freshly built C++ event reports it.
const DEFAULT_MATCH: &str = "gte";

impl Sequence {
    fn validation_errors(&self) -> Vec<&'static str> {
        let mut errors = Vec::new();
        if !self.events.values().any(|e| e.initial) {
            errors.push("CriticalEventSequence must have at least one initial vector");
        }
        if !self.events.values().any(|e| !e.outputs.is_empty()) {
            errors.push("CriticalEventSequence must have at least one vector with output");
        }
        errors
    }
}

fn cosine(a: &[f64], b: &[f64]) -> f64 {
    let dot: f64 = a.iter().zip(b).map(|(x, y)| x * y).sum();
    let na: f64 = a.iter().map(|v| v * v).sum();
    let nb: f64 = b.iter().map(|v| v * v).sum();
    if na == 0.0 || nb == 0.0 {
        return 0.0;
    }
    dot / (na.sqrt() * nb.sqrt())
}

/// What a stored vector is searched by: its `vector`, else its `elements`'
/// values (an element may be a bare number).
fn searchable(document: &Value) -> Vec<f64> {
    if let Some(values) = document["vector"].as_array() {
        return values.iter().map(|v| v.as_f64().unwrap_or(0.0)).collect();
    }
    document["elements"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|e| {
            if e.is_object() {
                e["value"].as_f64().unwrap_or(0.0)
            } else {
                e.as_f64().unwrap_or(0.0)
            }
        })
        .collect()
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

fn missing(what: &str) -> (StatusCode, Json<Value>) {
    error_response(EngineError::not_found(format!("{what} not found")))
}

fn lock<'a>(
    engine: &'a SharedEngine,
) -> Result<std::sync::RwLockWriteGuard<'a, Engine>, (StatusCode, Json<Value>)> {
    engine.write().map_err(|_| error_response(lock_error()))
}

pub(crate) async fn store_vector(State(engine): State<SharedEngine>, body: Bytes) -> Answer {
    let mut document = body_of(&body)?;
    if !document.is_object() {
        return Err(error_response(EngineError::bad_request(
            "a vector must be a JSON object",
        )));
    }
    let id = document["id"]
        .as_str()
        .map_or_else(|| mint("vector"), str::to_owned);
    document["id"] = json!(id);
    lock(&engine)?.vector_store.insert(id, document.clone());
    Ok(Json(json!({ "success": true, "vector": document })))
}

pub(crate) async fn search_vectors(State(engine): State<SharedEngine>, body: Bytes) -> Answer {
    let body = body_of(&body)?;
    let query = searchable(&json!({ "vector": body["vector"].clone() }));
    let limit = body["limit"].as_f64().unwrap_or(10.0) as i64;
    let threshold = body["threshold"].as_f64();
    let state = lock(&engine)?;
    let mut results = Vec::new();
    for document in state.vector_store.values() {
        if results.len() as i64 >= limit {
            break;
        }
        let score = cosine(&query, &searchable(document));
        if threshold.is_some_and(|t| score < t) {
            continue;
        }
        results.push(json!({ "vector": document, "score": score }));
    }
    Ok(Json(json!({ "results": results })))
}

pub(crate) async fn vector(State(engine): State<SharedEngine>, Path(id): Path<String>) -> Answer {
    let state = lock(&engine)?;
    let document = state
        .vector_store
        .get(&id)
        .ok_or_else(|| missing("Vector"))?;
    Ok(Json(json!({ "vector": document })))
}

pub(crate) async fn delete_vector(
    State(engine): State<SharedEngine>,
    Path(id): Path<String>,
) -> Answer {
    lock(&engine)?.vector_store.remove(&id);
    Ok(Json(json!({ "success": true, "id": id })))
}

pub(crate) async fn persist() -> Json<Value> {
    Json(json!({ "success": true }))
}

pub(crate) async fn list(State(engine): State<SharedEngine>) -> Answer {
    let state = lock(&engine)?;
    let sequences: Vec<Value> = state
        .sequence_store
        .values()
        .map(|s| s.to_json(DEFAULT_MATCH))
        .collect();
    Ok(Json(json!({ "sequences": sequences })))
}

pub(crate) async fn create(State(engine): State<SharedEngine>, body: Bytes) -> Answer {
    let body = body_of(&body)?;
    let sequence = sequence_from_json(&body).map_err(error_response)?;
    let errors = sequence.validation_errors();
    if !errors.is_empty() {
        return Err((StatusCode::BAD_REQUEST, Json(json!({ "error": errors }))));
    }
    let answer = sequence.to_json(DEFAULT_MATCH);
    lock(&engine)?
        .sequence_store
        .insert(sequence.id.clone(), sequence);
    Ok(Json(json!({ "success": true, "sequence": answer })))
}

pub(crate) async fn sequence(State(engine): State<SharedEngine>, Path(id): Path<String>) -> Answer {
    let state = lock(&engine)?;
    let sequence = state
        .sequence_store
        .get(&id)
        .ok_or_else(|| missing("Sequence"))?;
    Ok(Json(json!({ "sequence": sequence.to_json(DEFAULT_MATCH) })))
}

pub(crate) async fn delete_sequence(
    State(engine): State<SharedEngine>,
    Path(id): Path<String>,
) -> Answer {
    lock(&engine)?
        .sequence_store
        .remove(&id)
        .ok_or_else(|| missing("Sequence"))?;
    Ok(Json(json!({ "success": true })))
}

/// Every event back to its initial activation, `wasJustMatched` cleared.
pub(crate) async fn reset_sequence(
    State(engine): State<SharedEngine>,
    Path(id): Path<String>,
) -> Answer {
    let mut state = lock(&engine)?;
    let sequence = state
        .sequence_store
        .get_mut(&id)
        .ok_or_else(|| missing("Sequence"))?;
    for event in sequence.events.values_mut() {
        event.active = event.initial;
        event.was_just_matched = false;
    }
    Ok(Json(json!({ "success": true })))
}

pub(crate) async fn add_event(
    State(engine): State<SharedEngine>,
    Path(id): Path<String>,
    body: Bytes,
) -> Answer {
    let body = body_of(&body)?;
    let event = event_from_json(&body).map_err(error_response)?;
    let mut state = lock(&engine)?;
    let sequence = state
        .sequence_store
        .get_mut(&id)
        .ok_or_else(|| missing("Sequence"))?;
    let answer = event.to_json(DEFAULT_MATCH);
    sequence.events.insert(event.id.clone(), event);
    Ok(Json(json!({ "success": true, "vector": answer })))
}
