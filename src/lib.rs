use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs,
    path::Path as FsPath,
    sync::{Arc, RwLock},
    time::{SystemTime, UNIX_EPOCH},
};

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug)]
pub struct EngineError {
    pub status: StatusCode,
    pub message: String,
}

impl EngineError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for EngineError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for EngineError {}

#[derive(Clone)]
struct Element {
    value: f64,
    comparator: String,
    threshold: Option<f64>,
}

#[derive(Clone)]
struct Output {
    id: String,
    vector: Vec<f64>,
    metadata: Value,
}

struct Event {
    id: String,
    initial: bool,
    active: bool,
    elements: Vec<Element>,
    outputs: Vec<Output>,
    next_ids: Vec<String>,
    algorithm: String,
    was_just_matched: bool,
    predecessor_chain: Vec<String>,
}

struct Sequence {
    id: String,
    events: BTreeMap<String, Event>,
}

struct Machine {
    id: String,
    name: String,
    description: String,
    input_offset: usize,
    input_length: usize,
    match_algorithm: String,
    arbiter_rule: String,
    output_merge: String,
    output_alphabet_top: Option<usize>,
    raw: Value,
    sequences: BTreeMap<String, Sequence>,
}

#[derive(Default)]
pub struct Engine {
    machines: BTreeMap<String, Machine>,
    dimension: usize,
    threshold: f64,
    step: u64,
    history: Vec<Value>,
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn number(value: Option<&Value>, default: f64) -> f64 {
    value.and_then(Value::as_f64).unwrap_or(default)
}

fn integer(value: Option<&Value>, default: usize) -> usize {
    value
        .and_then(Value::as_u64)
        .and_then(|number| usize::try_from(number).ok())
        .unwrap_or(default)
}

fn string(value: Option<&Value>, default: &str) -> String {
    value.and_then(Value::as_str).unwrap_or(default).to_owned()
}

fn json_vector(value: Option<&Value>) -> Vec<f64> {
    value
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_f64)
        .collect()
}

impl Machine {
    fn from_json(input: Value) -> Result<Self, EngineError> {
        let root = input
            .as_object()
            .ok_or_else(|| EngineError::bad_request("machine definition must be a JSON object"))?;
        if let Some(version) = root.get("version").and_then(Value::as_str) {
            let major = version.split('.').next().unwrap_or_default();
            if major != "1" {
                return Err(EngineError::bad_request(format!(
                    "unsupported machine version: {version}"
                )));
            }
        }

        let machine_value = root.get("machine").unwrap_or(&input);
        let machine = machine_value
            .as_object()
            .ok_or_else(|| EngineError::bad_request("machine must be a JSON object"))?;
        let name = string(machine.get("name"), "");
        if name.trim().is_empty() {
            return Err(EngineError::bad_request(
                "machine.name is required and must not be empty",
            ));
        }
        let id = string(root.get("id").or_else(|| machine.get("id")), name.as_str());
        if id.trim().is_empty() {
            return Err(EngineError::bad_request("machine id must not be empty"));
        }

        let mapping = machine.get("perceptualMapping").unwrap_or(&Value::Null);
        let input_region = mapping.get("input").unwrap_or(&Value::Null);
        let sequences_json = machine
            .get("sequences")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let mut sequences = BTreeMap::new();
        let mut first_event_width = 0;

        for sequence_json in sequences_json {
            let sequence_id = string(sequence_json.get("id"), "");
            if sequence_id.is_empty() {
                return Err(EngineError::bad_request(
                    "every sequence must have a non-empty id",
                ));
            }
            let mut events = BTreeMap::new();
            for event_json in sequence_json
                .get("vectors")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let event_id = string(event_json.get("id"), "");
                if event_id.is_empty() {
                    return Err(EngineError::bad_request(format!(
                        "every vector in sequence {sequence_id} must have a non-empty id"
                    )));
                }
                let elements: Vec<Element> = event_json
                    .get("elements")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .map(|element| Element {
                        value: number(element.get("value"), 0.0),
                        comparator: string(element.get("comparatorType"), ""),
                        threshold: element.get("threshold").and_then(Value::as_f64),
                    })
                    .collect();
                if first_event_width == 0 && !elements.is_empty() {
                    first_event_width = elements.len();
                }
                let outputs = event_json
                    .get("outputVectors")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .enumerate()
                    .map(|(index, output)| Output {
                        id: string(output.get("id"), &format!("{event_id}-output-{index}")),
                        vector: json_vector(output.get("vector")),
                        metadata: output.get("metadata").cloned().unwrap_or_else(|| json!({})),
                    })
                    .collect();
                let initial = event_json
                    .get("isInitial")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                let event = Event {
                    id: event_id.clone(),
                    initial,
                    active: initial,
                    elements,
                    outputs,
                    next_ids: event_json
                        .get("nextVectorIds")
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten()
                        .filter_map(Value::as_str)
                        .map(str::to_owned)
                        .collect(),
                    algorithm: string(event_json.get("matchAlgorithm"), ""),
                    was_just_matched: false,
                    predecessor_chain: Vec::new(),
                };
                if events.insert(event_id.clone(), event).is_some() {
                    return Err(EngineError::bad_request(format!(
                        "duplicate vector id {event_id} in sequence {sequence_id}"
                    )));
                }
            }
            if sequences
                .insert(
                    sequence_id.clone(),
                    Sequence {
                        id: sequence_id.clone(),
                        events,
                    },
                )
                .is_some()
            {
                return Err(EngineError::bad_request(format!(
                    "duplicate sequence id {sequence_id}"
                )));
            }
        }
        let input_length = integer(input_region.get("length"), first_event_width);
        Ok(Self {
            id,
            name,
            description: string(machine.get("description"), ""),
            input_offset: integer(input_region.get("offset"), 0),
            input_length,
            match_algorithm: string(machine.get("matchAlgorithm"), "gte").to_lowercase(),
            arbiter_rule: string(machine.get("arbiterRule"), "passthrough").to_lowercase(),
            output_merge: string(machine.get("outputMergeTransformation"), "or").to_lowercase(),
            output_alphabet_top: mapping
                .get("outputAlphabetTop")
                .and_then(Value::as_u64)
                .and_then(|top| usize::try_from(top).ok()),
            raw: machine_value.clone(),
            sequences,
        })
    }

    fn summary(&self) -> Value {
        let total_events = self
            .sequences
            .values()
            .map(|sequence| sequence.events.len())
            .sum::<usize>();
        json!({
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "sequenceCount": self.sequences.len(),
            "totalEvents": total_events,
            "sequenceIds": self.sequences.keys().collect::<Vec<_>>(),
            "perceptualMapping": self.raw.get("perceptualMapping").cloned().unwrap_or(Value::Null),
        })
    }

    fn reset(&mut self) {
        for sequence in self.sequences.values_mut() {
            for event in sequence.events.values_mut() {
                event.active = event.initial;
                event.was_just_matched = false;
                event.predecessor_chain.clear();
            }
        }
    }

    fn transition(&mut self, input: &[f64]) -> Value {
        let timestamp = now_ms();
        let mut sequence_results = serde_json::Map::new();
        let mut all_outputs: Vec<(String, Output, Vec<String>)> = Vec::new();
        let mut sequence_output_count = 0;

        for sequence in self.sequences.values_mut() {
            for event in sequence.events.values_mut() {
                event.was_just_matched = false;
            }
            let active_ids: Vec<String> = sequence
                .events
                .values()
                .filter(|event| event.active)
                .map(|event| event.id.clone())
                .collect();
            let mut matched = Vec::new();
            let mut activated = Vec::new();
            let mut emitted = Vec::new();
            let mut pending: BTreeMap<String, Vec<String>> = BTreeMap::new();

            for event_id in active_ids {
                let Some(event) = sequence.events.get_mut(&event_id) else {
                    continue;
                };
                let matched_event = event.matches(input, &self.match_algorithm);
                if !matched_event {
                    if !event.initial {
                        event.active = false;
                        event.predecessor_chain.clear();
                    }
                    continue;
                }
                matched.push(event_id.clone());
                let mut provenance = event.predecessor_chain.clone();
                provenance.push(event.id.clone());
                if !event.outputs.is_empty() {
                    event.was_just_matched = true;
                }
                for output in event.outputs.iter().cloned() {
                    emitted.push(json!({
                        "id": output.id,
                        "vector": output.vector,
                        "metadata": output.metadata,
                        "timestamp": timestamp,
                        "provenance": provenance,
                    }));
                    all_outputs.push((sequence.id.clone(), output, provenance.clone()));
                }
                if !event.initial && event.outputs.is_empty() && !event.next_ids.is_empty() {
                    event.active = false;
                    event.predecessor_chain.clear();
                }
                for next_id in &event.next_ids {
                    pending
                        .entry(next_id.clone())
                        .or_insert_with(|| provenance.clone());
                }
            }
            for (next_id, provenance) in pending {
                if let Some(next) = sequence.events.get_mut(&next_id) {
                    if !next.active {
                        next.active = true;
                        next.predecessor_chain = provenance;
                        activated.push(next_id);
                    }
                }
            }
            if !emitted.is_empty() {
                sequence_output_count += 1;
            }
            sequence_results.insert(
                sequence.id.clone(),
                json!({
                    "matchedEvents": matched,
                    "activatedEvents": activated,
                    "assertedOutputs": emitted,
                }),
            );
        }

        let has_output = match self.arbiter_rule.as_str() {
            "and" => !self.sequences.is_empty() && sequence_output_count == self.sequences.len(),
            "or" => sequence_output_count > 0,
            _ => !all_outputs.is_empty(),
        };
        let picked = if has_output {
            all_outputs.first().map(|(sequence_id, output, provenance)| {
                json!({
                    "id": output.id,
                    "vector": output.vector,
                    "metadata": {
                        "arbiter": true,
                        "combinedFrom": all_outputs.len(),
                        "sources": all_outputs.iter().map(|(_, source, _)| source.id.clone()).collect::<Vec<_>>(),
                    },
                    "timestamp": timestamp,
                    "provenance": provenance,
                    "sequenceId": sequence_id,
                })
            })
        } else {
            None
        };
        let merged = if has_output {
            let vectors = all_outputs
                .iter()
                .map(|(_, output, _)| output.vector.as_slice())
                .collect::<Vec<_>>();
            fold_outputs(&vectors, &self.output_merge, self.output_alphabet_top)
        } else {
            None
        };
        json!({
            "machineId": self.id,
            "inputVector": input,
            "timestamp": timestamp,
            "sequenceResults": sequence_results,
            "machineOutput": picked,
            "mergedOutput": merged,
            "arbiterMetadata": {
                "rule": self.arbiter_rule,
                "totalInputs": self.sequences.len(),
                "sequencesWithOutput": sequence_output_count,
                "shouldOutput": has_output,
            },
        })
    }
}

impl Event {
    fn matches(&self, input: &[f64], machine_algorithm: &str) -> bool {
        if input.len() != self.elements.len() {
            return false;
        }
        self.elements.iter().zip(input).all(|(element, actual)| {
            let algorithm = if element.comparator.is_empty() {
                if self.algorithm.is_empty() {
                    machine_algorithm
                } else {
                    &self.algorithm
                }
            } else {
                &element.comparator
            };
            match algorithm {
                "equals" | "custom" => *actual == element.value,
                "threshold" => *actual >= element.threshold.unwrap_or(element.value),
                "pattern" => (*actual - element.value).abs() <= element.threshold.unwrap_or(0.5),
                _ => *actual >= element.value,
            }
        })
    }
}

fn fold_outputs(
    vectors: &[&[f64]],
    transformation: &str,
    chain_top: Option<usize>,
) -> Option<Vec<f64>> {
    if vectors.is_empty() {
        return None;
    }
    if matches!(transformation, "strong-conjunction" | "strong-disjunction") && chain_top.is_none()
    {
        return None;
    }
    let width = vectors.iter().map(|vector| vector.len()).max().unwrap_or(0);
    let mut result = Vec::with_capacity(width);
    for index in 0..width {
        let cells: Vec<f64> = vectors
            .iter()
            .map(|vector| vector.get(index).copied().unwrap_or(0.0))
            .collect();
        let asserted = cells.iter().filter(|cell| **cell != 0.0).count();
        let value = match transformation {
            "and" => f64::from(asserted == cells.len()),
            "xor" => f64::from(asserted % 2 == 1),
            "nor" => f64::from(asserted == 0),
            "nand" => f64::from(asserted < cells.len()),
            "meet" => cells.into_iter().fold(f64::INFINITY, f64::min),
            "join" => cells.into_iter().fold(f64::NEG_INFINITY, f64::max),
            "discrete-median" => {
                let mut sorted = cells;
                sorted.sort_by(f64::total_cmp);
                sorted[(sorted.len() - 1) / 2]
            }
            "strong-disjunction" => cells
                .iter()
                .sum::<f64>()
                .min(chain_top.unwrap_or_default() as f64),
            "strong-conjunction" => {
                let top = chain_top.unwrap_or_default() as f64;
                (cells.iter().sum::<f64>() - top * (cells.len() - 1) as f64).max(0.0)
            }
            "or" | _ => f64::from(asserted > 0),
        };
        result.push(value);
    }
    Some(result)
}

impl Engine {
    pub fn new(dimension: usize) -> Self {
        Self {
            dimension,
            threshold: 0.5,
            ..Self::default()
        }
    }

    pub fn load_machine(&mut self, input: Value) -> Result<Value, EngineError> {
        let machine = Machine::from_json(input)?;
        if self.machines.contains_key(&machine.id) {
            return Err(EngineError {
                status: StatusCode::CONFLICT,
                message: format!("machine {} already exists", machine.id),
            });
        }
        let summary = machine.summary();
        self.machines.insert(machine.id.clone(), machine);
        Ok(summary)
    }

    pub fn load_directory(&mut self, directory: &FsPath) -> Result<usize, EngineError> {
        if !directory.exists() {
            return Ok(0);
        }
        let entries = fs::read_dir(directory).map_err(|error| EngineError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: format!("could not read machine directory: {error}"),
        })?;
        let mut paths = entries
            .map(|entry| {
                entry
                    .map(|entry| entry.path())
                    .map_err(|error| EngineError {
                        status: StatusCode::INTERNAL_SERVER_ERROR,
                        message: format!("could not read machine directory entry: {error}"),
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        paths.sort();
        let mut loaded = 0;
        for path in paths.into_iter().filter(|path| {
            path.extension().and_then(|extension| extension.to_str()) == Some("json")
        }) {
            let contents = fs::read_to_string(&path).map_err(|error| EngineError {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                message: format!("could not read {}: {error}", path.display()),
            })?;
            let definition = serde_json::from_str(&contents).map_err(|error| EngineError {
                status: StatusCode::BAD_REQUEST,
                message: format!("invalid machine JSON in {}: {error}", path.display()),
            })?;
            self.load_machine(definition)?;
            loaded += 1;
        }
        Ok(loaded)
    }

    pub fn list_machines(&self) -> Vec<Value> {
        self.machines.values().map(Machine::summary).collect()
    }

    pub fn machine(&self, id: &str) -> Option<Value> {
        self.machines.get(id).map(|machine| {
            json!({
                "id": machine.id,
                "name": machine.name,
                "description": machine.description,
                "machine": machine.raw,
            })
        })
    }

    pub fn remove_machine(&mut self, id: &str) -> bool {
        self.machines.remove(id).is_some()
    }

    fn stats(&self) -> Value {
        json!({
            "machineCount": self.machines.len(),
            "step": self.step,
            "dimension": self.dimension,
        })
    }

    fn history(&self) -> Value {
        json!({ "history": self.history })
    }

    pub fn process_machine(&mut self, id: &str, input: &[f64]) -> Result<Value, EngineError> {
        let machine = self
            .machines
            .get_mut(id)
            .ok_or_else(|| EngineError::not_found(format!("machine {id} not found")))?;
        if machine.input_length != 0 && input.len() != machine.input_length {
            return Err(EngineError::bad_request(format!(
                "input vector length {} does not match machine input length {}",
                input.len(),
                machine.input_length
            )));
        }
        self.step += 1;
        let result = machine.transition(input);
        self.history.push(result.clone());
        Ok(result)
    }

    pub fn process_all(&mut self, input: &[f64]) -> Value {
        self.step += 1;
        let timestamp = now_ms();
        let universal = self.dimension > 0 && input.len() == self.dimension;
        let outputs: Vec<Value> = self
            .machines
            .values_mut()
            .filter_map(|machine| {
                let machine_input = if universal {
                    let start = machine.input_offset;
                    let end = start.saturating_add(machine.input_length);
                    input.get(start..end)?
                } else {
                    input
                };
                if machine.input_length != 0 && machine_input.len() != machine.input_length {
                    return None;
                }
                let result = machine.transition(machine_input);
                result
                    .get("machineOutput")
                    .filter(|output| !output.is_null())
                    .map(|output| {
                        json!({
                            "machineId": machine.id,
                            "machineOutput": output,
                            "mergedOutput": result["mergedOutput"],
                        })
                    })
            })
            .collect();
        let result = json!({
            "inputEvent": input,
            "timestamp": timestamp,
            "outputs": outputs,
        });
        self.history.push(result.clone());
        result
    }

    pub fn reset(&mut self) {
        for machine in self.machines.values_mut() {
            machine.reset();
        }
        self.step = 0;
        self.history.clear();
    }

    fn health(&self) -> Value {
        json!({
            "status": "healthy",
            "timestamp": now_ms(),
            "version": VERSION,
        })
    }
}

type SharedEngine = Arc<RwLock<Engine>>;

fn error_response(error: EngineError) -> (StatusCode, Json<Value>) {
    (error.status, Json(json!({ "error": error.message })))
}

fn lock_error() -> EngineError {
    EngineError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: "engine state is unavailable".to_owned(),
    }
}

async fn root(State(engine): State<SharedEngine>) -> Json<Value> {
    let health = engine
        .read()
        .map(|state| state.health())
        .unwrap_or_else(|_| json!({ "status": "unavailable", "version": VERSION }));
    Json(json!({ "name": "Reality Engine", "version": VERSION, "health": health }))
}

async fn health(
    State(engine): State<SharedEngine>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let state = engine.read().map_err(|_| error_response(lock_error()))?;
    Ok(Json(state.health()))
}

async fn config(
    State(engine): State<SharedEngine>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let state = engine.read().map_err(|_| error_response(lock_error()))?;
    Ok(Json(json!({
        "dimension": state.dimension,
        "threshold": state.threshold,
        "version": VERSION,
    })))
}

async fn engine_stats(
    State(engine): State<SharedEngine>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let state = engine.read().map_err(|_| error_response(lock_error()))?;
    Ok(Json(state.stats()))
}

async fn engine_history(
    State(engine): State<SharedEngine>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let state = engine.read().map_err(|_| error_response(lock_error()))?;
    Ok(Json(state.history()))
}

async fn list_machines(
    State(engine): State<SharedEngine>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let state = engine.read().map_err(|_| error_response(lock_error()))?;
    Ok(Json(json!({ "machines": state.list_machines() })))
}

async fn add_machine(
    State(engine): State<SharedEngine>,
    Json(input): Json<Value>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let mut state = engine.write().map_err(|_| error_response(lock_error()))?;
    state
        .load_machine(input)
        .map(|machine| Json(json!({ "success": true, "machine": machine })))
        .map_err(error_response)
}

async fn get_machine(
    State(engine): State<SharedEngine>,
    Path(id): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let state = engine.read().map_err(|_| error_response(lock_error()))?;
    state
        .machine(&id)
        .map(|machine| Json(machine))
        .ok_or_else(|| error_response(EngineError::not_found(format!("machine {id} not found"))))
}

async fn delete_machine(
    State(engine): State<SharedEngine>,
    Path(id): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let mut state = engine.write().map_err(|_| error_response(lock_error()))?;
    if state.remove_machine(&id) {
        Ok(Json(json!({ "success": true, "id": id })))
    } else {
        Err(error_response(EngineError::not_found(format!(
            "machine {id} not found"
        ))))
    }
}

fn input_from_body(body: &Value) -> Result<Vec<f64>, EngineError> {
    let vector = body
        .get("vector")
        .or_else(|| body.get("input"))
        .ok_or_else(|| EngineError::bad_request("request must include vector"))?;
    let values = vector
        .as_array()
        .ok_or_else(|| EngineError::bad_request("vector must be an array"))?;
    values
        .iter()
        .map(|value| {
            value
                .as_f64()
                .ok_or_else(|| EngineError::bad_request("vector values must be finite numbers"))
        })
        .collect()
}

async fn process_machine(
    State(engine): State<SharedEngine>,
    Path(id): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let input = input_from_body(&body).map_err(error_response)?;
    let mut state = engine.write().map_err(|_| error_response(lock_error()))?;
    state
        .process_machine(&id, &input)
        .map(|result| Json(json!({ "success": true, "result": result })))
        .map_err(error_response)
}

async fn process_all(
    State(engine): State<SharedEngine>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let input = input_from_body(&body).map_err(error_response)?;
    let mut state = engine.write().map_err(|_| error_response(lock_error()))?;
    Ok(Json(json!({ "result": state.process_all(&input) })))
}

async fn reset(
    State(engine): State<SharedEngine>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let mut state = engine.write().map_err(|_| error_response(lock_error()))?;
    state.reset();
    Ok(Json(json!({ "success": true, "message": "engine reset" })))
}

pub fn app(engine: Engine) -> Router {
    let state = Arc::new(RwLock::new(engine));
    Router::new()
        .route("/", get(root))
        .route("/api", get(root))
        .route("/api/health", get(health))
        .route("/api/config", get(config))
        .route("/api/machines", get(list_machines).post(add_machine))
        .route("/api/machines/json/list", get(list_machines))
        .route("/api/machines/:id", get(get_machine).delete(delete_machine))
        .route("/api/machines/:id/process", post(process_machine))
        .route("/api/engine/process", post(process_all))
        .route("/api/engine/stats", get(engine_stats))
        .route("/api/engine/history", get(engine_history))
        .route("/api/engine/reset", post(reset))
        .with_state(state)
}

pub fn engine_from_environment() -> Result<Engine, EngineError> {
    let dimension = std::env::var("VECTOR_DIMENSION")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(7680);
    let mut engine = Engine::new(dimension);
    let machine_directory = std::env::var("MACHINES_DIR")
        .unwrap_or_else(|_| "../RealityEngine_Machines/machines".to_owned());
    engine.load_directory(FsPath::new(&machine_directory))?;
    Ok(engine)
}

pub async fn serve() -> Result<(), Box<dyn std::error::Error>> {
    let port = std::env::var("REALITY_ENGINE_PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(5801);
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
    axum::serve(listener, app(engine_from_environment()?)).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn machine_definition() -> Value {
        json!({
            "version": "1.0.0",
            "machine": {
                "name": "two-step",
                "perceptualMapping": {
                    "input": { "offset": 0, "length": 1 },
                    "output": { "offset": 1, "length": 1 }
                },
                "sequences": [{
                    "id": "sequence",
                    "vectors": [
                        {
                            "id": "start",
                            "isInitial": true,
                            "elements": [{ "value": 1, "comparatorType": "equals" }],
                            "nextVectorIds": ["finish"]
                        },
                        {
                            "id": "finish",
                            "elements": [{ "value": 0, "comparatorType": "equals" }],
                            "outputVectors": [{ "id": "out", "vector": [1] }]
                        }
                    ]
                }]
            }
        })
    }

    #[test]
    fn event_successors_activate_after_current_transition_and_emit_next_cycle() {
        let mut engine = Engine::new(8);
        engine.load_machine(machine_definition()).unwrap();

        let first = engine.process_machine("two-step", &[1.0]).unwrap();
        assert_eq!(
            first["sequenceResults"]["sequence"]["matchedEvents"][0],
            "start"
        );
        assert_eq!(
            first["sequenceResults"]["sequence"]["activatedEvents"][0],
            "finish"
        );
        assert!(first["machineOutput"].is_null());

        let second = engine.process_machine("two-step", &[0.0]).unwrap();
        assert_eq!(second["machineOutput"]["vector"][0], 1.0);
        assert_eq!(second["mergedOutput"][0], 1.0);
    }

    #[test]
    fn reset_restores_initial_events_and_clears_match_state() {
        let mut engine = Engine::new(8);
        engine.load_machine(machine_definition()).unwrap();
        engine.process_machine("two-step", &[1.0]).unwrap();
        engine.process_machine("two-step", &[0.0]).unwrap();
        engine.reset();
        let result = engine.process_machine("two-step", &[1.0]).unwrap();
        assert_eq!(
            result["sequenceResults"]["sequence"]["matchedEvents"][0],
            "start"
        );
    }

    #[test]
    fn output_merge_uses_full_collection_and_refuses_missing_chain_top() {
        assert_eq!(
            fold_outputs(&[&[1.0, 0.0], &[0.0, 1.0]], "or", None),
            Some(vec![1.0, 1.0])
        );
        assert_eq!(
            fold_outputs(&[&[2.0], &[2.0]], "strong-conjunction", None),
            None
        );
        assert_eq!(
            fold_outputs(&[&[2.0], &[2.0]], "strong-conjunction", Some(3)),
            Some(vec![1.0])
        );
    }

    #[test]
    fn machine_versions_and_required_names_are_validated() {
        let mut invalid_version = machine_definition();
        invalid_version["version"] = json!("2.0.0");
        assert!(Engine::new(1).load_machine(invalid_version).is_err());

        let mut missing_name = machine_definition();
        missing_name["machine"]
            .as_object_mut()
            .unwrap()
            .remove("name");
        assert!(Engine::new(1).load_machine(missing_name).is_err());
    }
}
