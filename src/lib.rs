use axum::{
    Json, Router,
    body::Bytes,
    extract::Query,
    extract::{Path, State, rejection::JsonRejection},
    http::{Method, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::{delete, get, post, put},
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    collections::{BTreeMap, VecDeque},
    fs,
    path::Path as FsPath,
    sync::atomic::{AtomicU64, Ordering},
    sync::{Arc, RwLock},
    time::{SystemTime, UNIX_EPOCH},
};

mod arbitration;
pub mod instance_clock;
mod machine_routes;
mod machine_view;
mod sequences;
mod simulation;
mod step;

use instance_clock::InstanceClock;

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
    /// When the machine was loaded (ms), as C++ stamps a declared output.
    timestamp: u128,
}

#[derive(Clone)]
struct Event {
    id: String,
    initial: bool,
    active: bool,
    elements: Vec<Element>,
    outputs: Vec<Output>,
    next_ids: Vec<String>,
    /// The event's own `metadata`, as declared; `{}` when not.
    metadata: Value,
    was_just_matched: bool,
    predecessor_chain: Vec<String>,
}

#[derive(Clone)]
struct Sequence {
    id: String,
    /// As declared; `unnamed` when not, as C++ loads it.
    name: String,
    /// The sequence's own `metadata` object, `{}` when absent.
    metadata: Value,
    /// `schemaVersion`, as declared; empty when not.
    schema_version: String,
    events: BTreeMap<String, Event>,
    /// `deprecatedAt` and `replacedBy`, as declared; empty when not.
    deprecated_at: String,
    replaced_by: String,
}

#[derive(Clone)]
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
    /// SURFACE_SPEC.md, "`transitionsInhibited`": `true` accepts a Reality
    /// Event and does not pass it forward. Default `false`.
    transitions_inhibited: bool,
}

/// CES coverage, as C++'s `CesCoverageRegistry` keeps it: counters since the
/// registry was created, recorded at every transition. An engine reset does not
/// clear it (SURFACE_SPEC.md, "CES coverage"): coverage is a record of what the
/// corpus has been shown to do, and a reset does not un-show it.
struct Coverage {
    /// (machine id, machine name) -> transitions observed
    steps: BTreeMap<(String, String), u64>,
    /// (machine id, machine name, sequence, event) -> times matched / activated
    matched: BTreeMap<(String, String, String, String), u64>,
    activated: BTreeMap<(String, String, String, String), u64>,
    /// (machine id, machine name, sequence) -> asserted outputs emitted
    outputs: BTreeMap<(String, String, String), u64>,
    /// (owner team, process status, RAG status, machine id) -> governance
    /// decisions joined onto merge operations
    paging: BTreeMap<(String, String, String, String), u64>,
    /// (machine id, machine name, sequence, replaced by) -> outputs a
    /// deprecated sequence asserted
    deprecated: BTreeMap<(String, String, String, String), u64>,
    started_at_ms: u128,
}

impl Default for Coverage {
    fn default() -> Self {
        Self {
            steps: BTreeMap::new(),
            matched: BTreeMap::new(),
            activated: BTreeMap::new(),
            outputs: BTreeMap::new(),
            paging: BTreeMap::new(),
            deprecated: BTreeMap::new(),
            started_at_ms: now_ms(),
        }
    }
}

impl Coverage {
    fn record_paging_decision(
        &mut self,
        owner_team: String,
        process_status: String,
        rag_status_code: String,
        machine_id: String,
    ) {
        *self
            .paging
            .entry((owner_team, process_status, rag_status_code, machine_id))
            .or_default() += 1;
    }

    fn record_deprecated_fire(
        &mut self,
        machine_id: &str,
        machine_name: &str,
        sequence_id: &str,
        replaced_by: &str,
    ) {
        *self
            .deprecated
            .entry((
                machine_id.to_owned(),
                machine_name.to_owned(),
                sequence_id.to_owned(),
                replaced_by.to_owned(),
            ))
            .or_default() += 1;
    }

    fn record(&mut self, id: &str, name: &str, result: &Value) {
        let machine = (id.to_owned(), name.to_owned());
        *self.steps.entry(machine.clone()).or_default() += 1;
        let should_output = result["arbiterMetadata"]["shouldOutput"]
            .as_bool()
            .unwrap_or(false);
        let Some(sequences) = result["sequenceResults"].as_object() else {
            return;
        };
        for (sequence, sr) in sequences {
            let key = |event: &Value| {
                (
                    machine.0.clone(),
                    machine.1.clone(),
                    sequence.clone(),
                    event.as_str().unwrap_or_default().to_owned(),
                )
            };
            for event in sr["matchedEvents"].as_array().into_iter().flatten() {
                *self.matched.entry(key(event)).or_default() += 1;
            }
            for event in sr["activatedEvents"].as_array().into_iter().flatten() {
                *self.activated.entry(key(event)).or_default() += 1;
            }
            let asserted = sr["assertedOutputs"].as_array().map_or(0, Vec::len) as u64;
            if should_output && asserted > 0 {
                *self
                    .outputs
                    .entry((machine.0.clone(), machine.1.clone(), sequence.clone()))
                    .or_default() += asserted;
            }
        }
    }
}

#[derive(Default)]
pub struct Engine {
    machines: BTreeMap<String, Machine>,
    dimension: usize,
    threshold: f64,
    step: u64,
    history: VecDeque<Value>,
    qdrant_url: String,
    collection_name: String,
    /// The corpus directory machines are loaded from; the on-disk catalog
    /// (`GET /api/machines/json/list`) and load-by-name read it.
    machines_dir: Option<std::path::PathBuf>,
    coverage: Coverage,
    /// Bumped on every machine added or removed (`re_runtime_mapping_version`).
    mapping_version: u64,
    /// The instance's Lamport clock (RealityEngine_CI#296). Never reset.
    clock: InstanceClock,
    /// The step path: the perceptual space, the trajectories, step completion (#11).
    steps: step::StepState,
    /// Checkpoints by machine id, then checkpoint id: each a copy of the
    /// machine as it was running (`/api/machines/:id/checkpoints`).
    checkpoints: BTreeMap<String, BTreeMap<String, machine_routes::Checkpoint>>,
    /// The engine's own Critical Event Sequences and stored vectors
    /// (`/api/sequences`, `/api/vectors`): engine-scoped, never compared.
    sequence_store: BTreeMap<String, Sequence>,
    vector_store: BTreeMap<String, Value>,
    /// The declared rule for each contended output cell.
    arbitration: step::ArbitrationRegistry,
}

/// The id a corpus file's machine gets, on every engine: `machine-` and the
/// file stem lowercased, each character that is not an ASCII letter or digit
/// replaced by `-` (C++ `reality_engine_server.cpp`, LSP
/// `canonical-machine-id-fragment`). Load-by-name replaces the machine holding
/// it rather than adding a second one.
fn canonical_machine_id(stem: &str) -> String {
    let fragment: String = stem
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect();
    format!("machine-{fragment}")
}

fn collect_json_files(
    directory: &FsPath,
    paths: &mut Vec<std::path::PathBuf>,
) -> Result<(), EngineError> {
    let unreadable = |what: String| EngineError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: what,
    };
    let entries = fs::read_dir(directory)
        .map_err(|error| unreadable(format!("could not read {}: {error}", directory.display())))?;
    for entry in entries {
        let entry = entry.map_err(|error| {
            unreadable(format!(
                "could not read entry in {}: {error}",
                directory.display()
            ))
        })?;
        let path = entry.path();
        let file_type = entry.file_type().map_err(|error| {
            unreadable(format!("could not inspect {}: {error}", path.display()))
        })?;
        if file_type.is_dir() {
            collect_json_files(&path, paths)?;
        } else if file_type.is_file()
            && path.extension().and_then(|extension| extension.to_str()) == Some("json")
        {
            paths.push(path);
        }
    }
    Ok(())
}

/// The corpus OWL semantics manifest: `SEMANTICS_MANIFEST`, else the first
/// `semantics/abox-manifest.json` found walking up to six levels from the
/// machines directory, as C++ and LSP find it.
fn semantics_manifest(machines_dir: &FsPath) -> Option<Value> {
    let path = match std::env::var("SEMANTICS_MANIFEST") {
        Ok(explicit) if !explicit.is_empty() => std::path::PathBuf::from(explicit),
        _ => {
            let start = fs::canonicalize(machines_dir).ok()?;
            start
                .ancestors()
                .take(6)
                .map(|dir| dir.join("semantics").join("abox-manifest.json"))
                .find(|candidate| candidate.is_file())?
        }
    };
    let manifest: Value = serde_json::from_str(&fs::read_to_string(path).ok()?).ok()?;
    manifest.get("machines").filter(|m| m.is_object())?;
    Some(manifest)
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

fn new_machine_id() -> String {
    static NEXT_ID: AtomicU64 = AtomicU64::new(0);
    let timestamp = now_ms() & 0x0000_ffff_ffff_ffff;
    let counter = NEXT_ID.fetch_add(1, Ordering::Relaxed) as u128;
    format!(
        "machine-{:08x}-{:04x}-7{:03x}-8{:03x}-{:012x}",
        (timestamp >> 16) as u32,
        timestamp as u16,
        (counter & 0x0fff) as u16,
        ((counter >> 12) & 0x0fff) as u16,
        counter & 0x0000_ffff_ffff_ffff
    )
}

/// Refuses a retired Reality Event key. SURFACE_SPEC.md ("Reality Event key
/// names — migration complete") accepts only the canonical spelling, and the
/// machine schema rejects these with `not: { required: [...] }`. Reading the
/// canonical key alone is not enough: a legacy file would load as valid but
/// empty, the silent failure the migration section describes.
fn reject_retired_key(
    object: &Value,
    retired: &str,
    canonical: &str,
    at: &str,
) -> Result<(), EngineError> {
    if object.get(retired).is_some() {
        return Err(EngineError::bad_request(format!(
            "{at} uses the retired key `{retired}`; the canonical key is `{canonical}`"
        )));
    }
    Ok(())
}

fn version_base(name: &str) -> &str {
    if let Some((base, suffix)) = name.rsplit_once(" v")
        && suffix.parse::<u64>().is_ok()
    {
        base
    } else {
        name
    }
}

impl Machine {
    fn from_json(input: Value) -> Result<Self, EngineError> {
        let loaded_at = now_ms();
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
        let id = string(root.get("id").or_else(|| machine.get("id")), "");

        let mapping = machine.get("perceptualMapping").unwrap_or(&Value::Null);
        let input_region = mapping.get("input").unwrap_or(&Value::Null);
        let sequences_json = machine
            .get("sequences")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let mut sequences = BTreeMap::new();
        let mut first_event_width = 0;

        for (index, input_sequence) in machine
            .get("inputSequences")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .enumerate()
        {
            reject_retired_key(
                input_sequence,
                "vectors",
                "events",
                &format!("inputSequences[{index}]"),
            )?;
        }

        for sequence_json in sequences_json {
            let sequence_id = string(sequence_json.get("id"), "");
            if sequence_id.is_empty() {
                return Err(EngineError::bad_request(
                    "every sequence must have a non-empty id",
                ));
            }
            let at = format!("sequence {sequence_id}");
            reject_retired_key(&sequence_json, "vectors", "events", &at)?;
            // The schema requires at least one event. A sequence without any
            // can never fire, and is how a misread key used to load "successfully".
            let events_json = sequence_json
                .get("events")
                .and_then(Value::as_array)
                .filter(|events| !events.is_empty())
                .ok_or_else(|| EngineError::bad_request(format!("{at} has no events")))?;
            let mut events = BTreeMap::new();
            for event_json in events_json {
                let event_id = string(event_json.get("id"), "");
                if event_id.is_empty() {
                    return Err(EngineError::bad_request(format!(
                        "every event in {at} must have a non-empty id"
                    )));
                }
                let event_at = format!("event {event_id} in {at}");
                reject_retired_key(event_json, "outputVectors", "outputEvents", &event_at)?;
                reject_retired_key(event_json, "nextVectorIds", "nextEventIds", &event_at)?;
                let elements: Vec<Element> = event_json
                    .get("elements")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .map(|element| Element {
                        value: number(element.get("value"), 0.0),
                        comparator: string(element.get("comparatorType"), "").to_lowercase(),
                        threshold: element.get("threshold").and_then(Value::as_f64),
                    })
                    .collect();
                if first_event_width == 0 && !elements.is_empty() {
                    first_event_width = elements.len();
                }
                let outputs = event_json
                    .get("outputEvents")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .enumerate()
                    .map(|(index, output)| Output {
                        id: string(output.get("id"), &format!("{event_id}-output-{index}")),
                        vector: json_vector(output.get("vector")),
                        metadata: output.get("metadata").cloned().unwrap_or_else(|| json!({})),
                        timestamp: loaded_at,
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
                        .get("nextEventIds")
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten()
                        .filter_map(Value::as_str)
                        .map(str::to_owned)
                        .collect(),
                    metadata: event_json
                        .get("metadata")
                        .filter(|m| m.is_object())
                        .cloned()
                        .unwrap_or_else(|| json!({})),
                    was_just_matched: false,
                    predecessor_chain: Vec::new(),
                };
                if events.insert(event_id.clone(), event).is_some() {
                    return Err(EngineError::bad_request(format!(
                        "duplicate event id {event_id} in {at}"
                    )));
                }
            }
            if sequences
                .insert(
                    sequence_id.clone(),
                    Sequence {
                        id: sequence_id.clone(),
                        name: string(sequence_json.get("name"), "unnamed"),
                        metadata: sequence_json
                            .get("metadata")
                            .filter(|m| m.is_object())
                            .cloned()
                            .unwrap_or_else(|| json!({})),
                        schema_version: string(sequence_json.get("schemaVersion"), ""),
                        events,
                        deprecated_at: string(sequence_json.get("deprecatedAt"), ""),
                        replaced_by: string(sequence_json.get("replacedBy"), ""),
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
            transitions_inhibited: false,
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

    fn version_name(&mut self, name: String, input_offset: usize) {
        self.name = name;
        self.raw["name"] = json!(self.name);
        if !self.raw["perceptualMapping"].is_object() {
            return;
        }
        let input_length = self.input_length;
        let mapping = self.raw["perceptualMapping"].as_object_mut().unwrap();
        let previous_output = mapping.get("output").cloned().unwrap_or_else(|| json!({}));
        let output_length = integer(previous_output.get("length"), 0);
        let output_offset = input_offset.saturating_add(input_length);
        mapping.insert(
            "input".to_owned(),
            json!({ "offset": input_offset, "length": input_length }),
        );
        mapping.insert(
            "output".to_owned(),
            json!({ "offset": output_offset, "length": output_length }),
        );
        self.input_offset = input_offset;
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

    fn transition(&mut self, input: &[f64], overriding: Option<&str>) -> Value {
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
                let matched_event = event.matches(input, &self.match_algorithm, overriding);
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
                if let Some(next) = sequence.events.get_mut(&next_id)
                    && !next.active
                {
                    next.active = true;
                    next.predecessor_chain = provenance;
                    activated.push(next_id);
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
            all_outputs.first().map(|(_, output, provenance)| {
                // The pick is a new output, minted per transition, as every
                // engine mints it; `sources` names the outputs it came from.
                json!({
                    "id": format!("machine-output-{}", new_machine_id().trim_start_matches("machine-")),
                    "vector": output.vector,
                    "metadata": {
                        "arbiter": true,
                        "combinedFrom": all_outputs.len(),
                        "sources": all_outputs.iter().map(|(_, source, _)| source.id.clone()).collect::<Vec<_>>(),
                    },
                    "timestamp": timestamp,
                    "provenance": provenance,
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
            "inputEvent": input,
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

    /// A Reality Event presented to this machine. Inhibited, it is accepted
    /// and not passed forward: the shape of a machine that matched nothing,
    /// no state change, nothing recorded (SURFACE_SPEC.md,
    /// "`transitionsInhibited`"; the arbiter metadata zeroed as C++ and LSP do).
    fn present(&mut self, input: &[f64], coverage: &mut Coverage) -> Value {
        let result = self.respond(input, None);
        if !self.transitions_inhibited {
            coverage.record(&self.id, &self.name, &result);
        }
        result
    }

    /// The transition result for `input`, without recording coverage: the step
    /// composes machines in parallel and records their coverage after the join.
    fn respond(&mut self, input: &[f64], overriding: Option<&str>) -> Value {
        if self.transitions_inhibited {
            let empty: serde_json::Map<String, Value> = self
                .sequences
                .keys()
                .map(|id| {
                    (
                        id.clone(),
                        json!({ "matchedEvents": [], "activatedEvents": [], "assertedOutputs": [] }),
                    )
                })
                .collect();
            return json!({
                "machineId": self.id,
                "inputEvent": input,
                "timestamp": now_ms(),
                "sequenceResults": empty,
                "machineOutput": null,
                "mergedOutput": null,
                "arbiterMetadata": {
                    "rule": self.arbiter_rule,
                    "totalInputs": 0,
                    "sequencesWithOutput": 0,
                    "shouldOutput": false,
                },
            });
        }
        self.transition(input, overriding)
    }
}

impl Event {
    /// SURFACE_SPEC.md § "Comparators" (owner ruling on RealityEngine_CI#606):
    /// the element's `comparatorType`, else the machine's `matchAlgorithm`,
    /// else `gte`; `t` is the element's `threshold`, else the comparator's
    /// default. `gte` binarizes both sides at `t`: a `0` means "must be off".
    /// An explicit override (`matchAlgorithmOverride`) applies above all three.
    fn matches(&self, input: &[f64], machine_algorithm: &str, overriding: Option<&str>) -> bool {
        if input.len() != self.elements.len() {
            return false;
        }
        self.elements.iter().zip(input).all(|(element, &actual)| {
            let comparator = overriding.unwrap_or(if element.comparator.is_empty() {
                machine_algorithm
            } else {
                &element.comparator
            });
            let value = element.value;
            match comparator {
                "equals" | "custom" => actual == value,
                "threshold" => (value - actual).abs() <= element.threshold.unwrap_or(0.1),
                "pattern" => 1.0 - (value - actual).abs() >= element.threshold.unwrap_or(0.5),
                _ => {
                    let t = element.threshold.unwrap_or(0.5);
                    (actual >= t) == (value >= t)
                }
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
            _ => f64::from(asserted > 0),
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
            qdrant_url: "http://localhost:4333".to_owned(),
            collection_name: "reality-events".to_owned(),
            ..Self::default()
        }
    }

    pub fn load_machine(&mut self, input: Value) -> Result<Value, EngineError> {
        let mut machine = Machine::from_json(input)?;
        let name_is_resident = self
            .machines
            .values()
            .any(|resident| resident.name == machine.name);
        if name_is_resident {
            let base = version_base(&machine.name).to_owned();
            let mut version = 2_u64;
            loop {
                let candidate = format!("{base} v{version}");
                if !self
                    .machines
                    .values()
                    .any(|resident| resident.name == candidate)
                {
                    machine.version_name(candidate, self.dimension);
                    let mapping = machine.raw.get("perceptualMapping");
                    if mapping.is_some_and(Value::is_object) {
                        let output = mapping.and_then(|value| value.get("output"));
                        self.dimension = self
                            .dimension
                            .saturating_add(machine.input_length)
                            .saturating_add(integer(
                                output.and_then(|region| region.get("length")),
                                0,
                            ));
                    }
                    break;
                }
                version = version.saturating_add(1);
            }
        }
        if machine.id.is_empty() || self.machines.contains_key(&machine.id) {
            machine.id = new_machine_id();
        }
        let summary = machine.summary();
        self.admit(machine);
        Ok(summary)
    }

    /// Makes `machine` resident under its own id, replacing any machine
    /// holding it, and widens the space to its mapping.
    fn admit(&mut self, mut machine: Machine) {
        machine.raw["id"] = json!(machine.id);
        let mapping = machine.raw.get("perceptualMapping");
        if let Some(mapping) = mapping {
            let input = mapping.get("input");
            let output = mapping.get("output");
            self.dimension = self
                .dimension
                .max(machine.input_offset.saturating_add(machine.input_length))
                .max(
                    integer(output.and_then(|region| region.get("offset")), 0)
                        .saturating_add(integer(output.and_then(|region| region.get("length")), 0)),
                )
                .max(integer(input.and_then(|region| region.get("offset")), 0));
        }
        self.machines.insert(machine.id.clone(), machine);
        self.mapping_version += 1;
    }

    pub fn load_directory(&mut self, directory: &FsPath) -> Result<usize, EngineError> {
        if !directory.exists() {
            return Ok(0);
        }
        self.machines_dir = Some(directory.to_path_buf());

        let mut paths = Vec::new();
        collect_json_files(directory, &mut paths)?;
        paths.sort();
        let mut loaded = 0;
        for path in paths {
            let contents = fs::read_to_string(&path).map_err(|error| EngineError {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                message: format!("could not read {}: {error}", path.display()),
            })?;
            let mut definition: Value =
                serde_json::from_str(&contents).map_err(|error| EngineError {
                    status: StatusCode::BAD_REQUEST,
                    message: format!("invalid machine JSON in {}: {error}", path.display()),
                })?;
            let file_id = path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .ok_or_else(|| {
                    EngineError::bad_request(format!(
                        "machine file has no UTF-8 name: {}",
                        path.display()
                    ))
                })?;
            if let Some(object) = definition.as_object_mut() {
                object.insert("id".to_owned(), json!(canonical_machine_id(file_id)));
            }
            self.load_machine(definition).map_err(|error| EngineError {
                status: error.status,
                message: format!("{}: {}", path.display(), error.message),
            })?;
            loaded += 1;
        }
        Ok(loaded)
    }

    /// `GET /api/machines/json/list`: the corpus as it lies on disk, recursively,
    /// never the resident machines (that is `GET /api/machines`).
    ///
    /// The keys are the three engines'. For `name`, `description`, `version`,
    /// `metadata` and `sequenceCount` this follows C++ and LSP (the file stem
    /// and fixed placeholders); Scala reads them from the file. That split is
    /// RealityEngine_CI#677, and this follows C++ provisionally. No consumer
    /// reads those five; `relFile` is what the corpus-addressing contract and
    /// the OpenClaw corpus check read.
    fn machine_catalog(&self) -> Result<Vec<Value>, EngineError> {
        let Some(directory) = self.machines_dir.as_deref().filter(|d| d.exists()) else {
            return Ok(Vec::new());
        };
        let mut paths = Vec::new();
        collect_json_files(directory, &mut paths)?;
        paths.sort();
        let manifest = semantics_manifest(directory);
        let semantics = manifest.as_ref().and_then(|m| m.get("machines"));
        Ok(paths
            .iter()
            .filter_map(|path| {
                let rel_file = path
                    .strip_prefix(directory)
                    .ok()?
                    .to_str()?
                    .replace('\\', "/");
                let stem = path.file_stem()?.to_str()?;
                let mut row = json!({
                    "filename": path.file_name()?.to_str()?,
                    "relFile": rel_file,
                    "name": stem,
                    "description": "",
                    "version": "1.0.0",
                    "metadata": {},
                    "sequenceCount": 0,
                });
                // Manifest keys are "<domain>/<stem>": domains/<d>/X.json is d/X, else core/X.
                let key = rel_file
                    .strip_prefix("domains/")
                    .map(str::to_owned)
                    .unwrap_or_else(|| format!("core/{rel_file}"));
                let key = key.strip_suffix(".json").unwrap_or(&key);
                if let Some(entry) = semantics.and_then(|s| s.get(key)) {
                    row["semanticsIri"] = entry.get("iri").cloned().unwrap_or(Value::Null);
                    row["semanticsHash"] = entry.get("sha256").cloned().unwrap_or(Value::Null);
                }
                Some(row)
            })
            .collect())
    }

    /// `GET /api/machines/json/:name`: load a corpus file by basename, the flat
    /// path first and then a recursive search (corpus stems are globally
    /// unique). The machine takes its canonical id and replaces any machine
    /// already holding it, as the startup load would have.
    fn load_by_name(&mut self, name: &str) -> Result<Value, EngineError> {
        if name.contains("..") || name.contains('/') || name.contains('\\') {
            return Err(EngineError::bad_request(format!(
                "Invalid machine name: {name}"
            )));
        }
        let filename = if name.ends_with(".json") {
            name.to_owned()
        } else {
            format!("{name}.json")
        };
        let not_found = || EngineError::not_found(format!("Machine file not found: {name}"));
        let directory = self.machines_dir.clone().ok_or_else(not_found)?;
        let flat = directory.join(&filename);
        let path = if flat.is_file() {
            flat
        } else {
            let mut paths = Vec::new();
            collect_json_files(&directory, &mut paths)?;
            paths.sort();
            paths
                .into_iter()
                .find(|p| p.file_name().and_then(|f| f.to_str()) == Some(filename.as_str()))
                .ok_or_else(not_found)?
        };
        let contents = fs::read_to_string(&path).map_err(|_| not_found())?;
        let mut definition: Value = serde_json::from_str(&contents).map_err(|error| {
            EngineError::bad_request(format!("invalid machine JSON in {filename}: {error}"))
        })?;
        let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or(name);
        let id = canonical_machine_id(stem);
        if let Some(object) = definition.as_object_mut() {
            object.insert("id".to_owned(), json!(id));
        }
        // Validate before replacing, so a bad file leaves the resident machine.
        Machine::from_json(definition.clone())?;
        self.machines.remove(&id);
        self.load_machine(definition)
    }

    /// `GET /api/machines`: every resident machine, in canonical order, in
    /// list form.
    pub fn list_machines(&self) -> Vec<Value> {
        self.machines_in_canonical_order()
            .into_iter()
            .map(|machine| machine.to_json(false))
            .collect()
    }

    /// `GET /api/machines/:id`: the machine in full, as it is running.
    pub fn machine(&self, id: &str) -> Option<Value> {
        self.machines
            .get(id)
            .map(|machine| json!({ "machine": machine.to_json(true) }))
    }

    pub fn remove_machine(&mut self, id: &str) -> bool {
        let removed = self.machines.remove(id).is_some();
        if removed {
            self.mapping_version += 1;
        }
        removed
    }

    /// `max(offset + length)` over the input and output regions of every
    /// resident machine: the floor `PUT /api/config/dimension` may not go below.
    fn required_dimension(&self) -> usize {
        self.machines
            .values()
            .map(|machine| {
                let output = machine
                    .raw
                    .get("perceptualMapping")
                    .and_then(|m| m.get("output"));
                let output_end = integer(output.and_then(|r| r.get("offset")), 0)
                    .saturating_add(integer(output.and_then(|r| r.get("length")), 0));
                machine
                    .input_offset
                    .saturating_add(machine.input_length)
                    .max(output_end)
            })
            .max()
            .unwrap_or(0)
    }

    /// SURFACE_SPEC.md, "PUT /api/config/dimension is read-only downward":
    /// refused below either bound, naming which one; never clamped.
    fn widen_to(&mut self, requested: usize) -> Result<Value, EngineError> {
        let required = self.required_dimension();
        let current = self.dimension;
        if requested < required.max(current) {
            let why = if requested < required {
                format!("the {required} the resident corpus requires")
            } else {
                format!("the {current} this engine already holds")
            };
            return Err(EngineError::bad_request(format!(
                "dimension {requested} is below {why}; the perceptual space is read-only downward"
            )));
        }
        self.dimension = requested;
        Ok(json!({
            "success": true,
            "dimension": self.dimension,
            "requiredDimension": required,
        }))
    }

    /// SURFACE_SPEC.md, "`/api/engine/config`": every control this engine
    /// carries, sorted by name, each in the five-field shape.
    ///
    /// Only controls this engine acts on are carried: "a control that reports a
    /// value it does not act on is worse than an absent one". All eight
    /// universal controls are acted on.
    fn controls(&self) -> Vec<Value> {
        let control = |name: &str, value: Value, default: Value| json!({ "name": name, "scope": "engine", "value": value, "default": default, "mutable": true });
        let c = &self.steps.controls;
        let inhibited: serde_json::Map<String, Value> = self
            .machines
            .values()
            .map(|machine| (machine.id.clone(), json!(machine.transitions_inhibited)))
            .collect();
        vec![
            control(
                "arbitrationRetention",
                json!(c.arbitration_retention),
                json!(false),
            ),
            control(
                "arbitrationWindow",
                json!(c.arbitration_window),
                json!(arbitration::WINDOW_DEFAULT),
            ),
            control(
                "historyLimit",
                json!(c.history_limit),
                json!(step::HISTORY_LIMIT_DEFAULT),
            ),
            control(
                "includeActiveRegions",
                json!(c.include_active_regions),
                json!(true),
            ),
            control(
                "includeMachineResults",
                json!(c.include_machine_results),
                json!(true),
            ),
            control(
                "includePerceptualSpace",
                json!(c.include_perceptual_space),
                json!(true),
            ),
            control("phaseDetail", json!(c.phase_detail), json!(false)),
            json!({
                "name": "transitionsInhibited",
                "scope": "machine",
                "value": inhibited,
                "default": false,
                "mutable": true,
            }),
        ]
    }

    fn control(&self, name: &str) -> Result<Value, EngineError> {
        self.controls()
            .into_iter()
            .find(|c| c["name"] == name)
            .ok_or_else(|| EngineError::not_found(format!("Unknown control: {name}")))
    }

    /// `PUT /api/engine/config/:control`: a write that lands nowhere is an
    /// error, never a 200. For the machine-scoped control a body naming a
    /// machine sets that machine (404 if absent); one omitting it sets every
    /// machine, as C++ does.
    fn set_control(&mut self, name: &str, body: &Value) -> Result<Value, EngineError> {
        self.control(name)?;
        let value = &body["value"];
        let boolean = || {
            value.as_bool().ok_or_else(|| {
                EngineError::bad_request(format!("{name} requires a boolean `value`"))
            })
        };
        let controls = &mut self.steps.controls;
        match name {
            "arbitrationRetention" => {
                // Off is the legacy escape: it drops what was kept, restoring
                // the memory profile as well as the shape. On retains from the
                // next committed step.
                controls.arbitration_retention = boolean()?;
                if !controls.arbitration_retention {
                    self.steps.retained.clear();
                }
            }
            "arbitrationWindow" => {
                let window = value
                    .as_f64()
                    .filter(|n| {
                        *n >= 0.0 && *n <= arbitration::WINDOW_MAX as f64 && n.fract() == 0.0
                    })
                    .ok_or_else(|| {
                        EngineError::bad_request(format!(
                            "arbitrationWindow requires a whole-number `value` in [0, {}]",
                            arbitration::WINDOW_MAX
                        ))
                    })?;
                // Narrowing evicts at once.
                controls.arbitration_window = window as usize;
                let latest = self.steps.completed();
                self.steps.prune_arbitration(latest);
            }
            "historyLimit" => {
                // Refused rather than cast: -1 as a size bounds nothing.
                let limit = value
                    .as_f64()
                    .filter(|n| *n >= 0.0 && n.fract() == 0.0)
                    .ok_or_else(|| {
                        EngineError::bad_request(
                            "historyLimit requires a non-negative whole-number `value`",
                        )
                    })?;
                controls.history_limit = limit as usize;
                self.steps.trim_history();
            }
            "includeActiveRegions" => controls.include_active_regions = boolean()?,
            "includeMachineResults" => controls.include_machine_results = boolean()?,
            "includePerceptualSpace" => controls.include_perceptual_space = boolean()?,
            "phaseDetail" => controls.phase_detail = boolean()?,
            _ => {
                let value = boolean()?;
                match body["machine"].as_str() {
                    Some(id) => {
                        self.machines
                            .get_mut(id)
                            .ok_or_else(|| {
                                EngineError::not_found(format!("Machine not found: {id}"))
                            })?
                            .transitions_inhibited = value;
                    }
                    None => self
                        .machines
                        .values_mut()
                        .for_each(|m| m.transitions_inhibited = value),
                }
            }
        }
        self.control(name)
    }

    /// `DELETE /api/engine/config/:control`: restore the declared default.
    fn reset_control(&mut self, name: &str) -> Result<Value, EngineError> {
        self.control(name)?;
        let controls = &mut self.steps.controls;
        let defaults = step::StepControls::default();
        match name {
            "arbitrationRetention" => {
                controls.arbitration_retention = defaults.arbitration_retention;
                self.steps.retained.clear();
            }
            "arbitrationWindow" => {
                controls.arbitration_window = defaults.arbitration_window;
                let latest = self.steps.completed();
                self.steps.prune_arbitration(latest);
            }
            "historyLimit" => {
                controls.history_limit = defaults.history_limit;
                self.steps.trim_history();
            }
            "includeActiveRegions" => {
                controls.include_active_regions = defaults.include_active_regions;
            }
            "includeMachineResults" => {
                controls.include_machine_results = defaults.include_machine_results;
            }
            "includePerceptualSpace" => {
                controls.include_perceptual_space = defaults.include_perceptual_space;
            }
            "phaseDetail" => controls.phase_detail = defaults.phase_detail,
            _ => self
                .machines
                .values_mut()
                .for_each(|m| m.transitions_inhibited = false),
        }
        self.control(name)
    }

    /// `GET /api/metrics`: the CES coverage exposition the three engines share
    /// (names, HELP and TYPE text, label order and series order as C++'s
    /// `CesCoverageRegistry::to_prometheus_text`), then the runtime gauges.
    ///
    /// The step phases are present only while `phaseDetail` is on: absent
    /// rather than zero, since zero would claim a measurement.
    /// `re_step_phase_detail` is always present.
    fn metrics_text(&self) -> String {
        use std::fmt::Write;
        let esc = |v: &str| {
            v.replace('\\', "\\\\")
                .replace('"', "\\\"")
                .replace('\n', "\\n")
        };
        let labels = |pairs: &[(&str, &str)]| {
            let mut out = String::from("{runtime=\"rust\"");
            for (k, v) in pairs {
                let _ = write!(out, ",{k}=\"{}\"", esc(v));
            }
            out.push('}');
            out
        };
        let base = "{runtime=\"rust\"}";
        let mut out = String::new();
        let total_sequences: usize = self.machines.values().map(|m| m.sequences.len()).sum();
        let total_events: usize = self
            .machines
            .values()
            .flat_map(|m| m.sequences.values())
            .map(|s| s.events.len())
            .sum();
        let gauge = |out: &mut String, name: &str, help: &str, value: String| {
            let _ = write!(
                out,
                "# HELP {name} {help}\n# TYPE {name} gauge\n{name}{base} {value}\n"
            );
        };
        gauge(
            &mut out,
            "ces_machines_total",
            "Number of machines loaded into the reality engine.",
            self.machines.len().to_string(),
        );
        gauge(
            &mut out,
            "ces_sequences_total",
            "Number of sequences across all registered machines.",
            total_sequences.to_string(),
        );
        gauge(
            &mut out,
            "ces_vectors_total",
            "Number of event vectors across all registered machines.",
            total_events.to_string(),
        );

        let c = &self.coverage;
        out.push_str("# HELP ces_vector_matched_total Number of times a vector matched its input during a transition phase.\n# TYPE ces_vector_matched_total counter\n");
        for ((id, name, seq, ev), n) in &c.matched {
            let _ = writeln!(
                out,
                "ces_vector_matched_total{} {n}",
                labels(&[
                    ("machine", name),
                    ("machine_id", id),
                    ("sequence", seq),
                    ("vector", ev)
                ])
            );
        }
        out.push_str("# HELP ces_vector_activated_total Number of times a vector was activated as a successor in a transition.\n# TYPE ces_vector_activated_total counter\n");
        for ((id, name, seq, ev), n) in &c.activated {
            let _ = writeln!(
                out,
                "ces_vector_activated_total{} {n}",
                labels(&[
                    ("machine", name),
                    ("machine_id", id),
                    ("sequence", seq),
                    ("vector", ev)
                ])
            );
        }
        out.push_str("# HELP ces_sequence_outputs_total Number of asserted outputs emitted by a sequence.\n# TYPE ces_sequence_outputs_total counter\n");
        for ((id, name, seq), n) in &c.outputs {
            let _ = writeln!(
                out,
                "ces_sequence_outputs_total{} {n}",
                labels(&[("machine", name), ("machine_id", id), ("sequence", seq)])
            );
        }
        out.push_str("# HELP ces_machine_steps_total Number of process_input calls observed for this machine.\n# TYPE ces_machine_steps_total counter\n");
        for ((id, name), n) in &c.steps {
            let _ = writeln!(
                out,
                "ces_machine_steps_total{} {n}",
                labels(&[("machine", name), ("machine_id", id)])
            );
        }
        out.push_str("# HELP ces_paging_decisions_total Number of governance-resolved paging decisions issued by the engine.\n# TYPE ces_paging_decisions_total counter\n");
        for ((team, status, rag, id), n) in &c.paging {
            let name = self.machines.get(id).map_or("", |m| m.name.as_str());
            let _ = writeln!(
                out,
                "ces_paging_decisions_total{} {n}",
                labels(&[
                    ("owner_team", team),
                    ("process_status", status),
                    ("rag_status_code", rag),
                    ("machine_id", id),
                    ("machine", name)
                ])
            );
        }
        out.push_str("# HELP ces_deprecated_fires_total Number of times a deprecated sequence emitted output.\n# TYPE ces_deprecated_fires_total counter\n");
        for ((id, name, seq, replaced_by), n) in &c.deprecated {
            let _ = writeln!(
                out,
                "ces_deprecated_fires_total{} {n}",
                labels(&[
                    ("machine", name),
                    ("machine_id", id),
                    ("sequence", seq),
                    ("replaced_by", replaced_by)
                ])
            );
        }
        // Zero baselines, so a dashboard can plot every machine before it fires.
        for m in self.machines.values() {
            let l = labels(&[("machine", &m.name), ("machine_id", &m.id)]);
            let _ = writeln!(out, "ces_vector_matched_total{l} 0");
            let _ = writeln!(out, "ces_vector_activated_total{l} 0");
            let _ = writeln!(out, "ces_sequence_outputs_total{l} 0");
            let _ = writeln!(out, "ces_deprecated_fires_total{l} 0");
            if !c.steps.keys().any(|(id, _)| id == &m.id) {
                let _ = writeln!(out, "ces_machine_steps_total{l} 0");
            }
        }

        let per_machine = |out: &mut String,
                           name: &str,
                           help: &str,
                           kind: &str,
                           value: &dyn Fn(&Machine) -> usize| {
            let _ = write!(out, "# HELP {name} {help}\n# TYPE {name} {kind}\n");
            for m in self.machines.values() {
                let _ = writeln!(
                    out,
                    "{name}{} {}",
                    labels(&[("machine", &m.name), ("machine_id", &m.id)]),
                    value(m)
                );
            }
        };
        per_machine(
            &mut out,
            "ces_unfired_sequences",
            "Number of sequences in this machine that have never emitted output.",
            "gauge",
            &|m| {
                m.sequences
                    .keys()
                    .filter(|s| {
                        !c.outputs
                            .contains_key(&(m.id.clone(), m.name.clone(), (*s).clone()))
                    })
                    .count()
            },
        );
        per_machine(
            &mut out,
            "ces_unfired_vectors",
            "Number of vectors in this machine that have never matched or activated.",
            "gauge",
            &|m| {
                m.sequences
                    .values()
                    .flat_map(|s| s.events.keys().map(move |e| (s.id.clone(), e.clone())))
                    .filter(|(s, e)| {
                        let key = (m.id.clone(), m.name.clone(), s.clone(), e.clone());
                        !c.matched.contains_key(&key) && !c.activated.contains_key(&key)
                    })
                    .count()
            },
        );
        per_machine(
            &mut out,
            "ces_machine_sequence_count",
            "Total sequences declared by this machine.",
            "gauge",
            &|m| m.sequences.len(),
        );
        per_machine(
            &mut out,
            "ces_machine_vector_count",
            "Total vectors declared by this machine.",
            "gauge",
            &|m| m.sequences.values().map(|s| s.events.len()).sum(),
        );
        let uptime = now_ms().saturating_sub(c.started_at_ms) as f64 / 1000.0;
        gauge(
            &mut out,
            "ces_registry_uptime_seconds",
            "Seconds since the coverage registry was instantiated.",
            uptime.to_string(),
        );

        gauge(
            &mut out,
            "re_runtime_dimension",
            "Current dimension of the shared perceptual space.",
            self.dimension.to_string(),
        );
        gauge(
            &mut out,
            "re_runtime_required_dimension",
            "Max(offset+length) across all registered machine mappings.",
            self.required_dimension().to_string(),
        );
        gauge(
            &mut out,
            "re_runtime_mapping_version",
            "Monotonic version bumped on every add/remove_machine.",
            self.mapping_version.to_string(),
        );
        let controls = &self.steps.controls;
        if controls.phase_detail {
            out.push_str("# HELP re_step_phase_seconds_total Cumulative wall clock per phase of a step.\n# TYPE re_step_phase_seconds_total counter\n");
            for (phase, spent) in step::STEP_PHASES.iter().zip(controls.phase_time) {
                let _ = writeln!(
                    out,
                    "re_step_phase_seconds_total{} {:.9}",
                    labels(&[("phase", phase)]),
                    spent.as_secs_f64()
                );
            }
            out.push_str("# HELP re_step_phase_detail_steps_total Steps the universal step phases were measured over.\n# TYPE re_step_phase_detail_steps_total counter\n");
            let _ = writeln!(
                out,
                "re_step_phase_detail_steps_total{base} {}",
                controls.measured_steps
            );
        }
        gauge(
            &mut out,
            "re_step_phase_detail",
            "Whether step phase timing (phaseDetail) is on.",
            u8::from(controls.phase_detail).to_string(),
        );
        out
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

    /// SURFACE_SPEC.md, "The instance clock": `{instance, lamport, step}` for
    /// the newest committed step (and, with `arbitration::clock_json`, for
    /// each retained step). Only the step path (`POST /api/perceive`)
    /// commits a step, as in C++ (`onStepCommitted`); the process routes do not.
    fn clock(&self) -> Value {
        json!({
            "instance": self.clock.instance(),
            "lamport": self.clock.lamport(),
            "step": self.steps.completed(),
        })
    }

    fn record_history(&mut self, result: Value) {
        const HISTORY_LIMIT: usize = 256;
        if self.history.len() == HISTORY_LIMIT {
            self.history.pop_front();
        }
        self.history.push_back(result);
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
        let mut result = machine.present(input, &mut self.coverage);
        self.record_history(result.clone());
        // The transition response carries no machineId, as C++ and LSP answer
        // the single-machine routes: the caller named the machine.
        if let Some(object) = result.as_object_mut() {
            object.remove("machineId");
        }
        Ok(result)
    }

    pub fn process_all(&mut self, input: &[f64]) -> Value {
        self.step += 1;
        let timestamp = now_ms();
        let universal = self.dimension > 0 && input.len() == self.dimension;
        let coverage = &mut self.coverage;
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
                // SURFACE_SPEC.md, "POST /api/engine/process": outputs =
                // [r.machineOutput for r in results if r.machineOutput]. One
                // arbitrated output per machine that fired, the output itself;
                // the per-machine detail stays on POST /api/machines/:id/process.
                let mut result = machine.present(machine_input, coverage);
                Some(result.get_mut("machineOutput")?.take()).filter(|output| !output.is_null())
            })
            .collect();
        let result = json!({
            "inputEvent": input,
            "timestamp": timestamp,
            "outputs": outputs,
        });
        self.record_history(result.clone());
        result
    }

    pub fn reset(&mut self, clear_audit: bool) {
        self.reset_space();
        self.step = 0;
        if clear_audit {
            self.history.clear();
        }
    }

    /// Every machine back to its initial Reality Events, and the step state
    /// to its start (C++ `PerceptualSpaceRuntime::reset`).
    fn reset_space(&mut self) {
        for machine in self.machines.values_mut() {
            machine.reset();
        }
        self.steps.reset();
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

fn json_rejection(error: JsonRejection) -> (StatusCode, Json<Value>) {
    error_response(EngineError::bad_request(error.to_string()))
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
    // SURFACE_SPEC.md, "GET /api/config": exactly these four keys.
    // `eventDimension` is the width actually held. The Qdrant keys name the
    // store this engine will use (owner decision, RealityEngine_Rust#6).
    Ok(Json(json!({
        "eventDimension": state.dimension,
        "matchThreshold": state.threshold,
        "qdrantUrl": state.qdrant_url,
        "collectionName": state.collection_name,
    })))
}

async fn put_dimension(
    State(engine): State<SharedEngine>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let requested = query
        .get("dimension")
        .filter(|raw| !raw.is_empty())
        .ok_or_else(|| {
            error_response(EngineError::bad_request(
                "dimension query parameter is required",
            ))
        })?
        .parse::<usize>()
        .map_err(|_| error_response(EngineError::bad_request("dimension must be an integer")))?;
    let mut state = engine.write().map_err(|_| error_response(lock_error()))?;
    state.widen_to(requested).map(Json).map_err(error_response)
}

/// SURFACE_SPEC.md, "PUT /api/config/threshold" (owner ruling on
/// RealityEngine_CI#674): a finite number is stored and reported by
/// `GET /api/config`; an absent parameter changes nothing; anything else is a
/// 400. No engine uses the value when matching; it is held to become a learning
/// knob (RealityEngine_CI#675).
async fn put_threshold(
    State(engine): State<SharedEngine>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let requested = match query.get("threshold").filter(|raw| !raw.is_empty()) {
        Some(raw) => Some(
            raw.parse::<f64>()
                .ok()
                .filter(|t| t.is_finite())
                .ok_or_else(|| {
                    error_response(EngineError::bad_request("threshold must be a number"))
                })?,
        ),
        None => None,
    };
    let mut state = engine.write().map_err(|_| error_response(lock_error()))?;
    if let Some(threshold) = requested {
        state.threshold = threshold;
    }
    Ok(Json(
        json!({ "success": true, "threshold": state.threshold }),
    ))
}

async fn engine_config(
    State(engine): State<SharedEngine>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let state = engine.read().map_err(|_| error_response(lock_error()))?;
    Ok(Json(json!({ "controls": state.controls() })))
}

async fn engine_control(
    State(engine): State<SharedEngine>,
    Path(name): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let state = engine.read().map_err(|_| error_response(lock_error()))?;
    state.control(&name).map(Json).map_err(error_response)
}

async fn put_engine_control(
    State(engine): State<SharedEngine>,
    Path(name): Path<String>,
    body: Result<Json<Value>, JsonRejection>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let Json(body) = body.map_err(json_rejection)?;
    let mut state = engine.write().map_err(|_| error_response(lock_error()))?;
    state
        .set_control(&name, &body)
        .map(Json)
        .map_err(error_response)
}

async fn delete_engine_control(
    State(engine): State<SharedEngine>,
    Path(name): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let mut state = engine.write().map_err(|_| error_response(lock_error()))?;
    state.reset_control(&name).map(Json).map_err(error_response)
}

async fn metrics(
    State(engine): State<SharedEngine>,
) -> Result<([(axum::http::HeaderName, &'static str); 1], String), (StatusCode, Json<Value>)> {
    let state = engine.read().map_err(|_| error_response(lock_error()))?;
    Ok((
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        state.metrics_text(),
    ))
}

async fn engine_stats(
    State(engine): State<SharedEngine>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let state = engine.read().map_err(|_| error_response(lock_error()))?;
    Ok(Json(state.stats()))
}

async fn engine_clock(
    State(engine): State<SharedEngine>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let state = engine.read().map_err(|_| error_response(lock_error()))?;
    Ok(Json(state.clock()))
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

async fn machine_catalog(
    State(engine): State<SharedEngine>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let state = engine.read().map_err(|_| error_response(lock_error()))?;
    let machines = state.machine_catalog().map_err(error_response)?;
    Ok(Json(json!({ "machines": machines })))
}

async fn load_machine_by_name(
    State(engine): State<SharedEngine>,
    Path(name): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let mut state = engine.write().map_err(|_| error_response(lock_error()))?;
    state
        .load_by_name(&name)
        .map(|machine| {
            Json(json!({
                "success": true,
                "machine": machine,
                "message": "Machine loaded successfully",
            }))
        })
        .map_err(error_response)
}

async fn add_machine(
    State(engine): State<SharedEngine>,
    input: Result<Json<Value>, JsonRejection>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let Json(input) = input.map_err(json_rejection)?;
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
        .map(Json)
        .ok_or_else(|| error_response(EngineError::not_found("Machine not found")))
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

/// Reads the input under the one key the route takes, as C++, LSP and Scala
/// do: `inputEvent` on the single-machine transition routes, `vector` on
/// `POST /api/engine/process`.
fn input_from_body(body: &Value, key: &str) -> Result<Vec<f64>, EngineError> {
    let vector = body
        .get(key)
        .ok_or_else(|| EngineError::bad_request(format!("request must include {key}")))?;
    let values = vector
        .as_array()
        .ok_or_else(|| EngineError::bad_request(format!("{key} must be an array")))?;
    values
        .iter()
        .map(|value| {
            value.as_f64().ok_or_else(|| {
                EngineError::bad_request(format!("{key} values must be finite numbers"))
            })
        })
        .collect()
}

async fn process_machine(
    State(engine): State<SharedEngine>,
    Path(id): Path<String>,
    body: Result<Json<Value>, JsonRejection>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let Json(body) = body.map_err(json_rejection)?;
    let input = input_from_body(&body, "inputEvent").map_err(error_response)?;
    let mut state = engine.write().map_err(|_| error_response(lock_error()))?;
    // The transition result itself, at the top level, as all three engines answer.
    state
        .process_machine(&id, &input)
        .map(Json)
        .map_err(error_response)
}

async fn process_all(
    State(engine): State<SharedEngine>,
    body: Result<Json<Value>, JsonRejection>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let Json(body) = body.map_err(json_rejection)?;
    let input = input_from_body(&body, "vector").map_err(error_response)?;
    let mut state = engine.write().map_err(|_| error_response(lock_error()))?;
    Ok(Json(json!({ "result": state.process_all(&input) })))
}

async fn reset(
    State(engine): State<SharedEngine>,
    Query(query): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let body: Value = if body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&body).map_err(|error| {
            error_response(EngineError::bad_request(format!(
                "invalid JSON body: {error}"
            )))
        })?
    };
    let clear_audit = query
        .get("clearAudit")
        .is_some_and(|value| value.eq_ignore_ascii_case("true"))
        || body
            .get("clearAudit")
            .and_then(Value::as_bool)
            .unwrap_or(false);
    let mut state = engine.write().map_err(|_| error_response(lock_error()))?;
    state.reset(clear_audit);
    Ok(Json(json!({ "success": true, "message": "engine reset" })))
}

pub fn app(engine: Engine) -> Router {
    let state = Arc::new(RwLock::new(engine));
    Router::new()
        .route("/", get(root))
        .route("/api", get(root))
        .route("/api/health", get(health))
        .route("/api/config", get(config))
        .route("/api/config/dimension", put(put_dimension))
        .route("/api/config/threshold", put(put_threshold))
        .route("/api/machines", get(list_machines).post(add_machine))
        .route("/api/machines/json/list", get(machine_catalog))
        .route("/api/machines/json/{name}", get(load_machine_by_name))
        .route(
            "/api/machines/{id}",
            get(get_machine)
                .put(machine_routes::replace)
                .patch(machine_routes::patch)
                .delete(delete_machine),
        )
        .route("/api/machines/{id}/process", post(process_machine))
        .route("/api/machines/{id}/export", get(machine_view::export))
        .route(
            "/api/machines/{id}/process-universal",
            post(machine_routes::process_universal),
        )
        .route(
            "/api/machines/process-universal/all",
            post(machine_routes::process_universal_all),
        )
        .route("/api/machines/{id}/whatif", post(machine_routes::whatif))
        .route(
            "/api/machines/{id}/whatif-universal",
            post(machine_routes::whatif_universal),
        )
        .route("/api/machines/json/import", post(machine_routes::import))
        .route(
            "/api/machines/{id}/checkpoints",
            get(machine_routes::checkpoints).post(machine_routes::checkpoint),
        )
        .route(
            "/api/machines/{machine}/checkpoints/{checkpoint}/restore",
            post(machine_routes::restore),
        )
        .route(
            "/api/machines/{machine}/checkpoints/{checkpoint}",
            delete(machine_routes::delete_checkpoint),
        )
        .route("/api/machine-graph", get(machine_view::machine_graph))
        .route("/api/vectors", post(sequences::store_vector))
        .route("/api/vectors/search", post(sequences::search_vectors))
        .route(
            "/api/vectors/{id}",
            get(sequences::vector).delete(sequences::delete_vector),
        )
        .route(
            "/api/sequences",
            get(sequences::list).post(sequences::create),
        )
        .route("/api/sequences/persist", post(sequences::persist))
        .route(
            "/api/sequences/{id}",
            get(sequences::sequence).delete(sequences::delete_sequence),
        )
        .route("/api/sequences/{id}/reset", post(sequences::reset_sequence))
        .route("/api/sequences/{id}/vectors", post(sequences::add_event))
        .route("/api/buses/semantic", get(machine_view::semantic_buses))
        .route("/api/buses/semantic/{id}", get(machine_view::semantic_bus))
        .route("/api/engine/process", post(process_all))
        .route("/api/engine/stats", get(engine_stats))
        .route("/api/engine/config", get(engine_config))
        .route(
            "/api/engine/config/{control}",
            get(engine_control)
                .put(put_engine_control)
                .delete(delete_engine_control),
        )
        .route("/api/metrics", get(metrics))
        .route("/api/engine/history", get(engine_history))
        .route("/api/engine/clock", get(engine_clock))
        .route("/api/arbitration", get(arbitration::arbitration))
        .route("/api/perceive", post(step::perceive))
        .route("/api/engine/isre-history", get(step::isre_history))
        .route("/api/engine/osre-history", get(step::osre_history))
        .route("/api/engine/steps/{n}/pair", get(step::step_pair))
        .route("/api/engine/stream", get(simulation::stream))
        .route("/api/engine/active", get(simulation::active))
        .route(
            "/api/perceptual-simulation/configure/chunk",
            post(simulation::configure_chunk),
        )
        .route(
            "/api/perceptual-simulation/configure/commit",
            post(simulation::configure_commit),
        )
        .route("/api/perceptual-simulation/start", post(simulation::start))
        .route("/api/perceptual-simulation/stop", post(simulation::stop))
        .route("/api/perceptual-simulation/step", post(simulation::step))
        .route("/api/perceptual-simulation/reset", post(simulation::reset))
        .route("/api/perceptual-simulation/state", get(simulation::state))
        .route(
            "/api/perceptual-simulation/history",
            get(simulation::history),
        )
        .route("/api/engine/reset", post(reset))
        .fallback(|method: Method, uri: Uri| async move {
            unsupported_or(&method, &uri, StatusCode::NOT_FOUND)
        })
        .method_not_allowed_fallback(|method: Method, uri: Uri| async move {
            unsupported_or(&method, &uri, StatusCode::METHOD_NOT_ALLOWED)
        })
        .layer(axum::middleware::map_response(ecmascript_numbers))
        .with_state(state)
}

/// Every JSON body leaves with its numbers in ECMAScript `Number::toString`
/// form, which is `JSON.stringify`'s and what the three engines emit (C++
/// `reality::json::format_double`, RealityEngine_CI#91): `1`, not serde_json's
/// `1.0`; `0.0000015`, not `1.5e-6`; `1e+21`, not `1e21`. One rule for every
/// route, applied here rather than at each value (#33).
async fn ecmascript_numbers(response: Response) -> Response {
    let is_json = response
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("application/json"));
    if !is_json {
        return response;
    }
    let (mut parts, body) = response.into_parts();
    let Ok(bytes) = axum::body::to_bytes(body, usize::MAX).await else {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "response body unreadable",
        )
            .into_response();
    };
    let rewritten = serde_json::from_slice::<Value>(&bytes)
        .ok()
        .and_then(|value| ecmascript_json(&value).ok())
        .unwrap_or_else(|| bytes.to_vec());
    parts.headers.remove(axum::http::header::CONTENT_LENGTH);
    Response::from_parts(parts, axum::body::Body::from(rewritten))
}

/// `value` serialized with ECMAScript number formatting.
pub fn ecmascript_json(value: &Value) -> serde_json::Result<Vec<u8>> {
    use serde::Serialize;
    let mut out = Vec::new();
    value.serialize(&mut serde_json::Serializer::with_formatter(
        &mut out,
        EcmaFormatter,
    ))?;
    Ok(out)
}

struct EcmaFormatter;

impl serde_json::ser::Formatter for EcmaFormatter {
    fn write_f64<W: ?Sized + std::io::Write>(
        &mut self,
        writer: &mut W,
        value: f64,
    ) -> std::io::Result<()> {
        writer.write_all(ecmascript_number(value).as_bytes())
    }

    fn write_f32<W: ?Sized + std::io::Write>(
        &mut self,
        writer: &mut W,
        value: f32,
    ) -> std::io::Result<()> {
        writer.write_all(ecmascript_number(f64::from(value)).as_bytes())
    }
}

/// ECMA-262 `Number::toString` for a finite double (6.1.6.1.20): the shortest
/// round-trip digits `s` (k of them) with the decimal point at `n`; integer form
/// up to 1e21, plain decimals down to 1e-6, exponential outside. A non-finite
/// value is `null`, as `JSON.stringify` writes it.
pub fn ecmascript_number(value: f64) -> String {
    if !value.is_finite() {
        return "null".to_owned();
    }
    if value == 0.0 {
        return "0".to_owned(); // -0 too
    }
    // `{:e}` is Rust's shortest round-trip form: `1.5e-6`, `1e21`.
    let scientific = format!("{:e}", value.abs());
    let (mantissa, exponent) = scientific.split_once('e').expect("{:e} has an exponent");
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    let k = digits.len() as i64;
    let n = exponent
        .parse::<i64>()
        .expect("{:e} exponent is an integer")
        + 1;
    let body = if k <= n && n <= 21 {
        format!("{digits}{}", "0".repeat((n - k) as usize))
    } else if 0 < n && n <= 21 {
        format!("{}.{}", &digits[..n as usize], &digits[n as usize..])
    } else if -6 < n && n <= 0 {
        format!("0.{}{digits}", "0".repeat((-n) as usize))
    } else {
        let sign = if n - 1 < 0 { '-' } else { '+' };
        let e = (n - 1).abs();
        if k == 1 {
            format!("{digits}e{sign}{e}")
        } else {
            format!("{}.{}e{sign}{e}", &digits[..1], &digits[1..])
        }
    };
    if value < 0.0 {
        format!("-{body}")
    } else {
        body
    }
}

/// SURFACE_SPEC.md's route tables, vendored by `scripts/refresh-surface.sh` and
/// pinned to the spec's commit: the routes this engine is answerable for.
const SPEC_ROUTES: &str = include_str!("../conformance/surface/spec-routes.json");

/// The Reality Engine's specified routes as `(method, path)`, the path in the
/// spec's own form (`:param` placeholders). The PE's are not this process's to
/// answer.
fn specified_re_routes() -> &'static [(String, String)] {
    static ROUTES: std::sync::OnceLock<Vec<(String, String)>> = std::sync::OnceLock::new();
    ROUTES.get_or_init(|| {
        let spec: Value = serde_json::from_str(SPEC_ROUTES).expect("vendored spec-routes.json");
        spec["routes"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|route| matches!(route["layer"].as_str(), Some("RE" | "RE-phase2")))
            .filter_map(|route| {
                Some((
                    route["method"].as_str()?.to_owned(),
                    route["path"].as_str()?.to_owned(),
                ))
            })
            .collect()
    })
}

/// The specified route a request this router does not serve was addressed to,
/// as `"<METHOD> <path>"`; the most literal one when several templates match.
/// `SSE` and `WS` routes are reached with `GET`.
fn specified_surface(method: &Method, path: &str) -> Option<String> {
    let requested: Vec<&str> = path.split('/').collect();
    specified_re_routes()
        .iter()
        .filter(|(spec_method, _)| {
            spec_method == method.as_str()
                || (method == Method::GET && matches!(spec_method.as_str(), "SSE" | "WS"))
        })
        .filter_map(|(spec_method, template)| {
            let segments: Vec<&str> = template.split('/').collect();
            let matches = segments.len() == requested.len()
                && segments
                    .iter()
                    .zip(&requested)
                    .all(|(t, r)| (t.starts_with(':') && !r.is_empty()) || t == r);
            let literal = segments.iter().filter(|t| !t.starts_with(':')).count();
            matches.then(|| (literal, format!("{spec_method} {template}")))
        })
        .max_by_key(|(literal, _)| *literal)
        .map(|(_, surface)| surface)
}

/// SURFACE_SPEC.md, "An unimplemented route answers `unsupported`": a specified
/// route this engine does not implement answers 501 with the participation
/// state, never silence. Anything else keeps the router's own `otherwise`.
fn unsupported_or(method: &Method, uri: &Uri, otherwise: StatusCode) -> Response {
    match specified_surface(method, uri.path()) {
        Some(surface) => (
            StatusCode::NOT_IMPLEMENTED,
            Json(json!({
                "error": format!("{surface} is not implemented by this runtime"),
                "participation": "unsupported",
                "surface": surface,
            })),
        )
            .into_response(),
        None => otherwise.into_response(),
    }
}

pub fn engine_from_environment() -> Result<Engine, EngineError> {
    let dimension = std::env::var("VECTOR_DIMENSION")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(7680);
    // Before the corpus loads, so an allocated instance that cannot keep its
    // clock, or whose UUID another live process holds, fails at once rather
    // than after the load (RealityEngine_CI#296).
    let clock = InstanceClock::boot().map_err(|message| EngineError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message,
    })?;
    eprintln!(
        "reality-engine: instance {}, lamport {}",
        clock.instance(),
        clock.lamport()
    );
    let mut engine = Engine::new(dimension);
    engine.clock = clock;
    // SURFACE_SPEC.md, "GET /api/config": the Qdrant store's address and
    // collection, as C++, LSP and Scala report them.
    if let Ok(url) = std::env::var("QDRANT_URL") {
        engine.qdrant_url = url;
    }
    if let Ok(name) = std::env::var("COLLECTION_NAME") {
        engine.collection_name = name;
    }
    let machine_directory = std::env::var("MACHINES_DIR")
        .unwrap_or_else(|_| "../RealityEngine_Machines/machines".to_owned());
    let loaded = engine.load_directory(FsPath::new(&machine_directory))?;
    engine.arbitration = step::ArbitrationRegistry::load(FsPath::new(&machine_directory));
    // The corpus-load count is the check that catches a misread key
    // (SURFACE_SPEC.md, "Reality Event key names"); report it at every start.
    let events: usize = engine
        .machines
        .values()
        .flat_map(|machine| machine.sequences.values())
        .map(|sequence| sequence.events.len())
        .sum();
    eprintln!(
        "reality-engine: loaded {loaded} machines, {events} events from {machine_directory}; eventDimension {}",
        engine.dimension
    );
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
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use tower::ServiceExt;

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
                    "events": [
                        {
                            "id": "start",
                            "isInitial": true,
                            "elements": [{ "value": 1, "comparatorType": "equals" }],
                            "nextEventIds": ["finish"]
                        },
                        {
                            "id": "finish",
                            "elements": [{ "value": 0, "comparatorType": "equals" }],
                            "outputEvents": [{ "id": "out", "vector": [1] }]
                        }
                    ]
                }]
            }
        })
    }

    #[test]
    fn event_successors_activate_after_current_transition_and_emit_next_cycle() {
        let mut engine = Engine::new(8);
        let machine_id = engine.load_machine(machine_definition()).unwrap()["id"]
            .as_str()
            .unwrap()
            .to_owned();

        let first = engine.process_machine(&machine_id, &[1.0]).unwrap();
        assert_eq!(
            first["sequenceResults"]["sequence"]["matchedEvents"][0],
            "start"
        );
        assert_eq!(
            first["sequenceResults"]["sequence"]["activatedEvents"][0],
            "finish"
        );
        assert!(first["machineOutput"].is_null());

        let second = engine.process_machine(&machine_id, &[0.0]).unwrap();
        assert_eq!(second["machineOutput"]["vector"][0], 1.0);
        assert_eq!(second["mergedOutput"][0], 1.0);
    }

    #[test]
    fn reset_restores_initial_events_and_clears_match_state() {
        let mut engine = Engine::new(8);
        let machine_id = engine.load_machine(machine_definition()).unwrap()["id"]
            .as_str()
            .unwrap()
            .to_owned();
        engine.process_machine(&machine_id, &[1.0]).unwrap();
        engine.process_machine(&machine_id, &[0.0]).unwrap();
        engine.reset(false);
        assert_eq!(engine.history.len(), 2);
        let result = engine.process_machine(&machine_id, &[1.0]).unwrap();
        assert_eq!(
            result["sequenceResults"]["sequence"]["matchedEvents"][0],
            "start"
        );
        engine.reset(true);
        assert!(engine.history.is_empty());
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

    /// SURFACE_SPEC.md, "POST /api/engine/process": outputs[] holds each firing
    /// machine's arbitrated output itself, and nothing for a machine that did not fire.
    #[tokio::test]
    async fn engine_process_outputs_are_the_arbitrated_outputs() {
        let app = app(Engine::new(8));
        let post = |uri: &str, body: Value| {
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap()
        };
        let response = app
            .clone()
            .oneshot(post("/api/machines", machine_definition()))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let mut outputs = Vec::new();
        for first_cell in [1.0, 0.0] {
            let mut universal = vec![0.0; 8];
            universal[0] = first_cell;
            let response = app
                .clone()
                .oneshot(post("/api/engine/process", json!({ "vector": universal })))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let body: Value = serde_json::from_slice(&body).unwrap();
            outputs.push(body["result"]["outputs"].clone());
        }

        assert_eq!(
            outputs[0],
            json!([]),
            "a machine that did not fire contributes nothing"
        );
        let fired = outputs[1].as_array().unwrap();
        assert_eq!(fired.len(), 1);
        let output = &fired[0];
        assert_eq!(output["vector"], json!([1]));
        assert_eq!(output["metadata"]["sources"], json!(["out"]));
        assert!(output["provenance"].is_array(), "{output}");
        for wrapper_key in [
            "machineId",
            "machineOutput",
            "mergedOutput",
            "sequenceResults",
        ] {
            assert!(
                output.get(wrapper_key).is_none(),
                "{wrapper_key} in {output}"
            );
        }
    }

    async fn send(
        app: &Router,
        method: &str,
        uri: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let request = Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .body(body.map_or_else(Body::empty, |b| Body::from(b.to_string())))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    fn loaded_app() -> (Router, String) {
        let mut engine = Engine::new(8);
        let id = engine.load_machine(machine_definition()).unwrap()["id"]
            .as_str()
            .unwrap()
            .to_owned();
        (app(engine), id)
    }

    /// SURFACE_SPEC.md, "`/api/engine/config`": the five-field shape, sorted by
    /// name, with only the controls this engine acts on.
    #[tokio::test]
    async fn engine_config_carries_the_controls_this_engine_acts_on() {
        let (app, id) = loaded_app();
        let (status, body) = send(&app, "GET", "/api/engine/config", None).await;
        assert_eq!(status, StatusCode::OK);
        let engine = |name: &str, value: Value| json!({ "name": name, "scope": "engine", "value": value, "default": value, "mutable": true });
        assert_eq!(
            body,
            json!({ "controls": [
                engine("arbitrationRetention", json!(false)),
                engine("arbitrationWindow", json!(1)),
                engine("historyLimit", json!(250)),
                engine("includeActiveRegions", json!(true)),
                engine("includeMachineResults", json!(true)),
                engine("includePerceptualSpace", json!(true)),
                engine("phaseDetail", json!(false)),
                {
                    "name": "transitionsInhibited",
                    "scope": "machine",
                    "value": { id.clone(): false },
                    "default": false,
                    "mutable": true,
                },
            ] })
        );
        // Byte order matches C++ (std::map keys): default, mutable, name, scope, value.
        let request = Request::builder()
            .uri("/api/engine/config/transitionsInhibited")
            .body(Body::empty())
            .unwrap();
        let bytes = to_bytes(
            app.clone().oneshot(request).await.unwrap().into_body(),
            usize::MAX,
        )
        .await
        .unwrap();
        assert!(
            String::from_utf8(bytes.to_vec()).unwrap().starts_with(
                "{\"default\":false,\"mutable\":true,\"name\":\"transitionsInhibited\""
            ),
            "field order"
        );
        // A control the specification does not declare does not exist.
        let (status, _) = send(&app, "GET", "/api/engine/config/noSuchControl", None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    /// Each engine control acts: the include defaults shape the next push, a
    /// request's own flag still overrides, historyLimit bounds the step
    /// history, and a bad value is refused rather than stored.
    #[tokio::test]
    async fn the_step_controls_act_and_restore_their_defaults() {
        let (app, _) = loaded_app();
        let config = |name: &str| format!("/api/engine/config/{name}");
        let push = |body: Value| {
            let app = app.clone();
            async move { send(&app, "POST", "/api/perceive", Some(body)).await.1 }
        };
        for name in [
            "includeMachineResults",
            "includePerceptualSpace",
            "includeActiveRegions",
        ] {
            let (status, body) =
                send(&app, "PUT", &config(name), Some(json!({ "value": false }))).await;
            assert_eq!(
                (status, &body["value"]),
                (StatusCode::OK, &json!(false)),
                "{name}"
            );
        }
        let step = push(json!({ "vector": [1] })).await;
        let keys: Vec<&str> = step
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(keys, ["eventBus", "mergeBatch", "stepNumber", "timestamp"]);
        let step = push(json!({ "vector": [1], "includeMachineResults": true })).await;
        assert!(
            step["machineResults"].is_object(),
            "the request overrides the default"
        );
        for name in [
            "includeMachineResults",
            "includePerceptualSpace",
            "includeActiveRegions",
        ] {
            let (_, body) = send(&app, "DELETE", &config(name), None).await;
            assert_eq!(body["value"], json!(true), "{name}");
        }
        assert!(push(json!({ "vector": [1] })).await["activeRegions"].is_array());

        for bad in [json!(-1), json!(1.5), json!("2")] {
            let (status, _) = send(
                &app,
                "PUT",
                &config("historyLimit"),
                Some(json!({ "value": bad })),
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{bad}");
        }
        let (status, _) = send(
            &app,
            "PUT",
            &config("phaseDetail"),
            Some(json!({ "value": 1 })),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        send(
            &app,
            "PUT",
            &config("historyLimit"),
            Some(json!({ "value": 2 })),
        )
        .await;
        let (_, history) = send(&app, "GET", "/api/perceptual-simulation/history", None).await;
        let numbers: Vec<_> = history["history"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["stepNumber"].clone())
            .collect();
        assert_eq!(
            numbers,
            [json!(2), json!(1)],
            "narrowed at once, newest kept"
        );
        let (_, body) = send(&app, "DELETE", &config("historyLimit"), None).await;
        assert_eq!(body["value"], json!(250));
    }

    /// SURFACE_SPEC.md, "`phaseDetail`: step phase timing": absent while off,
    /// one measured step per push while on, counters that survive a reset.
    #[tokio::test]
    async fn phase_detail_times_the_five_step_phases() {
        let (app, _) = loaded_app();
        let metrics = || {
            let app = app.clone();
            async move {
                let request = Request::builder()
                    .uri("/api/metrics")
                    .body(Body::empty())
                    .unwrap();
                let bytes = to_bytes(app.oneshot(request).await.unwrap().into_body(), usize::MAX)
                    .await
                    .unwrap();
                String::from_utf8(bytes.to_vec()).unwrap()
            }
        };
        send(
            &app,
            "POST",
            "/api/perceive",
            Some(json!({ "vector": [1] })),
        )
        .await;
        let text = metrics().await;
        assert!(
            text.contains("re_step_phase_detail{runtime=\"rust\"} 0"),
            "{text}"
        );
        assert!(
            !text.contains("re_step_phase_seconds_total"),
            "absent while off"
        );
        send(
            &app,
            "PUT",
            "/api/engine/config/phaseDetail",
            Some(json!({ "value": true })),
        )
        .await;
        send(
            &app,
            "POST",
            "/api/perceive",
            Some(json!({ "vector": [1] })),
        )
        .await;
        send(&app, "POST", "/api/engine/reset", None).await;
        send(
            &app,
            "POST",
            "/api/perceive",
            Some(json!({ "vector": [0] })),
        )
        .await;
        let text = metrics().await;
        assert!(
            text.contains("re_step_phase_detail{runtime=\"rust\"} 1"),
            "{text}"
        );
        assert!(
            text.contains("re_step_phase_detail_steps_total{runtime=\"rust\"} 2"),
            "{text}"
        );
        for phase in step::STEP_PHASES {
            assert!(
                text.contains(&format!(
                    "re_step_phase_seconds_total{{runtime=\"rust\",phase=\"{phase}\"}} "
                )),
                "{phase} in {text}"
            );
        }
        send(&app, "DELETE", "/api/engine/config/phaseDetail", None).await;
        assert!(!metrics().await.contains("re_step_phase_seconds_total"));
    }

    /// SURFACE_SPEC.md, "`only` selector": presence selects, an unknown id
    /// selects nothing, and a name resolves through machineResults.
    #[tokio::test]
    async fn the_only_selector_keeps_what_it_attributes() {
        let mut engine = Engine::new(8);
        engine.load_machine(rival("writer-a", 1.0)).unwrap();
        engine.load_machine(rival("writer-b", 1.0)).unwrap();
        let app = app(engine);
        let push = |only: Value| {
            let app = app.clone();
            async move {
                send(
                    &app,
                    "POST",
                    "/api/perceive",
                    Some(json!({ "vector": [1], "only": only })),
                )
                .await
                .1
            }
        };
        let names = |step: &Value, field: &str| -> Vec<String> {
            let mut out: Vec<String> = match &step[field] {
                Value::Object(results) => results
                    .values()
                    .map(|r| r["machineName"].as_str().unwrap().to_owned())
                    .collect(),
                Value::Array(ops) => ops
                    .iter()
                    .map(|op| op["machineName"].as_str().unwrap_or("").to_owned())
                    .collect(),
                _ => Vec::new(),
            };
            out.sort();
            out
        };
        let all = push(Value::Null).await;
        assert_eq!(names(&all, "machineResults"), ["writer-a", "writer-b"]);
        let one = push(json!({ "machineNames": ["writer-b"] })).await;
        assert_eq!(names(&one, "machineResults"), ["writer-b"]);
        assert_eq!(one["activeRegions"].as_array().unwrap().len(), 1);
        assert!(
            one["mergeBatch"]
                .as_array()
                .unwrap()
                .iter()
                .all(|op| op["machineName"] == "writer-b")
        );
        assert_eq!(
            one["perceptualSpace"], all["perceptualSpace"],
            "the computation is unchanged"
        );
        for nothing in [json!({}), json!({ "sequenceIds": ["no-such-sequence"] })] {
            let none = push(nothing.clone()).await;
            assert_eq!(none["machineResults"], json!({}), "{nothing}");
            assert_eq!(none["mergeBatch"], json!([]), "{nothing}");
            assert_eq!(none["activeRegions"], json!([]), "{nothing}");
        }
    }

    /// An explicit `matchAlgorithmOverride` applies above every element's own
    /// comparator; an unknown one is refused.
    #[tokio::test]
    async fn match_algorithm_override_applies_above_the_elements() {
        let (app, id) = loaded_app();
        let matched = |step: &Value| {
            step["machineResults"][&id]["transitionResult"]["sequenceResults"]["sequence"]["matchedEvents"].clone()
        };
        // `start` asks `equals 1`; 0.7 matches it only under gte.
        let (_, step) = send(
            &app,
            "POST",
            "/api/perceive",
            Some(json!({ "vector": [0.7] })),
        )
        .await;
        assert_eq!(matched(&step), json!([]));
        let (_, step) = send(
            &app,
            "POST",
            "/api/perceive",
            Some(json!({ "vector": [0.7], "matchAlgorithmOverride": "GTE" })),
        )
        .await;
        assert_eq!(matched(&step), json!(["start"]));
        let (status, body) = send(
            &app,
            "POST",
            "/api/perceive",
            Some(json!({ "vector": [1], "matchAlgorithmOverride": "fuzzy" })),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"], "Unknown comparator type: fuzzy");
    }

    #[tokio::test]
    async fn transitions_inhibited_writes_and_resets() {
        let (app, id) = loaded_app();
        let uri = "/api/engine/config/transitionsInhibited";
        let (status, body) = send(
            &app,
            "PUT",
            uri,
            Some(json!({ "machine": id, "value": true })),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["value"][&id], true);
        let (status, _) = send(
            &app,
            "PUT",
            uri,
            Some(json!({ "machine": "machine-absent", "value": true })),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (status, _) = send(&app, "PUT", uri, Some(json!({ "value": "yes" }))).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (_, body) = send(&app, "PUT", uri, Some(json!({ "value": false }))).await;
        assert_eq!(body["value"][&id], false, "no machine named: every machine");
        send(&app, "PUT", uri, Some(json!({ "value": true }))).await;
        let (status, body) = send(&app, "DELETE", uri, None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body["value"][&id], false,
            "DELETE restores the declared default"
        );
    }

    /// SURFACE_SPEC.md, "`transitionsInhibited`": accepted, not passed forward.
    #[tokio::test]
    async fn an_inhibited_machine_accepts_and_does_not_flow() {
        let (app, id) = loaded_app();
        let process = |v: f64| json!({ "inputEvent": [v] });
        send(
            &app,
            "PUT",
            "/api/engine/config/transitionsInhibited",
            Some(json!({ "machine": id, "value": true })),
        )
        .await;
        let (status, r) = send(
            &app,
            "POST",
            &format!("/api/machines/{id}/process"),
            Some(process(1.0)),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "accepted, not refused");
        assert!(r["machineOutput"].is_null());
        assert_eq!(r["sequenceResults"]["sequence"]["matchedEvents"], json!([]));
        assert_eq!(r["arbiterMetadata"]["shouldOutput"], false);
        let mut universal = vec![0.0; 8];
        universal[0] = 1.0;
        let (_, r) = send(
            &app,
            "POST",
            "/api/engine/process",
            Some(json!({ "vector": universal })),
        )
        .await;
        assert_eq!(r["result"]["outputs"], json!([]));

        // No state changed while inhibited: once it flows, start then finish fires.
        send(
            &app,
            "DELETE",
            "/api/engine/config/transitionsInhibited",
            None,
        )
        .await;
        send(
            &app,
            "POST",
            &format!("/api/machines/{id}/process"),
            Some(process(1.0)),
        )
        .await;
        let (_, r) = send(
            &app,
            "POST",
            &format!("/api/machines/{id}/process"),
            Some(process(0.0)),
        )
        .await;
        assert_eq!(r["machineOutput"]["vector"], json!([1]));
    }

    #[test]
    fn numbers_take_the_ecmascript_form() {
        for (value, wire) in [
            (1.0, "1"),
            (-3.0, "-3"),
            (0.0, "0"),
            (-0.0, "0"),
            (0.5, "0.5"),
            (-3.25, "-3.25"),
            (100.0, "100"),
            (0.1 + 0.2, "0.30000000000000004"),
            (1234567.0, "1234567"),
            (1.5e-6, "0.0000015"),
            (1e-6, "0.000001"),
            (1e-7, "1e-7"),
            (2.5e-7, "2.5e-7"),
            (1e20, "100000000000000000000"),
            (1e21, "1e+21"),
            (1.5e300, "1.5e+300"),
            (f64::NAN, "null"),
        ] {
            assert_eq!(ecmascript_number(value), wire, "{value:e}");
        }
    }

    #[tokio::test]
    async fn the_wire_carries_integral_values_as_integers() {
        let (app, id) = loaded_app();
        let uri = format!("/api/machines/{id}/process");
        let request = Request::builder()
            .method("POST")
            .uri(&uri)
            .header("content-type", "application/json")
            .body(Body::from(json!({ "inputEvent": [1.0] }).to_string()))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(text.contains(r#""inputEvent":[1]"#), "{text}");
        assert!(!text.contains("1.0"), "{text}");
        // A fraction keeps its digits.
        let (_, config) = send(&app, "GET", "/api/config", None).await;
        assert_eq!(config["matchThreshold"], json!(0.5));
    }

    #[tokio::test]
    async fn an_unimplemented_specified_route_declares_unsupported() {
        let (app, id) = loaded_app();
        let unsupported = |surface: &str| {
            json!({
                "error": format!("{surface} is not implemented by this runtime"),
                "participation": "unsupported",
                "surface": surface,
            })
        };
        // Literal routes of three methods, and a phase-2 template filled in.
        for (method, uri, surface) in [
            ("GET", "/api/sampler/stats", "GET /api/sampler/stats"),
            ("POST", "/api/sampler/start", "POST /api/sampler/start"),
            (
                "POST",
                "/api/perception/observe",
                "POST /api/perception/observe",
            ),
            ("GET", "/api/governance/route", "GET /api/governance/route"),
            (
                "DELETE",
                "/api/runtime/snapshot/s",
                "DELETE /api/runtime/snapshot/:id",
            ),
        ] {
            let (status, body) = send(&app, method, uri, Some(json!({}))).await;
            assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "{method} {uri}");
            assert_eq!(body, unsupported(surface), "{method} {uri}");
        }
        // A served route still answers, and anything the spec does not define
        // is the router's own 404 or 405, not a declaration.
        let (status, _) = send(&app, "GET", &format!("/api/machines/{id}"), None).await;
        assert_eq!(status, StatusCode::OK);
        let (status, body) = send(&app, "GET", "/api/not-in-the-spec", None).await;
        assert_eq!((status, body), (StatusCode::NOT_FOUND, Value::Null));
        let (status, body) = send(&app, "PATCH", "/api/engine/reset", None).await;
        assert_eq!(
            (status, body),
            (StatusCode::METHOD_NOT_ALLOWED, Value::Null)
        );
        // The engine answers for its own routes only, not the PE's.
        let (status, _) = send(&app, "GET", "/api/sources", None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    /// The test machine, renamed and writing `value` to the same output cell.
    fn rival(name: &str, value: f64) -> Value {
        let mut definition = machine_definition();
        definition["machine"]["name"] = json!(name);
        // `join` keeps the magnitude; the default `or` folds to presence.
        definition["machine"]["outputMergeTransformation"] = json!("join");
        definition["machine"]["sequences"][0]["events"][1]["outputEvents"][0]["vector"] =
            json!([value]);
        definition
    }

    #[tokio::test]
    async fn a_step_commits_its_isre_and_osre_and_publishes_the_pair() {
        let (app, id) = loaded_app();
        // Step 0 arms `finish`; nothing is output.
        let (status, step0) = send(
            &app,
            "POST",
            "/api/perceive",
            Some(json!({ "vector": [1] })),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{step0}");
        assert_eq!(step0["stepNumber"], 0);
        assert_eq!(step0["mergeBatch"], json!([]));
        assert_eq!(step0["eventBus"], json!([]));
        assert_eq!(step0["machineResults"][&id]["machineName"], "two-step");
        assert_eq!(step0["machineResults"][&id]["outputRegion"], Value::Null);
        assert!(
            step0["machineResults"][&id]["transitionResult"]
                .get("machineId")
                .is_none()
        );
        assert_eq!(
            step0["activeRegions"],
            json!([{ "offset": 0, "length": 1, "machineId": id, "type": "input" }])
        );
        // Step 1 fires it: cell 1 is written.
        let (_, step1) = send(
            &app,
            "POST",
            "/api/perceive",
            Some(json!({ "vector": [0] })),
        )
        .await;
        assert_eq!(step1["stepNumber"], 1);
        assert_eq!(
            step1["mergeBatch"][0]["region"],
            json!({ "offset": 1, "length": 1 })
        );
        assert_eq!(step1["mergeBatch"][0]["values"], json!([1]));
        assert_eq!(step1["mergeBatch"][0]["sequenceIds"], json!(["sequence"]));
        assert_eq!(step1["perceptualSpace"][1], 1);
        assert_eq!(step1["perceptualSpaceIsDebugProjection"], true);

        let (status, pair) = send(&app, "GET", "/api/engine/steps/0/pair", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            pair,
            json!({ "stepNumber": 0,
            "isre": { "stepNumber": 0, "length": 8, "nonZero": [{ "index": 0, "value": 1 }] },
            "osre": { "stepNumber": 0, "length": 8, "nonZero": [] } })
        );
        let (_, pair) = send(&app, "GET", "/api/engine/steps/1/pair", None).await;
        assert_eq!(pair["isre"]["nonZero"], json!([]));
        assert_eq!(pair["osre"]["nonZero"], json!([{ "index": 1, "value": 1 }]));

        let (_, history) = send(&app, "GET", "/api/engine/osre-history?from=1", None).await;
        assert_eq!(history["history"].as_array().unwrap().len(), 1);
        let (_, history) = send(&app, "GET", "/api/engine/isre-history?limit=1", None).await;
        assert_eq!(history["history"][0]["stepNumber"], 0);
        assert_eq!(history["history"].as_array().unwrap().len(), 1);

        // The clock ticked once per committed step.
        let (_, clock) = send(&app, "GET", "/api/engine/clock", None).await;
        assert_eq!(
            (clock["lamport"].clone(), clock["step"].clone()),
            (json!(2), json!(1))
        );
        // A reset restarts the numbering and the histories; the clock keeps its value.
        send(&app, "POST", "/api/engine/reset", None).await;
        let (_, clock) = send(&app, "GET", "/api/engine/clock", None).await;
        assert_eq!(
            (clock["lamport"].clone(), clock["step"].clone()),
            (json!(2), json!(-1))
        );
        let (status, _) = send(&app, "GET", "/api/engine/steps/0/pair?timeoutMs=0", None).await;
        assert_eq!(status, StatusCode::REQUEST_TIMEOUT);
        let (_, step) = send(
            &app,
            "POST",
            "/api/perceive",
            Some(json!({ "sparseVector": [{ "index": 0, "value": 1 }] })),
        )
        .await;
        assert_eq!(step["stepNumber"], 0);
    }

    /// The test machine with governance: two sequences firing on the same
    /// step, GREEN and RED, so the join has to choose.
    fn governed() -> Value {
        let mut definition = machine_definition();
        let mut red = definition["machine"]["sequences"][0].clone();
        red["id"] = json!("alarm");
        red["deprecatedAt"] = json!("2026-01-01");
        red["replacedBy"] = json!("sequence");
        red["events"][1]["outputEvents"][0]["metadata"] = json!({ "action": "page-now" });
        definition["machine"]["sequences"]
            .as_array_mut()
            .unwrap()
            .push(red);
        definition["machine"]["metadata"] = json!({
            "governance": {
                "ownerTeam": "plant-ops", "runbook": "https://runbooks/x",
                "contact": { "primary": "ops@example.org" },
                "sla": { "error": 900, "info": null }
            },
            "triggerConfig": { "rules": [
                { "sequenceId": "sequence", "outputMatches": [1], "ragStatusCode": "GREEN",
                  "processStatus": "info", "description": "fine" },
                { "sequenceId": "alarm", "outputMatches": [1], "ragStatusCode": "RED",
                  "processStatus": "error", "governance": { "ownerTeam": "night-shift" } }
            ] }
        });
        definition
    }

    #[tokio::test]
    async fn governance_joins_the_contributors_and_marks_deprecation() {
        let mut engine = Engine::new(8);
        engine.load_machine(governed()).unwrap();
        let app = app(engine);
        send(
            &app,
            "POST",
            "/api/perceive",
            Some(json!({ "vector": [1] })),
        )
        .await;
        let (_, step) = send(
            &app,
            "POST",
            "/api/perceive",
            Some(json!({ "vector": [0] })),
        )
        .await;
        let op = &step["mergeBatch"][0];
        assert_eq!(op["sequenceIds"], json!(["alarm", "sequence"]));
        let governance = &op["governance"];
        // RED beats GREEN; the decision travels whole, with the winner's action.
        assert_eq!(governance["sequenceId"], "alarm");
        assert_eq!(governance["ragStatusCode"], "RED");
        assert_eq!(governance["actionCode"], "page-now");
        assert_eq!(
            governance["ownerTeam"], "night-shift",
            "the rule override wins"
        );
        assert_eq!(
            governance["runbook"], "https://runbooks/x",
            "the machine default fills the rest"
        );
        assert_eq!(governance["slaSeconds"], 900);
        assert_eq!(
            governance["contact"],
            json!({ "primary": "ops@example.org" })
        );
        assert_eq!(governance["escalationPolicy"], Value::Null);
        assert_eq!(governance["source"], "rule-with-override");
        assert_eq!(governance["hasMachineGovernance"], true);
        assert!(
            governance.get("description").is_none(),
            "absent when the rule has none"
        );
        assert_eq!(op["deprecation"]["since"], "2026-01-01");
        assert_eq!(op["deprecation"]["replacedBy"], "sequence");
        assert!(op["deprecation"]["ageDays"].as_i64().unwrap() > 0);

        // The counters: one joined decision for the operation, one fire for
        // the deprecated sequence's asserted output.
        let request = Request::builder()
            .uri("/api/metrics")
            .body(Body::empty())
            .unwrap();
        let bytes = to_bytes(
            app.clone().oneshot(request).await.unwrap().into_body(),
            usize::MAX,
        )
        .await
        .unwrap();
        let text = String::from_utf8(bytes.to_vec()).unwrap();
        let line = |prefix: &str, labels: &[&str]| {
            text.lines()
                .find(|l| l.starts_with(prefix) && labels.iter().all(|label| l.contains(label)))
                .unwrap_or_else(|| panic!("no {prefix} {labels:?} in\n{text}"))
                .to_owned()
        };
        assert!(
            line(
                "ces_paging_decisions_total{",
                &[
                    "owner_team=\"night-shift\"",
                    "process_status=\"error\"",
                    "rag_status_code=\"RED\""
                ],
            )
            .ends_with(" 1")
        );
        assert!(
            line(
                "ces_deprecated_fires_total{",
                &["sequence=\"alarm\"", "replaced_by=\"sequence\""],
            )
            .ends_with(" 1")
        );
    }

    #[tokio::test]
    async fn an_event_bus_write_latches_into_later_steps() {
        let mut engine = Engine::new(8);
        let producer = engine.load_machine(machine_definition()).unwrap()["id"]
            .as_str()
            .unwrap()
            .to_owned();
        let mut meta = rival("meta", 1.0);
        meta["machine"]["perceptualMapping"] = json!({
            "input": { "offset": 6, "length": 1 }, "output": { "offset": 7, "length": 1 } });
        meta["machine"]["metadata"] = json!({ "compose": { "subscriptions": [
            { "producerMachineId": producer, "producerSequenceId": "sequence", "bitOffset": 6 } ] } });
        engine.load_machine(meta).unwrap();
        let app = app(engine);
        send(
            &app,
            "POST",
            "/api/perceive",
            Some(json!({ "vector": [1] })),
        )
        .await;
        let (_, fired) = send(
            &app,
            "POST",
            "/api/perceive",
            Some(json!({ "vector": [0] })),
        )
        .await;
        assert_eq!(fired["eventBus"][0]["producerSequenceId"], "sequence");
        assert_eq!(fired["eventBus"][0]["bitOffset"], 6);
        assert_eq!(
            fired["eventBus"][0]["provenance"],
            json!(["start", "finish"])
        );
        assert_eq!(
            fired["perceptualSpace"][6], 1,
            "written after OSRE, into the space"
        );
        let (_, pair) = send(&app, "GET", "/api/engine/steps/1/pair", None).await;
        assert!(
            !pair["osre"]["nonZero"]
                .as_array()
                .unwrap()
                .iter()
                .any(|c| c["index"] == 6)
        );
        // The bit reaches the machines next step, over a caller's zeros.
        send(
            &app,
            "POST",
            "/api/perceive",
            Some(json!({ "vector": [0] })),
        )
        .await;
        let (_, pair) = send(&app, "GET", "/api/engine/steps/2/pair", None).await;
        assert!(
            pair["isre"]["nonZero"]
                .as_array()
                .unwrap()
                .iter()
                .any(|c| c["index"] == 6)
        );
        // A reset unlatches it.
        send(&app, "POST", "/api/engine/reset", None).await;
        send(
            &app,
            "POST",
            "/api/perceive",
            Some(json!({ "vector": [0] })),
        )
        .await;
        let (_, pair) = send(&app, "GET", "/api/engine/steps/0/pair", None).await;
        assert_eq!(pair["isre"]["nonZero"], json!([]));
    }

    #[tokio::test]
    async fn a_pair_waiter_wakes_at_the_completion_point() {
        let (app, _) = loaded_app();
        let waiter = tokio::spawn({
            let app = app.clone();
            async move { send(&app, "GET", "/api/engine/steps/0/pair?timeoutMs=5000", None).await }
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            !waiter.is_finished(),
            "the waiter answered before step 0 existed"
        );
        send(
            &app,
            "POST",
            "/api/perceive",
            Some(json!({ "vector": [1] })),
        )
        .await;
        let (status, pair) = waiter.await.unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(pair["stepNumber"], 0);
    }

    #[tokio::test]
    async fn the_pair_route_answers_every_status() {
        let (app, _) = loaded_app();
        for (uri, status) in [
            ("/api/engine/steps/x/pair", StatusCode::BAD_REQUEST),
            ("/api/engine/steps/-1/pair", StatusCode::BAD_REQUEST),
            (
                "/api/engine/steps/0/pair?timeoutMs=60001",
                StatusCode::BAD_REQUEST,
            ),
            (
                "/api/engine/steps/0/pair?timeoutMs=abc",
                StatusCode::BAD_REQUEST,
            ),
            (
                "/api/engine/steps/0/pair?timeoutMs=0",
                StatusCode::REQUEST_TIMEOUT,
            ),
        ] {
            let (got, body) = send(&app, "GET", uri, None).await;
            assert_eq!(got, status, "{uri}: {body}");
            assert!(body["error"].is_string(), "{uri}: {body}");
        }
        // Past the 1024-entry capacity, step 0 is resolved and gone.
        for _ in 0..1025 {
            send(&app, "POST", "/api/perceive", Some(json!({ "vector": [0], "includeMachineResults": false, "includePerceptualSpace": false }))).await;
        }
        let (status, body) = send(&app, "GET", "/api/engine/steps/0/pair?timeoutMs=0", None).await;
        assert_eq!(
            (status, body["error"].clone()),
            (StatusCode::GONE, json!("step 0 is no longer retained"))
        );
        let (status, _) = send(&app, "GET", "/api/engine/steps/1/pair?timeoutMs=0", None).await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn perceive_takes_one_input_and_honours_the_include_flags() {
        let (app, _) = loaded_app();
        let (status, body) = send(&app, "POST", "/api/perceive", Some(json!({}))).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            body["error"],
            "Provide exactly one of: vector, sparseVector, domainVectors"
        );
        let (_, step) = send(&app, "POST", "/api/perceive", Some(json!({ "domainVectors": [{ "offset": 0, "values": [1] }], "compact": true, "includePerceptualSpace": false, "includeActiveRegions": false }))).await;
        let keys: Vec<&str> = step
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(keys, ["eventBus", "mergeBatch", "stepNumber", "timestamp"]);
    }

    #[tokio::test]
    async fn a_contended_cell_resolves_and_the_batch_is_in_canonical_order() {
        let mut engine = Engine::new(8);
        engine.load_machine(rival("zulu", 1.0)).unwrap();
        engine.load_machine(rival("alpha", 3.0)).unwrap();
        let app = app(engine);
        send(
            &app,
            "POST",
            "/api/perceive",
            Some(json!({ "vector": [1] })),
        )
        .await;
        let (_, step) = send(
            &app,
            "POST",
            "/api/perceive",
            Some(json!({ "vector": [0] })),
        )
        .await;
        let names: Vec<&str> = step["mergeBatch"]
            .as_array()
            .unwrap()
            .iter()
            .map(|m| m["machineName"].as_str().unwrap())
            .collect();
        assert_eq!(names, ["alpha", "zulu"]);
        // Both write cell 1; PRECEDENCE between equal-rank machines keeps the larger.
        let (_, pair) = send(&app, "GET", "/api/engine/steps/1/pair", None).await;
        assert_eq!(pair["osre"]["nonZero"], json!([{ "index": 1, "value": 3 }]));
    }

    /// SURFACE_SPEC.md, "Arbitration retention and the instance clock": the
    /// legacy object while off; retained steps keyed by step, under their clock,
    /// in the window, while on; the five `?step=N` answers; a reset drops them.
    #[tokio::test]
    async fn arbitration_records_are_retained_by_step_under_the_clock() {
        let mut engine = Engine::new(8);
        engine.load_machine(rival("zulu", 1.0)).unwrap();
        engine.load_machine(rival("alpha", 3.0)).unwrap();
        let app = app(engine);
        let push = |vector: f64| {
            let app = app.clone();
            async move {
                send(
                    &app,
                    "POST",
                    "/api/perceive",
                    Some(json!({ "vector": [vector] })),
                )
                .await
            }
        };
        let config = |name: &str, value: Value| {
            let app = app.clone();
            let uri = format!("/api/engine/config/{name}");
            async move { send(&app, "PUT", &uri, Some(json!({ "value": value }))).await }
        };
        let read = |query: &str| {
            let app = app.clone();
            let uri = format!("/api/arbitration{query}");
            async move { send(&app, "GET", &uri, None).await }
        };

        // Off: the legacy object, the latest step only.
        push(1.0).await;
        push(0.0).await;
        let (status, legacy) = read("").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(legacy["count"], json!(1));
        let record = &legacy["records"][0];
        assert_eq!(
            (record["instant"].clone(), record["cell"].clone()),
            (json!(1), json!(1))
        );
        assert_eq!(record["rule"], "PRECEDENCE");
        assert_eq!(record["resolved"], json!(3));
        let origins: Vec<&str> = record["contributors"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["cesId"].as_str().unwrap())
            .collect();
        assert_eq!(origins, ["sequence", "sequence"]);
        assert_eq!(record["contributors"][0]["determinism"], "deterministic");
        assert_eq!(record["contributors"][0]["outputVectorId"], "0");
        assert_eq!(
            record["suppressed"][0]["value"],
            json!(1),
            "the smaller writer is set aside"
        );
        let (status, _) = read("?step=1").await;
        assert_eq!(status, StatusCode::CONFLICT);

        // On: retained from the next step, keyed by step, under its clock.
        config("arbitrationRetention", json!(true)).await;
        assert_eq!(
            read("").await.1,
            json!([]),
            "nothing retained before it was on"
        );
        let (status, _) = read("?step=1").await;
        assert_eq!(status, StatusCode::GONE, "ran before retention was on");
        push(1.0).await;
        push(0.0).await;
        let (_, steps) = read("").await;
        assert_eq!(
            steps.as_array().unwrap().len(),
            1,
            "the default window is one"
        );
        assert_eq!(steps[0]["clock"]["step"], json!(3));
        let (_, clock) = send(&app, "GET", "/api/engine/clock", None).await;
        assert_eq!(steps[0]["clock"], clock, "the clock it committed at");
        config("arbitrationWindow", json!(2)).await;
        push(1.0).await;
        let (_, steps) = read("").await;
        let numbers: Vec<_> = steps
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["clock"]["step"].clone())
            .collect();
        assert_eq!(numbers, [json!(3), json!(4)], "oldest first");
        assert_eq!(read("?step=4").await.0, StatusCode::OK);
        assert_eq!(read("?step=9").await.0, StatusCode::NOT_FOUND);
        assert_eq!(read("?step=-1").await.0, StatusCode::BAD_REQUEST);
        for bad in [json!(-1), json!(1025), json!(1.5), json!(true)] {
            assert_eq!(
                config("arbitrationWindow", bad.clone()).await.0,
                StatusCode::BAD_REQUEST,
                "{bad}"
            );
        }
        config("arbitrationWindow", json!(1)).await;
        assert_eq!(
            read("?step=3").await.0,
            StatusCode::GONE,
            "narrowing evicts at once"
        );

        // A reset drops every retained step and the latest records; the
        // lamport keeps counting.
        let lamport = clock["lamport"].as_u64().unwrap();
        send(&app, "POST", "/api/engine/reset", None).await;
        assert_eq!(read("").await.1, json!([]));
        assert_eq!(read("?step=0").await.0, StatusCode::NOT_FOUND);
        push(1.0).await;
        let (_, steps) = read("").await;
        assert_eq!(steps[0]["clock"]["step"], json!(0));
        assert!(steps[0]["clock"]["lamport"].as_u64().unwrap() > lamport);
        // Off again: the escape drops what was kept.
        config("arbitrationRetention", json!(false)).await;
        assert!(read("").await.1["records"].is_array());
    }

    #[tokio::test]
    async fn clock_reports_the_instance_and_no_committed_step() {
        let (app, id) = loaded_app();
        let (status, before) = send(&app, "GET", "/api/engine/clock", None).await;
        assert_eq!(status, StatusCode::OK);
        let instance = before["instance"].as_str().unwrap();
        assert!(instance_clock::canonical_uuid(instance), "{instance}");
        assert_eq!(before["lamport"], 0);
        assert_eq!(before["step"], -1);
        // Neither process route commits a step, and a reset leaves the clock.
        let uri = format!("/api/machines/{id}/process");
        send(&app, "POST", &uri, Some(json!({ "inputEvent": [1.0] }))).await;
        send(
            &app,
            "POST",
            "/api/engine/process",
            Some(json!({ "inputEvent": [1.0] })),
        )
        .await;
        send(&app, "POST", "/api/engine/reset", None).await;
        let (_, after) = send(&app, "GET", "/api/engine/clock", None).await;
        assert_eq!(after, before);
    }

    #[tokio::test]
    async fn metrics_report_ces_coverage_and_survive_reset() {
        let (app, id) = loaded_app();
        for v in [1.0, 0.0] {
            send(
                &app,
                "POST",
                &format!("/api/machines/{id}/process"),
                Some(json!({ "inputEvent": [v] })),
            )
            .await;
        }
        send(&app, "POST", "/api/engine/reset", None).await;
        let request = Request::builder()
            .uri("/api/metrics")
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(
            response.headers()[axum::http::header::CONTENT_TYPE],
            "text/plain; version=0.0.4; charset=utf-8"
        );
        let text = String::from_utf8(
            to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap();
        let labels = format!("machine=\"two-step\",machine_id=\"{id}\"");
        for line in [
            "ces_machines_total{runtime=\"rust\"} 1".to_owned(),
            "ces_vectors_total{runtime=\"rust\"} 2".to_owned(),
            format!("ces_machine_steps_total{{runtime=\"rust\",{labels}}} 2"),
            format!(
                "ces_vector_matched_total{{runtime=\"rust\",{labels},sequence=\"sequence\",vector=\"start\"}} 1"
            ),
            format!(
                "ces_sequence_outputs_total{{runtime=\"rust\",{labels},sequence=\"sequence\"}} 1"
            ),
            format!("ces_unfired_sequences{{runtime=\"rust\",{labels}}} 0"),
            format!("ces_unfired_vectors{{runtime=\"rust\",{labels}}} 0"),
            "re_runtime_required_dimension{runtime=\"rust\"} 2".to_owned(),
        ] {
            assert!(
                text.lines().any(|l| l == line),
                "missing `{line}` in\n{text}"
            );
        }
        // phaseDetail is off, so its phases are absent rather than zero; the
        // gauge saying so is always there.
        assert!(!text.contains("re_step_phase_seconds_total"), "{text}");
        assert!(
            text.contains("re_step_phase_detail{runtime=\"rust\"} 0"),
            "{text}"
        );
        assert!(
            text.contains(&format!(
                "ces_deprecated_fires_total{{runtime=\"rust\",{labels}}} 0"
            )),
            "the zero baseline: {text}"
        );
    }

    #[tokio::test]
    async fn config_reports_the_canonical_keys() {
        let app = app(Engine::new(8));
        let (status, config) = send(&app, "GET", "/api/config", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            config,
            json!({
                "eventDimension": 8,
                "matchThreshold": 0.5,
                "qdrantUrl": "http://localhost:4333",
                "collectionName": "reality-events",
            })
        );
    }

    /// SURFACE_SPEC.md, "PUT /api/config/dimension is read-only downward".
    #[tokio::test]
    async fn put_dimension_is_read_only_downward() {
        // machine_definition() maps input [0:1] and output [1:2]: it requires 2.
        let mut engine = Engine::new(4);
        engine.load_machine(machine_definition()).unwrap();
        let app = app(engine);
        let width = |app: Router| async move {
            send(&app, "GET", "/api/config", None).await.1["eventDimension"].clone()
        };

        for (uri, wanted) in [
            (
                "/api/config/dimension",
                "dimension query parameter is required",
            ),
            (
                "/api/config/dimension?dimension=wide",
                "dimension must be an integer",
            ),
            (
                "/api/config/dimension?dimension=1",
                "below the 2 the resident corpus requires",
            ),
            (
                "/api/config/dimension?dimension=3",
                "below the 4 this engine already holds",
            ),
        ] {
            let (status, body) = send(&app, "PUT", uri, None).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}");
            let error = body["error"].as_str().unwrap();
            assert!(error.contains(wanted), "{uri}: {error}");
            assert_eq!(width(app.clone()).await, 4, "{uri} must not mutate");
        }

        let (status, body) = send(&app, "PUT", "/api/config/dimension?dimension=16", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body,
            json!({ "success": true, "dimension": 16, "requiredDimension": 2 })
        );
        assert_eq!(width(app.clone()).await, 16);

        // At the width already held: applied, nothing changes.
        let (status, body) = send(&app, "PUT", "/api/config/dimension?dimension=16", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["dimension"], 16);
    }

    #[tokio::test]
    async fn put_threshold_is_stored_and_reported() {
        let app = app(Engine::new(8));
        let (status, body) = send(&app, "PUT", "/api/config/threshold?threshold=0.7", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, json!({ "success": true, "threshold": 0.7 }));
        assert_eq!(
            send(&app, "GET", "/api/config", None).await.1["matchThreshold"],
            0.7
        );

        // No parameter changes nothing; an unparseable one is refused.
        let (_, body) = send(&app, "PUT", "/api/config/threshold", None).await;
        assert_eq!(body["threshold"], 0.7);
        // SURFACE_SPEC.md: present but not a whole finite number is a 400.
        for invalid in ["high", "0.7abc", "nan", "inf"] {
            let uri = format!("/api/config/threshold?threshold={invalid}");
            let (status, body) = send(&app, "PUT", &uri, None).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{invalid}");
            assert!(body["error"].is_string(), "{invalid}: {body}");
        }
        assert_eq!(
            send(&app, "GET", "/api/config", None).await.1["matchThreshold"],
            0.7
        );
    }

    #[tokio::test]
    async fn transition_responses_use_the_canonical_input_key() {
        let mut engine = Engine::new(8);
        let id = engine.load_machine(machine_definition()).unwrap()["id"]
            .as_str()
            .unwrap()
            .to_owned();
        let result = engine.process_machine(&id, &[1.0]).unwrap();
        assert_eq!(result["inputEvent"], json!([1.0]));
        assert!(result.get("inputVector").is_none());
    }

    #[tokio::test]
    async fn api_accepts_machines_and_processes_transitions() {
        let app = app(Engine::new(8));
        let invalid_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/machines")
                    .header("content-type", "application/json")
                    .body(Body::from("{"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(invalid_response.status(), StatusCode::BAD_REQUEST);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/machines")
                    .header("content-type", "application/json")
                    .body(Body::from(machine_definition().to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let registered: Value = serde_json::from_slice(&body).unwrap();
        let machine_id = registered["machine"]["id"].as_str().unwrap();

        for (vector, expecting_output) in [(json!([1.0]), false), (json!([0.0]), true)] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(format!("/api/machines/{machine_id}/process"))
                        .header("content-type", "application/json")
                        .body(Body::from(
                            json!({ "inputEvent": vector.clone() }).to_string(),
                        ))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            // The transition result itself, at the top level, as the engines answer.
            let result: Value = serde_json::from_slice(&body).unwrap();
            // Echoed in the engines' number form: the input's numbers, as
            // ECMAScript writes them (#33).
            assert_eq!(
                ecmascript_json(&result["inputEvent"]).unwrap(),
                ecmascript_json(&vector).unwrap()
            );
            if expecting_output {
                assert_eq!(result["machineOutput"]["vector"][0], 1.0);
            } else {
                assert!(result["machineOutput"].is_null());
            }
        }

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/engine/reset?clearAudit=true")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/engine/history")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let history: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(history["history"], json!([]));
    }

    #[test]
    fn duplicate_machine_names_are_versioned_and_regions_reallocated() {
        let mut engine = Engine::new(8);
        let first = engine.load_machine(machine_definition()).unwrap();
        let second = engine.load_machine(machine_definition()).unwrap();
        assert_eq!(first["name"], "two-step");
        assert_eq!(second["name"], "two-step v2");
        assert_ne!(first["id"], second["id"]);
        assert_eq!(second["perceptualMapping"]["input"]["offset"], 8);
        assert_eq!(second["perceptualMapping"]["output"]["offset"], 9);
    }

    #[test]
    fn canonical_keys_load_every_event() {
        let loaded = Engine::new(8).load_machine(machine_definition()).unwrap();
        assert_eq!(loaded["totalEvents"], 2);
    }

    #[test]
    fn retired_event_keys_are_rejected_by_name() {
        // (the retired key, a mutation that writes the fixture with it)
        type Case = (&'static str, fn(&mut Value));
        let cases: [Case; 4] = [
            ("vectors", |m| {
                let sequence = &mut m["machine"]["sequences"][0];
                let events = sequence.as_object_mut().unwrap().remove("events").unwrap();
                sequence["vectors"] = events;
            }),
            ("outputVectors", |m| {
                let event = &mut m["machine"]["sequences"][0]["events"][1];
                let outputs = event
                    .as_object_mut()
                    .unwrap()
                    .remove("outputEvents")
                    .unwrap();
                event["outputVectors"] = outputs;
            }),
            ("nextVectorIds", |m| {
                let event = &mut m["machine"]["sequences"][0]["events"][0];
                let next = event
                    .as_object_mut()
                    .unwrap()
                    .remove("nextEventIds")
                    .unwrap();
                event["nextVectorIds"] = next;
            }),
            ("vectors", |m| {
                m["machine"]["inputSequences"] = json!([{ "name": "legacy", "vectors": [[1]] }]);
            }),
        ];
        for (retired, make_legacy) in cases {
            let mut definition = machine_definition();
            make_legacy(&mut definition);
            let error = Engine::new(8).load_machine(definition).err().unwrap();
            assert_eq!(error.status, StatusCode::BAD_REQUEST);
            assert!(
                error.message.contains(&format!("`{retired}`")),
                "{retired}: {}",
                error.message
            );
        }
    }

    #[test]
    fn a_sequence_without_events_is_a_load_error() {
        let mut definition = machine_definition();
        definition["machine"]["sequences"][0]["events"] = json!([]);
        let error = Engine::new(8).load_machine(definition).err().unwrap();
        assert!(error.message.contains("has no events"), "{}", error.message);
    }

    #[test]
    fn a_failing_machine_file_is_named() {
        let directory = std::env::temp_dir().join(new_machine_id());
        fs::create_dir_all(&directory).unwrap();
        let mut legacy = machine_definition();
        legacy["machine"]["sequences"][0]["events"] = json!([]);
        fs::write(directory.join("broken.json"), legacy.to_string()).unwrap();

        let error = Engine::new(8).load_directory(&directory).err().unwrap();
        fs::remove_dir_all(&directory).unwrap();
        assert!(error.message.contains("broken.json"), "{}", error.message);
    }

    fn event_with(comparator: &str, value: f64, threshold: Option<f64>) -> Event {
        Event {
            id: "e".to_owned(),
            initial: true,
            active: true,
            elements: vec![Element {
                value,
                comparator: comparator.to_owned(),
                threshold,
            }],
            outputs: Vec::new(),
            next_ids: Vec::new(),
            metadata: json!({}),
            was_just_matched: false,
            predecessor_chain: Vec::new(),
        }
    }

    /// SURFACE_SPEC.md § "Comparators", one non-binary input per rule.
    #[test]
    fn comparators_follow_the_engines() {
        // (comparator, value, threshold, input, accepts)
        let cases = [
            // gte binarizes both sides at t (default 0.5)
            ("gte", 1.0, None, 0.95, true),
            ("gte", 1.0, None, 0.4, false),
            ("gte", 0.0, None, 0.4, true),
            ("gte", 0.0, None, 1.0, false), // a 0 means "must be off", not "any input"
            ("gte", 1.0, Some(0.8), 0.7, false),
            ("gte", 1.0, Some(0.8), 0.85, true),
            // threshold: abs(value - input) <= t (default 0.1)
            ("threshold", 0.5, None, 0.55, true),
            ("threshold", 0.5, None, 0.7, false),
            ("threshold", 0.5, Some(0.3), 0.7, true),
            // pattern: 1 - abs(value - input) >= t (default 0.5)
            ("pattern", 1.0, None, 0.6, true),
            ("pattern", 1.0, None, 0.4, false),
            ("pattern", 1.0, Some(0.9), 0.85, false),
            // equals is exact; custom is its alias
            ("equals", 1.0, None, 1.0, true),
            ("equals", 1.0, None, 0.99, false),
            ("custom", 1.0, None, 0.99, false),
        ];
        for (comparator, value, threshold, input, accepts) in cases {
            let event = event_with(comparator, value, threshold);
            assert_eq!(
                event.matches(&[input], "gte", None),
                accepts,
                "{comparator} value {value} threshold {threshold:?} input {input}"
            );
        }
    }

    #[test]
    fn comparator_precedence_is_element_then_machine_then_gte() {
        // No element comparator: the machine's matchAlgorithm applies.
        let event = event_with("", 1.0, None);
        assert!(!event.matches(&[0.95], "equals", None));
        assert!(event.matches(&[0.95], "gte", None));
        // The element's own comparatorType wins over the machine's.
        let event = event_with("gte", 1.0, None);
        assert!(event.matches(&[0.95], "equals", None));

        // A matchAlgorithm on the event itself is not read, as in every engine.
        let mut definition = machine_definition();
        definition["machine"]["matchAlgorithm"] = json!("equals");
        let start = &mut definition["machine"]["sequences"][0]["events"][0];
        start["elements"] = json!([{ "value": 1 }]);
        start["matchAlgorithm"] = json!("gte");
        let mut engine = Engine::new(8);
        let id = engine.load_machine(definition).unwrap()["id"]
            .as_str()
            .unwrap()
            .to_owned();
        let result = engine.process_machine(&id, &[0.95]).unwrap();
        assert!(
            result["sequenceResults"]["sequence"]["matchedEvents"]
                .as_array()
                .is_none_or(Vec::is_empty),
            "machine-level equals applies, not the event's gte: {result}"
        );
    }

    #[test]
    fn declared_machine_ids_are_kept_when_available() {
        let mut definition = machine_definition();
        definition["id"] = json!("machine-declared");
        let loaded = Engine::new(8).load_machine(definition).unwrap();
        assert_eq!(loaded["id"], "machine-declared");
    }

    #[test]
    fn machine_files_load_recursively_with_file_stem_ids() {
        let directory = std::env::temp_dir().join(new_machine_id());
        let nested = directory.join("nested");
        fs::create_dir_all(&nested).unwrap();
        fs::write(
            nested.join("fixture.json"),
            machine_definition().to_string(),
        )
        .unwrap();

        let mut engine = Engine::new(8);
        assert_eq!(engine.load_directory(&directory).unwrap(), 1);
        assert!(engine.machine("machine-fixture").is_some());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn canonical_machine_ids_match_the_engines() {
        assert_eq!(
            canonical_machine_id("AGX001_aquaculture-water-quality-stability"),
            "machine-agx001-aquaculture-water-quality-stability"
        );
        assert_eq!(canonical_machine_id("RS2"), "machine-rs2");
    }

    /// A corpus at `<root>/machines` with a nested and a top-level file, and a
    /// semantics manifest at `<root>/semantics` naming only the nested one.
    fn corpus_fixture() -> std::path::PathBuf {
        let root = std::env::temp_dir().join(new_machine_id());
        let nested = root.join("machines").join("domains").join("energy");
        fs::create_dir_all(&nested).unwrap();
        fs::create_dir_all(root.join("semantics")).unwrap();
        fs::write(
            nested.join("EGX_Test-One.json"),
            machine_definition().to_string(),
        )
        .unwrap();
        let mut top = machine_definition();
        top["machine"]["name"] = json!("top-level");
        fs::write(root.join("machines").join("Top.json"), top.to_string()).unwrap();
        fs::write(
            root.join("semantics").join("abox-manifest.json"),
            json!({ "machines": { "energy/EGX_Test-One": {
                "iri": "https://realityengine.example.org/machines/energy/EGX_Test-One#machine",
                "sha256": "abc123",
            } } })
            .to_string(),
        )
        .unwrap();
        root
    }

    #[tokio::test]
    async fn json_list_is_the_corpus_on_disk() {
        let root = corpus_fixture();
        let mut engine = Engine::new(8);
        engine.load_directory(&root.join("machines")).unwrap();
        let app = app(engine);
        let (status, body) = send(&app, "GET", "/api/machines/json/list", None).await;
        fs::remove_dir_all(&root).unwrap();
        assert_eq!(status, StatusCode::OK);
        let rows = body["machines"].as_array().unwrap();
        assert_eq!(rows.len(), 2);
        let nested = rows
            .iter()
            .find(|r| r["filename"] == "EGX_Test-One.json")
            .unwrap();
        assert_eq!(nested["relFile"], "domains/energy/EGX_Test-One.json");
        assert_eq!(nested["name"], "EGX_Test-One");
        assert_eq!(nested["version"], "1.0.0");
        assert_eq!(nested["sequenceCount"], 0);
        assert_eq!(
            nested["semanticsIri"],
            "https://realityengine.example.org/machines/energy/EGX_Test-One#machine"
        );
        assert_eq!(nested["semanticsHash"], "abc123");
        let top = rows.iter().find(|r| r["filename"] == "Top.json").unwrap();
        assert_eq!(top["relFile"], "Top.json");
        assert!(
            top.get("semanticsHash").is_none(),
            "no manifest entry, no key: {top}"
        );
    }

    #[tokio::test]
    async fn json_name_loads_by_basename_and_replaces() {
        let root = corpus_fixture();
        let mut engine = Engine::new(8);
        engine.load_directory(&root.join("machines")).unwrap();
        let app = app(engine);

        let (status, body) = send(&app, "GET", "/api/machines/json/EGX_Test-One", None).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["success"], true);
        assert_eq!(body["machine"]["id"], "machine-egx-test-one");
        assert_eq!(body["machine"]["name"], "two-step");

        // Loading it again replaces the machine; it does not add a second.
        let (status, _) = send(&app, "GET", "/api/machines/json/EGX_Test-One.json", None).await;
        assert_eq!(status, StatusCode::OK);
        let (_, listed) = send(&app, "GET", "/api/machines", None).await;
        let ids: Vec<_> = listed["machines"]
            .as_array()
            .unwrap()
            .iter()
            .map(|m| m["id"].as_str().unwrap().to_owned())
            .collect();
        assert_eq!(ids.len(), 2, "{ids:?}");
        assert!(ids.contains(&"machine-egx-test-one".to_owned()), "{ids:?}");

        let (status, _) = send(&app, "GET", "/api/machines/json/Absent", None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (status, _) = send(&app, "GET", "/api/machines/json/..%2Fsecret", None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        fs::remove_dir_all(&root).unwrap();
    }

    /// SURFACE_SPEC.md, "`/api/perceptual-simulation` has one behaviour on
    /// every runtime" (RealityEngine_CI#489).
    #[tokio::test]
    async fn a_perceptual_simulation_walks_its_committed_sequence() {
        let (app, id) = loaded_app();
        let sim = |route: &str| format!("/api/perceptual-simulation/{route}");
        // Nothing committed: a caller precondition, so 400, never 500.
        for route in ["step", "start", "configure/commit"] {
            let (status, body) = send(&app, "POST", &sim(route), None).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{route}: {body}");
            assert!(body["error"].is_string(), "{route}: {body}");
        }
        let (_, body) = send(
            &app,
            "POST",
            &sim("configure/chunk"),
            Some(json!({ "events": [[1]] })),
        )
        .await;
        assert_eq!(body, json!({ "success": true, "bufferedEvents": 1 }));
        let (status, _) = send(&app, "POST", &sim("configure/commit"), None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "no region buffered yet");
        let chunk = json!({
            "events": [[0], [1]],
            "config": { "inputRegion": { "offset": 0, "length": 1 }, "stepDelayMs": 1, "maxSteps": 2 },
        });
        let (_, body) = send(&app, "POST", &sim("configure/chunk"), Some(chunk)).await;
        assert_eq!(body["bufferedEvents"], json!(3));
        let (_, body) = send(&app, "POST", &sim("configure/commit"), None).await;
        assert_eq!(body, json!({ "success": true }));

        // The walk writes sequence[n] at the region, over what step n-1 left.
        let (_, first) = send(&app, "POST", &sim("step"), None).await;
        assert_eq!(first["step"]["stepNumber"], json!(0));
        assert_eq!(
            first["step"]["machineResults"][&id]["inputEvent"],
            json!([1])
        );
        let (_, second) = send(&app, "POST", &sim("step"), None).await;
        assert_eq!(second["step"]["stepNumber"], json!(1));
        assert_eq!(second["step"]["perceptualSpace"][1], json!(1), "{second}");
        // maxSteps bounds the walk before the sequence's end.
        let (_, done) = send(&app, "POST", &sim("step"), None).await;
        assert_eq!(done, json!({ "done": true, "success": true }));
        let (_, state) = send(&app, "GET", &sim("state"), None).await;
        let state = &state["state"];
        assert_eq!(state["currentStep"], json!(2));
        assert_eq!(state["isRunning"], json!(false));
        assert_eq!(state["machines"][0]["id"], json!(id));
        assert_eq!(state["perceptualSpace"].as_array().unwrap().len(), 8);
        let (_, history) = send(&app, "GET", &sim("history"), None).await;
        let numbers: Vec<_> = history["history"]
            .as_array()
            .unwrap()
            .iter()
            .map(|step| step["stepNumber"].clone())
            .collect();
        assert_eq!(numbers, [json!(1), json!(0)], "newest first");

        // The reset rewinds the walk and keeps it committed; start auto-plays
        // it to the end, and the run stops by itself.
        send(&app, "POST", &sim("reset"), None).await;
        let (_, state) = send(&app, "GET", &sim("state"), None).await;
        assert_eq!(state["state"]["currentStep"], json!(0));
        let (_, body) = send(&app, "POST", &sim("start"), None).await;
        assert_eq!(body, json!({ "success": true }));
        let mut state = Value::Null;
        for _ in 0..200 {
            state = send(&app, "GET", &sim("state"), None).await.1;
            if state["state"]["isRunning"] == json!(false) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert_eq!(state["state"]["currentStep"], json!(2), "{state}");
        assert_eq!(state["state"]["isRunning"], json!(false));
    }

    #[tokio::test]
    async fn active_events_are_each_machines_running_reality_events() {
        let (app, id) = loaded_app();
        let active = |body: Value| -> Vec<String> {
            body["activeEvents"]
                .as_array()
                .unwrap()
                .iter()
                .map(|entry| {
                    assert_eq!(entry["machineId"], json!(id));
                    assert_eq!(entry["sequenceId"], json!("sequence"));
                    assert_eq!(entry["vector"]["state"], json!("active"));
                    entry["vector"]["id"].as_str().unwrap().to_owned()
                })
                .collect()
        };
        let (_, body) = send(&app, "GET", "/api/engine/active", None).await;
        assert_eq!(
            body["activeEvents"][0]["vector"]["elements"],
            json!([{ "value": 1, "comparatorType": "equals" }])
        );
        assert_eq!(active(body), ["start"]);
        send(
            &app,
            "POST",
            "/api/perceive",
            Some(json!({ "vector": [1] })),
        )
        .await;
        let (_, body) = send(&app, "GET", "/api/engine/active", None).await;
        // The initial event stays active; its successor joins it.
        assert_eq!(
            active(body),
            ["finish", "start"],
            "as the machine is running, not as registered"
        );
    }

    /// SURFACE_SPEC.md, "Streaming events": `data: <json>` per pushed step.
    #[tokio::test]
    async fn the_step_stream_carries_each_pushed_step() {
        use futures_util::StreamExt;
        let (app, _) = loaded_app();
        let request = Request::builder()
            .uri("/api/engine/stream")
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["content-type"], "text/event-stream");
        let mut frames = response.into_body().into_data_stream();
        let mut text = String::new();
        let (_, pushed) = send(
            &app,
            "POST",
            "/api/perceive",
            Some(json!({ "vector": [1] })),
        )
        .await;
        while !text.contains("data: ") || !text.ends_with("\n\n") {
            let frame = tokio::time::timeout(std::time::Duration::from_secs(5), frames.next())
                .await
                .expect("a step on the stream")
                .unwrap()
                .unwrap();
            text.push_str(std::str::from_utf8(&frame).unwrap());
        }
        assert!(text.starts_with(": connected\n\n"), "{text}");
        let data = text
            .lines()
            .find_map(|line| line.strip_prefix("data: "))
            .unwrap();
        assert_eq!(serde_json::from_str::<Value>(data).unwrap(), pushed);
        assert!(
            !data.contains(".0,"),
            "ECMAScript numbers on the stream too: {data}"
        );
    }

    /// #13 slice 1: canonical order, the list and full forms, and the export
    /// as a corpus document.
    #[tokio::test]
    async fn machines_are_shown_in_canonical_order_and_exported_as_declared() {
        let mut engine = Engine::new(8);
        let mut zulu = rival("zulu", 1.0);
        zulu["machine"]["metadata"] = json!({ "domain": "a-domain" });
        zulu["machine"]["arbiterRule"] = json!("OR");
        zulu["machine"]["inputSequences"] = json!([{ "id": "seed", "events": [[1]] }]);
        engine.load_machine(zulu).unwrap();
        engine.load_machine(rival("alpha", 3.0)).unwrap();
        let app = app(engine);
        let (_, list) = send(&app, "GET", "/api/machines", None).await;
        let machines = list["machines"].as_array().unwrap();
        let names: Vec<&str> = machines
            .iter()
            .map(|m| m["name"].as_str().unwrap())
            .collect();
        assert_eq!(names, ["alpha", "zulu"], "no domain sorts before a-domain");
        let zulu = &machines[1];
        assert_eq!(
            zulu["sequences"],
            json!([{ "id": "sequence", "name": "unnamed", "initialEventIds": ["start"] }])
        );
        assert_eq!(
            zulu["metadata"]["inputSequences"][0]["id"], "seed",
            "stashed in metadata"
        );
        assert_eq!(zulu["arbiterRule"], "or");
        let id = zulu["id"].as_str().unwrap();

        let (_, detail) = send(&app, "GET", &format!("/api/machines/{id}"), None).await;
        let events = &detail["machine"]["sequences"][0]["events"];
        assert_eq!(events[1]["id"], "start");
        assert_eq!(events[1]["state"], "active");
        assert_eq!(
            detail["machine"]["sequences"][0]["outputEventIds"],
            json!(["finish"])
        );

        let (_, export) = send(&app, "GET", &format!("/api/machines/{id}/export"), None).await;
        assert_eq!(export["version"], "1.0.0");
        assert_eq!(export["machine"]["arbiterRule"], "OR");
        assert_eq!(export["machine"]["inputSequences"][0]["id"], "seed");
        assert!(
            export["machine"]["metadata"]
                .get("inputSequences")
                .is_none()
        );
        let (status, body) = send(&app, "GET", "/api/machines/nope/export", None).await;
        assert_eq!(
            (status, body["error"].clone()),
            (StatusCode::NOT_FOUND, json!("Machine not found"))
        );

        // Both write the same cell, so neither's output overlaps the other's input.
        let (_, graph) = send(&app, "GET", "/api/machine-graph", None).await;
        assert_eq!(graph["nodes"].as_array().unwrap().len(), 2);
        assert_eq!(graph["edges"], json!([]));
    }

    /// #13 slice 2: what-if persists nothing, the universal drive advances,
    /// a checkpoint restores, PATCH keeps the machine's control state, and a
    /// definition that is not one is refused.
    #[tokio::test]
    async fn the_machine_family_drives_patches_and_restores() {
        let (app, id) = loaded_app();
        let active = |app: Router, id: String| async move {
            let (_, body) = send(&app, "GET", &format!("/api/machines/{id}"), None).await;
            body["machine"]["sequences"][0]["events"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|e| e["isActive"] == json!(true))
                .map(|e| e["id"].as_str().unwrap().to_owned())
                .collect::<Vec<_>>()
        };
        let universal = json!({ "universalInputSpace": [1, 0, 0] });
        let (_, whatif) = send(
            &app,
            "POST",
            &format!("/api/machines/{id}/whatif-universal"),
            Some(universal.clone()),
        )
        .await;
        assert_eq!(
            whatif["sequenceResults"]["sequence"]["matchedEvents"],
            json!(["start"])
        );
        assert!(whatif.get("machineId").is_none());
        assert_eq!(
            active(app.clone(), id.clone()).await,
            ["start"],
            "a what-if persists nothing"
        );

        let (_, cp) = send(
            &app,
            "POST",
            &format!("/api/machines/{id}/checkpoints"),
            Some(json!({ "label": "start" })),
        )
        .await;
        let cp = cp["checkpointId"].as_str().unwrap().to_owned();
        let (_, list) = send(
            &app,
            "GET",
            &format!("/api/machines/{id}/checkpoints"),
            None,
        )
        .await;
        assert_eq!(list[0]["label"], "start");

        let (_, drive) = send(
            &app,
            "POST",
            &format!("/api/machines/{id}/process-universal"),
            Some(universal),
        )
        .await;
        assert_eq!(
            drive["inputEvent"],
            json!([1]),
            "sliced at the input region"
        );
        assert_eq!(active(app.clone(), id.clone()).await, ["finish", "start"]);
        send(
            &app,
            "POST",
            &format!("/api/machines/{id}/checkpoints/{cp}/restore"),
            None,
        )
        .await;
        assert_eq!(
            active(app.clone(), id.clone()).await,
            ["start"],
            "restored as it was running"
        );
        let (status, _) = send(
            &app,
            "POST",
            &format!("/api/machines/{id}/checkpoints/nope/restore"),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (_, gone) = send(
            &app,
            "DELETE",
            &format!("/api/machines/{id}/checkpoints/{cp}"),
            None,
        )
        .await;
        assert_eq!(gone, json!({ "success": true }));

        send(
            &app,
            "PUT",
            "/api/engine/config/transitionsInhibited",
            Some(json!({ "machine": id, "value": true })),
        )
        .await;
        let (_, patched) = send(
            &app,
            "PATCH",
            &format!("/api/machines/{id}"),
            Some(json!({ "description": "d", "metadata": { "k": 1 } })),
        )
        .await;
        assert_eq!(patched["machine"]["description"], "d");
        assert_eq!(patched["machine"]["metadata"]["k"], json!(1));
        let (_, control) = send(&app, "GET", "/api/engine/config/transitionsInhibited", None).await;
        assert_eq!(
            control["value"][&id],
            json!(true),
            "PATCH keeps the machine's control state"
        );

        let (status, _) = send(
            &app,
            "PUT",
            &format!("/api/machines/{id}"),
            Some(json!({ "version": "1.0.0", "machine": 7 })),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (_, imported) = send(
            &app,
            "POST",
            "/api/machines/json/import",
            Some(machine_definition()),
        )
        .await;
        assert_ne!(imported["machine"]["id"], json!(id), "import mints an id");
        let (_, all) = send(
            &app,
            "POST",
            "/api/machines/process-universal/all",
            Some(json!({ "universalInputSpace": [0] })),
        )
        .await;
        assert_eq!(all["results"].as_object().unwrap().len(), 2);
    }

    /// #13 slice 3: the engine's own sequence and vector stores.
    #[tokio::test]
    async fn sequences_and_vectors_are_engine_scoped_stores() {
        let app = app(Engine::new(8));
        let event = |id: &str, initial: bool, output: bool| {
            json!({
                "id": id, "isInitial": initial, "elements": [{ "value": 1 }],
                "outputEvents": if output { json!([{ "id": "o", "vector": [1] }]) } else { json!([]) },
            })
        };
        let (status, body) = send(
            &app,
            "POST",
            "/api/sequences",
            Some(json!({ "id": "s", "events": [event("a", false, false)] })),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            body["error"].as_array().unwrap().len(),
            2,
            "every failure, as an array"
        );
        let (_, created) = send(
            &app,
            "POST",
            "/api/sequences",
            Some(json!({ "id": "s", "name": "S", "events": [event("a", true, true)] })),
        )
        .await;
        assert_eq!(created["sequence"]["initialEventIds"], json!(["a"]));
        assert_eq!(created["sequence"]["events"][0]["matchAlgorithm"], "gte");
        send(
            &app,
            "POST",
            "/api/sequences/s/vectors",
            Some(event("b", false, false)),
        )
        .await;
        let (_, got) = send(&app, "GET", "/api/sequences/s", None).await;
        assert_eq!(got["sequence"]["events"].as_array().unwrap().len(), 2);
        assert_eq!(
            send(&app, "POST", "/api/sequences/s/reset", None).await.0,
            StatusCode::OK
        );
        assert_eq!(
            send(&app, "DELETE", "/api/sequences/s", None).await.0,
            StatusCode::OK
        );
        assert_eq!(
            send(&app, "DELETE", "/api/sequences/s", None).await.0,
            StatusCode::NOT_FOUND
        );

        let (_, stored) = send(
            &app,
            "POST",
            "/api/vectors",
            Some(json!({ "vector": [1, 0] })),
        )
        .await;
        assert!(
            stored["vector"]["id"]
                .as_str()
                .unwrap()
                .starts_with("vector-")
        );
        send(
            &app,
            "POST",
            "/api/vectors",
            Some(json!({ "id": "w", "elements": [{ "value": 0 }, 1] })),
        )
        .await;
        let (_, found) = send(
            &app,
            "POST",
            "/api/vectors/search",
            Some(json!({ "vector": [1, 0], "threshold": 0.5 })),
        )
        .await;
        let results = found["results"].as_array().unwrap();
        assert_eq!(results.len(), 1, "w scores 0 and falls under the threshold");
        assert_eq!(results[0]["score"], json!(1));
        assert_eq!(
            send(&app, "GET", "/api/vectors/nope", None).await.0,
            StatusCode::NOT_FOUND
        );
        let (_, deleted) = send(&app, "DELETE", "/api/vectors/w", None).await;
        assert_eq!(deleted, json!({ "success": true, "id": "w" }));
    }
}
