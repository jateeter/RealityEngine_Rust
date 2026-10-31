use axum::{
    Json, Router,
    body::Bytes,
    extract::Query,
    extract::{Path, State, rejection::JsonRejection},
    http::StatusCode,
    routing::{get, post, put},
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
    history: VecDeque<Value>,
    qdrant_url: String,
    collection_name: String,
    /// The corpus directory machines are loaded from; the on-disk catalog
    /// (`GET /api/machines/json/list`) and load-by-name read it.
    machines_dir: Option<std::path::PathBuf>,
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
}

impl Event {
    /// SURFACE_SPEC.md § "Comparators" (owner ruling on RealityEngine_CI#606):
    /// the element's `comparatorType`, else the machine's `matchAlgorithm`,
    /// else `gte`; `t` is the element's `threshold`, else the comparator's
    /// default. `gte` binarizes both sides at `t`: a `0` means "must be off".
    fn matches(&self, input: &[f64], machine_algorithm: &str) -> bool {
        if input.len() != self.elements.len() {
            return false;
        }
        self.elements.iter().zip(input).all(|(element, &actual)| {
            let comparator = if element.comparator.is_empty() {
                machine_algorithm
            } else {
                &element.comparator
            };
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
        let summary = machine.summary();
        self.machines.insert(machine.id.clone(), machine);
        Ok(summary)
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
        let result = machine.transition(input);
        self.record_history(result.clone());
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
                // SURFACE_SPEC.md, "POST /api/engine/process": outputs =
                // [r.machineOutput for r in results if r.machineOutput]. One
                // arbitrated output per machine that fired, the output itself;
                // the per-machine detail stays on POST /api/machines/:id/process.
                let mut result = machine.transition(machine_input);
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
        for machine in self.machines.values_mut() {
            machine.reset();
        }
        self.step = 0;
        if clear_audit {
            self.history.clear();
        }
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
            get(get_machine).delete(delete_machine),
        )
        .route("/api/machines/{id}/process", post(process_machine))
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
        assert_eq!(output["vector"], json!([1.0]));
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
            assert_eq!(result["inputEvent"], vector);
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
                event.matches(&[input], "gte"),
                accepts,
                "{comparator} value {value} threshold {threshold:?} input {input}"
            );
        }
    }

    #[test]
    fn comparator_precedence_is_element_then_machine_then_gte() {
        // No element comparator: the machine's matchAlgorithm applies.
        let event = event_with("", 1.0, None);
        assert!(!event.matches(&[0.95], "equals"));
        assert!(event.matches(&[0.95], "gte"));
        // The element's own comparatorType wins over the machine's.
        let event = event_with("gte", 1.0, None);
        assert!(event.matches(&[0.95], "equals"));

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
}
