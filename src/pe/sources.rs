//! Sources and sensors (#20 workstream 2): registration, activity, the
//! canonical composition order, machine ingestion and the routes over them.
//!
//! C++ `PerceptionEngine` (`reality.cpp`) and `perception_engine_server.cpp`
//! are the concrete form this follows; SURFACE_SPEC.md, "Sources & Sensors"
//! and "Machine ingestion", is the contract.
//!
//! - **Activity is earned.** Registration derives `active` from the source's
//!   own state and never from the flag a caller asks for: a sensor is active
//!   iff it holds a value inside its TTL, a test source iff it has an interned
//!   sequence, a simulated source always. Only a sensor value (`POST
//!   /api/sensors/:sensorId`) originates a sensor's activity. An explicit
//!   `active: false` is honoured, as an operator's pause.
//! - **A read reports `stored AND validated`**, against one clock reading per
//!   payload, and never writes the flag back.
//! - **Composition is tiered** (`ARBITER_CONTRACT.md` §4.4b): seed (test)
//!   sources first, then live ones; within a tier newest activation first,
//!   then descending `(name, id)`, so the incumbent writes last and keeps the
//!   cell. Listings use plain `(name, id)` order.
//! - **Machine ingestion** interns each machine's `metadata.inputSequences`,
//!   end to end, as one test source over its input region, id `test-<machine
//!   id>`. A machine that already has one is skipped (skip-if-present);
//!   `PE_SOURCE_MERGE=false` rebuilds a source whose definition changed.
//! - **Contention** is recorded by the push assembly only. Until the push
//!   cycle lands nothing records, so `GET /api/sources/contention` reports
//!   transition 0 with no cells, folds or counters: what a PE that has not
//!   been pushed reports.

use super::*;
use std::sync::atomic::{AtomicU64, Ordering};

/// C++ `SimPattern`, by its wire name.
const PATTERNS: [&str; 8] = [
    "sine",
    "sawtooth",
    "square",
    "linear-ramp",
    "random-walk",
    "constant",
    "gaussian-noise",
    "binary",
];

#[derive(Clone, PartialEq)]
pub(crate) struct Region {
    pub(crate) offset: i64,
    pub(crate) length: i64,
}

impl Region {
    fn to_json(&self) -> Value {
        json!({ "offset": self.offset, "length": self.length })
    }

    fn end(&self) -> i64 {
        self.offset + self.length
    }
}

#[derive(Clone)]
pub(crate) enum Detail {
    Test {
        machine_id: String,
        machine_name: String,
        sequence_name: String,
        inputs: Vec<Vec<f64>>,
        loop_: bool,
        metadata: Value,
        sequence: Value,
    },
    Sensor {
        sensor_id: String,
        last_value: Vec<f64>,
        last_updated: Option<i64>,
        ttl_ms: i64,
        origin: String,
    },
    Simulated {
        pattern: &'static str,
        frequency: f64,
        amplitude: f64,
        dc_offset: f64,
    },
}

#[derive(Clone)]
pub(crate) struct Source {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) region: Region,
    /// The stored flag; a read reports it validated (`reported_active`).
    pub(crate) active: bool,
    /// The `globalStep` at which the source last became active (§4.4b).
    pub(crate) activated_at: u64,
    pub(crate) detail: Detail,
}

/// The numbers of `value`, a non-number read as 0 (C++ `json::to_numbers`).
fn numbers(value: &Value) -> Vec<f64> {
    machine_routes::numbers(value)
}

fn pattern(name: &str) -> Result<&'static str, EngineError> {
    PATTERNS
        .iter()
        .find(|p| **p == name)
        .copied()
        .ok_or_else(|| EngineError::internal(format!("Unknown sim pattern: {name}")))
}

fn now() -> i64 {
    now_ms() as i64
}

impl Source {
    pub(crate) fn kind(&self) -> &'static str {
        match self.detail {
            Detail::Test { .. } => "test",
            Detail::Sensor { .. } => "sensor",
            Detail::Simulated { .. } => "simulated",
        }
    }

    /// C++ `source_from_json`.
    fn from_json(j: &Value) -> Result<Self, EngineError> {
        let text = |key: &str| j[key].as_str().unwrap_or("").to_owned();
        let number = |key: &str, default: f64| j[key].as_f64().unwrap_or(default);
        let kind = j["type"]
            .as_str()
            .or_else(|| j["kind"].as_str())
            .unwrap_or("simulated");
        let detail = match kind {
            "test" => Detail::Test {
                machine_id: text("machineId"),
                machine_name: text("machineName"),
                sequence_name: text("sequenceName"),
                inputs: j["inputs"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(numbers)
                    .collect(),
                loop_: j["loop"].as_bool().unwrap_or(true),
                metadata: j
                    .get("metadata")
                    .filter(|m| m.is_object())
                    .cloned()
                    .unwrap_or_else(|| json!({})),
                sequence: j
                    .get("sequence")
                    .filter(|m| m.is_object())
                    .cloned()
                    .unwrap_or_else(|| json!({})),
            },
            "sensor" => Detail::Sensor {
                sensor_id: text("sensorId"),
                last_value: numbers(&j["lastValue"]),
                last_updated: j["lastUpdated"].as_f64().map(|n| n as i64),
                ttl_ms: number("ttlMs", 5000.0) as i64,
                origin: text("origin"),
            },
            _ => Detail::Simulated {
                pattern: pattern(j["pattern"].as_str().unwrap_or("constant"))?,
                frequency: number("frequency", 1.0),
                amplitude: number("amplitude", 1.0),
                dc_offset: number("dcOffset", 0.0),
            },
        };
        let region = &j["region"];
        Ok(Self {
            id: text("id"),
            name: j["name"].as_str().unwrap_or("source").to_owned(),
            region: if region.is_object() {
                Region {
                    offset: region["offset"].as_f64().unwrap_or(0.0) as i64,
                    length: region["length"].as_f64().unwrap_or(0.0) as i64,
                }
            } else {
                Region {
                    offset: 0,
                    length: 0,
                }
            },
            active: j["active"].as_bool().unwrap_or(true),
            activated_at: 0,
            detail,
        })
    }

    /// C++ `merge_source_patch`: the fields a PATCH may change, by kind.
    fn patched(mut self, patch: &Value) -> Result<Self, EngineError> {
        if let Some(name) = patch["name"].as_str() {
            self.name = name.to_owned();
        }
        if let Some(active) = patch["active"].as_bool() {
            self.active = active;
        }
        if patch["region"].is_object() {
            let region = &patch["region"];
            self.region.offset = region["offset"]
                .as_f64()
                .map_or(self.region.offset, |n| n as i64);
            self.region.length = region["length"]
                .as_f64()
                .map_or(self.region.length, |n| n as i64);
        }
        match &mut self.detail {
            Detail::Test { loop_, .. } => {
                if let Some(value) = patch["loop"].as_bool() {
                    *loop_ = value;
                }
            }
            Detail::Sensor {
                sensor_id, ttl_ms, ..
            } => {
                if let Some(value) = patch["sensorId"].as_str() {
                    *sensor_id = value.to_owned();
                }
                if let Some(value) = patch["ttlMs"].as_f64() {
                    *ttl_ms = value as i64;
                }
            }
            Detail::Simulated {
                pattern: p,
                frequency,
                amplitude,
                dc_offset,
            } => {
                if let Some(value) = patch["pattern"].as_str() {
                    *p = pattern(value)?;
                }
                for (key, field) in [
                    ("frequency", frequency),
                    ("amplitude", amplitude),
                    ("dcOffset", dc_offset),
                ] {
                    if let Some(value) = patch[key].as_f64() {
                        *field = value;
                    }
                }
            }
        }
        Ok(self)
    }

    /// A sensor holding a value inside its TTL (C++ `sensor_value_is_live`).
    fn live(&self, now: i64) -> bool {
        match &self.detail {
            Detail::Sensor {
                last_updated,
                ttl_ms,
                ..
            } => last_updated.is_some_and(|at| now - at <= *ttl_ms),
            _ => false,
        }
    }

    /// The activity the rules for its kind give it, now.
    fn validated(&self, now: i64) -> bool {
        match &self.detail {
            Detail::Sensor { .. } => self.live(now),
            Detail::Test { inputs, .. } => !inputs.is_empty(),
            Detail::Simulated { .. } => true,
        }
    }

    /// `stored AND validated` (RealityEngine_CI#175).
    fn reported_active(&self, now: i64) -> bool {
        self.active && self.validated(now)
    }

    /// C++ `to_json(SourceConfig, now)`.
    pub(crate) fn to_json(&self, now: i64) -> Value {
        let mut out = json!({
            "id": self.id,
            "name": self.name,
            "region": self.region.to_json(),
            "active": self.reported_active(now),
            "type": self.kind(),
        });
        match &self.detail {
            Detail::Test {
                machine_id,
                machine_name,
                sequence_name,
                inputs,
                loop_,
                metadata,
                sequence,
            } => {
                out["machineId"] = json!(machine_id);
                out["machineName"] = json!(machine_name);
                out["sequenceName"] = json!(sequence_name);
                out["inputs"] = json!(inputs);
                out["loop"] = json!(loop_);
                out["metadata"] = metadata.clone();
                out["sequence"] = sequence.clone();
            }
            Detail::Sensor {
                sensor_id,
                last_value,
                last_updated,
                ttl_ms,
                origin,
            } => {
                out["sensorId"] = json!(sensor_id);
                out["lastValue"] = json!(last_value);
                out["lastUpdated"] = json!(last_updated);
                out["ttlMs"] = json!(ttl_ms);
                if !origin.is_empty() {
                    out["origin"] = json!(origin);
                }
            }
            Detail::Simulated {
                pattern,
                frequency,
                amplitude,
                dc_offset,
            } => {
                out["pattern"] = json!(pattern);
                out["frequency"] = json!(frequency);
                out["amplitude"] = json!(amplitude);
                out["dcOffset"] = json!(dc_offset);
            }
        }
        out
    }
}

/// `GET /api/sources/contention`'s record. Only the push assembly writes it.
#[derive(Default)]
pub(crate) struct Contention {
    pub(crate) transition: u64,
    pub(crate) cells: Vec<Value>,
    pub(crate) folds: Vec<Value>,
    /// Source id -> (contended, suppressed).
    pub(crate) counters: BTreeMap<String, (u64, u64)>,
}

/// A lowercase id fragment (C++ `source_id_part`).
fn id_part(value: &str) -> String {
    let mut out = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() {
            out.push(byte.to_ascii_lowercase() as char);
        } else if !out.is_empty() && !out.ends_with('-') {
            out.push('-');
        }
    }
    while out.ends_with('-') {
        out.pop();
    }
    if out.is_empty() {
        "unnamed".to_owned()
    } else {
        out
    }
}

/// `{created, errors, machinesSeen, skipped, success}`.
#[derive(Default)]
pub(crate) struct Bootstrap {
    created: usize,
    skipped: usize,
    machines_seen: usize,
    errors: Vec<String>,
}

impl Bootstrap {
    pub(crate) fn to_json(&self) -> Value {
        json!({
            "created": self.created,
            "errors": self.errors,
            "machinesSeen": self.machines_seen,
            "skipped": self.skipped,
            "success": true,
        })
    }

    pub(crate) fn failed(error: String) -> Self {
        Self {
            errors: vec![error],
            ..Self::default()
        }
    }
}

impl PerceptionEngine {
    /// Grow the space to cover `[0, end)` (C++ `ensure_capacity`).
    fn ensure_capacity(&mut self, end: i64) {
        if end > self.dimension as i64 {
            self.dimension = end as usize;
            self.persistent_vector.resize(self.dimension, 0.0);
        }
    }

    /// C++ `add_source`: activity derived, the activation instant kept for a
    /// source that stays active, the space grown to its region, its cursor
    /// rewound.
    fn add_source(&mut self, mut source: Source) -> Source {
        if source.id.is_empty() {
            source.id = format!("source-{}", new_machine_id().trim_start_matches("machine-"));
        }
        source.active = source.validated(now());
        let continuing = self
            .sources
            .get(&source.id)
            .is_some_and(|previous| previous.active && source.active);
        source.activated_at = if continuing {
            self.sources[&source.id].activated_at
        } else {
            self.global_step
        };
        self.ensure_capacity(source.region.end());
        self.test_step.insert(source.id.clone(), 0);
        self.sources.insert(source.id.clone(), source.clone());
        source
    }

    fn deactivate(&mut self, id: &str) -> Option<Source> {
        let source = self.sources.get_mut(id)?;
        source.active = false;
        Some(source.clone())
    }

    fn remove_source(&mut self, id: &str) -> bool {
        self.test_step.remove(id);
        self.contention.counters.remove(id);
        self.sources.remove(id).is_some()
    }

    /// Listing order: `(name, id)`.
    pub(crate) fn sources_in_order(&self) -> Vec<&Source> {
        let mut sources: Vec<&Source> = self.sources.values().collect();
        sources.sort_by(|a, b| (&a.name, &a.id).cmp(&(&b.name, &b.id)));
        sources
    }

    pub(crate) fn sources_json(&self) -> Vec<Value> {
        let now = now();
        self.sources_in_order()
            .iter()
            .map(|s| s.to_json(now))
            .collect()
    }

    /// C++ `update_sensor_value`: the first sensor (in id order) with that
    /// `sensorId` takes the value, and earns activity by it.
    fn update_sensor(&mut self, sensor_id: &str, values: Vec<f64>) -> bool {
        let step = self.global_step;
        for source in self.sources.values_mut() {
            let Detail::Sensor {
                sensor_id: id,
                last_value,
                last_updated,
                ..
            } = &mut source.detail
            else {
                continue;
            };
            if id != sensor_id {
                continue;
            }
            *last_value = values;
            *last_updated = Some(now());
            if !source.active {
                source.activated_at = step;
            }
            source.active = true;
            return true;
        }
        false
    }

    /// Active sources in composition order (C++ `active_sources_canonical`):
    /// the seed tier first, then live; within a tier newest activation first,
    /// then descending `(name, id)`, so the incumbent writes last.
    fn composition_order(&self) -> Vec<&Source> {
        let mut active: Vec<&Source> = self.sources.values().filter(|s| s.active).collect();
        active.sort_by(|a, b| {
            let (a_live, b_live) = (a.kind() != "test", b.kind() != "test");
            a_live
                .cmp(&b_live)
                .then(b.activated_at.cmp(&a.activated_at))
                .then((&b.name, &b.id).cmp(&(&a.name, &a.id)))
        });
        active
    }

    /// What a source writes now (C++ `source_values`).
    fn source_values(&self, source: &Source, now: i64) -> Vec<f64> {
        let length = source.region.length.max(0) as usize;
        match &source.detail {
            Detail::Test { inputs, .. } => {
                let step = self.test_step.get(&source.id).copied().unwrap_or(0);
                inputs
                    .get(step)
                    .cloned()
                    .unwrap_or_else(|| vec![0.0; length])
            }
            Detail::Sensor { last_value, .. } => {
                let mut out = vec![0.0; length];
                if source.live(now) {
                    for (cell, value) in out.iter_mut().zip(last_value) {
                        *cell = *value;
                    }
                }
                out
            }
            Detail::Simulated {
                pattern,
                frequency,
                amplitude,
                dc_offset,
            } => (0..length)
                .map(|i| {
                    let t = self.global_step as f64 + i as f64 * 0.1;
                    let period = if *frequency > 0.0 {
                        1.0 / frequency
                    } else {
                        1.0
                    };
                    let phase = (t / period) % 1.0;
                    match *pattern {
                        "sine" => {
                            dc_offset + amplitude * (2.0 * std::f64::consts::PI * phase).sin()
                        }
                        "sawtooth" => dc_offset + amplitude * (2.0 * phase - 1.0),
                        "square" => dc_offset + amplitude * if phase < 0.5 { 1.0 } else { -1.0 },
                        "linear-ramp" => dc_offset + amplitude * phase,
                        // The walk starts at the offset and moves only when
                        // a push advances it.
                        "random-walk" => *dc_offset,
                        "gaussian-noise" => dc_offset + amplitude * (self.uniform() * 2.0 - 1.0),
                        "binary" => {
                            if phase < 0.5 {
                                1.0
                            } else {
                                0.0
                            }
                        }
                        _ => *dc_offset,
                    }
                })
                .collect(),
        }
    }

    /// A uniform draw in `[0, 1)` (xorshift; C++ uses `std::rand`).
    fn uniform(&self) -> f64 {
        let mut x = self.rng.load(Ordering::Relaxed);
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng.store(x, Ordering::Relaxed);
        (x >> 11) as f64 / (1_u64 << 53) as f64
    }

    /// C++ `assemble_vector`: the persistent vector with each active source
    /// written over its region in composition order, clamped to `[0, 1]`.
    pub(crate) fn assemble(&self) -> Vec<f64> {
        let now = now();
        let mut out = self.persistent_vector.clone();
        for source in self.composition_order() {
            let values = self.source_values(source, now);
            for (i, value) in values
                .iter()
                .enumerate()
                .take(source.region.length.max(0) as usize)
            {
                let cell = source.region.offset + i as i64;
                if cell < 0 || cell >= out.len() as i64 {
                    continue;
                }
                out[cell as usize] = value.clamp(0.0, 1.0);
            }
        }
        out
    }

    /// Reset's half for sources: cursors rewound, activity validated against
    /// one clock reading (never the prior flag), every source at instant 0,
    /// contention emptied. Membership and sensor values are untouched.
    pub(crate) fn reset_sources(&mut self) {
        let now = now();
        for (id, source) in &mut self.sources {
            self.test_step.insert(id.clone(), 0);
            source.active = source.validated(now);
            source.activated_at = 0;
        }
        self.contention = Contention::default();
    }

    pub(crate) fn contention_json(&self) -> Value {
        let counters: Vec<Value> = self
            .sources_in_order()
            .iter()
            .filter_map(|s| {
                let (contended, suppressed) = self.contention.counters.get(&s.id)?;
                Some(json!({ "id": s.id, "name": s.name, "contended": contended, "suppressed": suppressed }))
            })
            .collect();
        json!({
            "transition": self.contention.transition,
            "cells": self.contention.cells,
            "folds": self.contention.folds,
            "counters": counters,
        })
    }

    /// C++ `sync_test_sources_from_machine`: one test source per machine, its
    /// input sequences end to end over its input region. 1 when a source was
    /// created, else 0.
    fn intern(&mut self, machine: &Value) -> usize {
        let Some(machine_id) = machine["id"].as_str().filter(|id| !id.is_empty()) else {
            return 0;
        };
        let input = &machine["perceptualMapping"]["input"];
        if !machine["perceptualMapping"].is_object() || !input.is_object() {
            return 0;
        }
        let Some(sequences) = machine["metadata"]["inputSequences"].as_array() else {
            return 0;
        };
        let id = format!("test-{}", id_part(machine_id));
        let present = self.sources.get(&id);
        if present.is_some() && self.merge_only {
            return 0;
        }
        let machine_name = machine["name"].as_str().unwrap_or(machine_id).to_owned();
        let mut inputs = Vec::new();
        let mut segments = Vec::new();
        for sequence in sequences {
            let Some(events) = sequence["events"].as_array().filter(|e| !e.is_empty()) else {
                continue;
            };
            let name = sequence["name"]
                .as_str()
                .unwrap_or("Test sequence")
                .to_owned();
            inputs.extend(events.iter().map(numbers));
            segments.push((name, events.len()));
        }
        if inputs.is_empty() {
            return 0;
        }
        let sequence_name = match segments.as_slice() {
            [(only, _)] => only.clone(),
            many => format!("{} sequences", many.len()),
        };
        let source = Source {
            id: id.clone(),
            name: format!("{machine_name} / {sequence_name}"),
            region: Region {
                offset: input["offset"].as_f64().unwrap_or(0.0) as i64,
                length: input["length"].as_f64().unwrap_or(0.0) as i64,
            },
            active: false,
            activated_at: 0,
            detail: Detail::Test {
                machine_id: machine_id.to_owned(),
                machine_name,
                sequence_name,
                inputs,
                loop_: true,
                metadata: json!({ "segments": segments.iter().map(|(name, length)| json!({ "name": name, "length": length })).collect::<Vec<_>>() }),
                sequence: json!({}),
            },
        };
        // PE_SOURCE_MERGE=false: rebuilt only when the definition changed.
        if let Some(existing) = present
            && let (
                Detail::Test {
                    inputs: old,
                    loop_: old_loop,
                    ..
                },
                Detail::Test {
                    inputs: new,
                    loop_: new_loop,
                    ..
                },
            ) = (&existing.detail, &source.detail)
            && existing.region == source.region
            && old == new
            && old_loop == new_loop
        {
            return 0;
        }
        self.add_source(source);
        1
    }

    /// The machine list's interning (C++ `bootstrap_test_sources_from_machine_list`).
    pub(crate) fn bootstrap(&mut self, machines: &Value) -> Bootstrap {
        // Migration, as on C++: a machine holding more than one test source
        // (the old per-sequence form) loses them all, and is interned whole.
        let mut per_machine: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for source in self.sources.values() {
            if let Detail::Test { machine_id, .. } = &source.detail
                && !machine_id.is_empty()
            {
                per_machine
                    .entry(machine_id.clone())
                    .or_default()
                    .push(source.id.clone());
            }
        }
        for ids in per_machine.values().filter(|ids| ids.len() > 1) {
            for id in ids {
                self.remove_source(id);
            }
        }
        let mut summary = Bootstrap::default();
        for machine in machines["machines"].as_array().into_iter().flatten() {
            summary.machines_seen += 1;
            let added = self.intern(machine);
            summary.created += added;
            if added == 0 {
                summary.skipped += 1;
            }
        }
        summary
    }
}

pub(crate) fn new_rng() -> AtomicU64 {
    AtomicU64::new((now_ms() as u64) | 1)
}

type Answer = Result<Json<Value>, (StatusCode, Json<Value>)>;

fn not_found() -> (StatusCode, Json<Value>) {
    error_response(EngineError::not_found("Source not found"))
}

/// An explicit `active: false`: the one flag a caller may set (#43).
fn asks_for_deactivation(body: &Value) -> bool {
    body["active"] == json!(false)
}

pub(crate) async fn list(State(pe): State<SharedPe>) -> Answer {
    Ok(Json(json!({ "sources": read(&pe)?.sources_json() })))
}

pub(crate) async fn create(State(pe): State<SharedPe>, body: Bytes) -> Answer {
    let body = body_of(&body)?;
    let source = Source::from_json(&body).map_err(error_response)?;
    let mut state = write(&pe)?;
    let mut added = state.add_source(source);
    if asks_for_deactivation(&body) {
        added = state.deactivate(&added.id).unwrap_or(added);
    }
    Ok(Json(json!({ "source": added.to_json(now()) })))
}

pub(crate) async fn update(
    State(pe): State<SharedPe>,
    Path(id): Path<String>,
    body: Bytes,
) -> Answer {
    let body = body_of(&body)?;
    let mut state = write(&pe)?;
    let existing = state.sources.get(&id).cloned().ok_or_else(not_found)?;
    let mut updated = existing.patched(&body).map_err(error_response)?;
    updated.id = id;
    let mut added = state.add_source(updated);
    if asks_for_deactivation(&body) {
        added = state.deactivate(&added.id).unwrap_or(added);
    }
    Ok(Json(json!({ "source": added.to_json(now()) })))
}

pub(crate) async fn remove(State(pe): State<SharedPe>, Path(id): Path<String>) -> Answer {
    if !write(&pe)?.remove_source(&id) {
        return Err(not_found());
    }
    Ok(Json(json!({ "success": true })))
}

pub(crate) async fn contention(State(pe): State<SharedPe>) -> Answer {
    Ok(Json(read(&pe)?.contention_json()))
}

pub(crate) async fn sensor(
    State(pe): State<SharedPe>,
    Path(sensor_id): Path<String>,
    body: Bytes,
) -> Answer {
    let body = body_of(&body)?;
    let values = numbers(&body["values"]);
    if !write(&pe)?.update_sensor(&sensor_id, values) {
        return Err(error_response(EngineError::not_found(format!(
            "No sensor source with sensorId \"{sensor_id}\""
        ))));
    }
    Ok(Json(
        json!({ "success": true, "sensorId": sensor_id, "timestamp": now_ms() }),
    ))
}

/// `GET {re}/api/machines`, then the interning, outside the lock while the
/// RE answers. A failure is reported in `errors`, never as a failed request.
pub(crate) async fn bootstrap_from_re(pe: &SharedPe, re_url: &str) -> Bootstrap {
    let machines = match client::get(&format!("{re_url}/api/machines")).await {
        Ok(bytes) => match serde_json::from_slice::<Value>(&bytes) {
            Ok(machines) => machines,
            Err(error) => return Bootstrap::failed(error.to_string()),
        },
        Err(error) => return Bootstrap::failed(error),
    };
    match pe.write() {
        Ok(mut state) => state.bootstrap(&machines),
        Err(_) => Bootstrap::failed(lock_error().message),
    }
}

pub(crate) async fn bootstrap(State(pe): State<SharedPe>) -> Answer {
    let re_url = read(&pe)?.re_url.clone();
    Ok(Json(bootstrap_from_re(&pe, &re_url).await.to_json()))
}

/// The machines proxy: the RE's `GET /api/machines`, as it answered, or 502.
pub(crate) async fn machines(
    State(pe): State<SharedPe>,
) -> Result<Response, (StatusCode, Json<Value>)> {
    let re_url = read(&pe)?.re_url.clone();
    match client::get(&format!("{re_url}/api/machines")).await {
        Ok(bytes) => Ok((
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            bytes,
        )
            .into_response()),
        Err(error) => Err((StatusCode::BAD_GATEWAY, Json(json!({ "error": error })))),
    }
}
