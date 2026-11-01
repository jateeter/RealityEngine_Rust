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
//! - **Contention** is recorded by the push assembly only (`record_contention`);
//!   `GET /api/state` assembles for display and records nothing. A source on a
//!   cell the last push's `mergeBatch` wrote is **folded** with the OSRE value:
//!   by the cell's declared rule for a provider the arbitration registry names,
//!   else by the writing machine's `outputMergeTransformation` over `[0, 1]`
//!   (§4.4b, RealityEngine_CI#525).

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

    /// The contract provider (C++ `source_provider`): the first `.` segment of
    /// `origin` through the aliases; an empty origin or `signal` falls back
    /// to the kind.
    fn provider(&self) -> String {
        let origin = self.origin().to_lowercase();
        let head = origin.split('.').next().unwrap_or_default();
        match head {
            "" | "signal" => match self.detail {
                Detail::Sensor { .. } => "sensor",
                _ => "synthetic",
            }
            .to_owned(),
            "openclaw" => "acp".to_owned(),
            "ollama" | "localaistack" => "localai".to_owned(),
            other => other.to_owned(),
        }
    }

    fn origin(&self) -> &str {
        match &self.detail {
            Detail::Sensor { origin, .. } => origin,
            _ => "",
        }
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

/// Each declared operator's multi-valued form over `[0, 1]` (C++
/// `fold_unit_interval`); `or`, `join` and anything unrecognised are `max`.
fn fold_unit_interval(transformation: &str, s: f64, o: f64) -> f64 {
    match transformation {
        "and" | "meet" | "discrete-median" => s.min(o),
        "strong-disjunction" => (s + o).min(1.0),
        "strong-conjunction" => (s + o - 1.0).max(0.0),
        "xor" => s.min(1.0 - o).max((1.0 - s).min(o)),
        "nor" => 1.0 - s.max(o),
        "nand" => 1.0 - s.min(o),
        _ => s.max(o),
    }
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
        if let Detail::Simulated {
            pattern: "random-walk",
            dc_offset,
            ..
        } = source.detail
        {
            self.walk_state.insert(
                source.id.clone(),
                vec![dc_offset; source.region.length.max(0) as usize],
            );
        }
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
        self.walk_state.remove(id);
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
                        "random-walk" => self
                            .walk_state
                            .get(&source.id)
                            .and_then(|walk| walk.first())
                            .copied()
                            .unwrap_or(*dc_offset),
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
    /// written over its region in composition order, clamped to `[0, 1]`, and
    /// each source on an OSRE cell of the last push folded with it.
    pub(crate) fn assemble(&self) -> Vec<f64> {
        self.assemble_recording(None)
    }

    /// `assemble`, with every Source-vs-OSRE fold appended to `folds`,
    /// ascending by cell.
    fn assemble_recording(&self, mut folds: Option<&mut Vec<Value>>) -> Vec<f64> {
        let now = now();
        let mut out = self.persistent_vector.clone();
        // The source whose value landed in each cell: the last writer.
        let mut writer: Vec<Option<&Source>> = vec![None; out.len()];
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
                writer[cell as usize] = Some(source);
            }
        }
        for (cell, (machine, transformation)) in &self.osre_fold {
            let Some(source) = usize::try_from(*cell).ok().and_then(|c| *writer.get(c)?) else {
                continue;
            };
            let c = *cell as usize;
            let (s, o) = (out[c], self.persistent_vector[c]);
            let provider = source.provider();
            let entry = self.registry.entries.get(&c);
            let named = entry.is_some_and(|e| e.provider_ranks.contains_key(&provider));
            let (mut by_rule, mut osre_wins) = (false, false);
            if let Some(entry) = entry.filter(|e| named && e.rule == "PRECEDENCE") {
                let rank = |p: &str| {
                    entry
                        .provider_ranks
                        .get(p)
                        .map_or_else(|| step::determinism_rank(p), |r| *r as f64)
                };
                let (osre_rank, source_rank) = (rank("machine"), rank(&provider));
                by_rule = osre_rank != source_rank;
                osre_wins = osre_rank > source_rank;
            }
            let resolved = if by_rule {
                if osre_wins { o } else { s }
            } else {
                fold_unit_interval(transformation, s, o)
            };
            out[c] = resolved.clamp(0.0, 1.0);
            let Some(folds) = folds.as_deref_mut() else {
                continue;
            };
            let mut record = json!({
                "cell": cell,
                "osre": { "machine": machine, "provider": "machine", "value": o },
                "source": { "id": source.id, "name": source.name, "kind": source.kind(), "provider": provider, "value": s },
                "resolved": resolved,
            });
            if by_rule {
                record["resolution"] = json!("declared-rule");
                record["rule"] = json!(entry.map(|e| e.rule.clone()));
                record["kept"] = json!(if osre_wins { "osre" } else { "source" });
            } else {
                record["resolution"] = json!("osre-fold");
                record["operator"] = json!(transformation);
                if let Some(entry) = entry {
                    record["declaredRule"] = json!(entry.rule);
                    if !named {
                        record["review"] = json!("provider-unranked");
                    }
                }
                record["kept"] = json!(if resolved == o && resolved == s {
                    "both"
                } else if resolved == o {
                    "osre"
                } else if resolved == s {
                    "source"
                } else {
                    "combined"
                });
            }
            folds.push(record);
        }
        out
    }

    /// Cells written by more than one active source, resolved as `assemble`
    /// resolves them (C++ `source_contention`). Pure.
    fn source_contention(&self) -> Vec<Value> {
        let now = now();
        let size = self.persistent_vector.len() as i64;
        let mut writers: BTreeMap<i64, Vec<&Source>> = BTreeMap::new();
        for source in self.composition_order() {
            let values = self.source_values(source, now);
            for i in 0..(source.region.length.max(0) as usize).min(values.len()) {
                let cell = source.region.offset + i as i64;
                if (0..size).contains(&cell) {
                    writers.entry(cell).or_default().push(source);
                }
            }
        }
        let reference = |s: &Source| json!({ "id": s.id, "name": s.name, "kind": s.kind(), "activatedAt": s.activated_at });
        writers
            .into_iter()
            .filter(|(_, ws)| ws.len() > 1)
            .map(|(cell, mut ws)| {
                let winner = ws.pop().expect("two or more writers");
                ws.sort_by(|a, b| (&a.name, &a.id).cmp(&(&b.name, &b.id)));
                let winner_live = winner.kind() != "test";
                let same_tier = ws.iter().any(|l| (l.kind() != "test") == winner_live);
                json!({
                    "cell": cell,
                    "resolution": if same_tier { "incumbent" } else { "live-over-seed" },
                    "winner": reference(winner),
                    "suppressed": ws.iter().map(|l| reference(l)).collect::<Vec<_>>(),
                })
            })
            .collect()
    }

    /// The push is the transition: record what this assembly resolved, and
    /// count it toward each source (C++ `record_contention`).
    pub(crate) fn record_contention(&mut self) {
        let cells = self.source_contention();
        let mut folds = Vec::new();
        self.assemble_recording(Some(&mut folds));
        let mut contended = std::collections::BTreeSet::new();
        let mut lost = std::collections::BTreeSet::new();
        for fold in &folds {
            let id = fold["source"]["id"].as_str().unwrap_or_default().to_owned();
            if fold["kept"] == "osre" {
                lost.insert(id.clone());
            }
            contended.insert(id);
        }
        for cell in &cells {
            contended.insert(cell["winner"]["id"].as_str().unwrap_or_default().to_owned());
            for loser in cell["suppressed"].as_array().into_iter().flatten() {
                let id = loser["id"].as_str().unwrap_or_default().to_owned();
                contended.insert(id.clone());
                lost.insert(id);
            }
        }
        for id in contended {
            let counter = self.contention.counters.entry(id.clone()).or_default();
            counter.0 += 1;
            if lost.contains(&id) {
                counter.1 += 1;
            }
        }
        self.contention.transition = self.global_step;
        self.contention.cells = cells;
        self.contention.folds = folds;
    }

    /// The next transition (C++ `advance`): each active test source steps its
    /// cursor, a finished one looping or going inactive; each random walk
    /// moves.
    pub(crate) fn advance(&mut self) {
        self.global_step += 1;
        let ids: Vec<String> = self.sources.keys().cloned().collect();
        for id in ids {
            let source = &self.sources[&id];
            if !source.active {
                continue;
            }
            match &source.detail {
                Detail::Test { inputs, loop_, .. } => {
                    let (count, loops) = (inputs.len(), *loop_);
                    let next = self.test_step.get(&id).copied().unwrap_or(0) + 1;
                    if next >= count {
                        self.test_step.insert(id.clone(), 0);
                        if !loops {
                            self.sources.get_mut(&id).expect("listed").active = false;
                        }
                    } else {
                        self.test_step.insert(id.clone(), next);
                    }
                }
                Detail::Simulated {
                    pattern: "random-walk",
                    ..
                } => {
                    let steps: Vec<f64> = (0..self.walk_state.get(&id).map_or(0, Vec::len))
                        .map(|_| (self.uniform() * 2.0 - 1.0) * 0.05)
                        .collect();
                    if let Some(walk) = self.walk_state.get_mut(&id) {
                        for (value, step) in walk.iter_mut().zip(steps) {
                            *value = (*value + step).clamp(0.0, 1.0);
                        }
                    }
                }
                _ => {}
            }
        }
    }

    /// The perceptual space the RE handed back becomes the persistent vector,
    /// growing this PE to its width (C++ `update_from_perceptual_space`).
    pub(crate) fn adopt_space(&mut self, values: &[f64]) {
        self.ensure_capacity(values.len() as i64);
        self.persistent_vector = vec![0.0; self.dimension];
        for (cell, value) in self.persistent_vector.iter_mut().zip(values) {
            *cell = *value;
        }
    }

    /// Reset's half for sources: cursors rewound, walks restarted, activity
    /// validated against one clock reading (never the prior flag), every
    /// source at instant 0, contention and the OSRE fold emptied. Membership
    /// and sensor values are untouched.
    pub(crate) fn reset_sources(&mut self) {
        let now = now();
        for (id, source) in &mut self.sources {
            self.test_step.insert(id.clone(), 0);
            if let Detail::Simulated {
                pattern: "random-walk",
                dc_offset,
                ..
            } = source.detail
            {
                self.walk_state.insert(
                    id.clone(),
                    vec![dc_offset; source.region.length.max(0) as usize],
                );
            }
            source.active = source.validated(now);
            source.activated_at = 0;
        }
        self.contention = Contention::default();
        self.osre_fold.clear();
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
    state.broadcast_state();
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
    state.broadcast_state();
    Ok(Json(json!({ "source": added.to_json(now()) })))
}

pub(crate) async fn remove(State(pe): State<SharedPe>, Path(id): Path<String>) -> Answer {
    let mut state = write(&pe)?;
    if !state.remove_source(&id) {
        return Err(not_found());
    }
    state.broadcast_state();
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
    let mut state = write(&pe)?;
    if !state.update_sensor(&sensor_id, values) {
        return Err(error_response(EngineError::not_found(format!(
            "No sensor source with sensorId \"{sensor_id}\""
        ))));
    }
    state.broadcast_state();
    Ok(Json(
        json!({ "success": true, "sensorId": sensor_id, "timestamp": now_ms() }),
    ))
}

impl PerceptionEngine {
    /// C++ `sensor_exists`.
    pub(crate) fn has_sensor(&self, sensor_id: &str) -> bool {
        self.sensor_by_id(sensor_id).is_some()
    }

    /// The first sensor, in listing order, with that `sensorId` (C++
    /// `sensor_source_by_id`).
    fn sensor_by_id(&self, sensor_id: &str) -> Option<Source> {
        self.sources_in_order()
            .into_iter()
            .find(|s| matches!(&s.detail, Detail::Sensor { sensor_id: id, .. } if id == sensor_id))
            .cloned()
    }

    /// C++ `ingest_signal`, without the push: a value for a sensor named by
    /// `sensorId`, or a sensor registered for it. An unknown `sensorId` with a
    /// `region` registers a sensor under that id; a `region` alone registers
    /// one under a minted id. Either is live from the value it arrives with.
    fn ingest_signal(&mut self, body: &Value) -> Result<Option<Source>, EngineError> {
        let values = numbers(&body["values"]);
        if values.is_empty() {
            return Err(EngineError::bad_request("values must be a non-empty array"));
        }
        let origin = body["origin"].as_str().unwrap_or("signal").to_owned();
        let region = &body["region"];
        let sensor = |id: String, name: String, sensor_id: String| Source {
            id,
            name,
            region: Region {
                offset: region["offset"].as_f64().unwrap_or(0.0) as i64,
                length: region["length"].as_f64().unwrap_or(0.0) as i64,
            },
            active: true,
            activated_at: 0,
            detail: Detail::Sensor {
                sensor_id,
                last_value: values.clone(),
                last_updated: Some(now()),
                ttl_ms: body["ttlMs"].as_f64().unwrap_or(30000.0) as i64,
                origin: origin.clone(),
            },
        };
        if let Some(sensor_id) = body["sensorId"].as_str() {
            if self.update_sensor(sensor_id, values.clone()) {
                return Ok(self.sensor_by_id(sensor_id));
            }
            if !region.is_object() {
                return Err(EngineError::not_found(format!(
                    "No sensor source with sensorId \"{sensor_id}\""
                )));
            }
            // A declared sensorId is the source's identity on every runtime
            // (RealityEngine_CI#518); only undeclared identity is minted.
            let name = body["name"].as_str().unwrap_or(sensor_id).to_owned();
            let source = sensor(sensor_id.to_owned(), name, sensor_id.to_owned());
            return Ok(Some(self.add_source(source)));
        }
        if !region.is_object() {
            return Err(EngineError::bad_request(
                "signal requires sensorId or region",
            ));
        }
        let name = body["name"]
            .as_str()
            .unwrap_or("external/signal")
            .to_owned();
        let sensor_id = body["name"].as_str().map_or_else(
            || {
                format!(
                    "external-sensor-{}",
                    new_machine_id().trim_start_matches("machine-")
                )
            },
            str::to_owned,
        );
        Ok(Some(self.add_source(sensor(
            String::new(),
            name,
            sensor_id,
        ))))
    }
}

/// `POST /api/signals`: a value from an integration, and with `triggerPush`
/// the push it asks for, nested under `push` (`compactPush` trims it).
pub(crate) async fn signal(State(pe): State<SharedPe>, body: Bytes) -> Answer {
    let body = body_of(&body)?;
    let source = write(&pe)?.ingest_signal(&body).map_err(error_response)?;
    let mut response = json!({
        "success": true,
        "timestamp": now_ms(),
        "source": source.map(|s| s.to_json(now())),
    });
    if body["triggerPush"].as_bool().unwrap_or(false) {
        let include = !body["compactPush"].as_bool().unwrap_or(false);
        response["push"] = push::push_json(&pe, include, false, Value::Null).await.1;
    }
    read(&pe)?.broadcast_state();
    Ok(Json(response))
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
    let summary = bootstrap_from_re(&pe, &re_url).await.to_json();
    read(&pe)?.broadcast_state();
    Ok(Json(summary))
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
