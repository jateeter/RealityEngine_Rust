//! K-lines: episodes captured at their corpus boundaries while the engine
//! steps, kept in a persistent store per engine instance (RealityEngine_CI#603,
//! this repo's #81).
//!
//! SURFACE_SPEC.md governs: "Boundaries: where a K-line starts and ends",
//! "Paths that dwell, fork or merge", "Scope", "Identity, clock and parity",
//! "Managing K-lines during operation", "The K-line store" and "Record". This
//! is the RE half: the episode, its active lists, OSRE, OSRE(m) and scope.
//!
//! The tracker observes committed steps and never re-derives the transition
//! rule. At each step it reads, per sequence, which events matched and which
//! were activated (`sequenceResults`) and which are active afterwards (the
//! resident machines), and combines that with its own record of which K-line
//! holds which event. It follows C++ `kline.cpp` line for line, so the three
//! voters' records and this follower's are built the same way.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    io::Write,
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use serde_json::{Map, Value, json};

use crate::{Event, Machine, instance_clock::InstanceClock, step::TrajectoryEntry};

// ── JCS, SHA-256, the corpus fingerprint ────────────────────────────────────

/// RFC 8785 (JCS): keys in code-point order, numbers in ECMAScript form, the
/// minimal escapes. Keys here are ASCII, where UTF-8 byte order and JCS's
/// UTF-16 order agree.
pub(crate) fn jcs(value: &Value) -> String {
    let mut out = String::new();
    append_jcs(&mut out, value);
    out
}

fn append_jcs(out: &mut String, value: &Value) {
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Number(n) => out.push_str(&crate::ecmascript_number(n.as_f64().unwrap_or(0.0))),
        Value::String(s) => append_string(out, s),
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                append_jcs(out, item);
            }
            out.push(']');
        }
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort_by(|a, b| a.encode_utf16().cmp(b.encode_utf16()));
            out.push('{');
            for (i, key) in keys.into_iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                append_string(out, key);
                out.push(':');
                append_jcs(out, &map[key]);
            }
            out.push('}');
        }
    }
}

fn append_string(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Lower-case hex SHA-256.
pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    ring::digest::digest(&ring::digest::SHA256, bytes)
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// `sha256/resident-declared-v1` over (name, declared machine object) pairs.
pub(crate) fn corpus_fingerprint(declared: &[(String, Value)]) -> String {
    let mut lines: Vec<(String, String)> = declared
        .iter()
        .map(|(name, machine)| (name.clone(), sha256_hex(jcs(machine).as_bytes())))
        .collect();
    lines.sort();
    let all: String = lines
        .iter()
        .map(|(name, hex)| format!("{name}\t{hex}\n"))
        .collect();
    sha256_hex(all.as_bytes())
}

// ── Values ──────────────────────────────────────────────────────────────────

#[derive(Clone, Debug, Default)]
pub(crate) struct Stamp {
    pub(crate) instance: String,
    pub(crate) lamport: u64,
    pub(crate) commit: u64,
    pub(crate) step: i64,
}

impl Stamp {
    fn json(&self) -> Value {
        json!({ "instance": self.instance, "lamport": self.lamport, "commit": self.commit, "step": self.step })
    }

    fn from(v: &Value) -> Self {
        Self {
            instance: v["instance"].as_str().unwrap_or_default().to_owned(),
            lamport: number(&v["lamport"]) as u64,
            commit: number(&v["commit"]) as u64,
            step: number(&v["step"]) as i64,
        }
    }
}

fn number(v: &Value) -> f64 {
    v.as_f64().unwrap_or(0.0)
}

pub(crate) const FEEDBACK_HOPS_DEFAULT: u64 = 3;
pub(crate) const FEEDBACK_HOPS_MAX: u64 = 16;
pub(crate) const STORE_MAX_BYTES_DEFAULT: u64 = 2 * 1024 * 1024 * 1024;
pub(crate) const STORE_MAX_BYTES_MAX: u64 = 9_007_199_254_740_991;

#[derive(Clone, Debug)]
pub(crate) struct Controls {
    pub(crate) capture: bool,
    pub(crate) feedback_hops: u64,
    pub(crate) store_max_bytes: u64,
}

impl Default for Controls {
    fn default() -> Self {
        Self {
            capture: false,
            feedback_hops: FEEDBACK_HOPS_DEFAULT,
            store_max_bytes: STORE_MAX_BYTES_DEFAULT,
        }
    }
}

/// A region (offset, length).
pub(crate) type Region = (i64, i64);

/// One machine's operation in a step's mergeBatch, by corpus name.
#[derive(Clone, Debug)]
pub(crate) struct Op {
    pub(crate) machine: String,
    pub(crate) region: Region,
    pub(crate) values: Vec<f64>,
    pub(crate) sequence_ids: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
struct ActiveEntry {
    machine: String,
    sequence: String,
    event: String,
    just_matched: bool,
}

/// What the tracker reads at a step's completion point.
pub(crate) struct StepView<'a> {
    pub(crate) step: i64,
    pub(crate) commit: u64,
    pub(crate) machines: &'a BTreeMap<String, Machine>,
    /// `machineResults` by machine id: `transitionResult` and `inputEvent`.
    pub(crate) results: &'a Map<String, Value>,
    pub(crate) isre: &'a TrajectoryEntry,
    pub(crate) osre: &'a TrajectoryEntry,
    pub(crate) ops: Vec<Op>,
    pub(crate) fingerprint: String,
}

fn region_of(mapping: Option<&Value>) -> Region {
    let get = |k: &str| {
        mapping
            .and_then(|m| m.get(k))
            .and_then(Value::as_f64)
            .unwrap_or(0.0) as i64
    };
    (get("offset"), get("length"))
}

/// The machine's (input, output) regions, when it declares a mapping.
fn regions(m: &Machine) -> Option<(Region, Region)> {
    let mapping = m.raw.get("perceptualMapping").filter(|v| v.is_object())?;
    Some((
        region_of(mapping.get("input")),
        region_of(mapping.get("output")),
    ))
}

fn overlaps(a: Region, b: Region) -> bool {
    a.1 > 0 && b.1 > 0 && a.0 < b.0 + b.1 && b.0 < a.0 + a.1
}

fn entry_json(e: &TrajectoryEntry, keep: &[Region]) -> Value {
    let nz: Vec<Value> = e
        .non_zero
        .iter()
        .filter(|(index, _)| {
            let index = *index as i64;
            keep.is_empty() || keep.iter().any(|r| index >= r.0 && index < r.0 + r.1)
        })
        .map(|(index, value)| json!({ "index": index, "value": value }))
        .collect();
    json!({ "stepNumber": e.step_number, "length": e.length, "nonZero": nz })
}

fn actives_json(mut list: Vec<ActiveEntry>) -> Value {
    list.sort_by(|a, b| {
        (&a.machine, &a.sequence, &a.event).cmp(&(&b.machine, &b.sequence, &b.event))
    });
    Value::Array(
        list.into_iter()
            .map(|e| json!({ "machine": e.machine, "sequence": e.sequence, "event": e.event, "justMatched": e.just_matched }))
            .collect(),
    )
}

/// The comparator an element is held to, under C++'s names.
fn comparator_of(declared: &str, machine: &str) -> &'static str {
    let name = if declared.is_empty() {
        machine
    } else {
        declared
    };
    match name {
        "equals" | "custom" => "equals",
        "threshold" => "threshold",
        "pattern" => "pattern",
        _ => "gte",
    }
}

/// The first comparison that failed for `event` against `input`.
fn difference_of(event: &Event, machine_algorithm: &str, input: &[f64], region: Region) -> Value {
    let mut idx = 0;
    if input.len() == event.elements.len() {
        for (i, (element, &actual)) in event.elements.iter().zip(input).enumerate() {
            let comparator = comparator_of(&element.comparator, machine_algorithm);
            let v = element.value;
            let ok = match comparator {
                "equals" => actual == v,
                "threshold" => (v - actual).abs() <= element.threshold.unwrap_or(0.1),
                "pattern" => 1.0 - (v - actual).abs() >= element.threshold.unwrap_or(0.5),
                _ => {
                    let t = element.threshold.unwrap_or(0.5);
                    (actual >= t) == (v >= t)
                }
            };
            if !ok {
                idx = i;
                break;
            }
        }
    }
    let element = event.elements.get(idx);
    let comparator = element.map_or(comparator_of("", machine_algorithm), |e| {
        comparator_of(&e.comparator, machine_algorithm)
    });
    let threshold = match (comparator, element) {
        ("threshold", Some(e)) => json!(e.threshold.unwrap_or(0.1)),
        ("pattern" | "gte", Some(e)) => json!(e.threshold.unwrap_or(0.5)),
        _ => Value::Null,
    };
    json!({
        "expected": [event.id],
        "elementIndex": idx,
        "cell": region.0 + idx as i64,
        "value": input.get(idx).copied().unwrap_or(0.0),
        "comparator": comparator,
        "threshold": threshold,
        "cells": [region.0, region.0 + region.1 - 1],
    })
}

fn initial_actives(m: &Machine) -> Vec<ActiveEntry> {
    let mut out = Vec::new();
    for (sid, seq) in &m.sequences {
        for (eid, ev) in &seq.events {
            if ev.initial {
                out.push(ActiveEntry {
                    machine: m.name.clone(),
                    sequence: sid.clone(),
                    event: eid.clone(),
                    just_matched: false,
                });
            }
        }
    }
    out
}

fn actives_of(m: &Machine) -> Vec<ActiveEntry> {
    let mut out = Vec::new();
    for (sid, seq) in &m.sequences {
        for (eid, ev) in &seq.events {
            if ev.active {
                out.push(ActiveEntry {
                    machine: m.name.clone(),
                    sequence: sid.clone(),
                    event: eid.clone(),
                    just_matched: ev.was_just_matched,
                });
            }
        }
    }
    out
}

// ── The store ───────────────────────────────────────────────────────────────

const SEAL_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Clone, Debug, Default)]
pub(crate) struct Meta {
    id: String,
    machine: String,
    sequence: String,
    outcome: String,
    close_commit: u64,
    line: usize,
    segment: String,
    offset: u64,
}

#[derive(Clone, Debug, Default)]
struct Segment {
    name: String,
    bytes: u64,
    sealed: bool,
    exported: bool,
    sha256: String,
    fingerprint: String,
    archived: bool,
}

fn write_durable(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let tmp = PathBuf::from(format!("{}.tmp", path.display()));
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o644)
        .open(&tmp)
        .map_err(|e| format!("cannot write {}: {e}", tmp.display()))?;
    file.write_all(bytes)
        .map_err(|e| format!("write failed: {}: {e}", tmp.display()))?;
    file.sync_all()
        .map_err(|e| format!("fsync failed: {}: {e}", tmp.display()))?;
    fs::rename(&tmp, path).map_err(|e| format!("rename failed: {}: {e}", path.display()))
}

/// `segment-<commit>.jsonl` -> commit.
fn segment_commit(name: &str) -> Option<u64> {
    name.strip_prefix("segment-")?
        .strip_suffix(".jsonl")?
        .parse()
        .ok()
}

/// The append-only store under `<INSTANCE_KLINE_DIR>/<uuid>/`. Without a
/// directory it keeps its lines in memory (an instance whose UUID was minted).
#[derive(Default)]
pub(crate) struct Store {
    dir: Option<PathBuf>,
    segments: Vec<Segment>,
    metas: Vec<Meta>,
    by_id: BTreeMap<String, usize>,
    /// sustain-end events per K-line: (through, raw line)
    events: BTreeMap<String, Vec<(i64, String)>>,
    lines: Vec<String>,
    appends: u64,
    warnings: Arc<Mutex<Vec<String>>>,
    last_controls: Controls,
}

impl Store {
    pub(crate) fn open(dir: Option<PathBuf>) -> Result<Self, String> {
        let mut store = Self {
            dir,
            ..Self::default()
        };
        if let Some(dir) = store.dir.clone() {
            fs::create_dir_all(&dir)
                .map_err(|e| format!("cannot create {}: {e}", dir.display()))?;
            store.load(&dir)?;
        }
        Ok(store)
    }

    fn load(&mut self, dir: &Path) -> Result<(), String> {
        let mut known: BTreeMap<String, Segment> = BTreeMap::new();
        if let Ok(text) = fs::read_to_string(dir.join("manifest.json"))
            && let Ok(m) = serde_json::from_str::<Value>(&text)
        {
            for s in m["segments"].as_array().into_iter().flatten() {
                let seg = Segment {
                    name: s["segment"].as_str().unwrap_or_default().to_owned(),
                    sealed: s["sealed"].as_bool().unwrap_or(false),
                    exported: s["exported"].as_bool().unwrap_or(false),
                    sha256: s["sha256"].as_str().unwrap_or_default().to_owned(),
                    fingerprint: s["fingerprint"].as_str().unwrap_or_default().to_owned(),
                    archived: s["archived"].as_bool().unwrap_or(false),
                    bytes: 0,
                };
                known.insert(seg.name.clone(), seg);
            }
        }
        let mut names: Vec<String> = fs::read_dir(dir)
            .map_err(|e| format!("cannot read {}: {e}", dir.display()))?
            .filter_map(|e| e.ok().and_then(|e| e.file_name().into_string().ok()))
            .filter(|n| segment_commit(n).is_some())
            .collect();
        names.sort_by_key(|n| segment_commit(n));
        for name in names {
            let path = dir.join(&name);
            let mut bytes =
                fs::read(&path).map_err(|e| format!("cannot read {}: {e}", path.display()))?;
            // A half-written last record is truncated: everything after the last newline.
            let good = bytes.iter().rposition(|b| *b == b'\n').map_or(0, |i| i + 1);
            if good != bytes.len() {
                OpenOptions::new()
                    .write(true)
                    .open(&path)
                    .and_then(|f| f.set_len(good as u64))
                    .map_err(|e| format!("cannot truncate {}: {e}", path.display()))?;
                bytes.truncate(good);
            }
            let mut seg = known.remove(&name).unwrap_or_default();
            seg.name = name.clone();
            seg.bytes = bytes.len() as u64;
            let mut offset = 0u64;
            for line in bytes.split(|b| *b == b'\n') {
                if offset >= bytes.len() as u64 {
                    break;
                }
                if let Ok(text) = std::str::from_utf8(line) {
                    self.index_line(text, &name, offset, 0);
                }
                offset += line.len() as u64 + 1;
            }
            self.segments.push(seg);
        }
        self.write_index()
    }

    fn index_line(&mut self, raw: &str, segment: &str, offset: u64, line: usize) {
        let Ok(j) = serde_json::from_str::<Value>(raw) else {
            return; // an unparseable line inside a segment is not a record
        };
        match j["format"].as_str() {
            Some("re-kline/1") => {
                let meta = Meta {
                    id: j["id"].as_str().unwrap_or_default().to_owned(),
                    machine: j["focal"]["machine"]
                        .as_str()
                        .unwrap_or_default()
                        .to_owned(),
                    sequence: j["focal"]["sequence"]
                        .as_str()
                        .unwrap_or_default()
                        .to_owned(),
                    outcome: j["outcome"].as_str().unwrap_or_default().to_owned(),
                    close_commit: number(&j["close"]["commit"]) as u64,
                    line,
                    segment: segment.to_owned(),
                    offset,
                };
                self.by_id.insert(meta.id.clone(), self.metas.len());
                self.metas.push(meta);
            }
            Some("re-isre-composition/1") => {
                let commit = number(&j["re"]["commit"]) as u64;
                let meta = Meta {
                    id: commit.to_string(),
                    close_commit: commit,
                    line,
                    segment: segment.to_owned(),
                    offset,
                    ..Meta::default()
                };
                self.by_id.insert(meta.id.clone(), self.metas.len());
                self.metas.push(meta);
            }
            Some("re-kline-event/1") => {
                self.events
                    .entry(j["kline"].as_str().unwrap_or_default().to_owned())
                    .or_default()
                    .push((number(&j["through"]) as i64, raw.to_owned()));
            }
            _ => {}
        }
    }

    fn active_segment(&mut self, fingerprint: &str, commit: u64) -> Result<usize, String> {
        if let Some(last) = self.segments.last_mut()
            && !last.sealed
        {
            let other_corpus =
                !last.fingerprint.is_empty() && last.fingerprint != fingerprint && last.bytes > 0;
            if !other_corpus && last.bytes < SEAL_BYTES {
                if last.fingerprint.is_empty() {
                    last.fingerprint = fingerprint.to_owned();
                }
                return Ok(self.segments.len() - 1);
            }
            let i = self.segments.len() - 1;
            self.seal(i)?;
        }
        self.segments.push(Segment {
            name: format!("segment-{commit}.jsonl"),
            fingerprint: fingerprint.to_owned(),
            ..Segment::default()
        });
        self.write_manifest()?;
        Ok(self.segments.len() - 1)
    }

    fn seal(&mut self, i: usize) -> Result<(), String> {
        self.segments[i].sealed = true;
        if let Some(dir) = &self.dir {
            let bytes = fs::read(dir.join(&self.segments[i].name)).unwrap_or_default();
            self.segments[i].sha256 = sha256_hex(&bytes);
        }
        self.write_manifest()
    }

    /// Appends the step's lines (records and events, in order) and makes them
    /// durable before returning.
    fn append(&mut self, lines: &[Value], fingerprint: &str, commit: u64) -> Result<(), String> {
        if lines.is_empty() {
            return Ok(());
        }
        let si = self.active_segment(fingerprint, commit)?;
        let name = self.segments[si].name.clone();
        let mut bytes = String::new();
        let mut raws = Vec::new();
        let mut offset = self.segments[si].bytes;
        for line in lines {
            let raw = jcs(line);
            raws.push((raw.clone(), offset));
            offset += raw.len() as u64 + 1;
            bytes.push_str(&raw);
            bytes.push('\n');
        }
        if let Some(dir) = &self.dir {
            let path = dir.join(&name);
            let mut file = OpenOptions::new()
                .append(true)
                .create(true)
                .mode(0o644)
                .open(&path)
                .map_err(|e| format!("cannot append to {}: {e}", path.display()))?;
            file.write_all(bytes.as_bytes())
                .map_err(|e| format!("append failed: {}: {e}", path.display()))?;
            // Durable before the step's completion is published.
            file.sync_all()
                .map_err(|e| format!("fsync failed: {}: {e}", path.display()))?;
        }
        let persistent = self.dir.is_some();
        for (raw, off) in raws {
            let line = if persistent {
                0
            } else {
                self.lines.push(raw.clone());
                self.lines.len() - 1
            };
            self.index_line(&raw, &name, off, line);
        }
        self.segments[si].bytes += bytes.len() as u64;
        // index.json is a cache rebuilt at boot: every 64 appends, not every step.
        self.appends += 1;
        if self.appends.is_multiple_of(64) {
            self.write_index()?;
        }
        Ok(())
    }

    fn checkpoint(&self, open: &Value) -> Result<(), String> {
        match &self.dir {
            Some(dir) => write_durable(
                &dir.join("open.json"),
                format!("{}\n", jcs(open)).as_bytes(),
            ),
            None => Ok(()),
        }
    }

    fn read_checkpoint(&self) -> Option<Value> {
        let dir = self.dir.as_ref()?;
        serde_json::from_str(&fs::read_to_string(dir.join("open.json")).ok()?).ok()
    }

    fn raw_at(&self, m: &Meta) -> String {
        let Some(dir) = &self.dir else {
            return self.lines.get(m.line).cloned().unwrap_or_default();
        };
        let Ok(bytes) = fs::read(dir.join(&m.segment)) else {
            return String::new();
        };
        let start = (m.offset as usize).min(bytes.len());
        let end = bytes[start..]
            .iter()
            .position(|b| *b == b'\n')
            .map_or(bytes.len(), |i| start + i);
        String::from_utf8_lossy(&bytes[start..end]).into_owned()
    }

    fn raw_events(&self, id: &str) -> Vec<String> {
        self.events
            .get(id)
            .map(|list| list.iter().map(|(_, raw)| raw.clone()).collect())
            .unwrap_or_default()
    }

    fn record_at(&self, m: &Meta) -> Value {
        let mut j: Value = serde_json::from_str(&self.raw_at(m)).unwrap_or(Value::Null);
        // A sustain-end appended later belongs on the record as served.
        if let Some(list) = self.events.get(&m.id)
            && let Some((through, _)) = list.last()
        {
            j["sustain"] = json!({ "through": through, "ended": true });
        }
        j
    }

    fn record(&self, id: &str) -> Option<Value> {
        self.by_id.get(id).map(|i| self.record_at(&self.metas[*i]))
    }

    fn stats(&self, controls: &Controls) -> Value {
        let mut by_outcome: BTreeMap<String, u64> = BTreeMap::new();
        let mut by_machine: BTreeMap<String, BTreeMap<String, u64>> = BTreeMap::new();
        for m in &self.metas {
            *by_outcome.entry(m.outcome.clone()).or_default() += 1;
            *by_machine
                .entry(m.machine.clone())
                .or_default()
                .entry(m.outcome.clone())
                .or_default() += 1;
        }
        let live: Vec<&Segment> = self.segments.iter().filter(|s| !s.archived).collect();
        json!({
            "byOutcome": by_outcome,
            "byMachine": by_machine,
            "segments": live.len(),
            "bytes": live.iter().map(|s| s.bytes).sum::<u64>(),
            "budget": controls.store_max_bytes,
            "warnings": self.compute_warnings(controls),
        })
    }

    fn compute_warnings(&self, controls: &Controls) -> Vec<String> {
        let sealed: u64 = self
            .segments
            .iter()
            .filter(|s| s.sealed && !s.archived)
            .map(|s| s.bytes)
            .sum();
        let unexported = self
            .segments
            .iter()
            .filter(|s| s.sealed && !s.archived && !s.exported)
            .count();
        if sealed > controls.store_max_bytes && unexported > 0 {
            vec![format!(
                "klineStoreMaxBytes reached: {unexported} unexported sealed segment(s) are kept; export them to free space"
            )]
        } else {
            Vec::new()
        }
    }

    fn mark_exported(&mut self, names: &BTreeSet<String>) -> Result<(), String> {
        for s in &mut self.segments {
            if names.contains(&s.name) {
                s.exported = true;
            }
        }
        self.write_manifest()?;
        let controls = self.last_controls.clone();
        self.refresh_warnings(&controls);
        Ok(())
    }

    fn refresh_warnings(&mut self, controls: &Controls) {
        self.last_controls = controls.clone();
        let w = self.compute_warnings(controls);
        if let Ok(mut cache) = self.warnings.lock() {
            *cache = w;
        }
    }

    fn cached_warnings(&self) -> Vec<String> {
        self.warnings.lock().map(|w| w.clone()).unwrap_or_default()
    }

    fn enforce_budget(&mut self, controls: &Controls) -> Result<(), String> {
        let sealed_bytes = |segs: &[Segment]| -> u64 {
            segs.iter()
                .filter(|s| s.sealed && !s.archived)
                .map(|s| s.bytes)
                .sum()
        };
        let mut moved = false;
        while sealed_bytes(&self.segments) > controls.store_max_bytes {
            // Never evict an unexported segment.
            let Some(i) = self
                .segments
                .iter()
                .position(|s| s.sealed && s.exported && !s.archived)
            else {
                break;
            };
            if let Some(dir) = &self.dir {
                fs::create_dir_all(dir.join("archive")).map_err(|e| e.to_string())?;
                let name = &self.segments[i].name;
                fs::rename(dir.join(name), dir.join("archive").join(name))
                    .map_err(|e| e.to_string())?;
            }
            self.segments[i].archived = true;
            moved = true;
        }
        if moved {
            let gone: BTreeSet<String> = self
                .segments
                .iter()
                .filter(|s| s.archived)
                .map(|s| s.name.clone())
                .collect();
            self.metas.retain(|m| !gone.contains(&m.segment));
            self.by_id = self
                .metas
                .iter()
                .enumerate()
                .map(|(i, m)| (m.id.clone(), i))
                .collect();
            self.write_manifest()?;
            self.write_index()?;
        }
        self.refresh_warnings(controls);
        Ok(())
    }

    fn write_manifest(&self) -> Result<(), String> {
        let Some(dir) = &self.dir else { return Ok(()) };
        let segs: Vec<Value> = self
            .segments
            .iter()
            .map(|s| {
                json!({
                    "segment": s.name, "sealed": s.sealed, "exported": s.exported, "sha256": s.sha256,
                    "fingerprint": s.fingerprint, "archived": s.archived, "bytes": s.bytes,
                })
            })
            .collect();
        write_durable(
            &dir.join("manifest.json"),
            format!("{}\n", jcs(&json!({ "segments": segs }))).as_bytes(),
        )
    }

    fn write_index(&self) -> Result<(), String> {
        let Some(dir) = &self.dir else { return Ok(()) };
        let mut ids = Map::new();
        let mut counts: BTreeMap<String, BTreeMap<String, u64>> = BTreeMap::new();
        for m in &self.metas {
            ids.insert(
                m.id.clone(),
                json!({ "segment": m.segment, "offset": m.offset }),
            );
            *counts
                .entry(format!("{}\t{}", m.machine, m.sequence))
                .or_default()
                .entry(m.outcome.clone())
                .or_default() += 1;
        }
        write_durable(
            &dir.join("index.json"),
            format!("{}\n", jcs(&json!({ "ids": ids, "counts": counts }))).as_bytes(),
        )
    }
}

// ── The tracker ─────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
struct PathEntry {
    event: String,
    step: i64,
    milestone: bool,
    outputs: Value,
}

impl PathEntry {
    fn json(&self) -> Value {
        json!({ "event": self.event, "step": self.step, "milestone": self.milestone, "outputs": self.outputs })
    }
}

#[derive(Clone, Debug, Default)]
struct Open {
    id: String,
    machine: String,
    sequence: String,
    /// The event id that activated what it holds first.
    activator: String,
    open: Stamp,
    held: BTreeSet<String>,
    path: Vec<PathEntry>,
    refused_by: String,
    /// Projected onto the focal machine, step by step: what a record carries
    /// when the ring is gone (a shutdown).
    focal_pairs: Vec<Value>,
    focal_active_before: Value,
    input: Region,
    output: Region,
}

#[derive(Clone, Debug)]
struct Sustain {
    id: String,
    machine: String,
    sequence: String,
    event: String,
    through: i64,
}

#[derive(Clone, Debug)]
struct Closed {
    open_step: i64,
    close_step: i64,
}

#[derive(Clone, Debug)]
struct StepRecord {
    step: i64,
    isre: TrajectoryEntry,
    osre: TrajectoryEntry,
    ops: Vec<Op>,
    /// Active lists of the machines whose list changed at this step (deltas).
    changed: BTreeMap<String, Vec<ActiveEntry>>,
    /// (machine, sequence) -> the K-line that asserted for it at this step.
    asserted: BTreeMap<(String, String), String>,
    /// machine -> (input, output) regions, as they stood
    regions: BTreeMap<String, (Region, Option<Region>)>,
}

#[derive(Default)]
struct Touch {
    open_index: usize,
    is_new: bool,
    milestones: usize,
    close_outcome: String,
    difference: Value,
    subsumed_by: String,
    sustain_end_of: String,
    sustain_through: i64,
    machine: String,
    sequence: String,
    open_step: i64,
}

pub(crate) struct Tracker {
    instance: String,
    clock: Option<Arc<InstanceClock>>,
    /// The lamport when there is no clock (an engine built without one, in tests).
    fallback: u64,
    pub(crate) store: Store,
    pub(crate) controls: Controls,
    longest_path: i64,
    fingerprint: String,
    last_step: i64,
    last_commit: u64,
    open: Vec<Open>,
    sustain: Vec<Sustain>,
    closed: BTreeMap<String, Closed>,
    ring: Vec<StepRecord>,
    baseline: BTreeMap<String, Vec<ActiveEntry>>,
    current: BTreeMap<String, Vec<ActiveEntry>>,
}

impl Default for Tracker {
    fn default() -> Self {
        Self {
            instance: String::new(),
            clock: None,
            fallback: 0,
            store: Store::default(),
            controls: Controls::default(),
            longest_path: 7,
            fingerprint: String::new(),
            last_step: -1,
            last_commit: 0,
            open: Vec::new(),
            sustain: Vec::new(),
            closed: BTreeMap::new(),
            ring: Vec::new(),
            baseline: BTreeMap::new(),
            current: BTreeMap::new(),
        }
    }
}

impl Tracker {
    pub(crate) fn new(clock: Arc<InstanceClock>, store: Store) -> Self {
        Self {
            instance: clock.instance().to_owned(),
            clock: Some(clock),
            store,
            ..Self::default()
        }
    }

    fn tick(&mut self) -> Result<u64, String> {
        match &self.clock {
            Some(clock) => clock.send(),
            None => {
                self.fallback += 1;
                Ok(self.fallback)
            }
        }
    }

    fn stamp(&self, lamport: u64, step: i64, commit: u64) -> Stamp {
        Stamp {
            instance: self.instance.clone(),
            lamport,
            commit,
            step,
        }
    }

    pub(crate) fn set_longest_path(&mut self, l: i64) {
        self.longest_path = l;
    }

    fn ring_depth(&self) -> i64 {
        self.longest_path * (self.controls.feedback_hops as i64 + 1)
    }

    pub(crate) fn health_warnings(&self) -> Vec<String> {
        self.store.cached_warnings()
    }

    pub(crate) fn refresh_budget(&mut self) {
        let controls = self.controls.clone();
        self.store.refresh_warnings(&controls);
    }

    fn record_for(&self, step: i64) -> Option<&StepRecord> {
        self.ring.iter().find(|r| r.step == step)
    }

    fn active_at(&self, machine: &str, step: i64) -> Vec<ActiveEntry> {
        // The newest change at or before `step`, else the state before the ring.
        for r in self.ring.iter().rev() {
            if r.step > step {
                continue;
            }
            if let Some(list) = r.changed.get(machine) {
                return list.clone();
            }
        }
        self.baseline.get(machine).cloned().unwrap_or_default()
    }

    /// A committed step (its commit tick already taken).
    pub(crate) fn on_step(&mut self, v: &StepView) -> Result<(), String> {
        if !self.controls.capture {
            return Ok(());
        }
        let k = v.step;
        self.fingerprint = v.fingerprint.clone();

        // Machines by corpus name, with their step results.
        let mut by_name: BTreeMap<&str, (&Machine, Option<&Value>)> = BTreeMap::new();
        for (id, m) in v.machines {
            by_name.insert(m.name.as_str(), (m, v.results.get(id)));
        }
        if self.ring.is_empty() && self.current.is_empty() {
            for (name, (m, _)) in &by_name {
                self.baseline.insert((*name).to_owned(), initial_actives(m));
            }
        }

        // ── The step record (pushed now: closes this step read it) ──
        let mut rec = StepRecord {
            step: k,
            isre: v.isre.clone(),
            osre: v.osre.clone(),
            ops: v.ops.clone(),
            changed: BTreeMap::new(),
            asserted: BTreeMap::new(),
            regions: BTreeMap::new(),
        };
        for (name, (m, _)) in &by_name {
            if let Some((input, output)) = regions(m) {
                rec.regions
                    .insert((*name).to_owned(), (input, Some(output)));
            }
            let now = actives_of(m);
            let before = match self.current.get(*name) {
                Some(list) => list.clone(),
                None => self
                    .baseline
                    .get(*name)
                    .cloned()
                    .unwrap_or_else(|| initial_actives(m)),
            };
            if now != before {
                rec.changed.insert((*name).to_owned(), now.clone());
            }
            self.current.insert((*name).to_owned(), now);
        }

        // ── Boundaries, per sequence ──
        let mut touches: Vec<Touch> = Vec::new();
        let mut opened: Vec<Open> = Vec::new();

        // Who held each event before this step.
        let mut holder: BTreeMap<(String, String, String), String> = BTreeMap::new();
        for kl in &self.open {
            for e in &kl.held {
                holder.insert(
                    (kl.machine.clone(), kl.sequence.clone(), e.clone()),
                    kl.id.clone(),
                );
            }
        }
        for s in &self.sustain {
            holder.insert(
                (s.machine.clone(), s.sequence.clone(), s.event.clone()),
                s.id.clone(),
            );
        }
        let mut still_open = vec![true; self.open.len()];

        for (name, (m, result)) in &by_name {
            let Some(result) = result else { continue };
            let transition = &result["transitionResult"];
            let input: Vec<f64> = result["inputEvent"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_f64)
                .collect();
            let in_region = regions(m).map_or((0, 0), |r| r.0);
            for (sid, seq) in &m.sequences {
                let Some(sr) = transition["sequenceResults"].get(sid) else {
                    continue;
                };
                let ids = |key: &str| -> BTreeSet<String> {
                    sr[key]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter_map(|e| e.as_str().map(str::to_owned))
                        .collect()
                };
                let matched = ids("matchedEvents");
                let activated = ids("activatedEvents");
                let events = &seq.events;
                let is_active = |e: &str| events.get(e).is_some_and(|ev| ev.active);
                let mut claimed: BTreeMap<String, String> = BTreeMap::new();
                let path_entry = |ev: &Event| PathEntry {
                    event: ev.id.clone(),
                    step: k,
                    milestone: !ev.outputs.is_empty() && !ev.next_ids.is_empty(),
                    outputs: Value::Array(ev.outputs.iter().map(|o| json!(o.vector)).collect()),
                };

                // Existing K-lines of this sequence, older first.
                let mut mine: Vec<usize> = (0..self.open.len())
                    .filter(|i| self.open[*i].machine == *name && self.open[*i].sequence == *sid)
                    .collect();
                mine.sort_by(|a, b| {
                    let (a, b) = (&self.open[*a], &self.open[*b]);
                    (a.open.step, &a.activator).cmp(&(b.open.step, &b.activator))
                });
                for i in mine {
                    let kl = &mut self.open[i];
                    let mut t = Touch {
                        open_index: i,
                        machine: (*name).to_owned(),
                        sequence: sid.clone(),
                        open_step: kl.open.step,
                        ..Touch::default()
                    };
                    let mut next: BTreeSet<String> = BTreeSet::new();
                    let mut completed_by = String::new();
                    let mut first_failed = String::new();
                    let held: Vec<String> = kl.held.iter().cloned().collect();
                    for e in &held {
                        let Some(ev) = events.get(e) else { continue };
                        if matched.contains(e) {
                            kl.path.push(path_entry(ev));
                            if kl.path.last().is_some_and(|p| p.milestone) {
                                t.milestones += 1;
                            }
                            if ev.next_ids.is_empty() {
                                if completed_by.is_empty() {
                                    completed_by = e.clone();
                                }
                                continue;
                            }
                            for n in &ev.next_ids {
                                if n == e {
                                    // a dwell: re-activated, still this K-line's
                                    if is_active(n) {
                                        next.insert(n.clone());
                                    }
                                    continue;
                                }
                                if activated.contains(n) {
                                    match claimed.get(n) {
                                        None => {
                                            claimed.insert(n.clone(), kl.id.clone());
                                            next.insert(n.clone());
                                        }
                                        Some(c) => {
                                            if kl.refused_by.is_empty() {
                                                kl.refused_by = c.clone();
                                            }
                                        }
                                    }
                                } else {
                                    match holder.get(&((*name).to_owned(), sid.clone(), n.clone()))
                                    {
                                        Some(h) if *h != kl.id => {
                                            if kl.refused_by.is_empty() {
                                                kl.refused_by = h.clone();
                                            }
                                        }
                                        Some(_) => {
                                            next.insert(n.clone()); // already this K-line's
                                        }
                                        None => {}
                                    }
                                }
                            }
                            // An event with outputs stays active after it matches (a milestone).
                            if !ev.outputs.is_empty() && is_active(e) {
                                next.insert(e.clone());
                            }
                        } else if first_failed.is_empty() {
                            first_failed = e.clone();
                        }
                    }
                    if !completed_by.is_empty() {
                        t.close_outcome = "completed".to_owned();
                        self.sustain.push(Sustain {
                            id: String::new(),
                            machine: (*name).to_owned(),
                            sequence: sid.clone(),
                            event: completed_by,
                            through: k,
                        });
                        t.sustain_through = k;
                    } else if next.is_empty() {
                        if !kl.refused_by.is_empty() {
                            t.close_outcome = "subsumed".to_owned();
                            t.subsumed_by = kl.refused_by.clone();
                        } else {
                            t.close_outcome = "abandoned".to_owned();
                            let e = if first_failed.is_empty() {
                                kl.held.iter().next().cloned().unwrap_or_default()
                            } else {
                                first_failed
                            };
                            if let Some(ev) = events.get(&e) {
                                t.difference =
                                    difference_of(ev, &m.match_algorithm, &input, in_region);
                            }
                        }
                    }
                    let kl = &mut self.open[i];
                    kl.held = next;
                    if !t.close_outcome.is_empty() {
                        still_open[i] = false;
                    }
                    touches.push(t);
                }

                // Sustains of this sequence.
                let closed = &self.closed;
                for s in &mut self.sustain {
                    if s.machine != *name || s.sequence != *sid || s.id.is_empty() {
                        continue;
                    }
                    if matched.contains(&s.event) {
                        s.through = k;
                    } else {
                        touches.push(Touch {
                            machine: (*name).to_owned(),
                            sequence: sid.clone(),
                            sustain_end_of: s.id.clone(),
                            sustain_through: s.through,
                            open_step: closed.get(&s.id).map_or(s.through, |c| c.open_step),
                            ..Touch::default()
                        });
                        s.through = -1; // removed below
                    }
                }

                // A departure: an initial event matched.
                for (eid, ev) in events {
                    if !ev.initial || !matched.contains(eid) {
                        continue;
                    }
                    let sustaining = self.sustain.iter().any(|s| {
                        s.machine == *name
                            && s.sequence == *sid
                            && s.event == *eid
                            && s.through == k
                    });
                    if sustaining {
                        continue;
                    }
                    let mut kl = Open {
                        machine: (*name).to_owned(),
                        sequence: sid.clone(),
                        activator: eid.clone(),
                        open: self.stamp(0, k, v.commit),
                        input: in_region,
                        output: regions(m).map_or((0, 0), |r| r.1),
                        ..Open::default()
                    };
                    kl.path.push(path_entry(ev));
                    let mut t = Touch {
                        is_new: true,
                        machine: (*name).to_owned(),
                        sequence: sid.clone(),
                        open_step: k,
                        ..Touch::default()
                    };
                    if kl.path.last().is_some_and(|p| p.milestone) {
                        t.milestones += 1;
                    }
                    if ev.next_ids.is_empty() {
                        // a one-event sequence opens and completes
                        t.close_outcome = "completed".to_owned();
                        self.sustain.push(Sustain {
                            id: String::new(),
                            machine: (*name).to_owned(),
                            sequence: sid.clone(),
                            event: eid.clone(),
                            through: k,
                        });
                        t.sustain_through = k;
                    } else {
                        for n in &ev.next_ids {
                            if n == eid {
                                continue; // an initial event that lists itself
                            }
                            if activated.contains(n) && !claimed.contains_key(n) {
                                claimed.insert(n.clone(), "#new".to_owned());
                                kl.held.insert(n.clone());
                            }
                        }
                        if kl.held.is_empty() {
                            continue; // no successor activated: no departure
                        }
                    }
                    // Its focal start state is the list after k-1.
                    kl.focal_active_before = actives_json(self.active_at(name, k - 1));
                    t.open_index = opened.len();
                    opened.push(kl);
                    touches.push(t);
                }
            }
        }
        self.sustain.retain(|s| s.through >= 0);

        self.ring.push(rec);
        let now_index = self.ring.len() - 1;

        // ── Ticks, in record order ──
        touches.sort_by(|a, b| {
            (&a.machine, &a.sequence, a.open_step, a.is_new).cmp(&(
                &b.machine,
                &b.sequence,
                b.open_step,
                b.is_new,
            ))
        });
        let mut lines: Vec<Value> = Vec::new();
        for t in &touches {
            if !t.sustain_end_of.is_empty() {
                let l = self.tick()?;
                lines.push(json!({
                    "format": "re-kline-event/1", "kline": t.sustain_end_of, "event": "sustain-end",
                    "through": t.sustain_through, "at": self.stamp(l, k, v.commit).json(),
                }));
                continue;
            }
            if t.is_new {
                let lamport = self.tick()?;
                let kl = &mut opened[t.open_index];
                kl.open.lamport = lamport;
                kl.id = format!("{}:{}", self.instance, lamport);
            }
            for _ in 0..t.milestones {
                self.tick()?;
            }
            // The pair of this step, projected onto the focal machine (shutdown record).
            let (machine, input, output) = {
                let kl = if t.is_new {
                    &opened[t.open_index]
                } else {
                    &self.open[t.open_index]
                };
                (kl.machine.clone(), kl.input, kl.output)
            };
            let now = &self.ring[now_index];
            let contribs: Vec<Value> = now
                .ops
                .iter()
                .filter(|op| op.machine == machine)
                .map(|op| {
                    json!({ "machine": op.machine, "region": { "offset": op.region.0, "length": op.region.1 }, "values": op.values })
                })
                .collect();
            let pair = json!({
                "stepNumber": k,
                "isre": entry_json(&now.isre, &[input]),
                "osre": entry_json(&now.osre, &[output]),
                "contributions": contribs,
            });
            {
                let kl = if t.is_new {
                    &mut opened[t.open_index]
                } else {
                    &mut self.open[t.open_index]
                };
                kl.focal_pairs.push(pair);
            }
            if !t.close_outcome.is_empty() {
                let lamport = self.tick()?;
                let close = self.stamp(lamport, k, v.commit);
                let kl = if t.is_new {
                    opened[t.open_index].clone()
                } else {
                    self.open[t.open_index].clone()
                };
                if t.close_outcome == "completed"
                    && let Some(s) = self.sustain.iter_mut().find(|s| {
                        s.id.is_empty() && s.machine == kl.machine && s.sequence == kl.sequence
                    })
                {
                    s.id = kl.id.clone();
                }
                self.closed.insert(
                    kl.id.clone(),
                    Closed {
                        open_step: kl.open.step,
                        close_step: k,
                    },
                );
                lines.push(self.build_record(
                    &kl,
                    &t.close_outcome,
                    Value::Null,
                    &close,
                    t.difference.clone(),
                    &t.subsumed_by,
                    t.sustain_through,
                    true,
                ));
            }
        }
        // What asserted this step: contributor searches at later steps read it.
        let mut asserted = BTreeMap::new();
        for t in &touches {
            if !t.sustain_end_of.is_empty() {
                continue;
            }
            let kl = if t.is_new {
                &opened[t.open_index]
            } else {
                &self.open[t.open_index]
            };
            if t.milestones > 0 || t.close_outcome == "completed" {
                asserted.insert((kl.machine.clone(), kl.sequence.clone()), kl.id.clone());
            }
        }
        for s in &self.sustain {
            if s.through == k && !s.id.is_empty() {
                asserted.insert((s.machine.clone(), s.sequence.clone()), s.id.clone());
            }
        }
        self.ring[now_index].asserted.extend(asserted);

        // Keep what is still open.
        let mut keep: Vec<Open> = std::mem::take(&mut self.open)
            .into_iter()
            .zip(still_open)
            .filter_map(|(kl, open)| open.then_some(kl))
            .collect();
        for t in &touches {
            if t.is_new && t.close_outcome.is_empty() {
                keep.push(std::mem::take(&mut opened[t.open_index]));
            }
        }
        self.open = keep;

        self.last_step = k;
        self.last_commit = v.commit;
        self.persist_step(&lines)?;
        self.trim_ring();
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn build_record(
        &self,
        k: &Open,
        outcome: &str,
        reason: Value,
        close: &Stamp,
        difference: Value,
        subsumed_by: &str,
        sustain_through: i64,
        ring_available: bool,
    ) -> Value {
        let mut scope: Vec<String> = vec![k.machine.clone()];
        let mut contributors: Vec<Value> = Vec::new();
        let mut truncated = false;
        let mut pairs = Value::Array(k.focal_pairs.clone());
        let mut active_before = k.focal_active_before.clone();
        if active_before.is_null() {
            active_before = json!([]);
        }
        let mut active_after = json!([]);
        let mut shared: Vec<Value> = Vec::new();

        if ring_available {
            // Contributors through the recorded OSRE(m), hop by hop.
            let mut frontier: Vec<(String, i64, i64)> =
                vec![(k.machine.clone(), k.open.step, close.step)];
            let mut seen: BTreeSet<String> = BTreeSet::from([k.id.clone()]);
            let mut found: Vec<(u64, String, i64, String)> = Vec::new();
            let mut hop = 1u64;
            while !frontier.is_empty() {
                let mut next = Vec::new();
                for (machine, from, to) in &frontier {
                    let any = self.record_for(*to);
                    let mut region = if *machine == k.machine {
                        k.input
                    } else {
                        (0, 0)
                    };
                    if *machine != k.machine
                        && let Some(r) = any.and_then(|a| a.regions.get(machine))
                    {
                        region = r.0;
                    }
                    for s in *from..=*to {
                        let Some(prev) = self.record_for(s - 1) else {
                            if s > 0 {
                                truncated = true; // the ring no longer holds it
                            }
                            continue;
                        };
                        for op in &prev.ops {
                            if op.machine == *machine || !overlaps(op.region, region) {
                                continue;
                            }
                            for sq in &op.sequence_ids {
                                let Some(a) = prev.asserted.get(&(op.machine.clone(), sq.clone()))
                                else {
                                    continue;
                                };
                                if seen.contains(a) {
                                    continue;
                                }
                                if hop > self.controls.feedback_hops {
                                    truncated = true;
                                    continue;
                                }
                                seen.insert(a.clone());
                                let (from, to) = self
                                    .closed
                                    .get(a)
                                    .map_or((s - 1, s - 1), |c| (c.open_step, c.close_step));
                                found.push((hop, op.machine.clone(), from, a.clone()));
                                next.push((op.machine.clone(), from, to));
                            }
                        }
                    }
                }
                frontier = next;
                hop += 1;
            }
            found.sort();
            for (hops, machine, _, id) in found {
                contributors.push(json!({ "kline": id, "machine": machine, "hops": hops }));
                scope.push(machine);
            }
            scope.sort();
            scope.dedup();

            // Regions of the scope, as they stood at close.
            let mut inputs: Vec<Region> = Vec::new();
            let mut outputs: Vec<Region> = Vec::new();
            let in_scope: BTreeSet<&String> = scope.iter().collect();
            if let Some(last) = self.record_for(close.step) {
                for name in &scope {
                    let Some(r) = last.regions.get(name) else {
                        continue;
                    };
                    inputs.push(r.0);
                    if let Some(o) = r.1 {
                        outputs.push(o);
                    }
                }
                // Output cells shared with a machine outside the scope.
                let mut cells: BTreeSet<i64> = BTreeSet::new();
                for o in &outputs {
                    for (name, (_, out)) in &last.regions {
                        let Some(x) = out else { continue };
                        if in_scope.contains(name) {
                            continue;
                        }
                        for c in o.0.max(x.0)..(o.0 + o.1).min(x.0 + x.1) {
                            cells.insert(c);
                        }
                    }
                }
                shared = cells.into_iter().map(|c| json!(c)).collect();
            }
            let mut ps: Vec<Value> = Vec::new();
            for s in k.open.step..=close.step {
                let Some(r) = self.record_for(s) else {
                    continue;
                };
                let mut ops: Vec<&Op> = r
                    .ops
                    .iter()
                    .filter(|op| in_scope.contains(&op.machine))
                    .collect();
                ops.sort_by(|a, b| (a.region.0, &a.machine).cmp(&(b.region.0, &b.machine)));
                let contribs: Vec<Value> = ops
                    .into_iter()
                    .map(|op| json!({ "machine": op.machine, "region": { "offset": op.region.0, "length": op.region.1 }, "values": op.values }))
                    .collect();
                ps.push(json!({
                    "stepNumber": s,
                    "isre": entry_json(&r.isre, &inputs),
                    "osre": entry_json(&r.osre, &outputs),
                    "contributions": contribs,
                }));
            }
            if !ps.is_empty() {
                pairs = Value::Array(ps);
            }
            let mut before = Vec::new();
            let mut after = Vec::new();
            for name in &scope {
                before.extend(self.active_at(name, k.open.step - 1));
                after.extend(self.active_at(name, close.step));
            }
            active_before = actives_json(before);
            active_after = actives_json(after);
        } else {
            truncated = true; // the ring did not survive: scope is the focal machine alone
        }

        let completed = outcome == "completed";
        json!({
            "format": "re-kline/1",
            "id": k.id,
            "corpus": { "algorithm": "sha256/resident-declared-v1", "fingerprint": self.fingerprint },
            "focal": { "machine": k.machine, "sequence": k.sequence },
            "outcome": outcome,
            "reason": reason,
            "open": k.open.json(),
            "close": close.json(),
            "path": k.path.iter().map(PathEntry::json).collect::<Vec<_>>(),
            "difference": difference,
            "subsumedBy": if subsumed_by.is_empty() { Value::Null } else { json!(subsumed_by) },
            "sustain": { "through": if completed { sustain_through } else { close.step }, "ended": !completed },
            "scope": scope,
            "contributors": contributors,
            "feedbackTruncated": truncated,
            "sharedCells": shared,
            "activeBefore": active_before,
            "activeAfter": active_after,
            "pairs": pairs,
            "outputs": { "outputVector": null, "mergedOutputVector": null },
        })
    }

    fn sustain_end(&mut self, s: &Sustain) -> Result<Value, String> {
        let l = self.tick()?;
        Ok(json!({
            "format": "re-kline-event/1", "kline": s.id, "event": "sustain-end", "through": s.through,
            "at": self.stamp(l, self.last_step.max(0), self.last_commit).json(),
        }))
    }

    /// A reset: every open K-line closes `interrupted` ("reset") at the last
    /// committed step, and every sustain ends there.
    pub(crate) fn on_reset(&mut self) -> Result<(), String> {
        if !self.controls.capture {
            return Ok(());
        }
        let mut lines = Vec::new();
        let mut order: Vec<usize> = (0..self.open.len()).collect();
        order.sort_by(|a, b| {
            let (a, b) = (&self.open[*a], &self.open[*b]);
            (&a.machine, &a.sequence, a.open.step).cmp(&(&b.machine, &b.sequence, b.open.step))
        });
        for i in order {
            let lamport = self.tick()?;
            let close = self.stamp(lamport, self.last_step.max(0), self.last_commit);
            let kl = self.open[i].clone();
            self.closed.insert(
                kl.id.clone(),
                Closed {
                    open_step: kl.open.step,
                    close_step: close.step,
                },
            );
            lines.push(self.build_record(
                &kl,
                "interrupted",
                json!("reset"),
                &close,
                Value::Null,
                "",
                close.step,
                true,
            ));
        }
        let mut sustains = std::mem::take(&mut self.sustain);
        sustains.sort_by(|a, b| {
            (&a.machine, &a.sequence, &a.id).cmp(&(&b.machine, &b.sequence, &b.id))
        });
        for s in &sustains {
            lines.push(self.sustain_end(s)?);
        }
        self.open.clear();
        self.persist_step(&lines)?;
        self.ring.clear();
        self.baseline.clear();
        self.current.clear();
        self.last_step = -1;
        Ok(())
    }

    /// A machine removed at runtime: its open K-lines close `interrupted`
    /// ("machine-removed") and its sustains end.
    pub(crate) fn on_machine_removed(&mut self, machine: &str) -> Result<(), String> {
        if !self.controls.capture {
            return Ok(());
        }
        let mut lines = Vec::new();
        let (mut gone, keep): (Vec<Open>, Vec<Open>) = std::mem::take(&mut self.open)
            .into_iter()
            .partition(|kl| kl.machine == machine);
        self.open = keep;
        gone.sort_by(|a, b| (&a.sequence, a.open.step).cmp(&(&b.sequence, b.open.step)));
        for kl in &gone {
            let lamport = self.tick()?;
            let close = self.stamp(lamport, self.last_step.max(0), self.last_commit);
            self.closed.insert(
                kl.id.clone(),
                Closed {
                    open_step: kl.open.step,
                    close_step: close.step,
                },
            );
            lines.push(self.build_record(
                kl,
                "interrupted",
                json!("machine-removed"),
                &close,
                Value::Null,
                "",
                close.step,
                true,
            ));
        }
        let (ended, kept): (Vec<Sustain>, Vec<Sustain>) = std::mem::take(&mut self.sustain)
            .into_iter()
            .partition(|s| s.machine == machine);
        self.sustain = kept;
        for s in &ended {
            lines.push(self.sustain_end(s)?);
        }
        self.current.remove(machine);
        self.persist_step(&lines)
    }

    fn checkpoint_json(&self) -> Value {
        let open: Vec<Value> = self
            .open
            .iter()
            .map(|kl| {
                json!({
                    "id": kl.id, "machine": kl.machine, "sequence": kl.sequence, "activator": kl.activator,
                    "open": kl.open.json(), "held": kl.held, "path": kl.path.iter().map(PathEntry::json).collect::<Vec<_>>(),
                    "refusedBy": kl.refused_by, "focalPairs": kl.focal_pairs,
                    "focalActiveBefore": if kl.focal_active_before.is_null() { json!([]) } else { kl.focal_active_before.clone() },
                    "focalActiveAfter": actives_json(self.current.get(&kl.machine).cloned().unwrap_or_default()),
                    "input": { "offset": kl.input.0, "length": kl.input.1 },
                    "output": { "offset": kl.output.0, "length": kl.output.1 },
                })
            })
            .collect();
        let sustains: Vec<Value> = self
            .sustain
            .iter()
            .map(|s| json!({ "id": s.id, "machine": s.machine, "sequence": s.sequence, "event": s.event, "through": s.through }))
            .collect();
        json!({
            "format": "re-kline-open/1", "step": self.last_step, "commit": self.last_commit,
            "fingerprint": self.fingerprint, "open": open, "sustain": sustains,
        })
    }

    fn persist_step(&mut self, lines: &[Value]) -> Result<(), String> {
        let fingerprint = self.fingerprint.clone();
        self.store.append(lines, &fingerprint, self.last_commit)?;
        self.store.checkpoint(&self.checkpoint_json())?;
        let controls = self.controls.clone();
        self.store.enforce_budget(&controls)
    }

    /// At boot, and when capture turns on: closes every K-line in the
    /// checkpoint `interrupted` ("shutdown") at its step, and ends every
    /// sustain there.
    fn recover(&mut self) -> Result<(), String> {
        let Some(cp) = self.store.read_checkpoint().filter(Value::is_object) else {
            return Ok(());
        };
        let step = number(&cp["step"]) as i64;
        let commit = number(&cp["commit"]) as u64;
        self.fingerprint = cp["fingerprint"].as_str().unwrap_or_default().to_owned();
        self.last_step = step;
        self.last_commit = commit;
        let mut lines = Vec::new();
        for o in cp["open"].as_array().into_iter().flatten() {
            let kl = Open {
                id: o["id"].as_str().unwrap_or_default().to_owned(),
                machine: o["machine"].as_str().unwrap_or_default().to_owned(),
                sequence: o["sequence"].as_str().unwrap_or_default().to_owned(),
                activator: o["activator"].as_str().unwrap_or_default().to_owned(),
                open: Stamp::from(&o["open"]),
                path: o["path"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|p| PathEntry {
                        event: p["event"].as_str().unwrap_or_default().to_owned(),
                        step: number(&p["step"]) as i64,
                        milestone: p["milestone"].as_bool().unwrap_or(false),
                        outputs: p["outputs"].clone(),
                    })
                    .collect(),
                focal_pairs: o["focalPairs"].as_array().cloned().unwrap_or_default(),
                focal_active_before: o["focalActiveBefore"].clone(),
                ..Open::default()
            };
            let lamport = self.tick()?;
            let close = self.stamp(lamport, step, commit);
            let mut rec = self.build_record(
                &kl,
                "interrupted",
                json!("shutdown"),
                &close,
                Value::Null,
                "",
                step,
                false,
            );
            rec["activeAfter"] = o["focalActiveAfter"].clone();
            lines.push(rec);
        }
        for s in cp["sustain"].as_array().into_iter().flatten() {
            let id = s["id"].as_str().unwrap_or_default();
            if id.is_empty() {
                continue;
            }
            let lamport = self.tick()?;
            lines.push(json!({
                "format": "re-kline-event/1", "kline": id, "event": "sustain-end", "through": s["through"],
                "at": self.stamp(lamport, step, commit).json(),
            }));
        }
        self.open.clear();
        self.sustain.clear();
        self.persist_step(&lines)?;
        self.last_step = -1; // the engine's step count starts over
        Ok(())
    }

    /// klineCapture turned on (at boot or at runtime): capture starts afresh,
    /// first closing what the checkpoint holds, as at boot.
    pub(crate) fn start_capture(&mut self) -> Result<(), String> {
        self.open.clear();
        self.sustain.clear();
        self.closed.clear();
        self.ring.clear();
        self.baseline.clear();
        self.current.clear();
        self.last_step = -1;
        self.controls.capture = true;
        self.recover()
    }

    fn trim_ring(&mut self) {
        let mut keep_from = self.last_step - self.ring_depth().max(1);
        for kl in &self.open {
            keep_from = keep_from.min(kl.open.step - 1);
        }
        while self.ring.first().is_some_and(|r| r.step < keep_from) {
            let front = self.ring.remove(0);
            for (name, list) in front.changed {
                self.baseline.insert(name, list);
            }
        }
        // Closed K-lines are needed while their span is in the ring.
        self.closed.retain(|_, c| c.close_step >= keep_from - 1);
    }

    // ── Routes' bodies ──────────────────────────────────────────────────────

    pub(crate) fn open_json(&self) -> Value {
        let mut list: Vec<&Open> = self.open.iter().collect();
        list.sort_by(|a, b| {
            (&a.machine, &a.sequence, a.open.step).cmp(&(&b.machine, &b.sequence, b.open.step))
        });
        let open: Vec<Value> = list
            .into_iter()
            .map(|kl| {
                json!({
                    "id": kl.id, "focal": { "machine": kl.machine, "sequence": kl.sequence },
                    "open": kl.open.json(), "path": kl.path.iter().map(PathEntry::json).collect::<Vec<_>>(),
                    "held": kl.held,
                })
            })
            .collect();
        json!({ "commit": self.last_commit, "open": open })
    }

    pub(crate) fn list(
        &self,
        outcome: Option<&str>,
        machine: Option<&str>,
        sequence: Option<&str>,
        since: u64,
        limit: usize,
    ) -> Value {
        let limit = if limit == 0 { 100 } else { limit }.clamp(1, 1000);
        let sel: Vec<&Meta> = self
            .store
            .metas
            .iter()
            .filter(|m| m.close_commit >= since)
            .filter(|m| outcome.is_none_or(|o| m.outcome == o))
            .filter(|m| machine.is_none_or(|x| m.machine == x))
            .filter(|m| sequence.is_none_or(|x| m.sequence == x))
            .collect();
        // A page holds whole commits: it stops before the commit that would
        // cross `limit`, unless that commit is the page's first.
        let mut out = Vec::new();
        let mut next = Value::Null;
        let mut i = 0;
        while i < sel.len() {
            let mut j = i;
            while j < sel.len() && sel[j].close_commit == sel[i].close_commit {
                j += 1;
            }
            if !out.is_empty() && out.len() + (j - i) > limit {
                next = json!(sel[i].close_commit);
                break;
            }
            for m in &sel[i..j] {
                out.push(self.store.record_at(m));
            }
            i = j;
            if out.len() >= limit && i < sel.len() {
                next = json!(sel[i].close_commit);
                break;
            }
        }
        json!({ "klines": out, "next": next })
    }

    pub(crate) fn get(&self, id: &str) -> Option<Value> {
        self.store.record(id)
    }

    pub(crate) fn stats(&self) -> Value {
        self.store.stats(&self.controls)
    }

    /// A `re-kline-bundle/1`: JSON Lines, header first. Marks the segments it
    /// covered as exported.
    pub(crate) fn export_bundle(
        &mut self,
        outcomes: &BTreeSet<String>,
        since: u64,
        machines: &BTreeSet<String>,
    ) -> Result<String, String> {
        let mut body = String::new();
        let mut count = 0usize;
        let mut segments = BTreeSet::new();
        for m in &self.store.metas {
            if m.close_commit < since
                || (!outcomes.is_empty() && !outcomes.contains(&m.outcome))
                || (!machines.is_empty() && !machines.contains(&m.machine))
            {
                continue;
            }
            body.push_str(&self.store.raw_at(m));
            body.push('\n');
            count += 1;
            for e in self.store.raw_events(&m.id) {
                body.push_str(&e);
                body.push('\n');
                count += 1;
            }
            segments.insert(m.segment.clone());
        }
        let header = json!({
            "format": "re-kline-bundle/1",
            "source": { "instance": self.instance },
            "corpus": { "algorithm": "sha256/resident-declared-v1", "fingerprint": self.fingerprint },
            "controls": {
                "klineCapture": self.controls.capture,
                "klineFeedbackHops": self.controls.feedback_hops,
                "klineStoreMaxBytes": self.controls.store_max_bytes,
            },
            "count": count,
            "sha256": sha256_hex(body.as_bytes()),
        });
        self.store.mark_exported(&segments)?;
        Ok(format!("{}\n{body}", jcs(&header)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jcs_sorts_keys_and_writes_ecmascript_numbers() {
        let v = json!({ "b": 1.0, "a": [0.5, 2, "x\n"], "c": null });
        assert_eq!(jcs(&v), r#"{"a":[0.5,2,"x\n"],"b":1,"c":null}"#);
    }

    #[test]
    fn sha256_is_lower_hex() {
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn a_page_holds_whole_commits() {
        let mut store = Store::default();
        let rec = |id: &str, commit: u64| {
            json!({ "format": "re-kline/1", "id": id, "focal": { "machine": "m", "sequence": "s" },
                    "outcome": "completed", "open": { "step": 0 }, "close": { "commit": commit, "step": 0 } })
        };
        store
            .append(&[rec("a", 1), rec("b", 1), rec("c", 2)], "f", 1)
            .unwrap();
        let tracker = Tracker {
            store,
            ..Tracker::default()
        };
        let page = tracker.list(None, None, None, 0, 1);
        assert_eq!(page["klines"].as_array().unwrap().len(), 2);
        assert_eq!(page["next"], json!(2));
    }
}
