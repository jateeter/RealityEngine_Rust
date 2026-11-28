//! Runtime snapshots (RealityEngine_CI#287, phase 2; this repo's #81).
//! SURFACE_SPEC.md, "The snapshot body" and "Routes": a snapshot is a K-line
//! over retained steps plus what it takes to re-establish it on an engine
//! holding the same corpus, `re-snapshot/1`, runtime-independent, its digest
//! over the JCS form. A port of C++ `snapshot.cpp`.
//!
//! Retention keeps, for every retained step (`historyLimit`), the active list
//! after the step, its contributions and its clock, whether or not K-line
//! capture is on.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fs,
    path::PathBuf,
};

use axum::{
    Json,
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::{Map, Value, json};

use crate::{
    Engine, Machine, SharedEngine,
    kline::{self, ActiveEntry, Op, jcs, sha256_hex},
    step::TrajectoryEntry,
};

#[derive(Clone, Debug)]
pub(crate) struct Retained {
    step: u64,
    lamport: u64,
    isre: TrajectoryEntry,
    osre: TrajectoryEntry,
    ops: Vec<Op>,
    after: Vec<ActiveEntry>,
}

/// Every retained step's active list, contributions and clock.
#[derive(Default)]
pub(crate) struct Retention {
    base: Vec<ActiveEntry>,
    /// A restore sets it; otherwise the base is taken at the first step.
    base_set: bool,
    steps: VecDeque<Retained>,
}

fn all_actives(machines: &BTreeMap<String, Machine>) -> Vec<ActiveEntry> {
    machines.values().flat_map(kline::actives_of).collect()
}

impl Retention {
    /// At boot and after a reset: the base is taken at the first step after
    /// it, every machine at its initial events.
    pub(crate) fn on_reset(&mut self) {
        self.base.clear();
        self.base_set = false;
        self.steps.clear();
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_step(
        &mut self,
        step: u64,
        lamport: u64,
        isre: TrajectoryEntry,
        osre: TrajectoryEntry,
        ops: Vec<Op>,
        machines: &BTreeMap<String, Machine>,
        limit: usize,
    ) {
        if self.steps.is_empty() && !self.base_set {
            self.base = machines.values().flat_map(kline::initial_actives).collect();
            self.base_set = true;
        }
        self.steps.push_back(Retained {
            step,
            lamport,
            isre,
            osre,
            ops,
            after: all_actives(machines),
        });
        // Keep historyLimit steps; the newest one leaving becomes the base for the next.
        while self.steps.len() > limit.max(1) {
            if let Some(front) = self.steps.pop_front() {
                self.base = front.after;
            }
        }
    }

    /// After a corpus restore: nothing retained, and the restored list is the base.
    fn restore(&mut self, base: Vec<ActiveEntry>) {
        self.base = base;
        self.base_set = true;
        self.steps.clear();
    }

    fn at(&self, step: u64) -> Option<&Retained> {
        self.steps.iter().find(|r| r.step == step)
    }

    /// The list before `step`: after step-1, or the base when it is the first.
    fn before(&self, step: u64) -> Option<Vec<ActiveEntry>> {
        if step > 0
            && let Some(prev) = self.at(step - 1)
        {
            return Some(prev.after.clone());
        }
        match self.steps.front() {
            Some(front) if front.step == step => Some(self.base.clone()),
            None => Some(self.base.clone()),
            _ => None,
        }
    }
}

fn entry_json(e: &TrajectoryEntry, keep: Option<&[kline::Region]>) -> Value {
    let nz: Vec<Value> = e
        .non_zero
        .iter()
        .filter(|(index, _)| {
            let index = *index as i64;
            keep.is_none_or(|keep| keep.iter().any(|r| index >= r.0 && index < r.0 + r.1))
        })
        .map(|(index, value)| json!({ "index": index, "value": value }))
        .collect();
    json!({ "stepNumber": e.step_number, "length": e.length, "nonZero": nz })
}

fn entry_from(v: &Value) -> TrajectoryEntry {
    TrajectoryEntry {
        step_number: v["stepNumber"].as_f64().unwrap_or(0.0) as u64,
        length: v["length"].as_f64().unwrap_or(0.0) as usize,
        non_zero: v["nonZero"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|c| {
                (
                    c["index"].as_f64().unwrap_or(0.0) as usize,
                    c["value"].as_f64().unwrap_or(0.0),
                )
            })
            .collect(),
    }
}

fn only(list: &[ActiveEntry], scope: Option<&BTreeSet<String>>) -> Vec<ActiveEntry> {
    list.iter()
        .filter(|e| scope.is_none_or(|s| s.contains(&e.machine)))
        .cloned()
        .collect()
}

/// `"sha256:<hex>"` over the body without `digest` and the clocks' instance and lamport.
pub(crate) fn digest_of(body: &Value) -> String {
    let mut copy = body.clone();
    if let Some(o) = copy.as_object_mut() {
        o.remove("digest");
    }
    for end in ["from", "to"] {
        if let Some(c) = copy["kline"]["clocks"]
            .get_mut(end)
            .and_then(Value::as_object_mut)
        {
            c.remove("instance");
            c.remove("lamport");
        }
    }
    format!("sha256:{}", sha256_hex(jcs(&copy).as_bytes()))
}

/// The id a snapshot is stored under: its digest's hex.
fn id_of(body: &Value) -> String {
    let d = body["digest"].as_str().unwrap_or_default();
    d.strip_prefix("sha256:").unwrap_or(d).to_owned()
}

fn is_id(id: &str) -> bool {
    !id.is_empty()
        && id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// `RE_SNAPSHOT_STORE`: `memory` (default), `file:<dir>`, `off`. `qdrant:<url>`
/// is phase 3 and is off until then, never a boot failure.
#[derive(Default)]
pub(crate) enum Store {
    #[default]
    Memory,
    File(PathBuf),
    Off,
}

#[derive(Default)]
pub(crate) struct Snapshots {
    store: Store,
    memory: BTreeMap<String, Value>,
}

impl Snapshots {
    pub(crate) fn from_environment() -> Self {
        let mode = std::env::var("RE_SNAPSHOT_STORE").unwrap_or_default();
        let store = if mode.is_empty() || mode == "memory" {
            Store::Memory
        } else if let Some(dir) = mode.strip_prefix("file:") {
            let _ = fs::create_dir_all(dir);
            Store::File(PathBuf::from(dir))
        } else {
            Store::Off
        };
        Self {
            store,
            memory: BTreeMap::new(),
        }
    }

    fn enabled(&self) -> bool {
        !matches!(self.store, Store::Off)
    }

    fn put(&mut self, id: &str, body: &Value) {
        match &self.store {
            Store::File(dir) => {
                let tmp = dir.join(format!("{id}.json.tmp"));
                if fs::write(&tmp, format!("{}\n", jcs(body))).is_ok() {
                    let _ = fs::rename(tmp, dir.join(format!("{id}.json")));
                }
            }
            _ => {
                self.memory.insert(id.to_owned(), body.clone());
            }
        }
    }

    fn get(&self, id: &str) -> Option<Value> {
        if !is_id(id) {
            return None;
        }
        match &self.store {
            Store::File(dir) => {
                serde_json::from_str(&fs::read_to_string(dir.join(format!("{id}.json"))).ok()?).ok()
            }
            _ => self.memory.get(id).cloned(),
        }
    }

    fn remove(&mut self, id: &str) -> bool {
        if self.get(id).is_none() {
            return false;
        }
        match &self.store {
            Store::File(dir) => fs::remove_file(dir.join(format!("{id}.json"))).is_ok(),
            _ => self.memory.remove(id).is_some(),
        }
    }

    /// `{id, digest, scope, span, machines}`, oldest first by `clocks.to.lamport`.
    fn list(&self) -> Value {
        let mut bodies: Vec<Value> = match &self.store {
            Store::File(dir) => fs::read_dir(dir)
                .into_iter()
                .flatten()
                .filter_map(|e| e.ok())
                .filter_map(|e| {
                    let name = e.file_name().into_string().ok()?;
                    self.get(name.strip_suffix(".json")?)
                })
                .collect(),
            _ => self.memory.values().cloned().collect(),
        };
        bodies.sort_by(|a, b| {
            let l = |v: &Value| {
                v["kline"]["clocks"]["to"]["lamport"]
                    .as_f64()
                    .unwrap_or(0.0)
            };
            l(a).total_cmp(&l(b))
        });
        let out: Vec<Value> = bodies
            .iter()
            .map(|b| {
                json!({ "id": id_of(b), "digest": b["digest"], "scope": b["kline"]["scope"],
                        "span": b["kline"]["span"], "machines": b["corpus"]["machines"] })
            })
            .collect();
        json!({ "snapshots": out })
    }
}

fn reply(status: StatusCode, body: Value) -> Response {
    (status, Json(body)).into_response()
}

fn error(status: StatusCode, message: impl Into<String>) -> Response {
    reply(status, json!({ "error": message.into() }))
}

fn disabled() -> Response {
    error(StatusCode::SERVICE_UNAVAILABLE, "snapshots are disabled")
}

impl Engine {
    /// The engine's controls by scope, and the retunable overrides, by corpus
    /// machine name (SURFACE_SPEC "The snapshot body").
    fn snapshot_state(&self) -> (Value, Value, Value) {
        let name_of: BTreeMap<&str, &str> = self
            .machines
            .values()
            .map(|m| (m.id.as_str(), m.name.as_str()))
            .collect();
        let mut controls = Map::new();
        let mut machine_controls = Map::new();
        for c in self.controls() {
            let name = c["name"].as_str().unwrap_or_default().to_owned();
            if c["scope"] == "engine" {
                controls.insert(name, c["value"].clone());
            } else if let Some(per_id) = c["value"].as_object() {
                for (id, v) in per_id {
                    let Some(machine) = name_of.get(id.as_str()) else {
                        continue;
                    };
                    let entry = machine_controls
                        .entry((*machine).to_owned())
                        .or_insert_with(|| json!({}));
                    entry[&name] = v.clone();
                }
            }
        }
        let mut overrides = Map::new();
        for m in self.machines.values() {
            overrides.insert(
                m.name.clone(),
                json!({ "arbiterRule": m.arbiter_rule, "outputMergeTransformation": m.output_merge,
                        "outputMergeLocked": true }),
            );
        }
        (
            Value::Object(controls),
            Value::Object(machine_controls),
            Value::Object(overrides),
        )
    }

    fn capture_snapshot(&mut self, body: &Value) -> Result<Value, (StatusCode, Value)> {
        let fail = |status: StatusCode, message: String| (status, json!({ "error": message }));
        let known: BTreeSet<String> = self.machines.values().map(|m| m.name.clone()).collect();
        let scope: Option<BTreeSet<String>> = match &body["scope"] {
            Value::Array(names) => {
                let names: BTreeSet<String> = names
                    .iter()
                    .filter_map(|n| n.as_str().map(str::to_owned))
                    .collect();
                if let Some(missing) = names.iter().find(|n| !known.contains(*n)) {
                    return Err(fail(
                        StatusCode::UNPROCESSABLE_ENTITY,
                        format!("no machine {missing}"),
                    ));
                }
                Some(names)
            }
            Value::Null => None,
            Value::String(s) if s == "corpus" => None,
            _ => {
                return Err(fail(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "scope is \"corpus\" or a list of machine names".to_owned(),
                ));
            }
        };
        let mut ends = [None, None];
        for (i, end) in ["from", "to"].iter().enumerate() {
            let v = &body[*end];
            if v.is_null() {
                continue;
            }
            match v.as_f64() {
                Some(n) if n >= 0.0 && n.fract() == 0.0 => ends[i] = Some(n as u64),
                _ => {
                    return Err(fail(
                        StatusCode::UNPROCESSABLE_ENTITY,
                        format!("{end} is a step number"),
                    ));
                }
            }
        }
        self.refresh_corpus_facts();
        let r = &self.retention;
        let Some(newest) = r.steps.back().map(|s| s.step) else {
            return Err(fail(StatusCode::GONE, "no step is retained".to_owned()));
        };
        let to = ends[1].or(ends[0]).unwrap_or(newest);
        let from = ends[0].unwrap_or(to);
        if from > to {
            return Err(fail(
                StatusCode::UNPROCESSABLE_ENTITY,
                "span.from is after span.to".to_owned(),
            ));
        }
        for s in [from, to] {
            if r.at(s).is_none() {
                return Err(fail(
                    StatusCode::GONE,
                    format!("step {s} is no longer retained"),
                ));
            }
        }
        let Some(before) = r.before(from) else {
            return Err(fail(
                StatusCode::GONE,
                format!("step {} is no longer retained", from as i64 - 1),
            ));
        };

        // The scope's regions, and the output cells it shares with a machine outside it.
        let mut inputs = Vec::new();
        let mut outputs = Vec::new();
        let mut others = Vec::new();
        let scope_json = match &scope {
            Some(names) => {
                for m in self.machines.values() {
                    let Some((input, output)) = kline::regions(m) else {
                        continue;
                    };
                    if names.contains(&m.name) {
                        inputs.push(input);
                        outputs.push(output);
                    } else {
                        others.push(output);
                    }
                }
                json!(names)
            }
            None => json!("corpus"),
        };
        let mut shared = BTreeSet::new();
        for o in &outputs {
            for x in &others {
                for c in o.0.max(x.0)..(o.0 + o.1).min(x.0 + x.1) {
                    shared.insert(c);
                }
            }
        }
        let mut pairs = Vec::new();
        for k in from..=to {
            let Some(s) = r.at(k) else {
                return Err(fail(
                    StatusCode::GONE,
                    format!("step {k} is no longer retained"),
                ));
            };
            let mut ops: Vec<&Op> = s
                .ops
                .iter()
                .filter(|op| scope.as_ref().is_none_or(|n| n.contains(&op.machine)))
                .collect();
            ops.sort_by(|a, b| (a.region.0, &a.machine).cmp(&(b.region.0, &b.machine)));
            let contribs: Vec<Value> = ops
                .into_iter()
                .map(|op| json!({ "machine": op.machine, "region": { "offset": op.region.0, "length": op.region.1 }, "values": op.values }))
                .collect();
            let keep_in = scope.as_ref().map(|_| inputs.as_slice());
            let keep_out = scope.as_ref().map(|_| outputs.as_slice());
            pairs.push(json!({
                "stepNumber": k,
                "isre": entry_json(&s.isre, keep_in),
                "osre": entry_json(&s.osre, keep_out),
                "contributions": contribs,
            }));
        }
        let instance = self.clock.instance().to_owned();
        let clock =
            |s: &Retained| json!({ "instance": instance, "lamport": s.lamport, "step": s.step });
        let (controls, machine_controls, machine_overrides) = self.snapshot_state();
        let arbitration = if self.steps.controls.arbitration_retention {
            Value::Array(
                self.steps
                    .retained
                    .range(from..=to)
                    .map(|(step, (at, records))| self.retained_step_json(*step, *at, records))
                    .collect(),
            )
        } else {
            Value::Null
        };
        let r = &self.retention;
        let mut snapshot = json!({
            "format": "re-snapshot/1",
            "corpus": { "algorithm": "sha256/resident-declared-v1", "fingerprint": self.corpus_fingerprint,
                        "machines": self.machines.len() },
            "controls": controls,
            "machineControls": machine_controls,
            "machineOverrides": machine_overrides,
            "kline": {
                "scope": scope_json,
                "span": { "from": from, "to": to },
                "clocks": { "from": clock(r.at(from).expect("checked")), "to": clock(r.at(to).expect("checked")) },
                "activeBefore": kline::actives_json(only(&before, scope.as_ref())),
                "pairs": pairs,
                "activeAfter": kline::actives_json(only(&r.at(to).expect("checked").after, scope.as_ref())),
                "sharedCells": shared.into_iter().collect::<Vec<_>>(),
            },
            "arbitration": arbitration,
        });
        snapshot["digest"] = json!(digest_of(&snapshot));
        Ok(snapshot)
    }

    /// Re-establish a snapshot: controls, machine controls, overrides, then
    /// `activeAfter`; for a corpus scope also the history and the step count.
    /// A restore is a reset for K-line capture. It commits no step.
    fn restore_snapshot(&mut self, snap: &Value) -> Result<(), String> {
        let names: Vec<String> = self
            .controls()
            .into_iter()
            .filter(|c| c["scope"] == "engine")
            .filter_map(|c| c["name"].as_str().map(str::to_owned))
            .collect();
        for name in names {
            let v = &snap["controls"][&name];
            if v.is_null() {
                continue;
            }
            self.set_control(&name, &json!({ "value": v }))
                .map_err(|e| e.message)?;
        }
        let id_of: BTreeMap<String, String> = self
            .machines
            .values()
            .map(|m| (m.name.clone(), m.id.clone()))
            .collect();
        for (name, per) in snap["machineControls"].as_object().into_iter().flatten() {
            if let (Some(id), Some(b)) = (id_of.get(name), per["transitionsInhibited"].as_bool())
                && let Some(m) = self.machines.get_mut(id)
            {
                m.transitions_inhibited = b;
            }
        }
        for (name, o) in snap["machineOverrides"].as_object().into_iter().flatten() {
            let Some(id) = id_of.get(name) else { continue };
            if let Some(t) = o["outputMergeTransformation"].as_str() {
                let t = t.to_lowercase();
                if !crate::OUTPUT_MERGES.contains(&t.as_str()) {
                    return Err(format!("unknown outputMergeTransformation: {t}"));
                }
                if let Some(m) = self.machines.get_mut(id) {
                    m.output_merge = t;
                }
            }
        }
        let kl = &snap["kline"];
        let corpus = kl["scope"].is_string();
        let scope: BTreeSet<String> = if corpus {
            id_of.keys().cloned().collect()
        } else {
            kl["scope"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|n| n.as_str().map(str::to_owned))
                .collect()
        };
        let mut after = Vec::new();
        let mut by_machine: BTreeMap<String, Vec<(String, String, bool)>> = BTreeMap::new();
        for e in kl["activeAfter"].as_array().into_iter().flatten() {
            let s = |k: &str| e[k].as_str().unwrap_or_default().to_owned();
            let jm = e["justMatched"].as_bool().unwrap_or(false);
            by_machine
                .entry(s("machine"))
                .or_default()
                .push((s("sequence"), s("event"), jm));
            after.push(ActiveEntry {
                machine: s("machine"),
                sequence: s("sequence"),
                event: s("event"),
                just_matched: jm,
            });
        }
        for m in self.machines.values_mut() {
            if !scope.contains(&m.name) {
                continue;
            }
            for seq in m.sequences.values_mut() {
                for ev in seq.events.values_mut() {
                    ev.active = false;
                    ev.was_just_matched = false;
                    ev.predecessor_chain.clear();
                }
            }
            for (sequence, event, jm) in by_machine.get(&m.name).into_iter().flatten() {
                if let Some(ev) = m
                    .sequences
                    .get_mut(sequence)
                    .and_then(|s| s.events.get_mut(event))
                {
                    ev.active = true;
                    ev.was_just_matched = *jm;
                }
            }
        }
        if corpus {
            let to = kl["span"]["to"].as_f64().unwrap_or(0.0) as u64;
            let pairs = kl["pairs"].as_array().cloned().unwrap_or_default();
            self.steps.restore_history(
                pairs.iter().map(|p| entry_from(&p["isre"])).collect(),
                pairs.iter().map(|p| entry_from(&p["osre"])).collect(),
                to,
            );
            self.retention.restore(after);
        }
        if self.klines.controls.capture
            && let Err(e) = self.klines.on_reset()
        {
            crate::kline_failed(&mut self.klines, &e);
        }
        Ok(())
    }
}

fn body_of(bytes: &Bytes) -> Value {
    serde_json::from_slice(bytes).unwrap_or(Value::Null)
}

/// `POST /api/runtime/snapshot {scope?, from?, to?}` → `{id, digest, span}`.
pub(crate) async fn capture(State(engine): State<SharedEngine>, bytes: Bytes) -> Response {
    let body = body_of(&bytes);
    let Ok(mut state) = engine.write() else {
        return error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "engine state is unavailable",
        );
    };
    if !state.snapshots.enabled() {
        return disabled();
    }
    match state.capture_snapshot(&body) {
        Ok(snap) => {
            let id = id_of(&snap);
            state.snapshots.put(&id, &snap);
            reply(
                StatusCode::OK,
                json!({ "id": id, "digest": snap["digest"], "span": snap["kline"]["span"] }),
            )
        }
        Err((status, body)) => reply(status, body),
    }
}

/// `GET /api/runtime/snapshots`.
pub(crate) async fn list(State(engine): State<SharedEngine>) -> Response {
    let Ok(state) = engine.read() else {
        return error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "engine state is unavailable",
        );
    };
    if !state.snapshots.enabled() {
        return disabled();
    }
    reply(StatusCode::OK, state.snapshots.list())
}

/// `GET /api/runtime/snapshot/:id`.
pub(crate) async fn get(State(engine): State<SharedEngine>, Path(id): Path<String>) -> Response {
    let Ok(state) = engine.read() else {
        return error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "engine state is unavailable",
        );
    };
    if !state.snapshots.enabled() {
        return disabled();
    }
    match state.snapshots.get(&id) {
        Some(snap) => reply(StatusCode::OK, snap),
        None => error(StatusCode::NOT_FOUND, format!("no snapshot {id}")),
    }
}

/// `DELETE /api/runtime/snapshot/:id`.
pub(crate) async fn delete(State(engine): State<SharedEngine>, Path(id): Path<String>) -> Response {
    let Ok(mut state) = engine.write() else {
        return error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "engine state is unavailable",
        );
    };
    if !state.snapshots.enabled() {
        return disabled();
    }
    if state.snapshots.remove(&id) {
        reply(StatusCode::OK, json!({ "deleted": id }))
    } else {
        error(StatusCode::NOT_FOUND, format!("no snapshot {id}"))
    }
}

/// `POST /api/runtime/restore {id} | {snapshot}` → `{digest, step}`.
pub(crate) async fn restore(State(engine): State<SharedEngine>, bytes: Bytes) -> Response {
    let body = body_of(&bytes);
    let Ok(mut state) = engine.write() else {
        return error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "engine state is unavailable",
        );
    };
    if !state.snapshots.enabled() {
        return disabled();
    }
    let snap = if body["snapshot"].is_object() {
        body["snapshot"].clone()
    } else if let Some(id) = body["id"].as_str() {
        match state.snapshots.get(id) {
            Some(s) => s,
            None => return error(StatusCode::NOT_FOUND, format!("no snapshot {id}")),
        }
    } else {
        return error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "body is {id} or {snapshot}",
        );
    };
    if snap["format"] != "re-snapshot/1" {
        return error(StatusCode::UNPROCESSABLE_ENTITY, "unknown format");
    }
    if snap["digest"].as_str() != Some(digest_of(&snap).as_str()) {
        return error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "the digest does not match the body",
        );
    }
    state.refresh_corpus_facts();
    let expected = snap["corpus"]["fingerprint"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    if expected != state.corpus_fingerprint {
        return reply(
            StatusCode::CONFLICT,
            json!({ "error": "corpus mismatch", "expected": expected, "actual": state.corpus_fingerprint }),
        );
    }
    if let Err(message) = state.restore_snapshot(&snap) {
        return error(StatusCode::UNPROCESSABLE_ENTITY, message);
    }
    let step = if snap["kline"]["scope"].is_string() {
        snap["kline"]["span"]["to"].clone()
    } else {
        json!(state.steps.completed())
    };
    reply(
        StatusCode::OK,
        json!({ "digest": snap["digest"], "step": step }),
    )
}
