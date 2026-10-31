//! The step path (#11, slice 1): `POST /api/perceive`, the trajectory
//! histories and step completion.
//!
//! SURFACE_SPEC.md, "Trajectory histories" and "Step completion", governs; C++
//! `PerceptualSpaceRuntime::run_phases` and `to_json(SimulationStep)` are the
//! concrete form this follows. One step:
//!
//! 1. ISRE(n) is captured, before any machine reads its input.
//! 2. Every machine composes from ISRE(n) in parallel; the scoped threads are
//!    joined before anything resolves OSRE.
//! 3. Each machine's completed outputs fold under its declared transformation
//!    into one merge operation; the batch is ordered by `machineName`, then
//!    `region.offset`, both corpus-declared.
//! 4. A cell one machine writes is stored as written; a contended cell resolves
//!    under its `arbitration-registry.json` rule (ARBITER_CONTRACT.md). OSRE(n)
//!    is observed at that commit.
//! 5. The (ISRE, OSRE) pair is appended in one action, the instance clock ticks,
//!    and the completion is published, waking every waiter for step n.
//!
//! Not yet (later slices of #11): the event bus (`eventBus` is `[]`), governance
//! and deprecation on merge operations (so a SEVERITY tie falls through to the
//! value), the perceptual-simulation routes, the `only` selector and
//! `matchAlgorithmOverride`.

use super::*;
use std::collections::VecDeque;
use std::time::Duration;

/// Entries each trajectory history keeps (SURFACE_SPEC.md: capped at 1024).
const TRAJECTORY_CAPACITY: usize = 1024;
const PAIR_TIMEOUT_DEFAULT_MS: u64 = 5000;
const PAIR_TIMEOUT_MAX_MS: u64 = 60000;

/// One trajectory entry: the dense vector, sparse and lossless.
#[derive(Clone, Debug)]
pub(crate) struct TrajectoryEntry {
    step_number: u64,
    length: usize,
    non_zero: Vec<(usize, f64)>,
}

impl TrajectoryEntry {
    fn sparse(step_number: u64, dense: &[f64]) -> Self {
        Self {
            step_number,
            length: dense.len(),
            non_zero: dense
                .iter()
                .enumerate()
                .filter(|(_, value)| **value != 0.0)
                .map(|(index, value)| (index, *value))
                .collect(),
        }
    }

    fn to_json(&self) -> Value {
        json!({
            "stepNumber": self.step_number,
            "length": self.length,
            "nonZero": self
                .non_zero
                .iter()
                .map(|(index, value)| json!({ "index": index, "value": value }))
                .collect::<Vec<_>>(),
        })
    }
}

/// The step's state: the perceptual space, the two histories, and the
/// completion point observers wait on.
pub(crate) struct StepState {
    space: Vec<f64>,
    /// The next step's number; steps are numbered from 0 and a reset restarts them.
    next_step: u64,
    isre: VecDeque<TrajectoryEntry>,
    osre: VecDeque<TrajectoryEntry>,
    /// The newest committed step, -1 before the first since boot or reset.
    completed: tokio::sync::watch::Sender<i64>,
}

impl Default for StepState {
    fn default() -> Self {
        Self {
            space: Vec::new(),
            next_step: 0,
            isre: VecDeque::new(),
            osre: VecDeque::new(),
            completed: tokio::sync::watch::channel(-1).0,
        }
    }
}

impl StepState {
    pub(crate) fn completed(&self) -> i64 {
        *self.completed.borrow()
    }

    /// Step numbers restart, so the completion point does too; the space and
    /// both histories empty. The clock keeps its value.
    pub(crate) fn reset(&mut self) {
        self.space.iter_mut().for_each(|cell| *cell = 0.0);
        self.next_step = 0;
        self.isre.clear();
        self.osre.clear();
        self.completed.send_replace(-1);
    }

    fn commit(&mut self, isre: TrajectoryEntry, osre: TrajectoryEntry) {
        let step = isre.step_number as i64;
        for (history, entry) in [(&mut self.isre, isre), (&mut self.osre, osre)] {
            if history.len() == TRAJECTORY_CAPACITY {
                history.pop_front();
            }
            history.push_back(entry);
        }
        self.completed.send_replace(step);
    }

    fn pair(&self, step: u64) -> Option<(&TrajectoryEntry, &TrajectoryEntry)> {
        fn find(history: &VecDeque<TrajectoryEntry>, step: u64) -> Option<&TrajectoryEntry> {
            history.iter().find(|entry| entry.step_number == step)
        }
        Some((find(&self.isre, step)?, find(&self.osre, step)?))
    }
}

/// `domains/arbitration-registry.json`: the declared rule for each contended
/// cell. A cell it does not name resolves under PRECEDENCE.
#[derive(Default)]
pub(crate) struct ArbitrationRegistry {
    entries: BTreeMap<usize, ArbitrationEntry>,
}

struct ArbitrationEntry {
    rule: String,
    within_rank: String,
    provider_ranks: BTreeMap<String, i64>,
}

impl ArbitrationRegistry {
    /// From `ARBITRATION_REGISTRY`, else beside the corpus, as C++ looks.
    pub(crate) fn load(machines_dir: &FsPath) -> Self {
        let mut candidates: Vec<std::path::PathBuf> = Vec::new();
        if let Some(path) = std::env::var_os("ARBITRATION_REGISTRY") {
            candidates.push(path.into());
        }
        candidates.push(machines_dir.join("../domains/arbitration-registry.json"));
        candidates.push(machines_dir.join("domains/arbitration-registry.json"));
        candidates.push("../RealityEngine_Machines/domains/arbitration-registry.json".into());
        let Some(found) = candidates.into_iter().find(|path| path.is_file()) else {
            eprintln!(
                "[arbiter] no arbitration registry found; contended cells fall back to PRECEDENCE"
            );
            return Self::default();
        };
        let document: Value = match fs::read_to_string(&found)
            .ok()
            .and_then(|text| serde_json::from_str(&text).ok())
        {
            Some(document) => document,
            None => {
                eprintln!("[arbiter] failed to read {}", found.display());
                return Self::default();
            }
        };
        let entries: BTreeMap<usize, ArbitrationEntry> = document["entries"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|entry| {
                let cell = usize::try_from(entry["cell"].as_u64()?).ok()?;
                let provider_ranks = entry["providerRanks"]
                    .as_object()
                    .into_iter()
                    .flatten()
                    .filter_map(|(provider, rank)| Some((provider.clone(), rank.as_i64()?)))
                    .collect();
                Some((
                    cell,
                    ArbitrationEntry {
                        rule: string(entry.get("rule"), "PRECEDENCE"),
                        within_rank: string(entry.get("withinRank"), ""),
                        provider_ranks,
                    },
                ))
            })
            .collect();
        eprintln!(
            "[arbiter] loaded {} contended cell declaration(s) from {}",
            entries.len(),
            found.display()
        );
        Self { entries }
    }
}

/// One contestant for a contended cell.
#[derive(Clone)]
struct Contribution {
    value: f64,
    provider: &'static str,
    rag_status_code: String,
}

/// GREEN or absent 0 < AMBER 1 < RED 2 (lifeSafety 3 has no machine source).
fn severity_rank(rag: &str) -> f64 {
    match rag {
        "RED" => 2.0,
        "AMBER" => 1.0,
        _ => 0.0,
    }
}

/// Deterministic 3 > measured 2 > generated 1; a machine is deterministic.
fn determinism_rank(provider: &str) -> f64 {
    match provider {
        "machine" => 3.0,
        "sensor" | "mqtt" | "healthkit" | "stream" | "ui" | "synthetic" => 2.0,
        _ => 1.0,
    }
}

/// The contestants holding the best score, in their original order.
fn pick(
    contestants: &[Contribution],
    score: impl Fn(&Contribution) -> f64,
    max: bool,
) -> Vec<Contribution> {
    let best = contestants
        .iter()
        .map(&score)
        .fold(None, |best: Option<f64>, s| match best {
            Some(b) if (max && b >= s) || (!max && b <= s) => Some(b),
            _ => Some(s),
        });
    contestants
        .iter()
        .filter(|c| Some(score(c)) == best)
        .cloned()
        .collect()
}

/// C++ `resolve_cell`: the declared rule over a contended cell's contestants.
fn resolve_cell(contestants: &[Contribution], entry: Option<&ArbitrationEntry>) -> f64 {
    if contestants.len() == 1 {
        return contestants[0].value;
    }
    let rule = entry.map_or("PRECEDENCE", |e| e.rule.as_str());
    let value = |c: &Contribution| c.value;
    let severity = |c: &Contribution| severity_rank(&c.rag_status_code);
    let winners = match rule {
        "OR" | "MAX" => pick(contestants, value, true),
        "AND" | "MIN" => pick(contestants, value, false),
        "SEVERITY" => pick(&pick(contestants, severity, true), value, true),
        "MEAN" => {
            return contestants.iter().map(|c| c.value).sum::<f64>() / contestants.len() as f64;
        }
        _ => {
            let rank = |c: &Contribution| {
                entry
                    .and_then(|e| e.provider_ranks.get(c.provider))
                    .map_or_else(|| determinism_rank(c.provider), |rank| *rank as f64)
            };
            let at_top = pick(contestants, rank, true);
            match entry.map_or("", |e| e.within_rank.as_str()) {
                "SEVERITY" => pick(&pick(&at_top, severity, true), value, true),
                "MIN" | "AND" => pick(&at_top, value, false),
                _ => pick(&at_top, value, true),
            }
        }
    };
    winners[0].value
}

/// What one machine composed from ISRE(n).
struct Composed {
    id: String,
    snapshot: Vec<f64>,
    transition: Value,
}

struct MergeOperation {
    offset: usize,
    length: usize,
    machine_id: String,
    machine_name: String,
    sequence_ids: Vec<String>,
    values: Vec<f64>,
    provenance: Vec<String>,
}

impl MergeOperation {
    fn to_json(&self) -> Value {
        json!({
            "region": { "offset": self.offset, "length": self.length },
            "machineId": self.machine_id,
            "machineName": self.machine_name,
            "sequenceIds": self.sequence_ids,
            "values": self.values,
            "provenance": self.provenance,
        })
    }
}

/// A committed step, before the caller's include flags shape it.
pub(crate) struct Step {
    step_number: u64,
    timestamp: u128,
    machine_results: serde_json::Map<String, Value>,
    active_regions: Vec<Value>,
    merge_batch: Vec<Value>,
    perceptual_space: Vec<f64>,
}

impl Step {
    /// C++ `to_json(SimulationStep)`: a field not asked for is omitted, never
    /// emptied, since an empty array is a claim that there was nothing.
    fn to_json(
        &self,
        machine_results: bool,
        perceptual_space: bool,
        active_regions: bool,
    ) -> Value {
        let mut out = json!({
            "stepNumber": self.step_number,
            "timestamp": self.timestamp,
            "mergeBatch": self.merge_batch,
            "eventBus": [],
        });
        if active_regions {
            out["activeRegions"] = json!(self.active_regions);
        }
        if perceptual_space {
            out["perceptualSpace"] = json!(self.perceptual_space);
            out["perceptualSpaceIsDebugProjection"] = json!(true);
        }
        if machine_results {
            out["machineResults"] = Value::Object(self.machine_results.clone());
        }
        out
    }
}

fn region(mapping: Option<&Value>) -> (usize, usize) {
    (
        integer(mapping.and_then(|r| r.get("offset")), 0),
        integer(mapping.and_then(|r| r.get("length")), 0),
    )
}

impl Engine {
    /// The width of the perceptual space: the seed, grown to every machine's
    /// mapping and to what has been presented.
    fn space_width(&self) -> usize {
        self.dimension
            .max(self.required_dimension())
            .max(self.steps.space.len())
    }

    /// One step (`POST /api/perceive`): `input` becomes the space, and the
    /// corpus is presented with it.
    pub(crate) fn perceive(&mut self, input: &[f64]) -> Result<Step, EngineError> {
        let width = self.space_width().max(input.len());
        let space = &mut self.steps.space;
        space.resize(width, 0.0);
        space.iter_mut().for_each(|cell| *cell = 0.0);
        space[..input.len()].copy_from_slice(input);

        let step_number = self.steps.next_step;
        self.steps.next_step += 1;
        // ISRE(n): what every machine below reads, captured before any does.
        let isre = TrajectoryEntry::sparse(step_number, &self.steps.space);

        // Compose in parallel; the scope is the join, and nothing below runs
        // until every composer has returned.
        let space = &self.steps.space;
        let mut jobs: Vec<(&String, &mut Machine)> = self.machines.iter_mut().collect();
        let threads = std::thread::available_parallelism().map_or(1, |n| n.get());
        let chunk = jobs.len().div_ceil(threads).max(1);
        let composed: Vec<Composed> = std::thread::scope(|scope| {
            let handles: Vec<_> = jobs
                .chunks_mut(chunk)
                .map(|part| {
                    scope.spawn(move || {
                        part.iter_mut()
                            .map(|(id, machine)| {
                                let snapshot: Vec<f64> = (0..machine.input_length)
                                    .map(|i| {
                                        space.get(machine.input_offset + i).copied().unwrap_or(0.0)
                                    })
                                    .collect();
                                let transition = machine.respond(&snapshot);
                                Composed {
                                    id: (*id).clone(),
                                    snapshot,
                                    transition,
                                }
                            })
                            .collect::<Vec<_>>()
                    })
                })
                .collect();
            handles
                .into_iter()
                .flat_map(|handle| handle.join().expect("a composer panicked"))
                .collect()
        });

        let mut machine_results = serde_json::Map::new();
        let mut merges: Vec<MergeOperation> = Vec::new();
        let mut active_regions: Vec<(usize, usize, String, &'static str)> = Vec::new();
        for Composed {
            id,
            snapshot,
            mut transition,
        } in composed
        {
            let machine = &self.machines[&id];
            if !machine.transitions_inhibited {
                self.coverage.record(&id, &machine.name, &transition);
            }
            let mapping = machine.raw.get("perceptualMapping");
            let (input_offset, input_length) = region(mapping.and_then(|m| m.get("input")));
            let (output_offset, output_length) = region(mapping.and_then(|m| m.get("output")));
            let output_vector = transition["machineOutput"]["vector"].clone();
            let merged: Option<Vec<f64>> = transition["mergedOutput"]
                .as_array()
                .map(|values| values.iter().filter_map(Value::as_f64).collect());

            // The completed Reality Events and their evidence, recorded whether
            // or not the fold produced a value.
            let mut sequence_ids: Vec<String> = Vec::new();
            let mut provenance: Vec<String> = Vec::new();
            if transition["arbiterMetadata"]["shouldOutput"] == json!(true) {
                for (sequence_id, result) in transition["sequenceResults"]
                    .as_object()
                    .into_iter()
                    .flatten()
                {
                    for output in result["assertedOutputs"].as_array().into_iter().flatten() {
                        sequence_ids.push(sequence_id.clone());
                        for vertex in output["provenance"].as_array().into_iter().flatten() {
                            if let Some(vertex) = vertex.as_str()
                                && !provenance.iter().any(|seen| seen == vertex)
                            {
                                provenance.push(vertex.to_owned());
                            }
                        }
                    }
                }
            }
            sequence_ids.sort();
            sequence_ids.dedup();

            let has_output = !output_vector.is_null() || merged.is_some();
            active_regions.push((input_offset, input_length, id.clone(), "input"));
            if has_output {
                active_regions.push((output_offset, output_length, id.clone(), "output"));
            }
            if let Some(object) = transition.as_object_mut() {
                object.remove("machineId");
            }
            machine_results.insert(
                id.clone(),
                json!({
                    "machineId": id,
                    "machineName": machine.name,
                    "inputEvent": snapshot,
                    "outputVector": output_vector,
                    "mergedOutputVector": merged,
                    "outputMergeTransformation": machine.output_merge,
                    "inputRegion": { "offset": input_offset, "length": input_length },
                    "outputRegion": if has_output {
                        json!({ "offset": output_offset, "length": output_length })
                    } else {
                        Value::Null
                    },
                    "transitionResult": transition,
                }),
            );
            // A refused fold presents nothing, and nothing is not zeros.
            if let Some(values) = merged {
                merges.push(MergeOperation {
                    offset: output_offset,
                    length: output_length,
                    machine_id: id.clone(),
                    machine_name: machine.name.clone(),
                    sequence_ids,
                    values,
                    provenance,
                });
            }
        }
        merges.sort_by(|a, b| {
            a.machine_name
                .cmp(&b.machine_name)
                .then(a.offset.cmp(&b.offset))
        });

        // Gather, resolve, commit (ARBITER_CONTRACT.md 2). A cell with one
        // contestant is stored as written; it emits no arbitration record.
        let space = &mut self.steps.space;
        let mut fan_in = vec![0u32; space.len()];
        for merge in &merges {
            for i in 0..merge.length.min(merge.values.len()) {
                if let Some(count) = fan_in.get_mut(merge.offset + i) {
                    *count += 1;
                }
            }
        }
        let mut osre_cells: Vec<(usize, f64)> = Vec::new();
        let mut contended: BTreeMap<usize, Vec<Contribution>> = BTreeMap::new();
        for merge in &merges {
            for i in 0..merge.length.min(merge.values.len()) {
                let cell = merge.offset + i;
                let value = merge.values[i];
                match fan_in.get(cell) {
                    Some(1) => {
                        space[cell] = value;
                        if value != 0.0 {
                            osre_cells.push((cell, value));
                        }
                    }
                    Some(_) => contended.entry(cell).or_default().push(Contribution {
                        value,
                        provider: "machine",
                        rag_status_code: String::new(),
                    }),
                    None => {}
                }
            }
        }
        for (cell, contestants) in &contended {
            let value = resolve_cell(contestants, self.arbitration.entries.get(cell));
            space[*cell] = value;
            if value != 0.0 {
                osre_cells.push((*cell, value));
            }
        }
        osre_cells.sort_by_key(|(cell, _)| *cell);
        let osre = TrajectoryEntry {
            step_number,
            length: space.len(),
            non_zero: osre_cells,
        };

        active_regions.sort();
        let step = Step {
            step_number,
            timestamp: now_ms(),
            machine_results,
            active_regions: active_regions
                .into_iter()
                .map(|(offset, length, machine_id, kind)| {
                    json!({ "offset": offset, "length": length, "machineId": machine_id, "type": kind })
                })
                .collect(),
            merge_batch: merges.iter().map(MergeOperation::to_json).collect(),
            perceptual_space: space.clone(),
        };

        // The commit: the clock ticks for the step, then the pair is appended
        // and the completion published in one action.
        self.clock.tick().map_err(|message| EngineError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message,
        })?;
        self.steps.commit(isre, osre);
        Ok(step)
    }

    fn trajectory(&self, isre: bool, from: u64, limit: usize) -> Value {
        let history = if isre {
            &self.steps.isre
        } else {
            &self.steps.osre
        };
        let entries: Vec<Value> = history
            .iter()
            .filter(|entry| entry.step_number >= from)
            .take(if limit == 0 { usize::MAX } else { limit })
            .map(TrajectoryEntry::to_json)
            .collect();
        json!({ "history": entries })
    }
}

/// `POST /api/perceive`'s input: exactly one of `vector`, `sparseVector`,
/// `domainVectors`, assembled to at least the space's width.
fn assemble_input(body: &Value, width: usize) -> Result<Vec<f64>, EngineError> {
    let number = |value: &Value, what: &str| {
        value
            .as_f64()
            .ok_or_else(|| EngineError::bad_request(format!("{what} must be a number")))
    };
    let index = |value: &Value, what: &str| {
        value
            .as_u64()
            .and_then(|n| usize::try_from(n).ok())
            .ok_or_else(|| {
                EngineError::bad_request(format!("{what} must be a non-negative integer"))
            })
    };
    if body["vector"].is_array() {
        return input_from_body(body, "vector");
    }
    if let Some(cells) = body["sparseVector"].as_array() {
        let mut pairs = Vec::with_capacity(cells.len());
        for cell in cells {
            pairs.push((
                index(&cell["index"], "sparseVector index")?,
                number(&cell["value"], "sparseVector value")?,
            ));
        }
        let length = pairs
            .iter()
            .map(|(i, _)| i + 1)
            .max()
            .unwrap_or(0)
            .max(width);
        let mut assembled = vec![0.0; length];
        for (i, value) in pairs {
            assembled[i] = value;
        }
        return Ok(assembled);
    }
    if let Some(regions) = body["domainVectors"].as_array() {
        let mut writes = Vec::with_capacity(regions.len());
        for region in regions {
            let offset = index(&region["offset"], "domainVectors offset")?;
            let values = region["values"]
                .as_array()
                .map(|values| {
                    values
                        .iter()
                        .map(|v| number(v, "domainVectors value"))
                        .collect::<Result<Vec<_>, _>>()
                })
                .transpose()?
                .unwrap_or_default();
            writes.push((offset, values));
        }
        let length = writes
            .iter()
            .map(|(o, v)| o + v.len())
            .max()
            .unwrap_or(0)
            .max(width);
        let mut assembled = vec![0.0; length];
        for (offset, values) in writes {
            assembled[offset..offset + values.len()].copy_from_slice(&values);
        }
        return Ok(assembled);
    }
    Err(EngineError::bad_request(
        "Provide exactly one of: vector, sparseVector, domainVectors",
    ))
}

pub(crate) async fn perceive(
    State(engine): State<SharedEngine>,
    body: Result<Json<Value>, JsonRejection>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let Json(body) = body.map_err(json_rejection)?;
    // Not in this slice, and refused rather than ignored: ignoring `only`
    // would answer the whole universe to a caller who asked for a selection.
    for key in ["only", "matchAlgorithmOverride"] {
        if body.get(key).is_some_and(|v| !v.is_null()) {
            return Err(error_response(EngineError::bad_request(format!(
                "{key} is not implemented by this runtime yet (jateeter/RealityEngine_Rust#11)"
            ))));
        }
    }
    let flag = |key: &str, default: bool| body[key].as_bool().unwrap_or(default);
    let compact = flag("compact", false);
    let include_machine_results = flag("includeMachineResults", !compact);
    let include_perceptual_space = flag("includePerceptualSpace", true);
    let include_active_regions = flag("includeActiveRegions", true);

    let mut state = engine.write().map_err(|_| error_response(lock_error()))?;
    let input = assemble_input(&body, state.space_width()).map_err(error_response)?;
    let step = state.perceive(&input).map_err(error_response)?;
    Ok(Json(step.to_json(
        include_machine_results,
        include_perceptual_space,
        include_active_regions,
    )))
}

fn history_query(query: &HashMap<String, String>) -> Result<(u64, usize), EngineError> {
    let parse = |key: &str| -> Result<u64, EngineError> {
        match query.get(key).filter(|v| !v.is_empty()) {
            None => Ok(0),
            Some(raw) => raw
                .parse::<i64>()
                .map(|n| n.max(0) as u64)
                .map_err(|_| EngineError::bad_request(format!("{key} must be an integer"))),
        }
    };
    Ok((parse("from")?, parse("limit")? as usize))
}

pub(crate) async fn isre_history(
    State(engine): State<SharedEngine>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let (from, limit) = history_query(&query).map_err(error_response)?;
    let state = engine.read().map_err(|_| error_response(lock_error()))?;
    Ok(Json(state.trajectory(true, from, limit)))
}

pub(crate) async fn osre_history(
    State(engine): State<SharedEngine>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let (from, limit) = history_query(&query).map_err(error_response)?;
    let state = engine.read().map_err(|_| error_response(lock_error()))?;
    Ok(Json(state.trajectory(false, from, limit)))
}

fn whole_number(raw: &str) -> Option<u64> {
    (!raw.is_empty() && raw.bytes().all(|b| b.is_ascii_digit()))
        .then(|| raw.parse().ok())
        .flatten()
}

/// `GET /api/engine/steps/:n/pair?timeoutMs=W`: step n's committed pair,
/// waiting up to W ms on the completion point (not polling) for it.
pub(crate) async fn step_pair(
    State(engine): State<SharedEngine>,
    Path(raw): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let fail =
        |status: StatusCode, message: String| error_response(EngineError { status, message });
    let step = whole_number(&raw).ok_or_else(|| {
        fail(
            StatusCode::BAD_REQUEST,
            "step must be a non-negative integer".to_owned(),
        )
    })?;
    let timeout_ms = match query.get("timeoutMs") {
        None => PAIR_TIMEOUT_DEFAULT_MS,
        Some(raw) => whole_number(raw)
            .filter(|ms| *ms <= PAIR_TIMEOUT_MAX_MS)
            .ok_or_else(|| {
                fail(
                    StatusCode::BAD_REQUEST,
                    format!("timeoutMs must be an integer in [0, {PAIR_TIMEOUT_MAX_MS}]"),
                )
            })?,
    };
    // Subscribe under the lock, then wait without it, so the step being
    // waited for can run.
    let mut completed = engine
        .read()
        .map_err(|_| error_response(lock_error()))?
        .steps
        .completed
        .subscribe();
    let target = i64::try_from(step).unwrap_or(i64::MAX);
    let resolved = tokio::time::timeout(
        Duration::from_millis(timeout_ms),
        completed.wait_for(|newest| *newest >= target),
    )
    .await;
    if !matches!(resolved, Ok(Ok(_))) {
        return Err(fail(
            StatusCode::REQUEST_TIMEOUT,
            format!("step {step} not resolved within {timeout_ms} ms"),
        ));
    }
    let state = engine.read().map_err(|_| error_response(lock_error()))?;
    let (isre, osre) = state.steps.pair(step).ok_or_else(|| {
        fail(
            StatusCode::GONE,
            format!("step {step} is no longer retained"),
        )
    })?;
    Ok(Json(
        json!({ "stepNumber": step, "isre": isre.to_json(), "osre": osre.to_json() }),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn contestant(value: f64, rag: &str) -> Contribution {
        Contribution {
            value,
            provider: "machine",
            rag_status_code: rag.to_owned(),
        }
    }

    fn entry(rule: &str, within: &str) -> ArbitrationEntry {
        ArbitrationEntry {
            rule: rule.to_owned(),
            within_rank: within.to_owned(),
            provider_ranks: BTreeMap::new(),
        }
    }

    #[test]
    fn contended_cells_resolve_under_the_declared_rule() {
        let cs = [
            contestant(1.0, "GREEN"),
            contestant(3.0, ""),
            contestant(2.0, "RED"),
        ];
        assert_eq!(
            resolve_cell(&cs, None),
            3.0,
            "PRECEDENCE: equal rank, the larger value"
        );
        assert_eq!(
            resolve_cell(&cs, Some(&entry("PRECEDENCE", "SEVERITY"))),
            2.0
        );
        assert_eq!(resolve_cell(&cs, Some(&entry("PRECEDENCE", "MIN"))), 1.0);
        assert_eq!(resolve_cell(&cs, Some(&entry("SEVERITY", ""))), 2.0);
        assert_eq!(resolve_cell(&cs, Some(&entry("AND", ""))), 1.0);
        assert_eq!(resolve_cell(&cs, Some(&entry("OR", ""))), 3.0);
        assert_eq!(resolve_cell(&cs, Some(&entry("MEAN", ""))), 2.0);
        assert_eq!(
            resolve_cell(&cs[..1], Some(&entry("AND", ""))),
            1.0,
            "one contestant is the value"
        );
    }

    #[test]
    fn a_trajectory_entry_is_sparse_and_lossless() {
        let entry = TrajectoryEntry::sparse(4, &[0.0, 1.0, 0.0, 2.5]);
        assert_eq!(
            entry.to_json(),
            json!({ "stepNumber": 4, "length": 4, "nonZero": [
                { "index": 1, "value": 1.0 }, { "index": 3, "value": 2.5 } ] })
        );
    }
}
