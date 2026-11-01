//! The step path (#11): `POST /api/perceive`, the trajectory histories, step
//! completion, governance on merge operations and the event bus.
//!
//! SURFACE_SPEC.md, "Trajectory histories" and "Step completion", governs; C++
//! `PerceptualSpaceRuntime::run_phases` and `to_json(SimulationStep)` are the
//! concrete form this follows. One step:
//!
//! 1. ISRE(n) is captured, before any machine reads its input.
//! 2. Every machine composes from ISRE(n) in parallel; the scoped threads are
//!    joined before anything resolves OSRE.
//! 3. Each machine's completed outputs fold under its declared transformation
//!    into one merge operation, carrying the governance its contributors
//!    resolve to (the highest severity wins) and any deprecation mark; the
//!    batch is ordered by `machineName`, then `region.offset`, both
//!    corpus-declared.
//! 4. A cell one machine writes is stored as written; a contended cell resolves
//!    under its `arbitration-registry.json` rule (ARBITER_CONTRACT.md). OSRE(n)
//!    is observed at that commit.
//! 5. The event bus: a firing a meta-machine subscribes to (`metadata.compose`)
//!    writes 1 at the subscribed bit, which latches into every later step's
//!    input until a reset.
//! 6. The (ISRE, OSRE) pair is appended in one action, the instance clock ticks,
//!    and the completion is published, waking every waiter for step n.
//!
//! The perceptual-simulation routes step through the same phases
//! (`simulation.rs`). With `phaseDetail` on, each step is timed in the five
//! universal phases (SURFACE_SPEC.md, "`phaseDetail`: step phase timing").

use super::*;
use std::collections::{BTreeSet, VecDeque};
use std::time::{Duration, Instant};

/// Entries each trajectory history keeps (SURFACE_SPEC.md: capped at 1024).
const TRAJECTORY_CAPACITY: usize = 1024;
const PAIR_TIMEOUT_DEFAULT_MS: u64 = 5000;
const PAIR_TIMEOUT_MAX_MS: u64 = 60000;
/// `historyLimit`'s declared default: steps the step history keeps.
pub(crate) const HISTORY_LIMIT_DEFAULT: usize = 250;

/// The universal step phases, in order (SURFACE_SPEC.md, "`phaseDetail`").
pub(crate) const STEP_PHASES: [&str; 5] = [
    "step.isre_capture",
    "step.compose",
    "step.resolve",
    "step.commit",
    "step.publish",
];

/// The engine-scoped step controls (`/api/engine/config`) and what
/// `phaseDetail` has measured. Neither is touched by a reset: the controls are
/// configuration, and the phase totals are counters since boot.
pub(crate) struct StepControls {
    pub(crate) history_limit: usize,
    pub(crate) include_active_regions: bool,
    pub(crate) include_machine_results: bool,
    pub(crate) include_perceptual_space: bool,
    pub(crate) phase_detail: bool,
    pub(crate) arbitration_retention: bool,
    pub(crate) arbitration_window: usize,
    /// Time summed per phase over the measured steps, and how many there were.
    pub(crate) phase_time: [Duration; 5],
    pub(crate) measured_steps: u64,
}

impl Default for StepControls {
    fn default() -> Self {
        Self {
            history_limit: HISTORY_LIMIT_DEFAULT,
            include_active_regions: true,
            include_machine_results: true,
            include_perceptual_space: true,
            phase_detail: false,
            arbitration_retention: false,
            arbitration_window: crate::arbitration::WINDOW_DEFAULT,
            phase_time: [Duration::ZERO; 5],
            measured_steps: 0,
        }
    }
}

/// `POST /api/perceive`'s `only`: keep what is attributable to these sequences
/// or these machine names (SURFACE_SPEC.md, "`only` selector"). Present, it
/// selects, even when it names nothing.
#[derive(Default)]
pub(crate) struct Selector {
    pub(crate) sequence_ids: BTreeSet<String>,
    pub(crate) machine_names: BTreeSet<String>,
}

impl Selector {
    /// The selector a body's `only` declares: `None` when absent, `null` or
    /// not an object, which is no selector at all.
    pub(crate) fn from_body(only: &Value) -> Option<Self> {
        let only = only.as_object()?;
        let strings = |key: &str| -> BTreeSet<String> {
            only.get(key)
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        };
        Some(Self {
            sequence_ids: strings("sequenceIds"),
            machine_names: strings("machineNames"),
        })
    }

    fn wants_sequences(&self, ids: &Value) -> bool {
        ids.as_array()
            .into_iter()
            .flatten()
            .any(|id| id.as_str().is_some_and(|id| self.sequence_ids.contains(id)))
    }
}

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
    pub(crate) space: Vec<f64>,
    /// The next step's number; steps are numbered from 0 and a reset restarts them.
    next_step: u64,
    isre: VecDeque<TrajectoryEntry>,
    osre: VecDeque<TrajectoryEntry>,
    /// The newest committed step, -1 before the first since boot or reset.
    completed: tokio::sync::watch::Sender<i64>,
    /// Event-bus bits written so far: re-applied over every later input, so
    /// a caller's vector cannot clobber a milestone, until a reset.
    latched: BTreeSet<usize>,
    /// Every step, pushed or simulated, newest first
    /// (`GET /api/perceptual-simulation/history`).
    pub(crate) history: VecDeque<Arc<Step>>,
    pub(crate) simulation: crate::simulation::Simulation,
    pub(crate) controls: StepControls,
    /// The newest step's arbitration records (the legacy `/api/arbitration`).
    pub(crate) last_arbitration: Vec<ArbitrationRecord>,
    /// Retained steps' records, keyed by step, with the Lamport value each
    /// committed at, while `arbitrationRetention` is on.
    pub(crate) retained: BTreeMap<u64, (u64, Vec<ArbitrationRecord>)>,
    /// Each pushed step, as `POST /api/perceive` answered it, for
    /// `GET /api/engine/stream`.
    pub(crate) stream: tokio::sync::broadcast::Sender<String>,
}

impl Default for StepState {
    fn default() -> Self {
        Self {
            space: Vec::new(),
            next_step: 0,
            isre: VecDeque::new(),
            osre: VecDeque::new(),
            completed: tokio::sync::watch::channel(-1).0,
            latched: BTreeSet::new(),
            history: VecDeque::new(),
            simulation: crate::simulation::Simulation::default(),
            controls: StepControls::default(),
            last_arbitration: Vec::new(),
            retained: BTreeMap::new(),
            stream: tokio::sync::broadcast::channel(64).0,
        }
    }
}

impl StepState {
    pub(crate) fn completed(&self) -> i64 {
        *self.completed.borrow()
    }

    /// Step numbers restart, so the completion point does too; the space and
    /// every history empty, and a simulation run stops at its start. The clock
    /// keeps its value, and a committed sequence stays committed.
    pub(crate) fn reset(&mut self) {
        self.space.iter_mut().for_each(|cell| *cell = 0.0);
        self.next_step = 0;
        self.isre.clear();
        self.osre.clear();
        self.history.clear();
        self.simulation.rewind();
        self.last_arbitration.clear();
        self.retained.clear();
        self.latched.clear();
        self.completed.send_replace(-1);
    }

    /// Appends the (ISRE, OSRE) pair: B4.
    fn commit(&mut self, isre: TrajectoryEntry, osre: TrajectoryEntry) {
        for (history, entry) in [(&mut self.isre, isre), (&mut self.osre, osre)] {
            if history.len() == TRAJECTORY_CAPACITY {
                history.pop_front();
            }
            history.push_back(entry);
        }
    }

    /// Publishes the completion point, waking every waiter for `step`: B5.
    fn publish(&mut self, step: u64) {
        self.completed.send_replace(step as i64);
    }

    fn retain_arbitration(&mut self, step: u64, lamport: u64, records: &[ArbitrationRecord]) {
        let controls = &self.controls;
        if controls.arbitration_retention && controls.arbitration_window > 0 {
            self.retained.insert(step, (lamport, records.to_vec()));
            self.prune_arbitration(step as i64);
        }
    }

    /// Drops every retained step outside the window ending at `latest`.
    pub(crate) fn prune_arbitration(&mut self, latest: i64) {
        let first = latest - self.controls.arbitration_window as i64 + 1;
        self.retained.retain(|step, _| *step as i64 >= first);
    }

    /// Keeps the newest `historyLimit` steps.
    pub(crate) fn trim_history(&mut self) {
        self.history.truncate(self.controls.history_limit);
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
    pub(crate) entries: BTreeMap<usize, ArbitrationEntry>,
    /// The file the entries came from, as found (a deployment fact).
    pub(crate) source: Option<String>,
}

pub(crate) struct ArbitrationEntry {
    pub(crate) rule: String,
    within_rank: String,
    pub(crate) provider_ranks: BTreeMap<String, i64>,
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
        Self {
            entries,
            source: Some(found.to_string_lossy().into_owned()),
        }
    }
}

/// One contestant for a contended cell (C++ `Contribution`).
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Contribution {
    pub(crate) value: f64,
    pub(crate) provider: &'static str,
    /// The machine id, or a PE source id.
    pub(crate) origin_id: String,
    /// The contributing sequences, comma-joined: attribution, not identity.
    pub(crate) ces_id: String,
    /// `"0"` for a machine, which contributes one operation per cell.
    pub(crate) output_vector_id: String,
    pub(crate) rag_status_code: String,
}

/// How one contended cell resolved (ARBITER_CONTRACT.md 6): every contestant,
/// and the ones the rule set aside.
#[derive(Clone, Debug)]
pub(crate) struct ArbitrationRecord {
    pub(crate) instant: u64,
    pub(crate) cell: usize,
    pub(crate) rule: String,
    pub(crate) resolved: f64,
    pub(crate) contributors: Vec<Contribution>,
    pub(crate) suppressed: Vec<Contribution>,
}

/// GREEN or absent 0 < AMBER 1 < RED 2 (lifeSafety 3 has no machine source).
fn severity_rank(rag: &str) -> f64 {
    match rag {
        "RED" => 2.0,
        "AMBER" => 1.0,
        _ => 0.0,
    }
}

/// The provider's determinism class, as `/api/arbitration` spells it. An
/// unregistered provider is generated: misclassifying a generated source as
/// measured would let irreproducible content outrank a reading.
pub(crate) fn determinism(provider: &str) -> &'static str {
    match provider {
        "machine" => "deterministic",
        "sensor" | "mqtt" | "healthkit" | "stream" | "ui" | "synthetic" => "measured",
        _ => "generated",
    }
}

/// Deterministic 3 > measured 2 > generated 1.
pub(crate) fn determinism_rank(provider: &str) -> f64 {
    match determinism(provider) {
        "deterministic" => 3.0,
        "measured" => 2.0,
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

/// C++ `resolve_cell`: the declared rule over a contended cell's contestants,
/// and its record. One contestant resolves to itself and needs no record.
fn resolve_cell(
    cell: usize,
    instant: u64,
    contestants: &[Contribution],
    entry: Option<&ArbitrationEntry>,
) -> (f64, Option<ArbitrationRecord>) {
    if contestants.len() == 1 {
        return (contestants[0].value, None);
    }
    let rule = entry.map_or("PRECEDENCE", |e| e.rule.as_str());
    let record = |resolved: f64, contributors: Vec<Contribution>, suppressed| {
        Some(ArbitrationRecord {
            instant,
            cell,
            rule: rule.to_owned(),
            resolved,
            contributors,
            suppressed,
        })
    };
    let value = |c: &Contribution| c.value;
    let severity = |c: &Contribution| severity_rank(&c.rag_status_code);
    let winners = match rule {
        "OR" | "MAX" => pick(contestants, value, true),
        "AND" | "MIN" => pick(contestants, value, false),
        "SEVERITY" => pick(&pick(contestants, severity, true), value, true),
        "MEAN" => {
            // Floating-point addition is not associative: the sum runs in the
            // canonical order, so every runtime adds the same terms the same way.
            let mut ordered = contestants.to_vec();
            ordered.sort_by(|a, b| {
                (&a.origin_id, &a.ces_id, &a.output_vector_id).cmp(&(
                    &b.origin_id,
                    &b.ces_id,
                    &b.output_vector_id,
                ))
            });
            let mean = ordered.iter().map(|c| c.value).sum::<f64>() / ordered.len() as f64;
            return (mean, record(mean, ordered, Vec::new()));
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
    let resolved = winners[0].value;
    let suppressed = contestants
        .iter()
        .filter(|c| !winners.contains(c))
        .cloned()
        .collect();
    (resolved, record(resolved, contestants.to_vec(), suppressed))
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
    /// The joined paging decision of the contributors, whole (C++ `to_json(PagingDecision)`).
    governance: Option<Value>,
    /// Its `ragStatusCode`, which a SEVERITY cell resolves by.
    rag_status_code: String,
    deprecation: Option<Value>,
}

impl MergeOperation {
    fn to_json(&self) -> Value {
        let mut out = json!({
            "region": { "offset": self.offset, "length": self.length },
            "machineId": self.machine_id,
            "machineName": self.machine_name,
            "sequenceIds": self.sequence_ids,
            "values": self.values,
            "provenance": self.provenance,
        });
        if let Some(governance) = &self.governance {
            out["governance"] = governance.clone();
        }
        if let Some(deprecation) = &self.deprecation {
            out["deprecation"] = deprecation.clone();
        }
        out
    }
}

/// C++ `resolve_governance`: the first `metadata.triggerConfig.rules` entry for
/// `sequence_id` whose `outputMatches` equals `values`, with the rule's
/// `governance` overriding the machine's `metadata.governance`.
pub(crate) fn resolve_governance(
    machine: &Machine,
    sequence_id: &str,
    values: &[f64],
) -> Option<Value> {
    let metadata = machine.raw.get("metadata")?;
    let rule = metadata["triggerConfig"]["rules"]
        .as_array()?
        .iter()
        .filter(|rule| rule.is_object())
        .find(|rule| {
            rule["sequenceId"].as_str() == Some(sequence_id)
                && rule["outputMatches"].as_array().is_some_and(|matches| {
                    matches.len() == values.len()
                        && matches
                            .iter()
                            .zip(values)
                            .all(|(m, v)| m.as_f64() == Some(*v))
                })
        })?;
    let machine_gov = &metadata["governance"];
    let has_machine_gov = machine_gov.is_object();
    let rule_gov = &rule["governance"];
    let text = |value: &Value| value.as_str().filter(|s| !s.is_empty()).map(str::to_owned);
    // Rule override, then the machine's default, then the fallback.
    let pick = |key: &str, fallback: &str| -> String {
        rule_gov[key]
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| {
                if has_machine_gov {
                    machine_gov[key].as_str().unwrap_or(fallback).to_owned()
                } else {
                    fallback.to_owned()
                }
            })
    };
    let nullable = |s: String| if s.is_empty() { Value::Null } else { json!(s) };
    let process_status = rule["processStatus"].as_str().unwrap_or("").to_owned();
    let sla_seconds = if rule_gov.is_object() && rule_gov["slaSeconds"].is_number() {
        rule_gov["slaSeconds"].as_f64().map(|n| n as i64)
    } else if has_machine_gov && machine_gov["sla"].is_object() && !process_status.is_empty() {
        machine_gov["sla"][&process_status]
            .as_f64()
            .map(|n| n as i64)
    } else {
        None
    };
    let mut contact = serde_json::Map::new();
    for field in ["primary", "secondary"] {
        let from_rule = rule_gov["contact"][field].as_str();
        let from_machine = machine_gov["contact"][field]
            .as_str()
            .filter(|_| has_machine_gov);
        if let Some(value) = from_rule.or(from_machine).filter(|v| !v.is_empty()) {
            contact.insert(field.to_owned(), json!(value));
        }
    }
    let mut decision = json!({
        "machineId": machine.id,
        "machineName": machine.name,
        "sequenceId": sequence_id,
        "ragStatusCode": nullable(rule["ragStatusCode"].as_str().unwrap_or("").to_owned()),
        "actionCode": Value::Null,
        "processStatus": nullable(process_status),
        "ownerTeam": pick("ownerTeam", "unrouted"),
        "slaSeconds": sla_seconds,
        "runbook": nullable(pick("runbook", "")),
        "escalationPolicy": nullable(pick("escalationPolicy", "")),
        "contact": contact,
        "source": if rule_gov.is_object() {
            "rule-with-override"
        } else if has_machine_gov {
            "rule-only"
        } else {
            "machine-fallback"
        },
        "hasMachineGovernance": has_machine_gov,
    });
    if let Some(description) = text(&rule["description"]) {
        decision["description"] = json!(description);
    }
    Some(decision)
}

/// Whole days since a `YYYY-MM-DD` (or `YYYY-MM-DDT...`) date, 0 when it does
/// not parse. Midnight UTC; C++ reads it as local midnight.
fn days_since(date: &str) -> i64 {
    let mut parts = date
        .get(..10)
        .unwrap_or("")
        .split('-')
        .map(str::parse::<i64>);
    let (Some(Ok(y)), Some(Ok(m)), Some(Ok(d))) = (parts.next(), parts.next(), parts.next()) else {
        return 0;
    };
    // Days from 1970-01-01 to y-m-d (proleptic Gregorian).
    let (y, m) = if m <= 2 { (y - 1, m + 9) } else { (y, m - 3) };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * m + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let epoch_day = era * 146097 + doe - 719468;
    let today = (now_ms() / 86_400_000) as i64;
    today - epoch_day
}

/// A committed step, before the caller's include flags shape it.
pub(crate) struct Step {
    step_number: u64,
    timestamp: u128,
    machine_results: serde_json::Map<String, Value>,
    active_regions: Vec<Value>,
    merge_batch: Vec<Value>,
    event_bus: Vec<Value>,
    perceptual_space: Vec<f64>,
}

impl Step {
    /// C++ `to_json(SimulationStep)`: a field not asked for is omitted, never
    /// emptied, since an empty array is a claim that there was nothing. With a
    /// selector, each list keeps only what it attributes to the selection, and
    /// entries are skipped before they are copied, not removed afterwards.
    pub(crate) fn to_json(
        &self,
        machine_results: bool,
        perceptual_space: bool,
        active_regions: bool,
        only: Option<&Selector>,
    ) -> Value {
        // The machine ids the selected names resolve to, through
        // machineResults: ids are minted per runtime, so a caller names
        // machines, while mergeBatch, eventBus and activeRegions carry ids.
        let selected: BTreeSet<&str> = only
            .map(|only| {
                self.machine_results
                    .iter()
                    .filter(|(_, result)| {
                        result["machineName"]
                            .as_str()
                            .is_some_and(|name| only.machine_names.contains(name))
                    })
                    .map(|(id, _)| id.as_str())
                    .collect()
            })
            .unwrap_or_default();
        let by_id = |value: &Value| value.as_str().is_some_and(|id| selected.contains(id));
        let keep_merge = |op: &&Value| {
            only.is_none_or(|only| {
                only.wants_sequences(&op["sequenceIds"]) || by_id(&op["machineId"])
            })
        };
        let keep_write = |write: &&Value| {
            only.is_none_or(|only| {
                write["producerSequenceId"]
                    .as_str()
                    .is_some_and(|id| only.sequence_ids.contains(id))
                    || by_id(&write["producerMachineId"])
                    || by_id(&write["subscriberMachineId"])
            })
        };
        let keep_region = |region: &&Value| only.is_none() || by_id(&region["machineId"]);
        let keep_result = |id: &str| {
            only.is_none_or(|only| {
                selected.contains(id)
                    || self
                        .merge_batch
                        .iter()
                        .any(|op| op["machineId"] == id && only.wants_sequences(&op["sequenceIds"]))
            })
        };
        let merge_batch: Vec<&Value> = self.merge_batch.iter().filter(keep_merge).collect();
        let event_bus: Vec<&Value> = self.event_bus.iter().filter(keep_write).collect();
        let mut out = json!({
            "stepNumber": self.step_number,
            "timestamp": self.timestamp,
            "mergeBatch": merge_batch,
            "eventBus": event_bus,
        });
        if active_regions {
            let regions: Vec<&Value> = self.active_regions.iter().filter(keep_region).collect();
            out["activeRegions"] = json!(regions);
        }
        if perceptual_space {
            out["perceptualSpace"] = json!(self.perceptual_space);
            out["perceptualSpaceIsDebugProjection"] = json!(true);
        }
        if machine_results {
            let results: serde_json::Map<String, Value> = self
                .machine_results
                .iter()
                .filter(|(id, _)| keep_result(id))
                .map(|(id, result)| (id.clone(), result.clone()))
                .collect();
            out["machineResults"] = Value::Object(results);
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
    /// mapping, every subscribed event-bus bit, and what has been presented.
    pub(crate) fn space_width(&self) -> usize {
        let bits = self
            .subscriptions()
            .values()
            .flatten()
            .map(|(_, bit)| bit + 1)
            .max();
        self.dimension
            .max(self.required_dimension())
            .max(bits.unwrap_or(0))
            .max(self.steps.space.len())
    }

    /// Every meta-machine's `metadata.compose.subscriptions`, keyed by the
    /// producer (machine id, sequence id): the subscribers and their bits.
    fn subscriptions(&self) -> BTreeMap<(String, String), Vec<(String, usize)>> {
        let mut out: BTreeMap<(String, String), Vec<(String, usize)>> = BTreeMap::new();
        for machine in self.machines.values() {
            let subscriptions = machine.raw["metadata"]["compose"]["subscriptions"].as_array();
            for sub in subscriptions.into_iter().flatten() {
                let producer = sub["producerMachineId"].as_str().unwrap_or("");
                let sequence = sub["producerSequenceId"].as_str().unwrap_or("");
                let Some(bit) = sub["bitOffset"]
                    .as_u64()
                    .and_then(|b| usize::try_from(b).ok())
                else {
                    continue;
                };
                if producer.is_empty() || sequence.is_empty() {
                    continue;
                }
                out.entry((producer.to_owned(), sequence.to_owned()))
                    .or_default()
                    .push((machine.id.clone(), bit));
            }
        }
        out
    }

    /// One step (`POST /api/perceive`): `input` becomes the space, and the
    /// corpus is presented with it.
    pub(crate) fn perceive(
        &mut self,
        input: &[f64],
        overriding: Option<&str>,
    ) -> Result<Arc<Step>, EngineError> {
        let width = self.space_width().max(input.len());
        let space = &mut self.steps.space;
        space.resize(width, 0.0);
        space.iter_mut().for_each(|cell| *cell = 0.0);
        space[..input.len()].copy_from_slice(input);
        // Latched event-bus bits survive the caller's vector, which zero-fills
        // past its length.
        for bit in &self.steps.latched {
            if let Some(cell) = space.get_mut(*bit) {
                *cell = 1.0;
            }
        }

        let step_number = self.steps.next_step;
        self.steps.next_step += 1;
        self.run_phases(step_number, overriding)
    }

    /// The phases of one step over the space as it now stands: what
    /// `POST /api/perceive` and a perceptual-simulation step share (C++
    /// `run_phases`). The step is recorded in the step history, newest first.
    /// `overriding` is a comparator applied above every element's own.
    pub(crate) fn run_phases(
        &mut self,
        step_number: u64,
        overriding: Option<&str>,
    ) -> Result<Arc<Step>, EngineError> {
        // B0. Whether this step is timed is decided here, once, so a toggle
        // mid-step never splits one step's phases.
        let timed = self.steps.controls.phase_detail;
        let mut phase_time = [Duration::ZERO; 5];
        let mut mark = Instant::now();
        let mut boundary = |phase: usize| {
            if timed {
                let now = Instant::now();
                phase_time[phase] = now - mark;
                mark = now;
            }
        };
        let subscriptions = self.subscriptions();
        // ISRE(n): what every machine below reads, captured before any does.
        let isre = TrajectoryEntry::sparse(step_number, &self.steps.space);
        boundary(0);

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
                                let transition = machine.respond(&snapshot, overriding);
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
        boundary(1);

        let mut machine_results = serde_json::Map::new();
        let mut merges: Vec<MergeOperation> = Vec::new();
        let mut active_regions: Vec<(usize, usize, String, &'static str)> = Vec::new();
        // (machine id, sorted sequence ids, provenance) for every machine that
        // completed a Reality Event, whether or not its fold presented a value.
        let mut firings: Vec<(String, Vec<String>, Vec<String>)> = Vec::new();
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
            // One pending output per completed Reality Event: its sequence,
            // its own asserted values, and the action its metadata prescribes.
            let mut pending: Vec<(String, Vec<f64>, Option<String>)> = Vec::new();
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
                        let values = output["vector"]
                            .as_array()
                            .map(|v| v.iter().filter_map(Value::as_f64).collect())
                            .unwrap_or_default();
                        let action = output["metadata"]["action"].as_str().map(str::to_owned);
                        pending.push((sequence_id.clone(), values, action));
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
            if !sequence_ids.is_empty() {
                firings.push((id.clone(), sequence_ids.clone(), provenance.clone()));
            }

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
                // The join over the contributors (FOLD_PLACEMENT.md 3): each
                // resolves against its own asserted values; the highest
                // severity wins, ties to the smallest sequence id, and the
                // winning decision travels whole with its own action.
                let mut governance: Option<(f64, String, Value)> = None;
                for (sequence_id, values, action) in &pending {
                    // Deprecated fires count once per asserted output, so a
                    // sequence asserting twice in a step counts twice (C++,
                    // FOLD_PLACEMENT.md amendment A4).
                    if let Some(sequence) = machine.sequences.get(sequence_id)
                        && !sequence.deprecated_at.is_empty()
                    {
                        self.coverage.record_deprecated_fire(
                            &id,
                            &machine.name,
                            sequence_id,
                            &sequence.replaced_by,
                        );
                    }
                    let Some(mut decision) = resolve_governance(machine, sequence_id, values)
                    else {
                        continue;
                    };
                    let rank = severity_rank(decision["ragStatusCode"].as_str().unwrap_or(""));
                    let better = governance.as_ref().is_none_or(|(best, best_seq, _)| {
                        rank > *best || (rank == *best && sequence_id < best_seq)
                    });
                    if better {
                        decision["actionCode"] = action.clone().map_or(Value::Null, Value::from);
                        governance = Some((rank, sequence_id.clone(), decision));
                    }
                }
                // The mark: the smallest deprecated contributing sequence.
                let deprecation = sequence_ids.iter().find_map(|sequence_id| {
                    let sequence = machine.sequences.get(sequence_id)?;
                    if sequence.deprecated_at.is_empty() {
                        return None;
                    }
                    let mut mark = json!({
                        "since": sequence.deprecated_at,
                        "ageDays": days_since(&sequence.deprecated_at),
                    });
                    if !sequence.replaced_by.is_empty() {
                        mark["replacedBy"] = json!(sequence.replaced_by);
                    }
                    Some(mark)
                });
                let rag_status_code = governance
                    .as_ref()
                    .and_then(|(_, _, d)| d["ragStatusCode"].as_str())
                    .unwrap_or("")
                    .to_owned();
                // The joined decision is recorded once per merge operation.
                if let Some((_, _, decision)) = &governance {
                    let text = |key: &str, fallback: &'static str| {
                        decision[key]
                            .as_str()
                            .filter(|s| !s.is_empty())
                            .unwrap_or(fallback)
                            .to_owned()
                    };
                    self.coverage.record_paging_decision(
                        text("ownerTeam", "unrouted"),
                        text("processStatus", "unknown"),
                        text("ragStatusCode", "unknown"),
                        id.clone(),
                    );
                }
                merges.push(MergeOperation {
                    offset: output_offset,
                    length: output_length,
                    machine_id: id.clone(),
                    machine_name: machine.name.clone(),
                    sequence_ids,
                    values,
                    provenance,
                    governance: governance.map(|(_, _, decision)| decision),
                    rag_status_code,
                    deprecation,
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
                        origin_id: merge.machine_id.clone(),
                        ces_id: merge.sequence_ids.join(","),
                        output_vector_id: "0".to_owned(),
                        rag_status_code: merge.rag_status_code.clone(),
                    }),
                    None => {}
                }
            }
        }
        let mut records = Vec::new();
        for (cell, contestants) in &contended {
            let (value, record) = resolve_cell(
                *cell,
                step_number,
                contestants,
                self.arbitration.entries.get(cell),
            );
            records.extend(record);
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

        // The event bus, driven by the firings rather than the batch, so a
        // refused fold withdraws its value and not its subscribers. Written
        // after OSRE is observed: these bits reach the machines next step.
        let mut seen = BTreeSet::new();
        let mut writes: Vec<(String, usize, String, String, Vec<String>)> = Vec::new();
        for (producer, sequence_ids, provenance) in &firings {
            for sequence_id in sequence_ids {
                let key = (producer.clone(), sequence_id.clone());
                for (subscriber, bit) in subscriptions.get(&key).into_iter().flatten() {
                    if seen.insert((
                        subscriber.clone(),
                        *bit,
                        producer.clone(),
                        sequence_id.clone(),
                    )) {
                        writes.push((
                            subscriber.clone(),
                            *bit,
                            producer.clone(),
                            sequence_id.clone(),
                            provenance.clone(),
                        ));
                        self.steps.latched.insert(*bit);
                    }
                }
            }
        }
        writes.sort_by(|a, b| (&a.0, a.1, &a.2, &a.3).cmp(&(&b.0, b.1, &b.2, &b.3)));
        for (_, bit, ..) in &writes {
            if let Some(cell) = space.get_mut(*bit) {
                *cell = 1.0;
            }
        }
        let event_bus = writes
            .into_iter()
            .map(|(subscriber, bit, producer, sequence_id, provenance)| {
                json!({
                    "producerMachineId": producer,
                    "producerSequenceId": sequence_id,
                    "subscriberMachineId": subscriber,
                    "bitOffset": bit,
                    "value": 1.0,
                    "provenance": provenance,
                })
            })
            .collect();

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
            event_bus,
            perceptual_space: space.clone(),
        };

        boundary(2);

        // The commit: the clock ticks for the step and the pair is appended;
        // then the completion is published.
        let lamport = self.clock.tick().map_err(|message| EngineError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message,
        })?;
        self.steps.commit(isre, osre);
        // Retained before the completion is published, so an observer woken
        // for step n finds n's records already there.
        self.steps
            .retain_arbitration(step_number, lamport, &records);
        self.steps.last_arbitration = records;
        boundary(3);
        self.steps.publish(step_number);
        boundary(4);
        if timed {
            let controls = &mut self.steps.controls;
            for (total, spent) in controls.phase_time.iter_mut().zip(phase_time) {
                *total += spent;
            }
            controls.measured_steps += 1;
        }
        let step = Arc::new(step);
        self.steps.history.push_front(Arc::clone(&step));
        self.steps.trim_history();
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
    // Only an explicit override overrides; `matchAlgorithm` is not an
    // instruction (RealityEngine_CI#201).
    let overriding = match body["matchAlgorithmOverride"].as_str() {
        None => None,
        Some(name) => Some(comparator(name).map_err(error_response)?),
    };
    let only = Selector::from_body(&body["only"]);
    let flag = |key: &str, default: bool| body[key].as_bool().unwrap_or(default);

    let mut state = engine.write().map_err(|_| error_response(lock_error()))?;
    // The configured defaults; a request's own flag overrides one for that
    // call. `compact` omits exactly machineResults.
    let defaults = &state.steps.controls;
    let include_machine_results = flag(
        "includeMachineResults",
        !flag("compact", false) && defaults.include_machine_results,
    );
    let include_perceptual_space =
        flag("includePerceptualSpace", defaults.include_perceptual_space);
    let include_active_regions = flag("includeActiveRegions", defaults.include_active_regions);
    let input = assemble_input(&body, state.space_width()).map_err(error_response)?;
    let step = state.perceive(&input, overriding).map_err(error_response)?;
    let out = step.to_json(
        include_machine_results,
        include_perceptual_space,
        include_active_regions,
        only.as_ref(),
    );
    // No subscriber is not an error: the step stands whether or not anyone
    // is watching.
    if let Ok(bytes) = ecmascript_json(&out) {
        let _ = state
            .steps
            .stream
            .send(String::from_utf8_lossy(&bytes).into_owned());
    }
    Ok(Json(out))
}

/// A comparator's canonical name (C++ `comparator_from_string`). An unknown
/// name is a 500, as C++ answers it (owner ruling, 2026-11-01).
pub(crate) fn comparator(name: &str) -> Result<&'static str, EngineError> {
    match name.to_lowercase().as_str() {
        "equals" => Ok("equals"),
        "threshold" => Ok("threshold"),
        "pattern" => Ok("pattern"),
        "custom" => Ok("custom"),
        "gte" => Ok("gte"),
        _ => Err(EngineError::internal(format!(
            "Unknown comparator type: {name}"
        ))),
    }
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
            origin_id: format!("m{value}"),
            ces_id: "s".to_owned(),
            output_vector_id: "0".to_owned(),
            rag_status_code: rag.to_owned(),
        }
    }

    fn resolve(cs: &[Contribution], entry: Option<&ArbitrationEntry>) -> f64 {
        resolve_cell(7, 0, cs, entry).0
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
            resolve(&cs, None),
            3.0,
            "PRECEDENCE: equal rank, the larger value"
        );
        assert_eq!(resolve(&cs, Some(&entry("PRECEDENCE", "SEVERITY"))), 2.0);
        assert_eq!(resolve(&cs, Some(&entry("PRECEDENCE", "MIN"))), 1.0);
        assert_eq!(resolve(&cs, Some(&entry("SEVERITY", ""))), 2.0);
        assert_eq!(resolve(&cs, Some(&entry("AND", ""))), 1.0);
        assert_eq!(resolve(&cs, Some(&entry("OR", ""))), 3.0);
        assert_eq!(resolve(&cs, Some(&entry("MEAN", ""))), 2.0);
        assert_eq!(
            resolve(&cs[..1], Some(&entry("AND", ""))),
            1.0,
            "one contestant is the value"
        );
        // The record: every contestant, and the ones the rule set aside.
        let (_, record) = resolve_cell(7, 3, &cs, Some(&entry("PRECEDENCE", "SEVERITY")));
        let record = record.unwrap();
        assert_eq!(
            (record.instant, record.cell, record.rule.as_str()),
            (3, 7, "PRECEDENCE")
        );
        assert_eq!(record.resolved, 2.0);
        assert_eq!(record.contributors, cs);
        assert_eq!(record.suppressed, [cs[0].clone(), cs[1].clone()]);
        let (_, mean) = resolve_cell(7, 3, &cs, Some(&entry("MEAN", "")));
        assert!(mean.unwrap().suppressed.is_empty());
        assert!(resolve_cell(7, 3, &cs[..1], None).1.is_none());
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
