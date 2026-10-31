//! The step path: `POST /api/perceive`, the ISRE/OSRE trajectory histories and
//! the step completion point (SURFACE_SPEC.md, "Trajectory histories", "Step
//! completion", "Perceptual Simulation"; ARBITER_CONTRACT.md).
//!
//! One step, in the order the spec tiles it:
//!   B0 the step starts
//!   B1 ISRE(n) is captured from the perceptual space, before any machine reads
//!   B2 every machine composes from ISRE(n), in parallel, and the composition joins
//!   B3 each machine's collection is folded to one merge operation, the batch is
//!      canonically sorted, uncontended cells are stored and contended cells are
//!      resolved by the arbiter: OSRE(n)
//!   B4 the (ISRE(n), OSRE(n)) pair is committed together
//!   B5 the completion point is published (the caller, through its watch channel)
//!
//! Written against C++ `PerceptualSpaceRuntime::run_phases`, which every other
//! runtime matches; where this file departs from it, it says so.

use crate::{Engine, Machine, fold_outputs, integer, now_ms};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};

/// Trajectory histories are capped at 1024 entries (SURFACE_SPEC.md).
pub(crate) const TRAJECTORY_LIMIT: usize = 1024;

/// One trajectory entry: sparse, lossless, ascending index.
#[derive(Clone)]
pub(crate) struct TrajectoryEntry {
    pub step_number: i64,
    pub length: usize,
    pub non_zero: Vec<(usize, f64)>,
}

impl TrajectoryEntry {
    fn from_dense(step_number: i64, dense: &[f64]) -> Self {
        Self {
            step_number,
            length: dense.len(),
            non_zero: dense
                .iter()
                .enumerate()
                .filter(|(_, v)| **v != 0.0)
                .map(|(i, v)| (i, *v))
                .collect(),
        }
    }

    pub fn to_json(&self) -> Value {
        json!({
            "stepNumber": self.step_number,
            "length": self.length,
            "nonZero": self.non_zero.iter().map(|(i, v)| json!({ "index": i, "value": v })).collect::<Vec<_>>(),
        })
    }
}

#[derive(Default)]
pub(crate) struct Trajectories {
    pub isre: VecDeque<TrajectoryEntry>,
    pub osre: VecDeque<TrajectoryEntry>,
}

impl Trajectories {
    fn record(&mut self, isre: TrajectoryEntry, osre: TrajectoryEntry) {
        self.isre.push_back(isre);
        self.osre.push_back(osre);
        while self.isre.len() > TRAJECTORY_LIMIT {
            self.isre.pop_front();
            self.osre.pop_front();
        }
    }

    pub fn clear(&mut self) {
        self.isre.clear();
        self.osre.clear();
    }

    /// `?from=n` is the first stepNumber included, `?limit=n` caps the count.
    pub fn window(history: &VecDeque<TrajectoryEntry>, from: i64, limit: usize) -> Vec<Value> {
        history
            .iter()
            .filter(|e| e.step_number >= from)
            .take(if limit == 0 { usize::MAX } else { limit })
            .map(TrajectoryEntry::to_json)
            .collect()
    }

    pub fn pair(&self, n: i64) -> Option<Value> {
        let isre = self.isre.iter().find(|e| e.step_number == n)?;
        let osre = self.osre.iter().find(|e| e.step_number == n)?;
        Some(json!({ "stepNumber": n, "isre": isre.to_json(), "osre": osre.to_json() }))
    }
}

// ── The arbiter (ARBITER_CONTRACT.md §4, C++ arbiter.cpp) ─────────────────────

#[derive(Default)]
pub(crate) struct ArbitrationEntry {
    rule: String,
    within_rank: String,
    provider_ranks: BTreeMap<String, i64>,
}

/// The arbitration registry: the declared rule per contended cell.
#[derive(Default)]
pub(crate) struct ArbitrationRegistry {
    entries: BTreeMap<usize, ArbitrationEntry>,
    pub source: Option<PathBuf>,
}

impl ArbitrationRegistry {
    /// `ARBITRATION_REGISTRY`, else `<machines>/../domains/`, else
    /// `<machines>/domains/arbitration-registry.json`, as C++ finds it. Absent,
    /// contended cells fall back to `PRECEDENCE`.
    pub fn load(machines_dir: Option<&Path>) -> Self {
        let mut candidates: Vec<PathBuf> = Vec::new();
        if let Ok(explicit) = std::env::var("ARBITRATION_REGISTRY") {
            candidates.push(explicit.into());
        }
        if let Some(dir) = machines_dir {
            candidates.push(dir.join("../domains/arbitration-registry.json"));
            candidates.push(dir.join("domains/arbitration-registry.json"));
        }
        let Some(found) = candidates.into_iter().find(|p| p.is_file()) else {
            return Self::default();
        };
        let Ok(doc) = fs::read_to_string(&found).map(|t| serde_json::from_str::<Value>(&t)) else {
            return Self::default();
        };
        let mut registry = Self {
            source: Some(found),
            ..Self::default()
        };
        for entry in doc
            .ok()
            .as_ref()
            .and_then(|d| d["entries"].as_array())
            .into_iter()
            .flatten()
        {
            let Some(cell) = entry["cell"].as_u64() else {
                continue;
            };
            registry.entries.insert(
                cell as usize,
                ArbitrationEntry {
                    rule: entry["rule"].as_str().unwrap_or("PRECEDENCE").to_owned(),
                    within_rank: entry["withinRank"].as_str().unwrap_or("").to_owned(),
                    provider_ranks: entry["providerRanks"]
                        .as_object()
                        .into_iter()
                        .flatten()
                        .filter_map(|(k, v)| Some((k.clone(), v.as_i64()?)))
                        .collect(),
                },
            );
        }
        registry
    }
}

/// One machine's contribution to a contended cell.
struct Contribution {
    value: f64,
    provider: &'static str,
    rag_status_code: String,
    /// The contributing machine's id, and its contributing set joined by ","
    /// (C++ `originId`, `cesId`): MEAN sums in this order.
    origin_id: String,
    ces_id: String,
}

fn determinism_rank(provider: &str) -> i64 {
    match provider {
        "machine" => 3,
        "sensor" | "mqtt" | "healthkit" | "stream" | "ui" | "synthetic" => 2,
        _ => 1,
    }
}

pub(crate) fn severity_rank(rag: &str) -> i64 {
    match rag {
        "RED" => 2,
        "AMBER" => 1,
        _ => 0,
    }
}

/// The contributions maximising (or minimising) a score, ties kept.
fn pick<'a>(
    cs: &[&'a Contribution],
    score: impl Fn(&Contribution) -> f64,
    want_max: bool,
) -> Vec<&'a Contribution> {
    let best = cs.iter().map(|c| score(c)).fold(score(cs[0]), |b, s| {
        if (want_max && s > b) || (!want_max && s < b) {
            s
        } else {
            b
        }
    });
    cs.iter().copied().filter(|c| score(c) == best).collect()
}

/// C++ `resolve_cell`: one contested cell under its declared rule. Every
/// contributor in an OSRE is a machine (SURFACE_SPEC.md, "Trajectory
/// histories"), so `PRECEDENCE` falls through to its within-rank rule.
fn resolve_cell(contributions: &[Contribution], entry: Option<&ArbitrationEntry>) -> f64 {
    if contributions.len() == 1 {
        return contributions[0].value;
    }
    let all: Vec<&Contribution> = contributions.iter().collect();
    let value = |c: &Contribution| c.value;
    let severity = |c: &Contribution| severity_rank(&c.rag_status_code) as f64;
    let rule = entry.map_or("PRECEDENCE", |e| e.rule.as_str());
    let winners = match rule {
        "OR" | "MAX" => pick(&all, value, true),
        "AND" | "MIN" => pick(&all, value, false),
        "SEVERITY" => pick(&pick(&all, severity, true), value, true),
        // Float addition is not associative, so the sum runs in the canonical
        // order C++ sorts by: originId, then cesId.
        "MEAN" => {
            let mut ordered = all.clone();
            ordered.sort_by(|a, b| (&a.origin_id, &a.ces_id).cmp(&(&b.origin_id, &b.ces_id)));
            return ordered.iter().map(|c| c.value).sum::<f64>() / ordered.len() as f64;
        }
        _ => {
            let rank = |c: &Contribution| {
                entry
                    .and_then(|e| e.provider_ranks.get(c.provider).copied())
                    .unwrap_or_else(|| determinism_rank(c.provider)) as f64
            };
            let top = pick(&all, rank, true);
            match entry.map_or("", |e| e.within_rank.as_str()) {
                "SEVERITY" => pick(&pick(&top, severity, true), value, true),
                "MIN" | "AND" => pick(&top, value, false),
                _ => pick(&top, value, true),
            }
        }
    };
    winners[0].value
}

// ── Governance (C++ resolve_governance) ───────────────────────────────────────

/// The paging decision a machine's `metadata.triggerConfig` rule resolves for
/// one contributing sequence's own asserted values. `None` when no rule matches.
pub(crate) fn resolve_governance(
    machine: &Machine,
    sequence_id: &str,
    values: &[f64],
    action: &str,
) -> Option<Map<String, Value>> {
    let metadata = machine.raw.get("metadata")?;
    let rule = metadata["triggerConfig"]["rules"]
        .as_array()?
        .iter()
        .find(|rule| {
            rule["sequenceId"].as_str() == Some(sequence_id)
                && rule["outputMatches"].as_array().is_some_and(|m| {
                    m.len() == values.len()
                        && m.iter().zip(values).all(|(a, b)| a.as_f64() == Some(*b))
                })
        })?;
    let machine_gov = metadata.get("governance").filter(|g| g.is_object());
    let rule_gov = rule.get("governance").filter(|g| g.is_object());
    let text = |v: &Value| v.as_str().filter(|s| !s.is_empty()).map(str::to_owned);
    let pick = |field: &str, fallback: Option<String>| {
        rule_gov
            .and_then(|g| text(&g[field]))
            .or_else(|| machine_gov.and_then(|g| text(&g[field])))
            .or(fallback)
    };
    let nullable = |s: Option<String>| s.map_or(Value::Null, Value::String);
    let process_status = text(&rule["processStatus"]);
    let sla = rule_gov
        .and_then(|g| g["slaSeconds"].as_f64())
        .or_else(|| {
            let status = process_status.as_deref()?;
            machine_gov.and_then(|g| g["sla"][status].as_f64())
        })
        .map(|s| s.trunc());
    let mut contact = Map::new();
    for field in ["primary", "secondary"] {
        let found = rule_gov
            .and_then(|g| g["contact"][field].as_str())
            .or_else(|| machine_gov.and_then(|g| g["contact"][field].as_str()));
        if let Some(c) = found.filter(|c| !c.is_empty()) {
            contact.insert(field.to_owned(), json!(c));
        }
    }
    let mut d = Map::new();
    d.insert("machineId".into(), json!(machine.id));
    d.insert("machineName".into(), json!(machine.name));
    d.insert("sequenceId".into(), json!(sequence_id));
    d.insert(
        "ragStatusCode".into(),
        nullable(text(&rule["ragStatusCode"])),
    );
    d.insert(
        "actionCode".into(),
        nullable(Some(action.to_owned()).filter(|a| !a.is_empty())),
    );
    d.insert("processStatus".into(), nullable(process_status));
    d.insert(
        "ownerTeam".into(),
        json!(pick("ownerTeam", Some("unrouted".into()))),
    );
    d.insert("slaSeconds".into(), sla.map_or(Value::Null, |s| json!(s)));
    d.insert("runbook".into(), nullable(pick("runbook", None)));
    d.insert(
        "escalationPolicy".into(),
        nullable(pick("escalationPolicy", None)),
    );
    d.insert("contact".into(), Value::Object(contact));
    if let Some(description) = text(&rule["description"]) {
        d.insert("description".into(), json!(description));
    }
    let source = if rule_gov.is_some() {
        "rule-with-override"
    } else if machine_gov.is_some() {
        "rule-only"
    } else {
        "machine-fallback"
    };
    d.insert("source".into(), json!(source));
    d.insert("hasMachineGovernance".into(), json!(machine_gov.is_some()));
    Some(d)
}

// ── The step ──────────────────────────────────────────────────────────────────

/// What `POST /api/perceive` filters a step by (`only`, RealityEngine_CI#367).
#[derive(Default)]
pub(crate) struct Selector {
    pub active: bool,
    pub sequence_ids: BTreeSet<String>,
    pub machine_names: BTreeSet<String>,
}

pub(crate) struct StepRecord {
    pub step_number: i64,
    pub timestamp: u128,
    /// machine id -> the step's per-machine result (`machineResults` entry)
    pub machine_results: BTreeMap<String, Value>,
    pub machine_names: BTreeMap<String, String>,
    pub merge_batch: Vec<MergeOperation>,
    pub event_bus: Vec<Value>,
    pub perceptual_space: Vec<f64>,
    /// (offset, length, machineId, type), canonically sorted
    pub active_regions: Vec<(usize, usize, String, &'static str)>,
}

pub(crate) struct MergeOperation {
    pub offset: usize,
    pub length: usize,
    pub machine_id: String,
    pub machine_name: String,
    pub sequence_ids: Vec<String>,
    pub values: Vec<f64>,
    pub provenance: Vec<String>,
    pub governance: Option<Map<String, Value>>,
}

struct Composed {
    id: String,
    snapshot: Vec<f64>,
    transition: Value,
}

impl Engine {
    /// The perceptual space at the current width, zero-extended.
    fn space_at_width(&mut self) {
        if self.space.len() < self.dimension {
            self.space.resize(self.dimension, 0.0);
        }
    }

    /// `POST /api/perceive`: the vector replaces the space (zero-filled past its
    /// length, growing it if longer), the latched event-bus bits are re-applied,
    /// and one step runs.
    pub(crate) fn perceive(
        &mut self,
        input: &[f64],
        override_algorithm: Option<&str>,
    ) -> StepRecord {
        if input.len() > self.dimension {
            self.dimension = input.len();
        }
        self.space_at_width();
        self.space.iter_mut().for_each(|v| *v = 0.0);
        self.space[..input.len()].copy_from_slice(input);
        for &bit in &self.latched_bits {
            if bit < self.space.len() {
                self.space[bit] = 1.0;
            }
        }
        let step_number = self.next_step;
        self.next_step += 1;
        self.run_step(step_number, override_algorithm)
    }

    fn run_step(&mut self, step_number: i64, override_algorithm: Option<&str>) -> StepRecord {
        // B1: ISRE(n), before any machine reads its input.
        let isre = TrajectoryEntry::from_dense(step_number, &self.space);

        // B2: compose every mapped machine from ISRE(n), in parallel, and join.
        let space = &self.space;
        let mut jobs: Vec<&mut Machine> = self
            .machines
            .values_mut()
            .filter(|m| m.raw.get("perceptualMapping").is_some_and(Value::is_object))
            .collect();
        let workers = std::thread::available_parallelism()
            .map_or(1, usize::from)
            .min(jobs.len().max(1));
        let chunk = jobs.len().div_ceil(workers).max(1);
        let mut composed: Vec<Composed> = std::thread::scope(|scope| {
            let handles: Vec<_> = jobs
                .chunks_mut(chunk)
                .map(|machines| {
                    scope.spawn(move || {
                        machines
                            .iter_mut()
                            .map(|machine| {
                                let start = machine.input_offset;
                                let end = start + machine.input_length;
                                let snapshot = space.get(start..end).map_or_else(
                                    || vec![0.0; machine.input_length],
                                    <[f64]>::to_vec,
                                );
                                let transition = if machine.transitions_inhibited {
                                    Value::Null
                                } else {
                                    machine.transition(&snapshot, override_algorithm)
                                };
                                Composed {
                                    id: machine.id.clone(),
                                    snapshot,
                                    transition,
                                }
                            })
                            .collect::<Vec<_>>()
                    })
                })
                .collect();
            // The join: nothing below begins until every composer has completed.
            handles
                .into_iter()
                .flat_map(|h| h.join().expect("composer panicked"))
                .collect()
        });
        composed.sort_by(|a, b| a.id.cmp(&b.id));

        // Serial from here: coverage, the per-machine fold, governance, the batch.
        let mut step = StepRecord {
            step_number,
            timestamp: now_ms(),
            machine_results: BTreeMap::new(),
            machine_names: BTreeMap::new(),
            merge_batch: Vec::new(),
            event_bus: Vec::new(),
            perceptual_space: Vec::new(),
            active_regions: Vec::new(),
        };
        let mut firings: Vec<(String, Vec<String>, Vec<String>)> = Vec::new();
        for c in composed {
            let Some(machine) = self.machines.get(&c.id) else {
                continue;
            };
            let transition = if c.transition.is_null() {
                machine.inhibited_result(&c.snapshot)
            } else {
                c.transition
            };
            // Every composed machine is recorded, inhibited ones included, as
            // C++ records each result after the join.
            self.coverage
                .record(&machine.id, &machine.name, &transition);
            // The collection: one pending output per completed Reality Event, in
            // the sequence-id then event-id order the transition enumerates.
            let mut pending: Vec<(String, Vec<f64>, Vec<String>, String)> = Vec::new();
            if transition["arbiterMetadata"]["shouldOutput"].as_bool() == Some(true) {
                for (sequence_id, sr) in transition["sequenceResults"]
                    .as_object()
                    .into_iter()
                    .flatten()
                {
                    for output in sr["assertedOutputs"].as_array().into_iter().flatten() {
                        let values: Vec<f64> = output["vector"]
                            .as_array()
                            .into_iter()
                            .flatten()
                            .filter_map(Value::as_f64)
                            .collect();
                        let provenance = output["provenance"]
                            .as_array()
                            .into_iter()
                            .flatten()
                            .filter_map(|p| p.as_str().map(str::to_owned))
                            .collect();
                        let action = output["metadata"]["action"]
                            .as_str()
                            .unwrap_or_default()
                            .to_owned();
                        pending.push((sequence_id.clone(), values, provenance, action));
                    }
                }
            }
            let collection: Vec<&[f64]> = pending.iter().map(|p| p.1.as_slice()).collect();
            let merged = if pending.is_empty() {
                None
            } else {
                fold_outputs(
                    &collection,
                    &machine.output_merge,
                    machine.output_alphabet_top,
                )
            };
            let output_region = machine.raw["perceptualMapping"]["output"].clone();
            let out_offset = integer(output_region.get("offset"), 0);
            let out_length = integer(output_region.get("length"), 0);
            let pick = transition["machineOutput"]["vector"].clone();
            let presents = !pick.is_null() || merged.is_some();
            step.machine_results.insert(
                machine.id.clone(),
                json!({
                    "machineId": machine.id,
                    "machineName": machine.name,
                    "inputEvent": c.snapshot,
                    "outputVector": pick,
                    "mergedOutputVector": merged,
                    "outputMergeTransformation": machine.output_merge,
                    "inputRegion": { "offset": machine.input_offset, "length": machine.input_length },
                    "outputRegion": if presents { json!({ "offset": out_offset, "length": out_length }) } else { Value::Null },
                    "transitionResult": transition,
                }),
            );
            step.machine_names
                .insert(machine.id.clone(), machine.name.clone());
            step.active_regions.push((
                machine.input_offset,
                machine.input_length,
                machine.id.clone(),
                "input",
            ));
            if presents {
                step.active_regions
                    .push((out_offset, out_length, machine.id.clone(), "output"));
            }

            // Which CESs completed, recorded even when the fold refuses.
            let mut sequence_ids: Vec<String> = pending.iter().map(|p| p.0.clone()).collect();
            sequence_ids.sort();
            sequence_ids.dedup();
            let mut provenance: Vec<String> = Vec::new();
            for p in &pending {
                for id in &p.2 {
                    if !provenance.contains(id) {
                        provenance.push(id.clone());
                    }
                }
            }
            if !sequence_ids.is_empty() {
                firings.push((machine.id.clone(), sequence_ids.clone(), provenance.clone()));
            }
            let Some(values) = merged else { continue };

            // Governance: the join over contributors, highest severity, ties to
            // the smallest sequence id; the winner's decision travels whole.
            let mut governance: Option<Map<String, Value>> = None;
            let mut best = -1;
            for (sequence_id, values, _, action) in &pending {
                let Some(decision) = resolve_governance(machine, sequence_id, values, action)
                else {
                    continue;
                };
                let rank = severity_rank(decision["ragStatusCode"].as_str().unwrap_or(""));
                let current = governance
                    .as_ref()
                    .and_then(|g| g["sequenceId"].as_str())
                    .unwrap_or("");
                if rank > best || (rank == best && sequence_id.as_str() < current) {
                    best = rank;
                    governance = Some(decision);
                }
            }
            if let Some(g) = &governance {
                let text = |k: &str, d: &str| {
                    g[k].as_str()
                        .filter(|s| !s.is_empty())
                        .unwrap_or(d)
                        .to_owned()
                };
                self.coverage.record_paging(
                    text("ownerTeam", "unrouted"),
                    text("processStatus", "unknown"),
                    text("ragStatusCode", "unknown"),
                    machine.id.clone(),
                );
            }
            step.merge_batch.push(MergeOperation {
                offset: out_offset,
                length: out_length,
                machine_id: machine.id.clone(),
                machine_name: machine.name.clone(),
                sequence_ids,
                values,
                provenance,
                governance,
            });
        }
        // Canonical merge order: machineName, then region.offset.
        step.merge_batch.sort_by(|a, b| {
            a.machine_name
                .cmp(&b.machine_name)
                .then(a.offset.cmp(&b.offset))
        });

        // B3: gather, resolve, commit. A cell with one contestant is stored;
        // a contended cell is resolved under its declared rule.
        let dimension = self.space.len();
        let mut fan_in = vec![0u32; dimension];
        for op in &step.merge_batch {
            for i in 0..op.length.min(op.values.len()) {
                if let Some(n) = fan_in.get_mut(op.offset + i) {
                    *n += 1;
                }
            }
        }
        let mut osre_cells: Vec<(usize, f64)> = Vec::new();
        let mut contested: BTreeMap<usize, Vec<Contribution>> = BTreeMap::new();
        for op in &step.merge_batch {
            for i in 0..op.length.min(op.values.len()) {
                let cell = op.offset + i;
                let value = op.values[i];
                if cell >= dimension {
                    continue;
                }
                if fan_in[cell] == 1 {
                    self.space[cell] = value;
                    osre_cells.push((cell, value));
                } else {
                    contested.entry(cell).or_default().push(Contribution {
                        value,
                        provider: "machine",
                        origin_id: op.machine_id.clone(),
                        ces_id: op.sequence_ids.join(","),
                        rag_status_code: op
                            .governance
                            .as_ref()
                            .and_then(|g| g["ragStatusCode"].as_str())
                            .unwrap_or("")
                            .to_owned(),
                    });
                }
            }
        }
        for (cell, contributions) in &contested {
            let value = resolve_cell(contributions, self.arbitration.entries.get(cell));
            self.space[*cell] = value;
            osre_cells.push((*cell, value));
        }
        osre_cells.retain(|(_, v)| *v != 0.0);
        osre_cells.sort_by_key(|(i, _)| *i);
        let osre = TrajectoryEntry {
            step_number,
            length: dimension,
            non_zero: osre_cells,
        };

        // The event bus, from the firings: latch 1.0 at every subscriber's bit.
        let mut writes: Vec<(String, usize, String, String, Vec<String>)> = Vec::new();
        let mut seen = BTreeSet::new();
        for (producer, sequence_ids, provenance) in &firings {
            for sequence_id in sequence_ids {
                for (subscriber, bit) in self
                    .subscriptions
                    .get(&format!("{producer}|{sequence_id}"))
                    .into_iter()
                    .flatten()
                {
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
                        self.latched_bits.insert(*bit);
                    }
                }
            }
        }
        writes.sort_by(|a, b| (&a.0, a.1, &a.2, &a.3).cmp(&(&b.0, b.1, &b.2, &b.3)));
        for (subscriber, bit, producer, sequence_id, provenance) in writes {
            if bit < self.space.len() {
                self.space[bit] = 1.0;
            }
            step.event_bus.push(json!({
                "producerMachineId": producer,
                "producerSequenceId": sequence_id,
                "subscriberMachineId": subscriber,
                "bitOffset": bit,
                "value": 1.0,
                "provenance": provenance,
            }));
        }
        step.perceptual_space = self.space.clone();
        step.active_regions.sort();

        // B4: the pair is committed together.
        self.trajectories.record(isre, osre);
        self.completed_step = step_number;
        step
    }

    /// The step on the wire, as C++ `to_json(SimulationStep)` serialises it.
    pub(crate) fn step_json(
        step: &StepRecord,
        machine_results: bool,
        perceptual_space: bool,
        active_regions: bool,
        selector: &Selector,
    ) -> Value {
        let selected_ids: BTreeSet<&String> = step
            .machine_names
            .iter()
            .filter(|(_, name)| selector.machine_names.contains(*name))
            .map(|(id, _)| id)
            .collect();
        let keep_id = |id: &String| selected_ids.contains(id);
        let keep_merge = |op: &MergeOperation| {
            !selector.active
                || op
                    .sequence_ids
                    .iter()
                    .any(|s| selector.sequence_ids.contains(s))
                || keep_id(&op.machine_id)
        };
        let mut out = Map::new();
        out.insert("stepNumber".into(), json!(step.step_number));
        out.insert("timestamp".into(), json!(step.timestamp));
        if active_regions {
            let regions: Vec<Value> = step
                .active_regions
                .iter()
                .filter(|r| !selector.active || keep_id(&r.2))
                .map(|(offset, length, id, kind)| json!({ "offset": offset, "length": length, "machineId": id, "type": kind }))
                .collect();
            out.insert("activeRegions".into(), json!(regions));
        }
        let merge_batch: Vec<Value> = step
            .merge_batch
            .iter()
            .filter(|op| keep_merge(op))
            .map(|op| {
                let mut entry = json!({
                    "region": { "offset": op.offset, "length": op.length },
                    "machineId": op.machine_id,
                    "machineName": op.machine_name,
                    "sequenceIds": op.sequence_ids,
                    "values": op.values,
                    "provenance": op.provenance,
                });
                if let Some(g) = &op.governance {
                    entry["governance"] = Value::Object(g.clone());
                }
                entry
            })
            .collect();
        out.insert("mergeBatch".into(), json!(merge_batch));
        let event_bus: Vec<Value> = step
            .event_bus
            .iter()
            .filter(|w| {
                !selector.active
                    || w["producerSequenceId"]
                        .as_str()
                        .is_some_and(|s| selector.sequence_ids.contains(s))
                    || [&w["producerMachineId"], &w["subscriberMachineId"]]
                        .iter()
                        .any(|id| {
                            id.as_str()
                                .is_some_and(|id| selected_ids.iter().any(|s| s.as_str() == id))
                        })
            })
            .cloned()
            .collect();
        out.insert("eventBus".into(), json!(event_bus));
        if perceptual_space {
            out.insert("perceptualSpace".into(), json!(step.perceptual_space));
            out.insert("perceptualSpaceIsDebugProjection".into(), json!(true));
        }
        if machine_results {
            let results: Map<String, Value> = step
                .machine_results
                .iter()
                .filter(|(id, mr)| {
                    !selector.active
                        || mr["machineName"]
                            .as_str()
                            .is_some_and(|n| selector.machine_names.contains(n))
                        || keep_id(id)
                        || step.merge_batch.iter().any(|op| {
                            &op.machine_id == *id
                                && op
                                    .sequence_ids
                                    .iter()
                                    .any(|s| selector.sequence_ids.contains(s))
                        })
                })
                .map(|(id, mr)| (id.clone(), mr.clone()))
                .collect();
            out.insert("machineResults".into(), Value::Object(results));
        }
        Value::Object(out)
    }

    /// `GET /api/engine/active`: every active Reality Event, carrying no order.
    pub(crate) fn active_events(&self) -> Vec<Value> {
        let mut active = Vec::new();
        for machine in self.machines.values() {
            for sequence in machine.sequences.values() {
                for event in sequence.events.values().filter(|e| e.active) {
                    let elements: Vec<Value> = event
                        .elements
                        .iter()
                        .map(|e| {
                            let mut o = json!({ "value": e.value });
                            if !e.comparator.is_empty() {
                                o["comparatorType"] = json!(e.comparator);
                            }
                            if let Some(t) = e.threshold {
                                o["threshold"] = json!(t);
                            }
                            o
                        })
                        .collect();
                    active.push(json!({
                        "machineId": machine.id,
                        "sequenceId": sequence.id,
                        "vector": {
                            "elements": elements,
                            "id": event.id,
                            "isActive": true,
                            "isInitial": event.initial,
                            "matchAlgorithm": machine.match_algorithm,
                            "metadata": event.metadata,
                            "nextEventIds": event.next_ids,
                            "outputEvents": event.outputs.iter().map(|o| json!({ "id": o.id, "metadata": o.metadata, "vector": o.vector })).collect::<Vec<_>>(),
                            "state": "active",
                            "wasJustMatched": event.was_just_matched,
                        },
                    }));
                }
            }
        }
        active
    }

    /// `metadata.compose.subscriptions` of a machine: event-bus bits it reads.
    pub(crate) fn subscribe(&mut self, machine_id: &str, raw: &Value) {
        for sub in raw["metadata"]["compose"]["subscriptions"]
            .as_array()
            .into_iter()
            .flatten()
        {
            let (Some(pm), Some(ps), Some(bit)) = (
                sub["producerMachineId"].as_str().filter(|s| !s.is_empty()),
                sub["producerSequenceId"].as_str().filter(|s| !s.is_empty()),
                sub["bitOffset"].as_u64(),
            ) else {
                continue;
            };
            let bit = bit as usize;
            self.subscriptions
                .entry(format!("{pm}|{ps}"))
                .or_default()
                .push((machine_id.to_owned(), bit));
            if bit + 1 > self.dimension {
                self.dimension = bit + 1;
            }
        }
    }

    pub(crate) fn unsubscribe(&mut self, machine_id: &str) {
        self.subscriptions.retain(|_, subs| {
            subs.retain(|(s, _)| s != machine_id);
            !subs.is_empty()
        });
    }

    /// The step state a reset clears: the space, the histories, the latched
    /// bits and the step count. The completion point restarts with it.
    pub(crate) fn reset_steps(&mut self) {
        self.space.iter_mut().for_each(|v| *v = 0.0);
        self.trajectories.clear();
        self.latched_bits.clear();
        self.next_step = 0;
        self.completed_step = -1;
    }
}
