//! How a machine is shown: `GET /api/machines` (list form), `GET
//! /api/machines/:id` (full form), `GET /api/machines/:id/export` (the corpus
//! document), `GET /api/machine-graph` and the semantic buses (#13, slice 1).
//!
//! C++ `Machine::to_json`, `corpus_document` and `machine_graph_data` are the
//! concrete form this follows; SURFACE_SPEC.md governs where it speaks.
//!
//! - Machines are listed in canonical order: `metadata.domain`, then name, then
//!   id (C++ `machine_order_less`). A machine's sequences are in (name, id)
//!   order, and `sequenceIds` follows them.
//! - The list form summarises each sequence as `{id, name, initialEventIds}`
//!   (SURFACE_SPEC.md, "`GET /api/machines` — the sequence summary"); the full
//!   form carries every Reality Event as it is running.
//! - A machine's declared `inputSequences` travel in `metadata` on the runtime
//!   views, as C++ stashes them at load, and move back to the top level, with
//!   `arbiterRule` upper-cased, in the export.

use super::*;
use std::path::PathBuf;

impl Event {
    /// C++ `RealityEvent::to_json`: the event as it is running.
    pub(crate) fn to_json(&self, match_algorithm: &str) -> Value {
        let elements: Vec<Value> = self
            .elements
            .iter()
            .map(|element| {
                let mut value = json!({ "value": element.value });
                if !element.comparator.is_empty() {
                    value["comparatorType"] = json!(element.comparator);
                }
                if let Some(threshold) = element.threshold {
                    value["threshold"] = json!(threshold);
                }
                value
            })
            .collect();
        let outputs: Vec<Value> = self
            .outputs
            .iter()
            .map(|output| {
                json!({
                    "id": output.id,
                    "vector": output.vector,
                    "metadata": output.metadata,
                    "timestamp": output.timestamp,
                    "provenance": [],
                })
            })
            .collect();
        json!({
            "id": self.id,
            "matchAlgorithm": match_algorithm,
            "elements": elements,
            "state": if self.active { "active" } else { "inactive" },
            "isActive": self.active,
            "nextEventIds": self.next_ids,
            "outputEvents": outputs,
            "isInitial": self.initial,
            "wasJustMatched": self.was_just_matched,
            "metadata": self.metadata,
        })
    }
}

impl Sequence {
    fn initial_event_ids(&self) -> Vec<&String> {
        self.events
            .values()
            .filter(|event| event.initial)
            .map(|event| &event.id)
            .collect()
    }

    /// C++ `CriticalEventSequence::to_json`, events in id order.
    pub(crate) fn to_json(&self, match_algorithm: &str) -> Value {
        let mut out = json!({
            "id": self.id,
            "name": self.name,
            "events": self.events.values().map(|e| e.to_json(match_algorithm)).collect::<Vec<_>>(),
            "initialEventIds": self.initial_event_ids(),
            "outputEventIds": self
                .events
                .values()
                .filter(|event| !event.outputs.is_empty())
                .map(|event| &event.id)
                .collect::<Vec<_>>(),
            "metadata": self.metadata,
        });
        for (key, value) in [
            ("schemaVersion", &self.schema_version),
            ("deprecatedAt", &self.deprecated_at),
            ("replacedBy", &self.replaced_by),
        ] {
            if !value.is_empty() {
                out[key] = json!(value);
            }
        }
        out
    }
}

impl Machine {
    fn domain(&self) -> &str {
        self.raw["metadata"]["domain"].as_str().unwrap_or("")
    }

    /// Sequences in canonical order: by name, then id.
    fn sequences_in_order(&self) -> Vec<&Sequence> {
        let mut sequences: Vec<&Sequence> = self.sequences.values().collect();
        sequences.sort_by(|a, b| (&a.name, &a.id).cmp(&(&b.name, &b.id)));
        sequences
    }

    /// The machine's `metadata`, with its declared `inputSequences` stashed in
    /// it, as C++ holds it at load.
    fn metadata(&self) -> Value {
        let mut metadata = match self.raw.get("metadata") {
            Some(Value::Object(object)) => Value::Object(object.clone()),
            _ => json!({}),
        };
        if let Some(sequences) = self.raw.get("inputSequences").filter(|s| s.is_array()) {
            metadata["inputSequences"] = sequences.clone();
        }
        metadata
    }

    /// `{input, output}` with `bitsPerElement` and `outputAlphabetTop` only
    /// when declared and valid; `null` with no mapping.
    fn mapping(&self) -> Value {
        let Some(mapping) = self.raw.get("perceptualMapping").filter(|m| m.is_object()) else {
            return Value::Null;
        };
        let (input_offset, input_length) = region(mapping.get("input"));
        let (output_offset, output_length) = region(mapping.get("output"));
        let mut out = json!({
            "input": { "offset": input_offset, "length": input_length },
            "output": { "offset": output_offset, "length": output_length },
        });
        if let Some(bits) = mapping["bitsPerElement"]
            .as_u64()
            .filter(|bits| [1, 2, 4, 8].contains(bits))
        {
            out["bitsPerElement"] = json!(bits);
        }
        if let Some(top) = self.output_alphabet_top.filter(|top| *top >= 1) {
            out["outputAlphabetTop"] = json!(top);
        }
        out
    }

    /// C++ `Machine::to_json(full)`.
    pub(crate) fn to_json(&self, full: bool) -> Value {
        let sequences = self.sequences_in_order();
        json!({
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "matchAlgorithm": self.match_algorithm,
            "outputMergeTransformation": self.output_merge,
            // The merge interlock starts locked; this engine has no route that
            // unlocks it.
            "outputMergeLocked": true,
            "arbiterRule": self.arbiter_rule,
            "sequenceCount": sequences.len(),
            "totalEvents": sequences.iter().map(|s| s.events.len()).sum::<usize>(),
            "sequenceIds": sequences.iter().map(|s| &s.id).collect::<Vec<_>>(),
            "sequences": sequences
                .iter()
                .map(|s| {
                    if full {
                        s.to_json(&self.match_algorithm)
                    } else {
                        json!({ "id": s.id, "name": s.name, "initialEventIds": s.initial_event_ids() })
                    }
                })
                .collect::<Vec<_>>(),
            "metadata": self.metadata(),
            "perceptualMapping": self.mapping(),
        })
    }

    /// C++ `corpus_document`: the full form, as the corpus declares a machine —
    /// `arbiterRule` upper-cased and `inputSequences` back at the top level.
    fn corpus_document(&self) -> Value {
        let mut document = self.to_json(true);
        document["arbiterRule"] = json!(self.arbiter_rule.to_uppercase());
        if let Some(sequences) = document["metadata"]
            .as_object_mut()
            .and_then(|metadata| metadata.remove("inputSequences"))
        {
            document["inputSequences"] = sequences;
        }
        document
    }
}

fn region(mapping: Option<&Value>) -> (usize, usize) {
    (
        integer(mapping.and_then(|r| r.get("offset")), 0),
        integer(mapping.and_then(|r| r.get("length")), 0),
    )
}

impl Engine {
    pub(crate) fn machines_in_canonical_order(&self) -> Vec<&Machine> {
        let mut machines: Vec<&Machine> = self.machines.values().collect();
        machines.sort_by(|a, b| (a.domain(), &a.name, &a.id).cmp(&(b.domain(), &b.name, &b.id)));
        machines
    }

    /// C++ `machine_graph_data`: every mapped machine as a node, and an edge
    /// wherever one machine's output region overlaps another's input region,
    /// both in canonical order.
    fn machine_graph(&self) -> Value {
        let mapped: Vec<&Machine> = self
            .machines_in_canonical_order()
            .into_iter()
            .filter(|m| !m.mapping().is_null())
            .collect();
        let nodes: Vec<Value> = mapped
            .iter()
            .map(|m| {
                let mapping = m.mapping();
                json!({
                    "id": m.id,
                    "name": m.name,
                    "description": m.description,
                    "inputMapping": mapping["input"],
                    "outputMapping": mapping["output"],
                    "metadata": m.metadata(),
                })
            })
            .collect();
        let mut edges = Vec::new();
        for source in &mapped {
            let output = source.mapping()["output"].clone();
            let (out_offset, out_length) = region(Some(&output));
            for target in &mapped {
                if source.id == target.id {
                    continue;
                }
                let input = target.mapping()["input"].clone();
                let (in_offset, in_length) = region(Some(&input));
                if out_offset + out_length > in_offset && out_offset < in_offset + in_length {
                    edges.push(json!({
                        "source": source.id,
                        "target": target.id,
                        "sourceRegion": output,
                        "targetRegion": input,
                        "overlap": true,
                    }));
                }
            }
        }
        json!({
            "nodes": nodes,
            "edges": edges,
            "perceptualSpaceDimension": self.space_width(),
        })
    }

    /// `SEMANTIC_BUS_REGISTRY`, else `domains/semantic-bus-registry.json` in the
    /// corpus directory or up to five directories above it.
    fn semantic_bus_registry_path(&self) -> PathBuf {
        if let Some(path) = std::env::var_os("SEMANTIC_BUS_REGISTRY").filter(|p| !p.is_empty()) {
            return path.into();
        }
        let base = self
            .machines_dir
            .as_deref()
            .and_then(|dir| std::path::absolute(dir).ok())
            .unwrap_or_default();
        let mut cursor = Some(base.as_path());
        for _ in 0..6 {
            let Some(dir) = cursor.filter(|d| !d.as_os_str().is_empty()) else {
                break;
            };
            let candidate = dir.join("domains/semantic-bus-registry.json");
            if candidate.exists() {
                return candidate;
            }
            cursor = dir.parent();
        }
        base.parent()
            .unwrap_or(&base)
            .join("domains/semantic-bus-registry.json")
    }

    fn semantic_bus_registry(&self) -> Result<Value, EngineError> {
        let path = self.semantic_bus_registry_path();
        let unavailable = |why: String| {
            EngineError::not_found(format!("semantic bus registry unavailable: {why}"))
        };
        let text = fs::read_to_string(&path)
            .map_err(|_| unavailable(format!("cannot open {}", path.display())))?;
        let registry: Value =
            serde_json::from_str(&text).map_err(|error| unavailable(error.to_string()))?;
        if !registry["semanticBuses"].is_array() {
            return Err(unavailable(format!(
                "invalid registry shape at {}",
                path.display()
            )));
        }
        Ok(registry)
    }
}

type Answer = Result<Json<Value>, (StatusCode, Json<Value>)>;

pub(crate) async fn export(State(engine): State<SharedEngine>, Path(id): Path<String>) -> Answer {
    let state = engine.read().map_err(|_| error_response(lock_error()))?;
    let machine = state
        .machines
        .get(&id)
        .ok_or_else(|| error_response(EngineError::not_found("Machine not found")))?;
    Ok(Json(
        json!({ "version": "1.0.0", "machine": machine.corpus_document() }),
    ))
}

pub(crate) async fn machine_graph(State(engine): State<SharedEngine>) -> Answer {
    let state = engine.read().map_err(|_| error_response(lock_error()))?;
    Ok(Json(state.machine_graph()))
}

pub(crate) async fn semantic_buses(State(engine): State<SharedEngine>) -> Answer {
    let state = engine.read().map_err(|_| error_response(lock_error()))?;
    state
        .semantic_bus_registry()
        .map(Json)
        .map_err(error_response)
}

pub(crate) async fn semantic_bus(
    State(engine): State<SharedEngine>,
    Path(id): Path<String>,
) -> Answer {
    let state = engine.read().map_err(|_| error_response(lock_error()))?;
    let registry = state.semantic_bus_registry().map_err(error_response)?;
    registry["semanticBuses"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|bus| bus["id"].as_str() == Some(id.as_str()))
        .map(|bus| Json(json!({ "bus": bus })))
        .ok_or_else(|| error_response(EngineError::not_found("Semantic bus not found")))
}
