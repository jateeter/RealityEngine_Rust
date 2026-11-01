//! Dispatch & Triggers (#20 workstream 5): trigger dispatch on every push, the
//! dispatch ledger and records, replay, and `GET /api/triggers/status`.
//!
//! SURFACE_SPEC.md, "Dispatch surface shapes" and "Dispatch replay" (both
//! settled 2026-09-23, 3-of-3), is the contract; C++ `dispatch_triggers`,
//! `build_trigger_envelope`, `dispatch_semantics` and the record routes of
//! `perception_engine_server.cpp` are the concrete form.
//!
//! - **Trigger dispatch** runs on each push while `TRIGGERS_ENABLED` is on. A
//!   `mergeBatch` entry with governance, whose machine the catalog holds and
//!   binds to an agent and a trigger, becomes a `ces.terminal.event` envelope
//!   and a dispatch record (`status: "recorded"`, fire-and-record: no provider
//!   is called here). An entry without governance, one whose machine binds no
//!   agent, and one met before the catalog ever loaded are counted apart.
//! - **The machine catalog** is the RE's `GET /api/machines`, by id, read at
//!   boot, on every push, on bootstrap and through the machines proxy; a read
//!   that fails keeps the last one.
//! - **A record's `semantics`** links it to the corpus ABox
//!   (`abox-manifest.json`, `SEMANTICS_MANIFEST` or found above `MACHINES_DIR`).
//! - **The ledger** is a ring of `TRIGGER_DISPATCH_LEDGER_LIMIT` records
//!   (default 256), oldest first. PATCH changes delivery state only; replay
//!   re-emits a record's envelope as a new record and calls nothing.

use super::*;
use std::collections::VecDeque;

/// The RE's machines, by id, and when they were last read.
#[derive(Default)]
pub(crate) struct MachineCatalog {
    pub(crate) machines: BTreeMap<String, Value>,
    /// Epoch ms of the last successful read; 0 = never.
    pub(crate) refreshed_at: u128,
}

/// The dispatcher's configuration, ledger and counters.
pub(crate) struct Dispatch {
    pub(crate) enabled: bool,
    mode: String,
    graphql_endpoint: String,
    capacity: usize,
    pub(crate) records: BTreeMap<String, Value>,
    order: VecDeque<String>,
    envelopes_created: u64,
    replays_created: u64,
    dropped_no_governance: u64,
    dropped_no_dispatch: u64,
    dropped_catalog_cold: u64,
    /// machine name -> ABox base IRI, read once.
    semantics_bases: Option<BTreeMap<String, String>>,
}

impl Default for Dispatch {
    fn default() -> Self {
        Self {
            enabled: false,
            mode: "dry-run".to_owned(),
            graphql_endpoint: "http://localhost:4000/graphql".to_owned(),
            capacity: 256,
            records: BTreeMap::new(),
            order: VecDeque::new(),
            envelopes_created: 0,
            replays_created: 0,
            dropped_no_governance: 0,
            dropped_no_dispatch: 0,
            dropped_catalog_cold: 0,
            semantics_bases: None,
        }
    }
}

fn truthy(value: &str) -> bool {
    matches!(value, "1" | "true" | "TRUE" | "yes" | "YES")
}

pub(crate) fn mint(prefix: &str) -> String {
    format!(
        "{prefix}-{}",
        new_machine_id().trim_start_matches("machine-")
    )
}

fn nullable(value: &str) -> Value {
    if value.is_empty() {
        Value::Null
    } else {
        json!(value)
    }
}

fn strings(value: &Value) -> Vec<Value> {
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter(|v| v.is_string())
        .cloned()
        .collect()
}

/// `abox-manifest.json`: `SEMANTICS_MANIFEST`, else `semantics/` in
/// `MACHINES_DIR` or up to five of its parents.
fn manifest_bases() -> BTreeMap<String, String> {
    let explicit = std::env::var("SEMANTICS_MANIFEST")
        .ok()
        .filter(|p| !p.is_empty());
    let path = explicit.map(std::path::PathBuf::from).or_else(|| {
        let machines = std::env::var("MACHINES_DIR")
            .ok()
            .filter(|p| !p.is_empty())
            .unwrap_or_else(|| "../RealityEngine_Machines/machines".to_owned());
        let mut cursor = std::path::absolute(machines).ok()?;
        for _ in 0..6 {
            let candidate = cursor.join("semantics/abox-manifest.json");
            if candidate.exists() {
                return Some(candidate);
            }
            cursor = cursor.parent()?.to_path_buf();
        }
        None
    });
    let Some(manifest) = path
        .and_then(|p| std::fs::read_to_string(p).ok())
        .and_then(|text| serde_json::from_str::<Value>(&text).ok())
    else {
        return BTreeMap::new();
    };
    manifest["machines"]
        .as_object()
        .into_iter()
        .flatten()
        .filter_map(|(_, entry)| {
            let name = entry["name"].as_str().filter(|n| !n.is_empty())?;
            let (base, _) = entry["iri"].as_str()?.split_once('#')?;
            Some((name.to_owned(), base.to_owned()))
        })
        .collect()
}

impl Dispatch {
    /// `TRIGGERS_ENABLED`, `TRIGGER_DISPATCH_MODE`, `TRIGGER_GRAPHQL_URL`
    /// (default `<localAI base>/graphql`) and `TRIGGER_DISPATCH_LEDGER_LIMIT`.
    pub(crate) fn from_environment(localai_base_url: &str) -> Self {
        let env = |name: &str| std::env::var(name).ok();
        Self {
            enabled: env("TRIGGERS_ENABLED").is_some_and(|v| truthy(&v)),
            mode: env("TRIGGER_DISPATCH_MODE")
                .filter(|m| !m.is_empty())
                .unwrap_or_else(|| "dry-run".to_owned()),
            graphql_endpoint: env("TRIGGER_GRAPHQL_URL")
                .unwrap_or_else(|| format!("{localai_base_url}/graphql")),
            capacity: env("TRIGGER_DISPATCH_LEDGER_LIMIT")
                .and_then(|v| v.parse::<i64>().ok())
                .filter(|n| *n > 0)
                .map_or(256, |n| n as usize),
            ..Self::default()
        }
    }

    fn store(&mut self, record: Value) {
        let id = record["id"].as_str().unwrap_or_default().to_owned();
        self.order.push_back(id.clone());
        self.records.insert(id, record);
        while self.order.len() > self.capacity {
            if let Some(oldest) = self.order.pop_front() {
                self.records.remove(&oldest);
            }
        }
    }

    /// C++ `dispatch_semantics`: the record's link to the corpus ABox.
    fn semantics(
        &mut self,
        machine_name: &str,
        governance: &Value,
        sequence_ids: &[Value],
    ) -> Value {
        let bases = self.semantics_bases.get_or_insert_with(manifest_bases);
        let base = bases.get(machine_name).cloned().unwrap_or_default();
        let mut sequence = governance["sequenceId"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        if sequence.is_empty() && sequence_ids.len() == 1 {
            sequence = sequence_ids[0].as_str().unwrap_or_default().to_owned();
        }
        let local: String = sequence
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        let action = governance["actionCode"].as_str().unwrap_or_default();
        json!({
            "machineIri": if base.is_empty() { Value::Null } else { json!(format!("{base}#machine")) },
            "sequenceIri": if base.is_empty() || sequence.is_empty() {
                Value::Null
            } else {
                json!(format!("{base}#seq-{}", if local.is_empty() { "unnamed" } else { &local }))
            },
            "actionCode": nullable(action),
        })
    }
}

/// An agent binding (C++ `dispatch_binding_from_metadata`).
struct Binding {
    agent: String,
    trigger: String,
    action: String,
    autonomy_mode: String,
    actions: Vec<Value>,
    write_back: Value,
}

fn binding(metadata: &Value, values: &Value) -> Binding {
    let text = |v: &Value, key: &str| v[key].as_str().unwrap_or_default().to_owned();
    let agent_binding = &metadata["agentBinding"];
    let (agent, trigger, autonomy_mode, mut actions, write_back) = if agent_binding.is_object() {
        (
            agent_binding["agent"]
                .as_str()
                .map_or_else(|| text(metadata, "dispatchableAgent"), str::to_owned),
            agent_binding["trigger"]
                .as_str()
                .map_or_else(|| text(metadata, "aiTrigger"), str::to_owned),
            text(agent_binding, "mode"),
            strings(&agent_binding["allowedActions"]),
            agent_binding["writeBack"].clone(),
        )
    } else {
        (
            text(metadata, "dispatchableAgent"),
            text(metadata, "aiTrigger"),
            String::new(),
            Vec::new(),
            Value::Null,
        )
    };
    if actions.is_empty() {
        actions = strings(&metadata["agentActions"]);
    }
    // The action of the first asserted cell, else the first action.
    let asserted = values
        .as_array()
        .and_then(|vs| vs.iter().position(|v| v.as_f64().unwrap_or(0.0) != 0.0));
    let action = asserted
        .and_then(|i| actions.get(i))
        .or_else(|| actions.first())
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    Binding {
        agent,
        trigger,
        action,
        autonomy_mode,
        actions,
        write_back,
    }
}

/// `cell_i+cell_j` for the asserted cells, `none` when none is.
fn asserted_label(values: &Value) -> String {
    let Some(values) = values.as_array() else {
        return String::new();
    };
    let labels: Vec<String> = values
        .iter()
        .enumerate()
        .filter(|(_, v)| v.as_f64().unwrap_or(0.0) != 0.0)
        .map(|(i, _)| format!("cell_{i}"))
        .collect();
    if labels.is_empty() {
        "none".to_owned()
    } else {
        labels.join("+")
    }
}

impl PerceptionEngine {
    pub(crate) fn cache_catalog(&mut self, machines: &Value) {
        self.catalog.machines = machines["machines"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|m| {
                Some((
                    m["id"].as_str().filter(|id| !id.is_empty())?.to_owned(),
                    m.clone(),
                ))
            })
            .collect();
        self.catalog.refreshed_at = now_ms();
    }

    /// C++ `build_trigger_envelope`.
    fn envelope(
        &self,
        op: &Value,
        machine: &Value,
        envelope_id: &str,
        correlation_id: &str,
    ) -> Value {
        let metadata = &machine["metadata"];
        let trigger_config = &metadata["triggerConfig"];
        let binding = binding(metadata, &op["values"]);
        let values = &op["values"];
        let graphql = self.dispatch.mode == "graphql";
        let or_empty = |value: String| if graphql { value } else { String::new() };
        json!({
            "schemaVersion": "1.0.0",
            "envelopeType": "ces.terminal.event",
            "envelopeId": envelope_id,
            "correlationId": correlation_id,
            "emittedAtMs": now_ms(),
            "source": { "engine": "PE", "observedEngine": "RE", "endpoint": self.re_url },
            "ces": {
                "machineId": op["machineId"].as_str().unwrap_or_default(),
                "machineName": machine["name"].as_str().or_else(|| op["machineId"].as_str()).unwrap_or_default(),
                "machineCode": metadata["machineCode"].as_str().unwrap_or_default(),
                "sequenceIds": if op["sequenceIds"].is_array() { op["sequenceIds"].clone() } else { json!([]) },
                "stepNumber": 0,
                "perceptualMapping": { "output": op["region"] },
                "provenance": if op["provenance"].is_array() { op["provenance"].clone() } else { json!([]) },
                "deprecation": op["deprecation"],
            },
            "outputVector": {
                "values": if values.is_array() { values.clone() } else { json!([]) },
                "encoding": "vector",
                "semantics": values.as_array().into_iter().flatten().enumerate()
                    .map(|(i, _)| json!({ "index": i, "label": format!("cell_{i}") }))
                    .collect::<Vec<_>>(),
                "assertedLabel": asserted_label(values),
            },
            "projection": null,
            "governance": if op["governance"].is_object() { op["governance"].clone() } else { Value::Null },
            "dispatch": {
                "processId": trigger_config["processId"].as_str().unwrap_or_default(),
                "processName": trigger_config["processName"].as_str().unwrap_or_default(),
                "agent": binding.agent,
                "action": binding.action,
                "agentActionsCatalog": binding.actions,
                "trigger": binding.trigger,
                "autonomyMode": binding.autonomy_mode,
                "writeBack": binding.write_back,
                "endpoint": {
                    "kind": self.dispatch.mode,
                    "url": or_empty(trigger_config["endpoint"].as_str().unwrap_or(&self.dispatch.graphql_endpoint).to_owned()),
                    "mutation": or_empty(trigger_config["dispatch"]["mutation"].as_str().unwrap_or("updateProcessState").to_owned()),
                    "schemaRef": or_empty(
                        trigger_config["dispatch"]["schemaRef"]
                            .as_str()
                            .unwrap_or("localAIStack/services/api/routers/graphql_endpoint.py")
                            .to_owned(),
                    ),
                },
            },
        })
    }

    /// C++ `dispatch_triggers`: each merge entry of a step that governance and
    /// an agent binding make dispatchable becomes a record.
    pub(crate) fn dispatch_triggers(&mut self, step: &Value) {
        if !self.dispatch.enabled {
            return;
        }
        let cold = self.catalog.refreshed_at == 0;
        for op in step["mergeBatch"].as_array().into_iter().flatten() {
            if !op["governance"].is_object() {
                self.dispatch.dropped_no_governance += 1;
                continue;
            }
            let machine_id = op["machineId"].as_str().unwrap_or_default();
            let Some(machine) = self
                .catalog
                .machines
                .get(machine_id)
                .filter(|m| m.is_object())
                .cloned()
            else {
                if cold {
                    self.dispatch.dropped_catalog_cold += 1;
                } else {
                    self.dispatch.dropped_no_dispatch += 1;
                }
                continue;
            };
            let bound = binding(&machine["metadata"], &op["values"]);
            if bound.agent.is_empty() || bound.trigger.is_empty() {
                self.dispatch.dropped_no_dispatch += 1;
                continue;
            }
            let (envelope_id, correlation_id) =
                (mint("trigger-envelope"), mint("trigger-correlation"));
            let envelope = self.envelope(op, &machine, &envelope_id, &correlation_id);
            let sequence_ids = strings(&op["sequenceIds"]);
            let semantics = self.dispatch.semantics(
                machine["name"].as_str().unwrap_or_default(),
                &op["governance"],
                &sequence_ids,
            );
            let now = now_ms();
            let record = json!({
                "id": mint("dispatch"),
                "envelopeId": envelope_id,
                "correlationId": correlation_id,
                "status": "recorded",
                "mode": self.dispatch.mode,
                "target": bound.agent,
                "machineId": machine_id,
                "sequenceIds": sequence_ids,
                "ragStatusCode": nullable(op["governance"]["ragStatusCode"].as_str().unwrap_or_default()),
                "processStatus": nullable(op["governance"]["processStatus"].as_str().unwrap_or_default()),
                "attempts": 0,
                "createdAt": now,
                "updatedAt": now,
                "providerReceipt": null,
                "envelope": envelope,
                "error": null,
                "semantics": semantics,
                "replayOf": null,
            });
            self.dispatch.envelopes_created += 1;
            self.broadcast(&json!({
                "type": "trigger.envelope.created",
                "envelopeId": envelope_id,
                "correlationId": correlation_id,
                "dispatchId": record["id"],
                "target": bound.agent,
                "mode": self.dispatch.mode,
            }));
            self.dispatch.store(record);
        }
    }

    fn trigger_status(&self) -> Value {
        json!({
            "participation": if self.dispatch.enabled { "active" } else { "not-active" },
            "enabled": self.dispatch.enabled,
            "mode": self.dispatch.mode,
            "graphqlEndpoint": self.dispatch.graphql_endpoint,
            "records": self.dispatch.records.len(),
            "envelopesCreated": self.dispatch.envelopes_created,
            "droppedNoGovernance": self.dispatch.dropped_no_governance,
            "droppedNoDispatch": self.dispatch.dropped_no_dispatch,
            "droppedCatalogCold": self.dispatch.dropped_catalog_cold,
            "replaysCreated": self.dispatch.replays_created,
            // Nothing in this dispatcher can fail: it records, it calls nothing.
            "dispatchErrors": 0,
            "machineCatalogCold": self.catalog.refreshed_at == 0,
            "machineCatalogRefreshedAt": self.catalog.refreshed_at,
            "machineCatalogSize": self.catalog.machines.len(),
        })
    }

    pub(crate) fn dispatch_record(&self, id: &str) -> Option<Value> {
        self.dispatch.records.get(id).cloned()
    }

    /// `PATCH /api/dispatch/records/:id`: delivery state only.
    pub(crate) fn update_dispatch_record(
        &mut self,
        id: &str,
        body: &Value,
    ) -> Result<Value, EngineError> {
        if !body.is_object() {
            return Err(EngineError::bad_request(
                "dispatch update body must be a JSON object",
            ));
        }
        let record = self
            .dispatch
            .records
            .get_mut(id)
            .ok_or_else(|| EngineError::not_found("Dispatch record not found"))?;
        if let Some(status) = body["status"].as_str() {
            record["status"] = json!(status);
        }
        if let Some(error) = body["error"].as_str() {
            record["error"] = nullable(error);
        }
        if body["clearError"].as_bool().unwrap_or(false) {
            record["error"] = Value::Null;
        }
        if let Some(attempts) = body["attempts"].as_f64() {
            record["attempts"] = json!(attempts as i64);
        } else if body["incrementAttempts"].as_bool().unwrap_or(false) {
            record["attempts"] = json!(record["attempts"].as_i64().unwrap_or(0) + 1);
        }
        let mut receipt = if record["providerReceipt"].is_object() {
            record["providerReceipt"].clone()
        } else {
            json!({})
        };
        for (key, value) in body["providerReceipt"].as_object().into_iter().flatten() {
            receipt[key] = value.clone();
        }
        for key in ["provider", "adapter", "externalRunId"] {
            if let Some(value) = body[key].as_str() {
                receipt[key] = json!(value);
            }
        }
        if receipt.as_object().is_some_and(|r| !r.is_empty()) {
            record["providerReceipt"] = receipt;
        }
        record["updatedAt"] = json!(now_ms());
        let updated = record.clone();
        self.broadcast(&json!({
            "type": "dispatch.record.updated",
            "dispatchId": updated["id"],
            "status": updated["status"],
            "target": updated["target"],
            "attempts": updated["attempts"],
            "timestamp": updated["updatedAt"],
        }));
        Ok(updated)
    }

    fn replay(&mut self, id: &str, fresh_ids: bool) -> Option<Value> {
        let mut replay = self.dispatch.records.get(id)?.clone();
        let now = now_ms();
        replay["id"] = json!(mint("dispatch"));
        replay["status"] = json!("recorded");
        replay["mode"] = json!("replay");
        replay["attempts"] = json!(0);
        replay["providerReceipt"] = Value::Null;
        replay["error"] = Value::Null;
        replay["replayOf"] = json!(id);
        replay["createdAt"] = json!(now);
        replay["updatedAt"] = json!(now);
        if fresh_ids {
            let (envelope_id, correlation_id) =
                (mint("trigger-envelope"), mint("trigger-correlation"));
            replay["envelopeId"] = json!(envelope_id);
            replay["correlationId"] = json!(correlation_id);
            if replay["envelope"].is_object() {
                replay["envelope"]["envelopeId"] = json!(envelope_id);
                replay["envelope"]["correlationId"] = json!(correlation_id);
                replay["envelope"]["emittedAtMs"] = json!(now);
            }
        }
        self.dispatch.envelopes_created += 1;
        self.dispatch.replays_created += 1;
        self.dispatch.store(replay.clone());
        self.broadcast(&json!({
            "type": "trigger.envelope.created",
            "envelopeId": replay["envelopeId"],
            "correlationId": replay["correlationId"],
            "dispatchId": replay["id"],
            "target": replay["target"],
            "mode": "replay",
            "replayOf": id,
        }));
        Some(replay)
    }
}

/// Read the RE's machine list into the catalog; a failed read keeps the last
/// one (C++ `refresh_machine_catalog`).
pub(crate) async fn refresh_catalog(pe: &SharedPe) {
    let Ok(re_url) = pe.read().map(|state| state.re_url.clone()) else {
        return;
    };
    let Ok(bytes) = client::get(&format!("{re_url}/api/machines")).await else {
        return;
    };
    if let Ok(machines) = serde_json::from_slice::<Value>(&bytes)
        && let Ok(mut state) = pe.write()
    {
        state.cache_catalog(&machines);
    }
}

type Answer = Result<Json<Value>, (StatusCode, Json<Value>)>;

pub(crate) async fn triggers(State(pe): State<SharedPe>) -> Answer {
    Ok(Json(read(&pe)?.trigger_status()))
}

pub(crate) async fn ledger(State(pe): State<SharedPe>) -> Answer {
    let state = read(&pe)?;
    let records: Vec<&Value> = state
        .dispatch
        .order
        .iter()
        .filter_map(|id| state.dispatch.records.get(id))
        .collect();
    Ok(Json(
        json!({ "enabled": state.dispatch.enabled, "mode": state.dispatch.mode, "records": records }),
    ))
}

fn not_found() -> (StatusCode, Json<Value>) {
    error_response(EngineError::not_found("Dispatch record not found"))
}

pub(crate) async fn record(State(pe): State<SharedPe>, Path(id): Path<String>) -> Answer {
    let record = read(&pe)?.dispatch_record(&id).ok_or_else(not_found)?;
    Ok(Json(json!({ "record": record })))
}

pub(crate) async fn update(
    State(pe): State<SharedPe>,
    Path(id): Path<String>,
    body: Bytes,
) -> Answer {
    let body = body_of(&body)?;
    let record = write(&pe)?
        .update_dispatch_record(&id, &body)
        .map_err(error_response)?;
    Ok(Json(json!({ "success": true, "record": record })))
}

pub(crate) async fn replay(
    State(pe): State<SharedPe>,
    Path(id): Path<String>,
    body: Bytes,
) -> Answer {
    let body = body_of(&body)?;
    let fresh_ids = body["freshIds"].as_bool().unwrap_or(false);
    let record = write(&pe)?.replay(&id, fresh_ids).ok_or_else(not_found)?;
    Ok(Json(
        json!({ "success": true, "record": record, "replayOf": id, "freshIds": fresh_ids }),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use tower::ServiceExt;

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

    /// A governed merge entry of a bound machine becomes a record; one without
    /// governance, one of an unbound machine and one of an unknown machine are
    /// counted apart. PATCH changes delivery state only; replay re-emits.
    #[tokio::test]
    async fn trigger_dispatch_records_patches_and_replays() {
        let mut engine = PerceptionEngine::new(4);
        engine.dispatch.enabled = true;
        engine.cache_catalog(&json!({ "machines": [
            { "id": "m-bound", "name": "Bound", "metadata": {
                "agentBinding": { "agent": "nurse", "trigger": "t", "allowedActions": ["watch", "page"] } } },
            { "id": "m-unbound", "name": "Unbound", "metadata": {} },
        ] }));
        let governance = json!({ "ragStatusCode": "RED", "processStatus": "", "sequenceId": "s 1", "actionCode": "page" });
        engine.dispatch_triggers(&json!({ "mergeBatch": [
            { "machineId": "m-bound", "governance": governance, "values": [0, 1], "sequenceIds": ["s 1"], "region": { "offset": 0, "length": 2 } },
            { "machineId": "m-bound", "values": [1] },
            { "machineId": "m-unbound", "governance": governance },
            { "machineId": "m-gone", "governance": governance },
        ] }));
        let app = pe_app(engine);
        let (_, status) = send(&app, "GET", "/api/triggers/status", None).await;
        assert_eq!(
            (
                &status["participation"],
                &status["envelopesCreated"],
                &status["droppedNoGovernance"],
                &status["droppedNoDispatch"]
            ),
            (&json!("active"), &json!(1), &json!(1), &json!(2))
        );
        let (_, ledger) = send(&app, "GET", "/api/dispatch/ledger", None).await;
        let record = ledger["records"][0].clone();
        assert_eq!(
            (
                &record["target"],
                &record["ragStatusCode"],
                &record["processStatus"],
                &record["envelope"]["dispatch"]["action"]
            ),
            (&json!("nurse"), &json!("RED"), &Value::Null, &json!("page"))
        );
        assert_eq!(
            record["envelope"]["outputVector"]["assertedLabel"],
            json!("cell_1")
        );
        let id = record["id"].as_str().unwrap().to_owned();

        let (_, patched) = send(
            &app,
            "PATCH",
            &format!("/api/dispatch/records/{id}"),
            Some(json!({ "status": "delivered", "incrementAttempts": true, "provider": "ollama", "envelope": {} })),
        )
        .await;
        let patched = &patched["record"];
        assert_eq!(
            (
                &patched["status"],
                &patched["attempts"],
                &patched["providerReceipt"],
                &patched["envelope"]
            ),
            (
                &json!("delivered"),
                &json!(1),
                &json!({ "provider": "ollama" }),
                &record["envelope"]
            )
        );
        let (_, replayed) = send(
            &app,
            "POST",
            &format!("/api/dispatch/records/{id}/replay"),
            None,
        )
        .await;
        let replay = &replayed["record"];
        assert_eq!(
            (
                &replay["mode"],
                &replay["replayOf"],
                &replay["attempts"],
                &replay["providerReceipt"],
                &replay["envelopeId"]
            ),
            (
                &json!("replay"),
                &json!(id),
                &json!(0),
                &Value::Null,
                &record["envelopeId"]
            )
        );
        let (_, replayed) = send(
            &app,
            "POST",
            &format!("/api/dispatch/records/{id}/replay"),
            Some(json!({ "freshIds": true })),
        )
        .await;
        assert_ne!(replayed["record"]["envelopeId"], record["envelopeId"]);
        assert_eq!(
            replayed["record"]["envelope"]["envelopeId"],
            replayed["record"]["envelopeId"]
        );
        let (status, body) = send(&app, "GET", "/api/dispatch/records/none", None).await;
        assert_eq!(
            (status, body),
            (
                StatusCode::NOT_FOUND,
                json!({ "error": "Dispatch record not found" })
            )
        );
        let (_, status) = send(&app, "GET", "/api/triggers/status", None).await;
        assert_eq!(
            (&status["replaysCreated"], &status["records"]),
            (&json!(2), &json!(3))
        );
    }
}
