//! The machine family (#13, slice 2): `PUT` / `PATCH /api/machines/:id`, the
//! universal-space and what-if drives, `json/import`, and checkpoints.
//!
//! C++'s routes in `reality_engine_server.cpp` are the concrete form this
//! follows.
//!
//! - `PUT` replaces the machine at `:id` with the body, keeping the id; `PATCH`
//!   merges `name`, `description` and `metadata` into the declaration and
//!   reloads it. Both answer `{success, machine}` in full form, and both reload
//!   the machine from its declaration, so its Reality Events start over.
//! - `process-universal` slices a universal input space at the machine's input
//!   region and drives the machine, as `/process` does; `process-universal/all`
//!   drives every mapped machine. The what-if routes drive an uninhibited copy
//!   and persist nothing.
//! - `json/import` ingests a definition under a minted id.
//! - A checkpoint is a copy of the machine as it is running; restoring it
//!   replaces the resident machine with that copy.

use super::*;

/// A machine as it was running when the checkpoint was taken.
#[derive(Clone)]
pub(crate) struct Checkpoint {
    label: String,
    timestamp: u128,
    machine: Machine,
}

fn not_found() -> EngineError {
    EngineError::not_found("Machine not found")
}

fn body_of(bytes: &Bytes) -> Result<Value, EngineError> {
    if bytes.is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_slice(bytes)
        .map_err(|error| EngineError::bad_request(format!("invalid JSON body: {error}")))
}

/// The numbers of `value`, a non-number read as 0 (C++ `json::to_numbers`).
fn numbers(value: &Value) -> Vec<f64> {
    value
        .as_array()
        .into_iter()
        .flatten()
        .map(|v| v.as_f64().unwrap_or(0.0))
        .collect()
}

impl Machine {
    /// The machine's input region, `None` without a perceptual mapping.
    fn input_region(&self) -> Option<(usize, usize)> {
        let input = self
            .raw
            .get("perceptualMapping")
            .filter(|m| m.is_object())?
            .get("input");
        Some((
            integer(input.and_then(|r| r.get("offset")), 0),
            integer(input.and_then(|r| r.get("length")), 0),
        ))
    }

    /// Its slice of a universal input space; cells past the space read 0.
    fn resolve(&self, universal: &[f64]) -> Result<Vec<f64>, EngineError> {
        let (offset, length) = self
            .input_region()
            .ok_or_else(|| EngineError::bad_request("Machine has no perceptual mapping"))?;
        Ok((offset..offset + length)
            .map(|i| universal.get(i).copied().unwrap_or(0.0))
            .collect())
    }

    /// What `input` would do, asked of an uninhibited copy: nothing persists.
    fn whatif(&self, input: &[f64]) -> Value {
        let mut copy = self.clone();
        copy.transitions_inhibited = false;
        let mut result = copy.respond(input, None);
        if let Some(object) = result.as_object_mut() {
            object.remove("machineId");
        }
        result
    }
}

impl Engine {
    fn resident(&self, id: &str) -> Result<&Machine, EngineError> {
        self.machines.get(id).ok_or_else(not_found)
    }

    /// Replaces the machine at `id` with `machine`, keeping its control state.
    fn readmit(&mut self, id: &str, mut machine: Machine) -> Value {
        machine.id = id.to_owned();
        if let Some(previous) = self.machines.remove(id) {
            machine.transitions_inhibited = previous.transitions_inhibited;
        }
        self.admit(machine);
        self.machines[id].to_json(true)
    }

    fn replace_machine(&mut self, id: &str, body: Value) -> Result<Value, EngineError> {
        let machine = Machine::from_json(body)?;
        Ok(json!({ "success": true, "machine": self.readmit(id, machine) }))
    }

    fn patch_machine(&mut self, id: &str, body: &Value) -> Result<Value, EngineError> {
        let mut declaration = self.resident(id)?.raw.clone();
        for key in ["name", "description"] {
            if let Some(text) = body[key].as_str() {
                declaration[key] = json!(text);
            }
        }
        if let Some(changes) = body["metadata"].as_object() {
            if !declaration["metadata"].is_object() {
                declaration["metadata"] = json!({});
            }
            for (key, value) in changes {
                declaration["metadata"][key] = value.clone();
            }
        }
        let machine = Machine::from_json(declaration)?;
        Ok(json!({ "success": true, "machine": self.readmit(id, machine) }))
    }

    fn import_machine(&mut self, body: Value) -> Result<Value, EngineError> {
        let mut machine = Machine::from_json(body)?;
        machine.id = new_machine_id();
        let id = machine.id.clone();
        self.admit(machine);
        Ok(json!({ "success": true, "machine": self.machines[&id].to_json(true) }))
    }

    fn process_universal_all(&mut self, universal: &[f64]) -> Result<Value, EngineError> {
        let mut results = serde_json::Map::new();
        let mapped: Vec<String> = self
            .machines
            .values()
            .filter(|m| m.input_region().is_some())
            .map(|m| m.id.clone())
            .collect();
        for id in mapped {
            let input = self.machines[&id].resolve(universal)?;
            results.insert(id.clone(), self.process_machine(&id, &input)?);
        }
        Ok(json!({ "results": results }))
    }

    fn checkpoint(&mut self, id: &str, label: &str) -> Result<Value, EngineError> {
        let machine = self.resident(id)?.clone();
        let checkpoint_id = format!(
            "checkpoint-{}",
            new_machine_id().trim_start_matches("machine-")
        );
        self.checkpoints.entry(id.to_owned()).or_default().insert(
            checkpoint_id.clone(),
            Checkpoint {
                label: label.to_owned(),
                timestamp: now_ms(),
                machine,
            },
        );
        Ok(json!({ "success": true, "checkpointId": checkpoint_id }))
    }

    fn restore(&mut self, id: &str, checkpoint: &str) -> Result<Value, EngineError> {
        let machine = self
            .checkpoints
            .get(id)
            .and_then(|by_id| by_id.get(checkpoint))
            .map(|cp| cp.machine.clone())
            .ok_or_else(|| EngineError::not_found("Checkpoint not found"))?;
        self.machines.remove(id);
        self.admit(machine);
        Ok(json!({ "success": true }))
    }
}

type Answer = Result<Json<Value>, (StatusCode, Json<Value>)>;

fn write<T>(
    engine: &SharedEngine,
    f: impl FnOnce(&mut Engine) -> Result<T, EngineError>,
) -> Result<T, (StatusCode, Json<Value>)> {
    let mut state = engine.write().map_err(|_| error_response(lock_error()))?;
    f(&mut state).map_err(error_response)
}

pub(crate) async fn replace(
    State(engine): State<SharedEngine>,
    Path(id): Path<String>,
    body: Bytes,
) -> Answer {
    let body = body_of(&body).map_err(error_response)?;
    write(&engine, |e| e.replace_machine(&id, body)).map(Json)
}

pub(crate) async fn patch(
    State(engine): State<SharedEngine>,
    Path(id): Path<String>,
    body: Bytes,
) -> Answer {
    let body = body_of(&body).map_err(error_response)?;
    write(&engine, |e| e.patch_machine(&id, &body)).map(Json)
}

pub(crate) async fn process_universal(
    State(engine): State<SharedEngine>,
    Path(id): Path<String>,
    body: Bytes,
) -> Answer {
    let body = body_of(&body).map_err(error_response)?;
    let universal = numbers(&body["universalInputSpace"]);
    write(&engine, |e| {
        let input = e.resident(&id)?.resolve(&universal)?;
        e.process_machine(&id, &input)
    })
    .map(Json)
}

pub(crate) async fn process_universal_all(
    State(engine): State<SharedEngine>,
    body: Bytes,
) -> Answer {
    let body = body_of(&body).map_err(error_response)?;
    let universal = numbers(&body["universalInputSpace"]);
    write(&engine, |e| e.process_universal_all(&universal)).map(Json)
}

pub(crate) async fn whatif(
    State(engine): State<SharedEngine>,
    Path(id): Path<String>,
    body: Bytes,
) -> Answer {
    let body = body_of(&body).map_err(error_response)?;
    let input = numbers(&body["inputEvent"]);
    let state = engine.read().map_err(|_| error_response(lock_error()))?;
    let machine = state.resident(&id).map_err(error_response)?;
    Ok(Json(machine.whatif(&input)))
}

pub(crate) async fn whatif_universal(
    State(engine): State<SharedEngine>,
    Path(id): Path<String>,
    body: Bytes,
) -> Answer {
    let body = body_of(&body).map_err(error_response)?;
    let universal = numbers(&body["universalInputSpace"]);
    let state = engine.read().map_err(|_| error_response(lock_error()))?;
    let machine = state.resident(&id).map_err(error_response)?;
    let input = machine.resolve(&universal).map_err(error_response)?;
    Ok(Json(machine.whatif(&input)))
}

pub(crate) async fn import(State(engine): State<SharedEngine>, body: Bytes) -> Answer {
    let body = body_of(&body).map_err(error_response)?;
    write(&engine, |e| e.import_machine(body)).map(Json)
}

/// A bare array, as C++ answers: `{id, label, timestamp}` in id order.
pub(crate) async fn checkpoints(
    State(engine): State<SharedEngine>,
    Path(id): Path<String>,
) -> Answer {
    let state = engine.read().map_err(|_| error_response(lock_error()))?;
    let list: Vec<Value> = state
        .checkpoints
        .get(&id)
        .into_iter()
        .flatten()
        .map(|(cp_id, cp)| json!({ "id": cp_id, "label": cp.label, "timestamp": cp.timestamp }))
        .collect();
    Ok(Json(json!(list)))
}

pub(crate) async fn checkpoint(
    State(engine): State<SharedEngine>,
    Path(id): Path<String>,
    body: Bytes,
) -> Answer {
    let body = body_of(&body).map_err(error_response)?;
    let label = body["label"].as_str().unwrap_or("").to_owned();
    write(&engine, |e| e.checkpoint(&id, &label)).map(Json)
}

pub(crate) async fn restore(
    State(engine): State<SharedEngine>,
    Path((id, checkpoint)): Path<(String, String)>,
) -> Answer {
    write(&engine, |e| e.restore(&id, &checkpoint)).map(Json)
}

pub(crate) async fn delete_checkpoint(
    State(engine): State<SharedEngine>,
    Path((id, checkpoint)): Path<(String, String)>,
) -> Answer {
    write(&engine, |e| {
        let removed = e
            .checkpoints
            .get_mut(&id)
            .is_some_and(|by_id| by_id.remove(&checkpoint).is_some());
        Ok(json!({ "success": removed }))
    })
    .map(Json)
}
