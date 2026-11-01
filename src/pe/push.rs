//! The push cycle (#20 workstream 3): `POST /api/push`, `GET /api/push/:id`
//! and auto push.
//!
//! C++ `do_push` / `execute_push` / `push_worker_loop` are the concrete form
//! this follows; SURFACE_SPEC.md, "`POST /api/push` response shape", is the
//! contract.
//!
//! - A push assembles the universal Reality Event, records its contention,
//!   sends it to `POST {re}/api/perceive` with every observation field asked
//!   for, folds the gated machine outputs into the perceptual space the RE
//!   returns, and advances the sources. What the caller asked to be *shown*
//!   (`compact`, `includeMachineResults`, `only`) trims the reply afterwards
//!   and never changes what the engine computed (RealityEngine_CI#367).
//! - One push at a time. A push arriving while one is in flight answers 409
//!   `coalesced` and is owed one more push, run when the in-flight one ends.
//! - `async: true` answers 202 with a job id; `GET /api/push/:id` reports the
//!   job, its result once it has one. The newest 256 jobs are kept.
//! - A push the RE refuses answers 200 with `success: false` and the error,
//!   as C++ does: the push happened, the step did not.
//! - The machine catalog (`GET {re}/api/machines`) is read on every push for
//!   the OSRE fold: each mergeBatch cell's writing machine and its
//!   `outputMergeTransformation`.

use super::*;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};

const PUSH_RECORDS: usize = 256;

struct Job {
    status: &'static str,
    created_at: u128,
    updated_at: u128,
    result: Option<Value>,
}

/// The push queue's state: one in flight, the owed pushes, the job records,
/// and the auto-push loop.
#[derive(Default)]
pub(crate) struct Pushes {
    /// (a push in flight, the pushes owed to callers turned away meanwhile),
    /// under one lock as C++ keeps them under `pushQueueMutex`.
    queue: std::sync::Mutex<(bool, u64)>,
    jobs: std::sync::Mutex<(BTreeMap<String, Job>, VecDeque<String>)>,
    /// Bumped by every start and stop; a loop from an older start exits.
    auto_generation: AtomicU64,
}

impl Pushes {
    fn record(&self, id: &str, status: &'static str, result: Option<Value>) {
        let Ok(mut jobs) = self.jobs.lock() else {
            return;
        };
        let (records, order) = &mut *jobs;
        let now = now_ms();
        match records.get_mut(id) {
            Some(job) => {
                job.status = status;
                job.updated_at = now;
                job.result = result;
            }
            None => {
                records.insert(
                    id.to_owned(),
                    Job {
                        status,
                        created_at: now,
                        updated_at: now,
                        result,
                    },
                );
                order.push_back(id.to_owned());
                while order.len() > PUSH_RECORDS {
                    if let Some(oldest) = order.pop_front() {
                        records.remove(&oldest);
                    }
                }
            }
        }
    }
}

/// The OSRE cells of a step (C++ `osre_fold_cells`): each cell a mergeBatch
/// region covers, with its writing machine (first by name where several do)
/// and that machine's transformation.
fn osre_fold_cells(
    step: &Value,
    catalog: &BTreeMap<String, Value>,
) -> BTreeMap<i64, (String, String)> {
    let mut cells: BTreeMap<i64, (String, String)> = BTreeMap::new();
    for op in step["mergeBatch"].as_array().into_iter().flatten() {
        let Some(machine_id) = op["machineId"].as_str().filter(|id| !id.is_empty()) else {
            continue;
        };
        if !op["region"].is_object() {
            continue;
        }
        let declared = op["machineName"].as_str().unwrap_or(machine_id);
        let (name, transformation) = match catalog.get(machine_id) {
            Some(m) => (
                m["name"].as_str().unwrap_or(declared).to_owned(),
                m["outputMergeTransformation"]
                    .as_str()
                    .unwrap_or("or")
                    .to_owned(),
            ),
            None => (declared.to_owned(), "or".to_owned()),
        };
        let offset = op["region"]["offset"].as_f64().unwrap_or(-1.0) as i64;
        let length = op["region"]["length"].as_f64().unwrap_or(0.0) as i64;
        for cell in offset.max(0)..offset + length {
            if offset < 0 {
                break;
            }
            match cells.get(&cell) {
                Some((held, _)) if *held <= name => {}
                _ => {
                    cells.insert(cell, (name.clone(), transformation.clone()));
                }
            }
        }
    }
    cells
}

/// C++ `aggregator::aggregate_machine_outputs`: each machine that should
/// output writes its folded output over its output region, in machine-name
/// order, except where more than one did: the RE arbitrated those cells and
/// `base` already holds the answer.
fn aggregate(mut base: Vec<f64>, results: &Value) -> Vec<f64> {
    let Some(results) = results.as_object() else {
        return base;
    };
    let mut records: Vec<(String, usize, Vec<f64>)> = Vec::new();
    for (machine_id, result) in results {
        if result["transitionResult"]["arbiterMetadata"]["shouldOutput"] != json!(true) {
            continue;
        }
        let region = &result["outputRegion"];
        if !region.is_object() {
            continue;
        }
        let offset = region["offset"].as_f64().unwrap_or(-1.0) as i64;
        let length = region["length"].as_f64().unwrap_or(0.0) as i64;
        if offset < 0 || length <= 0 {
            continue;
        }
        let vector = if result["mergedOutputVector"].is_array() {
            &result["mergedOutputVector"]
        } else {
            &result["outputVector"]
        };
        if !vector.is_array() {
            continue;
        }
        let mut values = machine_routes::numbers(vector);
        if values.is_empty() {
            continue;
        }
        values.truncate(length as usize);
        let name = result["machineName"]
            .as_str()
            .filter(|n| !n.is_empty())
            .unwrap_or(machine_id);
        records.push((name.to_owned(), offset as usize, values));
    }
    records.sort_by(|a, b| a.0.cmp(&b.0));
    let mut writers: Vec<usize> = Vec::new();
    for (_, offset, values) in &records {
        let end = offset + values.len();
        if end > writers.len() {
            writers.resize(end, 0);
        }
        for count in &mut writers[*offset..end] {
            *count += 1;
        }
    }
    for (_, offset, values) in &records {
        if offset + values.len() > base.len() {
            base.resize(offset + values.len(), 0.0);
        }
        for (i, value) in values.iter().enumerate() {
            if writers[offset + i] == 1 {
                base[offset + i] = *value;
            }
        }
    }
    base
}

/// C++ `apply_step_selector`: a present `only` keeps the machines it names
/// (by name) and those that produced an operation carrying a sequence id it
/// names; naming nothing selects nothing.
fn select(step: &mut Value, only: &Value) {
    if !only.is_object() || !step.is_object() {
        return;
    }
    let strings = |v: &Value| -> std::collections::BTreeSet<String> {
        v.as_array()
            .into_iter()
            .flatten()
            .filter_map(|s| s.as_str().map(str::to_owned))
            .collect()
    };
    let (want_seq, want_name) = (
        strings(&only["sequenceIds"]),
        strings(&only["machineNames"]),
    );
    let named: std::collections::BTreeSet<String> = step["machineResults"]
        .as_object()
        .into_iter()
        .flatten()
        .filter(|(_, r)| {
            r["machineName"]
                .as_str()
                .is_some_and(|n| want_name.contains(n))
        })
        .map(|(id, _)| id.clone())
        .collect();
    let carries = |op: &Value| {
        strings(&op["sequenceIds"])
            .iter()
            .any(|s| want_seq.contains(s))
    };
    let by_sequence: std::collections::BTreeSet<String> = step["mergeBatch"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|op| carries(op))
        .filter_map(|op| op["machineId"].as_str().map(str::to_owned))
        .collect();
    let id_of = |v: &Value, key: &str| v[key].as_str().unwrap_or_default().to_owned();
    if let Some(results) = step["machineResults"].as_object() {
        let kept: serde_json::Map<String, Value> = results
            .iter()
            .filter(|(id, _)| named.contains(*id) || by_sequence.contains(*id))
            .map(|(id, r)| (id.clone(), r.clone()))
            .collect();
        step["machineResults"] = Value::Object(kept);
    }
    let keep = |key: &str, step: &mut Value, f: &dyn Fn(&Value) -> bool| {
        if let Some(items) = step[key].as_array() {
            step[key] = Value::Array(items.iter().filter(|v| f(v)).cloned().collect());
        }
    };
    keep("mergeBatch", step, &|op| {
        named.contains(&id_of(op, "machineId")) || carries(op)
    });
    keep("eventBus", step, &|w| {
        want_seq.contains(&id_of(w, "producerSequenceId"))
            || named.contains(&id_of(w, "producerMachineId"))
            || named.contains(&id_of(w, "subscriberMachineId"))
    });
    keep("activeRegions", step, &|r| {
        named.contains(&id_of(r, "machineId"))
    });
}

fn failure(pe: &SharedPe, error: String) -> Value {
    let global_step = pe.read().map_or(0, |state| state.global_step);
    json!({ "success": false, "step": null, "timestamp": now_ms(), "globalStep": global_step, "error": error })
}

/// One push, and what it tells the streams: the new state when the step
/// stood, and the reply either way (C++ `execute_push`'s broadcasts).
async fn execute(pe: &SharedPe, include_machine_results: bool, only: &Value) -> Value {
    let result = transition(pe, include_machine_results, only).await;
    if let Ok(state) = pe.read() {
        if result["success"] == json!(true) {
            state.broadcast_state();
        }
        state.broadcast_push_result(&result);
    }
    result
}

/// C++ `execute_push`: one transition, and the reply trimmed to what the
/// caller asked to see.
async fn transition(pe: &SharedPe, include_machine_results: bool, only: &Value) -> Value {
    let (vector, match_algorithm, re_url) = {
        let Ok(mut state) = pe.write() else {
            return failure(pe, lock_error().message);
        };
        let vector = state.assemble();
        state.record_contention();
        (vector, state.match_algorithm, state.re_url.clone())
    };
    // C++ reads the catalog on every push: the fold and trigger dispatch
    // need each merge entry's machine as it is now.
    dispatch::refresh_catalog(pe).await;
    // Every observation field, whatever the caller asked to see: machine
    // results are this engine's next input, not only part of the reply.
    let payload = json!({
        "vector": vector,
        "matchAlgorithm": match_algorithm,
        "includeMachineResults": true,
        "includePerceptualSpace": true,
    });
    let mut step = match client::post_json(&format!("{re_url}/api/perceive"), &payload).await {
        Ok(bytes) => match serde_json::from_slice::<Value>(&bytes) {
            Ok(step) => step,
            Err(error) => return failure(pe, error.to_string()),
        },
        Err(error) => return failure(pe, error),
    };
    let timestamp = now_ms();
    let global_step = {
        let Ok(mut state) = pe.write() else {
            return failure(pe, lock_error().message);
        };
        state.osre_fold = osre_fold_cells(&step, &state.catalog.machines);
        if step["perceptualSpace"].is_array() {
            let space = aggregate(
                machine_routes::numbers(&step["perceptualSpace"]),
                &step["machineResults"],
            );
            state.adopt_space(&space);
        }
        state.advance();
        state.last_push = step.clone();
        state.record_perception_events();
        // On the whole step, before the caller's selector narrows the reply.
        state.dispatch_triggers(&step);
        state.global_step
    };
    select(&mut step, only);
    if !include_machine_results && let Some(object) = step.as_object_mut() {
        object.remove("machineResults");
    }
    json!({ "success": true, "step": step, "timestamp": timestamp, "globalStep": global_step, "error": null })
}

impl Pushes {
    /// Take the queue, or count the caller as owed a push.
    fn claim(&self) -> bool {
        let Ok(mut queue) = self.queue.lock() else {
            return false;
        };
        if queue.0 {
            queue.1 += 1;
            return false;
        }
        queue.0 = true;
        true
    }

    /// Free the queue when nothing is owed; otherwise keep it, for the one
    /// more push the owed callers get.
    fn release_unless_owed(&self) -> bool {
        let Ok(mut queue) = self.queue.lock() else {
            return true;
        };
        if queue.1 == 0 {
            queue.0 = false;
            return true;
        }
        queue.1 = 0;
        false
    }

    fn release(&self) {
        if let Ok(mut queue) = self.queue.lock() {
            *queue = (false, 0);
        }
    }
}

/// The push owed to callers turned away while one was in flight: one more,
/// then the queue is free (C++ `push_worker_loop`).
async fn owed_push(pe: SharedPe, pushes: Arc<Pushes>) {
    execute(&pe, false, &Value::Null).await;
    pushes.release();
}

/// C++ `do_push`: the status and the body it answers.
pub(crate) async fn push_json(
    pe: &SharedPe,
    include_machine_results: bool,
    run_async: bool,
    only: Value,
) -> (StatusCode, Value) {
    let pushes = match pe.read() {
        Ok(state) => Arc::clone(&state.pushes),
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({ "error": lock_error().message }),
            );
        }
    };
    if !pushes.claim() {
        let mut answer = failure(pe, "push already in progress".to_owned());
        answer["coalesced"] = json!(true);
        return (StatusCode::CONFLICT, answer);
    }
    let id = format!("push-{}", new_machine_id().trim_start_matches("machine-"));
    pushes.record(&id, "queued", None);
    let job = {
        let (pe, pushes, id) = (Arc::clone(pe), Arc::clone(&pushes), id.clone());
        async move {
            pushes.record(&id, "running", None);
            let result = execute(&pe, include_machine_results, &only).await;
            pushes.record(&id, "completed", Some(result.clone()));
            if !pushes.release_unless_owed() {
                tokio::spawn(owed_push(pe, Arc::clone(&pushes)));
            }
            result
        }
    };
    if run_async {
        tokio::spawn(job);
        return (
            StatusCode::ACCEPTED,
            json!({ "success": true, "accepted": true, "jobId": id, "statusEndpoint": format!("/api/push/{id}") }),
        );
    }
    (StatusCode::OK, job.await)
}

pub(crate) async fn push(
    pe: &SharedPe,
    include_machine_results: bool,
    run_async: bool,
    only: Value,
) -> Response {
    let (status, body) = push_json(pe, include_machine_results, run_async, only).await;
    (status, Json(body)).into_response()
}

pub(crate) async fn push_route(State(pe): State<SharedPe>, body: Bytes) -> Response {
    let body = match body_of(&body) {
        Ok(body) => body,
        Err(error) => return error.into_response(),
    };
    let include_machine_results = body["includeMachineResults"]
        .as_bool()
        .unwrap_or(!body["compact"].as_bool().unwrap_or(false));
    let run_async = body["async"].as_bool().unwrap_or(false);
    push(
        &pe,
        include_machine_results,
        run_async,
        body["only"].clone(),
    )
    .await
}

pub(crate) async fn job(State(pe): State<SharedPe>, Path(id): Path<String>) -> Answer {
    let pushes = Arc::clone(&read(&pe)?.pushes);
    let jobs = pushes
        .jobs
        .lock()
        .map_err(|_| error_response(lock_error()))?;
    let job = jobs
        .0
        .get(&id)
        .ok_or_else(|| error_response(EngineError::not_found("Push job not found")))?;
    let mut out = json!({ "id": id, "status": job.status, "createdAt": job.created_at, "updatedAt": job.updated_at });
    if let Some(result) = &job.result {
        out["result"] = result.clone();
    }
    Ok(Json(out))
}

/// `POST /api/auto/start`: push every `intervalMs` (default and floor 1000
/// when not positive) until stopped. A start replaces a running loop.
pub(crate) async fn auto_start(State(pe): State<SharedPe>, body: Bytes) -> Answer {
    let body = body_of(&body)?;
    let requested = body["intervalMs"].as_f64().unwrap_or(1000.0) as i64;
    let interval = if requested > 0 {
        requested as u64
    } else {
        1000
    };
    let (pushes, generation) = {
        let mut state = write(&pe)?;
        state.auto_running = true;
        state.auto_interval_ms = interval;
        let pushes = Arc::clone(&state.pushes);
        let generation = pushes.auto_generation.fetch_add(1, Ordering::SeqCst) + 1;
        (pushes, generation)
    };
    let pe_loop = Arc::clone(&pe);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(interval)).await;
            if pushes.auto_generation.load(Ordering::SeqCst) != generation {
                return;
            }
            push(&pe_loop, true, false, Value::Null).await;
        }
    });
    Ok(Json(json!({ "success": true, "intervalMs": interval })))
}

pub(crate) async fn auto_stop(State(pe): State<SharedPe>) -> Answer {
    let mut state = write(&pe)?;
    state.auto_running = false;
    state.pushes.auto_generation.fetch_add(1, Ordering::SeqCst);
    Ok(Json(json!({ "success": true })))
}
