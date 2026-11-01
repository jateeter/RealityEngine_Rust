//! Perceptual simulation, the step stream and the active Reality Events (#11,
//! slice 3).
//!
//! SURFACE_SPEC.md, "`/api/perceptual-simulation` has one behaviour on every
//! runtime" (RealityEngine_CI#489) and "Perceptual simulation state", govern;
//! C++ `PerceptualSpaceRuntime::configure` / `step` and its server's routes are
//! the concrete form this follows.
//!
//! - `configure/chunk` buffers the input sequence's Reality Events (`events`)
//!   and, from `config` or the top level, `inputRegion`, `stepDelayMs` and
//!   `maxSteps`. `configure/commit` makes the buffer the committed sequence and
//!   resets; with no region buffered it is a 400.
//! - `step` writes `sequence[currentStep]` at the region's offset over the space
//!   as the last step left it and runs the phases `POST /api/perceive` runs. Past
//!   the end, or at `maxSteps`, it answers `{done, success}` and stops the run.
//! - `start` auto-plays: a step now and then every `stepDelayMs` while the run
//!   is live. Each start takes a new generation, so a loop left from an earlier
//!   start exits instead of stepping beside it.
//! - Simulation steps number from their own counter, as pushed steps do from
//!   theirs; both land in the trajectories and the step history.
//!
//! `GET /api/engine/stream` carries each pushed step as `POST /api/perceive`
//! answered it, bare, as C++ and Scala send it: the C++ signature is the
//! contract (owner ruling, 2026-11-01), not the `{type: "step-result", step}`
//! wrapper LSP sends.

use super::*;
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use futures_util::{Stream, StreamExt, stream};
use std::convert::Infallible;
use std::time::Duration;
use tokio::sync::broadcast::error::RecvError;

/// The committed sequence, the run over it, and what `configure/chunk` has
/// buffered for the next commit.
#[derive(Default)]
pub(crate) struct Simulation {
    sequence: Vec<Vec<f64>>,
    offset: usize,
    delay_ms: u64,
    max_steps: Option<u64>,
    configured: bool,
    running: bool,
    /// Simulation steps taken since the last reset; the next one's number.
    current_step: u64,
    /// Bumped by every `start`; an auto-play loop runs only while it holds the
    /// newest.
    generation: u64,
    buffer: Vec<Vec<f64>>,
    /// (offset, stepDelayMs, maxSteps), once a chunk has carried a region.
    buffered: Option<(usize, u64, Option<u64>)>,
}

impl Simulation {
    /// Back to the start of the committed sequence, not running.
    pub(crate) fn rewind(&mut self) {
        self.running = false;
        self.current_step = 0;
    }

    fn done(&self) -> bool {
        self.current_step >= self.sequence.len() as u64
            || self.max_steps.is_some_and(|max| self.current_step >= max)
    }
}

impl Engine {
    /// One simulation step; `None` when the walk is done, which stops the run.
    fn simulation_step(&mut self) -> Result<Option<Arc<step::Step>>, EngineError> {
        if !self.steps.simulation.configured {
            return Err(EngineError::bad_request("Simulation not configured"));
        }
        if self.steps.simulation.done() {
            self.steps.simulation.running = false;
            return Ok(None);
        }
        let width = self.space_width();
        let simulation = &self.steps.simulation;
        let event = &simulation.sequence[simulation.current_step as usize];
        let offset = simulation.offset;
        if offset + event.len() > width {
            return Err(EngineError {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                message: "Region outside perceptual space".to_owned(),
            });
        }
        let space = &mut self.steps.space;
        space.resize(width, 0.0);
        space[offset..offset + event.len()].copy_from_slice(event);
        let step_number = simulation.current_step;
        let step = self.run_phases(step_number, None)?;
        let simulation = &mut self.steps.simulation;
        simulation.current_step += 1;
        // A run ends on the step that finishes the walk, not on the call after it.
        if simulation.running && simulation.done() {
            simulation.running = false;
        }
        Ok(Some(step))
    }

    /// Every active Reality Event of every machine (C++ `active_vectors_json`).
    /// The list carries no order (SURFACE_SPEC.md, "Active events").
    fn active_events(&self) -> Vec<Value> {
        let mut out = Vec::new();
        for machine in self.machines.values() {
            for sequence in machine.sequences.values() {
                for event in sequence.events.values().filter(|event| event.active) {
                    let elements: Vec<Value> = event
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
                    let outputs: Vec<Value> = event
                        .outputs
                        .iter()
                        .map(|output| {
                            json!({
                                "id": output.id,
                                "metadata": output.metadata,
                                "vector": output.vector,
                            })
                        })
                        .collect();
                    out.push(json!({
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
                            "outputEvents": outputs,
                            "state": "active",
                            "wasJustMatched": event.was_just_matched,
                        },
                    }));
                }
            }
        }
        out
    }
}

type Answer = Result<Json<Value>, (StatusCode, Json<Value>)>;

fn optional_body(body: &Bytes) -> Result<Value, EngineError> {
    if body.is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_slice(body)
        .map_err(|error| EngineError::bad_request(format!("invalid JSON body: {error}")))
}

pub(crate) async fn configure_chunk(State(engine): State<SharedEngine>, body: Bytes) -> Answer {
    let body = optional_body(&body).map_err(error_response)?;
    let mut state = engine.write().map_err(|_| error_response(lock_error()))?;
    let simulation = &mut state.steps.simulation;
    if body["reset"].as_bool().unwrap_or(false) {
        simulation.buffer.clear();
    }
    // The input sequence's Reality Events, under `events` (RealityEngine_CI#489).
    for event in body["events"].as_array().into_iter().flatten() {
        simulation.buffer.push(json_vector(Some(event)));
    }
    let config = if body["config"].is_object() {
        &body["config"]
    } else {
        &body
    };
    if config["inputRegion"].is_object() {
        simulation.buffered = Some((
            integer(config["inputRegion"].get("offset"), 0),
            number(config.get("stepDelayMs"), 100.0) as u64,
            config["maxSteps"].as_f64().map(|max| max.max(0.0) as u64),
        ));
    }
    Ok(Json(
        json!({ "success": true, "bufferedEvents": simulation.buffer.len() }),
    ))
}

pub(crate) async fn configure_commit(State(engine): State<SharedEngine>) -> Answer {
    let mut state = engine.write().map_err(|_| error_response(lock_error()))?;
    let simulation = &mut state.steps.simulation;
    // Without a region there is nowhere to write the sequence.
    let Some((offset, delay_ms, max_steps)) = simulation.buffered.take() else {
        return Err(error_response(EngineError::bad_request(
            "No config buffered. Send a chunk with config first.",
        )));
    };
    simulation.sequence = std::mem::take(&mut simulation.buffer);
    simulation.offset = offset;
    simulation.delay_ms = delay_ms;
    simulation.max_steps = max_steps;
    simulation.configured = true;
    state.reset_space();
    Ok(Json(json!({ "success": true })))
}

pub(crate) async fn step(State(engine): State<SharedEngine>) -> Answer {
    let mut state = engine.write().map_err(|_| error_response(lock_error()))?;
    Ok(Json(
        match state.simulation_step().map_err(error_response)? {
            None => json!({ "done": true, "success": true }),
            Some(step) => json!({ "success": true, "step": step.to_json(true, true, true, None) }),
        },
    ))
}

pub(crate) async fn reset(State(engine): State<SharedEngine>) -> Answer {
    let mut state = engine.write().map_err(|_| error_response(lock_error()))?;
    state.reset_space();
    Ok(Json(json!({ "success": true })))
}

pub(crate) async fn start(State(engine): State<SharedEngine>) -> Answer {
    let generation = {
        let mut state = engine.write().map_err(|_| error_response(lock_error()))?;
        let simulation = &mut state.steps.simulation;
        if !simulation.configured {
            return Err(error_response(EngineError::bad_request(
                "Simulation not configured",
            )));
        }
        simulation.running = true;
        simulation.generation += 1;
        simulation.generation
    };
    tokio::spawn(autoplay(engine, generation));
    Ok(Json(json!({ "success": true })))
}

/// Steps the committed sequence while the run is live and this loop holds the
/// newest generation; a stop, reset or commit clears `running`. The lock is
/// taken per step and released for the delay, so the routes stay responsive.
async fn autoplay(engine: SharedEngine, generation: u64) {
    loop {
        let delay_ms = {
            let Ok(mut state) = engine.write() else {
                return;
            };
            let simulation = &state.steps.simulation;
            if simulation.generation != generation || !simulation.running {
                return;
            }
            match state.simulation_step() {
                Ok(Some(_)) => {}
                Ok(None) => return,
                Err(error) => {
                    eprintln!("perceptual-simulation auto-play stopped: {}", error.message);
                    state.steps.simulation.running = false;
                    return;
                }
            }
            if !state.steps.simulation.running {
                return;
            }
            state.steps.simulation.delay_ms
        };
        tokio::time::sleep(Duration::from_millis(delay_ms.max(1))).await;
    }
}

pub(crate) async fn stop(State(engine): State<SharedEngine>) -> Answer {
    let mut state = engine.write().map_err(|_| error_response(lock_error()))?;
    state.steps.simulation.running = false;
    Ok(Json(json!({ "success": true })))
}

pub(crate) async fn state(State(engine): State<SharedEngine>) -> Answer {
    let state = engine.read().map_err(|_| error_response(lock_error()))?;
    let mut space = state.steps.space.clone();
    space.resize(state.space_width(), 0.0);
    let simulation = &state.steps.simulation;
    Ok(Json(json!({
        "state": {
            "perceptualSpace": space,
            "currentStep": simulation.current_step,
            "isRunning": simulation.running,
            "machines": state.list_machines(),
        }
    })))
}

pub(crate) async fn history(State(engine): State<SharedEngine>) -> Answer {
    let state = engine.read().map_err(|_| error_response(lock_error()))?;
    let steps: Vec<Value> = state
        .steps
        .history
        .iter()
        .map(|step| step.to_json(true, true, true, None))
        .collect();
    Ok(Json(json!({ "history": steps })))
}

pub(crate) async fn active(State(engine): State<SharedEngine>) -> Answer {
    let state = engine.read().map_err(|_| error_response(lock_error()))?;
    Ok(Json(json!({ "activeEvents": state.active_events() })))
}

/// `SSE_HEARTBEAT_MS`, default 15 s (SURFACE_SPEC.md: a keepalive every 15 s).
fn heartbeat() -> Duration {
    let ms = std::env::var("SSE_HEARTBEAT_MS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|ms| *ms >= 10)
        .unwrap_or(15000);
    Duration::from_millis(ms)
}

/// Live only: a subscriber sees the steps pushed after it connects. One that
/// falls behind skips to the newest rather than holding the step path back.
pub(crate) async fn stream(
    State(engine): State<SharedEngine>,
) -> Result<Sse<impl Stream<Item = Result<SseEvent, Infallible>>>, (StatusCode, Json<Value>)> {
    let receiver = engine
        .read()
        .map_err(|_| error_response(lock_error()))?
        .steps
        .stream
        .subscribe();
    let connected = stream::once(async { Ok(SseEvent::default().comment("connected")) });
    let steps = stream::unfold(receiver, |mut receiver| async move {
        loop {
            match receiver.recv().await {
                Ok(text) => return Some((Ok(SseEvent::default().data(text)), receiver)),
                Err(RecvError::Lagged(_)) => continue,
                Err(RecvError::Closed) => return None,
            }
        }
    });
    Ok(Sse::new(connected.chain(steps))
        .keep_alive(KeepAlive::new().interval(heartbeat()).text("keepalive")))
}
