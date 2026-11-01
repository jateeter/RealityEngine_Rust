//! `GET /api/arbitration` and arbitration retention (#12, RealityEngine_CI#296).
//!
//! SURFACE_SPEC.md, "Arbitration retention and the instance clock", governs;
//! C++'s `/api/arbitration` route and `arbitration_records_json` are the
//! concrete form this follows.
//!
//! - `arbitrationRetention` off (the default) is the legacy escape: the latest
//!   step's records in the legacy object, in resolution (cell) order, and
//!   nothing retained per step.
//! - On, each committed step's records are retained under the clock it
//!   committed at, before its completion is published, and the route answers
//!   the retained steps in the `arbitrationWindow` ending at the newest, oldest
//!   first; `?step=N` addresses one (200 / 404 / 410 / 409 / 400).
//! - A retained step's records are canonical: ascending by cell, and each
//!   record's contributors and suppressed ascending by (provider, originId,
//!   cesId, outputVectorId).

use super::*;
use step::{ArbitrationRecord, Contribution};

/// `arbitrationWindow`'s declared default and ceiling (SURFACE_SPEC.md); the
/// ceiling is the trajectory capacity.
pub(crate) const WINDOW_DEFAULT: usize = 1;
pub(crate) const WINDOW_MAX: usize = 1024;

/// `ARBITER_SHARDS`, else the hardware's parallelism: the arbiter's declared
/// parallelism (a deployment fact, never compared).
fn shards() -> usize {
    std::env::var("ARBITER_SHARDS")
        .ok()
        .map(|raw| raw.trim().parse::<usize>().unwrap_or(0))
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, |n| n.get()))
        .max(1)
}

fn contribution_json(c: &Contribution) -> Value {
    let text = |s: &str| if s.is_empty() { Value::Null } else { json!(s) };
    json!({
        "provider": c.provider,
        "determinism": step::determinism(c.provider),
        "originId": c.origin_id,
        "cesId": text(&c.ces_id),
        "outputVectorId": text(&c.output_vector_id),
        "ragStatusCode": text(&c.rag_status_code),
        "value": c.value,
    })
}

/// The records as the route lists them; `canonical` orders them as the
/// retention world must, and without it the legacy order is kept.
fn records_json(records: &[ArbitrationRecord], canonical: bool) -> Vec<Value> {
    let mut records = records.to_vec();
    if canonical {
        let key = |c: &Contribution| {
            (
                c.provider,
                c.origin_id.clone(),
                c.ces_id.clone(),
                c.output_vector_id.clone(),
            )
        };
        records.sort_by_key(|r| r.cell);
        for record in &mut records {
            record.contributors.sort_by_key(key);
            record.suppressed.sort_by_key(key);
        }
    }
    records
        .iter()
        .map(|r| {
            json!({
                "instant": r.instant,
                "cell": r.cell,
                "rule": r.rule,
                "resolved": r.resolved,
                "contributors": r.contributors.iter().map(contribution_json).collect::<Vec<_>>(),
                "suppressed": r.suppressed.iter().map(contribution_json).collect::<Vec<_>>(),
            })
        })
        .collect()
}

impl Engine {
    /// `{registryEntries, registrySource, shards, count, records}`: the legacy
    /// object, and every retained step's body beneath its clock.
    fn arbitration_body(&self, records: &[ArbitrationRecord], canonical: bool) -> Value {
        let records = records_json(records, canonical);
        json!({
            "registryEntries": self.arbitration.entries.len(),
            "registrySource": self.arbitration.source,
            "shards": shards(),
            "count": records.len(),
            "records": records,
        })
    }

    fn retained_step_json(&self, step: u64, lamport: u64, records: &[ArbitrationRecord]) -> Value {
        let mut body = self.arbitration_body(records, true);
        body["clock"] = json!({
            "instance": self.clock.instance(),
            "lamport": lamport,
            "step": step,
        });
        body
    }

    fn arbitration(&self, step: Option<&str>) -> Result<Value, EngineError> {
        let fail = |status: StatusCode, message: String| EngineError { status, message };
        let completed = self.steps.completed();
        let controls = &self.steps.controls;
        if let Some(raw) = step {
            let n = (!raw.is_empty() && raw.bytes().all(|b| b.is_ascii_digit()))
                .then(|| raw.parse::<u64>().ok())
                .flatten()
                .ok_or_else(|| EngineError::bad_request("step must be a non-negative integer"))?;
            if !controls.arbitration_retention {
                return Err(fail(
                    StatusCode::CONFLICT,
                    "arbitration retention is off; GET /api/arbitration?step=N needs arbitrationRetention true".to_owned(),
                ));
            }
            if n as i64 > completed {
                return Err(EngineError::not_found(format!("step {n} has not resolved")));
            }
            let Some((lamport, records)) = self.steps.retained.get(&n) else {
                return Err(fail(
                    StatusCode::GONE,
                    format!("step {n} is no longer retained"),
                ));
            };
            return Ok(self.retained_step_json(n, *lamport, records));
        }
        if controls.arbitration_retention {
            let first = (completed - controls.arbitration_window as i64 + 1).max(0);
            let steps: Vec<Value> = self
                .steps
                .retained
                .iter()
                .filter(|(step, _)| (**step as i64) >= first && (**step as i64) <= completed)
                .map(|(step, (lamport, records))| self.retained_step_json(*step, *lamport, records))
                .collect();
            return Ok(json!(steps));
        }
        Ok(self.arbitration_body(&self.steps.last_arbitration, false))
    }
}

pub(crate) async fn arbitration(
    State(engine): State<SharedEngine>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let state = engine.read().map_err(|_| error_response(lock_error()))?;
    state
        .arbitration(query.get("step").map(String::as_str))
        .map(Json)
        .map_err(error_response)
}
