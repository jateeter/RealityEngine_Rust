//! `/api/runtime/*` (#10): metrics, the vector space, the storage footprint
//! and the flat options view.
//!
//! C++'s routes in `reality_engine_server.cpp` are the concrete form this
//! follows.
//!
//! - `metrics` is `/api/engine/stats` with a `domainWorkerPool` marker. Each
//!   runtime describes its own concurrency there (C++ pool counters, LSP
//!   `actor-mailbox`, Scala `akka-futures`); this one composes a step on
//!   scoped threads and has no pool to count, so it says so rather than
//!   reporting counters it does not keep.
//! - `options` is the flat view of five `/api/engine/config` controls, one
//!   state behind both (SURFACE_SPEC.md, "`/api/runtime/options` and
//!   `/api/engine/config` are one state"). `PATCH` sets the fields whose type
//!   is right and ignores the rest, as C++ does.

use super::*;

const ALLOWED_BITS: [u64; 4] = [1, 2, 4, 8];

impl Engine {
    fn runtime_options(&self) -> Value {
        let controls = &self.steps.controls;
        json!({
            "historyLimit": controls.history_limit,
            "includeActiveRegions": controls.include_active_regions,
            "includeMachineResults": controls.include_machine_results,
            "includePerceptualSpace": controls.include_perceptual_space,
            "phaseDetail": controls.phase_detail,
        })
    }

    fn patch_runtime_options(&mut self, body: &Value) -> Result<Value, EngineError> {
        if let Some(limit) = body["historyLimit"].as_f64() {
            // C++ casts a negative to size_t, which bounds nothing; refused here.
            if limit < 0.0 {
                return Err(EngineError::bad_request(
                    "historyLimit must be a non-negative number",
                ));
            }
            self.steps.controls.history_limit = limit as usize;
            self.steps.trim_history();
        }
        let controls = &mut self.steps.controls;
        for (key, field) in [
            (
                "includeMachineResults",
                &mut controls.include_machine_results,
            ),
            (
                "includePerceptualSpace",
                &mut controls.include_perceptual_space,
            ),
            ("includeActiveRegions", &mut controls.include_active_regions),
            ("phaseDetail", &mut controls.phase_detail),
        ] {
            if let Some(value) = body[key].as_bool() {
                *field = value;
            }
        }
        Ok(self.runtime_options())
    }

    fn vector_space(&self) -> Value {
        json!({
            "dimension": self.dimension,
            "requiredDimension": self.required_dimension(),
            "encoding": "dense-float64-clamped-0-1",
            "mappingVersion": self.mapping_version,
        })
    }

    /// C++ `storage_footprint_json`: each mapped machine's regions as float64
    /// and as packed at its `bitsPerElement` (8 when undeclared or invalid).
    fn storage_footprint(&self) -> Value {
        let mut histogram = json!({ "1": 0, "2": 0, "4": 0, "8": 0 });
        let (mut cells_total, mut float_total, mut packed_total) = (0_u64, 0_u64, 0_u64);
        let mut per_machine = Vec::new();
        for machine in self.machines.values() {
            let Some(mapping) = machine
                .raw
                .get("perceptualMapping")
                .filter(|m| m.is_object())
            else {
                continue;
            };
            let bits = mapping["bitsPerElement"]
                .as_u64()
                .filter(|bits| ALLOWED_BITS.contains(bits))
                .unwrap_or(8);
            let input = integer(mapping["input"].get("length"), 0) as u64;
            let output = integer(mapping["output"].get("length"), 0) as u64;
            let cells = input + output;
            let float64 = cells * 8;
            let packed = (cells * bits).div_ceil(8);
            cells_total += cells;
            float_total += float64;
            packed_total += packed;
            histogram[bits.to_string()] =
                json!(histogram[bits.to_string()].as_u64().unwrap_or(0) + 1);
            per_machine.push(json!({
                "machineId": machine.id,
                "machineName": machine.name,
                "bitsPerElement": bits,
                "inputCells": input,
                "outputCells": output,
                "float64Bytes": float64,
                "packedBytes": packed,
                "shrinkFactor": ratio(float64, packed),
            }));
        }
        json!({
            "machinesRegistered": self.machines.len(),
            "totalCells": cells_total,
            "totalFloat64Bytes": float_total,
            "totalPackedBytes": packed_total,
            "cumulativeShrinkFactor": ratio(float_total, packed_total),
            "widthHistogram": histogram,
            "perMachine": per_machine,
        })
    }
}

fn ratio(float64: u64, packed: u64) -> f64 {
    if packed == 0 {
        0.0
    } else {
        float64 as f64 / packed as f64
    }
}

type Answer = Result<Json<Value>, (StatusCode, Json<Value>)>;

fn read(
    engine: &SharedEngine,
) -> Result<std::sync::RwLockReadGuard<'_, Engine>, (StatusCode, Json<Value>)> {
    engine.read().map_err(|_| error_response(lock_error()))
}

pub(crate) async fn metrics(State(engine): State<SharedEngine>) -> Answer {
    Ok(Json(json!({
        "stats": read(&engine)?.stats(),
        "domainWorkerPool": { "semantics": "scoped-threads" },
    })))
}

pub(crate) async fn vector_space(State(engine): State<SharedEngine>) -> Answer {
    Ok(Json(read(&engine)?.vector_space()))
}

pub(crate) async fn storage_footprint(State(engine): State<SharedEngine>) -> Answer {
    Ok(Json(read(&engine)?.storage_footprint()))
}

pub(crate) async fn options(State(engine): State<SharedEngine>) -> Answer {
    Ok(Json(read(&engine)?.runtime_options()))
}

pub(crate) async fn patch_options(State(engine): State<SharedEngine>, body: Bytes) -> Answer {
    let body = if body.is_empty() {
        json!({})
    } else {
        serde_json::from_slice(&body).map_err(|error| {
            error_response(EngineError::bad_request(format!(
                "invalid JSON body: {error}"
            )))
        })?
    };
    let mut state = engine.write().map_err(|_| error_response(lock_error()))?;
    state
        .patch_runtime_options(&body)
        .map(Json)
        .map_err(error_response)
}
