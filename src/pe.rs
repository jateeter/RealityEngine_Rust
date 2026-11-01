//! The Perception Engine (#20), workstream 1 (#46): its binary, router, health,
//! state, configuration and reset.
//!
//! Every engine instance is an RE and a PE. The PE assembles the universal
//! Reality Event from its active sources and drives its RE with it; this first
//! slice carries the run state that needs no source model. C++'s
//! `perception_engine_server.cpp` and `PerceptionEngine` are the concrete form
//! this follows.
//!
//! - `GET /api/state` is C++ `state_json`. No source can be registered yet
//!   (`/api/sources` declares itself unsupported), so `sources` is empty and
//!   `assembledVector` is the zeroed persistent vector: what C++ reports with
//!   nothing registered (`PE_SOURCE_BOOTSTRAP=off`).
//! - `POST /api/reset` is layer-local (SURFACE_SPEC.md, "Reset is layer-local"):
//!   it clears this PE's run state and never touches the RE.
//! - `GET /api/health` is the specification's `{status, timestamp, version}`
//!   (owner ruling, 2026-10-31), as the RE answers it.
//! - Every other specified PE route answers 501 `unsupported`.

use super::*;
use axum::routing::patch;

/// The auto-push interval C++ reports before any `POST /api/auto/start`.
const AUTO_INTERVAL_MS: u64 = 1000;

pub struct PerceptionEngine {
    dimension: usize,
    global_step: u64,
    persistent_vector: Vec<f64>,
    /// `gte` or `equals` (C++ `MatchAlgorithm`).
    match_algorithm: &'static str,
    /// `null` before any push, then the step the last push produced.
    last_push: Value,
}

impl PerceptionEngine {
    pub fn new(dimension: usize) -> Self {
        Self {
            dimension,
            global_step: 0,
            persistent_vector: vec![0.0; dimension],
            match_algorithm: "gte",
            last_push: Value::Null,
        }
    }

    fn state(&self) -> Value {
        json!({
            "sources": [],
            "assembledVector": self.persistent_vector,
            "globalStep": self.global_step,
            "auto": { "running": false, "intervalMs": AUTO_INTERVAL_MS },
            "lastPush": self.last_push,
            "matchAlgorithm": self.match_algorithm,
            "perceptionDimension": self.dimension,
        })
    }

    /// C++ `match_algorithm_from_string`: the exact name, or a 500. A
    /// non-string value is ignored.
    fn configure(&mut self, body: &Value) -> Result<Value, EngineError> {
        if let Some(name) = body["matchAlgorithm"].as_str() {
            self.match_algorithm = match name {
                "gte" => "gte",
                "equals" => "equals",
                _ => {
                    return Err(EngineError::internal(format!(
                        "Unknown match algorithm: {name}"
                    )));
                }
            };
        }
        Ok(json!({ "success": true, "matchAlgorithm": self.match_algorithm }))
    }

    /// The run state, and nothing of the RE's.
    fn reset(&mut self) {
        self.global_step = 0;
        self.persistent_vector = vec![0.0; self.dimension];
        self.last_push = Value::Null;
    }
}

type SharedPe = Arc<RwLock<PerceptionEngine>>;
type Answer = Result<Json<Value>, (StatusCode, Json<Value>)>;

fn read(
    pe: &SharedPe,
) -> Result<std::sync::RwLockReadGuard<'_, PerceptionEngine>, (StatusCode, Json<Value>)> {
    pe.read().map_err(|_| error_response(lock_error()))
}

fn write(
    pe: &SharedPe,
) -> Result<std::sync::RwLockWriteGuard<'_, PerceptionEngine>, (StatusCode, Json<Value>)> {
    pe.write().map_err(|_| error_response(lock_error()))
}

async fn root() -> Json<Value> {
    Json(json!({ "service": "Perception Engine (Rust)", "status": "running" }))
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "healthy", "timestamp": now_ms(), "version": VERSION }))
}

async fn state(State(pe): State<SharedPe>) -> Answer {
    Ok(Json(read(&pe)?.state()))
}

async fn config(State(pe): State<SharedPe>, body: Bytes) -> Answer {
    let body = if body.is_empty() {
        json!({})
    } else {
        serde_json::from_slice(&body).map_err(|error| {
            error_response(EngineError::bad_request(format!(
                "invalid JSON body: {error}"
            )))
        })?
    };
    write(&pe)?
        .configure(&body)
        .map(Json)
        .map_err(error_response)
}

async fn reset(State(pe): State<SharedPe>) -> Answer {
    write(&pe)?.reset();
    Ok(Json(json!({ "success": true })))
}

pub fn pe_app(pe: PerceptionEngine) -> Router {
    Router::new()
        .route("/", get(root))
        .route("/api/health", get(health))
        .route("/api/state", get(state))
        .route("/api/config", patch(config))
        .route("/api/reset", post(reset))
        .fallback(|method: Method, uri: Uri| async move {
            unsupported_or(Layer::Pe, &method, &uri, StatusCode::NOT_FOUND)
        })
        .method_not_allowed_fallback(|method: Method, uri: Uri| async move {
            unsupported_or(Layer::Pe, &method, &uri, StatusCode::METHOD_NOT_ALLOWED)
        })
        .layer(axum::middleware::map_response(ecmascript_numbers))
        .with_state(Arc::new(RwLock::new(pe)))
}

/// `PERCEPTION_ENGINE_PORT` (default 5800), `VECTOR_DIMENSION` (default
/// 7680), and `RE_URL`, the RE this PE will drive (default
/// `http://localhost:5801`, the Rust RE's port).
pub async fn serve() -> Result<(), Box<dyn std::error::Error>> {
    let port = std::env::var("PERCEPTION_ENGINE_PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(5800);
    let dimension = std::env::var("VECTOR_DIMENSION")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(7680);
    let re_url = std::env::var("RE_URL").unwrap_or_else(|_| "http://localhost:5801".to_owned());
    eprintln!("perception-engine: port {port}, perceptionDimension {dimension}, RE {re_url}");
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
    axum::serve(listener, pe_app(PerceptionEngine::new(dimension))).await?;
    Ok(())
}
