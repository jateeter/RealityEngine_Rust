//! The Perception Engine (#20): its binary, router, health, state,
//! configuration and reset (workstream 1, #46), its sources, sensors and
//! machine ingestion (workstream 2, `sources.rs`), and the push cycle
//! (workstream 3, `push.rs`).
//!
//! Every engine instance is an RE and a PE. The PE assembles the universal
//! Reality Event from its active sources and drives its RE with it. C++'s
//! `perception_engine_server.cpp` and `PerceptionEngine` are the concrete form
//! this follows.
//!
//! - `GET /api/state` is C++ `state_json`: the sources in `(name, id)` order and
//!   the vector they assemble now.
//! - `POST /api/reset` is layer-local (SURFACE_SPEC.md, "Reset is layer-local"):
//!   it clears this PE's run state, validates each source's activity, and never
//!   touches the RE or the source set.
//! - `GET /api/health` is the specification's `{status, timestamp, version}`
//!   (owner ruling, 2026-10-31), as the RE answers it.
//! - At boot the PE interns its RE's machines as test sources unless
//!   `PE_SOURCE_BOOTSTRAP` says not to (SURFACE_SPEC.md, "Machine ingestion").
//! - Every other specified PE route answers 501 `unsupported`.

use super::*;
use axum::routing::patch;
use std::sync::atomic::AtomicU64;

mod adapters;
mod client;
mod dispatch;
mod ingest;
mod integrations;
mod localai;
mod metrics;
mod mqtt;
mod push;
mod sources;
mod streams;

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
    /// The RE this PE reads machines from and will drive.
    re_url: String,
    sources: BTreeMap<String, sources::Source>,
    /// Each test source's cursor into its interned inputs.
    test_step: BTreeMap<String, usize>,
    contention: sources::Contention,
    /// `PE_SOURCE_MERGE`: skip a machine that already has a source (the
    /// default, skip-if-present), or rebuild one whose definition changed.
    merge_only: bool,
    rng: AtomicU64,
    /// Each random walk's current values; a push moves them.
    walk_state: BTreeMap<String, Vec<f64>>,
    /// The last push's OSRE cells: cell -> (writing machine, its
    /// `outputMergeTransformation`). A source on one is folded with it.
    osre_fold: BTreeMap<i64, (String, String)>,
    /// The arbitration registry the RE loads: a fold on a declared cell
    /// follows the declared rule.
    registry: step::ArbitrationRegistry,
    pushes: Arc<push::Pushes>,
    auto_running: bool,
    auto_interval_ms: u64,
    /// `/api/events` and `/ws`: every message, to every listener.
    events: tokio::sync::broadcast::Sender<String>,
    /// The integrations configuration, read once at boot.
    integrations: Arc<integrations::Integrations>,
    /// `GET /api/integrations/localai/ledger`: every invocation, newest 256.
    localai_ledger: Arc<std::sync::Mutex<std::collections::VecDeque<Value>>>,
    /// The RE's machines by id: trigger dispatch and the OSRE fold read it.
    catalog: dispatch::MachineCatalog,
    dispatch: dispatch::Dispatch,
    /// The MQTT bridge, while one runs.
    mqtt: Option<Arc<mqtt::Bridge>>,
}

impl PerceptionEngine {
    pub fn new(dimension: usize) -> Self {
        Self {
            dimension,
            global_step: 0,
            persistent_vector: vec![0.0; dimension],
            match_algorithm: "gte",
            last_push: Value::Null,
            re_url: "http://localhost:5801".to_owned(),
            sources: BTreeMap::new(),
            test_step: BTreeMap::new(),
            contention: sources::Contention::default(),
            merge_only: true,
            rng: sources::new_rng(),
            walk_state: BTreeMap::new(),
            osre_fold: BTreeMap::new(),
            registry: step::ArbitrationRegistry::default(),
            pushes: Arc::default(),
            auto_running: false,
            auto_interval_ms: AUTO_INTERVAL_MS,
            events: streams::channel(),
            integrations: Arc::default(),
            localai_ledger: localai::ledger(),
            catalog: dispatch::MachineCatalog::default(),
            dispatch: dispatch::Dispatch::default(),
            mqtt: None,
        }
    }

    /// The RE this PE reads its machines from.
    pub fn with_re_url(mut self, re_url: impl Into<String>) -> Self {
        self.re_url = re_url.into();
        self
    }

    fn state(&self) -> Value {
        json!({
            "sources": self.sources_json(),
            "assembledVector": self.assemble(),
            "globalStep": self.global_step,
            "auto": { "running": self.auto_running, "intervalMs": self.auto_interval_ms },
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
        self.reset_sources();
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

fn body_of(bytes: &Bytes) -> Result<Value, (StatusCode, Json<Value>)> {
    if bytes.is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_slice(bytes).map_err(|error| {
        error_response(EngineError::bad_request(format!(
            "invalid JSON body: {error}"
        )))
    })
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
    let body = body_of(&body)?;
    let mut state = write(&pe)?;
    let answer = state.configure(&body).map_err(error_response)?;
    state.broadcast_state();
    Ok(Json(answer))
}

async fn reset(State(pe): State<SharedPe>) -> Answer {
    let mut state = write(&pe)?;
    state.reset();
    state.broadcast_state();
    Ok(Json(json!({ "success": true })))
}

fn router(pe: SharedPe) -> Router {
    Router::new()
        .route("/", get(root))
        .route("/api/health", get(health))
        .route("/api/state", get(state))
        .route("/api/config", patch(config))
        .route("/api/reset", post(reset))
        .route("/api/sources", get(sources::list).post(sources::create))
        .route(
            "/api/sources/{id}",
            patch(sources::update).delete(sources::remove),
        )
        .route(
            "/api/sources/bootstrap-from-machines",
            post(sources::bootstrap),
        )
        .route("/api/sources/contention", get(sources::contention))
        .route("/api/sensors/{sensor_id}", post(sources::sensor))
        .route("/api/signals", post(sources::signal))
        .route("/api/machines", get(sources::machines))
        .route("/api/push", post(push::push_route))
        .route("/api/push/{id}", get(push::job))
        .route("/api/auto/start", post(push::auto_start))
        .route("/api/auto/stop", post(push::auto_stop))
        .route("/api/integrations/status", get(integrations::status))
        .route(
            "/api/integrations/ollama/status",
            get(integrations::ollama_status),
        )
        .route(
            "/api/integrations/openai/status",
            get(integrations::openai_status),
        )
        .route(
            "/api/integrations/acp/status",
            get(integrations::acp_status),
        )
        .route(
            "/api/integrations/healthkit/status",
            get(integrations::healthkit_status),
        )
        .route(
            "/api/integrations/carekit/status",
            get(integrations::carekit_status),
        )
        .route(
            "/api/integrations/localai/status",
            get(integrations::localai_status),
        )
        .route("/api/integrations/localai/catalog", get(localai::catalog))
        .route(
            "/api/integrations/localai/bootstrap",
            post(localai::bootstrap),
        )
        .route("/api/integrations/localai/invoke", post(localai::invoke))
        .route(
            "/api/integrations/localai/ledger",
            get(localai::ledger_route),
        )
        .route("/api/integrations/completions", post(ingest::completion))
        .route("/api/integrations/ollama/dispatch", post(adapters::ollama))
        .route("/api/integrations/openai/dispatch", post(adapters::openai))
        .route("/api/integrations/acp/dispatch", post(adapters::acp))
        .route(
            "/api/integrations/healthkit/ingest",
            post(ingest::healthkit),
        )
        .route("/api/integrations/carekit/ingest", post(ingest::carekit))
        .route("/api/triggers/status", get(dispatch::triggers))
        .route("/api/dispatch/ledger", get(dispatch::ledger))
        .route(
            "/api/dispatch/records/{id}",
            get(dispatch::record).patch(dispatch::update),
        )
        .route("/api/dispatch/records/{id}/replay", post(dispatch::replay))
        .route("/api/metrics", get(metrics::metrics))
        .route("/api/mqtt/status", get(mqtt::status))
        .route(
            "/api/mqtt/mappings",
            get(mqtt::mappings).put(mqtt::replace_mappings),
        )
        .route("/api/mqtt/enable", post(mqtt::enable))
        .route("/api/mqtt/disable", post(mqtt::disable))
        .route("/api/events", get(streams::events))
        .route("/ws", get(streams::websocket))
        .fallback(|method: Method, uri: Uri| async move {
            unsupported_or(Layer::Pe, &method, &uri, StatusCode::NOT_FOUND)
        })
        .method_not_allowed_fallback(|method: Method, uri: Uri| async move {
            unsupported_or(Layer::Pe, &method, &uri, StatusCode::METHOD_NOT_ALLOWED)
        })
        .layer(axum::middleware::map_response(ecmascript_numbers))
        .with_state(pe)
}

pub fn pe_app(pe: PerceptionEngine) -> Router {
    router(Arc::new(RwLock::new(pe)))
}

/// A flag as SURFACE_SPEC.md, "Machine ingestion", reads `PE_SOURCE_BOOTSTRAP`:
/// unset, `auto`, `on`, `1`, `true` and `yes` are on; `off`, `0`, `false` and
/// `no` are off.
fn flag_on(value: Option<&str>) -> bool {
    !matches!(
        value,
        Some("off" | "OFF" | "0" | "false" | "FALSE" | "no" | "NO")
    )
}

/// `PERCEPTION_ENGINE_PORT` (default 5800), `VECTOR_DIMENSION` (default
/// 7680), `RE_URL`, the RE this PE reads and drives (default
/// `http://localhost:5801`, the Rust RE's port), `PE_SOURCE_BOOTSTRAP`,
/// `PE_SOURCE_MERGE`, and `MACHINES_DIR` / `ARBITRATION_REGISTRY` for the
/// arbitration registry, found as the RE finds it.
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
    let mut engine = PerceptionEngine::new(dimension).with_re_url(re_url.clone());
    let machines_dir = std::env::var("MACHINES_DIR")
        .unwrap_or_else(|_| "../RealityEngine_Machines/machines".to_owned());
    engine.registry = step::ArbitrationRegistry::load(std::path::Path::new(&machines_dir));
    engine.integrations = Arc::new(integrations::Integrations::from_environment());
    engine.dispatch = dispatch::Dispatch::from_environment(&engine.integrations.localai_base_url);
    if let Ok(merge) = std::env::var("PE_SOURCE_MERGE") {
        engine.merge_only = flag_on(Some(&merge));
    }
    let pe = Arc::new(RwLock::new(engine));
    // The catalog is a cache of the RE's machines, and reading it changes no
    // source membership, so it is read whatever PE_SOURCE_BOOTSTRAP says.
    dispatch::refresh_catalog(&pe).await;
    if flag_on(std::env::var("PE_SOURCE_BOOTSTRAP").ok().as_deref()) {
        let summary = sources::bootstrap_from_re(&pe, &re_url).await.to_json();
        eprintln!("perception-engine: boot bootstrap {summary}");
    }
    mqtt::start_from_environment(&pe);
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
    axum::serve(listener, router(pe)).await?;
    Ok(())
}
