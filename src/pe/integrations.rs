//! Integrations (#20 workstream 4), slice A: the integrations configuration
//! and the status routes over it.
//!
//! C++ `load_integration_registry`, `configure_ollama_from_environment` and the
//! `*_status` functions of `perception_engine_server.cpp` are the concrete form
//! this follows.
//!
//! - The configuration is `INTEGRATIONS_CONFIG`, else `config/integrations.json`
//!   beside the process, read once at boot: its `sourceMappings` by id, and each
//!   integration's settings by `kind`. The environment then overrides it
//!   (`OLLAMA_*`, `OPENAI_*`, `ACP_*` / `OPENCLAW_*`, `HEALTHKIT_*`, `CAREKIT_*`).
//! - `GET /api/integrations/status` reports what was loaded and every
//!   integration's settings; the per-integration routes report one each.
//!   Ollama, OpenAI and localAI are asked whether they answer, outside the
//!   engine lock; OpenAI is not asked without an API key.
//! - HealthKit's scope is reported as never declared: the scope and resync
//!   routes that would declare it are not in the specification.

use super::*;
use axum::http::Method;

/// What a localAI bridge sensor registers as (C++ `localai_sensor_specs`).
pub(crate) const LOCALAI_SENSORS: [(&str, &str, i64, i64); 3] = [
    ("localai_rag_retrieval", "localai/rag_retrieval", 52, 4),
    ("localai_rag_grading", "localai/rag_grading", 56, 4),
    ("localai_agent_activity", "localai/agent_activity", 64, 4),
];

/// The integrations configuration and the settings it, and the environment,
/// give each integration.
pub(crate) struct Integrations {
    path: Option<String>,
    loaded: bool,
    error: Option<String>,
    config: Value,
    pub(crate) source_mappings: BTreeMap<String, Value>,
    pub(crate) ollama_base_url: String,
    pub(crate) ollama_model: String,
    pub(crate) ollama_mapping: String,
    pub(crate) openai_base_url: String,
    pub(crate) openai_model: String,
    pub(crate) openai_mapping: String,
    pub(crate) openai_api_key: String,
    pub(crate) acp_enabled: bool,
    acp_platform: String,
    acp_surface: String,
    acp_command: String,
    pub(crate) acp_gateway_url: String,
    pub(crate) acp_session_key: String,
    pub(crate) acp_target_agent: String,
    pub(crate) acp_mapping: String,
    pub(crate) healthkit_bridge_id: String,
    pub(crate) healthkit_mapping: String,
    pub(crate) healthkit_token: String,
    pub(crate) carekit_bridge_id: String,
    pub(crate) carekit_mapping: String,
    pub(crate) carekit_token: String,
    pub(crate) localai_base_url: String,
    pub(crate) localai_machines_dir: String,
}

impl Default for Integrations {
    fn default() -> Self {
        Self {
            path: None,
            loaded: false,
            error: None,
            config: json!({}),
            source_mappings: BTreeMap::new(),
            ollama_base_url: "http://localhost:11434".to_owned(),
            // The engines' built-in default (RealityEngine_CI
            // docs/OLLAMA_INTEGRATION.md); a universe pins OLLAMA_MODEL.
            ollama_model: "llama3.1:8b".to_owned(),
            ollama_mapping: "agent-completion-risk".to_owned(),
            openai_base_url: "https://api.openai.com/v1".to_owned(),
            openai_model: "gpt-5".to_owned(),
            openai_mapping: "agent-completion-risk".to_owned(),
            openai_api_key: String::new(),
            acp_enabled: true,
            acp_platform: "OpenClaw".to_owned(),
            acp_surface: "xACP".to_owned(),
            acp_command: "openclaw acp".to_owned(),
            acp_gateway_url: "ws://127.0.0.1:18789".to_owned(),
            acp_session_key: "agent:main:main".to_owned(),
            acp_target_agent: "openclaw".to_owned(),
            acp_mapping: "acp-openclaw-completion".to_owned(),
            healthkit_bridge_id: "healthkit-ios-bridge".to_owned(),
            healthkit_mapping: "healthkit-activity".to_owned(),
            healthkit_token: String::new(),
            carekit_bridge_id: "carekit-ios-bridge".to_owned(),
            carekit_mapping: "carekit-task".to_owned(),
            carekit_token: String::new(),
            localai_base_url: "http://localhost:4000".to_owned(),
            localai_machines_dir: "../localAIStack/data/machines".to_owned(),
        }
    }
}

fn truthy(value: &str) -> bool {
    matches!(value, "1" | "true" | "TRUE" | "yes" | "YES")
}

impl Integrations {
    /// The configuration file, then the environment over it.
    pub(crate) fn from_environment() -> Self {
        let mut out = Self::default();
        let env = |name: &str| std::env::var(name).ok();
        let path = env("INTEGRATIONS_CONFIG")
            .filter(|p| !p.is_empty())
            .or_else(|| {
                std::path::Path::new("config/integrations.json")
                    .exists()
                    .then(|| "config/integrations.json".to_owned())
            });
        if let Some(path) = path {
            out.load(&path);
        }
        let set = |field: &mut String, name: &str| {
            if let Some(value) = env(name) {
                *field = value;
            }
        };
        set(&mut out.ollama_base_url, "OLLAMA_BASE_URL");
        set(&mut out.ollama_model, "OLLAMA_MODEL");
        set(
            &mut out.ollama_mapping,
            "OLLAMA_COMPLETION_SOURCE_MAPPING_ID",
        );
        set(&mut out.openai_base_url, "OPENAI_BASE_URL");
        set(&mut out.openai_model, "OPENAI_MODEL");
        set(
            &mut out.openai_mapping,
            "OPENAI_COMPLETION_SOURCE_MAPPING_ID",
        );
        set(&mut out.openai_api_key, "OPENAI_API_KEY");
        if let Some(enabled) = env("ACP_ENABLED") {
            out.acp_enabled = truthy(&enabled);
        }
        set(&mut out.acp_platform, "ACP_PLATFORM");
        set(&mut out.acp_surface, "ACP_SURFACE");
        set(&mut out.acp_command, "ACP_COMMAND");
        set(&mut out.acp_command, "OPENCLAW_ACP_COMMAND");
        set(&mut out.acp_gateway_url, "ACP_GATEWAY_URL");
        set(&mut out.acp_gateway_url, "OPENCLAW_GATEWAY_URL");
        set(&mut out.acp_session_key, "ACP_SESSION_KEY");
        set(&mut out.acp_session_key, "OPENCLAW_ACP_SESSION");
        set(&mut out.acp_target_agent, "ACP_TARGET_AGENT");
        set(&mut out.acp_mapping, "ACP_COMPLETION_SOURCE_MAPPING_ID");
        set(&mut out.healthkit_bridge_id, "HEALTHKIT_BRIDGE_ID");
        set(
            &mut out.healthkit_mapping,
            "HEALTHKIT_DEFAULT_SOURCE_MAPPING_ID",
        );
        set(&mut out.healthkit_token, "HEALTHKIT_BRIDGE_TOKEN");
        set(&mut out.carekit_bridge_id, "CAREKIT_BRIDGE_ID");
        set(
            &mut out.carekit_mapping,
            "CAREKIT_DEFAULT_SOURCE_MAPPING_ID",
        );
        set(&mut out.carekit_token, "CAREKIT_BRIDGE_TOKEN");
        set(&mut out.localai_base_url, "LOCAL_AI_API_URL");
        set(&mut out.localai_machines_dir, "LOCAL_AI_MACHINES_DIR");
        for url in [&mut out.ollama_base_url, &mut out.openai_base_url] {
            while url.ends_with('/') {
                url.pop();
            }
        }
        out
    }

    /// C++ `load_integration_registry`.
    fn load(&mut self, path: &str) {
        self.path = Some(path.to_owned());
        let document = std::fs::read_to_string(path)
            .map_err(|_| format!("unable to open integrations config: {path}"))
            .and_then(|text| {
                serde_json::from_str::<Value>(&text).map_err(|error| error.to_string())
            });
        let config = match document {
            Ok(config) => config,
            Err(error) => {
                eprintln!("perception-engine: integrations config failed to load: {error}");
                self.error = Some(error);
                return;
            }
        };
        for mapping in config["sourceMappings"].as_array().into_iter().flatten() {
            if let Some(id) = mapping["id"].as_str().filter(|id| !id.is_empty()) {
                self.source_mappings.insert(id.to_owned(), mapping.clone());
            }
        }
        for item in config["integrations"].as_array().into_iter().flatten() {
            let text = |key: &str| item[key].as_str().map(str::to_owned);
            let set = |field: &mut String, key: &str| {
                if let Some(value) = text(key) {
                    *field = value;
                }
            };
            match item["kind"].as_str().unwrap_or_default() {
                "ollama" => {
                    set(&mut self.ollama_base_url, "baseUrl");
                    set(&mut self.ollama_model, "model");
                    set(&mut self.ollama_mapping, "completionSourceMappingId");
                }
                "openai" => {
                    set(&mut self.openai_base_url, "baseUrl");
                    set(&mut self.openai_model, "model");
                    set(&mut self.openai_mapping, "completionSourceMappingId");
                }
                "acp" | "openclaw-acp" => {
                    self.acp_enabled = item["enabled"].as_bool().unwrap_or(self.acp_enabled);
                    set(&mut self.acp_platform, "platform");
                    set(&mut self.acp_surface, "surface");
                    set(&mut self.acp_command, "command");
                    set(&mut self.acp_gateway_url, "gatewayUrl");
                    set(&mut self.acp_session_key, "sessionKey");
                    set(&mut self.acp_target_agent, "targetAgent");
                    set(&mut self.acp_mapping, "completionSourceMappingId");
                }
                "healthkit" => {
                    set(&mut self.healthkit_bridge_id, "bridgeId");
                    set(&mut self.healthkit_mapping, "defaultSourceMappingId");
                }
                "carekit" => {
                    set(&mut self.carekit_bridge_id, "bridgeId");
                    set(&mut self.carekit_mapping, "defaultSourceMappingId");
                }
                _ => {}
            }
        }
        self.loaded = true;
        eprintln!(
            "perception-engine: integrations config loaded, path={path} sourceMappings={}",
            self.source_mappings.len()
        );
        self.config = config;
    }

    /// `allowedOperations` on the `localai` integration, verbatim, and the
    /// configuration it came from; none configured allows nothing.
    pub(crate) fn localai_allowed_operations(&self) -> (Value, Option<String>) {
        for item in self.config["integrations"].as_array().into_iter().flatten() {
            if item["kind"] != "localai" {
                continue;
            }
            return match item["allowedOperations"].as_array() {
                Some(_) => (item["allowedOperations"].clone(), self.path.clone()),
                None => (json!([]), None),
            };
        }
        (json!([]), None)
    }

    fn nullable(value: &str) -> Value {
        if value.is_empty() {
            Value::Null
        } else {
            json!(value)
        }
    }

    pub(crate) fn acp_status(&self) -> Value {
        json!({
            "enabled": self.acp_enabled,
            "platform": self.acp_platform,
            "surface": self.acp_surface,
            "adapter": "openclaw-xacp",
            "command": self.acp_command,
            "gatewayUrl": Self::nullable(&self.acp_gateway_url),
            "sessionKey": Self::nullable(&self.acp_session_key),
            "targetAgent": self.acp_target_agent,
            "completionSourceMappingId": self.acp_mapping,
            "dispatchEndpoint": "/api/integrations/acp/dispatch",
            "completionEndpoint": "/api/integrations/completions",
            "noWaitDispatch": true,
            "contract": {
                "dispatch": "Record an ACP/OpenClaw handoff receipt only; do not run or wait for the harness in the PE cycle.",
                "completion": "External ACP/OpenClaw adapters commit finished results through /api/integrations/completions.",
            },
        })
    }

    fn status(&self) -> Value {
        let integrations: Vec<Value> = self.config["integrations"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|item| {
                json!({
                    "id": item["id"].as_str().unwrap_or_default(),
                    "kind": item["kind"].as_str().unwrap_or_default(),
                    "enabled": item["enabled"].as_bool().unwrap_or(false),
                })
            })
            .collect();
        let mappings: Vec<Value> = self
            .source_mappings
            .iter()
            .map(|(id, m)| {
                json!({
                    "id": id,
                    "sensorId": m["sensorId"].as_str().unwrap_or_default(),
                    "sensorIdTemplate": m["sensorIdTemplate"].as_str().unwrap_or_default(),
                    "region": m.get("region").filter(|r| r.is_object()).cloned().unwrap_or(Value::Null),
                    "ttlMs": m.get("ttlMs").filter(|t| t.is_number()).cloned().unwrap_or(Value::Null),
                })
            })
            .collect();
        json!({
            "loaded": self.loaded,
            "path": self.path,
            "error": self.error,
            "integrationCount": integrations.len(),
            "sourceMappingCount": self.source_mappings.len(),
            "integrations": integrations,
            "sourceMappings": mappings,
            "completionEndpoint": "/api/integrations/completions",
            "ollama": {
                "baseUrl": self.ollama_base_url,
                "model": self.ollama_model,
                "completionSourceMappingId": self.ollama_mapping,
                "statusEndpoint": "/api/integrations/ollama/status",
                "dispatchEndpoint": "/api/integrations/ollama/dispatch",
            },
            "openai": {
                "baseUrl": self.openai_base_url,
                "model": self.openai_model,
                "hasApiKey": !self.openai_api_key.is_empty(),
                "completionSourceMappingId": self.openai_mapping,
                "statusEndpoint": "/api/integrations/openai/status",
                "dispatchEndpoint": "/api/integrations/openai/dispatch",
            },
            "acp": self.acp_status(),
            "healthkit": {
                "bridgeId": self.healthkit_bridge_id,
                "tokenConfigured": !self.healthkit_token.is_empty(),
                "registryKey": "healthkit:<typeIdentifier>",
                "statusEndpoint": "/api/integrations/healthkit/status",
                "ingestEndpoint": "/api/integrations/healthkit/ingest",
            },
            "carekit": {
                "bridgeId": self.carekit_bridge_id,
                "defaultSourceMappingId": self.carekit_mapping,
                "tokenConfigured": !self.carekit_token.is_empty(),
                "statusEndpoint": "/api/integrations/carekit/status",
                "ingestEndpoint": "/api/integrations/carekit/ingest",
            },
        })
    }

    fn healthkit_status(&self) -> Value {
        json!({
            "bridgeId": self.healthkit_bridge_id,
            "enabled": true,
            "tokenConfigured": !self.healthkit_token.is_empty(),
            "nativeAppRequired": true,
            "nativeWorkOutsideRepo": true,
            "registryKey": "healthkit:<typeIdentifier>",
            "statusEndpoint": "/api/integrations/healthkit/status",
            "ingestEndpoint": "/api/integrations/healthkit/ingest",
            "contract": {
                "transport": "https",
                "singleSample": ["type", "value", "sourceName"],
                "batchSamples": ["bridgeId", "samples[]"],
                "auth": if self.healthkit_token.is_empty() { "none" } else { "bridgeToken|bearer" },
            },
            "scope": { "declared": false, "generation": 0, "types": {}, "resyncRequests": [] },
        })
    }

    fn carekit_status(&self) -> Value {
        json!({
            "bridgeId": self.carekit_bridge_id,
            "enabled": true,
            "defaultSourceMappingId": self.carekit_mapping,
            "tokenConfigured": !self.carekit_token.is_empty(),
            "nativeAppRequired": true,
            "nativeWorkOutsideRepo": true,
            "registryKey": "carekit:<sampleType>",
            "statusEndpoint": "/api/integrations/carekit/status",
            "ingestEndpoint": "/api/integrations/carekit/ingest",
            "contract": {
                "transport": "https",
                "singleSample": ["bridgeId", "sampleType", "sourceMappingId", "values"],
                "batchSamples": ["bridgeId", "samples[]"],
                "auth": if self.carekit_token.is_empty() { "external-transport" } else { "bridgeToken" },
            },
        })
    }
}

/// `GET url` as JSON, the body whatever the status.
async fn fetch_json(url: &str, headers: &[(&str, String)]) -> Result<Value, String> {
    let bytes = client::request(Method::GET, url, headers, None).await?;
    serde_json::from_slice(&bytes).map_err(|error| format!("GET {url}: {error}"))
}

fn integrations(pe: &SharedPe) -> Result<Arc<Integrations>, (StatusCode, Json<Value>)> {
    Ok(Arc::clone(&read(pe)?.integrations))
}

type Answer = Result<Json<Value>, (StatusCode, Json<Value>)>;

pub(crate) async fn status(State(pe): State<SharedPe>) -> Answer {
    Ok(Json(integrations(&pe)?.status()))
}

pub(crate) async fn acp_status(State(pe): State<SharedPe>) -> Answer {
    Ok(Json(integrations(&pe)?.acp_status()))
}

pub(crate) async fn healthkit_status(State(pe): State<SharedPe>) -> Answer {
    Ok(Json(integrations(&pe)?.healthkit_status()))
}

pub(crate) async fn carekit_status(State(pe): State<SharedPe>) -> Answer {
    Ok(Json(integrations(&pe)?.carekit_status()))
}

pub(crate) async fn ollama_status(State(pe): State<SharedPe>) -> Answer {
    let config = integrations(&pe)?;
    let (reachable, tags, error) =
        match fetch_json(&format!("{}/api/tags", config.ollama_base_url), &[]).await {
            Ok(tags) => (true, tags, Value::Null),
            Err(error) => (false, Value::Null, json!(error)),
        };
    Ok(Json(json!({
        "baseUrl": config.ollama_base_url,
        "model": config.ollama_model,
        "completionSourceMappingId": config.ollama_mapping,
        "reachable": reachable,
        "tags": tags,
        "error": error,
        "dispatchEndpoint": "/api/integrations/ollama/dispatch",
    })))
}

pub(crate) async fn openai_status(State(pe): State<SharedPe>) -> Answer {
    let config = integrations(&pe)?;
    let (reachable, models, error) = if config.openai_api_key.is_empty() {
        (
            false,
            Value::Null,
            json!("OPENAI_API_KEY is not configured"),
        )
    } else {
        let headers = [("Authorization", format!("Bearer {}", config.openai_api_key))];
        match fetch_json(&format!("{}/models", config.openai_base_url), &headers).await {
            Ok(models) if models["error"].is_object() => {
                (false, models.clone(), models["error"].clone())
            }
            Ok(models) => (true, models, Value::Null),
            Err(error) => (false, Value::Null, json!(error)),
        }
    };
    Ok(Json(json!({
        "baseUrl": config.openai_base_url,
        "model": config.openai_model,
        "hasApiKey": !config.openai_api_key.is_empty(),
        "completionSourceMappingId": config.openai_mapping,
        "reachable": reachable,
        "models": models,
        "error": error,
        "dispatchEndpoint": "/api/integrations/openai/dispatch",
    })))
}

pub(crate) async fn localai_status(State(pe): State<SharedPe>) -> Answer {
    let config = integrations(&pe)?;
    let base = &config.localai_base_url;
    // As C++: a root that answered is kept when the health probe then fails.
    let (reachable, root, health) = match fetch_json(&format!("{base}/"), &[]).await {
        Ok(root) => match fetch_json(&format!("{base}/health"), &[]).await {
            Ok(health) => (true, root, health),
            Err(error) => (false, root, json!({ "error": error })),
        },
        Err(error) => (false, Value::Null, json!({ "error": error })),
    };
    let state = read(&pe)?;
    let sensors: Vec<Value> = LOCALAI_SENSORS
        .iter()
        .map(|(sensor_id, name, offset, length)| {
            json!({
                "sensorId": sensor_id,
                "name": name,
                "region": { "offset": offset, "length": length },
                "registered": state.has_sensor(sensor_id),
            })
        })
        .collect();
    Ok(Json(json!({
        "localAIBaseUrl": base,
        "reachable": reachable,
        "root": root,
        "health": health,
        "sensors": sensors,
        "machineDirectory": config.localai_machines_dir,
    })))
}
