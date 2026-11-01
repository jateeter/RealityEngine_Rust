//! The MQTT bridge (#20, the last PE workstream): `GET /api/mqtt/status`,
//! `GET`/`PUT /api/mqtt/mappings`, `POST /api/mqtt/enable` and
//! `POST /api/mqtt/disable`.
//!
//! C++ `MqttBridge` and its routes are the concrete form; SURFACE_SPEC lists
//! the routes 3-of-3 without shapes. The bridge subscribes to every filter
//! its mapping registry declares and fans each PUBLISH out to every rule
//! that matches. A decoded message becomes a value for its sensor through
//! the same path as `POST /api/sensors/:id` (a new sensor is registered
//! `mqtt:<topic>`, live from that value), announced as `mqtt-ingest`. Push
//! policy across a fan-out: any `immediate` rule pushes once, at once;
//! otherwise the smallest `debounced` window schedules one push after the
//! last message; `manual` never pushes. A push is the async push of
//! `POST /api/push`, so it coalesces with one in flight.
//!
//! The bridge starts at boot from `MQTT_*` (`from_environment`) only when a
//! registry resolves; `enable` starts it from a request, `PUT mappings`
//! replaces its registry on the same broker settings (409 without one), and
//! `disable` stops it. Every replacement starts fresh counters, as in C++.

mod client;
mod mapping;

use super::*;
use mapping::{PushMode, Registry};
use sources::now;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Mutex, Weak};
use std::time::Duration;
use tokio::sync::{Notify, watch};

#[derive(Default, Clone, Copy)]
struct BridgeStats {
    messages_mapped: u64,
    messages_rejected: u64,
    messages_unmatched: u64,
    pushes_triggered: u64,
}

pub(crate) struct Bridge {
    client: client::Client,
    registry: Mutex<Registry>,
    stats: Mutex<BridgeStats>,
    next_push_at: AtomicI64,
    wake: Notify,
    stop: watch::Sender<bool>,
    pe: Weak<RwLock<PerceptionEngine>>,
}

impl Bridge {
    /// A bridge over `registry`, subscribed to each of its filters once at
    /// the highest QoS any rule declares for it. Not yet started.
    fn new(config: client::Config, registry: Registry, pe: &SharedPe) -> Arc<Self> {
        let mut filters: BTreeMap<String, i64> = BTreeMap::new();
        for rule in &registry.rules {
            let qos = filters.entry(rule.topic_filter.clone()).or_insert(rule.qos);
            *qos = (*qos).max(rule.qos);
        }
        Arc::new(Self {
            client: client::Client::new(config, filters.into_iter().collect()),
            registry: Mutex::new(registry),
            stats: Mutex::default(),
            next_push_at: AtomicI64::new(0),
            wake: Notify::new(),
            stop: watch::channel(false).0,
            pe: Arc::downgrade(pe),
        })
    }

    fn start(self: &Arc<Self>) {
        let bridge = Arc::clone(self);
        let stop = self.stop.subscribe();
        tokio::spawn(async move {
            let handler = {
                let bridge = Arc::clone(&bridge);
                Box::new(move |topic: String, payload: Vec<u8>| bridge.on_message(&topic, &payload))
            };
            bridge.client.run(stop, handler).await;
        });
        tokio::spawn(Arc::clone(self).debounce_loop());
    }

    fn stop(&self) {
        let _ = self.stop.send(true);
        self.wake.notify_one();
    }

    fn stopped(&self) -> bool {
        *self.stop.borrow()
    }

    fn count(&self, update: impl FnOnce(&mut BridgeStats)) {
        if let Ok(mut stats) = self.stats.lock() {
            update(&mut stats);
        }
    }

    fn rule_count(&self) -> usize {
        self.registry.lock().map(|r| r.len()).unwrap_or(0)
    }

    fn on_message(&self, topic: &str, payload: &[u8]) {
        let Some(pe) = self.pe.upgrade() else {
            return;
        };
        let mut immediate = false;
        let mut debounce: Option<i64> = None;
        let mut delivered = Vec::new();
        {
            let Ok(mut registry) = self.registry.lock() else {
                return;
            };
            let matches = registry.match_all(topic);
            if matches.is_empty() {
                self.count(|s| s.messages_unmatched += 1);
                return;
            }
            for (index, captures) in matches {
                let at = now();
                registry.metrics[index].received += 1;
                registry.metrics[index].last_message_at_ms = at;
                let rule = &registry.rules[index];
                match Registry::decode(rule, payload) {
                    Err(error) => {
                        let metrics = &mut registry.metrics[index];
                        metrics.last_error = format!("[{topic}] {error}");
                        metrics.last_error_at_ms = now();
                        metrics.rejected += 1;
                        self.count(|s| s.messages_rejected += 1);
                    }
                    Ok(values) => {
                        let sensor_id = Registry::resolve_sensor_id(rule, topic, &captures);
                        delivered.push((
                            sensor_id,
                            rule.offset,
                            rule.length,
                            values,
                            rule.ttl_ms,
                            rule.id.clone(),
                        ));
                        match rule.push_mode {
                            PushMode::Immediate => immediate = true,
                            PushMode::Debounced => {
                                debounce = Some(
                                    debounce.map_or(rule.debounce_ms, |d| d.min(rule.debounce_ms)),
                                );
                            }
                            PushMode::Manual => {}
                        }
                        registry.metrics[index].mapped += 1;
                        self.count(|s| s.messages_mapped += 1);
                    }
                }
            }
        }
        for (sensor_id, offset, length, values, ttl_ms, mapping_id) in delivered {
            if let Ok(mut state) = pe.write() {
                state.feed_mqtt(
                    &sensor_id,
                    offset,
                    length,
                    values,
                    ttl_ms,
                    topic,
                    &mapping_id,
                );
            }
        }
        if immediate {
            trigger_push(&pe);
            self.count(|s| s.pushes_triggered += 1);
        } else if let Some(window) = debounce {
            self.schedule_push(window);
        }
    }

    /// One push `window` ms after the last message; a later deadline
    /// already set stands.
    fn schedule_push(&self, window: i64) {
        let target = now() + window;
        let mut current = self.next_push_at.load(Ordering::SeqCst);
        while current < target {
            match self.next_push_at.compare_exchange_weak(
                current,
                target,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    self.wake.notify_one();
                    return;
                }
                Err(seen) => current = seen,
            }
        }
    }

    async fn debounce_loop(self: Arc<Self>) {
        while !self.stopped() {
            let target = self.next_push_at.load(Ordering::SeqCst);
            let wait = if target == 0 { 200 } else { target - now() };
            if target == 0 || wait > 0 {
                tokio::select! {
                    () = self.wake.notified() => {}
                    () = tokio::time::sleep(Duration::from_millis(wait.max(0) as u64)) => {}
                }
                continue;
            }
            if self.next_push_at.load(Ordering::SeqCst) > target {
                continue;
            }
            self.next_push_at.store(0, Ordering::SeqCst);
            let Some(pe) = self.pe.upgrade() else {
                return;
            };
            trigger_push(&pe);
            self.count(|s| s.pushes_triggered += 1);
        }
    }

    fn status(&self) -> Value {
        let client = self.client.stats();
        let bridge = self.stats.lock().map(|s| *s).unwrap_or_default();
        let config = &self.client.config;
        json!({
            "enabled": true,
            "connected": self.client.is_connected(),
            "brokerHost": config.host,
            "brokerPort": config.port,
            "clientId": config.client_id,
            "connectAttempts": client.connect_attempts,
            "connectSuccesses": client.connect_successes,
            "messagesReceived": client.messages_received,
            "bytesReceived": client.bytes_received,
            "pingsSent": client.pings_sent,
            "reconnects": client.reconnects,
            "lastMessageAtMs": client.last_message_at_ms,
            "bridge": {
                "messagesMapped": bridge.messages_mapped,
                "messagesRejected": bridge.messages_rejected,
                "messagesUnmatched": bridge.messages_unmatched,
                "pushesTriggered": bridge.pushes_triggered,
            },
        })
    }
}

/// C++ `do_push(false, true)`: an async push, which coalesces with one in
/// flight.
fn trigger_push(pe: &SharedPe) {
    let pe = Arc::clone(pe);
    tokio::spawn(async move {
        push::push_json(&pe, false, true, Value::Null).await;
    });
}

impl PerceptionEngine {
    /// C++ `feed_mqtt_signal`: the value for the sensor `sensor_id`, or a
    /// sensor registered for it (`mqtt:<topic>`, live from the value), and
    /// the `mqtt-ingest` announcement.
    #[allow(clippy::too_many_arguments)]
    fn feed_mqtt(
        &mut self,
        sensor_id: &str,
        offset: i64,
        length: i64,
        values: Vec<f64>,
        ttl_ms: i64,
        topic: &str,
        mapping_id: &str,
    ) {
        if values.is_empty() || values.len() as i64 != length {
            return;
        }
        let source = if self.update_sensor(sensor_id, values.clone()) {
            self.sensor_by_id(sensor_id)
        } else {
            Some(self.add_source(sources::Source {
                id: String::new(),
                name: format!("mqtt:{topic}"),
                region: sources::Region { offset, length },
                active: false,
                activated_at: 0,
                detail: sources::Detail::Sensor {
                    sensor_id: sensor_id.to_owned(),
                    last_value: values,
                    last_updated: Some(now()),
                    ttl_ms: if ttl_ms > 0 { ttl_ms } else { 30000 },
                    origin: "mqtt".to_owned(),
                },
            }))
        };
        self.broadcast(&json!({
            "type": "mqtt-ingest",
            "topic": topic,
            "mappingId": mapping_id,
            "sensorId": sensor_id,
            "source": source.map(|s| s.to_json(now())),
            "timestamp": now_ms(),
        }));
    }
}

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

/// C++ `truthy_env`: set and not `0`, `false`, `no` or `off`.
fn truthy(value: Option<String>) -> bool {
    value.is_some_and(|v| !matches!(v.to_lowercase().as_str(), "0" | "false" | "no" | "off"))
}

/// `host[:port]` after an optional scheme; a port `stoi` cannot read is the
/// default.
fn host_and_port(url: &str, default_port: i64) -> (String, i64) {
    let authority = url.find("://").map_or(url, |at| &url[at + 3..]);
    match authority.rfind(':') {
        Some(colon) => (
            authority[..colon].to_owned(),
            mapping::leading_int(&authority[colon + 1..]).unwrap_or(default_port),
        ),
        None => (authority.to_owned(), default_port),
    }
}

/// The broker C++ defaults to when nothing names one; the bridge still
/// starts only when mappings resolve.
const DEFAULT_BROKER_HOST: &str = "yuma.lateraledge.cloud";

/// C++ `MqttBridge::from_environment`: the broker settings and registry,
/// or none (`MQTT_DISABLED`, `MQTT_BROKER_HOST=""`, or no mappings).
fn from_environment() -> Option<(client::Config, Registry)> {
    if truthy(env("MQTT_DISABLED")) {
        return None;
    }
    let (host, url_port) = match std::env::var("MQTT_BROKER_HOST") {
        Ok(host) if host.is_empty() => return None,
        Ok(host) => (host, 1883),
        Err(_) => match env("MQTT_BROKER_URL") {
            Some(url) => host_and_port(&url, 1883),
            None => (DEFAULT_BROKER_HOST.to_owned(), 1883),
        },
    };
    let atoi = |value: String| mapping::leading_int(&value).unwrap_or(0);
    let mut config = client::Config {
        host: host.clone(),
        port: env("MQTT_BROKER_PORT").map_or(url_port, atoi),
        ..client::Config::default()
    };
    if let Some(id) = env("MQTT_CLIENT_ID") {
        config.client_id = id;
    }
    config.username = env("MQTT_USERNAME");
    config.password = env("MQTT_PASSWORD");
    if let Some(keepalive) = env("MQTT_KEEPALIVE") {
        config.keepalive_sec = atoi(keepalive);
    }
    let mut registry = None;
    if let Some(path) = env("MQTT_MAPPINGS_FILE") {
        let loaded = std::fs::read_to_string(&path)
            .map_err(|_| format!("mqtt-mappings: cannot open {path}"))
            .and_then(|text| serde_json::from_str::<Value>(&text).map_err(|e| e.to_string()))
            .and_then(|root| Registry::from_json(&root));
        match loaded {
            Ok(loaded) => registry = Some(loaded),
            Err(error) => {
                eprintln!("MQTT mappings file error ({path}): {error} — trying JSON fallback")
            }
        }
    }
    if registry.is_none() {
        let parsed = |raw: String, legacy: bool| {
            serde_json::from_str::<Value>(&raw)
                .map_err(|e| e.to_string())
                .and_then(|root| {
                    if legacy {
                        Registry::from_legacy(&root)
                    } else {
                        Registry::from_json(&root)
                    }
                })
        };
        if let Some(raw) = env("MQTT_MAPPINGS_JSON") {
            match parsed(raw, false) {
                Ok(loaded) => registry = Some(loaded),
                Err(error) => {
                    eprintln!("MQTT mappings JSON error: {error} — bridge disabled");
                    return None;
                }
            }
        } else if let Some(raw) = env("MQTT_TOPIC_MAP") {
            match parsed(raw, true) {
                Ok(loaded) => registry = Some(loaded),
                Err(error) => {
                    eprintln!("MQTT legacy topic map error: {error} — bridge disabled");
                    return None;
                }
            }
        }
    }
    let Some(registry) = registry.filter(|r| r.len() > 0) else {
        eprintln!("MQTT broker {host} configured but no mappings resolved — bridge disabled");
        return None;
    };
    for warning in registry.validate_overlaps(truthy(env("MQTT_ALLOW_REGION_OVERLAP"))) {
        eprintln!("MQTT mapping warning: {warning}");
    }
    Some((config, registry))
}

/// At boot: the bridge `MQTT_*` configures, started, if any.
pub(crate) fn start_from_environment(pe: &SharedPe) {
    let Some((config, registry)) = from_environment() else {
        return;
    };
    eprintln!(
        "MQTT bridge enabled — broker={}:{}, mappings={}",
        config.host,
        config.port,
        registry.len()
    );
    let bridge = Bridge::new(config, registry, pe);
    bridge.start();
    if let Ok(mut state) = pe.write() {
        state.mqtt = Some(bridge);
    }
}

/// The routes' overlap switch: exactly `1` or `true`.
fn overlap_allowed() -> bool {
    matches!(
        std::env::var("MQTT_ALLOW_REGION_OVERLAP").as_deref(),
        Ok("1" | "true")
    )
}

fn registry_from(root: &Value) -> Result<Registry, (StatusCode, Json<Value>)> {
    let registry = Registry::from_json(root)
        .map_err(|error| error_response(EngineError::bad_request(format!("schema: {error}"))))?;
    if registry.len() == 0 {
        return Err(error_response(EngineError::bad_request(
            "mappings array is empty — at least one rule is required",
        )));
    }
    Ok(registry)
}

/// Replace the PE's bridge (stopping the old) with one over `registry`,
/// started: the answer `enable` and `PUT mappings` give.
fn install(pe: &SharedPe, config: client::Config, registry: Registry) -> Answer {
    let warnings = registry.validate_overlaps(overlap_allowed());
    let bridge = Bridge::new(config, registry, pe);
    {
        let mut state = write(pe)?;
        if let Some(old) = state.mqtt.take() {
            old.stop();
        }
        state.mqtt = Some(Arc::clone(&bridge));
    }
    bridge.start();
    Ok(Json(json!({
        "success": true,
        "enabled": true,
        "mappings": bridge.rule_count(),
        "warnings": warnings,
    })))
}

pub(crate) async fn status(State(pe): State<SharedPe>) -> Answer {
    let bridge = read(&pe)?.mqtt.clone();
    Ok(Json(bridge.map_or_else(
        || json!({ "enabled": false }),
        |b| b.status(),
    )))
}

pub(crate) async fn mappings(State(pe): State<SharedPe>) -> Answer {
    let bridge = read(&pe)?.mqtt.clone();
    let Some(bridge) = bridge else {
        return Ok(Json(json!({ "enabled": false, "mappings": [] })));
    };
    let mut out = bridge
        .registry
        .lock()
        .map_err(|_| error_response(lock_error()))?
        .to_json();
    out["enabled"] = json!(true);
    Ok(Json(out))
}

/// `PUT /api/mqtt/mappings`: a new registry on the running bridge's broker
/// settings, announced as `mqtt-mappings-reloaded`.
pub(crate) async fn replace_mappings(State(pe): State<SharedPe>, body: Bytes) -> Answer {
    let Some(current) = read(&pe)?.mqtt.clone() else {
        return Err(error_response(EngineError {
            status: StatusCode::CONFLICT,
            message:
                "no broker config — set MQTT_BROKER_HOST at PE startup before reloading mappings"
                    .to_owned(),
        }));
    };
    let registry = registry_from(&body_of(&body)?)?;
    let answer = install(&pe, current.client.config.clone(), registry)?;
    read(&pe)?.broadcast(&json!({
        "type": "mqtt-mappings-reloaded",
        "mappings": answer.0["mappings"],
        "timestamp": now_ms(),
    }));
    Ok(answer)
}

/// `POST /api/mqtt/enable`: `{brokerUrl, mappings: {mappings: [...]},
/// username?, password?}`, keeping a running bridge's client id.
pub(crate) async fn enable(State(pe): State<SharedPe>, body: Bytes) -> Answer {
    let body = body_of(&body)?;
    let url = body["brokerUrl"].as_str().unwrap_or_default();
    if url.is_empty() {
        return Err(error_response(EngineError::bad_request(
            "brokerUrl is required",
        )));
    }
    let (host, port) = host_and_port(url, 1883);
    let registry = registry_from(&body["mappings"])?;
    let client_id = read(&pe)?.mqtt.as_ref().map_or_else(
        || "reality-engine-pe".to_owned(),
        |b| b.client.config.client_id.clone(),
    );
    let nonempty = |key: &str| {
        body[key]
            .as_str()
            .filter(|v| !v.is_empty())
            .map(str::to_owned)
    };
    let config = client::Config {
        host: host.clone(),
        port,
        client_id,
        username: nonempty("username"),
        password: nonempty("password"),
        ..client::Config::default()
    };
    let answer = install(&pe, config, registry)?;
    eprintln!("MQTT bridge enabled via API — broker={host}:{port}");
    Ok(answer)
}

pub(crate) async fn disable(State(pe): State<SharedPe>) -> Answer {
    if let Some(bridge) = write(&pe)?.mqtt.take() {
        bridge.stop();
        eprintln!("MQTT bridge disabled via API");
    }
    Ok(Json(json!({ "success": true, "enabled": false })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{Body, to_bytes};
    use tower::ServiceExt;

    async fn send(
        app: &Router,
        method: &str,
        uri: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let request = axum::http::Request::builder()
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

    /// Enable with a broker that is not there, deliver messages as the
    /// client would, and read them back: a value lands on a sensor
    /// registered `mqtt:<topic>`, a bad one is counted against its rule, an
    /// unmatched topic against the bridge. PUT replaces the registry with
    /// fresh counters; disable stops it; PUT then has no broker (409).
    #[tokio::test]
    async fn the_bridge_maps_messages_onto_sensors() {
        let pe = Arc::new(RwLock::new(PerceptionEngine::new(8)));
        let app = router(Arc::clone(&pe));
        let registry = json!({ "mappings": [
            {"id": "temp", "topicFilter": "farm/+/temp", "sensorIdTemplate": "zone.{1}.temp",
             "region": {"offset": 2, "length": 1}, "normalize": {"mode": "minmax", "min": 0, "max": 40},
             "pushMode": "manual"},
        ]});
        let (status, body) = send(
            &app,
            "POST",
            "/api/mqtt/enable",
            Some(json!({"brokerUrl": "mqtt://127.0.0.1:1"})),
        )
        .await;
        assert_eq!(
            (status, &body["error"]),
            (
                StatusCode::BAD_REQUEST,
                &json!("schema: mqtt-mappings: missing top-level \"mappings\" array")
            )
        );
        let (status, body) = send(
            &app,
            "POST",
            "/api/mqtt/enable",
            Some(json!({"brokerUrl": "mqtt://127.0.0.1:1", "mappings": registry})),
        )
        .await;
        assert_eq!(
            (status, body),
            (
                StatusCode::OK,
                json!({"success": true, "enabled": true, "mappings": 1, "warnings": []})
            )
        );

        let bridge = pe.read().unwrap().mqtt.clone().unwrap();
        bridge.on_message("farm/north/temp", b"10");
        bridge.on_message("farm/north/temp", b"1,2");
        bridge.on_message("barn/temp", b"1");
        let (_, sources) = send(&app, "GET", "/api/sources", None).await;
        let sensor = &sources["sources"][0];
        assert_eq!(
            (&sensor["name"], &sensor["sensorId"], &sensor["lastValue"]),
            (
                &json!("mqtt:farm/north/temp"),
                &json!("zone.north.temp"),
                &json!([0.25])
            )
        );
        let (_, mappings) = send(&app, "GET", "/api/mqtt/mappings", None).await;
        let counters = &mappings["mappings"][0]["counters"];
        assert_eq!(
            (
                &counters["received"],
                &counters["mapped"],
                &counters["rejected"]
            ),
            (&json!(2), &json!(1), &json!(1))
        );
        assert_eq!(
            counters["lastError"],
            json!("[farm/north/temp] transformed value count 2 != region.length 1")
        );
        let (_, status) = send(&app, "GET", "/api/mqtt/status", None).await;
        assert_eq!(
            (&status["brokerPort"], &status["connected"]),
            (&json!(1), &json!(false))
        );
        assert_eq!(
            status["bridge"],
            json!({"messagesMapped": 1, "messagesRejected": 1, "messagesUnmatched": 1, "pushesTriggered": 0})
        );

        let (status, body) = send(&app, "PUT", "/api/mqtt/mappings", Some(registry.clone())).await;
        assert_eq!((status, &body["mappings"]), (StatusCode::OK, &json!(1)));
        let (_, mappings) = send(&app, "GET", "/api/mqtt/mappings", None).await;
        assert_eq!(mappings["mappings"][0]["counters"]["received"], json!(0));
        let (status, body) = send(
            &app,
            "PUT",
            "/api/mqtt/mappings",
            Some(json!({"mappings": []})),
        )
        .await;
        assert_eq!(
            (status, &body["error"]),
            (
                StatusCode::BAD_REQUEST,
                &json!("mappings array is empty — at least one rule is required")
            )
        );

        assert_eq!(
            send(&app, "POST", "/api/mqtt/disable", None).await.1,
            json!({"success": true, "enabled": false})
        );
        assert_eq!(
            send(&app, "GET", "/api/mqtt/mappings", None).await.1,
            json!({"enabled": false, "mappings": []})
        );
        assert_eq!(
            send(&app, "PUT", "/api/mqtt/mappings", Some(registry))
                .await
                .0,
            StatusCode::CONFLICT
        );
        assert!(bridge.stopped());
    }
}
