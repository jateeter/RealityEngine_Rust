//! Integrations (#20 workstream 4), slice C: the dispatch adapters
//! `POST /api/integrations/ollama/dispatch`, `/openai/dispatch` and
//! `/acp/dispatch`.
//!
//! C++ `dispatch_ollama`, `dispatch_openai` and `dispatch_acp` are the
//! concrete form this follows. Each takes a dispatch record (`dispatchId`,
//! else `id`) and delivers its trigger envelope:
//!
//! - **Ollama:** `POST {base}/api/chat`. The reply's content is parsed as JSON
//!   and, with a completion source mapping and `commitCompletion` (default
//!   on), its values are committed through `POST /api/integrations/completions`'
//!   own path. `format` is sent, or the completion contract is put in the
//!   system prompt, by the model's family (`docs/OLLAMA_INTEGRATION.md`,
//!   "Structured output").
//! - **OpenAI:** `POST {base}/responses`, with the bearer key when one is set,
//!   constrained by a JSON schema built from the mapping; then the same
//!   commit. Its provider
//!   is the local LLM: by default localAIStack's Ollama at
//!   `http://localhost:11434/v1`, the key optional (owner ruling, 2026-11-01).
//! - **ACP:** no call at all. The PE records an OpenClaw handoff receipt and
//!   answers 202; the harness commits its own completion later.
//!
//! The record moves `delivering` -> `delivered`, or `failed` with the error,
//! and a failure answers 502. The prompt sent to a model is the C++ text,
//! with the envelope serialized as C++ serializes it, so the same model sees
//! the same input from either engine.

use super::*;
use axum::http::Method;
use dispatch::mint;
use integrations::Integrations;

const OLLAMA_SYSTEM: &str = "You are a local PE-controlled Ollama adapter. Analyze the trigger envelope and respond as JSON. When committing a PE completion, include a numeric values array matching the configured source mapping.";
const OPENAI_INSTRUCTIONS: &str = "You are a PE-controlled OpenAI adapter. Analyze the trigger envelope and respond as JSON. When committing a PE completion, include a numeric values array matching the configured source mapping.";
const REVIEW_PROMPT: &str = "Review this RealityEngine trigger envelope and produce a concise JSON result for PE completion ingest.";
const ACP_PROMPT: &str = "Handle this RealityEngine trigger envelope through the configured OpenClaw ACP session and return a PE completion values array.";

/// Models Ollama refuses `format` for, by family; the same list as every
/// other PE's.
const NO_STRUCTURED_OUTPUT: [&str; 1] = ["nemotron-3-nano"];

/// C++ `as_string(fallback)`: the string, else `fallback`.
fn text_or(value: &Value, key: &str, fallback: &str) -> String {
    value[key].as_str().unwrap_or(fallback).to_owned()
}

/// C++ `as_bool(fallback)`: the boolean, else `fallback`.
fn flag(value: &Value, key: &str, fallback: bool) -> bool {
    value[key].as_bool().unwrap_or(fallback)
}

/// C++ `json::stringify`, as text.
fn stringify(value: &Value) -> String {
    ecmascript_json(value)
        .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
        .unwrap_or_default()
}

fn parse_or_null(raw: &str) -> Value {
    serde_json::from_str(raw).unwrap_or(Value::Null)
}

// --- Completion values -----------------------------------------------------

/// A JSON pointer as C++ `eval_json_pointer` resolves it.
fn pointer<'a>(doc: &'a Value, pointer: &str) -> Option<&'a Value> {
    if pointer.is_empty() {
        return Some(doc);
    }
    let rest = pointer.strip_prefix('/')?;
    let mut cursor = doc;
    for raw in rest.split('/') {
        let token = raw.replace("~1", "/").replace("~0", "~");
        cursor = match cursor {
            Value::Object(object) => object.get(&token)?,
            Value::Array(items) => {
                if token.is_empty() || !token.bytes().all(|b| b.is_ascii_digit()) {
                    return None;
                }
                items.get(token.parse::<usize>().ok()?)?
            }
            _ => return None,
        };
    }
    Some(cursor)
}

/// The first token of `pointer`, decoded: the key a schema names for it.
fn top_level_key(pointer: &str) -> String {
    let Some(rest) = pointer.strip_prefix('/') else {
        return "value".to_owned();
    };
    let token = rest.split('/').next().unwrap_or_default();
    token.replace("~1", "/").replace("~0", "~")
}

/// C++ `scalar_to_completion_number`: a finite number, a boolean as 0 or 1,
/// or a string that is wholly a finite number.
fn completion_number(value: &Value) -> Option<f64> {
    let n = match value {
        Value::Number(n) => n.as_f64()?,
        Value::Bool(b) => return Some(if *b { 1.0 } else { 0.0 }),
        Value::String(raw) => {
            let raw = raw.trim_start();
            if raw.is_empty() {
                return None;
            }
            raw.parse::<f64>().ok()?
        }
        _ => return None,
    };
    n.is_finite().then_some(n)
}

fn validate_values(values: &Value) -> Result<Value, String> {
    let items = values
        .as_array()
        .ok_or("provider completion values must be an array")?;
    items
        .iter()
        .map(|value| {
            completion_number(value)
                .map(|n| json!(n))
                .ok_or_else(|| "provider completion value is not a finite number".to_owned())
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Value::Array)
}

/// `values`, else `completion.values`, else none.
fn response_values(content: &Value) -> Result<Option<Value>, String> {
    if content["values"].is_array() {
        return validate_values(&content["values"]).map(Some);
    }
    if content["completion"]["values"].is_array() {
        return validate_values(&content["completion"]["values"]).map(Some);
    }
    Ok(None)
}

fn no_values() -> String {
    "provider response did not include completion values".to_owned()
}

fn pointed_number(content: &Value, at: &str) -> Result<f64, String> {
    let value =
        pointer(content, at).ok_or_else(|| format!("missing required JSON pointer: {at}"))?;
    completion_number(value)
        .ok_or_else(|| format!("JSON pointer resolved to a non-finite value: {at}"))
}

/// The values a mapping's `extract` names: each pointer's number, or the
/// one pointer's array or number, else the content's own values.
fn extract_values(content: &Value, mapping: &Value) -> Result<Value, String> {
    let extract = &mapping["extract"];
    if !extract.is_object() || extract["type"].as_str() != Some("json") {
        return response_values(content)?.ok_or_else(no_values);
    }
    if let Some(pointers) = extract["pointers"].as_array() {
        return pointers
            .iter()
            .map(|item| {
                pointed_number(content, item.as_str().unwrap_or_default()).map(|n| json!(n))
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array);
    }
    let at = extract["pointer"].as_str().unwrap_or_default();
    if !at.is_empty() {
        let value =
            pointer(content, at).ok_or_else(|| format!("missing required JSON pointer: {at}"))?;
        if value.is_array() {
            return validate_values(value);
        }
        return Ok(json!([pointed_number(content, at)?]));
    }
    response_values(content)?.ok_or_else(no_values)
}

fn clamp01(value: f64) -> f64 {
    if value.is_finite() {
        value.clamp(0.0, 1.0)
    } else {
        0.0
    }
}

/// A mapping's `normalize`: `minmax`, `linear` or `passthrough`, then the
/// optional clamp to [0, 1].
fn normalize(values: Value, mapping: &Value) -> Value {
    let normalize = &mapping["normalize"];
    let (Some(items), true) = (values.as_array(), normalize.is_object()) else {
        return values;
    };
    let number = |key: &str, fallback: f64| normalize[key].as_f64().unwrap_or(fallback);
    let mode = text_or(normalize, "mode", "passthrough");
    let clamp = flag(normalize, "clamp", false);
    items
        .iter()
        .map(|item| {
            let mut n = item.as_f64().unwrap_or(0.0);
            if mode == "minmax" {
                let (min, max) = (number("min", 0.0), number("max", 0.0));
                let span = max - min;
                n = if span == 0.0 { 0.0 } else { (n - min) / span };
            } else if mode == "linear" {
                n = n * number("scale", 1.0) + number("offset", 0.0);
            }
            json!(if clamp { clamp01(n) } else { n })
        })
        .collect()
}

/// C++ `completion_values_from_content`.
fn values_from_content(content: &Value, mapping: &Value) -> Result<Value, String> {
    if content.is_null() {
        return Err("provider response content is not valid JSON".to_owned());
    }
    if mapping.is_object() {
        Ok(normalize(extract_values(content, mapping)?, mapping))
    } else {
        response_values(content)?.ok_or_else(no_values)
    }
}

/// The keys a mapping's JSON extract names, in declaration order.
fn extract_keys(mapping: &Value) -> Vec<String> {
    let extract = &mapping["extract"];
    if !mapping.is_object() || !extract.is_object() || extract["type"].as_str() != Some("json") {
        return Vec::new();
    }
    let pointers = if let Some(pointers) = extract["pointers"].as_array() {
        pointers.clone()
    } else if extract["pointer"].is_string() {
        vec![extract["pointer"].clone()]
    } else {
        Vec::new()
    };
    pointers
        .iter()
        .map(|item| {
            let key = top_level_key(item.as_str().unwrap_or_default());
            if key.is_empty() {
                "value".to_owned()
            } else {
                key
            }
        })
        .collect()
}

/// C++ `completion_schema_for_mapping`: the reply an extract's pointers
/// name, else `{values: number[]}`.
fn completion_schema(mapping: &Value) -> Value {
    let keys = extract_keys(mapping);
    if keys.is_empty() {
        return json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {"values": {"type": "array", "items": {"type": "number"}}},
            "required": ["values"],
        });
    }
    let mut properties = serde_json::Map::new();
    for key in &keys {
        properties.insert(key.clone(), json!({"type": ["number", "boolean"]}));
    }
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": properties,
        "required": keys,
    })
}

/// C++ `completion_contract_prompt`: for a model that cannot take `format`,
/// the schema's keys in words, appended to the system prompt.
fn contract_prompt(mapping: &Value) -> String {
    let mut keys: Vec<String> = Vec::new();
    for key in extract_keys(mapping) {
        if !keys.contains(&key) {
            keys.push(key);
        }
    }
    let lead = "\n\nRespond with ONLY one JSON object, with no prose and no markdown fences, that has exactly ";
    if keys.is_empty() {
        return format!(
            "{lead}this key, an array of numbers: values. For example: {{\"values\": [0]}}"
        );
    }
    let example = keys
        .iter()
        .map(|key| format!("\"{key}\": 0"))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "{lead}these keys, each a number or boolean: {}. For example: {{{example}}}",
        keys.join(", ")
    )
}

fn openai_text_format(mapping: &Value) -> Value {
    json!({
        "format": {
            "type": "json_schema",
            "name": "reality_engine_completion",
            "strict": true,
            "schema": completion_schema(mapping),
        }
    })
}

// --- The shared shape of a dispatch ----------------------------------------

/// A dispatch request resolved to its record, or the 400/404 that ends it.
struct Request {
    body: Value,
    id: String,
    record: Value,
    config: Arc<Integrations>,
}

type Refusal = (StatusCode, Json<Value>);

fn resolve(pe: &SharedPe, bytes: &Bytes, name: &str) -> Result<Request, Refusal> {
    let fail = error_response;
    let body = body_of(bytes)?;
    if !body.is_object() {
        return Err(fail(EngineError::bad_request(format!(
            "{name} dispatch body must be a JSON object"
        ))));
    }
    let id = text_or(&body, "dispatchId", &text_or(&body, "id", ""));
    if id.is_empty() {
        return Err(fail(EngineError::bad_request(format!(
            "{name} dispatch requires dispatchId"
        ))));
    }
    let state = read(pe)?;
    let record = state
        .dispatch_record(&id)
        .ok_or_else(|| fail(EngineError::not_found("Dispatch record not found")))?;
    let config = Arc::clone(&state.integrations);
    Ok(Request {
        body,
        id,
        record,
        config,
    })
}

/// The mapping `id` names, `Null` when `id` is empty, or the 404 for an
/// unknown one.
fn completion_mapping(config: &Integrations, id: &str) -> Result<Value, Refusal> {
    if id.is_empty() {
        return Ok(Value::Null);
    }
    config.source_mappings.get(id).cloned().ok_or_else(|| {
        error_response(EngineError::not_found(format!(
            "Unknown sourceMappingId \"{id}\""
        )))
    })
}

/// Move the record's delivery state, as C++ does, ignoring the outcome.
fn mark(pe: &SharedPe, id: &str, patch: Value) {
    if let Ok(mut state) = pe.write() {
        let _ = state.update_dispatch_record(id, &patch);
    }
}

/// Commit a provider's values through the completions path: the ingest's
/// reply, or why it failed.
async fn commit(
    pe: &SharedPe,
    request: &Request,
    provider: &str,
    mapping_id: &str,
    values: Value,
    completion_id: String,
    metadata: Value,
) -> Result<Value, String> {
    let record = &request.record;
    let target = record["target"].as_str().unwrap_or_default();
    let body = json!({
        "provider": provider,
        "agent": if target.is_empty() { provider } else { target },
        "sourceMappingId": mapping_id,
        "correlationId": text_or(record, "correlationId", ""),
        "envelopeId": text_or(record, "envelopeId", ""),
        "completionId": completion_id,
        "values": values,
        "metadata": metadata,
        "triggerPush": flag(&request.body, "triggerPush", false),
    });
    let (status, reply) = ingest::ingest_completion(pe, &body).await;
    if !status.is_success() {
        return Err(format!("completion ingest failed: {}", stringify(&reply)));
    }
    Ok(reply)
}

/// The values to commit: the body's own, else those in the reply's content.
fn values_to_commit(body: &Value, content: &Value, mapping: &Value) -> Result<Value, String> {
    if body["values"].is_array() {
        validate_values(&body["values"])
    } else {
        values_from_content(content, mapping)
    }
}

fn failure(
    pe: &SharedPe,
    request: &Request,
    provider: &str,
    error: &str,
    receipt: Value,
    model: &str,
) -> Response {
    let mut stored = receipt.clone();
    mark(
        pe,
        &request.id,
        json!({
            "status": "failed",
            "adapter": provider,
            "provider": provider,
            "error": error,
            "providerReceipt": receipt,
        }),
    );
    if let Some(stored) = stored.as_object_mut() {
        stored.remove("baseUrl");
    }
    (
        StatusCode::BAD_GATEWAY,
        Json(json!({
            "success": false,
            "dispatchId": request.id,
            "provider": provider,
            "model": model,
            "error": error,
            "receipt": {
                "provider": provider,
                "adapter": provider,
                "status": "failed",
                "error": error,
                "providerReceipt": stored,
            },
        })),
    )
        .into_response()
}

// --- Ollama ----------------------------------------------------------------

fn supports_structured_output(model: &str) -> bool {
    let family = model.split(':').next().unwrap_or_default();
    !NO_STRUCTURED_OUTPUT.contains(&family)
}

fn ollama_payload(record: &Value, body: &Value, model: &str, mapping: &Value) -> Value {
    if body["payload"].is_object() {
        return body["payload"].clone();
    }
    let structured = supports_structured_output(model);
    let send_format = body["format"].is_string() || flag(body, "formatJson", structured);
    let mut system = text_or(body, "systemPrompt", OLLAMA_SYSTEM);
    // No `format` to constrain the reply: say what it must contain, in the prompt.
    if !send_format {
        system += &contract_prompt(mapping);
    }
    let prompt = text_or(body, "prompt", REVIEW_PROMPT);
    let mut payload = json!({
        "model": model,
        "stream": false,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": format!("{prompt}\n\n{}", stringify(&record["envelope"]))},
        ],
    });
    if let Some(format) = body["format"].as_str() {
        payload["format"] = json!(format);
    } else if send_format {
        payload["format"] = json!("json");
    }
    // Reasoning adds nothing to filling in named fields, and made replies
    // slower and less reliable on the models that cannot take `format`.
    if !structured {
        payload["think"] = json!(false);
    }
    if body["options"].is_object() {
        payload["options"] = body["options"].clone();
    }
    payload
}

pub(crate) async fn ollama(State(pe): State<SharedPe>, bytes: Bytes) -> Response {
    let request = match resolve(&pe, &bytes, "Ollama") {
        Ok(request) => request,
        Err(refusal) => return refusal.into_response(),
    };
    let config = Arc::clone(&request.config);
    let model = text_or(&request.body, "model", &config.ollama_model);
    let mapping_id = text_or(&request.body, "sourceMappingId", &config.ollama_mapping);
    let mapping = match completion_mapping(&config, &mapping_id) {
        Ok(mapping) => mapping,
        Err(refusal) => return refusal.into_response(),
    };
    let base = &config.ollama_base_url;
    mark(
        &pe,
        &request.id,
        json!({
            "status": "delivering",
            "adapter": "ollama",
            "provider": "ollama",
            "incrementAttempts": true,
            "clearError": true,
            "providerReceipt": {"baseUrl": base, "model": model},
        }),
    );
    let outcome = async {
        let payload = ollama_payload(&request.record, &request.body, &model, &mapping);
        let raw = client::request(
            Method::POST,
            &format!("{base}/api/chat"),
            &[],
            Some(stringify(&payload)),
        )
        .await?;
        let response: Value = serde_json::from_slice(&raw).map_err(|error| error.to_string())?;
        let content = response["message"]["content"]
            .as_str()
            .or_else(|| response["response"].as_str())
            .unwrap_or_default()
            .to_owned();
        let content_json = parse_or_null(&content);
        let mut completion = Value::Null;
        let mut committed = false;
        if flag(&request.body, "commitCompletion", true) && !mapping_id.is_empty() {
            let values = values_to_commit(&request.body, &content_json, &mapping)?;
            completion = commit(
                &pe,
                &request,
                "ollama",
                &mapping_id,
                values,
                mint("ollama-completion"),
                json!({"model": model, "dispatchId": request.id, "content": content}),
            )
            .await?;
            committed = true;
        }
        Ok::<_, String>((response, content, content_json, completion, committed))
    }
    .await;
    match outcome {
        Ok((response, content, content_json, completion, committed)) => {
            let run = response["created_at"].as_str();
            mark(
                &pe,
                &request.id,
                json!({
                    "status": "delivered",
                    "adapter": "ollama",
                    "provider": "ollama",
                    "externalRunId": run.map_or_else(|| mint("ollama-run"), str::to_owned),
                    "providerReceipt": {
                        "baseUrl": base,
                        "model": model,
                        "content": content,
                        "completionCommitted": committed,
                    },
                }),
            );
            Json(json!({
                "success": true,
                "dispatchId": request.id,
                "provider": "ollama",
                "model": model,
                "response": response,
                "contentJson": content_json,
                "completionCommitted": committed,
                "completion": completion,
                "receipt": {
                    "provider": "ollama",
                    "adapter": "ollama",
                    "status": "sent",
                    "externalRunId": run.unwrap_or_default(),
                    "providerReceipt": {"model": model, "completionCommitted": committed},
                },
            }))
            .into_response()
        }
        Err(error) => failure(
            &pe,
            &request,
            "ollama",
            &error,
            json!({"baseUrl": base, "model": model}),
            &model,
        ),
    }
}

// --- OpenAI ----------------------------------------------------------------

fn openai_payload(record: &Value, body: &Value, model: &str, mapping: &Value) -> Value {
    if body["payload"].is_object() {
        return body["payload"].clone();
    }
    let instructions = text_or(body, "instructions", OPENAI_INSTRUCTIONS);
    let prompt = text_or(body, "prompt", REVIEW_PROMPT);
    let mut payload = json!({
        "model": model,
        "instructions": instructions,
        "input": format!("{prompt}\n\n{}", stringify(&record["envelope"])),
        "store": flag(body, "store", true),
        "stream": false,
        "metadata": {
            "dispatchId": text_or(record, "id", ""),
            "envelopeId": text_or(record, "envelopeId", ""),
            "correlationId": text_or(record, "correlationId", ""),
            "target": text_or(record, "target", ""),
        },
    });
    for (from, to) in [
        ("maxOutputTokens", "max_output_tokens"),
        ("temperature", "temperature"),
    ] {
        if body[from].is_number() {
            payload[to] = body[from].clone();
        }
    }
    if body["reasoning"].is_object() {
        payload["reasoning"] = body["reasoning"].clone();
    }
    payload["text"] = if body["text"].is_object() {
        body["text"].clone()
    } else if text_or(body, "responseFormatMode", "json-schema") == "json-object" {
        json!({"format": {"type": "json_object"}})
    } else {
        openai_text_format(mapping)
    };
    payload
}

/// C++ `assert_openai_response_ready`: an error, an incomplete status or a
/// refusal anywhere is a failed dispatch.
fn openai_ready(response: &Value) -> Result<(), String> {
    if response["error"].is_object() {
        return Err(stringify(&response["error"]));
    }
    let status = text_or(response, "status", "");
    if !status.is_empty() && status != "completed" {
        return Err(format!("OpenAI response did not complete: {status}"));
    }
    if let Some(refusal) = response["refusal"].as_str().filter(|r| !r.is_empty()) {
        return Err(format!("OpenAI response refused: {refusal}"));
    }
    for item in response["output"].as_array().into_iter().flatten() {
        for part in item["content"].as_array().into_iter().flatten() {
            if part["type"].as_str() == Some("refusal") {
                return Err("OpenAI response refused".to_owned());
            }
            if let Some(refusal) = part["refusal"].as_str().filter(|r| !r.is_empty()) {
                return Err(format!("OpenAI response refused: {refusal}"));
            }
        }
    }
    Ok(())
}

/// `output_text`, else every output text part, one per line.
fn openai_text(response: &Value) -> String {
    if let Some(text) = response["output_text"].as_str() {
        return text.to_owned();
    }
    let mut text = String::new();
    for item in response["output"].as_array().into_iter().flatten() {
        for part in item["content"].as_array().into_iter().flatten() {
            let kind = part["type"].as_str().unwrap_or_default();
            if let (true, Some(part)) = (
                kind == "output_text" || kind == "text",
                part["text"].as_str(),
            ) {
                if !text.is_empty() {
                    text.push('\n');
                }
                text.push_str(part);
            }
        }
    }
    text
}

pub(crate) async fn openai(State(pe): State<SharedPe>, bytes: Bytes) -> Response {
    let request = match resolve(&pe, &bytes, "OpenAI") {
        Ok(request) => request,
        Err(refusal) => return refusal.into_response(),
    };
    let config = Arc::clone(&request.config);
    let model = text_or(&request.body, "model", &config.openai_model);
    let mapping_id = text_or(&request.body, "sourceMappingId", &config.openai_mapping);
    let mapping = match completion_mapping(&config, &mapping_id) {
        Ok(mapping) => mapping,
        Err(refusal) => return refusal.into_response(),
    };
    let base = &config.openai_base_url;
    mark(
        &pe,
        &request.id,
        json!({
            "status": "delivering",
            "adapter": "openai",
            "provider": "openai",
            "incrementAttempts": true,
            "clearError": true,
            "providerReceipt": {"baseUrl": base, "model": model, "endpoint": "/responses"},
        }),
    );
    let outcome = async {
        let payload = openai_payload(&request.record, &request.body, &model, &mapping);
        // The key is optional: sent when set, never required (owner ruling,
        // 2026-11-01).
        let headers: Vec<(&str, String)> = if config.openai_api_key.is_empty() {
            Vec::new()
        } else {
            vec![("Authorization", format!("Bearer {}", config.openai_api_key))]
        };
        let raw = client::request(
            Method::POST,
            &format!("{base}/responses"),
            &headers,
            Some(stringify(&payload)),
        )
        .await?;
        let response: Value = serde_json::from_slice(&raw).map_err(|error| error.to_string())?;
        openai_ready(&response)?;
        let content = openai_text(&response);
        let content_json = parse_or_null(&content);
        let response_id = text_or(&response, "id", "");
        let mut completion = Value::Null;
        let mut committed = false;
        if flag(&request.body, "commitCompletion", true) && !mapping_id.is_empty() {
            let values = values_to_commit(&request.body, &content_json, &mapping)?;
            completion = commit(
                &pe,
                &request,
                "openai",
                &mapping_id,
                values,
                response["id"]
                    .as_str()
                    .map_or_else(|| mint("openai-completion"), str::to_owned),
                json!({
                    "model": model,
                    "dispatchId": request.id,
                    "responseId": response_id,
                    "content": content,
                }),
            )
            .await?;
            committed = true;
        }
        Ok::<_, String>((response, response_id, content_json, completion, committed))
    }
    .await;
    match outcome {
        Ok((response, response_id, content_json, completion, committed)) => {
            mark(
                &pe,
                &request.id,
                json!({
                    "status": "delivered",
                    "adapter": "openai",
                    "provider": "openai",
                    "externalRunId": response_id,
                    "providerReceipt": {
                        "baseUrl": base,
                        "model": model,
                        "responseId": response_id,
                        "status": text_or(&response, "status", ""),
                        "completionCommitted": committed,
                    },
                }),
            );
            Json(json!({
                "success": true,
                "dispatchId": request.id,
                "provider": "openai",
                "model": model,
                "response": response,
                "contentJson": content_json,
                "completionCommitted": committed,
                "completion": completion,
                "receipt": {
                    "provider": "openai",
                    "adapter": "openai",
                    "status": "sent",
                    "externalRunId": response_id,
                    "providerReceipt": {"model": model, "completionCommitted": committed},
                },
            }))
            .into_response()
        }
        Err(error) => failure(
            &pe,
            &request,
            "openai",
            &error,
            json!({"baseUrl": base, "model": model, "endpoint": "/responses"}),
            &model,
        ),
    }
}

// --- ACP -------------------------------------------------------------------

pub(crate) async fn acp(State(pe): State<SharedPe>, bytes: Bytes) -> Response {
    let request = match resolve(&pe, &bytes, "ACP") {
        Ok(request) => request,
        Err(refusal) => return refusal.into_response(),
    };
    let (body, record, config) = (&request.body, &request.record, &request.config);
    let target = record["target"].as_str().unwrap_or_default();
    let target = text_or(
        body,
        "targetAgent",
        &text_or(
            body,
            "agent",
            if target.is_empty() {
                &config.acp_target_agent
            } else {
                target
            },
        ),
    );
    let session = text_or(body, "sessionKey", &config.acp_session_key);
    let run = body["externalRunId"]
        .as_str()
        .map_or_else(|| mint("acp-handoff"), str::to_owned);
    let mut handoff = json!({
        "protocol": "ACP",
        "surface": config.acp_surface,
        "platform": config.acp_platform,
        "adapter": "openclaw-xacp",
        "command": text_or(body, "command", &config.acp_command),
        "gatewayUrl": text_or(body, "gatewayUrl", &config.acp_gateway_url),
        "sessionKey": if session.is_empty() { Value::Null } else { json!(session) },
        "targetAgent": target,
        "completionEndpoint": "/api/integrations/completions",
        "completionSourceMappingId": text_or(body, "sourceMappingId", &config.acp_mapping),
        "noWaitDispatch": true,
        "prompt": text_or(body, "prompt", ACP_PROMPT),
        "dispatchId": request.id,
        "envelopeId": text_or(record, "envelopeId", ""),
        "correlationId": text_or(record, "correlationId", ""),
    });
    if body["metadata"].is_object() {
        handoff["metadata"] = body["metadata"].clone();
    }
    mark(
        &pe,
        &request.id,
        json!({
            "status": text_or(body, "status", "accepted"),
            "adapter": "openclaw-xacp",
            "provider": "acp",
            "externalRunId": run,
            "incrementAttempts": flag(body, "incrementAttempts", true),
            "clearError": true,
            "providerReceipt": handoff,
        }),
    );
    (
        StatusCode::ACCEPTED,
        Json(json!({
            "success": true,
            "accepted": true,
            "dispatchId": request.id,
            "provider": "acp",
            "platform": config.acp_platform,
            "surface": config.acp_surface,
            "externalRunId": run,
            "noWaitDispatch": true,
            "handoff": handoff,
        })),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{Body, to_bytes};
    use tower::ServiceExt;

    async fn send(app: &Router, uri: &str, body: Value) -> (StatusCode, Value) {
        let request = axum::http::Request::post(uri)
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    async fn record(app: &Router, id: &str) -> Value {
        let request = axum::http::Request::get(format!("/api/dispatch/records/{id}"))
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice::<Value>(&bytes).unwrap()["record"].clone()
    }

    /// A model stand-in on loopback: Ollama's `/api/chat` and OpenAI's
    /// `/responses`, each answering a JSON reply for the mapping's pointers.
    async fn provider() -> String {
        let app = Router::new()
            .route(
                "/api/chat",
                post(|| async {
                    Json(json!({
                        "created_at": "2026-11-01T00:00:00Z",
                        "message": {"role": "assistant", "content": "{\"risk\": 3, \"flag\": true}"},
                    }))
                }),
            )
            .route(
                "/responses",
                post(|| async {
                    Json(json!({
                        "id": "resp_1",
                        "status": "completed",
                        "output": [{"content": [{"type": "output_text", "text": "{\"risk\": 8, \"flag\": false}"}]}],
                    }))
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        base
    }

    /// Each adapter delivers a trigger record: Ollama and OpenAI commit the
    /// reply's values through the completions path and mark it delivered;
    /// OpenAI needs no key, and a provider that does not answer fails the
    /// record with a 502; ACP records a handoff, 202.
    #[tokio::test]
    async fn the_adapters_deliver_a_trigger_record() {
        let base = provider().await;
        let mut engine = PerceptionEngine::new(4);
        let config = Arc::get_mut(&mut engine.integrations).unwrap();
        let mapping = json!({
            "sensorId": "agent.nurse.completion",
            "region": {"offset": 0, "length": 2},
            "extract": {"type": "json", "pointers": ["/risk", "/flag"]},
            "normalize": {"mode": "minmax", "min": 0, "max": 10},
        });
        config.source_mappings.insert("nurse".into(), mapping);
        config.ollama_base_url.clone_from(&base);
        config.ollama_mapping = "nurse".into();
        config.openai_base_url.clone_from(&base);
        config.openai_mapping = "nurse".into();
        engine.dispatch.enabled = true;
        engine.cache_catalog(&json!({ "machines": [{ "id": "m", "name": "M", "metadata": {
            "agentBinding": { "agent": "nurse", "trigger": "t", "allowedActions": ["page"] } } }] }));
        let governance = json!({ "ragStatusCode": "RED", "sequenceId": "s", "actionCode": "page" });
        engine.dispatch_triggers(&json!({ "mergeBatch": [
            { "machineId": "m", "governance": governance, "values": [0, 1], "region": { "offset": 0, "length": 2 } },
        ] }));
        let id = engine.dispatch.records.keys().next().unwrap().clone();
        let shared = Arc::new(RwLock::new(engine));
        let app = router(Arc::clone(&shared));

        let (status, reply) = send(
            &app,
            "/api/integrations/ollama/dispatch",
            json!({"dispatchId": id}),
        )
        .await;
        assert_eq!(
            (status, &reply["completionCommitted"]),
            (StatusCode::OK, &json!(true))
        );
        assert_eq!(reply["contentJson"], json!({"risk": 3, "flag": true}));
        assert_eq!(
            reply["completion"]["completion"]["sensorId"],
            json!("agent.nurse.completion")
        );
        let delivered = record(&app, &id).await;
        assert_eq!(
            (
                &delivered["status"],
                &delivered["attempts"],
                &delivered["providerReceipt"]["externalRunId"]
            ),
            (
                &json!("delivered"),
                &json!(1),
                &json!("2026-11-01T00:00:00Z")
            )
        );

        // A provider that does not answer fails the record with a 502.
        let set_base = |base: &str| {
            if let Some(config) = Arc::get_mut(&mut shared.write().unwrap().integrations) {
                config.openai_base_url = base.to_owned();
            }
        };
        set_base("http://127.0.0.1:1");
        let (status, reply) =
            send(&app, "/api/integrations/openai/dispatch", json!({"id": id})).await;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert_eq!(
            reply["receipt"]["providerReceipt"],
            json!({"model": "nemotron-3-nano:4b-mlx", "endpoint": "/responses"})
        );
        assert_eq!(record(&app, &id).await["status"], json!("failed"));
        // No key is needed: the local LLM ignores it (owner ruling, 2026-11-01).
        set_base(&base);
        let (status, reply) = send(
            &app,
            "/api/integrations/openai/dispatch",
            json!({"dispatchId": id}),
        )
        .await;
        assert_eq!(
            (status, &reply["receipt"]["externalRunId"]),
            (StatusCode::OK, &json!("resp_1"))
        );
        assert_eq!(
            reply["completion"]["completion"]["completionId"],
            json!("resp_1")
        );

        let (status, reply) = send(
            &app,
            "/api/integrations/acp/dispatch",
            json!({"dispatchId": id, "sessionKey": ""}),
        )
        .await;
        assert_eq!(
            (
                status,
                &reply["handoff"]["targetAgent"],
                &reply["handoff"]["sessionKey"]
            ),
            (StatusCode::ACCEPTED, &json!("nurse"), &Value::Null)
        );
        let accepted = record(&app, &id).await;
        assert_eq!(
            (&accepted["status"], &accepted["attempts"]),
            (&json!("accepted"), &json!(4))
        );

        for (body, expected) in [
            (json!([]), StatusCode::BAD_REQUEST),
            (json!({}), StatusCode::BAD_REQUEST),
            (
                json!({"dispatchId": "dispatch-none"}),
                StatusCode::NOT_FOUND,
            ),
            (
                json!({"dispatchId": id, "sourceMappingId": "none"}),
                StatusCode::NOT_FOUND,
            ),
        ] {
            assert_eq!(
                send(&app, "/api/integrations/ollama/dispatch", body)
                    .await
                    .0,
                expected
            );
        }
    }

    #[test]
    fn completion_values_follow_the_mapping() {
        let mapping = json!({
            "extract": {"type": "json", "pointers": ["/risk", "/flags/0"]},
            "normalize": {"mode": "minmax", "min": 0, "max": 10, "clamp": true},
        });
        let content = json!({"risk": "5", "flags": [true]});
        assert_eq!(
            values_from_content(&content, &mapping),
            Ok(json!([0.5, 0.1]))
        );
        assert_eq!(
            values_from_content(&json!({"risk": 1}), &mapping),
            Err("missing required JSON pointer: /flags/0".to_owned())
        );
        assert_eq!(
            values_from_content(&Value::Null, &mapping),
            Err("provider response content is not valid JSON".to_owned())
        );
        assert_eq!(
            values_from_content(&json!({"completion": {"values": [1, "2"]}}), &json!({})),
            Ok(json!([1.0, 2.0]))
        );
        assert_eq!(
            validate_values(&json!([1, "x"])),
            Err("provider completion value is not a finite number".to_owned())
        );
    }

    #[test]
    fn the_completion_contract_names_the_extracted_keys() {
        let mapping = json!({"extract": {"type": "json", "pointers": ["/a", "/b/c", "/a"]}});
        assert!(contract_prompt(&mapping).ends_with(
            "these keys, each a number or boolean: a, b. For example: {\"a\": 0, \"b\": 0}"
        ));
        assert!(contract_prompt(&Value::Null).ends_with("For example: {\"values\": [0]}"));
        assert_eq!(
            completion_schema(&mapping)["required"],
            json!(["a", "b", "a"])
        );
        assert!(!supports_structured_output("nemotron-3-nano:4b-mlx"));
        assert!(supports_structured_output("llama3.1:8b"));
    }
}
