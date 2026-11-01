//! The MQTT mapping registry: how broker topics project into perceptual space
//! (C++ `mqtt_mapping.cpp`). Topics describe the outside world; a rule, never
//! a topic string, carries the region a value lands in.
//!
//! A rule is a topic filter (MQTT `+` and `#` wildcards, their captures
//! interpolated into `sensorIdTemplate` as `{1}`, `{2}`, ...), a region, an
//! extract (`csv-float`, `json` by pointer, `raw`/`single-float`), a
//! normalize (`passthrough`, `minmax`, `linear`, `band`, then an optional
//! clamp to [0, 1]), a TTL and a push policy. A decoded message must carry
//! exactly the region's length of values. Error texts are C++'s.

use crate::auxiliary::leading_number;
use serde_json::{Value, json};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Extract {
    Raw,
    CsvFloat,
    Json,
    SingleFloat,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Normalize {
    Passthrough,
    MinMax,
    Linear,
    Band,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum PushMode {
    Debounced,
    Manual,
    Immediate,
}

pub(crate) struct Rule {
    pub(crate) id: String,
    pub(crate) topic_filter: String,
    sensor_id_template: String,
    pub(crate) offset: i64,
    pub(crate) length: i64,
    extract: Extract,
    pointer: String,
    csv_index: Option<i64>,
    mode: Normalize,
    min: f64,
    max: f64,
    scale: f64,
    shift: f64,
    clamp: bool,
    pub(crate) ttl_ms: i64,
    pub(crate) qos: i64,
    accept_retained: bool,
    pub(crate) push_mode: PushMode,
    pub(crate) debounce_ms: i64,
}

/// A rule's counters, as `/api/mqtt/mappings` reports them. C++ declares
/// `stale` but never counts it.
#[derive(Default)]
pub(crate) struct Metrics {
    pub(crate) received: u64,
    pub(crate) mapped: u64,
    pub(crate) rejected: u64,
    stale: u64,
    pub(crate) last_message_at_ms: i64,
    pub(crate) last_error: String,
    pub(crate) last_error_at_ms: i64,
}

#[derive(Default)]
pub(crate) struct Registry {
    pub(crate) rules: Vec<Rule>,
    pub(crate) metrics: Vec<Metrics>,
}

/// C++ `std::stoi`: leading whitespace, a sign, at least one digit, the
/// rest ignored; none, or outside `int`, is no number.
pub(crate) fn leading_int(text: &str) -> Option<i64> {
    let text = text.trim_start_matches([' ', '\t', '\n', '\x0b', '\x0c', '\r']);
    let digits_from = usize::from(text.starts_with(['+', '-']));
    let digits = text[digits_from..]
        .bytes()
        .take_while(u8::is_ascii_digit)
        .count();
    if digits == 0 {
        return None;
    }
    text[..digits_from + digits]
        .parse::<i64>()
        .ok()
        .filter(|n| i32::try_from(*n).is_ok())
}

/// C++ `std::stod`, or NaN when nothing converts.
fn stod(text: &str) -> f64 {
    leading_number(text).unwrap_or(f64::NAN)
}

fn extract_type(name: &str) -> Result<Extract, String> {
    match name {
        "json" => Ok(Extract::Json),
        "csv-float" => Ok(Extract::CsvFloat),
        "raw" => Ok(Extract::Raw),
        "single-float" => Ok(Extract::SingleFloat),
        _ => Err(format!("unknown extract.type: {name}")),
    }
}

fn normalize_mode(name: &str) -> Result<Normalize, String> {
    match name {
        "minmax" => Ok(Normalize::MinMax),
        "linear" => Ok(Normalize::Linear),
        "passthrough" | "" => Ok(Normalize::Passthrough),
        "band" => Ok(Normalize::Band),
        _ => Err(format!("unknown normalize.mode: {name}")),
    }
}

fn push_mode(name: &str) -> Result<PushMode, String> {
    match name {
        "debounced" | "" => Ok(PushMode::Debounced),
        "manual" => Ok(PushMode::Manual),
        "immediate" => Ok(PushMode::Immediate),
        _ => Err(format!("unknown pushMode: {name}")),
    }
}

/// C++ `json::Value::as_string(fallback)`.
fn text_or<'a>(value: &'a Value, fallback: &'a str) -> &'a str {
    value.as_str().unwrap_or(fallback)
}

/// Comma- or whitespace-separated numbers, each `stod`'s reading.
fn csv_floats(text: &str) -> Vec<f64> {
    text.split([',', ' ', '\n', '\r', '\t'])
        .filter(|token| !token.is_empty())
        .map(stod)
        .collect()
}

/// C++ `navigate_pointer`: `""` and `"/"` are the root; tokens are not
/// unescaped; an array index is `stoi`'s reading of its token.
fn navigate<'a>(root: &'a Value, pointer: &str) -> Option<&'a Value> {
    if pointer.is_empty() || pointer == "/" {
        return Some(root);
    }
    let rest = pointer.strip_prefix('/')?;
    let mut cursor = root;
    for token in rest.split('/') {
        cursor = match cursor {
            Value::Object(object) => object.get(token)?,
            Value::Array(items) => usize::try_from(leading_int(token)?)
                .ok()
                .and_then(|index| items.get(index))?,
            _ => return None,
        };
    }
    Some(cursor)
}

fn clamp_unit(value: f64) -> f64 {
    if value.is_finite() {
        value.clamp(0.0, 1.0)
    } else {
        f64::NAN
    }
}

/// MQTT v3.1.1 §4.7 filter matching, with the wildcards' captures in order.
fn match_one(topic: &[&str], filter: &[&str]) -> Option<Vec<String>> {
    let mut captures = Vec::new();
    let mut at = 0;
    for (index, level) in filter.iter().enumerate() {
        if *level == "#" {
            captures.push(topic.get(at..).unwrap_or_default().join("/"));
            return (index + 1 == filter.len()).then_some(captures);
        }
        let current = topic.get(at)?;
        if *level == "+" {
            captures.push((*current).to_owned());
        } else if level != current {
            return None;
        }
        at += 1;
    }
    (at == topic.len()).then_some(captures)
}

impl Registry {
    /// C++ `MappingRegistry::from_json`: `{defaults?, mappings: [...]}`.
    pub(crate) fn from_json(root: &Value) -> Result<Self, String> {
        let defaults = &root["defaults"];
        let number = |value: &Value| value.as_f64();
        let ttl = number(&defaults["ttlMs"]).map_or(30000, |n| n as i64);
        let qos = number(&defaults["qos"]).map_or(0, |n| n as i32 as i64);
        let retained = defaults["acceptRetained"].as_bool().unwrap_or(true);
        let pushes = match defaults["pushMode"].as_str() {
            Some(name) => push_mode(name)?,
            None => PushMode::Debounced,
        };
        let debounce = number(&defaults["debounceMs"]).map_or(250, |n| n as i64);
        let mappings = root["mappings"]
            .as_array()
            .ok_or("mqtt-mappings: missing top-level \"mappings\" array")?;
        let mut registry = Self::default();
        for mapping in mappings {
            if !mapping.is_object() {
                return Err("mqtt-mappings: each mapping must be a JSON object".to_owned());
            }
            let id = text_or(&mapping["id"], "").to_owned();
            let topic_filter = text_or(&mapping["topicFilter"], "").to_owned();
            if id.is_empty() || topic_filter.is_empty() {
                return Err("mqtt-mappings: id and topicFilter are required".to_owned());
            }
            let region = &mapping["region"];
            let (offset, length) = if region.is_object() {
                (
                    region["offset"].as_f64().unwrap_or(0.0) as i32 as i64,
                    (region["length"].as_f64().unwrap_or(1.0) as i32).max(1) as i64,
                )
            } else {
                (0, 1)
            };
            let extract = &mapping["extract"];
            let (kind, pointer, csv_index) = if extract.is_object() {
                (
                    extract_type(text_or(&extract["type"], "csv-float"))?,
                    text_or(&extract["pointer"], "").to_owned(),
                    extract["index"].as_f64().map(|n| n as i32 as i64),
                )
            } else {
                (Extract::CsvFloat, String::new(), None)
            };
            let norm = &mapping["normalize"];
            let given = |key: &str, fallback: f64| {
                if norm.is_object() {
                    norm[key].as_f64().unwrap_or(fallback)
                } else {
                    fallback
                }
            };
            let mode = if norm.is_object() {
                normalize_mode(text_or(&norm["mode"], "passthrough"))?
            } else {
                Normalize::Passthrough
            };
            registry.rules.push(Rule {
                id,
                topic_filter,
                sensor_id_template: text_or(&mapping["sensorIdTemplate"], "").to_owned(),
                offset,
                length,
                extract: kind,
                pointer,
                csv_index,
                mode,
                min: given("min", 0.0),
                max: given("max", 1.0),
                scale: given("scale", 1.0),
                shift: given("offset", 0.0),
                clamp: norm["clamp"].as_bool().unwrap_or(true),
                ttl_ms: number(&mapping["ttlMs"]).map_or(ttl, |n| n as i64),
                qos: number(&mapping["qos"]).map_or(qos, |n| n as i32 as i64),
                accept_retained: mapping["acceptRetained"].as_bool().unwrap_or(retained),
                push_mode: match mapping["pushMode"].as_str() {
                    Some(name) => push_mode(name)?,
                    None => pushes,
                },
                debounce_ms: number(&mapping["debounceMs"]).map_or(debounce, |n| n as i64),
            });
            registry.metrics.push(Metrics::default());
        }
        Ok(registry)
    }

    /// The legacy `MQTT_TOPIC_MAP` shape (`[{topic, sensorId, offset,
    /// length, codec}]`) as a registry. As in C++, every object entry
    /// advances the fallback id's counter.
    pub(crate) fn from_legacy(legacy: &Value) -> Result<Self, String> {
        let mut mappings = Vec::new();
        for (counter, item) in legacy
            .as_array()
            .into_iter()
            .flatten()
            .filter(|item| item.is_object())
            .enumerate()
        {
            mappings.push(json!({
                "id": item["sensorId"].as_str().map_or_else(|| format!("mqtt-legacy-{counter}"), str::to_owned),
                "topicFilter": text_or(&item["topic"], ""),
                "sensorIdTemplate": text_or(&item["sensorId"], ""),
                "region": {
                    "offset": item["offset"].as_f64().unwrap_or(0.0),
                    "length": item["length"].as_f64().unwrap_or(1.0),
                },
                "extract": {"type": text_or(&item["codec"], "csv-float")},
                "normalize": {"mode": "passthrough"},
            }));
        }
        Self::from_json(&json!({ "mappings": mappings }))
    }

    pub(crate) fn len(&self) -> usize {
        self.rules.len()
    }

    /// Every rule whose filter matches `topic`, in declaration order, with
    /// its captures: one message fans out to all of them.
    pub(crate) fn match_all(&self, topic: &str) -> Vec<(usize, Vec<String>)> {
        let levels: Vec<&str> = topic.split('/').collect();
        self.rules
            .iter()
            .enumerate()
            .filter_map(|(index, rule)| {
                let filter: Vec<&str> = rule.topic_filter.split('/').collect();
                match_one(&levels, &filter).map(|captures| (index, captures))
            })
            .collect()
    }

    /// The template with `{n}` replaced by the n-th capture (1-based); an
    /// index out of range or a malformed placeholder is dropped. No
    /// template: the topic itself.
    pub(crate) fn resolve_sensor_id(rule: &Rule, topic: &str, captures: &[String]) -> String {
        let template = &rule.sensor_id_template;
        if template.is_empty() {
            return topic.to_owned();
        }
        let mut out = String::new();
        let mut rest = template.as_str();
        while let Some(open) = rest.find('{') {
            let Some(close) = rest[open + 1..].find('}').map(|c| open + 1 + c) else {
                break;
            };
            out.push_str(&rest[..open]);
            if let Some(index) = leading_int(&rest[open + 1..close])
                && let Some(capture) = usize::try_from(index)
                    .ok()
                    .filter(|i| *i >= 1)
                    .and_then(|i| captures.get(i - 1))
            {
                out.push_str(capture);
            }
            rest = &rest[close + 1..];
        }
        out.push_str(rest);
        out
    }

    /// Extract, normalize and check the length: the values, or why not.
    pub(crate) fn decode(rule: &Rule, payload: &[u8]) -> Result<Vec<f64>, String> {
        let text = String::from_utf8_lossy(payload);
        let raw = match rule.extract {
            Extract::Raw | Extract::SingleFloat => vec![stod(&text)],
            Extract::CsvFloat => {
                let all = csv_floats(&text);
                match rule.csv_index {
                    Some(index) => {
                        let found = usize::try_from(index).ok().and_then(|i| all.get(i));
                        let Some(value) = found else {
                            return Err(format!(
                                "csv index {index} out of range (have {})",
                                all.len()
                            ));
                        };
                        vec![*value]
                    }
                    None => all,
                }
            }
            Extract::Json => {
                let parsed: Value =
                    serde_json::from_str(&text).map_err(|error| format!("json parse: {error}"))?;
                let node = navigate(&parsed, &rule.pointer)
                    .ok_or_else(|| format!("json pointer \"{}\" not found", rule.pointer))?;
                match node {
                    Value::Number(n) => vec![n.as_f64().unwrap_or(f64::NAN)],
                    Value::Array(items) => items
                        .iter()
                        .map(|item| item.as_f64().unwrap_or(f64::NAN))
                        .collect(),
                    Value::String(s) => vec![stod(s)],
                    Value::Bool(b) => vec![if *b { 1.0 } else { 0.0 }],
                    _ => {
                        return Err(
                            "json pointer target is not a number / array / string / bool"
                                .to_owned(),
                        );
                    }
                }
            }
        };
        let mut values = Vec::with_capacity(raw.len());
        for v in raw {
            if !v.is_finite() {
                return Err("value is not finite".to_owned());
            }
            let mut n = match rule.mode {
                Normalize::Passthrough => v,
                Normalize::MinMax => {
                    let span = rule.max - rule.min;
                    if span == 0.0 {
                        return Err("normalize.min == normalize.max".to_owned());
                    }
                    (v - rule.min) / span
                }
                Normalize::Linear => v * rule.scale + rule.shift,
                Normalize::Band => {
                    if v >= rule.min && v <= rule.max {
                        1.0
                    } else {
                        0.0
                    }
                }
            };
            if rule.clamp {
                n = clamp_unit(n);
            }
            if !n.is_finite() {
                return Err("value not finite after normalize".to_owned());
            }
            values.push(n);
        }
        if values.len() as i64 != rule.length {
            return Err(format!(
                "transformed value count {} != region.length {}",
                values.len(),
                rule.length
            ));
        }
        Ok(values)
    }

    /// One warning per pair of rules whose regions overlap, unless allowed.
    pub(crate) fn validate_overlaps(&self, allow: bool) -> Vec<String> {
        let mut warnings = Vec::new();
        if allow {
            return warnings;
        }
        for (i, a) in self.rules.iter().enumerate() {
            let (a_start, a_end) = (a.offset, a.offset + a.length);
            for b in &self.rules[i + 1..] {
                let (b_start, b_end) = (b.offset, b.offset + b.length);
                if a_start < b_end && b_start < a_end {
                    warnings.push(format!(
                        "mappings \"{}\" [{a_start},{a_end}) and \"{}\" [{b_start},{b_end}) overlap",
                        a.id, b.id
                    ));
                }
            }
        }
        warnings
    }

    /// `GET /api/mqtt/mappings`: each rule as configured, with its counters.
    pub(crate) fn to_json(&self) -> Value {
        let mappings: Vec<Value> = self
            .rules
            .iter()
            .zip(&self.metrics)
            .map(|(rule, metrics)| {
                let mut extract = json!({ "type": match rule.extract {
                    Extract::Raw => "raw",
                    Extract::CsvFloat => "csv-float",
                    Extract::Json => "json",
                    Extract::SingleFloat => "single-float",
                }});
                if !rule.pointer.is_empty() {
                    extract["pointer"] = json!(rule.pointer);
                }
                if let Some(index) = rule.csv_index {
                    extract["index"] = json!(index);
                }
                json!({
                    "id": rule.id,
                    "topicFilter": rule.topic_filter,
                    "sensorIdTemplate": rule.sensor_id_template,
                    "region": {"offset": rule.offset, "length": rule.length},
                    "extract": extract,
                    "normalize": {
                        "mode": match rule.mode {
                            Normalize::Passthrough => "passthrough",
                            Normalize::MinMax => "minmax",
                            Normalize::Linear => "linear",
                            Normalize::Band => "band",
                        },
                        "min": rule.min,
                        "max": rule.max,
                        "scale": rule.scale,
                        "offset": rule.shift,
                        "clamp": rule.clamp,
                    },
                    "ttlMs": rule.ttl_ms,
                    "qos": rule.qos,
                    "acceptRetained": rule.accept_retained,
                    "pushMode": match rule.push_mode {
                        PushMode::Debounced => "debounced",
                        PushMode::Manual => "manual",
                        PushMode::Immediate => "immediate",
                    },
                    "debounceMs": rule.debounce_ms,
                    "counters": {
                        "received": metrics.received,
                        "mapped": metrics.mapped,
                        "rejected": metrics.rejected,
                        "stale": metrics.stale,
                        "lastMessageAtMs": metrics.last_message_at_ms,
                        "lastError": metrics.last_error,
                        "lastErrorAtMs": metrics.last_error_at_ms,
                    },
                })
            })
            .collect();
        json!({ "mappings": mappings })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registry(mappings: Value) -> Registry {
        Registry::from_json(&json!({ "mappings": mappings })).unwrap()
    }

    #[test]
    fn filters_match_and_capture_into_the_sensor_id() {
        let reg = registry(json!([
            {"id": "a", "topicFilter": "farm/+/temp", "sensorIdTemplate": "zone.{1}.temp{3}{x}"},
            {"id": "b", "topicFilter": "farm/#"},
            {"id": "c", "topicFilter": "farm/+"},
        ]));
        let matches = reg.match_all("farm/north/temp");
        assert_eq!(
            matches,
            vec![
                (0, vec!["north".to_owned()]),
                (1, vec!["north/temp".to_owned()])
            ]
        );
        assert_eq!(
            Registry::resolve_sensor_id(&reg.rules[0], "farm/north/temp", &matches[0].1),
            "zone.north.temp"
        );
        assert_eq!(
            Registry::resolve_sensor_id(&reg.rules[1], "farm/x", &[]),
            "farm/x"
        );
        assert_eq!(reg.match_all("farm").len(), 1);
    }

    #[test]
    fn decoding_extracts_normalizes_and_checks_the_length() {
        let reg = registry(json!([
            {"id": "csv", "topicFilter": "t", "region": {"offset": 0, "length": 2},
             "normalize": {"mode": "minmax", "min": 0, "max": 10}},
            {"id": "json", "topicFilter": "t", "extract": {"type": "json", "pointer": "/r/1"},
             "normalize": {"mode": "band", "min": 2, "max": 4}},
            {"id": "idx", "topicFilter": "t", "extract": {"type": "csv-float", "index": 5}},
            {"id": "raw", "topicFilter": "t", "extract": {"type": "raw"},
             "normalize": {"mode": "linear", "scale": 2, "offset": 1, "clamp": false}},
        ]));
        assert_eq!(
            Registry::decode(&reg.rules[0], b"5, 20"),
            Ok(vec![0.5, 1.0])
        );
        assert_eq!(
            Registry::decode(&reg.rules[0], b"5"),
            Err("transformed value count 1 != region.length 2".to_owned())
        );
        assert_eq!(
            Registry::decode(&reg.rules[1], br#"{"r": [0, "3"]}"#),
            Ok(vec![1.0])
        );
        assert_eq!(
            Registry::decode(&reg.rules[1], br#"{"r": []}"#),
            Err("json pointer \"/r/1\" not found".to_owned())
        );
        assert_eq!(
            Registry::decode(&reg.rules[2], b"1 2"),
            Err("csv index 5 out of range (have 2)".to_owned())
        );
        assert_eq!(Registry::decode(&reg.rules[3], b" 1.5kg"), Ok(vec![4.0]));
        assert_eq!(
            Registry::decode(&reg.rules[3], b"x"),
            Err("value is not finite".to_owned())
        );
        assert_eq!(
            reg.validate_overlaps(false),
            vec![
                "mappings \"csv\" [0,2) and \"json\" [0,1) overlap",
                "mappings \"csv\" [0,2) and \"idx\" [0,1) overlap",
                "mappings \"csv\" [0,2) and \"raw\" [0,1) overlap",
                "mappings \"json\" [0,1) and \"idx\" [0,1) overlap",
                "mappings \"json\" [0,1) and \"raw\" [0,1) overlap",
                "mappings \"idx\" [0,1) and \"raw\" [0,1) overlap",
            ]
        );
        assert_eq!(
            Registry::from_json(
                &json!({"mappings": [{"id": "x", "topicFilter": "t", "pushMode": "soon"}]})
            )
            .err(),
            Some("unknown pushMode: soon".to_owned())
        );
        assert_eq!(leading_int(" -12abc"), Some(-12));
        assert_eq!(leading_int("x1"), None);
    }
}
