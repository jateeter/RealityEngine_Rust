//! `GET /api/metrics` (#20, the metrics workstream): the Prometheus
//! exposition `RealityEngine_CI` `docs/PE_METRICS_CONTRACT.md` defines, and
//! C++ `semantic_metrics_text` and `mqtt_metrics_text` write.
//!
//! The whole text must be byte-identical across runtimes once the `runtime`
//! label's value is normalized, so the writer is literal: every metric is
//! three lines (HELP, TYPE, sample), labels sorted by key, integer values,
//! multi-series counters in ascending label order and absent until a label
//! value is seen. The HELP strings are the contract's.
//!
//! - **Core gauges:** sources, `globalStep`, the vector dimension, and the
//!   last push's step `timestamp` (0 before any push).
//! - **Semantic block:** the corpus ABox manifest (resolved, machines), the
//!   audit records (capped at 1000), one perception event per active source
//!   after each push by its integration (origin, else kind) and whether its
//!   machine resolved to an IRI, dispatch records with a semantics link, and
//!   escalation-class dispatches by RAG status (`unstated` when absent).
//! - **MQTT block:** the two gauges at 0 when no bridge runs; otherwise the
//!   bridge's counters and its rule count.

use super::*;

/// The `runtime` label this engine writes; the parity check normalizes it.
const RUNTIME: &str = "rust";

/// The audit ring buffer's bound, as C++ caps `semanticAuditRecords`.
const AUDIT_CAPACITY: u64 = 1000;

/// The semantic guardrail counters: monotonic for the process lifetime,
/// bumped where records are made, so no eviction loses a count.
#[derive(Default)]
pub(crate) struct Tally {
    events: BTreeMap<String, u64>,
    events_joined: BTreeMap<String, u64>,
    escalations: BTreeMap<String, u64>,
    pub(crate) dispatch_total: u64,
    pub(crate) dispatch_joined: u64,
    audit_records: u64,
}

impl Tally {
    /// C++ `record_perception_event`.
    pub(crate) fn perception_event(&mut self, integration: &str, joined: bool) {
        *self.events.entry(integration.to_owned()).or_default() += 1;
        *self
            .events_joined
            .entry(integration.to_owned())
            .or_default() += u64::from(joined);
        self.audit_records = (self.audit_records + 1).min(AUDIT_CAPACITY);
    }

    /// An escalation-class action (`emergency-dispatch`,
    /// `urgent-intervention`) by the RAG status behind it.
    pub(crate) fn escalation(&mut self, rag: &str) {
        let rag = if rag.is_empty() { "unstated" } else { rag };
        *self.escalations.entry(rag.to_owned()).or_default() += 1;
    }
}

/// One metric's three lines, the `runtime` label among the others by key.
fn metric(
    out: &mut String,
    name: &str,
    help: &str,
    kind: &str,
    labels: &[(&str, &str)],
    value: u64,
) {
    let mut all: Vec<(&str, &str)> = labels.to_vec();
    all.push(("runtime", RUNTIME));
    all.sort_by_key(|(key, _)| *key);
    let labels: Vec<String> = all.iter().map(|(k, v)| format!("{k}=\"{v}\"")).collect();
    out.push_str(&format!(
        "# HELP {name} {help}\n# TYPE {name} {kind}\n{name}{{{}}} {value}\n",
        labels.join(",")
    ));
}

impl PerceptionEngine {
    /// One `re:PerceptionEvent` per active source after a push, attributed to
    /// the integration that feeds it and joined when its machine names a
    /// corpus ABox IRI.
    pub(crate) fn record_perception_events(&mut self) {
        let bases = self.dispatch.bases().clone();
        let events: Vec<(String, bool)> = self
            .sources
            .values()
            .filter(|source| source.active)
            .map(|source| {
                let (origin, machine) = match &source.detail {
                    sources::Detail::Sensor { origin, .. } => (origin.as_str(), ""),
                    sources::Detail::Test { machine_name, .. } => ("", machine_name.as_str()),
                    sources::Detail::Simulated { .. } => ("", ""),
                };
                let integration = if origin.is_empty() {
                    source.kind()
                } else {
                    origin
                };
                (
                    integration.to_owned(),
                    !machine.is_empty() && bases.contains_key(machine),
                )
            })
            .collect();
        for (integration, joined) in events {
            self.dispatch.tally.perception_event(&integration, joined);
        }
    }

    fn metrics_text(&mut self) -> String {
        let last_push_ms = self.last_push["timestamp"].as_f64().map_or(0, |t| t as u64);
        let machines = self.dispatch.bases().len() as u64;
        let tally = &self.dispatch.tally;
        let mut out = String::new();
        let gauge = "gauge";
        let counter = "counter";
        metric(
            &mut out,
            "perception_engine_sources_total",
            "Total sensor/test/simulated sources registered.",
            gauge,
            &[],
            self.sources.len() as u64,
        );
        metric(
            &mut out,
            "perception_engine_global_step",
            "Engine globalStep counter (push count since start).",
            gauge,
            &[],
            self.global_step,
        );
        metric(
            &mut out,
            "perception_engine_vector_size",
            "Configured vector dimension.",
            gauge,
            &[],
            self.dimension as u64,
        );
        metric(
            &mut out,
            "perception_engine_last_push_ms",
            "Wall-clock timestamp of the last successful push (0 if never).",
            gauge,
            &[],
            last_push_ms,
        );
        metric(
            &mut out,
            "semantic_manifest_available",
            "Corpus OWL semantics manifest resolved (1/0).",
            gauge,
            &[],
            u64::from(machines > 0),
        );
        metric(
            &mut out,
            "semantic_manifest_machines",
            "Machines carrying a semantic identity in the manifest.",
            gauge,
            &[],
            machines,
        );
        metric(
            &mut out,
            "semantic_audit_buffer_records",
            "re:PerceptionEvent records held in the audit ring buffer.",
            gauge,
            &[],
            tally.audit_records,
        );
        for (integration, count) in &tally.events {
            metric(
                &mut out,
                "semantic_perception_events_total",
                "re:PerceptionEvent records emitted, by originating integration.",
                counter,
                &[("integration", integration)],
                *count,
            );
        }
        for (integration, count) in &tally.events_joined {
            metric(
                &mut out,
                "semantic_perception_events_iri_joined_total",
                "Perception events whose machine resolved to a corpus ABox IRI.",
                counter,
                &[("integration", integration)],
                *count,
            );
        }
        metric(
            &mut out,
            "semantic_dispatch_records_total",
            "Dispatch records created with a semantics link.",
            counter,
            &[],
            tally.dispatch_total,
        );
        metric(
            &mut out,
            "semantic_dispatch_records_iri_joined_total",
            "Dispatch records whose machine resolved to a corpus ABox IRI.",
            counter,
            &[],
            tally.dispatch_joined,
        );
        for (rag, count) in &tally.escalations {
            metric(
                &mut out,
                "semantic_escalation_dispatches_total",
                "Escalation-class actions dispatched, by RAG status of the determination.",
                counter,
                &[("rag", rag)],
                *count,
            );
        }
        out.push_str(&mqtt::metrics_text(self.mqtt.as_deref()));
        out
    }
}

/// The MQTT block's writer, for `mqtt::metrics_text`.
pub(crate) fn write(out: &mut String, name: &str, help: &str, kind: &str, value: u64) {
    metric(out, name, help, kind, &[], value);
}

pub(crate) async fn metrics(State(pe): State<SharedPe>) -> Response {
    let text = match pe.write() {
        Ok(mut state) => state.metrics_text(),
        Err(_) => return error_response(lock_error()).into_response(),
    };
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; charset=utf-8",
        )],
        text,
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The zero state emits every single-series metric and no multi-series
    /// counter; a tallied series appears in ascending label order, with the
    /// `runtime` label sorted among the others.
    #[test]
    fn the_exposition_follows_the_contract() {
        let mut engine = PerceptionEngine::new(16);
        let zero = engine.metrics_text();
        assert_eq!(zero.matches("# HELP ").count(), 11);
        assert!(zero.starts_with(
            "# HELP perception_engine_sources_total Total sensor/test/simulated sources registered.\n\
             # TYPE perception_engine_sources_total gauge\n\
             perception_engine_sources_total{runtime=\"rust\"} 0\n"
        ));
        assert!(!zero.contains("semantic_perception_events_total"));
        assert!(zero.ends_with("mqtt_bridge_connected{runtime=\"rust\"} 0\n"));

        engine
            .dispatch
            .tally
            .perception_event("unattributed", false);
        engine.dispatch.tally.perception_event("healthkit", true);
        engine.dispatch.tally.escalation("");
        let text = engine.metrics_text();
        let events: Vec<&str> = text
            .lines()
            .filter(|l| l.starts_with("semantic_perception_events_iri_joined_total{"))
            .collect();
        assert_eq!(
            events,
            [
                "semantic_perception_events_iri_joined_total{integration=\"healthkit\",runtime=\"rust\"} 1",
                "semantic_perception_events_iri_joined_total{integration=\"unattributed\",runtime=\"rust\"} 0",
            ]
        );
        assert!(text.contains(
            "semantic_escalation_dispatches_total{rag=\"unstated\",runtime=\"rust\"} 1\n"
        ));
        assert!(text.contains("semantic_audit_buffer_records{runtime=\"rust\"} 2\n"));
    }
}
