//! The PE half of a K-line: one ISRE composition record per committed step
//! (`re-isre-composition/1`), kept in a persistent store per PE instance
//! (RealityEngine_CI#603; SURFACE_SPEC "The PE half: ISRE composition"). A port
//! of C++ `isre.cpp`.
//!
//! Which source won each input cell, of what kind, under the versioned
//! source-kind taxonomy the corpus carries
//! (`<MACHINES_DIR>/../schemas/source-kinds/re-source-kinds-<N>.json`).

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

use serde_json::{Map, Value, json};

use crate::kline::{Controls, Store, jcs, sha256_hex};

/// The taxonomy version the PEs record (owner, 2026-11-27).
pub(crate) const TAXONOMY_VERSION: u32 = 2;
pub(crate) const STORE_MAX_DEFAULT: u64 = 2 * 1024 * 1024 * 1024;
pub(crate) const STORE_MAX_MAX: u64 = 9_007_199_254_740_991;

#[derive(Clone, Debug, PartialEq)]
struct Entry {
    kind: String,
    provider: Option<String>,
    class: String,
}

#[derive(Clone, Debug)]
pub(crate) struct Taxonomy {
    version: u32,
    provider_mapping: BTreeMap<String, String>,
    /// kind -> class (kinds without providers)
    kind_class: BTreeMap<String, String>,
    /// kind -> provider -> class
    provider_class: BTreeMap<String, BTreeMap<String, String>>,
    unknown_provider: String,
    unknown_class: String,
}

impl Taxonomy {
    /// `<MACHINES_DIR>/../schemas/source-kinds`.
    pub(crate) fn dir_for(machines_dir: &str) -> PathBuf {
        let dir = if machines_dir.is_empty() {
            "../RealityEngine_Machines/machines"
        } else {
            machines_dir
        };
        let abs = std::path::absolute(dir).unwrap_or_else(|_| PathBuf::from(dir));
        abs.parent()
            .map(Path::to_path_buf)
            .unwrap_or_default()
            .join("schemas")
            .join("source-kinds")
    }

    /// `re-source-kinds-<version>.json` from `dir`; None when it is not there
    /// or does not parse.
    pub(crate) fn load(dir: &Path, version: u32) -> Option<Self> {
        let text =
            std::fs::read_to_string(dir.join(format!("re-source-kinds-{version}.json"))).ok()?;
        let doc: Value = serde_json::from_str(&text).ok()?;
        if doc["format"] != "re-source-kinds" || doc["version"].as_f64() != Some(f64::from(version))
        {
            return None;
        }
        let mut t = Self {
            version,
            provider_mapping: BTreeMap::new(),
            kind_class: BTreeMap::new(),
            provider_class: BTreeMap::new(),
            unknown_provider: doc["unknownProvider"]["provider"]
                .as_str()
                .unwrap_or("unattributed")
                .to_owned(),
            unknown_class: doc["unknownProvider"]["class"]
                .as_str()
                .unwrap_or("generated")
                .to_owned(),
        };
        for (k, v) in doc["providerMapping"].as_object().into_iter().flatten() {
            if let Some(v) = v.as_str() {
                t.provider_mapping.insert(k.clone(), v.to_owned());
            }
        }
        for k in doc["kinds"].as_array().into_iter().flatten() {
            let kind = k["kind"].as_str().unwrap_or_default().to_owned();
            if let Some(providers) = k["providers"].as_array() {
                let per = t.provider_class.entry(kind).or_default();
                for p in providers {
                    per.insert(
                        p["provider"].as_str().unwrap_or_default().to_owned(),
                        p["class"].as_str().unwrap_or_default().to_owned(),
                    );
                }
            } else if let Some(class) = k["class"].as_str() {
                t.kind_class.insert(kind, class.to_owned());
            }
        }
        Some(t)
    }

    pub(crate) fn name(&self) -> String {
        format!("re-source-kinds/{}", self.version)
    }

    /// A source's entry: its kind; a sensor's provider from its origin (first
    /// `.` segment, lowercased, through providerMapping; empty is `signal`;
    /// unlisted is the unknown provider); and the version's class.
    fn source_entry(&self, kind: &str, origin: &str) -> Entry {
        if let Some(class) = self.kind_class.get(kind) {
            return Entry {
                kind: kind.to_owned(),
                provider: None,
                class: class.clone(),
            };
        }
        // A kind with providers (sensor), or one the version does not list,
        // which is read as a sensor of its origin.
        let as_kind = if self.provider_class.contains_key(kind) {
            kind
        } else {
            "sensor"
        };
        let mut provider = origin.split('.').next().unwrap_or_default().to_lowercase();
        if provider.is_empty() {
            provider = "signal".to_owned();
        }
        if let Some(mapped) = self.provider_mapping.get(&provider) {
            provider = mapped.clone();
        }
        match self
            .provider_class
            .get(as_kind)
            .and_then(|per| per.get(&provider))
        {
            Some(class) => Entry {
                kind: as_kind.to_owned(),
                provider: Some(provider),
                class: class.clone(),
            },
            None => Entry {
                kind: as_kind.to_owned(),
                provider: Some(self.unknown_provider.clone()),
                class: self.unknown_class.clone(),
            },
        }
    }

    fn feedback_entry(&self) -> Entry {
        Entry {
            kind: "feedback".to_owned(),
            provider: None,
            class: self
                .kind_class
                .get("feedback")
                .cloned()
                .unwrap_or_else(|| "deterministic".to_owned()),
        }
    }
}

/// One cell's winner at a step: a source, or the feedback term.
#[derive(Clone, Debug, Default)]
pub(crate) struct Winner {
    pub(crate) cell: i64,
    pub(crate) feedback: bool,
    pub(crate) id: String,
    pub(crate) kind: String,
    pub(crate) origin: String,
    pub(crate) caused_by: Value,
}

/// The step's `sources`: runs of consecutive cells with one winner, ascending.
fn sources_json(cells: &[Winner], taxonomy: &Taxonomy) -> Value {
    struct Run {
        first: i64,
        last: i64,
        entry: Entry,
        id: String,
        caused_by: Value,
    }
    let mut runs: Vec<Value> = Vec::new();
    let flush = |run: Run, runs: &mut Vec<Value>| {
        let mut o = Map::new();
        o.insert("cells".into(), json!([run.first, run.last]));
        o.insert("kind".into(), json!(run.entry.kind));
        o.insert("class".into(), json!(run.entry.class));
        o.insert("causedBy".into(), run.caused_by);
        if let Some(p) = run.entry.provider {
            o.insert("provider".into(), json!(p));
        }
        if !run.id.is_empty() {
            o.insert("id".into(), json!(run.id));
        }
        runs.push(Value::Object(o));
    };
    let mut sorted = cells.to_vec();
    sorted.sort_by_key(|w| w.cell);
    let mut cur: Option<Run> = None;
    for w in sorted {
        let entry = if w.feedback {
            taxonomy.feedback_entry()
        } else {
            taxonomy.source_entry(&w.kind, &w.origin)
        };
        let id = if w.feedback { String::new() } else { w.id };
        let caused_by = if w.feedback { Value::Null } else { w.caused_by };
        if let Some(run) = cur.as_mut()
            && run.last + 1 == w.cell
            && run.entry == entry
            && run.id == id
            && run.caused_by == caused_by
        {
            run.last = w.cell;
            continue;
        }
        if let Some(run) = cur.take() {
            flush(run, &mut runs);
        }
        cur = Some(Run {
            first: w.cell,
            last: w.cell,
            entry,
            id,
            caused_by,
        });
    }
    if let Some(run) = cur {
        flush(run, &mut runs);
    }
    Value::Array(runs)
}

pub(crate) struct Recorder {
    instance: String,
    store: Store,
    taxonomy: Option<Taxonomy>,
    pub(crate) capture: bool,
    pub(crate) store_max_bytes: u64,
    re_instance: String,
}

impl Default for Recorder {
    fn default() -> Self {
        Self {
            instance: String::new(),
            store: Store::default().with_budget_name("isreStoreMaxBytes"),
            taxonomy: None,
            capture: false,
            store_max_bytes: STORE_MAX_DEFAULT,
            re_instance: String::new(),
        }
    }
}

impl Recorder {
    pub(crate) fn new(instance: String, store: Store, taxonomy: Option<Taxonomy>) -> Self {
        Self {
            instance,
            store: store.with_budget_name("isreStoreMaxBytes"),
            taxonomy,
            ..Self::default()
        }
    }

    pub(crate) fn has_taxonomy(&self) -> bool {
        self.taxonomy.is_some()
    }

    fn controls(&self) -> Controls {
        Controls {
            capture: self.capture,
            store_max_bytes: self.store_max_bytes,
            ..Controls::default()
        }
    }

    pub(crate) fn refresh_budget(&mut self) {
        let c = self.controls();
        self.store.refresh_warnings(&c);
    }

    pub(crate) fn health_warnings(&self) -> Vec<String> {
        self.store.cached_warnings()
    }

    /// One committed step: `re` is the RE step response's clock, `pe` the
    /// PE's tick for the record, `folds` the step's fold records.
    pub(crate) fn record(
        &mut self,
        re: &Value,
        pe: Value,
        cells: &[Winner],
        folds: &Value,
    ) -> Result<(), String> {
        let Some(taxonomy) = self.taxonomy.as_ref().filter(|_| self.capture) else {
            return Ok(());
        };
        if let Some(i) = re["instance"].as_str() {
            self.re_instance = i.to_owned();
        }
        let rec = json!({
            "format": "re-isre-composition/1",
            "taxonomy": taxonomy.name(),
            "re": re,
            "pe": pe,
            "sources": sources_json(cells, taxonomy),
            "folds": if folds.is_array() { folds.clone() } else { json!([]) },
            "delta": false,
        });
        let commit = re["commit"].as_f64().unwrap_or(0.0) as u64;
        let name = taxonomy.name();
        self.store.append(&[rec], &name, commit)?;
        let c = self.controls();
        self.store.enforce_budget(&c)
    }

    pub(crate) fn list(&self, from: u64, to: Option<u64>, limit: usize) -> Value {
        let limit = if limit == 0 { 100 } else { limit }.clamp(1, 1000);
        let mut out = Vec::new();
        let mut next = Value::Null;
        for (commit, raw, _) in self.store.lines_between(from, to) {
            if out.len() >= limit {
                next = json!(commit);
                break;
            }
            out.push(serde_json::from_str::<Value>(&raw).unwrap_or(Value::Null));
        }
        json!({ "records": out, "next": next })
    }

    pub(crate) fn stats(&self) -> Value {
        let s = self.store.stats(&self.controls());
        json!({
            "segments": s["segments"], "bytes": s["bytes"], "budget": s["budget"], "warnings": s["warnings"],
            "records": self.store.len(), "isreCapture": self.capture,
            "taxonomy": self.taxonomy.as_ref().map(Taxonomy::name),
        })
    }

    /// A `re-isre-bundle/1`: JSON Lines, header first. Marks the segments it
    /// covered as exported.
    pub(crate) fn export_bundle(&mut self, from: u64, to: Option<u64>) -> Result<String, String> {
        let mut body = String::new();
        let mut count = 0usize;
        let mut segments = BTreeSet::new();
        for (_, raw, segment) in self.store.lines_between(from, to) {
            body.push_str(&raw);
            body.push('\n');
            count += 1;
            segments.insert(segment);
        }
        let header = json!({
            "format": "re-isre-bundle/1",
            "source": { "instance": self.instance },
            "re": { "instance": self.re_instance },
            "taxonomy": self.taxonomy.as_ref().map(Taxonomy::name),
            "count": count,
            "sha256": sha256_hex(body.as_bytes()),
        });
        self.store.mark_exported(&segments)?;
        Ok(format!("{}\n{body}", jcs(&header)))
    }
}

/// The recorder for this PE: `<INSTANCE_ISRE_DIR>/<pe uuid>/`, else the clock
/// directory's sibling `isre/`; a minted UUID keeps it in memory.
/// `RE_ISRE_CAPTURE=1` turns capture on at boot, and then a missing taxonomy
/// refuses the boot.
pub(crate) fn recorder_from_environment(instance: &str) -> Result<Recorder, String> {
    let set = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());
    let uuid = set("PE_INSTANCE_UUID")
        .unwrap_or_default()
        .to_ascii_lowercase();
    let dir = (!uuid.is_empty() && uuid == instance).then(|| {
        let parent = set("INSTANCE_ISRE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                let clock_dir = set("INSTANCE_CLOCK_DIR")
                    .map(PathBuf::from)
                    .unwrap_or_else(|| {
                        PathBuf::from(set("HOME").unwrap_or_else(|| ".".to_owned()))
                            .join(".reality-engine")
                            .join("clock")
                    });
                clock_dir
                    .parent()
                    .map(Path::to_path_buf)
                    .unwrap_or_default()
                    .join("isre")
            });
        parent.join(&uuid)
    });
    let taxonomy_dir = Taxonomy::dir_for(&set("MACHINES_DIR").unwrap_or_default());
    let taxonomy = Taxonomy::load(&taxonomy_dir, TAXONOMY_VERSION);
    let capture = matches!(
        std::env::var("RE_ISRE_CAPTURE").as_deref(),
        Ok("1" | "true")
    );
    if capture && taxonomy.is_none() {
        return Err(format!(
            "isreCapture is on and re-source-kinds/{TAXONOMY_VERSION} is not in the corpus ({})",
            taxonomy_dir.display()
        ));
    }
    let store = Store::open(dir.clone())?;
    let mut recorder = Recorder::new(instance.to_owned(), store, taxonomy);
    recorder.capture = capture;
    recorder.refresh_budget();
    if capture {
        eprintln!(
            "perception-engine: ISRE composition capture on, store {}",
            dir.as_ref()
                .map_or("in memory".to_owned(), |d| d.display().to_string())
        );
    }
    Ok(recorder)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn taxonomy() -> Taxonomy {
        let dir = Taxonomy::dir_for("../RealityEngine_Machines/machines");
        Taxonomy::load(&dir, TAXONOMY_VERSION).expect("the corpus carries re-source-kinds/2")
    }

    #[test]
    fn runs_join_consecutive_cells_with_one_winner() {
        let t = taxonomy();
        let w = |cell, kind: &str, origin: &str, id: &str| Winner {
            cell,
            kind: kind.into(),
            origin: origin.into(),
            id: id.into(),
            ..Winner::default()
        };
        let cells = vec![
            w(3, "test", "", "a"),
            w(4, "test", "", "a"),
            w(5, "sensor", "OpenClaw.agent", "b"),
            Winner {
                cell: 6,
                feedback: true,
                ..Winner::default()
            },
            Winner {
                cell: 7,
                feedback: true,
                ..Winner::default()
            },
            w(9, "sensor", "", "c"),
            w(10, "sensor", "nobody", "c"),
        ];
        let runs = sources_json(&cells, &t);
        assert_eq!(
            runs,
            json!([
                { "cells": [3, 4], "kind": "test", "class": "deterministic", "causedBy": null, "id": "a" },
                { "cells": [5, 5], "kind": "sensor", "provider": "acp", "class": "generated", "causedBy": null, "id": "b" },
                { "cells": [6, 7], "kind": "feedback", "class": "deterministic", "causedBy": null },
                { "cells": [9, 9], "kind": "sensor", "provider": "signal", "class": "measured", "causedBy": null, "id": "c" },
                { "cells": [10, 10], "kind": "sensor", "provider": "unattributed", "class": "generated", "causedBy": null, "id": "c" },
            ])
        );
    }
}
