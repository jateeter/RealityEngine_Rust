//! R1: self-contained conformance against the vendored regression corpus.
//!
//! The protocol is `RealityEngine_CI/docs/RUST_VALIDATION_WORKFLOW.md`; this
//! file implements its R1 tier. It needs nothing but this checkout: the machines
//! come from `conformance/corpus/`, pinned by `conformance/corpus.lock.json`, and
//! every request goes through the HTTP router in process, so the surface under
//! test is the one a client sees.
//!
//! The oracle is what the corpus itself declares, never another engine:
//!
//! - `expectedOutputCount`: how many process steps of the sequence emit a
//!   non-null arbitrated output.
//! - `expectedOutputVector`: the vector of the last output the sequence emits.
//!
//! That reading of the two keys is this file's interpretation; no specification
//! defines them. R2 holds the declarations against the three reference engines,
//! and a disagreement there is a document-versus-engines case for the owner to
//! arbitrate, not something to settle here.
//!
//! Canonical keys are read first (`eventDimension`, `outputs[].vector`,
//! `inputEvent`, `events`). Where Rust answers with a non-canonical key, the
//! driver falls back so the downstream checks still measure something, and the
//! key itself is recorded as a failing check.
//!
//! Failures are ratcheted against `conformance/known-failures.json`: an
//! unlisted failure fails the run, and so does a listed one that now passes
//! (the entry is stale and must be removed). `R1_RECORD_KNOWN=1` rewrites the
//! file from the current failures with issue `UNCLASSIFIED`, which itself fails
//! until each entry names its issue.

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use reality_engine::{Engine, app};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use tower::ServiceExt;

const SEED_DIMENSION: usize = 7680;
const UNCLASSIFIED: &str = "UNCLASSIFIED";

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn read_json(path: &Path) -> Value {
    let text = fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("parse {}: {e}", path.display()))
}

async fn call(
    router: &axum::Router,
    method: &str,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(body.map_or_else(Body::empty, |b| Body::from(b.to_string())))
        .expect("request");
    let response = router
        .clone()
        .oneshot(request)
        .await
        .expect("router answers");
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, value)
}

/// One named check and its outcome.
#[derive(Default)]
struct Checks {
    results: BTreeMap<String, Result<String, String>>,
}

impl Checks {
    fn pass(&mut self, id: impl Into<String>, detail: impl Into<String>) {
        self.results.insert(id.into(), Ok(detail.into()));
    }
    fn fail(&mut self, id: impl Into<String>, detail: impl Into<String>) {
        self.results.insert(id.into(), Err(detail.into()));
    }
    /// Records the first failure of a check that many paths can hit, and a pass
    /// only if nothing has failed it.
    fn once(&mut self, id: &str, ok: bool, detail: &str) {
        match self.results.get(id) {
            Some(Err(_)) => {}
            _ if ok => {
                self.results
                    .entry(id.to_owned())
                    .or_insert_with(|| Ok(detail.to_owned()));
            }
            _ => {
                self.results.insert(id.to_owned(), Err(detail.to_owned()));
            }
        }
    }
}

fn parse_vector(value: &Value) -> Option<Vec<f64>> {
    let parsed;
    let array = match value {
        Value::String(text) => {
            parsed = serde_json::from_str::<Value>(text).ok()?;
            parsed.as_array()?
        }
        Value::Array(items) => items,
        _ => return None,
    };
    array.iter().map(Value::as_f64).collect()
}

fn same(a: &[f64], b: &[f64]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| (x - y).abs() < 1e-9)
}

async fn replay_machine(rel_file: &str, definition: &Value, checks: &mut Checks) {
    let router = app(Engine::new(SEED_DIMENSION));
    let machine = definition.get("machine").unwrap_or(definition);

    // Corpus-load guard: the machine registers and keeps its events. A loader
    // reading retired key names registers it with zero (RealityEngine_Rust#3).
    let (status, registered) =
        call(&router, "POST", "/api/machines", Some(definition.clone())).await;
    let id = registered["machine"]["id"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    let total_events = registered["machine"]["totalEvents"].as_u64().unwrap_or(0);
    let declared_events: usize = machine["sequences"]
        .as_array()
        .map(|s| {
            s.iter()
                .map(|q| q["events"].as_array().map_or(0, Vec::len))
                .sum()
        })
        .unwrap_or(0);
    if status != StatusCode::OK || id.is_empty() {
        checks.fail(
            format!("load:{rel_file}"),
            format!("POST /api/machines answered {status}"),
        );
        return;
    }
    if total_events as usize == declared_events && declared_events > 0 {
        checks.pass(format!("load:{rel_file}"), format!("{total_events} events"));
    } else {
        checks.fail(
            format!("load:{rel_file}"),
            format!("registered {total_events} events; the file declares {declared_events}"),
        );
    }

    let (_, config) = call(&router, "GET", "/api/config", None).await;
    let canonical_dimension = config["eventDimension"].as_u64();
    checks.once(
        "key:config.eventDimension",
        canonical_dimension.is_some(),
        "GET /api/config must report eventDimension",
    );
    let dimension = canonical_dimension
        .or_else(|| config["dimension"].as_u64())
        .unwrap_or(0) as usize;
    let offset = machine["perceptualMapping"]["input"]["offset"]
        .as_u64()
        .unwrap_or(0) as usize;

    let Some(sequences) = machine["inputSequences"].as_array() else {
        return;
    };
    for (index, sequence) in sequences.iter().enumerate() {
        let metadata = &sequence["metadata"];
        let expected_count = metadata["expectedOutputCount"].as_u64();
        let expected_vector = parse_vector(&metadata["expectedOutputVector"]);
        let name = sequence["name"].as_str().unwrap_or("unnamed");
        let check = format!("seq:{rel_file}#{index}");
        if expected_count.is_none() && expected_vector.is_none() {
            checks.results.insert(
                check,
                Ok(format!("no declared oracle ({name}); not compared")),
            );
            continue;
        }

        call(&router, "POST", "/api/engine/reset", None).await;
        let mut emitted: Vec<Vec<f64>> = Vec::new();
        let mut problem = None;
        for event in sequence["events"].as_array().into_iter().flatten() {
            let Some(values) = parse_vector(event) else {
                problem = Some("an event is not a numeric vector".to_owned());
                break;
            };
            if offset + values.len() > dimension {
                problem = Some(format!("input region ends past eventDimension {dimension}"));
                break;
            }
            let mut universal = vec![0.0; dimension];
            universal[offset..offset + values.len()].copy_from_slice(&values);
            let (status, response) = call(
                &router,
                "POST",
                "/api/engine/process",
                Some(json!({ "vector": universal })),
            )
            .await;
            if status != StatusCode::OK {
                problem = Some(format!("POST /api/engine/process answered {status}"));
                break;
            }
            let result = &response["result"];
            checks.once(
                "key:engine-process.inputEvent",
                result.get("inputEvent").is_some(),
                "result must carry inputEvent",
            );
            for output in result["outputs"].as_array().into_iter().flatten() {
                // SURFACE_SPEC: outputs[] is the arbitrated machine output itself.
                let canonical = parse_vector(&output["vector"]);
                checks.once(
                    "shape:engine-process.outputs",
                    canonical.is_some(),
                    "outputs[] entries must be the arbitrated output ({vector, ...}), not a per-machine result",
                );
                if let Some(vector) =
                    canonical.or_else(|| parse_vector(&output["machineOutput"]["vector"]))
                {
                    emitted.push(vector);
                }
            }
        }
        if let Some(problem) = problem {
            checks.fail(check, format!("{name}: {problem}"));
            continue;
        }

        let mut wrong = Vec::new();
        if let Some(count) = expected_count
            && emitted.len() as u64 != count
        {
            wrong.push(format!(
                "emitted {} outputs, declared {count}",
                emitted.len()
            ));
        }
        if let Some(vector) = &expected_vector {
            match emitted.last() {
                Some(last) if same(last, vector) => {}
                Some(last) => wrong.push(format!("last output {last:?}, declared {vector:?}")),
                None => wrong.push(format!("no output, declared {vector:?}")),
            }
        }
        if wrong.is_empty() {
            checks.pass(check, name.to_owned());
        } else {
            checks.fail(check, format!("{name}: {}", wrong.join("; ")));
        }
    }
}

#[tokio::test]
async fn r1_corpus_conformance() {
    let base = root().join("conformance");
    let lock = read_json(&base.join("corpus.lock.json"));
    let mut checks = Checks::default();

    let files = lock["files"].as_array().expect("lock lists files");
    assert!(!files.is_empty(), "corpus.lock.json lists no files");
    for file in files {
        let rel_file = file["relFile"].as_str().expect("relFile");
        let definition = read_json(&base.join("corpus").join(rel_file));
        replay_machine(rel_file, &definition, &mut checks).await;
    }
    // A shape check no output reached is reported as such, never left absent.
    checks
        .results
        .entry("shape:engine-process.outputs".to_owned())
        .or_insert_with(|| {
            Ok("not reached: no sequence emitted an output; not compared".to_owned())
        });

    let known_path = base.join("known-failures.json");
    let known: BTreeMap<String, Value> = if known_path.exists() {
        serde_json::from_value(read_json(&known_path)["entries"].clone()).unwrap_or_default()
    } else {
        BTreeMap::new()
    };

    if std::env::var("R1_RECORD_KNOWN").as_deref() == Ok("1") {
        let entries: BTreeMap<_, _> = checks
            .results
            .iter()
            .filter_map(|(id, r)| r.as_ref().err().map(|d| (id, d)))
            .map(|(id, detail)| {
                let issue = known
                    .get(id)
                    .and_then(|e| e["issue"].as_str())
                    .unwrap_or(UNCLASSIFIED);
                (id.clone(), json!({ "issue": issue, "detail": detail }))
            })
            .collect();
        let body = json!({
            "schema": 1,
            "purpose": "R1 ratchet: every entry is a failure with an issue. Rewrite with R1_RECORD_KNOWN=1, then classify each UNCLASSIFIED entry by hand.",
            "entries": entries,
        });
        fs::write(
            &known_path,
            serde_json::to_string_pretty(&body).unwrap() + "\n",
        )
        .expect("write known failures");
    }

    let mut unexpected = Vec::new();
    let mut stale = Vec::new();
    let mut unclassified = Vec::new();
    let mut report = BTreeMap::new();
    let (mut passed, mut known_failing, mut not_compared) = (0, 0, 0);
    for (id, result) in &checks.results {
        let listed = known.get(id);
        let verdict = match (result, listed) {
            (Ok(detail), None) if detail.contains("not compared") => {
                not_compared += 1;
                "not-compared"
            }
            (Ok(_), None) => {
                passed += 1;
                "pass"
            }
            (Ok(_), Some(_)) => {
                stale.push(id.clone());
                "stale-known-failure"
            }
            (Err(_), Some(entry)) => {
                known_failing += 1;
                if entry["issue"].as_str().is_none_or(|i| i == UNCLASSIFIED) {
                    unclassified.push(id.clone());
                }
                "known-failure"
            }
            (Err(_), None) => {
                unexpected.push(id.clone());
                "UNEXPECTED-FAILURE"
            }
        };
        let detail = match result {
            Ok(d) | Err(d) => d,
        };
        report.insert(
            id.clone(),
            json!({ "verdict": verdict, "detail": detail, "known": listed }),
        );
    }
    let orphaned: Vec<_> = known
        .keys()
        .filter(|id| !checks.results.contains_key(*id))
        .cloned()
        .collect();

    let report_dir = root().join("target").join("conformance");
    fs::create_dir_all(&report_dir).expect("report dir");
    let summary = json!({
        "tier": "R1",
        "corpus": lock["source"],
        "manifest": lock["manifest"],
        "counts": {
            "pass": passed,
            "knownFailure": known_failing,
            "notCompared": not_compared,
            "unexpectedFailure": unexpected.len(),
            "staleKnownFailure": stale.len(),
            "orphanedKnownFailure": orphaned.len(),
        },
        "checks": report,
    });
    fs::write(
        report_dir.join("r1-report.json"),
        serde_json::to_string_pretty(&summary).unwrap(),
    )
    .expect("write report");

    println!(
        "R1: {passed} pass, {known_failing} known failure, {not_compared} not compared (no declared oracle), \
         {} unexpected, {} stale, {} orphaned; report: target/conformance/r1-report.json",
        unexpected.len(),
        stale.len(),
        orphaned.len()
    );
    for id in &unexpected {
        println!("  UNEXPECTED {id}: {:?}", checks.results[id]);
    }
    let mut problems = Vec::new();
    if !unexpected.is_empty() {
        problems.push(format!("{} unexpected failure(s)", unexpected.len()));
    }
    if !stale.is_empty() {
        problems.push(format!(
            "known failures that now pass, remove them: {stale:?}"
        ));
    }
    if !orphaned.is_empty() {
        problems.push(format!("known failures naming no check: {orphaned:?}"));
    }
    if !unclassified.is_empty() {
        problems.push(format!(
            "{} known failure(s) still {UNCLASSIFIED}",
            unclassified.len()
        ));
    }
    assert!(problems.is_empty(), "R1 failed: {}", problems.join("; "));
}
