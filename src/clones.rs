//! Cloneable machines and the clone space (RealityEngine_CI SURFACE_SPEC.md,
//! "Cloneable machines"; RealityEngine_CI#713, localAssetStorageStack#8, #71).
//!
//! Recognition is decided here, at load, by the specification's nine-rule
//! table; nothing in a machine file marks a machine cloneable. The clone space
//! is a band of 64-cell blocks in the nest layout, allocated lowest free first,
//! each with a generation the recycle route must present. Follows C++
//! `clone_space.cpp` and its server routes, which agree 3-of-3 with LSP and
//! Scala (RealityEngine_Machines `clone-recycle-quorum.spec.ts`).

use super::*;
use std::collections::BTreeSet;

/// Fixed by the specification ("The clone space"); RealityEngine_Machines
/// `domains/clone-space.json` records the same values.
pub(crate) const BAND_OFFSET: usize = 20480;
pub(crate) const BAND_LENGTH: usize = 65536;
pub(crate) const BLOCK_CELLS: usize = 64;
pub(crate) const QUARTER: usize = 16;
pub(crate) const BLOCKS: usize = BAND_LENGTH / BLOCK_CELLS; // 1024
pub(crate) const BUS_CELLS: usize = 4;
pub(crate) const MAX_ACTIVE_DEFAULT: usize = 8;

pub(crate) struct Verdict {
    pub cloneable: bool,
    /// The first rule the machine fails, as the specification's reason code.
    pub reason: &'static str,
    /// Folded fanout cells + bus cells, present whenever rule 9 was evaluated.
    pub n: Option<usize>,
    /// Folded fanout cells alone (rule 9).
    pub folded: usize,
}

fn fail(reason: &'static str) -> Verdict {
    Verdict {
        cloneable: false,
        reason,
        n: None,
        folded: 0,
    }
}

fn edges(machine: &Machine) -> &[Value] {
    machine.raw["metadata"]["interconnections"]
        .as_array()
        .map_or(&[], Vec::as_slice)
}

fn is_producer(edge: &Value) -> bool {
    edge["type"].as_str() == Some("published-domain-bus-producer")
}

/// The machine's file stem: its corpus file name without `.json`. A machine
/// registered from a request body has no file, so no handoff can name it.
fn file_stem(machine: &Machine) -> &str {
    machine
        .source_file
        .strip_suffix(".json")
        .unwrap_or(&machine.source_file)
}

fn is_outgoing_handoff(edge: &Value, stem: &str) -> bool {
    !stem.is_empty()
        && edge["type"].as_str() == Some("document-workflow-handoff")
        && edge["sourceMachine"].as_str() == Some(stem)
}

/// The file names other machines' `published-domain-bus-producer` edges target.
pub(crate) fn bus_target_files(machines: &BTreeMap<String, Machine>) -> BTreeSet<String> {
    machines
        .values()
        .flat_map(edges)
        .filter(|edge| is_producer(edge))
        .filter_map(|edge| edge["targetMachineFile"].as_str())
        .filter(|file| !file.is_empty())
        .map(str::to_owned)
        .collect()
}

/// A region length as declared, `None` when the machine has no
/// `perceptualMapping`.
fn region_length(machine: &Machine, which: &str) -> Option<usize> {
    let mapping = machine
        .raw
        .get("perceptualMapping")
        .filter(|m| m.is_object())?;
    let fallback = if which == "input" {
        machine.input_length
    } else {
        0
    };
    Some(integer(mapping[which].get("length"), fallback))
}

/// The nine-rule table, in order. `bus_targets` is [`bus_target_files`] over
/// every registered machine: rule 3 needs the whole set.
pub(crate) fn assess(machine: &Machine, bus_targets: &BTreeSet<String>) -> Verdict {
    let stem = file_stem(machine);
    let (mut producers, mut handoffs, mut folded) = (0, 0, 0);
    for edge in edges(machine) {
        if is_producer(edge) {
            producers += 1;
            folded += edge["targetInputElements"].as_array().map_or(0, Vec::len);
        } else if is_outgoing_handoff(edge, stem) {
            handoffs += 1;
        }
    }
    let metadata = &machine.raw["metadata"];
    // 1. It dispatches an agentic action.
    if metadata.get("agentBinding").is_none_or(Value::is_null) && handoffs == 0 {
        return fail("no-agentic-action");
    }
    // 2. It has fanout.
    if producers == 0 && handoffs == 0 {
        return fail("no-fanout");
    }
    // 3. It is not a bus target.
    if !machine.source_file.is_empty() && bus_targets.contains(&machine.source_file) {
        return fail("bus-target");
    }
    // 4. Not life-safety.
    if metadata["severity"].as_str() == Some("life-safety") {
        return fail("life-safety");
    }
    // 5. Not a fixture: string tags only.
    if metadata["tags"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .any(|tag| tag.contains("fixture"))
    {
        return fail("fixture");
    }
    // 6. Not opted out.
    if metadata["cloneable"].as_bool() == Some(false) {
        return fail("opted-out");
    }
    // 7, 8. Its own regions fit their quarters. No perceptualMapping fails 7.
    match region_length(machine, "input") {
        Some(length) if length <= QUARTER => {}
        _ => return fail("input-too-long"),
    }
    if region_length(machine, "output").unwrap_or(0) > QUARTER {
        return fail("output-too-long");
    }
    // 9. The cells its fanout folds into fit the fanout quarter.
    let cloneable = folded <= QUARTER;
    Verdict {
        cloneable,
        reason: if cloneable { "" } else { "fanout-too-wide" },
        n: Some(folded + BUS_CELLS),
        folded,
    }
}

/// `GET /api/machines/cloneable`: every registered machine except clones,
/// sorted by id, with its verdict.
pub(crate) fn listing(machines: &BTreeMap<String, Machine>) -> Value {
    let targets = bus_target_files(machines);
    let rows: Vec<Value> = machines
        .iter()
        .filter(|(_, m)| m.clone_of.is_none())
        .map(|(id, m)| {
            let v = assess(m, &targets);
            json!({
                "id": id,
                "cloneable": v.cloneable,
                "reason": if v.cloneable { Value::Null } else { json!(v.reason) },
                "n": v.n,
            })
        })
        .collect();
    json!({ "machines": rows })
}

#[derive(Clone)]
pub(crate) struct CloneRecord {
    pub clone_id: String,
    pub machine_id: String,
    pub engine_instance: String,
    pub envelope_id: String,
    pub asset_id: String,
    pub block: usize,
    pub generation: u64,
    pub input_length: usize,
    pub output_length: usize,
    pub fanout_length: usize,
}

fn region(offset: usize, length: usize) -> Value {
    json!({ "offset": offset, "length": length })
}

impl CloneRecord {
    pub fn offset(&self) -> usize {
        BAND_OFFSET + BLOCK_CELLS * self.block
    }
    pub fn input(&self) -> (usize, usize) {
        (self.offset(), self.input_length)
    }
    pub fn output(&self) -> (usize, usize) {
        (self.offset() + QUARTER, self.output_length)
    }
    fn block_json(&self) -> Value {
        json!({ "index": self.block, "offset": self.offset(), "length": BLOCK_CELLS })
    }
    /// The `201` body, and each entry of `GET /api/engine/clones`.
    pub fn to_json(&self) -> Value {
        json!({
            "cloneId": self.clone_id,
            "machineId": self.machine_id,
            "engineInstance": self.engine_instance,
            "envelopeId": self.envelope_id,
            "assetId": self.asset_id,
            "block": self.block_json(),
            "input": region(self.offset(), self.input_length),
            "output": region(self.offset() + QUARTER, self.output_length),
            "fanout": region(self.offset() + 2 * QUARTER, self.fanout_length),
            "bus": region(self.offset() + 3 * QUARTER, BUS_CELLS),
            "generation": self.generation,
        })
    }
}

/// The live clones and every block's generation. A restart loses them all,
/// and every generation starts again at 0.
pub(crate) struct Space {
    generations: Vec<u64>,
    by_block: BTreeMap<usize, CloneRecord>,
}

impl Default for Space {
    fn default() -> Self {
        Self {
            generations: vec![0; BLOCKS],
            by_block: BTreeMap::new(),
        }
    }
}

impl Space {
    pub fn lowest_free_block(&self) -> Option<usize> {
        (0..BLOCKS).find(|k| !self.by_block.contains_key(k))
    }
    pub fn live_of(&self, machine_id: &str) -> usize {
        self.by_block
            .values()
            .filter(|c| c.machine_id == machine_id)
            .count()
    }
    pub fn by_id(&self, clone_id: &str) -> Option<&CloneRecord> {
        self.by_block.values().find(|c| c.clone_id == clone_id)
    }
    pub fn by_envelope(&self, envelope_id: &str) -> Option<&CloneRecord> {
        self.by_block
            .values()
            .find(|c| c.envelope_id == envelope_id)
    }
    pub fn generation(&self, block: usize) -> u64 {
        self.generations[block]
    }
    pub fn is_clone(&self, machine_id: &str) -> bool {
        self.by_id(machine_id).is_some()
    }
    pub fn add(&mut self, clone: CloneRecord) {
        self.by_block.insert(clone.block, clone);
    }
    /// Frees the clone's block and increments its generation; the new one.
    pub fn recycle(&mut self, clone_id: &str) -> Option<(CloneRecord, u64)> {
        let block = self.by_id(clone_id)?.block;
        let freed = self.by_block.remove(&block)?;
        self.generations[block] += 1;
        Some((freed, self.generations[block]))
    }
    /// Live clones in block order.
    pub fn live(&self) -> Vec<Value> {
        self.by_block.values().map(CloneRecord::to_json).collect()
    }
}

/// An error carrying fields beside its message: a clone refusal's `reason`, a
/// recycle's current `generation`.
fn refused(status: StatusCode, message: &str, extra: Value) -> Response {
    let mut body = extra;
    body["error"] = json!(message);
    (status, Json(body)).into_response()
}

fn plain(status: StatusCode, message: impl Into<String>) -> Response {
    (status, Json(json!({ "error": message.into() }))).into_response()
}

/// Percent-decoded, as the specification requires of `asset` and `envelope`.
/// (axum's `Path` has already decoded the path's ids.)
fn decode(raw: &str) -> String {
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("");
                match u8::from_str_radix(hex, 16) {
                    Ok(b) => {
                        out.push(b);
                        i += 3;
                    }
                    Err(_) => {
                        out.push(b'%');
                        i += 1;
                    }
                }
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The raw query string's value for `key`, percent-decoded; empty when absent.
fn query_value(query: Option<&str>, key: &str) -> Option<String> {
    query?
        .split('&')
        .filter_map(|pair| pair.split_once('=').or(Some((pair, ""))))
        .find(|(k, _)| *k == key)
        .map(|(_, v)| decode(v))
}

impl Engine {
    /// `POST /api/engine/clone/machine/:machineId`, the checks in the
    /// specification's order, so engines refusing the same request refuse it
    /// alike.
    fn clone_machine(&mut self, machine_id: &str, asset: &str, envelope: &str) -> Response {
        if asset.is_empty() || envelope.is_empty() {
            return plain(StatusCode::BAD_REQUEST, "asset and envelope are required");
        }
        let Some(template) = self.machines.get(machine_id) else {
            return plain(StatusCode::NOT_FOUND, "Machine not found");
        };
        if template.clone_of.is_some() {
            return refused(
                StatusCode::BAD_REQUEST,
                "a clone is not cloneable",
                json!({ "reason": "clone" }),
            );
        }
        let verdict = assess(template, &bus_target_files(&self.machines));
        if !verdict.cloneable {
            return refused(
                StatusCode::BAD_REQUEST,
                "machine is not cloneable",
                json!({ "reason": verdict.reason }),
            );
        }
        if self.engine_instance.is_empty() {
            return plain(
                StatusCode::CONFLICT,
                "engine launched without an INSTANCE_ID",
            );
        }
        if let Some(held) = self.clone_space.by_envelope(envelope) {
            if held.machine_id == machine_id {
                return (StatusCode::OK, Json(held.to_json())).into_response();
            }
            return plain(
                StatusCode::CONFLICT,
                format!("envelope {envelope} holds a clone of another machine"),
            );
        }
        if self.clone_space.live_of(machine_id) >= self.clone_max_active {
            return plain(StatusCode::CONFLICT, "template is at clone.maxActive");
        }
        let Some(block) = self.clone_space.lowest_free_block() else {
            return plain(StatusCode::CONFLICT, "the clone band has no free block");
        };

        let record = CloneRecord {
            clone_id: format!("{machine_id}.{}.{envelope}", self.engine_instance),
            machine_id: machine_id.to_owned(),
            engine_instance: self.engine_instance.clone(),
            envelope_id: envelope.to_owned(),
            asset_id: asset.to_owned(),
            block,
            generation: self.clone_space.generation(block),
            input_length: region_length(template, "input").unwrap_or(0),
            output_length: region_length(template, "output").unwrap_or(0),
            fanout_length: verdict.folded,
        };

        // The template as declared, at its initial state: only the id and the
        // two regions differ. It starts uninhibited, whatever the template's
        // control says.
        let mut clone = template.clone();
        clone.reset();
        clone.id = record.clone_id.clone();
        clone.source_file.clear();
        clone.transitions_inhibited = false;
        clone.clone_of = Some(machine_id.to_owned());
        clone.asset_id = asset.to_owned();
        clone.envelope_id = envelope.to_owned();
        let (input_offset, input_length) = record.input();
        let (output_offset, output_length) = record.output();
        clone.input_offset = input_offset;
        clone.raw["perceptualMapping"]["input"] =
            json!({ "offset": input_offset, "length": input_length });
        clone.raw["perceptualMapping"]["output"] =
            json!({ "offset": output_offset, "length": output_length });
        // Not load_machine: that versions a resident name, and a clone keeps
        // its template's name unchanged.
        self.admit(clone);
        let body = record.to_json();
        self.clone_space.add(record);
        (StatusCode::CREATED, Json(body)).into_response()
    }

    /// `POST /api/engine/recycle/machine/:cloneId?generation=`: 400, then 404,
    /// then 409.
    fn recycle_clone(&mut self, clone_id: &str, generation: Option<&str>) -> Response {
        let Some(generation) = generation.and_then(parse_whole) else {
            return plain(
                StatusCode::BAD_REQUEST,
                "generation is required, as an integer",
            );
        };
        let Some(live) = self.clone_space.by_id(clone_id) else {
            return plain(StatusCode::NOT_FOUND, format!("No live clone {clone_id}"));
        };
        if i128::from(live.generation) != generation {
            return refused(
                StatusCode::CONFLICT,
                "generation is not the clone's",
                json!({ "generation": live.generation }),
            );
        }
        self.remove_machine(clone_id);
        let (freed, next) = self
            .clone_space
            .recycle(clone_id)
            .expect("a live clone was found above");
        (
            StatusCode::OK,
            Json(json!({ "cloneId": clone_id, "block": freed.block_json(), "generation": next })),
        )
            .into_response()
    }
}

/// A whole number, optionally negative, and nothing else (C++ `parse_whole`).
fn parse_whole(text: &str) -> Option<i128> {
    let digits = text.strip_prefix('-').unwrap_or(text);
    if text.is_empty() || text.len() > 18 || digits.is_empty() {
        return None;
    }
    if !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    text.parse().ok()
}

/// A clone changes only through recycle: `PUT`, `PATCH` or `DELETE` on one
/// would leave its block held by a machine that no longer exists.
pub(crate) fn clone_refused() -> (StatusCode, Json<Value>) {
    (
        StatusCode::CONFLICT,
        Json(json!({
            "error": "a clone changes only through POST /api/engine/recycle/machine/:cloneId"
        })),
    )
}

pub(crate) async fn cloneable(State(engine): State<SharedEngine>) -> Response {
    match engine.read() {
        Ok(state) => Json(listing(&state.machines)).into_response(),
        Err(_) => error_response(lock_error()).into_response(),
    }
}

pub(crate) async fn clone_machine(
    State(engine): State<SharedEngine>,
    Path(machine_id): Path<String>,
    uri: Uri,
) -> Response {
    let query = uri.query();
    let asset = query_value(query, "asset").unwrap_or_default();
    let envelope = query_value(query, "envelope").unwrap_or_default();
    match engine.write() {
        Ok(mut state) => state.clone_machine(&machine_id, &asset, &envelope),
        Err(_) => error_response(lock_error()).into_response(),
    }
}

pub(crate) async fn recycle(
    State(engine): State<SharedEngine>,
    Path(clone_id): Path<String>,
    uri: Uri,
) -> Response {
    let generation = query_value(uri.query(), "generation");
    match engine.write() {
        Ok(mut state) => state.recycle_clone(&clone_id, generation.as_deref()),
        Err(_) => error_response(lock_error()).into_response(),
    }
}

pub(crate) async fn clones(State(engine): State<SharedEngine>) -> Response {
    match engine.read() {
        Ok(state) => Json(json!({ "clones": state.clone_space.live() })).into_response(),
        Err(_) => error_response(lock_error()).into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use tower::ServiceExt;

    const PRODUCER: &str = r#"{"type": "published-domain-bus-producer", "targetMachineFile": "Bus.json", "targetInputElements": [0, 1, 2]}"#;

    /// A corpus machine loaded from `file`, with `metadata` and its regions.
    fn fixture(engine: &mut Engine, file: &str, metadata: &str, mapping: Option<(usize, usize)>) {
        let stem = file.strip_suffix(".json").unwrap();
        let id = canonical_machine_id(stem);
        let pm = mapping.map_or(String::new(), |(i, o)| {
            format!(r#""perceptualMapping": {{"input": {{"offset": 0, "length": {i}}}, "output": {{"offset": 100, "length": {o}}}}},"#)
        });
        let raw = format!(
            r#"{{"version": "1.0.0", "id": "{id}", "machine": {{"name": "{stem}", {pm} "metadata": {metadata},
              "sequences": [{{"id": "s", "events": [
                {{"id": "a", "isInitial": true, "elements": [{{"value": 1}}], "nextEventIds": ["b"]}},
                {{"id": "b", "elements": [{{"value": 1}}], "outputEvents": [{{"id": "o", "vector": [1]}}]}}]}}]}}}}"#
        );
        let id = engine
            .load_machine(serde_json::from_str(&raw).unwrap())
            .unwrap()["id"]
            .as_str()
            .unwrap()
            .to_owned();
        engine.machines.get_mut(&id).unwrap().source_file = file.to_owned();
    }

    fn agentic(extra: &str, edges: &str) -> String {
        let extra = if extra.is_empty() {
            String::new()
        } else {
            format!("{extra}, ")
        };
        format!(r#"{{"agentBinding": {{"agent": "a"}}, {extra}"interconnections": [{edges}]}}"#)
    }

    fn corpus() -> Engine {
        let mut e = Engine::new(8);
        e.engine_instance = "rust-1".to_owned();
        let four = Some((4, 4));
        fixture(&mut e, "Ok.json", &agentic("", PRODUCER), four);
        fixture(
            &mut e,
            "NoAgent.json",
            &format!(r#"{{"interconnections": [{PRODUCER}]}}"#),
            four,
        );
        let handoff = r#"{"interconnections": [{"type": "document-workflow-handoff", "sourceMachine": "Handoff"}]}"#;
        fixture(&mut e, "Handoff.json", handoff, four);
        fixture(
            &mut e,
            "HandoffElsewhere.json",
            &handoff.replace("\"Handoff\"", "\"Other\""),
            four,
        );
        fixture(&mut e, "NoFanout.json", &agentic("", ""), four);
        fixture(&mut e, "Bus.json", &agentic("", PRODUCER), four);
        fixture(
            &mut e,
            "Safety.json",
            &agentic(r#""severity": "life-safety""#, PRODUCER),
            four,
        );
        fixture(
            &mut e,
            "Fixture.json",
            &agentic(r#""tags": ["x", "test-fixture"]"#, PRODUCER),
            four,
        );
        fixture(
            &mut e,
            "Out.json",
            &agentic(r#""cloneable": false"#, PRODUCER),
            four,
        );
        fixture(&mut e, "WideIn.json", &agentic("", PRODUCER), Some((17, 4)));
        fixture(
            &mut e,
            "WideOut.json",
            &agentic("", PRODUCER),
            Some((16, 17)),
        );
        fixture(&mut e, "NoMapping.json", &agentic("", PRODUCER), None);
        let wide = [PRODUCER; 6].join(", "); // 18 folded cells
        fixture(&mut e, "WideFan.json", &agentic("", &wide), four);
        e
    }

    async fn send(
        app: &Router,
        method: &str,
        uri: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let request = Request::builder()
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

    #[test]
    fn each_rule_has_its_reason_code() {
        let e = corpus();
        let targets = bus_target_files(&e.machines);
        assert_eq!(targets, BTreeSet::from(["Bus.json".to_owned()]));
        let verdict = |stem: &str| {
            assess(
                &e.machines[&format!("machine-{}", stem.to_lowercase())],
                &targets,
            )
        };
        assert!(verdict("Ok").cloneable);
        assert_eq!(verdict("Ok").n, Some(7));
        assert!(verdict("Handoff").cloneable);
        assert_eq!(verdict("Handoff").n, Some(4), "a handoff folds no cells");
        for (stem, reason) in [
            ("NoAgent", "no-agentic-action"),
            ("HandoffElsewhere", "no-agentic-action"),
            ("NoFanout", "no-fanout"),
            ("Bus", "bus-target"),
            ("Safety", "life-safety"),
            ("Fixture", "fixture"),
            ("Out", "opted-out"),
            ("WideIn", "input-too-long"),
            ("WideOut", "output-too-long"),
            ("NoMapping", "input-too-long"),
            ("WideFan", "fanout-too-wide"),
        ] {
            let v = verdict(stem);
            assert!(!v.cloneable, "{stem}");
            assert_eq!(v.reason, reason, "{stem}");
        }
        assert_eq!(verdict("WideFan").n, Some(22));
        assert_eq!(verdict("Bus").n, None, "n only once rule 9 is evaluated");
        // A machine with no file is neither a handoff source nor a bus target.
        let mut bare = e.machines["machine-handoff"].clone();
        bare.source_file.clear();
        assert_eq!(assess(&bare, &targets).reason, "no-agentic-action");
    }

    #[tokio::test]
    async fn clone_recycle_and_the_listing() {
        let app = app(corpus());
        let (status, listing) = send(&app, "GET", "/api/machines/cloneable", None).await;
        assert_eq!(status, StatusCode::OK);
        let rows = listing["machines"].as_array().unwrap();
        let ids: Vec<&str> = rows.iter().map(|r| r["id"].as_str().unwrap()).collect();
        let mut sorted = ids.clone();
        sorted.sort_unstable();
        assert_eq!(ids, sorted, "sorted by id");
        let ok = rows.iter().find(|r| r["id"] == "machine-ok").unwrap();
        assert_eq!(
            ok,
            &json!({"id": "machine-ok", "cloneable": true, "reason": null, "n": 7})
        );
        let bus = rows.iter().find(|r| r["id"] == "machine-bus").unwrap();
        assert_eq!(
            bus,
            &json!({"id": "machine-bus", "cloneable": false, "reason": "bus-target", "n": null})
        );

        // The refusals, in the specified order.
        let clone = |q: &str| format!("/api/engine/clone/machine/machine-ok{q}");
        assert_eq!(
            send(&app, "POST", &clone("?asset=a"), None).await.0,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            send(
                &app,
                "POST",
                "/api/engine/clone/machine/nope?asset=a&envelope=e",
                None
            )
            .await
            .0,
            StatusCode::NOT_FOUND
        );
        let (status, body) = send(
            &app,
            "POST",
            "/api/engine/clone/machine/machine-bus?asset=a&envelope=e",
            None,
        )
        .await;
        assert_eq!(
            (status, &body["reason"]),
            (StatusCode::BAD_REQUEST, &json!("bus-target"))
        );

        // 201: block 0, the nest layout, percent-decoded asset and envelope.
        let (status, c) = send(
            &app,
            "POST",
            &clone("?asset=asset%2F42&envelope=env-1"),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(
            c,
            json!({
                "cloneId": "machine-ok.rust-1.env-1", "machineId": "machine-ok", "engineInstance": "rust-1",
                "envelopeId": "env-1", "assetId": "asset/42",
                "block": {"index": 0, "offset": 20480, "length": 64},
                "input": {"offset": 20480, "length": 4}, "output": {"offset": 20496, "length": 4},
                "fanout": {"offset": 20512, "length": 3}, "bus": {"offset": 20528, "length": 4},
                "generation": 0
            })
        );
        // A retried dispatch is idempotent; another machine on the envelope is not.
        assert_eq!(
            send(
                &app,
                "POST",
                &clone("?asset=asset%2F42&envelope=env-1"),
                None
            )
            .await,
            (StatusCode::OK, c.clone())
        );
        assert_eq!(
            send(
                &app,
                "POST",
                "/api/engine/clone/machine/machine-handoff?asset=a&envelope=env-1",
                None
            )
            .await
            .0,
            StatusCode::CONFLICT
        );
        // A clone is registered with its three fields, keeps the template's
        // name, starts at the initial state, and is not itself cloneable.
        let (_, m) = send(&app, "GET", "/api/machines/machine-ok.rust-1.env-1", None).await;
        assert_eq!(m["machine"]["cloneOf"], "machine-ok");
        assert_eq!(m["machine"]["assetId"], "asset/42");
        assert_eq!(m["machine"]["name"], "Ok");
        assert_eq!(
            m["machine"]["perceptualMapping"]["input"],
            json!({"offset": 20480, "length": 4})
        );
        let (status, body) = send(
            &app,
            "POST",
            "/api/engine/clone/machine/machine-ok.rust-1.env-1?asset=a&envelope=x",
            None,
        )
        .await;
        assert_eq!(
            (status, &body["reason"]),
            (StatusCode::BAD_REQUEST, &json!("clone"))
        );
        let (_, listing) = send(&app, "GET", "/api/machines/cloneable", None).await;
        assert!(
            !listing.to_string().contains("env-1"),
            "clones are not listed"
        );
        // A clone changes only through recycle.
        for method in ["PUT", "PATCH", "DELETE"] {
            let (status, _) = send(
                &app,
                method,
                "/api/machines/machine-ok.rust-1.env-1",
                Some(json!({"name": "x"})),
            )
            .await;
            assert_eq!(status, StatusCode::CONFLICT, "{method}");
        }

        // Lowest free first; maxActive bounds the template.
        let (_, c2) = send(&app, "POST", &clone("?asset=a&envelope=env-2"), None).await;
        assert_eq!(c2["block"]["index"], 1);
        let (status, _) = send(
            &app,
            "PUT",
            "/api/engine/config/clone.maxActive",
            Some(json!({"value": 2})),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            send(&app, "POST", &clone("?asset=a&envelope=env-3"), None)
                .await
                .0,
            StatusCode::CONFLICT
        );
        let (_, clones) = send(&app, "GET", "/api/engine/clones", None).await;
        assert_eq!(clones["clones"].as_array().unwrap().len(), 2);

        // Recycle: 400, 404, 409 with the current generation, then 200.
        let recycle = |id: &str, q: &str| format!("/api/engine/recycle/machine/{id}{q}");
        let id = "machine-ok.rust-1.env-1";
        assert_eq!(
            send(&app, "POST", &recycle(id, ""), None).await.0,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            send(&app, "POST", &recycle(id, "?generation=x"), None)
                .await
                .0,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            send(&app, "POST", &recycle("nope", "?generation=0"), None)
                .await
                .0,
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            send(&app, "POST", &recycle(id, "?generation=1"), None).await,
            (
                StatusCode::CONFLICT,
                json!({"error": "generation is not the clone's", "generation": 0})
            )
        );
        assert_eq!(
            send(&app, "POST", &recycle(id, "?generation=0"), None).await,
            (
                StatusCode::OK,
                json!({"cloneId": id, "block": {"index": 0, "offset": 20480, "length": 64}, "generation": 1})
            )
        );
        assert_eq!(
            send(&app, "GET", &format!("/api/machines/{id}"), None)
                .await
                .0,
            StatusCode::NOT_FOUND
        );
        // A replayed recycle finds no clone; block 0 is reused at generation 1.
        assert_eq!(
            send(&app, "POST", &recycle(id, "?generation=0"), None)
                .await
                .0,
            StatusCode::NOT_FOUND
        );
        let (status, c3) = send(&app, "POST", &clone("?asset=a&envelope=env-4"), None).await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(
            (c3["block"]["index"].clone(), c3["generation"].clone()),
            (json!(0), json!(1))
        );
    }

    #[tokio::test]
    async fn no_instance_id_refuses_with_409() {
        let mut e = corpus();
        e.engine_instance.clear();
        let app = app(e);
        let (status, _) = send(
            &app,
            "POST",
            "/api/engine/clone/machine/machine-ok?asset=a&envelope=e",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
    }
}
