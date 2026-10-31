#!/usr/bin/env python3
"""differential.py: the live comparison of a candidate RE against a reference RE.

The R2 driver (RealityEngine_CI/docs/RUST_VALIDATION_WORKFLOW.md). Both engines
must already be running on the same corpus. For every machine file in the
corpus directory, and every input sequence that machine declares, both engines
are reset and driven event by event through the single-machine route:
    POST /api/engine/reset
    POST /api/machines/<id>/process   {"inputEvent": <event>}
Each step's arbitrated output (`machineOutput.vector`) and presented output
(`mergedOutput`) are compared. The single-machine route needs no attribution,
and the canonical id `machine-<stem>` is the same on every engine, so a step on
one engine is paired with the same step on the other by construction.

With --mode perceive the same sequences drive the step path instead, with the
whole corpus resident: each event is placed at the machine's declared input
region and stepped through
    POST /api/perceive   {"domainVectors": [{"offset": <input.offset>, "values": <event>}]}
    GET  /api/engine/steps/<n>/pair
and every step's mergeBatch, eventBus, activeRegions, perceptualSpace and
committed (ISRE, OSRE) pair are compared key by key; timestamps are not.

No engine is the oracle. A difference carries both engines' emissions, and the
report never says which one is right. With three references (C++, LSP, Scala),
run once per reference: agreement with all three is the claim worth making.

Exit status: 0 every compared sequence identical; 1 a difference or an error;
2 usage; 3 the reference (or the candidate) did not answer, so nothing ran.

    scripts/differential.py --candidate http://127.0.0.1:5801 \\
        --reference http://127.0.0.1:5301 --corpus conformance/corpus \\
        --out target/conformance/differential-report.json
"""
import argparse, json, pathlib, sys, urllib.error, urllib.request

TOLERANCE = 1e-9


def canonical_id(stem):
    return "machine-" + "".join(c.lower() if c.isascii() and c.isalnum() else "-" for c in stem)


def call(base, method, path, body=None):
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(base + path, data=data, method=method,
                                     headers={"content-type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.status, json.loads(response.read() or b"null")
    except urllib.error.HTTPError as error:
        try:
            return error.code, json.loads(error.read() or b"null")
        except ValueError:
            return error.code, None


def same(a, b):
    if isinstance(a, list) and isinstance(b, list):
        return len(a) == len(b) and all(same(x, y) for x, y in zip(a, b))
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return abs(a - b) <= TOLERANCE
    return a == b


def step_of(body):
    output = (body or {}).get("machineOutput") or {}
    return {"machineOutput": output.get("vector"), "mergedOutput": (body or {}).get("mergedOutput")}


def drive(base, machine_id, events):
    status, _ = call(base, "POST", "/api/engine/reset")
    if status != 200:
        return None, f"reset answered {status}"
    steps = []
    for event in events:
        status, body = call(base, "POST", f"/api/machines/{machine_id}/process", {"inputEvent": event})
        if status != 200:
            return steps, f"process answered {status}: {(body or {}).get('error')}"
        steps.append(step_of(body))
    return steps, None


PERCEIVE_KEYS = ("stepNumber", "mergeBatch", "eventBus", "activeRegions", "perceptualSpace")


def perceive_input(form, width, offset, event):
    """The same input in either form. Dense exists because Scala reads only
    `vector` and answers any other form with an all-zero step."""
    if form == "domainVectors":
        return {"domainVectors": [{"offset": offset, "values": event}]}
    dense = [0.0] * max(width, offset + len(event))
    dense[offset:offset + len(event)] = event
    return {"vector": dense}


def perceive_drive(base, offset, events, form):
    status, config = call(base, "GET", "/api/config")
    if status != 200:
        return None, f"config answered {status}"
    status, _ = call(base, "POST", "/api/engine/reset")
    if status != 200:
        return None, f"reset answered {status}"
    steps = []
    for event in events:
        body = perceive_input(form, (config or {}).get("eventDimension", 0), offset, event)
        status, body = call(base, "POST", "/api/perceive", {**body, "includeMachineResults": False})
        if status != 200:
            return steps, f"perceive answered {status}: {(body or {}).get('error')}"
        step = {key: (body or {}).get(key) for key in PERCEIVE_KEYS}
        status, pair = call(base, "GET", f"/api/engine/steps/{step['stepNumber']}/pair?timeoutMs=1000")
        if status != 200:
            return steps, f"steps/{step['stepNumber']}/pair answered {status}"
        step["pair"] = pair
        steps.append(step)
    return steps, None


def differing_keys(c, r):
    return [k for k in (*PERCEIVE_KEYS, "pair") if not same_deep(c.get(k), r.get(k))]


def same_deep(a, b):
    if isinstance(a, dict) and isinstance(b, dict):
        return a.keys() == b.keys() and all(same_deep(a[k], b[k]) for k in a)
    if isinstance(a, list) and isinstance(b, list):
        return len(a) == len(b) and all(same_deep(x, y) for x, y in zip(a, b))
    return same(a, b)


def resident(base):
    status, body = call(base, "GET", "/api/machines")
    if status != 200:
        return None
    return {m.get("id") for m in (body or {}).get("machines", [])}


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--reference", required=True)
    parser.add_argument("--corpus", required=True, help="the machines directory both engines booted on")
    parser.add_argument("--out", help="write the full report here")
    parser.add_argument("--mode", choices=("machine", "perceive"), default="machine",
                        help="the single-machine route (default) or the step path")
    parser.add_argument("--perceive-form", choices=("domainVectors", "vector"), default="domainVectors",
                        help="how --mode perceive sends each event (default domainVectors)")
    args = parser.parse_args()
    corpus = pathlib.Path(args.corpus)
    if not corpus.is_dir():
        print(f"differential.py: no corpus directory at {corpus}", file=sys.stderr)
        return 2

    ids = {}
    for name, base in (("candidate", args.candidate), ("reference", args.reference)):
        try:
            ids[name] = resident(base)
        except (urllib.error.URLError, OSError) as error:
            ids[name] = None
            print(f"{name} {base} did not answer: {error}")
        if ids[name] is None:
            print(f"unavailable: the {name} at {base} could not list its machines; nothing compared")
            return 3

    report, counts = {}, {"identical": 0, "different": 0, "error": 0, "notResident": 0}
    for path in sorted(corpus.rglob("*.json")):
        rel = path.relative_to(corpus).as_posix()
        machine = json.loads(path.read_text()).get("machine", {})
        machine_id = canonical_id(path.stem)
        missing = [n for n in ("candidate", "reference") if machine_id not in ids[n]]
        for index, sequence in enumerate(machine.get("inputSequences") or []):
            key = f"{rel}#{index}"
            entry = {"machineId": machine_id, "name": sequence.get("name")}
            if missing:
                entry.update(verdict="not-resident", detail=f"{machine_id} not resident on {', '.join(missing)}")
                counts["notResident"] += 1
            else:
                events = sequence.get("events") or []
                if args.mode == "perceive":
                    offset = ((machine.get("perceptualMapping") or {}).get("input") or {}).get("offset", 0)
                    candidate, candidate_error = perceive_drive(args.candidate, offset, events, args.perceive_form)
                    reference, reference_error = perceive_drive(args.reference, offset, events, args.perceive_form)
                    equal = same_deep
                else:
                    candidate, candidate_error = drive(args.candidate, machine_id, events)
                    reference, reference_error = drive(args.reference, machine_id, events)
                    equal = same
                entry.update(candidate=candidate, reference=reference)
                if candidate_error or reference_error:
                    entry.update(verdict="error", detail={"candidate": candidate_error, "reference": reference_error})
                    counts["error"] += 1
                elif equal(candidate, reference):
                    entry["verdict"] = "identical"
                    counts["identical"] += 1
                else:
                    first = next((i for i, (c, r) in enumerate(zip(candidate, reference)) if not equal(c, r)),
                                 min(len(candidate), len(reference)))
                    entry.update(verdict="different", firstDifferentStep=first)
                    if args.mode == "perceive" and first < min(len(candidate), len(reference)):
                        entry["differingKeys"] = differing_keys(candidate[first], reference[first])
                    counts["different"] += 1
            report[key] = entry

    compared = counts["identical"] + counts["different"]
    print(f"differential: {counts['identical']} of {compared} compared sequences identical; "
          f"{counts['different']} different, {counts['error']} error, {counts['notResident']} not resident "
          f"({args.candidate} vs {args.reference})")
    for key, entry in report.items():
        if entry["verdict"] in ("different", "error"):
            step = entry.get("firstDifferentStep")
            if step is None:
                detail = f": {entry.get('detail')}"
            elif "differingKeys" in entry:
                detail = f" at step {step}: {', '.join(entry['differingKeys'])} differ (see --out)"
            elif step >= min(len(entry["candidate"]), len(entry["reference"])):
                detail = f": {len(entry['candidate'])} steps vs {len(entry['reference'])}"
            else:
                detail = f" at step {step}: candidate {entry['candidate'][step]} | reference {entry['reference'][step]}"
            print(f"  {entry['verdict'].upper()} {key} ({entry['name']}){detail}")
    if args.out:
        pathlib.Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        pathlib.Path(args.out).write_text(json.dumps(
            {"candidate": args.candidate, "reference": args.reference, "corpus": str(corpus),
             "counts": counts, "sequences": report}, indent=2))
    failed = counts["different"] or counts["error"] or counts["notResident"]
    return 1 if failed or compared == 0 else 0


if __name__ == "__main__":
    sys.exit(main())
