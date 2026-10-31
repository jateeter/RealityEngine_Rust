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
                candidate, candidate_error = drive(args.candidate, machine_id, events)
                reference, reference_error = drive(args.reference, machine_id, events)
                entry.update(candidate=candidate, reference=reference)
                if candidate_error or reference_error:
                    entry.update(verdict="error", detail={"candidate": candidate_error, "reference": reference_error})
                    counts["error"] += 1
                elif same(candidate, reference):
                    entry["verdict"] = "identical"
                    counts["identical"] += 1
                else:
                    first = next(i for i, (c, r) in enumerate(zip(candidate, reference)) if not same(c, r))
                    entry.update(verdict="different", firstDifferentStep=first)
                    counts["different"] += 1
            report[key] = entry

    compared = counts["identical"] + counts["different"]
    print(f"differential: {counts['identical']} of {compared} compared sequences identical; "
          f"{counts['different']} different, {counts['error']} error, {counts['notResident']} not resident "
          f"({args.candidate} vs {args.reference})")
    for key, entry in report.items():
        if entry["verdict"] in ("different", "error"):
            step = entry.get("firstDifferentStep")
            print(f"  {entry['verdict'].upper()} {key} ({entry['name']})"
                  + (f" at step {step}: candidate {entry['candidate'][step]} | reference {entry['reference'][step]}"
                     if step is not None else f": {entry.get('detail')}"))
    if args.out:
        pathlib.Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        pathlib.Path(args.out).write_text(json.dumps(
            {"candidate": args.candidate, "reference": args.reference, "corpus": str(corpus),
             "counts": counts, "sequences": report}, indent=2))
    failed = counts["different"] or counts["error"] or counts["notResident"]
    return 1 if failed or compared == 0 else 0


if __name__ == "__main__":
    sys.exit(main())
