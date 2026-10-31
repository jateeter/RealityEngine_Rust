#!/usr/bin/env bash
# refresh-surface.sh — vendor the route tables of SURFACE_SPEC.md, or check them.
#
# The surface test (tests/surface.rs) probes every route the specification
# defines. It runs from this checkout alone, so the route list is a snapshot:
# parsed from a clean RealityEngine_CI checkout's SURFACE_SPEC.md into
# conformance/surface/spec-routes.json, with the spec's commit and sha256 and a
# digest of the parsed routes, which validate.sh recomputes.
#
# What is taken, and why:
#   RE   every row of the Reality Engine section's `| Method | Path | CPP | LSP |
#        Scala |` and `| Protocol | Path | ... |` tables — the implemented contract.
#   PE   the same tables in the Perception Engine section.
#   RE-phase2  the K-line snapshot routes, which the spec writes outside those
#        tables on purpose ("those declare what is implemented").
# The Manager section is Manager's surface, not an engine's, and is not taken.
#
# Local maintenance, never a hosted step: it reads a sibling checkout. R2 runs
# `--check` so a stale snapshot is named.
#
# Usage:
#   scripts/refresh-surface.sh [--spec FILE] [--check]
#     --spec FILE  Default: ../RealityEngine_CI/SURFACE_SPEC.md
#     --check      Compare the snapshot with the spec; write nothing. Exit 1 on drift.
set -euo pipefail

if [ -z "${BASH_VERSINFO:-}" ] || [ "${BASH_VERSINFO[0]}" -lt 5 ]; then
  echo "refresh-surface.sh: needs bash 5 (/opt/homebrew/bin/bash)" >&2
  exit 2
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
workspace="$(cd "$repo_root/.." && pwd)"
# A worktree under .claude/worktrees sits four levels below the workspace.
if [ ! -d "$workspace/RealityEngine_CI" ] && [ -d "$repo_root/../../../../RealityEngine_CI" ]; then
  workspace="$(cd "$repo_root/../../../.." && pwd)"
fi
spec="$workspace/RealityEngine_CI/SURFACE_SPEC.md"
check=false

while [ $# -gt 0 ]; do
  case "$1" in
    --spec) spec="$2"; shift 2 ;;
    --check) check=true; shift ;;
    -h|--help) sed -n '2,27p' "${BASH_SOURCE[0]}"; exit 0 ;;
    *) echo "refresh-surface.sh: unknown argument $1" >&2; exit 2 ;;
  esac
done

[ -f "$spec" ] || { echo "refresh-surface.sh: no spec at $spec" >&2; exit 2; }
spec_repo="$(cd "$(dirname "$spec")" && git rev-parse --show-toplevel)"

if [ "$check" = false ] && [ -n "$(git -C "$spec_repo" status --porcelain -- "$spec")" ]; then
  echo "refresh-surface.sh: $spec has uncommitted changes; refusing to vendor them" >&2
  exit 1
fi

python3 -I - "$repo_root" "$spec" "$spec_repo" "$check" <<'PY'
import hashlib, json, pathlib, re, subprocess, sys

repo, spec, spec_repo, check = (pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2]),
                                pathlib.Path(sys.argv[3]), sys.argv[4] == "true")
out = repo / "conformance" / "surface" / "spec-routes.json"

LAYERS = {
    "Reality Engine (RE) Surface": "RE",
    "Perception Engine (PE) Surface": "PE",
    "K-lines and runtime snapshots": "RE-phase2",
}
ENGINE_HEADER = re.compile(r"^\|\s*(Method|Protocol)\s*\|\s*Path\s*\|\s*CPP\s*\|\s*LSP\s*\|\s*Scala\s*\|")
PHASE2_HEADER = re.compile(r"^\|\s*Method\s*\|\s*Path\s*\|\s*Does\s*\|")
ROW = re.compile(r"^\|\s*([A-Za-z]+)\s*\|\s*`([^`]+)`\s*\|")
PROTOCOL = {"SSE": "SSE", "WebSocket": "WS"}

routes, section, group, table = [], None, None, False
for number, line in enumerate(spec.read_text().splitlines(), 1):
    if line.startswith("## "):
        section, group, table = line[3:].strip(), None, False
        continue
    if line.startswith("### "):
        group, table = line[4:].strip(), False
        continue
    layer = LAYERS.get(section)
    if layer is None:
        continue
    if ENGINE_HEADER.match(line) or (layer == "RE-phase2" and PHASE2_HEADER.match(line)):
        table = True
        continue
    if not line.startswith("|"):
        table = False
        continue
    row = ROW.match(line) if table else None
    if row:
        method = PROTOCOL.get(row.group(1), row.group(1).upper())
        path = row.group(2)
        routes.append({"id": f"{layer} {method} {path}", "layer": layer, "group": group,
                       "method": method, "path": path, "specLine": number})

ids = [r["id"] for r in routes]
duplicates = sorted({i for i in ids if ids.count(i) > 1})
if duplicates:
    print("refresh-surface.sh: the spec lists a route twice:", duplicates)
    sys.exit(1)

def digest(rows):
    canonical = json.dumps([{k: r[k] for k in ("id", "layer", "group", "method", "path")} for r in rows],
                           sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()

counts = {layer: sum(r["layer"] == layer for r in routes) for layer in LAYERS.values()}
if check:
    vendored = json.loads(out.read_text())
    if vendored["routesSha256"] != digest(routes):
        old = {r["id"] for r in vendored["routes"]}
        new = {r["id"] for r in routes}
        print("surface snapshot DRIFT against", spec)
        for i in sorted(new - old):
            print("  added  ", i)
        for i in sorted(old - new):
            print("  removed", i)
        print("re-vendor with scripts/refresh-surface.sh, then re-record the surface status")
        sys.exit(1)
    print(f"surface snapshot current: {counts} match {spec}")
    sys.exit(0)

git = lambda *a: subprocess.run(["git", "-C", str(spec_repo), *a], check=True,
                                capture_output=True, text=True).stdout.strip()
body = {
    "schema": 1,
    "purpose": "Route tables of SURFACE_SPEC.md; regenerate with scripts/refresh-surface.sh, never by hand",
    "source": {
        "repository": "jateeter/RealityEngine_CI",
        "path": "SURFACE_SPEC.md",
        # The commit that last changed the spec: stable across unrelated commits.
        "commit": git("log", "-1", "--format=%H", "--", spec.name),
        "sha256": hashlib.sha256(spec.read_bytes()).hexdigest(),
    },
    "excluded": ["Manager (Visualizer) Surface: Manager's surface, not an engine's"],
    "counts": counts,
    "routesSha256": digest(routes),
    "routes": routes,
}
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(json.dumps(body, indent=2) + "\n")
print(f"vendored {len(routes)} routes {counts} from {spec}")
PY
