#!/usr/bin/env bash
# validate.sh — the RealityEngine_Rust validation tiers.
#
# The protocol is RealityEngine_CI/docs/RUST_VALIDATION_WORKFLOW.md. This script
# runs its tiers; it does not restate what each one proves.
#
#   R0  static       fmt, clippy -D warnings, unit tests, release build
#   R1  conformance  the vendored regression corpus replayed through the HTTP
#                    router against what the corpus declares (tests/conformance.rs),
#                    and every SURFACE_SPEC route probed for its implementation
#                    status (tests/surface.rs)
#   R2  follower     against live reference engines. With RE_REFERENCE_URL set,
#                    this checkout's release build is started on the corpus in
#                    RE_DIFF_CORPUS (default conformance/corpus) and compared with
#                    that reference by scripts/differential.py; the reference must
#                    be running on the same corpus. Without it, R2 delegates to
#                    RealityEngine_CI/scripts/rust-conformance.sh (CI#607).
#
# R3, the full regression-test.sh with Rust as a follower, is run from
# RealityEngine_CI, never from here.
#
# Usage:
#   scripts/validate.sh [fast|r0|r1|r2|all]    default: fast (= r0 + r1)
#
# Exit status: 0 every tier asked for passed; 1 a tier failed; 2 usage;
# 3 a tier is unavailable here (reported, never a pass).
set -euo pipefail

if [ -z "${BASH_VERSINFO:-}" ] || [ "${BASH_VERSINFO[0]}" -lt 5 ]; then
  echo "validate.sh: needs bash 5; on a Mac run it with /opt/homebrew/bin/bash" >&2
  exit 2
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

say() { printf '\n== %s\n' "$*"; }

# Tiers are called from an `||` context, where bash ignores `set -e`, so every
# step checks its own status and returns it.

check_toolchain() {
  local pinned actual
  pinned="$(sed -n 's/^channel = "\(.*\)"/\1/p' rust-toolchain.toml)"
  actual="$(rustc --version | awk '{print $2}')"
  if [ "$pinned" = "$actual" ]; then
    echo "rustc $actual (pinned)"
  elif [ -n "${GITHUB_ACTIONS:-}" ]; then
    echo "rustc $actual, but rust-toolchain.toml pins $pinned" >&2
    return 1
  else
    echo "warning: rustc $actual, but rust-toolchain.toml pins $pinned (Homebrew cargo ignores the pin)" >&2
  fi
}

tier_r0() {
  say "R0 static: toolchain"
  check_toolchain || return 1
  say "R0 static: rustfmt"
  cargo fmt --all --check || return 1
  say "R0 static: clippy"
  cargo clippy --locked --all-targets -- -D warnings || return 1
  say "R0 static: unit tests"
  cargo test --locked --lib --bins || return 1
  say "R0 static: release build"
  cargo build --locked --release || return 1
}

verify_corpus_lock() {
  python3 -I - "$repo_root/conformance" <<'PY'
import hashlib, json, pathlib, sys
base = pathlib.Path(sys.argv[1])
lock = json.loads((base / "corpus.lock.json").read_text())
locked = {f["relFile"]: f["sha256"] for f in lock["files"]}
present = {p.relative_to(base / "corpus").as_posix()
           for p in (base / "corpus").rglob("*.json")}
problems = [f"unlocked file {p}" for p in sorted(present - set(locked))]
problems += [f"missing file {p}" for p in sorted(set(locked) - present)]
for rel, digest in sorted(locked.items()):
    if rel in present and hashlib.sha256((base / "corpus" / rel).read_bytes()).hexdigest() != digest:
        problems.append(f"edited by hand {rel}")
if problems:
    print("corpus snapshot does not match corpus.lock.json:")
    print("\n".join("  " + p for p in problems))
    print("re-vendor with scripts/refresh-corpus.sh; never edit conformance/corpus/ by hand")
    sys.exit(1)
src = lock["source"]
print(f"corpus snapshot: {len(locked)} machines from {src['repository']}@{src['commit'][:12]}")
PY
}

verify_surface_snapshot() {
  python3 -I - "$repo_root/conformance/surface/spec-routes.json" <<'PY'
import hashlib, json, sys
doc = json.load(open(sys.argv[1]))
rows = [{k: r[k] for k in ("id", "layer", "group", "method", "path")} for r in doc["routes"]]
canonical = json.dumps(rows, sort_keys=True, separators=(",", ":"))
if hashlib.sha256(canonical.encode()).hexdigest() != doc["routesSha256"]:
    print("spec-routes.json does not match its routesSha256: edited by hand?")
    print("re-vendor with scripts/refresh-surface.sh; never edit it by hand")
    sys.exit(1)
src = doc["source"]
print(f"surface snapshot: {doc['counts']} from {src['repository']} {src['path']}@{src['commit'][:12]}")
PY
}

tier_r1() {
  say "R1 conformance: corpus snapshot"
  verify_corpus_lock || return 1
  say "R1 conformance: corpus replay"
  cargo test --locked --test conformance -- --nocapture || return 1
  say "R1 conformance: surface snapshot"
  verify_surface_snapshot || return 1
  say "R1 conformance: API surface"
  cargo test --locked --test surface -- --nocapture || return 1
}

r2_differential() {
  local corpus="${RE_DIFF_CORPUS:-$repo_root/conformance/corpus}"
  local port pid status=0
  port="$(python3 -I -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')" || return 1
  cargo build --locked --release || return 1
  say "R2 differential: Rust on :$port vs $RE_REFERENCE_URL, corpus $corpus"
  MACHINES_DIR="$corpus" REALITY_ENGINE_PORT="$port" target/release/reality-engine &
  pid=$!
  local ready=false
  for _ in $(seq 1 120); do
    if curl -sf "http://127.0.0.1:$port/api/health" >/dev/null 2>&1; then ready=true; break; fi
    sleep 1
  done
  if [ "$ready" = true ]; then
    python3 -I scripts/differential.py --candidate "http://127.0.0.1:$port" \
      --reference "$RE_REFERENCE_URL" --corpus "$corpus" \
      --out "$repo_root/target/conformance/differential-report.json" || status=$?
  else
    echo "Rust did not answer on :$port within 120s"
    status=1
  fi
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  return "$status"
}

tier_r2() {
  say "R2 follower"
  if [ -n "${RE_REFERENCE_URL:-}" ]; then
    r2_differential || return $?
    return 0
  fi
  local ci_repo="${RE_CI_REPO:-$repo_root/../RealityEngine_CI}"
  local runner="$ci_repo/scripts/rust-conformance.sh"
  if [ ! -x "$runner" ]; then
    echo "R2 unavailable: $runner does not exist yet (RealityEngine_CI#607)."
    echo "R2 is not a pass; it has not run."
    return 3
  fi
  bash scripts/refresh-corpus.sh --check || return 1
  bash scripts/refresh-surface.sh --check || return 1
  "$runner" --rust-repo "$repo_root" || return 1
}

tiers=()
case "${1:-fast}" in
  fast) tiers=(r0 r1) ;;
  r0|r1|r2) tiers=("$1") ;;
  all) tiers=(r0 r1 r2) ;;
  -h|--help) sed -n '2,21p' "${BASH_SOURCE[0]}"; exit 0 ;;
  *) echo "validate.sh: unknown tier ${1}" >&2; exit 2 ;;
esac

overall=0
for tier in "${tiers[@]}"; do
  status=0
  "tier_$tier" || status=$?
  if [ "$status" -eq 0 ]; then
    echo "-- ${tier^^}: pass"
  elif [ "$status" -eq 3 ]; then
    echo "-- ${tier^^}: unavailable"
    [ "$overall" -eq 1 ] || overall=3
  else
    echo "-- ${tier^^}: FAIL"
    overall=1
  fi
done
exit "$overall"
