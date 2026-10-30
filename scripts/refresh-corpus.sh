#!/usr/bin/env bash
# refresh-corpus.sh — vendor the regression corpus R1 replays, or check it.
#
# R1 is self-contained: it runs with this checkout alone. The machines it
# replays are therefore a snapshot, copied from a clean RealityEngine_Machines
# checkout and pinned in conformance/corpus.lock.json (source commit, manifest
# digest, one sha256 per file). R1 verifies the lock before it replays anything.
#
# This script is local maintenance, never a CI step: it reads sibling checkouts,
# which the hosted lane must not do (ENGINEERING_CONTRACT, "this repository is
# the authority"). R2 runs `--check` so drift between the snapshot and the
# corpus is caught where the siblings exist.
#
# Usage:
#   scripts/refresh-corpus.sh [--machines DIR] [--manifest FILE] [--check]
#
#   --machines DIR   RealityEngine_Machines checkout. Default: ../RealityEngine_Machines
#   --manifest FILE  Corpus manifest. Default: ../RealityEngine_CI/config/regression-corpus.txt
#   --check          Compare the snapshot with the checkout; write nothing.
#                    Exit 1 on drift.
#
# Entries the manifest names that are not in RealityEngine_Machines (the three
# localAIStack machines) are recorded as excluded, with the reason, rather than
# silently dropped.
set -euo pipefail

if [ -z "${BASH_VERSINFO:-}" ] || [ "${BASH_VERSINFO[0]}" -lt 5 ]; then
  echo "refresh-corpus.sh: needs bash 5 (/opt/homebrew/bin/bash)" >&2
  exit 2
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
workspace="$(cd "$repo_root/.." && pwd)"
# A worktree under .claude/worktrees sits four levels below the workspace.
if [ ! -d "$workspace/RealityEngine_Machines" ] && [ -d "$repo_root/../../../../RealityEngine_Machines" ]; then
  workspace="$(cd "$repo_root/../../../.." && pwd)"
fi
machines="$workspace/RealityEngine_Machines"
manifest="$workspace/RealityEngine_CI/config/regression-corpus.txt"
check=false

while [ $# -gt 0 ]; do
  case "$1" in
    --machines) machines="$2"; shift 2 ;;
    --manifest) manifest="$2"; shift 2 ;;
    --check) check=true; shift ;;
    -h|--help) sed -n '2,24p' "${BASH_SOURCE[0]}"; exit 0 ;;
    *) echo "refresh-corpus.sh: unknown argument $1" >&2; exit 2 ;;
  esac
done

[ -d "$machines/machines" ] || { echo "refresh-corpus.sh: no machines/ under $machines" >&2; exit 2; }
[ -f "$manifest" ] || { echo "refresh-corpus.sh: no manifest at $manifest" >&2; exit 2; }

if [ "$check" = false ] && [ -n "$(git -C "$machines" status --porcelain -- machines)" ]; then
  echo "refresh-corpus.sh: $machines has uncommitted corpus changes; refusing to vendor them" >&2
  exit 1
fi

python3 -I - "$repo_root" "$machines" "$manifest" "$check" <<'PY'
import hashlib, json, pathlib, shutil, subprocess, sys

repo, machines, manifest, check = (pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2]),
                                   pathlib.Path(sys.argv[3]), sys.argv[4] == "true")
corpus_dir = repo / "conformance" / "corpus"
lock_path = repo / "conformance" / "corpus.lock.json"

def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def git(repo_dir, *args):
    return subprocess.run(["git", "-C", str(repo_dir), *args], check=True,
                          capture_output=True, text=True).stdout.strip()

entries = [line.strip() for line in manifest.read_text().splitlines()
           if line.strip() and not line.lstrip().startswith("#")]
files, excluded = [], []
for entry in entries:
    source = machines / "machines" / entry
    if source.is_file():
        files.append({"relFile": entry, "sha256": sha256(source)})
    else:
        excluded.append({"entry": entry,
                         "reason": "not in RealityEngine_Machines/machines (a localAIStack machine)"})

if check:
    lock = json.loads(lock_path.read_text())
    locked = {f["relFile"]: f["sha256"] for f in lock["files"]}
    current = {f["relFile"]: f["sha256"] for f in files}
    drift = sorted(set(locked) ^ set(current)) + sorted(
        k for k in set(locked) & set(current) if locked[k] != current[k])
    if drift:
        print("corpus snapshot DRIFT against", machines)
        for k in drift:
            print("  ", k)
        print("re-vendor with scripts/refresh-corpus.sh and re-run R1")
        sys.exit(1)
    print(f"corpus snapshot current: {len(current)} files match {machines}")
    sys.exit(0)

if corpus_dir.exists():
    shutil.rmtree(corpus_dir)
for f in files:
    target = corpus_dir / f["relFile"]
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(machines / "machines" / f["relFile"], target)

ci_repo = manifest.parent.parent
lock = {
    "schema": 1,
    "purpose": "R1 corpus snapshot; regenerate with scripts/refresh-corpus.sh, never by hand",
    "source": {
        "repository": "jateeter/RealityEngine_Machines",
        "commit": git(machines, "rev-parse", "HEAD"),
    },
    "manifest": {
        "repository": "jateeter/RealityEngine_CI",
        "path": "config/regression-corpus.txt",
        "commit": git(ci_repo, "rev-parse", "HEAD"),
        "sha256": sha256(manifest),
    },
    "files": files,
    "excluded": excluded,
}
lock_path.write_text(json.dumps(lock, indent=2) + "\n")
print(f"vendored {len(files)} machines, excluded {len(excluded)}; lock written to {lock_path}")
PY
