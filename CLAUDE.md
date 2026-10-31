# RealityEngine_Rust Guidance

Last reviewed: 2026-10-31

See `/Users/johnt/workspace/GitHub/CLAUDE.md` for the integrated application map. Update both this file and the root map when the Rust engine's role, validation tiers, ports or parity responsibilities change.

## Role

This repo is the Rust Reality Engine. It is a **non-voting conformance follower** (RealityEngine_CI#607): measured against the behaviour C++, LSP and Scala agree on, never a fourth vote. It implements part of the RE surface and no PE; where it stands is `RealityEngine_CI/docs/RUST_RUNTIME_EVALUATION.md`, and the repair plan is this repo's #2. Default port 5801 (RE), with 5800 reserved for a PE.

## Codebase Map

- `src/lib.rs`: the engine (`Engine`, machine transitions, arbitration, output merge) and the axum router (`app`).
- `src/instance_clock.rs`: the instance's Lamport clock and identity (RealityEngine_CI#296): `INSTANCE_UUID`, `<uuid>.lamport` in `INSTANCE_CLOCK_DIR`, and the `fcntl` lock on `<uuid>.lock` that refuses a second live process with the same UUID. It must stay a POSIX record lock (not `File::try_lock`, which is `flock`): `instance_uuids.py live` probes with `F_GETLK`.
- `src/main.rs`: entrypoint; `serve()` reads `REALITY_ENGINE_PORT`, `VECTOR_DIMENSION`, `MACHINES_DIR`, `INSTANCE_UUID`, `INSTANCE_CLOCK_DIR`.
- `tests/instance_identity.rs`: a second engine process with a held UUID refuses to boot.
- `tests/conformance.rs`: tier R1, the corpus replay.
- `tests/surface.rs`: tier R1, every SURFACE_SPEC route probed for its implementation status.
- `conformance/SURFACE_STATUS.md`: **the readable answer to "which documented APIs exist"**, generated from `conformance/surface-status.json`, which records each route's status and tracking issue.
- `conformance/surface/spec-routes.json`: the spec's route tables, vendored by `scripts/refresh-surface.sh`. Generated; never edit by hand.
- `conformance/corpus/`: the vendored regression corpus R1 replays. **Generated; never edit by hand.**
- `conformance/corpus.lock.json`: its provenance (Machines commit, manifest digest, one sha256 per file).
- `conformance/known-failures.json`: the R1 ratchet; every entry is a failure with an issue.
- `scripts/validate.sh`: runs the tiers. `scripts/refresh-corpus.sh`: re-vendors or checks the corpus.

## Validation — this repo validates itself

**Owner decision, 2026-10-30.** Unlike the other engines, this repo carries its
own self-contained validation workflow, and the full RealityEngine_CI
regression suite runs on it much less often. The protocol is
`RealityEngine_CI/docs/RUST_VALIDATION_WORKFLOW.md` and the exception is
recorded in `RealityEngine_CI/docs/ENGINEERING_CONTRACT.md`; do not restate
either here.

```bash
scripts/validate.sh          # fast: R0 + R1, what every PR runs
scripts/validate.sh r0       # fmt, clippy -D warnings, unit tests, release build
scripts/validate.sh r1       # corpus snapshot check + corpus replay
RE_REFERENCE_URL=http://127.0.0.1:5301 scripts/validate.sh r2
                             # live: this build vs a running reference RE on the same corpus
                             # (RE_DIFF_CORPUS, default conformance/corpus); local only
scripts/refresh-corpus.sh    # re-vendor from a clean RealityEngine_Machines
scripts/refresh-surface.sh   # re-vendor SURFACE_SPEC.md's route tables
```

On a Mac run them with `/opt/homebrew/bin/bash`. Exit 3 means a tier was
unavailable, which is never a pass.

Rules that bite:

- **R1's oracle is the corpus, not an engine.** `expectedOutputCount` is the
  number of steps with a non-null arbitrated output; `expectedOutputVector` is
  the last output's vector. That reading is ours; no specification defines the
  keys. If R2 finds the three engines disagree with a declaration, that is a
  document-versus-engines case: **report it to the owner, who arbitrates.**
  Never resolve it by changing the oracle or the expectation.
- **The ratchet runs both ways.** A failure not in `known-failures.json` fails
  R1; so does a listed failure that now passes. After a fix, delete the entries
  it closed. `R1_RECORD_KNOWN=1 cargo test --test conformance` rewrites the file
  with `UNCLASSIFIED` issues, which fail until each names its issue.
- **Every specified route has a known status.** `tests/surface.rs` probes each
  route of `SURFACE_SPEC.md`'s RE and PE tables (and the phase-2 K-line routes)
  and classifies it `implemented`, `implemented-bad-envelope`,
  `implemented-error`, `declared-unsupported` or `missing`. When you add or
  change a route the test fails until you re-record:
  `SURFACE_RECORD=1 cargo test --test surface`, then name the issue for any new
  non-implemented entry. Re-vendor the spec with `scripts/refresh-surface.sh`
  when it changes. Status says a route exists, not that it behaves.
- **Numbers leave in ECMAScript form (#33).** A response layer re-serializes
  every JSON body so its numbers read as `JSON.stringify` writes them, as the
  three engines do: `1`, not serde_json's `1.0`. Don't format numbers by hand in
  a handler, and compare wire bytes, not parsed `Value`s, when a test is about
  encoding (`json!(1) != json!(1.0)` in serde_json).
- **An unimplemented RE route declares itself (#15).** The router's fallbacks
  answer any specified RE route it does not serve with `501` and
  `{error, participation: "unsupported", surface}` (SURFACE_SPEC.md, "An
  unimplemented route answers `unsupported`"), matched against the vendored
  `spec-routes.json`; anything else is a plain 404/405. So re-vendoring the spec
  changes what the engine declares, and implementing a route needs nothing
  removed.
- **R2 compares against a live engine, and no engine is the oracle.**
  `scripts/differential.py` resets both, drives every corpus input sequence
  through `POST /api/machines/:id/process` (`{"inputEvent": ...}`, as the
  engines take it) and compares each step's `machineOutput.vector` and
  `mergedOutput`. A difference carries both emissions. Run it against each of
  C++, LSP and Scala; agreement with all three is the claim. Check the
  reference's build provenance first
  (`RealityEngine_CI/scripts/verify-build-provenance.py --repos cpp`).
- **Read canonical keys first.** R1 reads `eventDimension`, `outputs[].vector`,
  `events`; where Rust answers otherwise it falls back so the rest still
  measures, and records the key as a failing check.
- **A green R0/R1 is not a parity claim.** It says this checkout conforms to the
  snapshot it carries. Agreement with the reference engines is R2 and R3.

## Toolchain

`rust-toolchain.toml` pins one Rust release for every lane. The hosted
workflow installs exactly that release through rustup. Homebrew's cargo ignores
the file, so `scripts/validate.sh` compares `rustc --version` with the pin: a
mismatch fails the hosted lane and warns locally. Dependabot advances the crates
and the workflow's actions (`.github/dependabot.yml`), but not this pin.

To bump it:

1. On a branch from `origin/main`, set `channel` in `rust-toolchain.toml` to the
   new stable release.
2. Move the local compiler to the same release (`brew upgrade rust`, or
   `rustup toolchain install <version>`), so the local R0 is not a warning.
3. Run `scripts/validate.sh`. A new release's clippy may add lints; fix them in
   the same PR rather than allowing them.
4. Open the PR. The hosted `validate` run is the proof that the pin installs.

## Runtime Contract

The HTTP contract is `RealityEngine_CI/SURFACE_SPEC.md`; quorum is
`RealityEngine_CI/docs/QUORUM_CONTRACT.md`; machine ingestion, step completion
(#375), arbitration retention and the instance clock (#296) are specified there
and implemented by the sibling engines as their `CLAUDE.md` files describe.
Follow those, not this file, when they differ.

## Standing rules — authoritative in `../RealityEngine_CI/docs/ENGINEERING_CONTRACT.md`

These apply here and are **not** restated in this file. The table is an index
to the contract, not a copy of it: it names every rule so you know what to look
up, and the contract's wording governs wherever the two differ.

| Rule | In short |
| --- | --- |
| Qualify every "registry" | Never the bare word — instance / machine / cesgen / arbitration / domain / semantic-bus / tag. |
| Regenerate a stale `<name>` registry, don't fail it | Each `<name>` registry is a view of the running system. A gate regenerates it and fails only on a disagreement that survives regeneration. |
| Verify a merge beyond the hosted checks | A green PR is not a verified PR; the hosted path cannot reach the integration points. Name what you could not exercise, and record what you noticed but did not chase. |
| _CI is the authority | Peripheral repos keep minimal CI that forces local validation; RealityEngine_CI verifies fixes against a live universe. Check its `docs/` before adding CI anywhere else. This repo is the recorded exception: its own R0/R1 lane, R2/R3 on a slower cadence. |
| Name it `CLAUDE.md` | Uppercase, always. On a case-insensitive filesystem `claude.md` is the same inode; dedupe on `st_ino`, never on a resolved path. |
| Never commit to main | Branch from `origin/main`, PR, verify, squash-merge, clean up. |
| Use bash, not zsh | Shell work runs in `/opt/homebrew/bin/bash` (5.x), not zsh or macOS `/bin/bash` 3.2: any loop, unquoted variable, glob or `set --` goes through it with `set -euo pipefail`, and you check the command's exit status, not the pipeline tail. |

Read the contract for the full text, the qualifier table, and the cleanup steps.
