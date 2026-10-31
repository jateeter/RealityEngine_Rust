# Codex Guidance: RealityEngine_Rust

Read `CLAUDE.md` for the current codebase map, the validation tiers and the
parity context.

## Role

This repo contains the Rust Reality Engine, a non-voting conformance follower
measured against the behaviour C++, LSP and Scala agree on. It has no
Perception Engine yet (#20).

## Development Rules

- Run `scripts/validate.sh` (R0 + R1) before every PR; on a Mac use
  `/opt/homebrew/bin/bash`. Exit 3 means a tier was unavailable, never a pass.
- Treat routes, response keys and machine loading as cross-engine contracts:
  `RealityEngine_CI/SURFACE_SPEC.md` governs, and the three engines' agreed
  behaviour is the reference.
- Read and write only canonical Reality Event keys (`events`, `outputEvents`,
  `nextEventIds`, `inputEvent`, `eventDimension`).
- Never edit `conformance/corpus/` or `conformance/surface/spec-routes.json` by
  hand; re-vendor with `scripts/refresh-corpus.sh` / `scripts/refresh-surface.sh`.
- When a route's status changes, re-record `conformance/surface-status.json`
  (`SURFACE_RECORD=1 cargo test --test surface`); when R1 failures close,
  delete their `known-failures.json` entries.

## Bug Triage

- Corpus behaviour: `tests/conformance.rs` (R1) against the corpus's declared
  expectations.
- Engine agreement: `RE_REFERENCE_URL=<re> scripts/validate.sh r2` runs
  `scripts/differential.py` against a live reference built from current source.
- Where a specification and the three engines disagree, report it to the owner
  rather than resolving it in code.

## Verification

```bash
scripts/validate.sh
cargo test
RE_REFERENCE_URL=http://127.0.0.1:5301 scripts/validate.sh r2
```

## Artifact Hygiene

Do not commit `target/`, conformance reports, logs or runtime state unless
explicitly requested.
