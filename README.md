# RealityEngine_Rust
An implementation of the RealityEngine in the Rust Language providing black-box equivalence to other RealityEngine implementations.

## Build and run

```sh
cargo test
cargo run
```

The service listens on `0.0.0.0:5801` by default. Configure `REALITY_ENGINE_PORT`
to use another port, `VECTOR_DIMENSION` for the universal perceptual-space
dimension (default `7680`), and `MACHINES_DIR` for the machine corpus directory
(default `../RealityEngine_Machines/machines`). JSON machine files are loaded
recursively at startup.

## Validation

```sh
scripts/validate.sh        # R0 (fmt, clippy, unit tests, release build) + R1 (corpus replay)
```

R1 replays the regression corpus vendored in `conformance/corpus/` through the
HTTP router and checks each input sequence against what the corpus declares,
and probes every route `SURFACE_SPEC.md` defines for its implementation status.
It needs nothing but this checkout. Which documented APIs exist today is
[`conformance/SURFACE_STATUS.md`](conformance/SURFACE_STATUS.md). Agreement with the C++, LSP and Scala
engines is checked beside a live universe (R2) and by the full
RealityEngine_CI regression suite (R3), on a slower cadence. The protocol is
`RealityEngine_CI/docs/RUST_VALIDATION_WORKFLOW.md`; repo guidance is
`CLAUDE.md`.

## Implemented surface

The current runtime provides health and configuration reads, machine
registration/listing/lookup/removal, machine transition processing, engine-wide
processing, transition history, and engine reset:

- `GET /`, `/api`, `/api/health`, `/api/config`
- `GET|POST /api/machines`, `GET|DELETE /api/machines/:id`
- `GET /api/machines/json/list`
- `POST /api/machines/:id/process`
- `POST /api/engine/process`, `/api/engine/reset`
- `GET /api/engine/stats`, `/api/engine/history`

Machine transitions use active Reality Events, deferred successor activation,
per-element comparator overrides, provenance, sequence arbitration, and the
declared output merge transformation. The HTTP contract is maintained in
`RealityEngine_CI/SURFACE_SPEC.md`; this Rust runtime currently implements the
core RE surface rather than the complete RE/PE contract.
