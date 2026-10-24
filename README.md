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
