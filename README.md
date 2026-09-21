# SKA SDP Long Observation Control

Prefect-orchestrated control pipeline for a long-running SKA SDP observation campaign against mock visibility data. It receives, processes, and reviews observations continuously until storage fills up, with a human-in-the-loop review step that can trigger automatic reprocessing.

## Overview

The campaign runs as a single Prefect flow (`main.py`). For each observation, it:

1. Receives raw visibilities into a Measurement Set via a mock Docker container.
2. Processes the Measurement Set (imaging) with a concurrency-limited Docker container.
3. Pauses for a human reviewer to inspect a generated preview artifact and choose **Continue** or **Re-process**.
4. Acts on that decision automatically — cleaning up on **Continue**, or reprocessing and re-reviewing (up to a configured attempt cap) on **Re-process**.

The loop keeps receiving new observations until the total size of stored Measurement Sets crosses a configured threshold.

## Pipeline flow

```
receive_visibilities
        │
        ▼
process_visibilities      (capped at processing.max_concurrency)
        │
        ▼
review_processed_visibilities   (pause_flow_run: reviewer picks Continue / Re-process)
        │
        ├── Continue    → remove_ms            (delete the Measurement Set)
        └── Re-process  → process_visibilities → review_processed_visibilities   (loops up to quality_gate.max_attempts)
```

## Requirements

- Python >= 3.13
- [uv](https://docs.astral.sh/uv/)
- Docker (for the receive/process containers)

## Setup

```bash
uv sync
```

The receive and process steps run mock SDP images (`docker.io/pw410/ska-sdp-mock:0.1` by default, configured in `config/settings.yaml`) via Docker.

## Configuration

Settings live in `config/settings.yaml`, loaded into `src/sdp_control/config.py::Config`. Key knobs:

| Key | Purpose |
|---|---|
| `storage.data_dir` / `storage.storage_threshold_mb` | Where Measurement Sets land and the total size that stops the receive loop |
| `processing.max_concurrency` | Max concurrent `process_visibilities` Docker runs |
| `quality_gate.max_attempts` | Max reprocess+review cycles per observation before giving up |
| `containers.receive` / `containers.process` | Docker images and commands for each step |
| `containers.mount_path` | Container-side mount point for the data volume |

## Running

Start the Prefect server first — check `uv run prefect config view`; if `PREFECT_API_URL` is set (the default local profile points at `http://127.0.0.1:4200/api`), `main.serve()` will try to reach that URL and fail with `httpx.ConnectError` unless a server is running there:

```bash
uv run prefect server start
```

Then, in another terminal, run the workflow:
```bash
uv run python main.py
```

This calls `main.serve()`, registering `long_term_observation_campaign` as a servable Prefect flow. (Only skip the server if your profile has no `PREFECT_API_URL` configured — then Prefect falls back to an ephemeral in-process API automatically.)

## Testing

```bash
uv run pytest tests/unit
```

## Project layout

```
src/sdp_control/
  config.py            # settings.yaml loader / dataclasses
  models.py             # Observation, ObservationState
  tasks/
    receive_vis.py       # receive_visibilities, remove_ms
    process_vis.py        # process_visibilities
    review.py              # review_processed_visibilities, resolve_review_cycle
    preview_artifact.py     # create_preview_artifact
    storage.py               # storage size / threshold checks
  utils/
    docker_runner.py          # run_container
    plot_preview.py            # FITS -> PNG preview rendering
config/settings.yaml
tests/unit/
```

## See also

`presentation/notes.md` has the manual mock-command walkthrough (running the receive/process Docker images by hand) and the development log.
