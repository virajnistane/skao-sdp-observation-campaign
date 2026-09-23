# SKA SDP Long Observation Control

Prefect-orchestrated control pipeline for a long-running SKA SDP observation campaign against mock visibility data. It receives, processes, and reviews observations continuously until storage fills up, with a human-in-the-loop review step that can trigger automatic reprocessing.

## Overview

The campaign runs as a single Prefect flow (`main.py`). For each observation, it:

1. Receives raw visibilities into a Measurement Set via a mock Docker container.
2. Processes the Measurement Set (imaging) with a concurrency-limited Docker container.
3. Pauses for a human reviewer to inspect a generated preview artifact and choose **Continue** or **Re-process**. The resume-run prompt identifies the observation and links its preview artifact, so it's clear which observation a given prompt is for.
4. Acts on that decision — cleaning up on **Continue**, or reprocessing and re-reviewing on **Re-process**. Each reprocess attempt gets its own output directory (`{id}_processed_{timestamp}_attempt{N}`), so prior attempts and their previews aren't overwritten and the reviewer always sees the image that matches the attempt actually being reviewed. If `quality_gate.max_attempts` is exhausted without a **Continue**, the observation is marked `FAILED` and both its raw `.ms` and every attempt's processed output are moved (not deleted) into `storage.failed_dir` for manual inspection — this also keeps them from permanently counting toward the storage threshold, which would otherwise stall the campaign forever waiting on space that was never coming.

The loop keeps receiving new observations while the total size of stored Measurement Sets stays under a configured threshold. `resolve_review_cycle` (delete-or-reprocess) is submitted per-observation as soon as its review future exists, not batched at the end of the campaign, so `remove_ms` can free space concurrently with the receive loop still running. When the threshold is crossed, the loop doesn't stop outright — it waits and rechecks (real disk rescan each time), since the backlog is often transient and clears once in-flight reviews resolve; see `storage.count_scope` and `observation.storage_wait_indefinite` below for how long it waits and what counts toward the threshold. Each observation's post-review action runs as soon as its own decision is known, independent of other observations — an early observation's reprocess cycle doesn't stall a later observation's cleanup. Review pauses themselves are serialized flow-wide (only one reviewer prompt is ever open at a time, including reprocess-triggered re-reviews), since `pause_flow_run` pauses the whole flow run, not just the calling task.

## Pipeline flow

```
receive_visibilities
        │
        ▼
process_visibilities      (capped at processing.max_concurrency; each attempt writes to its own _attemptN dir)
        │
        ▼
review_processed_visibilities   (pause_flow_run: reviewer picks Continue / Re-process)
        │                        serialized flow-wide via the "review-pause" concurrency limit
        ├── Continue         → remove_ms       (delete the Measurement Set)
        ├── Re-process       → process_visibilities → review_processed_visibilities   (loops up to quality_gate.max_attempts)
        └── max_attempts hit → mark FAILED, quarantine_ms   (move .ms + all processed attempts to storage.failed_dir)
```

`resolve_review_cycle` (`src/sdp_control/tasks/review.py`) is itself a Prefect task, submitted once per observation inside the receive loop (not after it), so each observation's outcome (delete or reprocess) is handled concurrently rather than one observation blocking the next — and so `remove_ms` can keep freeing storage even while the receive loop itself is waiting out a `storage_full` retry.

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
| `storage.data_dir` / `storage.storage_threshold_mb` | Where Measurement Sets land and the total size that triggers the storage-wait retry |
| `storage.failed_dir` | Where `quarantine_ms` moves a quality-gate-exhausted observation's `.ms` and processed output — outside `data_dir`, so it's excluded from the threshold count under either `count_scope` |
| `storage.count_scope` | What counts toward the threshold: `ms_only` (default, raw `.ms` dirs only) or `all` (everything under `data_dir`, including processed/preview output) |
| `observation.storage_wait_indefinite` | When storage is full: `true` (default) polls forever until space frees; `false` gives up after `observation.retry_attempts` |
| `observation.retry_attempts` / `observation.retry_delay_seconds` | Retry cap (when not waiting indefinitely) and poll interval (seconds) between storage rechecks |
| `observation.storage_wait_safety_limit` | Hard ceiling on wait-retries even when `storage_wait_indefinite` is `true` — a last-resort guard against the wait loop spinning forever |
| `processing.max_concurrency` | Max concurrent `process_visibilities` Docker runs |
| `quality_gate.max_attempts` | Max reprocess+review cycles per observation before giving up and quarantining it |
| `containers.receive` / `containers.process` | Docker images and commands for each step |
| `containers.mount_path` | Container-side mount point for the data volume |

Two Prefect global concurrency limits are created automatically on first run (visible in the Prefect UI under Concurrency Limits): `process-visibilities` (limit = `processing.max_concurrency`) caps concurrent Docker processing runs, and `review-pause` (fixed at 1) serializes `pause_flow_run` calls so only one review is ever awaiting reviewer input at a time.

**Optional Slack alerts**: when a review pauses, a Slack message (with the preview artifact link and the Prefect UI URL) is sent via a `SlackWebhook` block named `sdp-review-alerts`. It's registered lazily (only right before a notification is actually sent, never at import time) from `PREFECT_SLACK_WEBHOOK_URL` if that env var is set (`.env` is loaded automatically); otherwise create the block once via the Prefect UI (Blocks → Slack Webhook) or `SlackWebhook(url="...").save("sdp-review-alerts")`. If the block doesn't exist or the webhook call fails, a warning is logged and the review pause proceeds normally regardless — Slack is a best-effort side-channel, never a blocker.

## Kubernetes (design artifact, not shipped)

`k8s/pvc.yaml` and `src/sdp_control/utils/k8s_runner.py` are a verified-but-unwired alternative to `docker_runner`: `k8s_runner.run_container(image, command, volumes)` matches `docker_runner.run_container`'s interface exactly, so swapping the import in `receive_vis.py`/`process_vis.py` is a one-line change. Verified manually against minikube — the PVC (`ska-sdp-data`) binds, a real Job runs to completion (`kubectl get jobs -w` shows `Complete 1/1`), and the output `.ms` was confirmed present inside the PVC via a debug pod.

It's intentionally not the shipped path. The PVC is cluster-local storage, not the project's local `./data` — so swapping it in would also require moving `storage.py`'s local disk scan, preview-artifact FITS reading, and `remove_ms` to operate against the cluster-mounted volume instead of the local filesystem, which is out of scope here. The demo runs on `docker_runner`; `k8s_runner` stands as a proven design artifact for how the Kubernetes path would work.

To reproduce: `minikube start`, `kubectl apply -f k8s/pvc.yaml`, swap the import in `receive_vis.py` to `from sdp_control.utils.k8s_runner import run_container`, run it, and watch `kubectl get jobs -w`.

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
uv run pytest tests/unit          # fast, hermetic — run_container is mocked, no Docker needed
uv run pytest tests/integration   # slower — real Docker containers, real Prefect orchestration
```

`tests/integration/test_end_to_end.py` exercises the full receive→process→resolve cycle (both `Continue` and quality-gate-exhaustion quarantine) against real containers; the human review pause itself (`pause_flow_run`) isn't automated and is tested manually via the Prefect UI.

## Project layout

```
src/sdp_control/
  config.py                     # settings.yaml loader / dataclasses
  models.py                     # Observation, ObservationState
  tasks/
    receive_vis.py              # receive_visibilities, remove_ms, quarantine_ms
    process_vis.py              # process_visibilities
    review.py                   # review_processed_visibilities, resolve_review_cycle
    preview_artifact.py         # create_preview_artifact
    storage.py                  # storage size / threshold checks
  utils/
    docker_runner.py            # run_container (shipped)
    k8s_runner.py                # run_container on minikube (design artifact, not wired in — see Kubernetes section)
    plot_preview.py             # FITS -> PNG preview rendering
config/settings.yaml
k8s/pvc.yaml                    # PVC manifest for k8s_runner
tests/
  unit/
  integration/
```

## See also

- `documentation/notes.md` has the manual mock-command walkthrough (running the receive/process Docker images by hand), the development log, and the "Extended scope" section (Kubernetes runner rationale).