## Visibility Receive
Execute a mock “observation” by executing the following script:

```bash
mkdir data
docker run -v `pwd`/data:/data docker.io/pw410/ska-sdp-mock:0.1 /scripts/generate_visibilities.sh /data/out.ms
```

## Visibility Processing
For each observation, you can “process” the data by executing:

```bash
docker run -v `pwd`/data:/data docker.io/pw410/ska-sdp-mock:0.1 /scripts/process_visibilities.sh /data/out.ms /data/out
```

# Developer Notes

### Step 1: Setup & Scaffolding

1. Set up the dir structure, pyproject.toml, venv, Python, and dependencies.
2. Manual run of docker commands

### Step 2: Define models and tests

1. Write `models.py`: `Observation` dataclass, `ObservationState` enum (`RECEIVING → STORED → PROCESSING → AWAITING_REVIEW → DONE`, plus `FAILED`).
2. Write `tests/unit/test_models.py`: valid/invalid state transitions.

### Step 3: Wrap receive/process steps in Python

1. Module, `docker_runner.py`: one function, `run_container(image, command, volume)`, wrapping `subprocess.run`.
2. Modules, `receive.py` and `process.py`, as **plain functions** calling `docker_runner`.
3. Unit test: `test_task_receive_process_vis.py` covering both, `receive` and `process`, steps.

### Step 4: Storage gate

1. Module, `storage.py`: sum .ms directory sizes under `/data`, compare to a configured threshold
2. Unit test: `test_storage.py`
3. `main.py`: Wire into a simple loop

### Step 5: Prefect

1. `receive_vis`, `process_vis` -> `@task`
2. `main.py` -> `@flow`, processing task calls from awaited/sequential to `.submit()`, collecting futures.

Example log:

| Time     | Event                        | Holders after event   |
|----------|------------------------------|------------------------|
| 20:33:13 | acquire obs_000              | {obs_000}              |
| 20:33:24 | acquire obs_001              | {obs_000, obs_001}     |
| 20:33:39 | 423 Locked (attempt blocked) | {obs_000, obs_001}     |
| 20:33:40 | release obs_000              | {obs_001}              |
| 20:33:43 | acquire obs_002              | {obs_001, obs_002}     |
| 20:33:52 | release obs_001              | {obs_002}              |
| 20:33:53 | acquire obs_003              | {obs_002, obs_003}     |
| 20:34:09 | 423 Locked                   | {obs_002, obs_003}     |
| 20:34:12 | release obs_002              | {obs_003}              |
| 20:34:18 | acquire obs_004              | {obs_003, obs_004}     |
| 20:34:21 | release obs_003              | {obs_004}              |
| 20:34:23 | acquire obs_005              | {obs_004, obs_005}     |
| 20:34:38 | 423 Locked                   | {obs_004, obs_005}     |
| 20:34:45 | release obs_004              | {obs_005}              |
| 20:34:50 | release obs_005              | {}                     |
| 20:34:51 | acquire obs_006              | {obs_006}              |
| 20:35:13 | release obs_006              | {}                     |


### Step 6: Processing concurrency

1. Introduced concurrency (with `max_concurrency` parameters in settings) in processing task calls by changing from sequential to `.submit()`, collecting futures.


### Step 7: Quality review gate

1. Module, `review.py`, to gate the post-processing actions based on user review, through `pause_flow_run` / `wait_for_input`, `create_preview_artifact` with a concurrency implementation similar to the processing task.
2. `resolve_review_cycle` to resolve post-review actions, 
    a. `continue` -> `remove_ms` or 
    b. `re-process` -> `process` -> `review` (max 2 re-process attempts, configurable)
3. Wired in `main.py` through `.submit()` collecting futures, in sequence with `process` and waiting for `prior_review_future` (if any).


## Key features: 

### Campaign break with pause-and-retry

The storage check happens by default over only the `.ms` results in the `storage.data_dir`. It is configurable through `storage.count_scope` to include everything in the dir for the check with threshold.

The campaign carries on until the storage is full. If the `.ms` results are cleared after revievs, the storage-gate will retry after a pause to check for any available storage space, and the campaign flow will continue until the observations are stopped manually (or until the prefect flow is cancelled in this case).


### Prefect UI based review-and-resume

Prefect's `create_preview_artifact` allows the user to review the images without switching to another interface and select the review-based decision in the same modal interface.


### Mapping to SDP architecture

Similarities: 

1. Dependency-gated execution: `process_visibilities` only runs once its own observation's `receive_visibilities` has succeeded 
2. Resource-gated admission: batch receive only gets resource allocation once the storage-check dependency is satisfied
3. Decoupled concurrent execution per unit of work: each observation's processing and review-resolution proceeds independently

Divergence:

1. one long-running flow spans the entire campaign (many observations), with each observation's steps as tasks within that one flow, rather than each observation getting its own execution scope.
2. No real-time/batch PB distinction

The architecture follows SDP's dependency-and-resource-gating pattern closely, but simplifies two things: 

1. it's one continuous campaign-scoped flow rather than per-observation Execution Blocks, and
2. it doesn't model the real-time/batch PB split since this exercise has no real-time requirement.

