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


### Step 6: