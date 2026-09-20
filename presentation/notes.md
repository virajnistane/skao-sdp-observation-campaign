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

### Step 4: Storage module

1. Module, `storage.py`: sum .ms directory sizes under /data, compare to a configured threshold