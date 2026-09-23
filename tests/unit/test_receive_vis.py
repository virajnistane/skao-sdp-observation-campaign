#!/usr/bin/env python3
# Author: Viraj Nistane
# Description: Unit tests for quarantine_ms (receive_vis.py) - no Docker needed.

from pathlib import Path

from sdp_control.config import config
from sdp_control.models import Observation, ObservationState
from sdp_control.tasks.receive_vis import quarantine_ms


def test_quarantine_ms_multi_attempt(tmp_path, monkeypatch):
    failed_dir = tmp_path / "failed"
    monkeypatch.setattr(config.storage, "failed_dir", str(failed_dir))

    ms_dir = tmp_path / "data"
    ms_dir.mkdir()

    raw = ms_dir / "obs_x_raw_2024-01-01_00-00-00.ms"
    raw.mkdir()
    (raw / "table.dat").write_bytes(b"0")

    attempt_dirs = []
    for n in (1, 2):
        d = ms_dir / f"obs_x_processed_2024-01-01_00-00-00_attempt{n}"
        d.mkdir()
        (d / "out-image.fits").write_bytes(b"0")
        attempt_dirs.append(d)

    obs = Observation(
        id="obs_x",
        ms_dir=str(ms_dir),
        state=ObservationState.FAILED,
        datetime_stamp="2024-01-01_00-00-00",
        processing_attempt=2,
    )
    quarantine_ms(obs)

    assert not raw.exists()
    assert (failed_dir / raw.name).exists()
    for d in attempt_dirs:
        assert not d.exists()
        assert (failed_dir / d.name).exists()


def test_quarantine_ms_missing_raw_ms_does_not_raise(tmp_path, monkeypatch):
    failed_dir = tmp_path / "failed"
    monkeypatch.setattr(config.storage, "failed_dir", str(failed_dir))

    ms_dir = tmp_path / "data"
    ms_dir.mkdir()

    obs = Observation(
        id="obs_y",
        ms_dir=str(ms_dir),
        state=ObservationState.FAILED,
        datetime_stamp="2024-01-01_00-00-00",
    )
    quarantine_ms(obs)  # should just log a warning, not raise

    assert failed_dir.exists()  # still created up front
