#!/usr/bin/env python3
# Author: Viraj Nistane
# Description: Unit tests for create_preview_artifact (preview_artifact.py) - fits2png
# and create_image_artifact are mocked; no Docker, no matplotlib/astropy work needed.

from pathlib import Path
from unittest.mock import MagicMock

from sdp_control.config import config
from sdp_control.models import Observation, ObservationState
from sdp_control.tasks import preview_artifact


def _setup_processed_dir(tmp_path, monkeypatch, attempt=1):
    # create_preview_artifact keys off config.storage.data_dir (global), not
    # observation.ms_dir, unlike receive_vis.py/process_vis.py - so this must be
    # monkeypatched for the function to find the fixture files at all.
    monkeypatch.setattr(config.storage, "data_dir", str(tmp_path))
    out_dir = tmp_path / f"obs_p_processed_2024-01-01_00-00-00_attempt{attempt}"
    out_dir.mkdir()
    (out_dir / "out-image.fits").write_bytes(b"0")
    return out_dir


def _make_observation():
    return Observation(
        id="obs_p",
        ms_dir="unused",  # see note above: this function doesn't actually use ms_dir
        state=ObservationState.AWAITING_REVIEW,
        datetime_stamp="2024-01-01_00-00-00",
    )


def test_create_preview_artifact_generates_when_missing(tmp_path, monkeypatch):
    out_dir = _setup_processed_dir(tmp_path, monkeypatch)

    def fake_fits2png(files):
        (out_dir / "previews.png").write_bytes(b"\x89PNG\r\n")

    mock_fits2png = MagicMock(side_effect=fake_fits2png)
    mock_create_artifact = MagicMock(return_value="fake-uuid")
    monkeypatch.setattr(preview_artifact, "fits2png", mock_fits2png)
    monkeypatch.setattr(preview_artifact, "create_image_artifact", mock_create_artifact)

    result = preview_artifact.create_preview_artifact(_make_observation())

    mock_fits2png.assert_called_once()
    mock_create_artifact.assert_called_once()
    assert result == "fake-uuid"


def test_create_preview_artifact_skips_regeneration_when_cached(tmp_path, monkeypatch):
    out_dir = _setup_processed_dir(tmp_path, monkeypatch)
    (out_dir / "previews.png").write_bytes(b"\x89PNG\r\n")  # already exists

    mock_fits2png = MagicMock()
    mock_create_artifact = MagicMock(return_value="fake-uuid")
    monkeypatch.setattr(preview_artifact, "fits2png", mock_fits2png)
    monkeypatch.setattr(preview_artifact, "create_image_artifact", mock_create_artifact)

    preview_artifact.create_preview_artifact(_make_observation())

    # Regression guard: reprocess attempts must not reuse a stale cached preview.
    mock_fits2png.assert_not_called()
