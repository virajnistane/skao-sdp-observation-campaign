#!/usr/bin/env python3
# Author: Viraj Nistane
# Description: End-to-end integration tests against real Docker containers and real
# Prefect orchestration (no mocking). Covers receive -> process -> resolve, excluding
# the human-in-the-loop pause_flow_run step, which requires manual/UI testing.

from pathlib import Path

from prefect import flow

from sdp_control.config import config
from sdp_control.models import Observation, ObservationState
from sdp_control.tasks.process_vis import process_visibilities
from sdp_control.tasks.receive_vis import receive_visibilities
from sdp_control.tasks.review import ReviewDecision, resolve_review_cycle
from sdp_control.tasks.storage import get_total_ms_size_mb


def _receive_and_process(tmp_path):
    obs = Observation(
        id="obs_e2e",
        ms_dir=str(tmp_path),
        state=ObservationState.RECEIVING,
        datetime_stamp="2024-01-01_00-00-00",
    )
    received = receive_visibilities(obs)
    assert received.state == ObservationState.STORED
    processed = process_visibilities(received)
    assert processed.state == ObservationState.AWAITING_REVIEW
    return processed


def test_receive_and_process_real_docker(tmp_path):
    processed = _receive_and_process(tmp_path)

    ms_path = tmp_path / "obs_e2e_raw_2024-01-01_00-00-00.ms"
    assert ms_path.exists(), f"Expected {ms_path} to be created by receive_visibilities"

    out_dir = tmp_path / "obs_e2e_processed_2024-01-01_00-00-00_attempt1"
    expected_fits = [
        "out-dirty.fits",
        "out-image.fits",
        "out-psf.fits",
        "out-residual.fits",
        "out-model.fits",
    ]
    for file_name in expected_fits:
        assert (out_dir / file_name).exists(), f"Expected {file_name} in {out_dir}"

    ms_only_mb = get_total_ms_size_mb(str(tmp_path), 0.0, storage_count_scope="ms_only")
    all_mb = get_total_ms_size_mb(str(tmp_path), 0.0, storage_count_scope="all")
    assert ms_only_mb > 0
    assert (
        all_mb > ms_only_mb
    ), "count_scope='all' should count processed output too, not just the raw .ms"

    del processed  # only used to assert intermediate state above


def test_continue_removes_ms(tmp_path):
    processed = _receive_and_process(tmp_path)
    ms_path = tmp_path / "obs_e2e_raw_2024-01-01_00-00-00.ms"
    assert ms_path.exists()

    @flow
    def _resolve_continue(observation):
        resolve_review_cycle(observation, ReviewDecision.CONTINUE)

    _resolve_continue(processed)

    assert not ms_path.exists(), "remove_ms should have deleted the .ms on CONTINUE"


def test_reprocess_exhaustion_quarantines(tmp_path, monkeypatch):
    failed_dir = tmp_path / "failed"
    monkeypatch.setattr(config.quality_gate, "max_attempts", 1)
    monkeypatch.setattr(config.storage, "failed_dir", str(failed_dir))

    processed = _receive_and_process(tmp_path)
    ms_path = tmp_path / "obs_e2e_raw_2024-01-01_00-00-00.ms"
    processed_dir = tmp_path / "obs_e2e_processed_2024-01-01_00-00-00_attempt1"
    assert ms_path.exists()
    assert processed_dir.exists()

    @flow
    def _resolve_reprocess(observation):
        resolve_review_cycle(observation, ReviewDecision.REPROCESS)

    _resolve_reprocess(processed)

    assert processed.state == ObservationState.FAILED
    assert not ms_path.exists(), "raw .ms should have been moved out, not left in place"
    assert (
        not processed_dir.exists()
    ), "processed output should have been moved out, not left in place"
    assert (
        failed_dir / ms_path.name
    ).exists(), "raw .ms should be quarantined into failed_dir"
    assert (
        failed_dir / processed_dir.name
    ).exists(), "processed output should be quarantined into failed_dir"
