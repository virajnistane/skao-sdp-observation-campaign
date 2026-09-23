
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from sdp_control.config import config
from sdp_control.models import ObservationState, Observation
from sdp_control.tasks import receive_vis, process_vis
from sdp_control.tasks.review import ReviewDecision


@pytest.fixture
def mock_run_container(monkeypatch):
    # Patched at the point of use in each task module (both do
    # `from sdp_control.utils.docker_runner import run_container`, binding a
    # local name), not on docker_runner itself.
    mock = MagicMock()
    monkeypatch.setattr(receive_vis, "run_container", mock)
    monkeypatch.setattr(process_vis, "run_container", mock)
    return mock


@pytest.fixture
def received_observation(tmp_path, mock_run_container):
    # process_visibilities operates on an existing .ms, so receive it first
    obs = Observation(id="obs_test", ms_dir=str(tmp_path), state=ObservationState.RECEIVING, datetime_stamp="2024-01-01_00-00-00")
    return receive_vis.receive_visibilities(obs)


def test_review_processed_visibilities():
    assert ReviewDecision.CONTINUE.value == "continue"
    assert ReviewDecision.REPROCESS.value == "re-process"


def test_receive_visibilities(received_observation, mock_run_container):
    assert received_observation.state == ObservationState.STORED

    mock_run_container.assert_called_once()
    kwargs = mock_run_container.call_args.kwargs
    assert kwargs["image"] == config.containers.receive.image
    assert "obs_test_raw_2024-01-01_00-00-00.ms" in kwargs["command"]
    assert kwargs["volumes"] == {received_observation.ms_dir: config.containers.mount_path}


def test_process_visibilities(received_observation, mock_run_container):
    updated_obs = process_vis.process_visibilities(received_observation)

    # Check that the state has been updated to AWAITING_REVIEW
    assert updated_obs.state == ObservationState.AWAITING_REVIEW

    # process_visibilities creates the output dir itself (before invoking the
    # container), so this is real, meaningful coverage without needing Docker.
    output_dir = Path(updated_obs.ms_dir) / "obs_test_processed_2024-01-01_00-00-00_attempt1"
    assert output_dir.is_dir()

    process_call = mock_run_container.call_args  # last call = the process step
    assert process_call.kwargs["image"] == config.containers.process.image
    assert "obs_test_processed_2024-01-01_00-00-00_attempt1" in process_call.kwargs["command"]
