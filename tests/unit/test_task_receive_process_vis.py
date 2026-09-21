
from pathlib import Path
from sdp_control.models import ObservationState, Observation
from sdp_control.tasks.receive_vis import receive_visibilities
from sdp_control.tasks.process_vis import process_visibilities
from sdp_control.tasks.review import ReviewDecision
import pytest


@pytest.fixture
def received_observation(tmp_path):
    # process_visibilities operates on an existing .ms, so receive it first
    obs = Observation(id="obs_test", ms_dir=str(tmp_path), state=ObservationState.RECEIVING)
    return receive_visibilities(obs)


def test_review_processed_visibilities():
    assert ReviewDecision.CONTINUE.value == "continue"
    assert ReviewDecision.REPROCESS.value == "re-process"


def test_receive_visibilities(received_observation):
    assert received_observation.state == ObservationState.STORED

    ms_path = Path(received_observation.ms_dir) / "obs_test.ms"
    assert ms_path.exists(), f"Expected {ms_path} to be created by receive_visibilities"


def test_process_visibilities(received_observation):
    updated_obs = process_visibilities(received_observation)

    # Check that the state has been updated to AWAITING_REVIEW
    assert updated_obs.state == ObservationState.AWAITING_REVIEW

    # Check that the ms_dir exists and is a directory
    ms_dir_path = Path(updated_obs.ms_dir)
    assert ms_dir_path.exists()
    assert ms_dir_path.is_dir()

    # Check that the expected files are present in the processed output
    output_dir = ms_dir_path / "obs_test_processed"
    expected = [
        "out-dirty.fits",
        "out-image.fits",
        "out-psf.fits",
        "out-residual.fits",
        "out-model.fits",
    ]
    for file_name in expected:
        file_path = output_dir / file_name
        assert file_path.exists(), f"Expected file {file_name} not found in {output_dir}"