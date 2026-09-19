
from pathlib import Path
from sdp_control.models import ObservationState, Observation # type: ignore
from sdp_control.tasks.process_vis import process_visibilities # type: ignore
import pytest

ROOT_DIR = Path(__file__).parent.parent


def test_receive_visibilities():
    # Create an observation in the RECEIVING state
    obs = Observation(id="obs_test", ms_dir=str(ROOT_DIR / "data"), state=ObservationState.RECEIVING)


    # Call the process_visibilities function
    updated_obs = process_visibilities(obs)

    # Check that the state has been updated to AWAITING_REVIEW
    assert updated_obs.state == ObservationState.AWAITING_REVIEW

    # Check that the ms_dir exists and is a directory
    ms_dir_path = Path(updated_obs.ms_dir)
    assert ms_dir_path.exists()
    assert ms_dir_path.is_dir()

    # Check that the expected files are present in the ms_dir
    expected = [
        "obs_test-dirty.fits", 
        "obs_test-image.fits", 
        "obs_test-psf.fits", 
        "obs_test-residual.fits", 
        "obs_test-model.fits"
    ]
    for file_name in expected:
        file_path = ms_dir_path / file_name
        assert file_path.exists(), f"Expected file {file_name} not found in {ms_dir_path}"