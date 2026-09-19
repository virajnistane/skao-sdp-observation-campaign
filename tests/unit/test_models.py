#!/usr/bin/env python3
# Author: Viraj Nistane
# Description: This file contains unit tests for the models defined in src/sdp_control/models.py.

from pathlib import Path

from sdp_control.models import ObservationState, Observation # type: ignore

ROOT_DIR = Path(__file__).parent.parent.parent

def test_observation_state_enum():
    # Test the ObservationState enum
    assert ObservationState.RECEIVING.value == "receiving"
    assert ObservationState.STORED.value == "stored"
    assert ObservationState.PROCESSING.value == "processing"
    assert ObservationState.AWAITING_REVIEW.value == "awaiting_review"
    assert ObservationState.DONE.value == "done"
    assert ObservationState.FAILED.value == "failed"

    assert ObservationState.RECEIVING.name == "RECEIVING"
    assert ObservationState.STORED.name == "STORED"
    assert ObservationState.FAILED.name == "FAILED"
    assert ObservationState.PROCESSING.name == "PROCESSING"
    assert ObservationState.AWAITING_REVIEW.name == "AWAITING_REVIEW"
    assert ObservationState.DONE.name == "DONE"

    assert len(ObservationState) == 6  # There are 6 states defined in the enum

def test_observation_model():
    # Test the Observation model
    obs = Observation(id="obs_test", state=ObservationState.RECEIVING, ms_dir=str(ROOT_DIR / "data"))
    assert obs.id == "obs_test"
    assert obs.state == ObservationState.RECEIVING
    assert obs.ms_dir == f"{ROOT_DIR}/data"
