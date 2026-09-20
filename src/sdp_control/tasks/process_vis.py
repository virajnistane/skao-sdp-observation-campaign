#!/usr/bin/env python3
# Author: Viraj Nistane
# Description: This file contains the task to process visibilities from the SDP pipeline and store them in a specified directory.

import logging
from pathlib import Path

from prefect import task, get_run_logger

from sdp_control.utils.docker_runner import run_container # type: ignore
from sdp_control.models import Observation, ObservationState # type: ignore

MOCK_IMAGE = "docker.io/pw410/ska-sdp-mock:0.1"
PROCESS_SCRIPT = "/scripts/process_visibilities.sh"

def process_visibilities(observation: Observation) -> Observation:

    logger = logging.getLogger(__name__)

    ms_dir_host = Path(observation.ms_dir)
    ms_dir_host.mkdir(parents=True, exist_ok=True)
    ms_path_container = Path('/data') / f"{observation.id}.ms"
    out_path_container_prefix = Path('/data') / f"{observation.id}"

    observation.update_state(ObservationState.RECEIVING)
    logger.info(f"{observation.state.name}: {observation.id} -> {ms_dir_host}")

    try:
        # Run the Docker container to process visibilities
        run_container(
            image=MOCK_IMAGE,
            command=f"{PROCESS_SCRIPT} {str(ms_path_container)} {str(out_path_container_prefix)}",
            volumes={str(ms_dir_host): "/data"},
        )


        # Update the observation state to AWAITING_REVIEW after successful processing
        observation.update_state(ObservationState.AWAITING_REVIEW)
        logger.info(f"Successfully processed visibilities for observation {observation.id}")

    except Exception as e:
        logger.error(f"Failed to process visibilities for observation {observation.id}: {e}")
        observation.update_state(ObservationState.FAILED)

    return observation