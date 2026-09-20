#!/usr/bin/env python3
# Author: Viraj Nistane
# Description: This file contains the task to receive visibilities from the SDP pipeline and store them in a specified directory.

import logging
from pathlib import Path

from prefect import task, get_run_logger

from sdp_control.utils.docker_runner import run_container # type: ignore
from sdp_control.models import Observation, ObservationState # type: ignore
from sdp_control.config import config


@task(
    name="receive_visibilities", 
    retries=3, 
    retry_delay_seconds=10, 
    log_prints=True
)
def receive_visibilities(observation: Observation) -> Observation:

    logger = get_run_logger()
    # logger = logging.getLogger(__name__)

    receive_cfg = config.containers.receive
    mount_path = config.containers.mount_path

    ms_dir_host = Path(observation.ms_dir)
    ms_dir_host.mkdir(parents=True, exist_ok=True)
    ms_path_container = Path(mount_path) / f"{observation.id}.ms"

    observation.update_state(ObservationState.RECEIVING)
    logger.info(f"{observation.state.name}: {observation.id} -> {ms_dir_host}")

    try:
        # Run the Docker container to receive visibilities
        command_parts = [*receive_cfg.command, str(ms_path_container)]
        run_container(
            image=receive_cfg.image,
            command=" ".join(command_parts),
            volumes={str(ms_dir_host): mount_path},
        )

        # Update the observation state to STORED after successful reception
        observation.update_state(ObservationState.STORED)
        logger.info(f"Successfully received visibilities for observation {observation.id}")

    except Exception as e:
        logger.error(f"Failed to receive visibilities for observation {observation.id}: {e}")
        observation.update_state(ObservationState.FAILED)

    return observation