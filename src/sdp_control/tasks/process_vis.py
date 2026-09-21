#!/usr/bin/env python3
# Author: Viraj Nistane
# Description: This file contains the task to process visibilities from the SDP pipeline and store them in a specified directory.

import logging
from pathlib import Path

from prefect import task, get_run_logger
from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import GlobalConcurrencyLimitCreate
from prefect.concurrency.sync import concurrency

from sdp_control.utils.docker_runner import run_container
from sdp_control.models import Observation, ObservationState
from sdp_control.config import config

PROCESS_CONCURRENCY_LIMIT_NAME = "process-visibilities"


def _ensure_process_concurrency_limit() -> None:
    """Create the global concurrency limit backing max_concurrency, if it doesn't exist yet."""
    client = get_client(sync_client=True)
    try:
        client.read_global_concurrency_limit_by_name(PROCESS_CONCURRENCY_LIMIT_NAME)
    except Exception:
        client.create_global_concurrency_limit(
            GlobalConcurrencyLimitCreate(
                name=PROCESS_CONCURRENCY_LIMIT_NAME,
                limit=config.processing.max_concurrency,
            )
        )


@task(
    name="process_visibilities",
    task_run_name="process-{observation.id}",
    retries=3, 
    retry_delay_seconds=10, 
    log_prints=True
)
def process_visibilities(observation: Observation) -> Observation:

    logger = get_run_logger()
    # logger = logging.getLogger(__name__)

    process_cfg = config.containers.process
    mount_path = config.containers.mount_path

    ms_dir_host = Path(observation.ms_dir)
    ms_dir_host.mkdir(parents=True, exist_ok=True)

    ms_path_container = Path(mount_path) / f"{observation.id}_raw_{observation.datetime_stamp}.ms"

    out_dir_host = ms_dir_host / f"{observation.id}_processed_{observation.datetime_stamp}"
    out_dir_host.mkdir(parents=True, exist_ok=True)
    out_path_container_prefix = Path(mount_path) / f"{observation.id}_processed_{observation.datetime_stamp}" / "out"

    observation.update_state(ObservationState.RECEIVING)
    logger.info(f"{observation.state.name}: {observation.id} -> {ms_dir_host}")

    try:
        # Cap concurrent Docker runs at config.processing.max_concurrency
        _ensure_process_concurrency_limit()
        command_parts = [*process_cfg.command, str(ms_path_container), str(out_path_container_prefix)]
        with concurrency(PROCESS_CONCURRENCY_LIMIT_NAME, occupy=1):
            run_container(
                image=process_cfg.image,
                command=" ".join(command_parts),
                volumes={str(ms_dir_host): mount_path},
            )


        # Update the observation state to AWAITING_REVIEW after successful processing
        observation.update_state(ObservationState.AWAITING_REVIEW)
        logger.info(f"Successfully processed visibilities for observation {observation.id}")

    except Exception as e:
        logger.error(f"Failed to process visibilities for observation {observation.id}: {e}")
        observation.update_state(ObservationState.FAILED)

    return observation