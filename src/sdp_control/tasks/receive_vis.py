#!/usr/bin/env python3
# Author: Viraj Nistane
# Description: This file contains the task to receive visibilities from the SDP pipeline and store them in a specified directory.

import logging
import shutil
from pathlib import Path

from prefect import get_run_logger, task

from sdp_control.config import config
# from sdp_control.utils.k8s_runner import run_container
from sdp_control.models import Observation, ObservationState
from sdp_control.utils.docker_runner import run_container


@task(
    name="receive_visibilities",
    task_run_name="receive-{observation.id}",
    retries=3,
    retry_delay_seconds=10,
    log_prints=True,
)
def receive_visibilities(observation: Observation) -> Observation:

    logger = get_run_logger()
    # logger = logging.getLogger(__name__)

    receive_cfg = config.containers.receive
    mount_path = config.containers.mount_path

    ms_dir_host = Path(observation.ms_dir)
    ms_dir_host.mkdir(parents=True, exist_ok=True)
    ms_path_container = (
        Path(mount_path) / f"{observation.id}_raw_{observation.datetime_stamp}.ms"
    )

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
        logger.info(
            f"Successfully received visibilities for observation {observation.id}"
        )

    except Exception as e:
        logger.error(
            f"Failed to receive visibilities for observation {observation.id}: {e}"
        )
        observation.update_state(ObservationState.FAILED)

    return observation


@task(
    name="remove_ms",
    task_run_name="remove-ms-{observation.id}",
    retries=3,
    retry_delay_seconds=10,
    log_prints=True,
)
def remove_ms(observation: Observation) -> None:
    """Remove the Measurement Set directory for a given observation.

    Args:
        observation (Observation): The observation whose Measurement Set directory is to be removed.
    """
    logger = get_run_logger()
    ms_path_host = (
        Path(observation.ms_dir)
        / f"{observation.id}_raw_{observation.datetime_stamp}.ms"
    )
    if ms_path_host.exists():
        logger.info(f"Removing Measurement Set directory: {ms_path_host}")
        remove_directory(ms_path_host)
        logger.info(f"Successfully removed Measurement Set directory: {ms_path_host}")
    else:
        logger.warning(f"Measurement Set directory does not exist: {ms_path_host}")


@task(
    name="quarantine_ms",
    task_run_name="quarantine-ms-{observation.id}",
    retries=3,
    retry_delay_seconds=10,
    log_prints=True,
)
def quarantine_ms(observation: Observation) -> None:
    """Move a quality-gate-exhausted observation's Measurement Set and processed output out of storage.data_dir.

    Unlike remove_ms, this preserves the data (moved to storage.failed_dir) for manual
    inspection, while still excluding it from the storage_threshold_mb count — this matters
    under count_scope: "all" too, since every attempt's processed output dir would otherwise
    keep counting toward the threshold, not just the raw .ms.

    Args:
        observation (Observation): The observation whose Measurement Set and processed output are to be quarantined.
    """
    logger = get_run_logger()
    ms_dir_host = Path(observation.ms_dir)
    failed_dir = Path(config.storage.failed_dir)
    failed_dir.mkdir(parents=True, exist_ok=True)

    ms_path_host = ms_dir_host / f"{observation.id}_raw_{observation.datetime_stamp}.ms"
    if ms_path_host.exists():
        destination = failed_dir / ms_path_host.name
        logger.info(
            f"Quarantining Measurement Set directory: {ms_path_host} -> {destination}"
        )
        shutil.move(str(ms_path_host), str(destination))
        logger.info(
            f"Successfully quarantined Measurement Set directory to: {destination}"
        )
    else:
        logger.warning(
            f"Measurement Set directory does not exist, nothing to quarantine: {ms_path_host}"
        )

    processed_dirs = sorted(
        ms_dir_host.glob(
            f"{observation.id}_processed_{observation.datetime_stamp}_attempt*"
        )
    )
    for processed_dir in processed_dirs:
        destination = failed_dir / processed_dir.name
        logger.info(
            f"Quarantining processed output directory: {processed_dir} -> {destination}"
        )
        shutil.move(str(processed_dir), str(destination))
        logger.info(
            f"Successfully quarantined processed output directory to: {destination}"
        )


def remove_directory(path: Path) -> None:
    """Recursively remove a directory and its contents."""
    if path.is_dir():
        for item in path.iterdir():
            if item.is_dir():
                remove_directory(item)
            else:
                item.unlink()
        path.rmdir()


if __name__ == "__main__":
    # Example usage
    obs = Observation(
        id="obs_test",
        ms_dir=config.storage.data_dir,
        state=ObservationState.RECEIVING,
        datetime_stamp="2026-01-01T12-00-00",
    )
    updated_obs = receive_visibilities(obs)
