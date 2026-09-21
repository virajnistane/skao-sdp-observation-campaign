#!/usr/bin/env python3
# Author: Viraj Nistane
# Description: This file contains the task to review the processed visibilities 
# and generate an interactive preview using Dash. 

from pathlib import Path
from enum import StrEnum

from prefect import task, get_run_logger
from prefect.flow_runs import pause_flow_run

from sdp_control.config import config
from sdp_control.models import Observation, ObservationState

class ReviewDecision(StrEnum):
    CONTINUE = "continue"
    REPROCESS = "re-process"

@task(
    name="review_processed_visibilities",
    retries=3,
    retry_delay_seconds=10,
    log_prints=True
)
def review_processed_visibilities(observation: Observation) -> ReviewDecision | None:

    """
    Review the processed visibilities for a given observation.

    Args:
        observation (Observation): The observation to review.
    """
    logger = get_run_logger()

    if observation.state != ObservationState.AWAITING_REVIEW:
        logger.info(f"Skipping review for observation {observation.id}. Current state: {observation.state.name}")
        return None

    # Construct the path to the processed data directory
    processed_data_dir = Path(config.storage.data_dir) / f"{observation.id}_processed"

    # Check if the processed data directory exists
    if not processed_data_dir.exists():
        raise FileNotFoundError(f"Processed data directory does not exist: {processed_data_dir}")

    # Pause until the reviewer chooses the next workflow action.
    decision = pause_flow_run(
        wait_for_input=ReviewDecision,
        timeout=3600,  # Timeout after 1 hour
    )
    logger.info(f"Review decision for {observation.id}: {decision}")
    return decision  # Return the decision made by the reviewer


def remove_ms(observation_id: str | Path) -> None:
    """
    Remove the Measurement Set (MS) directory for a given observation ID.

    Args:
        observation_id (str): The ID of the observation whose MS directory is to be removed.
    """
    ms_dir = Path(config.storage.data_dir) / f"{observation_id}.ms"
    
    if ms_dir.exists():
        for item in ms_dir.iterdir():
            if item.is_file():
                item.unlink()  # Remove file
            elif item.is_dir():
                remove_ms(item)  # Recursively remove subdirectory
        ms_dir.rmdir()  # Remove the now-empty directory