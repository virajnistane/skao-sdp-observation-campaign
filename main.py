#!/usr/bin/env python3
# Author: Viraj Nistane
# Description: This file contains the task to receive visibilities from the SDP pipeline
# and store them in a specified directory.


import os
import numpy as np
from pathlib import Path
from sdp_control.config import config # type: ignore
from sdp_control.tasks.storage import ( # type: ignore
    get_ms_size_mb, get_total_ms_size_mb, storage_full
 ) 
from sdp_control.models import Observation, ObservationState # type: ignore
from sdp_control.tasks.receive_vis import receive_visibilities # type: ignore
from sdp_control.tasks.process_vis import process_visibilities # type: ignore
from prefect import flow, task, get_run_logger

@flow(name="long_term_observation_campaign", log_prints=True)
def main():

    logger = get_run_logger()

    # long term observation campaign
    # This is a simple example of how to use the SDP pipeline tasks

    # Measure size of all the Measurement Sets before processing
    ms_size_total_mb = get_total_ms_size_mb(config.storage.data_dir, 0.0)
    logger.info(f"Total size of all Measurement Sets before starting campaign: {ms_size_total_mb:.2f} MB")

    iter = 0
    process_futures = []

    while True:
        if storage_full(ms_size_total_mb):
            logger.info(
                f"Storage limit exceeded: {ms_size_total_mb:.2f} MB > {config.storage.storage_threshold_mb:.2f} MB"
            )
            break

        # Create an observation in the RECEIVING state
        obs = Observation(
            id=f"obs_{iter:03d}", 
            ms_dir=config.storage.data_dir, 
            state=ObservationState.RECEIVING
        )

        # Call the receive_visibilities function
        updated_obs = receive_visibilities(obs)

        # Check that the state has been updated to STORED
        if updated_obs.state != ObservationState.STORED:
            logger.info(f"Failed to receive visibilities for observation {updated_obs.id}. Current state: {updated_obs.state.name}")
            break

        # Submit process_visibilities to run concurrently; the loop doesn't wait on it
        process_futures.append(process_visibilities.submit(updated_obs))

        # Update the total size of all the Measurement Sets
        ms_size_total_mb = get_total_ms_size_mb(config.storage.data_dir, ms_size_total_mb)
        logger.info(f"Total size of all Measurement Sets after processing: {ms_size_total_mb:.2f} MB")

        iter += 1

    # Wait for all submitted processing tasks to finish and check their outcome
    for future in process_futures:
        processed_obs = future.result()
        if processed_obs.state != ObservationState.AWAITING_REVIEW:
            logger.info(f"Failed to process visibilities for observation {processed_obs.id}. Current state: {processed_obs.state.name}")

if __name__ == "__main__":
    main()
