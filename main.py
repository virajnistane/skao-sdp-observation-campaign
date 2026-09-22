#!/usr/bin/env python3
# Author: Viraj Nistane
# Description: This file contains the task to receive visibilities from the SDP pipeline
# and store them in a specified directory.


import os
import numpy as np
from pathlib import Path
from typing import Any, cast
from datetime import datetime
import logging

from sdp_control.config import config
from sdp_control.tasks.storage import (
    get_ms_size_mb, get_total_ms_size_mb, storage_full
 ) 
from sdp_control.models import Observation, ObservationState
from sdp_control.tasks.receive_vis import receive_visibilities
from sdp_control.tasks.process_vis import process_visibilities
from sdp_control.tasks.review import ReviewDecision, review_processed_visibilities, resolve_review_cycle
from prefect import flow, task, get_run_logger
from prefect.futures import PrefectFuture



@flow(name="long_term_observation_campaign", log_prints=True)
def main() -> None:
    logger = get_run_logger()
    logger.info("Starting long-term observation campaign")
    
    # Get the current timestamp for naming observations
    datetime_stamp: str = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")

    # long term observation campaign
    # This is a simple example of how to use the SDP pipeline tasks

    # Measure size of all the Measurement Sets before processing
    ms_size_total_mb = get_total_ms_size_mb(config.storage.data_dir, 0.0)
    logger.info(f"Total size of all Measurement Sets before starting campaign: {ms_size_total_mb:.2f} MB")

    # Start the observation loop
    logger.info("Starting observation campaign. Will continue until storage limit is exceeded or an error occurs.")
    iter = 0
    
    # List of tuples (PrefectFuture[Observation], PrefectFuture[ReviewDecision | None]) for review tasks
    # This allows us to wait for all review tasks to complete at the end of the flow and handle their outcomes.
    future_list = []

    # Keep track of the prior review future to chain reviews
    # This ensures that each review starts after the previous one has completed, allowing for sequential decision-making.
    prior_review_future: PrefectFuture[ReviewDecision | None] | None = None

    while True:
        # Check if the storage limit has been exceeded before starting a new observation
        if storage_full(current_total_size_mb=ms_size_total_mb):
            logger.info(
                f"Storage limit exceeded: {ms_size_total_mb:.2f} MB > {config.storage.storage_threshold_mb:.2f} MB"
            )
            break

        # Create an observation in the RECEIVING state
        obs = Observation(
            id=f"obs_{iter:03d}", 
            ms_dir=config.storage.data_dir, 
            state=ObservationState.RECEIVING,
            datetime_stamp=datetime_stamp
        )

        # Call the receive_visibilities function
        updated_obs = receive_visibilities(obs)

        # Check that the state has been updated to STORED
        if updated_obs.state != ObservationState.STORED:
            logger.info(f"Failed to receive visibilities for observation {updated_obs.id}. Current state: {updated_obs.state.name}")
            break

        # Submit process_visibilities to run concurrently; the loop doesn't wait on it
        process_future = process_visibilities.submit(updated_obs)

        # Submit review chained to its own process future and the prior review,
        # so it starts as soon as both are done rather than waiting on the whole receive loop
        submit_review = cast(Any, review_processed_visibilities.submit) # Type hint for the review function
        review_future = submit_review(
            cast(Observation, process_future), # Type hint for the process future
            wait_for=prior_review_future, # Wait for the prior review to finish before starting this one
        )
        future_list.append((process_future, review_future))
        prior_review_future = review_future

        # Update the total size of all the Measurement Sets
        ms_size_total_mb = get_total_ms_size_mb(config.storage.data_dir, ms_size_total_mb)
        logger.info(f"Total size of all Measurement Sets after processing: {ms_size_total_mb:.2f} MB")

        iter += 1

    # Wait for all submitted processing tasks to finish and check their outcome
    resolve_futures = []
    for process_future, review_future in future_list:
        processed_obs = process_future.result()
        if processed_obs.state != ObservationState.AWAITING_REVIEW:
            logger.info(f"Failed to process visibilities for observation {processed_obs.id}. Current state: {processed_obs.state.name}")

        # Submit (not call) so each observation's decision is acted on as soon as
        # it resolves, instead of blocking behind earlier observations' reprocess cycles.
        resolve_futures.append(resolve_review_cycle.submit(processed_obs, review_future))

    for f in resolve_futures:
        f.result()

if __name__ == "__main__":
    main.serve()
