#!/usr/bin/env python3
# Author: Viraj Nistane
# Description: This file contains the task to receive visibilities from the SDP pipeline
# and store them in a specified directory.


import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Literal, cast

import numpy as np
from prefect import flow, get_run_logger, task
from prefect.futures import PrefectFuture

from sdp_control.config import config
from sdp_control.models import Observation, ObservationState
from sdp_control.tasks.process_vis import process_visibilities
from sdp_control.tasks.receive_vis import receive_visibilities
from sdp_control.tasks.review import (ReviewDecision, resolve_review_cycle,
                                      review_processed_visibilities)
from sdp_control.tasks.storage import get_total_ms_size_mb, storage_full


@flow(name="long-term-observation-campaign", log_prints=True)
def main(
    storage_threshold_mb: int | float = config.storage.storage_threshold_mb,
    storage_count_scope: Literal["all", "ms_only"] = config.storage.count_scope,
    storage_wait_indefinite: bool = config.observation.storage_wait_indefinite,
) -> None:

    logger = get_run_logger()
    logger.info("Starting long-term observation campaign")

    # Get the current timestamp for naming observations
    datetime_stamp: str = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")

    # long term observation campaign
    # This is a simple example of how to use the SDP pipeline tasks

    # Measure size of all the Measurement Sets before processing
    ms_size_total_mb = get_total_ms_size_mb(
        config.storage.data_dir, 0.0, storage_count_scope=storage_count_scope
    )
    logger.info(
        f"Total size of all Measurement Sets before starting campaign: {ms_size_total_mb:.2f} MB"
    )

    # Start the observation loop
    logger.info(
        "Starting observation campaign. Will continue until storage limit is exceeded or an error occurs."
    )
    iter = 0

    # Futures for resolve_review_cycle (remove_ms / reprocess), submitted per-observation
    # inside the loop below so cleanup runs concurrently and isn't gated behind loop exit
    # (the storage-wait retry loop can only ever free space if remove_ms keeps firing).
    resolve_futures = []

    # Keep track of the prior review future to chain reviews
    # This ensures that each review starts after the previous one has completed, allowing for sequential decision-making.
    prior_review_future: PrefectFuture[ReviewDecision | None] | None = None

    while True:
        # Check if the storage limit has been exceeded before starting a new observation.
        # Wait and retry instead of stopping outright, since review actions running
        # concurrently free space (remove_ms) and the backlog is often transient.
        if storage_full(
            current_total_size_mb=ms_size_total_mb,
            storage_threshold_mb=storage_threshold_mb,
        ):
            logger.info(
                f"Storage limit exceeded: {ms_size_total_mb:.2f} MB > {storage_threshold_mb:.2f} MB. "
                "Waiting for in-flight review actions to free space."
            )
            attempt = 0
            while attempt < config.observation.retry_attempts or (
                storage_wait_indefinite
                and attempt < config.observation.storage_wait_safety_limit
            ):
                attempt += 1
                time.sleep(config.observation.retry_delay_seconds)
                ms_size_total_mb = get_total_ms_size_mb(
                    config.storage.data_dir,
                    ms_size_total_mb,
                    storage_count_scope=storage_count_scope,
                )
                if not storage_full(
                    current_total_size_mb=ms_size_total_mb,
                    storage_threshold_mb=storage_threshold_mb,
                ):
                    logger.info(
                        f"Storage freed to {ms_size_total_mb:.2f} MB after {attempt} retry(ies); resuming campaign."
                    )
                    break
                logger.info(
                    f"Still over threshold ({ms_size_total_mb:.2f} MB); retry {attempt}."
                )
            else:
                limit_hit = (
                    config.observation.storage_wait_safety_limit
                    if storage_wait_indefinite
                    else config.observation.retry_attempts
                )
                logger.info(
                    f"Storage still full after {limit_hit} retries; stopping campaign."
                )
                break

        # Create an observation in the RECEIVING state
        obs = Observation(
            id=f"obs_{iter:03d}",
            ms_dir=config.storage.data_dir,
            state=ObservationState.RECEIVING,
            datetime_stamp=datetime_stamp,
        )

        # Call the receive_visibilities function
        updated_obs = receive_visibilities(obs)

        # Check that the state has been updated to STORED
        if updated_obs.state != ObservationState.STORED:
            logger.info(
                f"Failed to receive visibilities for observation {updated_obs.id}. Current state: {updated_obs.state.name}"
            )
            break

        # Submit process_visibilities to run concurrently; the loop doesn't wait on it
        process_future = process_visibilities.submit(updated_obs)

        # Submit review chained to its own process future and the prior review,
        # so it starts as soon as both are done rather than waiting on the whole receive loop
        submit_review = cast(
            Any, review_processed_visibilities.submit
        )  # Type hint for the review function
        review_future = submit_review(
            cast(Observation, process_future),  # Type hint for the process future
            wait_for=prior_review_future,  # Wait for the prior review to finish before starting this one
        )
        prior_review_future = review_future

        # Submit (not call) immediately so remove_ms/reprocess runs as soon as this
        # observation's decision resolves, independent of the receive loop still running
        # (including while it's waiting out a storage_full retry above).
        submit_resolve = cast(Any, resolve_review_cycle.submit)
        resolve_futures.append(
            submit_resolve(cast(Observation, process_future), review_future)
        )

        # Update the total size of all the Measurement Sets
        ms_size_total_mb = get_total_ms_size_mb(
            config.storage.data_dir,
            ms_size_total_mb,
            storage_count_scope=storage_count_scope,
        )
        logger.info(
            f"Total size of all Measurement Sets after processing: {ms_size_total_mb:.2f} MB"
        )

        iter += 1

    # Wait for all in-flight resolve_review_cycle tasks (remove_ms / reprocess) to finish.
    for f in resolve_futures:
        f.result()


if __name__ == "__main__":
    main.serve(
        name="long-term-observation-campaign",
        tags=["skao", "sdp", "long-term-observation-campaign"],
        parameters={
            "storage_threshold_mb": config.storage.storage_threshold_mb,
            "storage_count_scope": config.storage.count_scope,
            "storage_wait_indefinite": config.observation.storage_wait_indefinite,
        },
    )
