#!/usr/bin/env python3
# Author: Viraj Nistane
# Description: This file contains the task to review the processed visibilities 
# and generate an interactive preview using Dash. 

from dotenv import load_dotenv
import os
from pathlib import Path
from enum import StrEnum
from typing import Any, cast

from prefect import task, get_run_logger
from prefect.flow_runs import pause_flow_run
from prefect.input import RunInput
from prefect.settings import PREFECT_UI_URL
from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import GlobalConcurrencyLimitCreate
from prefect.concurrency.sync import concurrency
from prefect.blocks.notifications import SlackWebhook
from pydantic import SecretStr

from sdp_control.config import config
from sdp_control.models import Observation, ObservationState
from sdp_control.tasks.preview_artifact import create_preview_artifact
from sdp_control.tasks.receive_vis import receive_visibilities, remove_ms, quarantine_ms
from sdp_control.tasks.process_vis import process_visibilities

load_dotenv()  # reads .env in the current working directory into os.environ
SlackWebhook(url=SecretStr(os.environ["PREFECT_SLACK_WEBHOOK_URL"])).save(name="sdp-review-alerts", overwrite=True)

class ReviewDecision(StrEnum):
    CONTINUE = "continue"
    REPROCESS = "re-process"

class ReviewDecisionInput(RunInput):
    decision: ReviewDecision

REVIEW_PAUSE_LIMIT_NAME = "review-pause"


def _ensure_review_pause_limit() -> None:
    """Create the global concurrency limit that serializes pause_flow_run calls, if it doesn't exist yet."""
    client = get_client(sync_client=True)
    try:
        client.read_global_concurrency_limit_by_name(REVIEW_PAUSE_LIMIT_NAME)
    except Exception:
        client.create_global_concurrency_limit(
            GlobalConcurrencyLimitCreate(
                name=REVIEW_PAUSE_LIMIT_NAME,
                limit=1,
            )
        )

def _notify_review_needed(observation_id: str, artifact_link: str, ui_url: str) -> None:
    """Best-effort Slack alert for a paused review; never blocks the pause itself."""
    logger = get_run_logger()
    try:
        slack = SlackWebhook.load("sdp-review-alerts")
        slack.notify( # type: ignore
            f"Observation {observation_id} needs review: {artifact_link}\n"
            f"Respond in Prefect UI: {ui_url}"
        )
    except Exception as e:
        logger.warning(f"Failed to send Slack review notification for {observation_id}: {e}")


@task(
    name="review_processed_visibilities",
    task_run_name="review-{observation.id}-attempt{observation.processing_attempt}",
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
    processed_data_dir = Path(config.storage.data_dir) / f"{observation.id}_processed_{observation.datetime_stamp}_attempt{observation.processing_attempt}"

    # Check if the processed data directory exists
    if not processed_data_dir.exists():
        raise FileNotFoundError(f"Processed data directory does not exist: {processed_data_dir}")

    # Create a preview artifact for the observation
    preview_artifact_id = create_preview_artifact(observation)

    # Pause until the reviewer chooses the next workflow action.
    # Serialized: pause_flow_run pauses the whole flow run, not just this task,
    # so only one review (original or reprocess-triggered) may be paused at a time.
    # description identifies the observation and links its preview artifact in the resume-run modal.
    preview_url = f"{PREFECT_UI_URL.value().rstrip('/')}/artifacts/artifact/{preview_artifact_id}"
    review_input = ReviewDecisionInput.with_initial_data(
        description=(
            f"Review decision for observation **{observation.id}**\n\n"
            f"[View preview artifact]({preview_url})"
        )
    )
    _ensure_review_pause_limit()
    with concurrency(REVIEW_PAUSE_LIMIT_NAME, occupy=1):
        logger.warning(f"⏸️  PAUSED — awaiting review for observation {observation.id}. Open {PREFECT_UI_URL.value().rstrip('/')} to respond.")
        _notify_review_needed(observation.id, preview_url, PREFECT_UI_URL.value().rstrip('/'))
        result = pause_flow_run(
            wait_for_input=review_input,
            timeout=900,  # Timeout after 15 minutes
        )
    decision = result.decision
    logger.info(f"Review decision for {observation.id}: {decision}")
    return decision  # Return the decision made by the reviewer


@task(
    name="resolve_review_cycle", 
    task_run_name="resolve-review-{observation.id}",
    retries=3,
    retry_delay_seconds=10,
    log_prints=True)
def resolve_review_cycle(
    observation: Observation,
    decision: ReviewDecision | None,
) -> None:
    """Act on a review decision, resubmitting process+review on REPROCESS until resolved."""
    logger = get_run_logger()
    max_attempts = config.quality_gate.max_attempts
    while True:
        if decision == ReviewDecision.CONTINUE:
            remove_ms.submit(observation)
            return
    
        if decision == ReviewDecision.REPROCESS:
            if observation.processing_attempt >= max_attempts:
                logger.warning(
                    f"Observation {observation.id} hit max reprocess attempts ({max_attempts}); "
                    "marking FAILED and quarantining .ms and processed files out of the storage count."
                )
                observation.update_state(ObservationState.FAILED)
                quarantine_ms.submit(observation)
                return
            observation.processing_attempt += 1
            process_future = process_visibilities.submit(observation)
            submit_review = cast(Any, review_processed_visibilities.submit)
            review_future = submit_review(cast(Observation, process_future))
            observation = process_future.result()
            decision = review_future.result()
            continue

        # decision is None (pause_flow_run timed out) or unexpected — stop, leave .ms as-is
        logger.warning(
            f"No actionable review decision for {observation.id} (got {decision!r}); leaving .ms in place."
        )
        return

