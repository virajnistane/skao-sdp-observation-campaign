#!/usr/bin/env python3
# Author: Viraj Nistane
# Description: Unit tests for resolve_review_cycle's REPROCESS loop and
# _notify_review_needed's fail-open Slack behavior. No real Prefect orchestration
# or Docker involved - .submit() calls are monkeypatched to plain fakes.

import logging
from unittest.mock import MagicMock

from sdp_control.config import config
from sdp_control.models import Observation, ObservationState
from sdp_control.tasks import review as review_module
from sdp_control.tasks.review import ReviewDecision, resolve_review_cycle


class _FakeFuture:
    def __init__(self, value):
        self._value = value

    def result(self):
        return self._value


def test_resolve_review_cycle_reprocess_then_continue(monkeypatch):
    monkeypatch.setattr(config.quality_gate, "max_attempts", 5)

    initial_obs = Observation(
        id="obs_r",
        ms_dir="/tmp/whatever",
        state=ObservationState.AWAITING_REVIEW,
        datetime_stamp="x",
        processing_attempt=1,
    )
    reprocessed_obs = Observation(
        id="obs_r",
        ms_dir="/tmp/whatever",
        state=ObservationState.AWAITING_REVIEW,
        datetime_stamp="x",
        processing_attempt=2,
    )

    process_submit = MagicMock(return_value=_FakeFuture(reprocessed_obs))
    review_submit = MagicMock(return_value=_FakeFuture(ReviewDecision.CONTINUE))
    remove_submit = MagicMock()

    monkeypatch.setattr(review_module.process_visibilities, "submit", process_submit)
    monkeypatch.setattr(
        review_module.review_processed_visibilities, "submit", review_submit
    )
    monkeypatch.setattr(review_module.remove_ms, "submit", remove_submit)
    # Storage isn't actually the point of this test; keep it off the real
    # config.storage.data_dir so the new pre-reprocess recheck doesn't scan
    # the real project disk.
    monkeypatch.setattr(review_module, "storage_full", lambda **kwargs: False)

    resolve_review_cycle(initial_obs, ReviewDecision.REPROCESS)

    process_submit.assert_called_once()
    remove_submit.assert_called_once()
    resolved_obs = remove_submit.call_args.args[0]
    assert (
        resolved_obs.processing_attempt == 2
    ), "the loop's incremented observation should carry through to CONTINUE"


def test_resolve_review_cycle_reprocess_skipped_when_storage_full(monkeypatch):
    monkeypatch.setattr(config.quality_gate, "max_attempts", 5)  # plenty of attempts left
    monkeypatch.setattr(review_module, "get_total_ms_size_mb", lambda *a, **k: 9999.0)
    monkeypatch.setattr(review_module, "storage_full", lambda **kwargs: True)

    process_submit = MagicMock()
    quarantine_submit = MagicMock()
    monkeypatch.setattr(review_module.process_visibilities, "submit", process_submit)
    monkeypatch.setattr(review_module.quarantine_ms, "submit", quarantine_submit)

    obs = Observation(
        id="obs_s",
        ms_dir="/tmp/whatever",
        state=ObservationState.AWAITING_REVIEW,
        datetime_stamp="x",
        processing_attempt=1,
    )

    resolve_review_cycle(
        obs, ReviewDecision.REPROCESS, storage_threshold_mb=100, storage_count_scope="ms_only"
    )

    process_submit.assert_not_called()
    quarantine_submit.assert_called_once()
    assert obs.state == ObservationState.FAILED


def test_notify_review_needed_fails_open(monkeypatch, caplog):
    # _notify_review_needed is a plain helper, not a @task, so it has no Prefect
    # run context of its own when called directly - get_run_logger() would raise
    # MissingContextError without this patch.
    monkeypatch.setattr(
        review_module, "get_run_logger", lambda: logging.getLogger("test_review")
    )
    monkeypatch.setattr(review_module, "_ensure_slack_block", lambda: None)
    monkeypatch.setattr(
        review_module.SlackWebhook, "load", MagicMock(side_effect=RuntimeError("boom"))
    )

    with caplog.at_level(logging.WARNING, logger="test_review"):
        review_module._notify_review_needed(
            "obs_x", "http://artifact", "http://ui"
        )  # must not raise

    assert "Failed to send Slack review notification" in caplog.text
