#!/usr/bin/env python3
# Author: Viraj Nistane
# Description: This file contains the task to create a preview artifact for the processed visibilities.

from pathlib import Path
from typing import TypeAlias
import base64

from sdp_control.config import config
from sdp_control.models import Observation
from sdp_control.utils.plot_preview import fits2png

from prefect import task, get_run_logger
from prefect.artifacts import create_image_artifact, create_markdown_artifact


@task(
    name="create_preview_artifact",
    task_run_name="preview-{observation.id}",
    retries=3,
    retry_delay_seconds=10,
    log_prints=True
)
def create_preview_artifact(observation: Observation) -> None:

    """Create a Prefect artifact for the preview image.

    Args:
        observation (Observation): The observation object.
    """

    preview_path = Path(config.storage.data_dir) / f"{observation.id}_processed_{observation.datetime_stamp}" / "previews.png"
    if not preview_path.exists():
        processed_obs_files = list(
            (
                Path(config.storage.data_dir) / f"{observation.id}_processed_{observation.datetime_stamp}"
            ).glob("out*.fits")
        )
        processed_obs_files = [f for f in processed_obs_files if f.exists()]
        fits2png(processed_obs_files)

        preview_path = Path(config.storage.data_dir) / f"{observation.id}_processed_{observation.datetime_stamp}" / "previews.png"

    if not preview_path.exists():
        raise FileNotFoundError(f"Preview image does not exist: {preview_path}")

    image_b64 = base64.b64encode(preview_path.read_bytes()).decode("ascii")
    image_url = f"data:image/png;base64,{image_b64}"

    create_image_artifact(
        image_url=image_url,
        key=f"preview-{observation.id.replace('_', '-')}",
        description=f"Processed visibility preview for {observation.id}",
    )
