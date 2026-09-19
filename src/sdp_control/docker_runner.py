#!/usr/bin/env python3
# Author: Viraj Nistane
# Description: This file contains the task to receive visibilities from the SDP pipeline
# and store them in a specified directory.

import logging
import shlex
import subprocess

logger = logging.getLogger(__name__)


def run_container(image: str, command: str, volumes: dict) -> None:
    """
    Run a Docker container with the specified image, command, and volume mappings.

    Args:
        image (str): The Docker image to use.
        command (str): The command to run inside the container.
        volumes (dict): A dictionary mapping host paths to container paths for volume mounting.

    Returns:
        None

    Raises:
        RuntimeError: If the Docker command fails.
    """
    # Construct the volume arguments for the Docker command
    volume_args: list[str] = []
    for host_path, container_path in volumes.items():
        volume_args.extend(["-v", f"{host_path}:{container_path}"])

    # Construct the full Docker command
    docker_command = ["docker", "run", "--rm"] + volume_args + [image] + shlex.split(command)

    logger.info(f"Running Docker container: {' '.join(docker_command)}")

    try:
        # Run the Docker command
        subprocess.run(docker_command, check=True)
        logger.info("Docker container ran successfully.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Docker command failed with exit code {e.returncode}: {e}")
        raise RuntimeError(f"Docker command failed: {e}") from e
