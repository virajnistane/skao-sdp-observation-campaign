#!/usr/bin/env python3
# Author: Viraj Nistane
# Description: This file contains utility functions to run commands in a Kubernetes pod.

import logging
import shlex
import time
import uuid

from kubernetes import client
from kubernetes import config as k8s_config  # type: ignore[import]
from kubernetes.client.rest import ApiException  # type: ignore[import]

from sdp_control.config import config

logger = logging.getLogger(__name__)

logger.info("Loading Kubernetes configuration...")
k8s_config.load_kube_config()


def _job_name(image: str) -> str:
    """Generate a unique job name for the Kubernetes job."""
    slug = (
        image.split("/")[-1].split(":")[0].lower()
    )  # Get the last part of the image name and remove the tag
    return f"{slug}-{uuid.uuid4().hex[:8]}"  # Append a short UUID for uniqueness


def run_container(image: str, command: str, volumes: dict) -> None:
    """
    Run a command in a Kubernetes pod using the specified image and volume.

    Args:
        image (str): The Docker image to use.
        command (str): The command to run inside the container.
        volumes (dict): A dictionary mapping host paths to container paths for volume mounting.

    Returns:
        None

    Raises:
        RuntimeError: If the Kubernetes job fails.
    """
    job_name = _job_name(image)
    logger.info(
        f"Running Kubernetes job: {job_name} with image: {image} and command: {command}"
    )

    # Define the container
    container = client.V1Container(
        name=job_name,
        image=image,
        command=shlex.split(command),
        volume_mounts=[
            client.V1VolumeMount(name="data-volume", mount_path=container_path)
            for host_path, container_path in volumes.items()
        ],
    )

    # Define the volume
    volumes_2 = [
        client.V1Volume(
            name="data-volume",
            persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                claim_name=config.kubernetes.pvc_name  # Assuming you have a PVC defined in your Kubernetes cluster
            ),
        )
    ]

    # Define the pod template
    template = client.V1PodTemplateSpec(
        metadata=client.V1ObjectMeta(labels={"job-name": job_name}),
        spec=client.V1PodSpec(
            restart_policy="Never", containers=[container], volumes=volumes_2
        ),
    )

    # Define the job spec
    job_spec = client.V1JobSpec(
        template=template,
        backoff_limit=0,
        active_deadline_seconds=config.kubernetes.active_deadline_seconds,
    )

    # Define the job
    job = client.V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=client.V1ObjectMeta(name=job_name),
        spec=job_spec,
    )

    # Create the job
    batch_v1 = client.BatchV1Api()
    batch_v1.create_namespaced_job(namespace=config.kubernetes.namespace, body=job)

    try:
        while True:
            # Check the job status
            job_status = batch_v1.read_namespaced_job_status(
                name=job_name, namespace=config.kubernetes.namespace
            ).status
            if job_status.succeeded is not None and job_status.succeeded > 0:
                logger.info(f"Kubernetes job {job_name} completed successfully.")
                break
            elif job_status.failed is not None and job_status.failed > 0:
                logger.error(f"Kubernetes job {job_name} failed.")
                raise RuntimeError(f"Kubernetes job {job_name} failed.")
            else:
                logger.info(f"Kubernetes job {job_name} is still running...")
                time.sleep(2)  # Polling interval
    finally:
        # Clean up the job after completion or failure
        try:
            batch_v1.delete_namespaced_job(
                name=job_name,
                namespace=config.kubernetes.namespace,
                propagation_policy="Background",  # Ensure the job and its pods are deleted
            )
            logger.info(f"Kubernetes job {job_name} deleted.")
        except ApiException as e:
            logger.error(f"Failed to delete Kubernetes job {job_name}: {e}")
