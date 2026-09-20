#!/usr/bin/env python3
# Author: Viraj Nistane
# Description: This file contains the task to receive visibilities from the SDP pipeline
# and store them in a specified directory.


import os
import numpy as np
from pathlib import Path
import logging
from sdp_control.config import config

logging.basicConfig(level=config.logging.level, format="%(asctime)s - %(levelname)s - %(message)s")
logging.info("Starting storage tasks...")

ROOT_DIR = config.ROOT_DIR
MOUNT_PATH = config.containers.mount_path

def get_ms_size(ms_path: str) -> float:
    """
    Get the size of a Measurement Set (MS) directory in bytes.

    Args:
        ms_path (str): The path to the MS directory.

    Returns:
        float: The size of the MS directory in bytes.
    """
    logging.debug(f"Calculating size of Measurement Set at: {ms_path}")

    ms_size = 0.
    for dirpath, dirnames, filenames in os.walk(ms_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            ms_size += os.path.getsize(fp)
    return ms_size


def get_ms_size_mb(ms_path: str) -> float:
    """
    Get the size of a Measurement Set (MS) directory in megabytes.

    Args:
        ms_path (str): The path to the MS directory.
    
    Returns:
        float: The size of the MS directory in megabytes.
    """
    ms_size_bytes = get_ms_size(ms_path)
    ms_size_mb = ms_size_bytes / (1024 * 1024)  # Convert bytes to megabytes
    return ms_size_mb

def get_total_ms_size_mb(ms_dir: str | Path, current_total_size_mb: float) -> float:
    """
    Get the total size of all Measurement Set (MS) directories in a given directory in megabytes.

    Args:
        ms_dir (str | Path): The path to the directory containing MS directories.
        current_total_size_mb (float): The current total size in megabytes.
    Returns:
        float: The total size of all MS directories in megabytes.
    """
    if ms_dir is None or not isinstance(ms_dir, (str, Path)):
        raise ValueError("ms_dir must be a valid path")
    if current_total_size_mb is None or not isinstance(current_total_size_mb, (int, float)):
        raise ValueError("current_total_size_mb must be a valid int/float")

    current_total_size_mb = np.sum([
        get_ms_size_mb(str(Path(ms_dir) / ms_dir_name))
        for ms_dir_name in os.listdir(ms_dir)
        if (
            (Path(ms_dir) / ms_dir_name).is_dir() and 
            (Path(ms_dir) / ms_dir_name).suffix == ".ms"
        )
    ])
    return current_total_size_mb

def storage_full(current_total_size_mb: float) -> bool:
    """
    Check if the storage is full based on the configured threshold.

    Args:
        current_total_size_mb (float): The current total size in megabytes.

    Returns:
        bool: True if storage is full, False otherwise.
    """
    return current_total_size_mb > config.storage.storage_threshold_mb

if __name__ == "__main__":
    # Example usage
    ms_path = str(ROOT_DIR / "data" / "out.ms")
    size_mb = get_ms_size_mb(ms_path)
    logging.info(f"Size of the Measurement Set: {size_mb:.2f} MB")
