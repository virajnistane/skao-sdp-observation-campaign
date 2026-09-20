#!/usr/bin/env python3
# Author: Viraj Nistane
# Description: This file contains the task to receive visibilities from the SDP pipeline
# and store them in a specified directory.


import os
from pathlib import Path
import logging
from config import config

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


if __name__ == "__main__":
    # Example usage
    ms_path = str(ROOT_DIR / "data" / "out.ms")
    size_mb = get_ms_size_mb(ms_path)
    logging.info(f"Size of the Measurement Set: {size_mb:.2f} MB")
