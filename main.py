#!/usr/bin/env python3
# Author: Viraj Nistane
# Description: This file contains the task to receive visibilities from the SDP pipeline
# and store them in a specified directory.


import os
import numpy as np
from pathlib import Path
from sdp_control.config import config # type: ignore
from sdp_control.tasks.storage import get_ms_size_mb # type: ignore
from sdp_control.models import Observation, ObservationState # type: ignore
from sdp_control.tasks.receive_vis import receive_visibilities # type: ignore
from sdp_control.tasks.process_vis import process_visibilities # type: ignore


def main():

    # long term observation campaign
    # This is a simple example of how to use the SDP pipeline tasks

    # Measure size of all the Measurement Sets before processing
    ms_dir = str(config.ROOT_DIR / "data")
    ms_size_total_mb = np.sum([
        get_ms_size_mb(str(Path(ms_dir) / ms_dir_name))
        for ms_dir_name in os.listdir(ms_dir)
        if (Path(ms_dir) / ms_dir_name).is_dir()
    ])
    print(f"Total size of all Measurement Sets before processing: {ms_size_total_mb:.2f} MB")

    iter = 0 
    
    while True:
        
        if ms_size_total_mb > config.storage.storage_threshold_mb:
            print(f"Storage limit exceeded: {ms_size_total_mb:.2f} MB > {config.storage.storage_threshold_mb:.2f} MB")
            break

        # Create an observation in the RECEIVING state
        obs = Observation(id=f"obs_{iter:03d}", ms_dir=ms_dir, state=ObservationState.RECEIVING)

        # Call the receive_visibilities function
        updated_obs = receive_visibilities(obs)

        # Check that the state has been updated to STORED
        if updated_obs.state != ObservationState.STORED:
            print(f"Failed to receive visibilities for observation {updated_obs.id}. Current state: {updated_obs.state.name}")
            break

        # Call the process_visibilities function
        processed_obs = process_visibilities(updated_obs)

        # Check that the state has been updated to AWAITING_REVIEW
        if processed_obs.state != ObservationState.AWAITING_REVIEW:
            print(f"Failed to process visibilities for observation {processed_obs.id}. Current state: {processed_obs.state.name}")
            break

        # Update the total size of all the Measurement Sets
        ms_size_total_mb = np.sum([
            get_ms_size_mb(str(Path(ms_dir) / ms_dir_name))
            for ms_dir_name in os.listdir(ms_dir)
            if (Path(ms_dir) / ms_dir_name).is_dir()
        ])
        print(f"Total size of all Measurement Sets after processing: {ms_size_total_mb:.2f} MB")

        iter += 1

if __name__ == "__main__":
    main()
