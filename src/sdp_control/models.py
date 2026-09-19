#!/usr/bin/env python3
# Author: Viraj Nistane
# Description: This file contains the task to receive visibilities from the SDP pipeline 
# and store them in a specified directory.

from enum import StrEnum
from dataclasses import dataclass
from pathlib import Path


class ObservationState(StrEnum):
    RECEIVING = "receiving"
    STORED = "stored"
    PROCESSING = "processing"
    AWAITING_REVIEW = "awaiting_review"
    DONE = "done"
    FAILED = "failed"


@dataclass
class Observation:
    id: str
    state: ObservationState
    data_dir: str

    def __post_init__(self):
        # Validate that the state is a valid ObservationState
        if not isinstance(self.state, ObservationState):
            raise ValueError(f"Invalid state: {self.state}. Must be an instance of ObservationState.")

        # Validate that the data_dir is a valid path
        if not Path(self.data_dir).exists():
            raise ValueError(f"Invalid data_dir: {self.data_dir}. Must be a valid path.")

        # Validate that the data_dir is a directory
        if not Path(self.data_dir).is_dir():
            raise ValueError(f"Invalid data_dir: {self.data_dir}. Must be a directory.")

    def update_state(self, new_state: ObservationState):
        if not isinstance(new_state, ObservationState):
            raise ValueError(f"Invalid new_state: {new_state}. Must be an instance of ObservationState.")
        self.state = new_state