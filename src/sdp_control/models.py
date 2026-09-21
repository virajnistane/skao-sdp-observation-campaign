#!/usr/bin/env python3
# Author: Viraj Nistane
# Description: This file contains the task to receive visibilities from the SDP pipeline
# and store them in a specified directory.

import datetime
from dataclasses import dataclass
from enum import StrEnum
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
    ms_dir: str
    datetime_stamp: datetime.datetime | str

    def __str__(self):
        return f"Observation(id={self.id}, state={self.state}, ms_path={self.ms_dir}, datetime_stamp={self.datetime_stamp})"

    def __repr__(self):
        return f"Observation(id={self.id}, state={self.state}, ms_path={self.ms_dir}, datetime_stamp={self.datetime_stamp})"

    # def __post_init__(self):
    #     # Validate that the state is a valid ObservationState
    #     if not isinstance(self.state, ObservationState):
    #         raise ValueError(f"Invalid state: {self.state}. Must be an instance of ObservationState.")
    #     # Validate that the ms_path is a valid path
    #     if not self.ms_path.exists():
    #         raise ValueError(f"Invalid ms_path: {self.ms_path}. Must be a valid path.")
    #     # Validate that the ms_path is a directory
    #     if not self.ms_path.is_dir():
    #         raise ValueError(f"Invalid ms_path: {self.ms_path}. Must be a directory.")

    def update_state(self, new_state: ObservationState):
        if not isinstance(new_state, ObservationState):
            raise ValueError(
                f"Invalid new_state: {new_state}. Must be an instance of ObservationState."
            )
        self.state = new_state
