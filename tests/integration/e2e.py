#!/usr/bin/env python3
# Author: Viraj Nistane
# Description: This file contains the integration tests for the end-to-end flow of receiving, processing, and storing visibilities in the SDP pipeline.

import pytest

from sdp_control.tasks.receive_vis import receive_visibilities
from sdp_control.tasks.process_vis import process_visibilities
from sdp_control.tasks.storage import get_total_ms_size_mb




