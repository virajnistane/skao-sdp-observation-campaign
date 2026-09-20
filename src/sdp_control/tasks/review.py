#!/usr/bin/env python3
# Author: Viraj Nistane
# Description: This file contains the task to review the processed visibilities 
# and generate an interactive preview using Dash. 

from prefect.artifacts import create_link_artifact

create_link_artifact(
    key=f"dash-preview-{obs_id}",
    link="http://localhost:8050",
    link_text="Open interactive preview",
)


def pause_flow_run(flow_run_id: str) -> None:
    """Pause a flow run by setting its state to PAUSED."""
    from prefect.client import get_client
    from prefect.states import Paused

    client = get_client(sync_client=True)
    client.set_flow_run_state(flow_run_id, state=Paused())

def resume_flow_run(flow_run_id: str) -> None:
    """Resume a flow run by setting its state to RUNNING."""
    from prefect.client import get_client
    from prefect.states import Running

    client = get_client(sync_client=True)
    client.set_flow_run_state(flow_run_id, state=Running())