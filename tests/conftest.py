import os

# Tasks are invoked directly in unit tests, outside a flow run, so there's no
# flow run id for task logs to attach to; skip shipping them to the API.
os.environ.setdefault("PREFECT_LOGGING_TO_API_WHEN_MISSING_FLOW", "ignore")
