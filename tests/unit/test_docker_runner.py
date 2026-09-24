import pytest

from sdp_control.utils.docker_runner import run_container


def test_run_container(tmp_path):
    # Create a temporary directory to simulate volume mounting
    host_volume = tmp_path / "host_volume"
    host_volume.mkdir()

    # Define the Docker image and command
    image = "alpine:latest"
    command = "sh -c 'echo Hello, World! > /container_volume/output.txt'"

    # Define the volume mapping
    volumes = {str(host_volume): "/container_volume"}

    # Run the Docker container
    try:
        run_container(image, command, volumes)
    except RuntimeError as e:
        pytest.fail(f"run_container raised RuntimeError unexpectedly: {e}")

    # Check if the container ran successfully by checking the output file
    output_file = host_volume / "output.txt"
    assert output_file.exists(), "Output file was not created in the host volume."
