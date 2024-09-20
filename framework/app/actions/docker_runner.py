# app/actions/docker_runner.py
import docker
import logging

client = docker.from_env()


def run_docker_image(docker_image: str):
    try:
        # First, check if the image is already available locally
        try:
            client.images.get(docker_image)
            logging.info(f"Image {docker_image} found locally.")
        except docker.errors.ImageNotFound:
            # Pull the Docker image if not found locally
            logging.info(f"Pulling Docker image: {docker_image}")
            client.images.pull(docker_image)

        container_name = f"{docker_image.replace(':', '_').replace('/', '_')}_container"

        # Run the Docker container
        logging.info(f"Running Docker container for image: {docker_image}")
        container = client.containers.run(docker_image, detach=True, name=container_name)

        # Wait for the container to finish
        logging.info(f"Waiting for container to finish for image: {docker_image}")
        container.wait()

        # Get logs from the container
        logs = container.logs().decode('utf-8')
        logging.info(f"Docker image {docker_image} completed with logs: {logs}")

        # Clean up the container
        # container.remove()

        return logs

    except docker.errors.DockerException as e:
        logging.error(f"Failed to run Docker image: {e}")
        return f"Failed to run Docker image: {e}"
