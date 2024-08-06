# Azure ACR Image Cleanup

ACR includes an [auto-purge solution](https://learn.microsoft.com/en-us/azure/container-registry/container-registry-auto-purge), but it has a critical drawback: the auto-purge has no way to know if an image is currently in use by a Kubernetes cluster. Auto-purging images of a certain date can lead to a situation where a sufficiently-aged container is cleaned up from ACR, and when your clusters need to restart a pod, the image no longer exists and results in image pull errors.

This script will interrogate a set of AKS clusters for running images, ensuring that any images currently being utilized are not cleaned up regardless of how old they may be.

Additionally, since a container registry may provide images for more than just Kubernetes, a mechanism to match the origin of the container to a cluster keeps the script from removing unrelated images. Only containers running in your Kubernetes clusters that match the container registry's URL are considered for cleanup.

By running this script, containers in ACR will be cleaned up if they meet the following criteria:

- Container image name is running in at least one of the configured Kubernetes clusters,
- Container image tag is not running in any of the configured Kubernetes clusters, and
- Container image tag was created \[a configurable number of days\] prior to the cleanup run.

## Configuration File Format

This script uses a JSON configuration file, demonstrated by [sample.config.json](./sample.config.json). Any number of Kubernetes clusters may be considered, and a single container registry is allowed. Kubernetes clusters and the container registry are not required to exist within the same subscription or resource group, allowing for greater flexibility when dealing with container registries that serve multiple subscriptions.

## Prerequisites

- Python >= 3.11
- Existing Azure authentication method

This script utilizes the default authentication chain for Azure; it will use stored credentials created by the run of `az login`, environment variables, etc. to determine its identity and authenticate with Azure. This allows the script to be run from a user's workstation utilizing their identity, and allows the script to be run from a pipeline context utilizing its Azure identity without code changes.

## Installation

### Bare installation

Clone the repository to the machine in question, then execute the following command to install the dependencies:

```sh
pip install -r requirements.txt
```

This installation method is suitable for use inside pipelines, dev containers, and other throwaway execution environments.

### Virtual Environment

Clone the repository to the machine in question, then execute the following commands to set up a virtual environment, enter the virtual environment, and then install dependencies:

```sh
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

This installation method is suitable for end user workstations and developers. Remember to `source` the activation script to reenter the virtual environment between reboots or when switching projects.