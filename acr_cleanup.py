import sys
import os
import json
from pathlib import Path
from datetime import datetime, timedelta, UTC
from typing import Self

from azure.identity import DefaultAzureCredential, AzureAuthorityHosts
from azure.mgmt.containerservice import ContainerServiceClient
from azure.mgmt.containerservice.models import RunCommandRequest
from azure.containerregistry import ContainerRegistryClient

from pydantic import BaseModel, UUID4, Field, ConfigDict

if os.environ.get("ARM_ENVIRONMENT") == "usgovernment":
    authority = AzureAuthorityHosts.AZURE_GOVERNMENT
    resource_manager = "https://management.usgovcloudapi.net"
else:
    authority = AzureAuthorityHosts.AZURE_PUBLIC_CLOUD
    resource_manager = "https://management.azure.com"

CREDENTIAL = DefaultAzureCredential(authority=authority)
CONTAINER_DISCOVERY_COMMAND = '''kubectl get pod --all-namespaces -o jsonpath="{.items[*].spec['initContainers', 'containers'][*].image}"'''


class TaggedImage(BaseModel):
    model_config = ConfigDict(frozen=True)

    registry: str = Field(hash=True)
    image_name: str = Field(hash=True)
    image_tag: str = Field(hash=True)

    @classmethod
    def from_string(cls, payload: str) -> Self:
        split_tag = payload.split(":")
        split_slashes = split_tag[0].split("/")
        registry = "/".join(split_slashes[:-1])
        image_name = split_slashes[-1]
        image_tag = split_tag[1]
        return cls(registry=registry, image_name=image_name, image_tag=image_tag)

    def __eq__(self, other: Self):
        return (
            self.registry == other.registry
            and self.image_name == other.image_name
            and self.image_tag == other.image_tag
        )

    def __lt__(self, other: Self):
        return (
            self.registry == other.registry
            and self.image_name == other.image_name
            and self.image_tag < other.image_tag
        )

    def __gt__(self, other: Self):
        return (
            self.registry == other.registry
            and self.image_name == other.image_name
            and self.image_tag > other.image_tag
        )

    def __str__(self):
        return f"{self.registry}/{self.image_name}:{self.image_tag}"

    def __repr__(self):
        return str(self)


class KubernetesClusterConfiguration(BaseModel):
    name: str
    subscription_id: UUID4
    resource_group: str
    _client: ContainerServiceClient = None

    def get_running_images(self, prefix: str = None) -> set[TaggedImage]:
        if not self._client:
            self._client = ContainerServiceClient(
                credential=CREDENTIAL,
                base_url=resource_manager,
                credential_scopes=[resource_manager + "/.default"],
                subscription_id=self.subscription_id,
            )
        request = RunCommandRequest(command=CONTAINER_DISCOVERY_COMMAND)
        response = self._client.managed_clusters.begin_run_command(
            self.resource_group, self.name, request
        ).result()
        running_images = set(response.logs.split(" "))
        if prefix is not None:
            running_images = set(
                [
                    TaggedImage.from_string(r)
                    for r in running_images
                    if r.startswith(prefix)
                ]
            )
        return running_images


class ContainerRegistryTaggedImage(TaggedImage):
    created_on: datetime


class ContainerRegistryConfiguration(BaseModel):
    url: str
    subscription_id: UUID4
    resource_group: str
    _client: ContainerRegistryClient = None

    def get_stored_images(self) -> set[ContainerRegistryTaggedImage]:
        """Returns all the images stored within a container registry. Does not return the 'latest' tag.

        Returns:
            set[ContainerRegistryTaggedImage]: Images contained within the registry.
        """
        all_images = set()
        if not self._client:
            self._client = ContainerRegistryClient(
                endpoint=f"https://{self.url}", credential=CREDENTIAL
            )

        for repository_name in self._client.list_repository_names():
            for tag in self._client.list_tag_properties(repository=repository_name):
                if not tag.name == "latest":
                    tagged_image = ContainerRegistryTaggedImage(
                        registry=self.url,
                        image_name=repository_name,
                        image_tag=tag.name,
                        created_on=tag.created_on,
                    )
                    all_images.add(tagged_image)
        print(
            f"{len(all_images)} unique container image tags stored in the container registry."
        )
        return all_images

    def remove_image(self, image: ContainerRegistryTaggedImage):
        if not self._client:
            self._client = ContainerRegistryClient(
                endpoint=f"https://{self.url}", credential=CREDENTIAL
            )
        self._client.delete_manifest(
            repository=image.image_name, tag_or_digest=image.image_tag
        )


def load_configuration(
    config_file_path: Path,
) -> tuple[dict[str, KubernetesClusterConfiguration], ContainerRegistryConfiguration]:
    config_raw = json.loads(config_file_path.read_text())
    kubernetes_cluster_configs = {}

    for alias, config in config_raw.get("kubernetes_clusters", {}).items():
        kubernetes_cluster_configs[alias] = (
            KubernetesClusterConfiguration.model_validate(obj=config)
        )
    container_registry = ContainerRegistryConfiguration.model_validate(
        obj=config_raw["container_registry"]
    )

    return kubernetes_cluster_configs, container_registry


def get_all_running_images(
    kubernetes_clusters: list[KubernetesClusterConfiguration],
    registry_url: str
) -> set[TaggedImage]:
    all_running_images = set()
    for cluster_alias, cluster in kubernetes_clusters.items():
        print(
            f"Retrieving images running on cluster {cluster_alias} ({cluster.name})...",
            end="",
        )
        cluster_running_images = cluster.get_running_images(registry_url)
        print(f"found {len(cluster_running_images)} images.")
        all_running_images = all_running_images.union(cluster_running_images)
    print(
        f"Discovered {len(all_running_images)} unique container image tags running across {len(kubernetes_clusters)} clusters."
    )
    return all_running_images


def filter_inactive_images(
    registry_images: set[ContainerRegistryTaggedImage], running_images: set[TaggedImage]
) -> set[ContainerRegistryTaggedImage]:
    """Filters the supplied registry_images using running_images to return images that are not currently in use.

    If a registry_image's image_name is not found in the set of running images, it will be filtered as well,
    because we only care to see the images utilized by the Kubernetes clusters. We don't want to remove any
    images that aren't a part of the Kubernetes deployment.

    Args:
        registry_images (set[ContainerRegistryTaggedImage]): _description_
        running_images (set[TaggedImage]): _description_

    Returns:
        set[ContainerRegistryTaggedImage]: _description_
    """
    inactive_images = set()

    running_image_map = {i.image_name: set() for i in running_images}
    for running_image in running_images:
        if running_image.image_name in running_image_map:
            running_image_map[running_image.image_name].add(running_image.image_tag)

    for registry_image in registry_images:
        if registry_image.image_name in running_image_map:
            if (
                registry_image.image_tag
                not in running_image_map[registry_image.image_name]
            ):
                inactive_images.add(registry_image)

    return inactive_images


def filter_aged_images(
    registry_images: set[ContainerRegistryTaggedImage], min_age_days: int
) -> set[ContainerRegistryTaggedImage]:
    """Filters a set of registry_images based on min_age_days, the number of whole days the image must have existed in
    order for it to be considered aged and be returned.

    Args:
        registry_images (set[ContainerRegistryTaggedImage]): Set of images contained within a registry to be evaluated
        min_age_days (int): Minimum age in whole days that for the image to be contained in the output

    Returns:
        set[ContainerRegistryTaggedImage]: Images from the running_images input that are sufficiently aged.
    """

    aged_images = set()
    evaluation_time = datetime.now(UTC)
    for image in registry_images:
        age: timedelta = evaluation_time - image.created_on
        if int(age.total_seconds() // 86400) >= min_age_days:
            aged_images.add(image)
    return aged_images


def main(config_file: Path, min_age_days: int):
    try:
        kubernetes_clusters, container_registry = load_configuration(
            config_file_path=config_file
        )
    except Exception as e:
        raise RuntimeError("Failed to load configuration!") from e
    all_running_images = get_all_running_images(kubernetes_clusters=kubernetes_clusters, registry_url=container_registry.url)
    stored_images = container_registry.get_stored_images()
    inactive_images = filter_inactive_images(
        registry_images=stored_images, running_images=all_running_images
    )
    print(
        f"Filtered down to {len(inactive_images)} that have names utilized by Kubernetes and tags that are not currently utilized."
    )
    aged_images = filter_aged_images(inactive_images, min_age_days=min_age_days)
    print(
        f"Filtered down to {len(aged_images)} that are sufficiently aged for cleanup."
    )

    for aged_image in aged_images:
        print(f"Removing image {aged_image}...")
        container_registry.remove_image(aged_image)

    print(f"Cleanup complete, {len(aged_images)} deleted.")


def usage():
    print(
        """
acr_cleanup.py 
    Performs a cleanup of ACR images based on what images are currently 
    running on your Kubernetes cluster(s). In order to be considered for 
    cleanup, images within the repository must:

    - Be an image_name that is utilized by a Kubernetes cluster
    - Be a tag that is not utilized by a Kubernetes cluster
    - Be at least [age] days old
          
Usage:
    acr_cleanup.py <config_file> [age]
          
Arguments:
    config_file
        Path to a configuration file describing the Kubernetes clusters and 
        container registry. Many Kubernetes clusters are supported for a 
        single run, but only one container registry may be considered at a 
        time. 
    
    age (Optional)
        Integer number of whole days required for the image to have existed 
        before cleanup is possible. All time handling is conducted in UTC 
        and a whole day consists of 86,400 secoonds starting when the image
        was created. Defaults to 7.
"""
    )


if __name__ == "__main__":
    if len(sys.argv) == 2:
        config_file = Path(sys.argv[1])
        age_days = 7
    elif len(sys.argv) == 3:
        config_file = Path(sys.argv[1])
        age_days = int(sys.argv[2])
    else:
        usage()
        exit(-1)

    try:
        main(config_file=config_file, min_age_days=age_days)
    except Exception as e:
        print(f"Failure: {e}")
        exit(-2)
