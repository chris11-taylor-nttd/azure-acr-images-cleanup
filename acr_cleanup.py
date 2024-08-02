import os
import json
from pathlib import Path
from datetime import datetime
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
        return self.registry == other.registry and self.image_name == other.image_name and self.image_tag == other.image_tag

    def __lt__(self, other: Self):
         return self.registry == other.registry and self.image_name == other.image_name and self.image_tag < other.image_tag
    
    def __gt__(self, other: Self):
         return self.registry == other.registry and self.image_name == other.image_name and self.image_tag > other.image_tag

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
        client = ContainerServiceClient(
            credential=CREDENTIAL,
            base_url=resource_manager,
            credential_scopes=[resource_manager + "/.default"],
            subscription_id=self.subscription_id
        )
        request = RunCommandRequest(command=CONTAINER_DISCOVERY_COMMAND)
        response = client.managed_clusters.begin_run_command(self.resource_group, self.name, request).result()
        running_images = set(response.logs.split(" "))
        if prefix is not None:
            running_images = set([TaggedImage.from_string(r) for r in running_images if r.startswith(prefix)])
        return running_images

class ContainerRegistryTaggedImage(TaggedImage):
    model_config = ConfigDict(frozen=True)
    created_on: datetime


class ContainerRegistryConfiguration(BaseModel):
    url: str
    subscription_id: UUID4
    resource_group: str
    
    def get_stored_images(self) -> set[ContainerRegistryTaggedImage]:
        """Returns all the images stored within a container registry. Does not return the 'latest' tag.

        Returns:
            set[ContainerRegistryTaggedImage]: Images contained within the registry.
        """
        all_images = set()
        registry_client = ContainerRegistryClient(endpoint=f"https://{self.url}", credential=CREDENTIAL)
        for repository_name in registry_client.list_repository_names():
            for tag in registry_client.list_tag_properties(repository=repository_name):
                if not tag.name == 'latest':
                    tagged_image = ContainerRegistryTaggedImage(registry=self.url, image_name=repository_name, image_tag=tag.name, created_on=tag.created_on)
                    all_images.add(tagged_image)
        print(f"{len(all_images)} unique container image tags stored in the container registry.")
        return all_images

def load_configuration(config_file_path: Path = Path("cleanup_config.json")) -> tuple[dict[str, KubernetesClusterConfiguration], dict[str, ContainerRegistryConfiguration]]:
    config_raw = json.loads(config_file_path.read_text())
    kubernetes_cluster_configs = {}
    
    for alias, config in config_raw.get("kubernetes_clusters", {}).items():
        kubernetes_cluster_configs[alias] = KubernetesClusterConfiguration.model_validate(obj=config)
    container_registry = ContainerRegistryConfiguration.model_validate(obj=config_raw["container_registry"])
    
    return kubernetes_cluster_configs, container_registry

def get_all_running_images(kubernetes_clusters: list[KubernetesClusterConfiguration]) -> set[TaggedImage]:
    all_running_images = set()
    for cluster_alias, cluster in kubernetes_clusters.items():
        print(f"Retrieving images running on cluster {cluster_alias} ({cluster.name})")
        cluster_running_images = cluster.get_running_images("trpnonprodcr")
        all_running_images = all_running_images.union(cluster_running_images)
    print(f"Discovered {len(all_running_images)} unique container image tags running across {len(kubernetes_clusters)} clusters.")
    return all_running_images


def filter_active_images(registry_images: set[ContainerRegistryTaggedImage], running_images: set[TaggedImage]) -> set[ContainerRegistryTaggedImage]:
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

    registry_images_map = {}

    for registry_image in registry_images:
        if not registry_image.image_name in registry_images_map:
            registry_images_map[registry_image.image_name] = set()
        registry_images_map[registry_image.image_name].add(registry_image.image_tag)
    
    breakpoint()
    

def main():
    kubernetes_clusters, container_registry = load_configuration()
    
    all_running_images = get_all_running_images(kubernetes_clusters=kubernetes_clusters)
    stored_images = container_registry.get_stored_images()
    filter_active_images(registry_images=stored_images, running_images=all_running_images)


def usage():
    print(
        """
acr_cleanup.py 
    Performs a cleanup of ACR images based on what images are currently running on your Kubernetes cluster(s).


          
Usage:
    acr_cleanup.py <config_file>
          
Arguments:
    config_file
        Path to a configuration file describing the 
    
          

"""
    )


if __name__ == "__main__":
    # if len(sys.argv) == 3:
    #     strategy = None
    # elif len(sys.argv) == 4:
    #     strategy = sys.argv[3]
    # else:
    #     usage()
    #     exit(-1)


    try:
        main()
    except Exception as e:
        print(f"Failure: {e}")
        exit(-2)
