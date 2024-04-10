from typing import Sequence

import yaml
import re
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gke import GKEHook
from airflow.providers.google.cloud.hooks.kubernetes_engine import GKEPodHook
from airflow.providers.google.cloud.links.kubernetes_engine import KubernetesEngineClusterLink
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator
from airflow.utils.context import Context
from airflow.utils.decorators import cached_property
from airflow.providers.google.cloud.utils.gke_cluster_auth_details import GKEClusterAuthDetails


class GKEClusterAuthDetails:
    """
    Helper for fetching information about cluster for connecting.

    :param cluster_name: The name of the Google Kubernetes Engine cluster.
    :param project_id: The Google Developers Console project id.
    :param use_internal_ip: Use the internal IP address as the endpoint.
    :param cluster_hook: airflow hook for working with kubernetes cluster.
    """

    def __init__(
        self,
        cluster_name,
        project_id,
        use_internal_ip,
        cluster_hook,
    ):
        self.cluster_name = cluster_name
        self.project_id = project_id
        self.use_internal_ip = use_internal_ip
        self.cluster_hook = cluster_hook
        self._cluster_url = None
        self._ssl_ca_cert = None

    def fetch_cluster_info(self) -> tuple[str, str]:
        """Fetch cluster info for connecting to it."""
        cluster = self.cluster_hook.get_cluster(
            name=self.cluster_name,
            project_id=self.project_id,
        )

        if not self.use_internal_ip:
            self._cluster_url = f"https://{cluster.endpoint}"
        else:
            self._cluster_url = f"https://{cluster.private_cluster_config.private_endpoint}"
        self._ssl_ca_cert = cluster.master_auth.cluster_ca_certificate
        return self._cluster_url, self._ssl_ca_cert

class GKEDeployPodOperator(GoogleCloudBaseOperator):
    """
    Installs a pod manifest file inside a GKE cluster.

    :param project_id: The Google Developers Console [project ID or project number].
    :param location: The name of the Google Kubernetes Engine zone or region in which the cluster resides.
    :param cluster_name: The Cluster name in which to install the pod.
    :param manifest_path: The local file path of the pod manifest file to install.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "project_id",
        "location",
        "manifest_path",
        "cluster_name",
        "gcp_conn_id",
        "impersonation_chain",
    )
    operator_extra_links = (KubernetesEngineClusterLink(),)

    def __init__(
        self,
        *,
        location: str,
        cluster_name: str,
        manifest_path: str,
        use_internal_ip: bool = False,
        project_id: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.cluster_name = cluster_name
        self.manifest_path = manifest_path
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.use_internal_ip = use_internal_ip

    @cached_property
    def cluster_hook(self) -> GKEHook:
        return GKEHook(
            gcp_conn_id=self.gcp_conn_id,
            location=self.location,
            impersonation_chain=self.impersonation_chain,
        )

    @cached_property
    def pod_hook(self) -> GKEPodHook:
        if self._cluster_url is None or self._ssl_ca_cert is None:
            raise AttributeError(
                    "Cluster url and ssl_ca_cert should be defined before using self.hook method. "
                    "Try to use self.get_kube_creds method",
                )
        return GKEPodHook(
                gcp_conn_id=self.gcp_conn_id,
                impersonation_chain=self.impersonation_chain,
                cluster_url=self._cluster_url,
                ssl_ca_cert=self._ssl_ca_cert,
                enable_tcp_keepalive=True,
        )

    @staticmethod
    def _get_yaml_content_from_file(manifest_path) -> list[dict]:
        """Read content of local YAML file and separate it into several dictionaries."""
        yaml_dicts = []
        try:
            with open(manifest_path, 'r') as file:
                yaml_data = file.read()
                documents = re.split(r"---\n", yaml_data)

                for document in documents:
                    document_dict = yaml.safe_load(document)
                    if document_dict:
                        yaml_dicts.append(document_dict)
        except FileNotFoundError:
            raise AirflowException(f"Manifest file not found: {manifest_path}")
        except Exception as e:
            raise AirflowException(f"Error reading manifest file: {str(e)}")

        return yaml_dicts

    def execute(self, context: Context):
        self._cluster_url, self._ssl_ca_cert = GKEClusterAuthDetails(
            cluster_name=self.cluster_name,
            project_id=self.project_id,
            use_internal_ip=self.use_internal_ip,
            cluster_hook=self.cluster_hook,
        ).fetch_cluster_info()

        cluster = self.cluster_hook.get_cluster(
            name=self.cluster_name,
            project_id=self.project_id,
        )
        KubernetesEngineClusterLink.persist(context=context, task_instance=self, cluster=cluster)

        yaml_objects = self._get_yaml_content_from_file(manifest_path=self.manifest_path)

        try:
            self.pod_hook.apply_from_yaml_file(yaml_objects=yaml_objects)
            self.log.info("Pod manifest deployed successfully!")
        except Exception as e:
            self.log.error(f"Failed to deploy pod manifest: {str(e)}")
            raise
