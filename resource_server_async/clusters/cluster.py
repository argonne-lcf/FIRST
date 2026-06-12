import ast
import importlib
import logging
from abc import ABC, abstractmethod
from typing import List, Self, Type

from cachetools import TTLCache
from django.forms.models import model_to_dict

from inference_gateway.settings import MAINTENANCE_ERROR_NOTICES
from resource_server_async.cache import get_item_from_cache_async

from ..auth import check_permission as auth_utils_check_permission
from ..errors import ClusterNotFound, Unauthorized
from ..models import Cluster
from ..schemas.clusters import (
    CheckMaintenanceResult,
    ClusterStatus,
    JobsByStatus,
)
from ..schemas.structured_logs import UserPydantic

log = logging.getLogger(__name__)

_adapter_cache: TTLCache[str, "BaseCluster"] = TTLCache(maxsize=64, ttl=60)


class BaseCluster(ABC):
    """Generic abstract base class that enforces a common set of methods for compute clusters."""

    # Class initialization
    def __init__(
        self,
        id: str,
        cluster_name: str,
        cluster_adapter: str,
        frameworks: List[str],
        openai_endpoints: List[str],
        allowed_globus_groups: List[str] = [],
        allowed_domains: List[str] = [],
    ):
        # Assign common self variables
        self.__id = id
        self.__cluster_name = cluster_name
        self.__cluster_adapter = cluster_adapter
        self.__frameworks = frameworks
        self.__openai_endpoints = openai_endpoints
        self.__allowed_globus_groups = allowed_globus_groups
        self.__allowed_domains = allowed_domains

    # Check maintenance
    async def check_maintenance(self) -> CheckMaintenanceResult:
        """Verify is the cluster is currently under maintenance."""

        # Check Redis cache for cluster status from ALCF facility API
        cache_key = f"cluster_status:{self.cluster_name}"
        cluster_status: ClusterStatus | None = await get_item_from_cache_async(
            cache_key
        )

        if not isinstance(cluster_status, dict):
            cluster_status = {"status": "unknown", "message": ""}

        if cluster_status.get("status") == "down":
            msg = cluster_status.get(
                "message", f"Cluster {self.cluster_name} is currently down."
            )
            return CheckMaintenanceResult(is_under_maintenance=True, message=msg)

        if cluster_status.get("status") == "error":
            log.warning(
                f"Cluster status check error for {self.cluster_name}: {cluster_status}"
            )

        if notice := MAINTENANCE_ERROR_NOTICES.get(self.cluster_name):
            return CheckMaintenanceResult(
                is_under_maintenance=True,
                message=notice,
            )

        return CheckMaintenanceResult(is_under_maintenance=False, message="")

    # Check permission
    def check_permission(self, auth: UserPydantic, *, raise_exc: bool = True) -> bool:
        """
        Verify is the user is permitted to access this endpoint.
        If raise_exc is True, raises Unauthorized.
        Otherwise, returns authorization status as boolean.
        """

        # Check permission
        try:
            auth_utils_check_permission(
                auth, self.allowed_globus_groups, self.allowed_domains
            )
        except Unauthorized:
            if raise_exc:
                raise
            return False

        return True

    # Mandatory definitions
    # ---------------------

    @abstractmethod
    async def get_jobs(self, auth: UserPydantic) -> JobsByStatus:
        """Provides a status of the cluster as a whole, including which models are running."""
        pass

    # Read-only properties
    # --------------------

    @property
    def id(self) -> str:
        return self.__id

    @property
    def cluster_name(self) -> str:
        return self.__cluster_name

    @property
    def cluster_adapter(self) -> str:
        return self.__cluster_adapter

    @property
    def frameworks(self) -> list[str]:
        return self.__frameworks

    @property
    def openai_endpoints(self) -> list[str]:
        return self.__openai_endpoints

    @property
    def allowed_globus_groups(self) -> list[str]:
        return self.__allowed_globus_groups

    @property
    def allowed_domains(self) -> list[str]:
        return self.__allowed_domains

    @classmethod
    async def load_adapter(cls, cluster_name: str) -> Self:
        """Extract the cluster from the database and return its underlying wrapper object."""
        if (adapter := _adapter_cache.get(cluster_name)) is not None and isinstance(
            adapter, cls
        ):
            return adapter

        try:
            db_cluster = await Cluster.objects.aget(cluster_name=cluster_name)
        except Cluster.DoesNotExist:
            raise ClusterNotFound(
                f"The requested cluster {cluster_name!r} does not exist."
            )

        # Convert the config field into a dictionary
        cluster_dictionary = model_to_dict(db_cluster)
        cluster_dictionary["config"] = ast.literal_eval(db_cluster.config)

        # Extract the adapter class from the cluster's database configuration
        parts = db_cluster.cluster_adapter.rsplit(".", 1)
        module = importlib.import_module(parts[0])
        AdapterClass: Type[BaseCluster] = getattr(module, parts[1])

        # Make sure the adaptor inherits from the BaseCluster generic class
        if not issubclass(AdapterClass, BaseCluster):
            raise AssertionError(
                f"Cluster adapter {db_cluster.cluster_adapter} should inherit from BaseCluster."
            )

        # Instantiate the adaptor class
        cluster_adapter = AdapterClass(**cluster_dictionary)
        if not isinstance(cluster_adapter, cls):
            raise AssertionError(
                f"Cannot load {db_cluster.cluster_adapter!r} from {cls.__name__}.load_adapter"
            )

        _adapter_cache[cluster_name] = cluster_adapter
        return cluster_adapter
