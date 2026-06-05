import json
import uuid
from logging import getLogger
from typing import Any, Iterable, Self, override

from django.core.exceptions import ValidationError
from django.db import models
from django.db.models.base import ModelBase
from django.utils import timezone
from django.utils.text import slugify
from django.utils.timezone import now

from resource_server_async.schemas.batch import BatchStatus
from resource_server_async.schemas.endpoints import BatchStatusResult
from resource_server_async.schemas.structured_logs import (
    BatchLogPydantic,
)

from .logging import RequestContext
from .schemas.endpoints import SubmitBatchResult

logger = getLogger(__name__)


# Supported authentication origins
class AuthService(models.TextChoices):
    GLOBUS = "globus", "Globus"


# Function to validate that some inputs are list of strings
def validate_str_list(value: Any) -> None:
    if not isinstance(value, list):
        raise ValidationError("Value must be a list.")
    if not all(isinstance(v, str) for v in value):
        raise ValidationError("All items must be strings.")


# JSON field specifically containing a list of strings
class StrListJSONField(models.JSONField):
    def get_prep_value(self, value: Any) -> Any:
        validate_str_list(value)
        return super().get_prep_value(value)


# OpenAI endpoint list
class OpenAIEndpointListJSONField(models.JSONField):
    def get_prep_value(self, value: Any) -> Any:
        validate_str_list(value)
        if value:
            for endpoint in value:
                if endpoint[-1] == "/" or endpoint[0] == "/":
                    raise ValidationError(
                        "OpenAI endpoints cannot end or start with '/'."
                    )
        return super().get_prep_value(value)


# User model
class User(models.Model):
    """Details about a user who was authorized to access the service"""

    # User info
    id = models.CharField(max_length=100, primary_key=True)
    name = models.CharField(max_length=100)
    username = models.CharField(max_length=100)

    # Identity provider info
    idp_id = models.CharField(max_length=100, default="", blank=True)
    idp_name = models.CharField(max_length=100)

    # Where the user info is coming from (e.g. Globus, Slack)
    auth_service = models.CharField(
        max_length=100, choices=AuthService.choices, default=AuthService.GLOBUS.value
    )

    # Custom display
    def __str__(self) -> str:
        return f"<User - {self.name} - {self.username} - {self.idp_name}>"


# Access log model
class AccessLog(models.Model):
    # Unique access ID
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    # User details (link to the User model)
    user = models.ForeignKey(
        User,
        on_delete=models.PROTECT,
        related_name="access_logs",  # reverse relation (e.g. user.access_logs.all())
        db_index=True,
        null=True,  # For when an AccessLog is used to report un-authorized attempts
    )

    # Timestamps (request received and response returned)
    timestamp_request = models.DateTimeField(null=True, db_index=True)
    timestamp_response = models.DateTimeField(null=True)

    # Which API route was requested (e.g. /resource_server/list-endpoints)
    api_route = models.CharField(max_length=256, null=False, blank=False)

    # IP address from where the request is coming from
    origin_ip = models.CharField(max_length=250, null=False, blank=False)

    # HTTP status of the request
    status_code = models.IntegerField(null=False, db_index=True)

    # Error message if any
    error = models.TextField(null=True)

    # Globus Groups that were used to authorize the request
    # If None, it simply used the high-assurance policy
    authorized_groups = models.TextField(null=True)

    class Meta:
        indexes = [
            models.Index(
                fields=["user", "status_code"], name="idx_accesslog_user_status"
            ),  # Composite for joins
            models.Index(
                fields=["user_id"],
                name="idx_accesslog_user_id",
                condition=models.Q(user_id__isnull=False) & ~models.Q(user_id=""),
            ),  # For COUNT DISTINCT queries
        ]

    # Custom display
    def __str__(self) -> str:
        if self.user:
            username = self.user.username
        else:
            username = "Unauthorized"
        return f"<Access - {username} - {self.api_route} - {self.status_code}>"


# Request log model
class RequestLog(models.Model):
    # Unique request ID
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    # Link to the access log tied to this request
    access_log = models.OneToOneField(
        AccessLog,
        on_delete=models.PROTECT,
        related_name="request_log",  # reverse relation (e.g., access_log.request_log)
        db_index=True,
    )

    # Inference endpoint details
    cluster = models.CharField(max_length=100, null=False, blank=False)
    framework = models.CharField(max_length=100, null=False, blank=False)
    model = models.CharField(max_length=100, null=False, blank=False, db_index=True)
    openai_endpoint = models.CharField(max_length=100, null=False, blank=False)

    # Timestamps (before and after the remote computation)
    timestamp_compute_request = models.DateTimeField(null=False, blank=False)
    timestamp_compute_response = models.DateTimeField(null=False, blank=False)

    # Inference data
    prompt = models.TextField(null=True)
    result = models.TextField(null=True)

    # Compute task ID
    task_uuid = models.CharField(null=True, max_length=100)

    metrics_processed = models.BooleanField(default=False)

    class Meta:
        indexes = [
            models.Index(fields=["cluster", "framework"], name="idx_rlog_clstr_frmwrk"),
            models.Index(fields=["model"], name="idx_requestlog_model"),
            models.Index(
                fields=["access_log_id", "model"], name="idx_requestlog_access_model"
            ),  # Critical for dashboard joins
            models.Index(
                fields=["timestamp_compute_request"], name="idx_rlog_ts_compute_req"
            ),  # For time-range queries
        ]

    # Custom display
    def __str__(self) -> str:
        username = (
            self.access_log.user.username if self.access_log.user else "<anonymous>"
        )
        return (
            f"<Request - {username} - {self.cluster} - {self.framework} - {self.model}>"
        )


# Batch log model
class BatchLog(models.Model):
    # Unique request ID
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    access_log_id = models.CharField(max_length=100, editable=False)
    user_id = models.CharField(max_length=100)

    # What did the user request?
    input_file = models.CharField(max_length=500)
    output_folder_path = models.CharField(max_length=500, blank=True)
    cluster = models.CharField(max_length=100)
    framework = models.CharField(max_length=100)
    model = models.CharField(max_length=250)

    # List of Globus task UUIDs tied to the batch (string separated with ,)
    globus_batch_uuid = models.CharField(max_length=100, null=True)
    task_ids = models.TextField(null=True)
    result = models.TextField(blank=True)

    # What is the status of the batch?
    status = models.CharField(max_length=250, default="pending")
    in_progress_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(
        null=True, blank=True, db_index=True
    )  # For dashboard ORDER BY
    failed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        indexes = [
            models.Index(
                fields=["-completed_at", "-in_progress_at"],
                name="idx_batchlog_completion",
            ),  # Dashboard sorting
            models.Index(
                fields=["status"], name="idx_batchlog_status"
            ),  # Status filtering
        ]

    @classmethod
    async def create(
        cls,
        context: RequestContext,
        submit_response: SubmitBatchResult,
        cluster: str,
        framework: str,
        model: str,
    ) -> Self:
        obj = await cls.objects.acreate(
            id=submit_response.batch_id,
            access_log_id=context.access_log.id,
            user_id=context.user.id if context.user else "",
            input_file=submit_response.input_file,
            output_folder_path=submit_response.output_folder_path,
            cluster=cluster,
            framework=framework,
            model=model,
            task_ids=submit_response.task_ids,
            status=BatchStatus.pending,
            in_progress_at=timezone.now(),
        )

        batch_log = BatchLogPydantic.model_validate(obj)
        batch_log.emit("submitted-batch")
        return obj

    async def update(self, new_status: BatchStatusResult) -> None:
        status = new_status.status
        result = new_status.result

        # No status change:
        if self.status == status:
            return

        # Update status and result
        self.status = status

        # Adjust timestamp
        if self.status == BatchStatus.failed:
            self.failed_at = timezone.now()
        elif self.status == BatchStatus.completed:
            self.completed_at = timezone.now()

        if result:
            self.result = result

        await self.asave()
        batch_log = BatchLogPydantic.model_validate(self)
        batch_log.emit("updated")

        # Try to parse metrics summary from result if available
        if result:
            total_tokens = None
            num_responses = None
            response_time_sec = None
            throughput = None

            try:
                result_data: dict[str, Any] = json.loads(self.result)
                if "metrics" in result_data:
                    metrics: dict[str, Any] = result_data.get("metrics", {})
                    total_tokens = metrics.get("total_tokens")
                    num_responses = metrics.get("num_responses")
                    response_time_sec = metrics.get("response_time_sec")
                    throughput = metrics.get("throughput_tokens_per_sec")
            except Exception:
                pass
            else:
                batch_log.emit_metrics(
                    total_tokens=total_tokens,
                    num_responses=num_responses,
                    response_time_sec=response_time_sec,
                    throughput_tokens_per_sec=throughput,
                )


# Request metrics model (1:1 with RequestLog)
class RequestMetrics(models.Model):
    # Tie metrics to a single request (and reuse its UUID as primary key)
    request = models.OneToOneField(
        RequestLog,
        primary_key=True,
        on_delete=models.CASCADE,
        related_name="metrics",
        db_index=True,
    )

    # Duplicate key identifiers for faster filtering/aggregations
    cluster = models.CharField(max_length=100)
    framework = models.CharField(max_length=100)
    model = models.CharField(max_length=100)

    # Status code at time of response (from AccessLog)
    status_code = models.IntegerField(null=True)

    # Token usage
    prompt_tokens = models.BigIntegerField(null=True)
    completion_tokens = models.BigIntegerField(null=True)
    total_tokens = models.BigIntegerField(null=True)

    # Performance metrics
    response_time_sec = models.FloatField(null=True)
    throughput_tokens_per_sec = models.FloatField(null=True)

    # Copy timestamps for efficient window queries
    timestamp_compute_request = models.DateTimeField(null=True, db_index=True)
    timestamp_compute_response = models.DateTimeField(null=True)

    # Audit
    created_at = models.DateTimeField(default=now)

    class Meta:
        indexes = [
            models.Index(
                fields=["cluster", "framework"], name="idx_rmetrics_clstr_frmwrk"
            ),
            models.Index(fields=["model"], name="idx_requestmetrics_model"),
        ]

    def __str__(self) -> str:
        return f"<Metrics - {self.request_id}>"


# Batch metrics model (1:1 with BatchLog)
class BatchMetrics(models.Model):
    batch = models.OneToOneField(
        BatchLog,
        primary_key=True,
        on_delete=models.CASCADE,
        related_name="metrics",
        db_index=True,
    )

    cluster = models.CharField(max_length=100, db_index=True)
    framework = models.CharField(max_length=100, db_index=True)
    model = models.CharField(max_length=250, db_index=True)
    status = models.CharField(max_length=50, null=True, blank=True, db_index=True)

    total_tokens = models.BigIntegerField(null=True)
    num_responses = models.BigIntegerField(null=True)
    response_time_sec = models.FloatField(null=True)
    throughput_tokens_per_sec = models.FloatField(null=True)

    created_at = models.DateTimeField(default=now, db_index=True)
    completed_at = models.DateTimeField(null=True)

    # class Meta:
    #     indexes = [
    #         models.Index(fields=["model"]),
    #         models.Index(fields=["cluster", "framework"]),
    #     ]

    def __str__(self) -> str:
        return f"<BatchMetrics - {self.batch_id}>"


# Details of a given inference endpoint
class Endpoint(models.Model):
    # Slug for the endpoint
    # <cluster>-<framework>-<model> (all lower case)
    # Example: sophia-vllm-meta-llamameta-llama-3-70b-instruct
    endpoint_slug = models.SlugField(max_length=100, unique=True)

    # HPC machine the endpoint is running on (e.g. sophia)
    # TODO Foreign key here to point to cluster
    # TODO add endpoint uuid if GC, URL if Metis, etc...
    cluster = models.CharField(max_length=100)

    # Framework (e.g. vllm)
    framework = models.CharField(max_length=100)

    # Model name (e.g. cpp_meta-Llama3-8b-instruct)
    model = models.CharField(max_length=100)

    # Endpoint adapter (e.g. resource_server_async.endpoints.globus_compute.GlobusComputeEndpoint)
    endpoint_adapter = models.CharField(max_length=250)

    # Additional Globus group restrictions to access the endpoint (no restriction if empty)
    # Example: ["group1-uuid", "group2-uuid"]
    allowed_globus_groups = StrListJSONField(default=list, blank=True)

    # Additional domains restrictions to access the endpoint (no restriction if empty)
    # Example: ["anl.gov", "alcf.anl.gov"]
    allowed_domains = StrListJSONField(default=list, blank=True)

    # tokens/minute rate limit for the model (total usage by all users).
    # Set to 0 to disable.
    tpm_model = models.IntegerField(default=100_000)

    # tokens/minute rate limit for the model per-user.
    # Set to 0 to disable.
    tpm_user = models.IntegerField(default=60_000)

    # Extra configuration needed to instantiate the endpoint class
    # Should be json.dumps string. Will be converted into a python dictionaty within the endpoint object
    config = models.TextField(blank=True)

    # String function
    def __str__(self) -> str:
        return f"<Endpoint {self.endpoint_slug}>"

    # Automatically generate slug if not provided
    @override
    def save(
        self,
        *args: Any,
        force_insert: bool | tuple[ModelBase, ...] = False,
        force_update: bool = False,
        using: str | None = None,
        update_fields: Iterable[str] | None = None,
    ) -> None:
        if self.endpoint_slug is None or self.endpoint_slug == "":
            self.endpoint_slug = slugify(
                " ".join([self.cluster, self.framework, self.model])
            )
        super().save(
            *args,
            force_insert=force_insert,
            force_update=force_update,
            using=using,
            update_fields=update_fields,
        )


# Details of a given inference cluster
class Cluster(models.Model):
    # Cluster name
    cluster_name = models.CharField(max_length=100, unique=True)

    # Inference serving framework
    # e.g. ["vllm"]
    frameworks = StrListJSONField(null=False)

    # OpenAI endpoints
    # e.g. ["/v1/completions", "/v1/chat/completions"], cannot end with '/'
    openai_endpoints = OpenAIEndpointListJSONField(null=False)

    # Cluster adapter (e.g. resource_server_async.clusters.globus_compute.GlobusComputeCluster)
    cluster_adapter = models.CharField(max_length=250)

    # Additional Globus group restrictions to access the cluster (no restriction if empty)
    # Example: ["group1-uuid", "group2-uuid"]
    allowed_globus_groups = StrListJSONField(default=list, blank=True)

    # Additional domains restrictions to access the cluster (no restriction if empty)
    # Example: ["anl.gov", "alcf.anl.gov"]
    allowed_domains = StrListJSONField(default=list, blank=True)

    # Extra configuration needed to instantiate the cluster class
    # Should be json.dumps string. Will be converted into a python dictionaty within the cluster object
    config = models.TextField(blank=True)

    # String function
    def __str__(self) -> str:
        return f"<Cluster {self.cluster_name}>"
