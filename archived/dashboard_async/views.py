import logging
import secrets
import time
from datetime import timedelta
from functools import wraps
from urllib.parse import urlencode

from asgiref.sync import sync_to_async
from django.contrib import messages
from django.core.cache import cache
from django.db import connection
from django.db.models import (
    Count,
    F,
    FilteredRelation,
    Max,
    Q,
    Sum,
)
from django.http import HttpRequest, JsonResponse
from django.shortcuts import redirect, render
from django.utils import timezone
from ninja import NinjaAPI, Router
from ninja.security import SessionAuth
from resource_server_async.clusters import BaseCluster
from resource_server_async.models import (
    AccessLog as AsyncAccessLog,
)
from resource_server_async.models import (
    BatchLog as AsyncBatchLog,
)
from resource_server_async.models import (
    BatchMetrics as AsyncBatchMetrics,
)
from resource_server_async.models import (
    Endpoint as AsyncEndpoint,
)
from resource_server_async.models import (
    RequestLog as AsyncRequestLog,
)
from resource_server_async.models import (
    RequestMetrics as AsyncRequestMetrics,
)
from resource_server_async.models import (
    User as AsyncUser,
)
from resource_server_async.schemas.clusters import JobsByStatus

from dashboard_async.globus_auth import (
    exchange_code_for_tokens,
    get_authorization_url,
    refresh_access_token,
    revoke_token,
    validate_dashboard_token,
)

log = logging.getLogger(__name__)


# Custom authentication for Django Ninja that uses Globus session authentication
class DjangoSessionAuth(SessionAuth):
    """Use Globus session authentication for API endpoints."""

    def authenticate(self, request: HttpRequest, key):
        # Check for Globus tokens in session
        if "globus_tokens" not in request.session:
            return None

        try:
            tokens = request.session["globus_tokens"]
            auth_tokens = tokens.get("auth.globus.org", {})
            access_token = auth_tokens.get("access_token")
            expires_at = auth_tokens.get("expires_at_seconds", 0)

            # Check if token is still valid (not expired)
            if time.time() >= expires_at:
                return None

            # Validate token with groups check
            groups_token = tokens.get("groups.api.globus.org", {}).get("access_token")
            is_valid, user_data, error = validate_dashboard_token(
                access_token, groups_token
            )

            if is_valid:
                # Return user data for use in API views
                return user_data

        except Exception as e:
            log.warning(f"API auth error: {e}")

        return None


# Create Ninja API with session authentication
api = NinjaAPI(urls_namespace="dashboard_api", auth=DjangoSessionAuth())
router = Router()

api.add_router("/", router)


# ========================= Authentication Views =========================


def dashboard_login_view(request):
    """Initiate Globus OAuth2 login flow."""
    # Check if already authenticated via Globus
    if "globus_tokens" in request.session:
        tokens = request.session["globus_tokens"]
        access_token = tokens["auth.globus.org"]["access_token"]
        groups_token = tokens.get("groups.api.globus.org", {}).get("access_token")
        is_valid, user_data, error = validate_dashboard_token(
            access_token, groups_token
        )

        if is_valid:
            return redirect("dashboard_analytics")
        else:
            # Clear invalid session to prevent loops
            request.session.flush()

    # Clear any stale OAuth state from previous failed attempts
    # This prevents reusing OAuth state after errors
    request.session.pop("oauth_state", None)
    request.session.pop("next_url", None)

    # Generate new state for CSRF protection
    state = secrets.token_urlsafe(32)
    request.session["oauth_state"] = state
    request.session["next_url"] = request.GET.get("next", "dashboard_analytics")

    # Force session save before redirect
    request.session.modified = True
    request.session.save()

    # Get Globus authorization URL
    auth_url = get_authorization_url(state=state)

    return redirect(auth_url)


def dashboard_callback_view(request):
    """Handle Globus OAuth2 callback."""

    # Check for errors from Globus
    error = request.GET.get("error")
    if error:
        error_description = request.GET.get("error_description", error)
        log.error(f"Globus OAuth error: {error} - {error_description}")

        request.session.pop("oauth_state", None)
        request.session.pop("next_url", None)

        if error == "unauthorized_client":
            messages.error(
                request,
                f"OAuth Configuration Error: The redirect URI is not registered with Globus. "
                f"Error: {error_description}",
            )
        else:
            messages.error(request, f"Authentication failed: {error_description}")

        return render(request, "login.html", {"form": None})

    # Verify CSRF state
    state = request.GET.get("state")
    saved_state = request.session.get("oauth_state")

    if not state or state != saved_state:
        log.warning("CSRF state mismatch")
        messages.error(request, "Invalid authentication state. Please try again.")
        request.session.pop("oauth_state", None)
        return render(request, "login.html", {"form": None})

    # Exchange authorization code for tokens
    auth_code = request.GET.get("code")
    if not auth_code:
        log.error("No authorization code in callback")
        messages.error(request, "No authorization code received from Globus.")
        return render(request, "login.html", {"form": None})

    try:
        # Get tokens from Globus
        tokens = exchange_code_for_tokens(auth_code)
        request.session["globus_tokens"] = tokens

        # Validate token and get user info
        access_token = tokens["auth.globus.org"]["access_token"]
        groups_token = tokens.get("groups.api.globus.org", {}).get("access_token")
        is_valid, user_data, error = validate_dashboard_token(
            access_token, groups_token
        )

        if not is_valid:
            log.error(f"Token validation failed: {error}")

            # Clear all OAuth-related session data
            request.session.pop("globus_tokens", None)
            request.session.pop("oauth_state", None)
            request.session.pop("next_url", None)

            # Render error page directly (don't redirect to avoid loop)
            messages.error(request, error)
            return render(request, "login.html", {"form": None})

        # Store user info in session
        request.session["globus_user"] = {
            "id": user_data.id,
            "name": user_data.name,
            "username": user_data.username,
            "idp_id": user_data.idp_id,
            "idp_name": user_data.idp_name,
        }

        request.session.modified = True
        request.session.save()

        log.info(f"Dashboard login successful: {user_data.name} ({user_data.username})")

        # Redirect to original destination
        next_url = request.session.pop("next_url", "dashboard_analytics")
        request.session.pop("oauth_state", None)

        return redirect(next_url)

    except Exception as e:
        log.error(f"OAuth callback error: {e}")
        messages.error(request, f"Authentication error: {str(e)}")
        return redirect("dashboard_login")


def dashboard_logout_view(request):
    """Logout and clear both local and Globus sessions."""

    # Revoke Globus tokens if present
    if "globus_tokens" in request.session:
        try:
            access_token = request.session["globus_tokens"]["auth.globus.org"][
                "access_token"
            ]
            revoke_token(access_token)
        except Exception as e:
            log.warning(f"Token revocation error during logout: {e}")

    # Clear local session
    request.session.flush()

    # Build Globus logout URL with redirect back to login
    # This ensures the Globus session is also cleared
    logout_redirect = request.build_absolute_uri("/dashboard/login")
    globus_logout_url = f"https://auth.globus.org/v2/web/logout?{urlencode({'redirect_uri': logout_redirect})}"

    log.info("Logging out and clearing Globus session")

    # Redirect to Globus logout, which will then redirect back to our login
    return redirect(globus_logout_url)


# Password change views removed - Globus manages authentication


# ========================= Globus Authentication Decorator =========================


def globus_login_required(view_func):
    """
    Decorator to require Globus authentication for dashboard views.
    Validates Globus token from session and refreshes if needed.
    """

    @wraps(view_func)
    def wrapped_view(request, *args, **kwargs):
        # Check if Globus tokens exist in session
        if "globus_tokens" not in request.session:
            messages.warning(request, "Please log in to access the dashboard.")
            return redirect(f"/dashboard/login?next={request.path}")

        try:
            tokens = request.session["globus_tokens"]
            auth_tokens = tokens.get("auth.globus.org", {})
            access_token = auth_tokens.get("access_token")
            expires_at = auth_tokens.get("expires_at_seconds", 0)

            # Check if token is expired or about to expire (within 5 minutes)
            if time.time() >= (expires_at - 300):
                # Try to refresh token
                refresh_token = auth_tokens.get("refresh_token")
                if refresh_token:
                    try:
                        new_tokens = refresh_access_token(refresh_token)
                        # Update session with new tokens
                        request.session["globus_tokens"]["auth.globus.org"].update(
                            new_tokens
                        )
                        access_token = new_tokens["access_token"]
                    except Exception as e:
                        log.warning(f"Token refresh failed: {e}")
                        messages.error(
                            request, "Your session has expired. Please log in again."
                        )
                        return redirect("dashboard_login")
                else:
                    messages.error(
                        request, "Your session has expired. Please log in again."
                    )
                    return redirect("dashboard_login")

            # Validate token
            groups_token = tokens.get("groups.api.globus.org", {}).get("access_token")
            is_valid, user_data, error = validate_dashboard_token(
                access_token, groups_token
            )

            if not is_valid:
                log.warning(f"Token validation failed: {error}")
                messages.error(request, f"Authentication failed: {error}")
                # Clear invalid session
                request.session.flush()
                return redirect("dashboard_login")

            # Store user info in request for use in view
            request.globus_user = user_data

            return view_func(request, *args, **kwargs)

        except Exception as e:
            log.error(f"Authentication error: {e}")
            messages.error(request, "Authentication error. Please log in again.")
            request.session.flush()
            return redirect("dashboard_login")

    return wrapped_view


# ========================= New Realtime Dashboard (Async tables, no MVs) =========================


@globus_login_required
def analytics_realtime_view(request):
    """Main dashboard view - regular Django view (not API endpoint)."""
    # Access Globus user info via request.globus_user
    context = {"user": request.globus_user}
    return render(request, "realtime.html", context)


@router.get("/analytics/metrics")
async def get_realtime_metrics(request, cluster: str = "all"):
    """Overall realtime metrics from RequestMetrics (no window)."""
    try:
        # Check cache first (include cluster in cache key)
        cache_key = f"dashboard:realtime_metrics:{cluster}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        # Refined breakdown of requests using HTTP status codes:
        # - Successful: status_code 200-299 or 0 (all successful requests)
        # - Failed Auth: status_code = 401 OR 403 OR (status_code >= 300 AND no RequestLog)
        #   - 401 = Unauthorized (authentication failed)
        #   - 403 = Forbidden (authorization failed)
        #   - No RequestLog with error status = failed before reaching inference (likely auth/validation)
        # - Failed Inference: Has RequestLog AND status_code >= 300 AND status_code NOT IN (401, 403)
        #   - These reached inference but failed during processing
        access_log_set = AsyncAccessLog.objects
        request_metrics_set = AsyncRequestMetrics.objects
        unique_users_set = AsyncUser.objects
        if cluster and cluster.lower() != "all":
            access_log_set = access_log_set.select_related("request_log").filter(
                request_log__cluster__iexact=cluster
            )
            request_metrics_set = request_metrics_set.filter(cluster__iexact=cluster)
            unique_users_set = unique_users_set.select_related(
                "access_logs__request_log"
            ).filter(access_logs__request_log__cluster__iexact=cluster)

        request_counts = await access_log_set.aaggregate(
            all=Count("id"),
            successful=Count(
                "id",
                filter=Q(status_code__exact=0) | Q(status_code__range=(200, 299)),
            ),
            auth_failures=Count(
                "id",
                filter=~Q(status_code__in=(401, 403))
                | Q(request_log__isnull=True) & Q(status_code__gte=300),
            ),
            failed_inference=Count(
                "id",
                filter=Q(request_log__isnull=False)
                & Q(status_code__gte=300)
                & ~Q(status_code__in=(401, 403)),
            ),
        )
        metrics_counts = await request_metrics_set.aaggregate(
            total_tokens=Sum("total_tokens")
        )

        # Success rate calculation:
        # - Numerator: Successful requests (AccessLog with 200-299)
        # - Denominator: All requests that reached inference (successful + failed_inference)
        # - Excludes: Auth failures (never reached inference)
        total_inference_requests = (
            request_counts["successful"] + request_counts["failed_inference"]
        )

        # Success rate based on real request/response (not auth failures)
        success_rate = (
            request_counts["successful"] / total_inference_requests
            if total_inference_requests > 0
            else 0
        )

        # Unique users: count users who have requests in this cluster
        try:
            unique_users = await unique_users_set.distinct().acount()
        except Exception:
            # Fallback to 0 on any ORM error
            unique_users = 0

        # Per-model aggregates directly from RequestMetrics
        per_model_counts = [
            c
            async for c in request_metrics_set.values("model")
            .annotate(
                total_requests=Count("request"),
                successful=Count(
                    "request",
                    filter=Q(status_code__exact=0) | Q(status_code__range=(200, 299)),
                ),
                failed=Count(
                    "request",
                    filter=Q(status_code__isnull=True) | Q(status_code__gte=300),
                ),
                total_tokens=Sum("total_tokens"),
            )
            .order_by("-total_requests")
        ]

        result = {
            "totals": {
                "total_tokens": int(metrics_counts["total_tokens"] or 0),
                "total_requests": int(request_counts["all"] or 0),
                "total_inference_requests": int(total_inference_requests or 0),
                "successful": int(request_counts["successful"] or 0),
                "failed": int(request_counts["failed_inference"] or 0),
                "auth_failures": int(request_counts["auth_failures"] or 0),
                "success_rate": success_rate,
                "unique_users": int(unique_users or 0),
            },
            "per_model": per_model_counts,
            "time_bounds": None,
        }

        cache.set(cache_key, result, timeout=300)
        return result
    except Exception as e:
        log.error(f"Error fetching realtime metrics: {e}")
        return JsonResponse({"error": str(e)}, status=500)


@router.get("/analytics/logs")
async def get_realtime_logs(
    request, page: int = 0, per_page: int = 500, cluster: str = "all"
):
    """Latest AccessLog with optional joined RequestLog and User (LEFT JOIN semantics)."""
    try:
        start_index = page * per_page
        end_index = start_index + per_page

        # OPTIMIZED: LEFT JOIN semantics with metrics for pre-calculated latency
        # Utilize DB indexes: order by indexed timestamp_request desc, then status_code
        qs = AsyncAccessLog.objects.select_related(
            "user", "request_log", "request_log__metrics"
        ).only(  # Add metrics  # only pull these fields, defer everything else
            "id",
            "timestamp_request",
            "status_code",
            "api_route",
            "error",
            "user__id",
            "user__name",
            "user__username",
            "user__idp_id",
            "user__idp_name",
            "user__auth_service",
            "request_log__id",
            "request_log__cluster",
            "request_log__model",
            "request_log__openai_endpoint",
            "request_log__timestamp_compute_request",
            "request_log__timestamp_compute_response",
            "request_log__task_uuid",
            "request_log__metrics__response_time_sec",  # Pre-calculated latency
            "request_log__prompt",  # Expensive fields are lazy-loaded
            "request_log__result",
        )

        # Filter by cluster if specified
        if cluster and cluster.lower() != "all":
            qs = qs.filter(request_log__cluster=cluster.lower())

        qs = qs.order_by("-timestamp_request", "-status_code")

        sliced = qs[start_index:end_index]

        results = []
        async for al in sliced:
            rl = getattr(al, "request_log", None)
            user = getattr(al, "user", None)

            # OPTIMIZED: Use pre-calculated latency from RequestMetrics with fallback
            latency_secs = None
            if rl:
                # Try to get pre-calculated value first
                try:
                    metrics = getattr(rl, "metrics", None)
                    if metrics and metrics.response_time_sec is not None:
                        latency_secs = metrics.response_time_sec
                except Exception:
                    pass

                # Fallback: Calculate if metrics not available (e.g., not processed yet)
                if (
                    latency_secs is None
                    and rl.timestamp_compute_response
                    and rl.timestamp_compute_request
                ):
                    latency_secs = (
                        rl.timestamp_compute_response - rl.timestamp_compute_request
                    ).total_seconds()

            # Truncated prompt and result from request_log when available
            def _truncate(val: str, limit: int = 500) -> str:
                if not val:
                    return ""
                try:
                    text = str(val)
                except Exception:
                    text = ""
                if len(text) > limit:
                    return text[:limit] + "…"
                return text

            results.append(
                {
                    "request_id": str(rl.id) if rl else None,
                    "cluster": rl.cluster if rl else None,
                    "model": rl.model if rl else None,
                    "openai_endpoint": rl.openai_endpoint if rl else None,
                    "timestamp_request": al.timestamp_request.isoformat()
                    if al and al.timestamp_request
                    else None,
                    "latency_seconds": latency_secs,
                    "task_uuid": rl.task_uuid if rl else None,
                    "accesslog_id": str(al.id),
                    "status_code": al.status_code,
                    "error_message": al.error,
                    "error_snippet": _truncate(al.error) if al and al.error else "",
                    "api_route": al.api_route,
                    "prompt_snippet": _truncate(getattr(rl, "prompt", "")),
                    "result_snippet": _truncate(getattr(rl, "result", "")),
                    "user_id": str(user.id) if user else None,
                    "user_name": user.name if user else None,
                    "user_username": user.username if user else None,
                    "idp_id": user.idp_id if user else None,
                    "idp_name": user.idp_name if user else None,
                    "auth_service": user.auth_service if user else None,
                }
            )

        return results
    except Exception as e:
        log.error(f"Error fetching realtime logs: {e}")
        return JsonResponse({"error": str(e)}, status=500)


# ===== Additional realtime endpoints from RequestMetrics =====


def _parse_series_window(window: str):
    """Map UI window to a time delta and Postgres date_trunc unit.
    Supported: 1h->minute, 1d->hour, 1w->day, 1m->week, 1y->month.
    """
    window = (window or "1d").strip().lower()
    if window == "1h":
        return timedelta(hours=1), "minute"
    if window in ("1d", "24h"):
        return timedelta(days=1), "hour"
    if window in ("1w", "7d"):
        return timedelta(days=7), "day"
    if window in ("1m", "30d"):
        return timedelta(days=30), "week"
    if window in ("1y", "12m"):
        return timedelta(days=365), "month"
    if window in ("3y", "36m"):
        return timedelta(days=365 * 3), "month"
    # default
    return timedelta(days=1), "hour"


@router.get("/analytics/users-per-model")
async def get_users_per_model(request, cluster: str = "all"):
    """Get unique users per model with caching to reduce DB load."""
    try:
        cache_key = f"dashboard:users_per_model:{cluster}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        request_log_set = (
            AsyncRequestLog.objects.select_related("access_log")
            .annotate(
                filtered_log=FilteredRelation(
                    "access_log",
                    condition=Q(access_log__user__isnull=False)
                    & ~Q(access_log__user__exact=""),
                ),
            )
            .values("model")
            .annotate(
                user_count=Count("filtered_log__user", distinct=True),
            )
            .order_by("-user_count")
        )

        if cluster and cluster.lower() != "all":
            request_log_set = request_log_set.filter(Q(cluster__iexact=cluster))

        result = [r async for r in request_log_set]

        cache.set(cache_key, result, timeout=300)
        return result
    except Exception as e:
        log.error(f"Error fetching users per model: {e}")
        return JsonResponse({"error": str(e)}, status=500)


@router.get("/analytics/users-table")
async def get_users_table(request, cluster: str = "all"):
    """Tabular list of users with last access, success/failure counts, success%, last failure time."""
    try:
        cache_key = f"dashboard:users_table:{cluster}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        users_table_set = AsyncUser.objects.select_related("access_logs")
        if cluster and cluster.lower() != "all":
            users_table_set = users_table_set.select_related(
                "access_logs__request_log"
            ).filter(
                Q(access_logs__request_log__cluster__iexact=cluster)
                | Q(access_logs__request_log__cluster__isnull=True)
            )

        users_table_set = (
            users_table_set.values("name", "username")
            .annotate(
                last_access=Max("access_logs__timestamp_request"),
                successful=Count(
                    "access_logs",
                    filter=Q(access_logs__status_code__exact=0)
                    | Q(access_logs__status_code__range=(200, 299)),
                ),
                failed=Count(
                    "access_logs",
                    filter=Q(access_logs__status_code__isnull=True)
                    | Q(access_logs__status_code__gte=300),
                ),
                last_failure=Max(
                    "access_logs__timestamp_request",
                    filter=Q(access_logs__status_code__isnull=True)
                    | Q(access_logs__status_code__gte=300),
                ),
            )
            .order_by(F("last_access").desc(nulls_last=True), "username")
        )

        results = []
        async for r in users_table_set:
            total = int((r["successful"] or 0)) + int((r["failed"] or 0))
            success_rate = (float(r["successful"]) / total) if total > 0 else 0.0
            results.append(
                {
                    "name": r["name"],
                    "username": r["username"],
                    "last_access": r["last_access"].isoformat()
                    if r["last_access"]
                    else None,
                    "successful": int(r["successful"] or 0),
                    "failed": int(r["failed"] or 0),
                    "success_rate": success_rate,
                    "last_failure": r["last_failure"].isoformat()
                    if r["last_failure"]
                    else None,
                }
            )

        cache.set(cache_key, results, timeout=300)
        return results
    except Exception as e:
        log.error(f"Error fetching users table: {e}")
        return JsonResponse({"error": str(e)}, status=500)


@router.get("/analytics/series")
async def get_overall_series(request, window: str = "24h", cluster: str = "all"):
    try:
        delta, trunc_unit = _parse_series_window(window)
        end_ts = timezone.now()
        start_ts = end_ts - delta

        # Build cluster filter condition
        cluster_join = ""
        cluster_filter = ""
        cluster_params = []
        if cluster and cluster.lower() != "all":
            cluster_join = (
                "JOIN resource_server_async_requestlog rl ON rl.access_log_id = al.id"
            )
            cluster_filter = "AND rl.cluster = %s"
            cluster_params = [cluster.lower()]

        @sync_to_async
        def _get_rows():
            with connection.cursor() as cursor:
                if cluster and cluster.lower() != "all":
                    cursor.execute(
                        f"""
                        WITH series AS (
                          SELECT generate_series(
                            date_trunc(%s, %s::timestamptz),
                            date_trunc(%s, %s::timestamptz),
                            CASE %s
                              WHEN 'minute' THEN interval '1 minute'
                              WHEN 'hour' THEN interval '1 hour'
                              WHEN 'day' THEN interval '1 day'
                              WHEN 'week' THEN interval '1 week'
                              WHEN 'month' THEN interval '1 month'
                            END
                          ) AS bucket
                        )
                        SELECT s.bucket,
                               COALESCE(a.ok, 0) AS ok,
                               COALESCE(a.fail, 0) AS fail
                        FROM series s
                        LEFT JOIN (
                          SELECT date_trunc(%s, al.timestamp_request) AS bucket,
                                 COUNT(*) FILTER (WHERE al.status_code=0 OR al.status_code BETWEEN 200 AND 299) AS ok,
                                 COUNT(*) FILTER (WHERE al.status_code >= 300 OR al.status_code IS NULL) AS fail
                          FROM resource_server_async_accesslog al
                          {cluster_join}
                          WHERE al.timestamp_request >= %s AND al.timestamp_request <= %s {cluster_filter}
                          GROUP BY bucket
                        ) a ON a.bucket = s.bucket
                        ORDER BY s.bucket
                        """,
                        [
                            trunc_unit,
                            start_ts,
                            trunc_unit,
                            end_ts,
                            trunc_unit,
                            trunc_unit,
                            start_ts,
                            end_ts,
                        ]
                        + cluster_params,
                    )
                else:
                    cursor.execute(
                        """
                        WITH series AS (
                          SELECT generate_series(
                            date_trunc(%s, %s::timestamptz),
                            date_trunc(%s, %s::timestamptz),
                            CASE %s
                              WHEN 'minute' THEN interval '1 minute'
                              WHEN 'hour' THEN interval '1 hour'
                              WHEN 'day' THEN interval '1 day'
                              WHEN 'week' THEN interval '1 week'
                              WHEN 'month' THEN interval '1 month'
                            END
                          ) AS bucket
                        )
                        SELECT s.bucket,
                               COALESCE(a.ok, 0) AS ok,
                               COALESCE(a.fail, 0) AS fail
                        FROM series s
                        LEFT JOIN (
                          SELECT date_trunc(%s, timestamp_request) AS bucket,
                                 COUNT(*) FILTER (WHERE status_code=0 OR status_code BETWEEN 200 AND 299) AS ok,
                                 COUNT(*) FILTER (WHERE status_code >= 300 OR status_code IS NULL) AS fail
                          FROM resource_server_async_accesslog
                          WHERE timestamp_request >= %s AND timestamp_request <= %s
                          GROUP BY bucket
                        ) a ON a.bucket = s.bucket
                        ORDER BY s.bucket
                        """,
                        [
                            trunc_unit,
                            start_ts,
                            trunc_unit,
                            end_ts,
                            trunc_unit,
                            trunc_unit,
                            start_ts,
                            end_ts,
                        ],
                    )
                return cursor.fetchall()

        rows = await _get_rows()

        # OPTIMIZED: Removed unnecessary debug query that duplicates the main query
        return [
            {"t": r[0].isoformat(), "ok": int(r[1] or 0), "fail": int(r[2] or 0)}
            for r in rows
        ]
    except Exception as e:
        log.error(f"Error fetching overall series: {e}")
        return JsonResponse({"error": str(e)}, status=500)


@router.get("/analytics/model/series")
async def get_model_series(request, model: str, window: str = "24h"):
    try:
        delta, trunc_unit = _parse_series_window(window)
        end_ts = timezone.now()
        start_ts = end_ts - delta
        log.debug(
            f"model_series: model={model} window={window} trunc={trunc_unit} start={start_ts.isoformat()} end={end_ts.isoformat()}"
        )

        @sync_to_async
        def _get_rows():
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    WITH series AS (
                      SELECT generate_series(
                        date_trunc(%s, %s::timestamptz),
                        date_trunc(%s, %s::timestamptz),
                        CASE %s
                          WHEN 'minute' THEN interval '1 minute'
                          WHEN 'hour' THEN interval '1 hour'
                          WHEN 'day' THEN interval '1 day'
                          WHEN 'week' THEN interval '1 week'
                          WHEN 'month' THEN interval '1 month'
                        END
                      ) AS bucket
                    )
                    SELECT s.bucket,
                           COALESCE(a.ok, 0) AS ok,
                           COALESCE(a.fail, 0) AS fail
                    FROM series s
                    LEFT JOIN (
                      SELECT date_trunc(%s, al.timestamp_request) AS bucket,
                             COUNT(*) FILTER (WHERE al.status_code=0 OR al.status_code BETWEEN 200 AND 299) AS ok,
                             COUNT(*) FILTER (WHERE al.status_code >= 300 OR al.status_code IS NULL) AS fail
                      FROM resource_server_async_accesslog al JOIN resource_server_async_requestlog rl ON al.id = rl.access_log_id
                      WHERE rl.model = %s AND al.timestamp_request >= %s AND al.timestamp_request <= %s
                      GROUP BY bucket
                    ) a ON a.bucket = s.bucket
                    ORDER BY s.bucket
                    """,
                    [
                        trunc_unit,
                        start_ts,
                        trunc_unit,
                        end_ts,
                        trunc_unit,
                        trunc_unit,
                        model,
                        start_ts,
                        end_ts,
                    ],
                )
                return cursor.fetchall()

        rows = await _get_rows()
        total_ok = sum(int(r[1] or 0) for r in rows)
        total_fail = sum(int(r[2] or 0) for r in rows)
        log.debug(
            f"model_series: model={model} points={len(rows)} total_ok={total_ok} total_fail={total_fail}"
        )
        return [
            {"t": r[0].isoformat(), "ok": int(r[1] or 0), "fail": int(r[2] or 0)}
            for r in rows
        ]
    except Exception as e:
        log.error(f"Error fetching model series: {e}")
        return JsonResponse({"error": str(e)}, status=500)


@router.get("/analytics/model/box")
async def get_model_box(request, model: str, window: str = "24h"):
    try:
        delta, _ = _parse_series_window(window)
        end_ts = timezone.now()
        start_ts = end_ts - delta

        @sync_to_async
        def _get_row():
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                      AVG(throughput_tokens_per_sec),
                      PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY throughput_tokens_per_sec),
                      PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY throughput_tokens_per_sec),
                      AVG(response_time_sec),
                      PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY response_time_sec),
                      PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY response_time_sec)
                    FROM resource_server_async_requestmetrics
                    WHERE model = %s AND timestamp_compute_request >= %s AND timestamp_compute_request <= %s
                      AND throughput_tokens_per_sec IS NOT NULL AND response_time_sec IS NOT NULL
                    """,
                    [model, start_ts, end_ts],
                )
                return cursor.fetchone()

        row = await _get_row()

        return {
            "throughput": {
                "mean": float(row[0] or 0.0),
                "p50": float(row[1] or 0.0),
                "p99": float(row[2] or 0.0),
            },
            "latency": {
                "mean": float(row[3] or 0.0),
                "p50": float(row[4] or 0.0),
                "p99": float(row[5] or 0.0),
            },
        }
    except Exception as e:
        log.error(f"Error fetching model box: {e}")
        return JsonResponse({"error": str(e)}, status=500)


@router.get("/analytics/health")
async def get_health_status(request, cluster: str = "sophia", refresh: int = 0):
    """Proxy health info so the browser doesn't need a bearer token.
    Combines qstat job data (for Sophia/Polaris) or Metis API status with configured endpoints to mark offline models.
    """
    try:
        # Try cache first unless refresh requested
        cache_key = f"dashboard_health:{cluster}"
        if not refresh:
            cached_payload = cache.get(cache_key)
            if cached_payload:
                return JsonResponse(cached_payload)

        mock_auth_data = {
            "id": "ALCF-dashboard-id",
            "name": "ALCF-dashboard-name",
            "username": "ALCF-dashboard-username",
            "idp_id": "ALCF-dashboard-idp-id",
            "idp_name": "ALCF-dashboard-idp-name",
        }
        mock_auth = AsyncUser(**mock_auth_data)

        # Get the jobs response from the cluster wrapper
        try:
            cluster_adapter = await BaseCluster.load_adapter(cluster)
            jobs_response: JobsByStatus = await cluster_adapter.get_jobs(mock_auth)
            cluster_status = jobs_response.cluster_status
        except Exception as exc:
            # Empty (or cached values) if error occured
            return JsonResponse({"error": str(exc)}, status=500)

        # Fill model status for what is reported in the cluster status (/jobs URL)
        items = []
        for block_list in [jobs_response.running, jobs_response.queued]:
            for block in block_list:
                block = block.model_dump()
                model_list = [
                    m.strip() for m in block["Models"].split(",") if m.strip()
                ]
                for model in model_list:
                    items.append(
                        {
                            "model": model,
                            "status": block.get("Model Status"),
                            "nodes_reserved": block.get("Nodes Reserved", ""),
                            "host_name": block.get("Host Name", ""),
                            "start_info": block.get("Job Comments", "")
                            + block.get("Description", ""),
                        }
                    )

        # Gather the list of models that are already present in the items list
        present_models = {i["model"] for i in items}

        # Get all models listed for the targeted cluster
        configured_models = AsyncEndpoint.objects.filter(
            Q(cluster=cluster) & ~Q(model__in=present_models)
        ).values_list("model", flat=True)

        # Add offline models to the list
        async for model in configured_models:
            items.append(
                {
                    "model": model,
                    "status": "offline",
                    "nodes_reserved": "",
                    "host_name": "",
                    "start_info": "",
                }
            )

        # Build data to be displayed on the dashboard
        payload = {
            "items": items,
            "free_nodes": cluster_status.get("free_nodes"),
        }

        cache.set(cache_key, payload, timeout=300)
        return JsonResponse(payload)

    # Error if something wrong happened
    except Exception as e:
        log.error(f"Error fetching health status for cluster {cluster}: {e}")
        return JsonResponse({"error": str(e)}, status=500)


# ========= Additional realtime endpoints =========
@router.get("/analytics/requests-per-user")
async def get_requests_per_user(request, cluster: str = "all"):
    """Overall requests per user (from AccessLog/User)."""
    try:
        cache_key = f"dashboard:requests_per_user:{cluster}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        requests_per_user_set = AsyncAccessLog.objects.select_related("user").filter(
            user__isnull=False
        )
        if cluster and cluster.lower() != "all":
            requests_per_user_set = requests_per_user_set.select_related(
                "request_log"
            ).filter(request_log__cluster__iexact=cluster)

        requests_per_user_set = (
            requests_per_user_set.values(
                name=F("user__name"), username=F("user__username")
            )
            .annotate(
                total=Count("id"),
                successful=Count(
                    "id",
                    filter=Q(status_code__exact=0) | Q(status_code__range=(200, 299)),
                ),
                failed=Count(
                    "id",
                    filter=Q(status_code__isnull=True) | Q(status_code__gte=300),
                ),
            )
            .order_by("-total")
        )

        result = [r async for r in requests_per_user_set]

        cache.set(cache_key, result, timeout=300)
        return result
    except Exception as e:
        log.error(f"Error fetching requests per user: {e}")
        return JsonResponse({"error": str(e)}, status=500)


@router.get("/analytics/batch/overview")
async def get_batch_overview(request):
    """Batch metrics overview (prefers BatchMetrics, falls back to parsing BatchLog.result)."""
    try:
        cache_key = "dashboard:batch_overview"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        # Try BatchMetrics
        try:
            row = await AsyncBatchMetrics.objects.aaggregate(
                tokens=Sum("total_tokens"),
                requests=Sum("num_responses"),
                total_jobs=Count("*"),
                completed_jobs=Count("*", filter=Q(status__exact="completed")),
            )
            total_tokens = int(row["tokens"] or 0)
            total_requests = int(row["requests"] or 0)
            total_jobs = int(row["total_jobs"] or 0)
            completed_jobs = int(row["completed_jobs"] or 0)
            success_rate = (completed_jobs / total_jobs) if total_jobs > 0 else 0.0
            result = {
                "total_tokens": total_tokens,
                "total_requests": total_requests,
                "total_jobs": total_jobs,
                "completed_jobs": completed_jobs,
                "success_rate": success_rate,
            }
            cache.set(cache_key, result, timeout=300)
            return result
        except Exception:
            pass

        # Fallback to BatchLog parsing
        @sync_to_async
        def _get_row():
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                      COALESCE(SUM((CASE WHEN jsonb_typeof(result::jsonb -> 'metrics') = 'object'
                                         THEN (result::jsonb -> 'metrics' ->> 'total_tokens')::bigint ELSE 0 END)),0) AS tokens,
                      COALESCE(SUM((CASE WHEN jsonb_typeof(result::jsonb -> 'metrics') = 'object'
                                         THEN (result::jsonb -> 'metrics' ->> 'num_responses')::bigint ELSE 0 END)),0) AS requests,
                      COUNT(*)::bigint AS total_jobs,
                      COUNT(*) FILTER (WHERE status = 'completed') AS completed_jobs
                    FROM resource_server_async_batchlog
                    """
                )
                return cursor.fetchone()

        row = await _get_row()
        total_tokens = int(row[0] or 0)
        total_requests = int(row[1] or 0)
        total_jobs = int(row[2] or 0)
        completed_jobs = int(row[3] or 0)
        success_rate = (completed_jobs / total_jobs) if total_jobs > 0 else 0.0
        result = {
            "total_tokens": total_tokens,
            "total_requests": total_requests,
            "total_jobs": total_jobs,
            "completed_jobs": completed_jobs,
            "success_rate": success_rate,
        }
        cache.set(cache_key, result, timeout=300)
        return result
    except Exception as e:
        log.error(f"Error fetching batch overview: {e}")
        return JsonResponse({"error": str(e)}, status=500)


@router.get("/analytics/batch/model-summary")
async def get_batch_model_summary(request, model: str):
    """Batch model throughput/latency summary (mean, p50, p99)."""
    try:

        @sync_to_async
        def _get_row():
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                      AVG(throughput_tokens_per_sec),
                      PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY throughput_tokens_per_sec),
                      PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY throughput_tokens_per_sec),
                      AVG(response_time_sec),
                      PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY response_time_sec),
                      PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY response_time_sec)
                    FROM resource_server_async_batchmetrics
                    WHERE model = %s
                      AND throughput_tokens_per_sec IS NOT NULL AND response_time_sec IS NOT NULL
                    """,
                    [model],
                )
                return cursor.fetchone()

        row = await _get_row()
        return {
            "throughput": {
                "mean": float(row[0] or 0.0),
                "p50": float(row[1] or 0.0),
                "p99": float(row[2] or 0.0),
            },
            "latency": {
                "mean": float(row[3] or 0.0),
                "p50": float(row[4] or 0.0),
                "p99": float(row[5] or 0.0),
            },
        }
    except Exception as e:
        log.error(f"Error fetching batch model summary: {e}")
        return JsonResponse({"error": str(e)}, status=500)


@router.get("/analytics/batch-logs")
async def get_batch_logs_rt(request, page: int = 0, per_page: int = 100):
    """Paginated batch logs from Async tables with user info and duration."""
    try:
        start_index = page * per_page
        end_index = start_index + per_page
        users = AsyncUser.objects.all()
        qs = AsyncBatchLog.objects.order_by("-completed_at", "-in_progress_at")
        sliced = qs[start_index:end_index]
        results = []
        async for bl in sliced:
            user = await users.aget(id__exact=bl.user_id)
            results.append(
                {
                    "time": (bl.completed_at or bl.in_progress_at).isoformat()
                    if (bl.completed_at or bl.in_progress_at)
                    else None,
                    "name": user.name if user else None,
                    "username": user.username if user else None,
                    "model": bl.model,
                    "cluster": bl.cluster,
                    "status": bl.status,
                    "latency": (bl.completed_at - bl.in_progress_at).total_seconds()
                    if (bl.completed_at and bl.in_progress_at)
                    else None,
                }
            )
        return results
    except Exception as e:
        log.error(f"Error fetching batch logs rt: {e}")
        return JsonResponse({"error": str(e)}, status=500)


@router.get("/analytics/query-logs")
async def query_logs_custom(request):
    """Custom log query builder with flexible filters."""
    try:
        # Parse query parameters
        rows = int(request.GET.get("rows", 10))
        rows = min(max(1, rows), 10000)  # Clamp between 1 and 10000

        status_op = request.GET.get("status_op", "")
        status_val = request.GET.get("status_val", "")
        name_filter = request.GET.get("name", "")
        prompt_filter = request.GET.get("prompt", "")
        api_filter = request.GET.get("api", "")
        cluster_filter = request.GET.get("cluster", "")
        from_ts = request.GET.get("from_ts", "")
        to_ts = request.GET.get("to_ts", "")
        tzname = "America/Chicago"  # Fixed timezone

        # Build WHERE clauses
        conditions = ["1=1"]
        params = []

        # Status filter
        if status_op and status_val:
            allowed_ops = ["=", "!=", ">", "<", ">=", "<="]
            if status_op in allowed_ops:
                conditions.append(f"a.status_code {status_op} %s")
                params.append(int(status_val))

        # Name filter (ILIKE)
        if name_filter:
            conditions.append("u.name ILIKE %s")
            params.append(name_filter)

        # Prompt filter (ILIKE)
        if prompt_filter:
            conditions.append("r.prompt ILIKE %s")
            params.append(prompt_filter)

        # API route filter (ILIKE)
        if api_filter:
            conditions.append("a.api_route ILIKE %s")
            params.append(api_filter)

        # Cluster filter
        if cluster_filter:
            conditions.append("r.cluster = %s")
            params.append(cluster_filter.lower())

        # Timestamp expression
        ts_expr = "COALESCE(r.timestamp_compute_request, a.timestamp_request)"

        # Date filters
        if from_ts:
            conditions.append(f"{ts_expr} >= %s::timestamptz")
            params.append(from_ts)

        if to_ts:
            conditions.append(f"{ts_expr} <= %s::timestamptz")
            params.append(to_ts)

        where_clause = " AND ".join(conditions)

        # Build final query
        query = f"""
        SELECT json_agg(row_to_json(t))
        FROM (
            SELECT
                r.id AS request_id,
                r.cluster,
                r.framework,
                r.model,
                r.openai_endpoint,
                r.timestamp_compute_request,
                r.timestamp_compute_response,
                r.prompt,
                r.result,
                r.task_uuid,
                a.id AS accesslog_id,
                a.timestamp_request,
                a.timestamp_response,
                a.api_route,
                a.origin_ip,
                a.status_code,
                a.error,
                u.id AS user_id,
                u.name AS user_name,
                u.username AS user_username,
                u.idp_id,
                u.idp_name,
                u.auth_service
            FROM resource_server_async_accesslog a
            LEFT JOIN resource_server_async_requestlog r
              ON r.access_log_id = a.id
            LEFT JOIN resource_server_async_user u
              ON a.user_id = u.id
            WHERE {where_clause}
            ORDER BY {ts_expr} DESC
            LIMIT %s
        ) t
        """

        # Execute query
        @sync_to_async
        def _get_row():
            with connection.cursor() as cursor:
                # Set timezone first
                cursor.execute("SET TIME ZONE %s", [tzname])
                # Then execute the main query
                cursor.execute(query, params + [rows])
                return cursor.fetchone()

        result = await _get_row()

        # Return JSON array or empty array if no results
        data = result[0] if result and result[0] else []
        return JsonResponse(
            {"results": data, "count": len(data) if data else 0}, safe=False
        )

    except Exception as e:
        log.error(f"Error in custom log query: {e}")
        return JsonResponse({"error": str(e)}, status=500)
