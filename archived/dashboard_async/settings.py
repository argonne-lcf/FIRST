"""
Django settings for `dashboard_async`. Depends on the `inference_gateway`'s
settings.
"""

import os

from inference_gateway.settings import *  # noqa: F403
from inference_gateway.utils import textfield_to_strlist

# Globus Dashboard Application Credentials
GLOBUS_DASHBOARD_APPLICATION_ID = os.getenv("GLOBUS_DASHBOARD_APPLICATION_ID")
GLOBUS_DASHBOARD_APPLICATION_SECRET = os.getenv("GLOBUS_DASHBOARD_APPLICATION_SECRET")

# Dashboard Globus OAuth settings
GLOBUS_DASHBOARD_REDIRECT_URI = os.getenv(
    "GLOBUS_DASHBOARD_REDIRECT_URI",
    "http://localhost:8000/dashboard/callback",  # Update for production
)

# Scopes needed for dashboard access
GLOBUS_DASHBOARD_SCOPES = [
    "openid",
    "profile",
    "email",
    "urn:globus:auth:scope:groups.api.globus.org:view_my_groups_and_memberships",
]

# Dashboard-specific Globus Group requirement
# Users must be members of this group to access the dashboard
GLOBUS_DASHBOARD_GROUP = os.getenv("GLOBUS_DASHBOARD_GROUP", "")
DASHBOARD_GROUP_ENABLED = len(GLOBUS_DASHBOARD_GROUP) > 0

GLOBUS_DASHBOARD_POLICY_ID = os.getenv("GLOBUS_DASHBOARD_POLICY_ID", "")
# Extract Globus policies that will determine which domains get access
GLOBUS_DASHBOARD_POLICIES = textfield_to_strlist(
    os.getenv("GLOBUS_DASHBOARD_POLICY_ID", "")
)
NUMBER_OF_GLOBUS_DASHBOARD_POLICIES = len(GLOBUS_DASHBOARD_POLICIES)
GLOBUS_DASHBOARD_POLICIES = ",".join(GLOBUS_DASHBOARD_POLICIES)

# Authentication settings for dashboard
LOGIN_URL = "/dashboard/login/"
LOGIN_REDIRECT_URL = "/dashboard/analytics"
LOGOUT_REDIRECT_URL = "/dashboard/login/"

# Override settings specific to the Dashboard
INSTALLED_APPS = [
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "resource_server_async",
    "dashboard_async",
    # Configuration checks (mostly for making sure auth guards are in place)
    "inference_gateway.apps.AuthCheckConfig",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
]

ROOT_URLCONF = "dashboard_async.urls"
FORCE_SCRIPT_NAME = "/dashboard"

WSGI_APPLICATION = "dashboard_async.wsgi.application"

# Dashboard hard depends on Postgres as the DB backend
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": os.getenv("PGDATABASE", "postgres"),
        "USER": os.getenv("PGUSER", "postgres"),
        "PASSWORD": os.getenv("PGPASSWORD", "postgres"),
        "HOST": os.getenv("PGHOST", "pgbouncer"),  # Connect to the pgbouncer service
        "PORT": os.getenv("PGPORT", "6432"),  # Default PgBouncer port
        "OPTIONS": {
            "connect_timeout": 10,
            "pool": {
                "min_size": 0,
                "max_size": 2,  # HARD LIMIT imposed here
            },
        },
        "CONN_MAX_AGE": 0,
        "ATOMIC_REQUESTS": False,
        "CONN_HEALTH_CHECKS": False,
    }
}
