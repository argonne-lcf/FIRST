#!/usr/bin/env python3

import os
import sys

import django
import requests

# Setup Django environment to access cache
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "inference_gateway.settings")
django.setup()
from django.core.cache import cache
from resource_server_async.schemas.clusters import ClusterStatus

# ===================
# ALCF cluster status
# ===================

STATUS_URLs = {
    "sophia": "https://api.alcf.anl.gov/api/v1/status/resources/9674c7e1-aecc-4dbb-bf01-c9197e027cd6",
}

# Cache TTL in seconds (30 minutes)
CACHE_TTL = 1800

# For each cluster ...
for cluster, url in STATUS_URLs.items():
    # Set Redis cache key
    cache_key = f"cluster_status:{cluster}"

    # Get status and store in Redis cache
    try:
        response = requests.get(url, timeout=10)
        data = response.json()
        current_status = data.get("current_status", "unknown")
        cache.set(
            cache_key,
            ClusterStatus(
                status=current_status,
                cluster=cluster,
                message=f"{cluster} is under maintenance"
                if current_status == "down"
                else f"{cluster} is online",
            ),
            timeout=CACHE_TTL,
        )
        print(f"{cluster}: {current_status}")

    # Cache error if something went wrong Log the status
    except requests.RequestException as e:
        cache.set(
            cache_key,
            ClusterStatus(
                status="error",
                cluster=cluster,
                message=str(e),
            ),
            timeout=CACHE_TTL,
        )
        print(f"{cluster}: ERROR - {e}")
