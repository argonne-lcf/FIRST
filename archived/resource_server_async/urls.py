from django.urls import path

from resource_server_async.api import api

# Use the unique namespace or versioned API instance
urlpatterns = [
    path(
        "", api.urls
    ),  # This will serve all routes under the 'resource_server_async/' URL namespace
]
