from django.urls import include, path

urlpatterns = [
    path("resource_server/", include("resource_server_async.urls")),
]
