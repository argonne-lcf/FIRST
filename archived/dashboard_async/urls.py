from django.urls import path

from dashboard_async.views import (
    analytics_realtime_view,
    api,
    dashboard_callback_view,
    dashboard_login_view,
    dashboard_logout_view,
)

# Use the unique namespace or versioned API instance
urlpatterns = [
    # Globus OAuth Authentication URLs (no trailing slash, consistent with APPEND_SLASH = False)
    path("login", dashboard_login_view, name="dashboard_login"),
    path("callback", dashboard_callback_view, name="dashboard_callback"),
    path("logout", dashboard_logout_view, name="dashboard_logout"),
    # Main dashboard view (protected by @globus_login_required)
    path("analytics", analytics_realtime_view, name="dashboard_analytics"),
    # API URLs (Django Ninja router - protected by DjangoSessionAuth with Globus validation)
    path(
        "", api.urls
    ),  # This will serve all API routes under the 'dashboard_async/' URL namespace
]
