from django.test import SimpleTestCase

from resource_server_async.globus_utils import has_no_managers


class HasNoManagersTestCase(SimpleTestCase):
    def test_v3_endpoint_with_managers(self):
        status = {"status": "online", "details": {"managers": 2}}
        self.assertFalse(has_no_managers(status))

    def test_v3_endpoint_without_managers(self):
        status = {"status": "online", "details": {"managers": 0}}
        self.assertTrue(has_no_managers(status))

    def test_v4_manager_endpoint_reports_no_details(self):
        self.assertFalse(has_no_managers({"status": "online"}))
