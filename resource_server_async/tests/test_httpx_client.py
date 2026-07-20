import ssl
from unittest import TestCase
from unittest.mock import MagicMock, patch

from resource_server_async.httpx_client import create_ssl_context


class CreateSslContextTests(TestCase):
    def test_custom_tls_context_requires_tls13(self) -> None:
        context = MagicMock(spec=ssl.SSLContext)
        with patch(
            "resource_server_async.httpx_client.ssl.create_default_context",
            return_value=context,
        ) as create_default_context:
            result = create_ssl_context(
                ca_cert_path="/secure/ca.crt",
                check_hostname=True,
            )

        self.assertIs(result, context)
        self.assertEqual(context.minimum_version, ssl.TLSVersion.TLSv1_3)
        self.assertTrue(context.check_hostname)
        create_default_context.assert_called_once_with(cafile="/secure/ca.crt")

    def test_system_default_is_unchanged_without_custom_tls(self) -> None:
        self.assertIs(create_ssl_context(), True)
