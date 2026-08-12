from pathlib import Path
from unittest.mock import patch

from alcf_ai.client import InferenceClient


def test_inference_client_passes_private_ca_and_disables_environment() -> None:
    with patch("alcf_ai.client.Client.__init__", return_value=None) as initialize:
        InferenceClient(
            "https://inference.example/resource_server/",
            verify=Path("/service/private/gateway-ca.pem"),
            trust_env=False,
        )

    kwargs = initialize.call_args.kwargs
    assert kwargs["verify"] == "/service/private/gateway-ca.pem"
    assert kwargs["trust_env"] is False
