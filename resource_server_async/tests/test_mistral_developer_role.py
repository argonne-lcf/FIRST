from django.test import SimpleTestCase

from resource_server_async.services import _adapt_mistral_developer_role


class MistralDeveloperRoleTests(SimpleTestCase):
    def test_chat_completions_maps_developer_role_to_system(self) -> None:
        payload = {
            "messages": [
                {"role": "developer", "content": "Be brief."},
                {"role": "user", "content": "Say hi."},
            ]
        }

        _adapt_mistral_developer_role(payload, "chat/completions")

        self.assertEqual(payload["messages"][0]["role"], "system")
        self.assertEqual(payload["messages"][1]["role"], "user")

    def test_responses_maps_developer_role_without_changing_other_parts(self) -> None:
        payload = {
            "input": [
                {"role": "developer", "content": "Be brief."},
                {"role": "user", "content": [{"type": "input_text", "text": "hi"}]},
            ]
        }

        _adapt_mistral_developer_role(payload, "responses")

        self.assertEqual(payload["input"][0]["role"], "system")
        self.assertEqual(
            payload["input"][1]["content"], [{"type": "input_text", "text": "hi"}]
        )
