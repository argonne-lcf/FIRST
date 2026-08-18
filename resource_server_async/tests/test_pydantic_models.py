import json

from django.test import testcases
from pydantic import ValidationError

from resource_server_async.schemas.batch import BatchSubmit
from resource_server_async.schemas.openai_control import (
    ChatCompletionsControl,
    CompletionsControl,
    EmbeddingsControl,
)

# Constants
COMPLETIONS = "completions"
CHAT_COMPLETIONS = "chat_completions"
EMBEDDINGS = "embeddings"
BATCH = "batch"

# Pydantic models
PYDANTIC_MODELS = {
    COMPLETIONS: CompletionsControl,
    CHAT_COMPLETIONS: ChatCompletionsControl,
    EMBEDDINGS: EmbeddingsControl,
    BATCH: BatchSubmit,
}

INVALID_CONTROL_PARAMS = {
    COMPLETIONS: [
        {"prompt": "missing model"},
        {"model": 1, "prompt": "wrong model type"},
        {"model": "model", "prompt": {"wrong": "prompt type"}},
        {"model": "model", "prompt": "hello", "stream": "false"},
    ],
    CHAT_COMPLETIONS: [
        {"messages": []},
        {"model": "model"},
        {"model": "model", "messages": "wrong messages type"},
        {"model": "model", "messages": ["not an object"]},
    ],
    EMBEDDINGS: [
        {"input": "missing model"},
        {"model": "model", "input": {"wrong": "input type"}},
        {"model": "model", "input": "hello", "stream": True},
    ],
}


# Test OpenAI pydantic models
class PydanticModelsTestCase(testcases.TestCase):
    # Initialization
    @classmethod
    def setUp(self):
        """
        Initialization that will only happen once before running all tests.
        """

        # Load test input data (OpenAI format)
        base_path = "resource_server_async/tests/json"
        self.valid_params = {}
        self.invalid_params = {}
        for model in PYDANTIC_MODELS:
            with open(f"{base_path}/valid_{model}.json") as json_file:
                self.valid_params[model] = json.load(json_file)
            if model == BATCH:
                with open(f"{base_path}/invalid_{model}.json") as json_file:
                    self.invalid_params[model] = json.load(json_file)
            else:
                self.invalid_params[model] = INVALID_CONTROL_PARAMS[model]

    # Test OpenAICompletions pydantic model for validation
    def test_completions_control_validation(self):
        self.__generic_serializer_validation(COMPLETIONS)

    # Test OpenAIChatCompletions pydantic model for validation
    def test_chat_completions_control_validation(self):
        self.__generic_serializer_validation(CHAT_COMPLETIONS)

    # Test OpenAIEmbeddings pydantic model for validation
    def test_embeddings_control_validation(self):
        self.__generic_serializer_validation(EMBEDDINGS)

    # Test Batch pydantic model for validation
    def test_Batch_validation(self):
        self.__generic_serializer_validation(BATCH)

    # Reusable function to validate pydantic model definitions
    def __generic_serializer_validation(self, model):
        # For each valid set of parameters ...
        for valid_params in self.valid_params[model]:
            # Make sure the pydantic model does not raise a validation error
            try:
                PYDANTIC_MODELS[model](**valid_params)
            except ValidationError:
                self.fail(
                    f"The following data was supposed to be valid, but was flagged as invalid: {valid_params}"
                )

        # For each invalid set of parameters ...
        for invalid_params in self.invalid_params[model]:
            # Make sure the pydantic model raises a validation error
            try:
                PYDANTIC_MODELS[model](**invalid_params)
                self.fail(
                    f"The following data was supposed to be invalid, but was flagged as valid: {invalid_params}"
                )
            except ValidationError:
                pass
