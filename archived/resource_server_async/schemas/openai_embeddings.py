from enum import Enum
from typing import List, Literal, Optional, Self, Union

from pydantic import BaseModel, Field, model_validator


# Extention of the Pydantic BaseModel that prevent extra attributes
class BaseModelExtraForbid(BaseModel):
    class Config:
        extra = "forbid"


# Encoding format
class EncodingFormat(str, Enum):
    float = "float"
    base64 = "base64"


# OpenAI embeddings
# https://platform.openai.com/docs/api-reference/embeddings/create
class OpenAIEmbeddingsPydantic(BaseModelExtraForbid):
    openai_endpoint: Literal["embeddings"] = "embeddings"
    input: Union[str, List[str], List[int], List[List[int]]]
    model: str
    dimensions: Optional[int] = Field(default=None, ge=1)
    encoding_format: Optional[EncodingFormat] = EncodingFormat.float
    user: Optional[str] = None

    # Extra validations
    @model_validator(mode="after")
    def extra_validations(self) -> Self:
        # Check length of input arrays
        min_length = 1
        max_lentgh = 2048
        error_message = "Length of all 'input' arrays must be between 1 and 2048."
        if isinstance(self.input, list):
            if len(self.input) > max_lentgh or len(self.input) < min_length:
                raise ValueError(error_message)
            for item in self.input:
                if isinstance(item, list):
                    if len(item) > max_lentgh or len(item) < min_length:
                        raise ValueError(error_message)

        # Return self if nothing wrong happened in the valudation step
        return self
