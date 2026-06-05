"""Globus Compute function for calling the GenSLM-ESM FastAPI server."""

from __future__ import annotations

from functools import lru_cache
from typing import Any, cast

import numpy as np
import torch
from genslm_esm.data import FastaDataset, GenslmEsmcDataCollator
from genslm_esm.modeling import GenslmEsmcModel, GenslmEsmcModelOutput
from parsl_object_registry import clear_torch_cuda_memory_callback, register
from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from torch.utils.data import DataLoader
from tqdm import tqdm
from transformers import AutoModel, AutoTokenizer


########################################################
# Application settings
########################################################
class AppSettings(BaseSettings):
    """Application settings."""

    # Uses .env file if available, otherwise uses environment variables.
    # Environment variables override .env file. Extra settings are ignored.
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    hf_token: SecretStr = Field(
        description="Hugging Face token to use for model loading (Required)",
    )
    batch_size: int = Field(
        default=64,
        ge=1,
        description="Batch size to use for embeddings",
    )
    num_workers: int = Field(
        default=4,
        ge=0,
        description="Number of workers to use for embeddings",
    )
    pin_memory: bool = Field(
        default=True,
        description="Whether to pin memory for embeddings",
    )


@lru_cache
def get_settings() -> AppSettings:
    """Get application settings."""
    return AppSettings()


########################################################
# Model functions
########################################################


# This function acts as an in-memory cache for the model, tokenizer, and
# device. It behaves like lru_cache with size one but automatically cleans
# up the memory when a new model is loaded.
@register(shutdown_callback=clear_torch_cuda_memory_callback)
def load_model(
    model_id: str,
) -> tuple[GenslmEsmcModel, AutoTokenizer, torch.device]:
    """Load a model and return the model, tokenizer, and device."""
    # Load model and tokenizer
    print(f"Loading model: {model_id}")
    model = AutoModel.from_pretrained(model_id, trust_remote_code=True)
    tokenizer = AutoTokenizer.from_pretrained(model_id, trust_remote_code=True)

    # Set model to evaluation mode
    model.eval()

    # Initialize device
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    # Move model to device
    model = model.to(device)

    # Convert to bfloat16 if not on CPU
    if device.type != "cpu":
        model = model.to(torch.bfloat16)

    print(f"Model loaded successfully on {device}")
    print(f"Model dtype: {next(model.parameters()).dtype}")

    return model, tokenizer, device


def average_pool(
    embeddings: torch.Tensor,
    attention_mask: torch.Tensor,
    sos_token: bool = True,
    eos_token: bool = True,
) -> torch.Tensor:
    """Average pool the hidden states using the attention mask.

    Parameters
    ----------
    embeddings : torch.Tensor
        The hidden states to pool (batch_size, seq_len, d_model).
    attention_mask : torch.Tensor
        The attention mask for the hidden states (batch_size, seq_len).
    sos_token : bool, optional
        Whether to include the start token in the pooling, by default True.
    eos_token : bool, optional
        Whether to include the end token in the pooling, by default True.

    Returns
    -------
    torch.Tensor
        The pooled embeddings (batch_size, d_model).
    """
    # Clone the attention mask to avoid modifying the original tensor
    attn_mask = attention_mask.clone()

    # Get the sequence lengths
    seq_lengths = attn_mask.sum(axis=1)

    # Set the attention mask to 0 for start and end tokens
    if sos_token:
        attn_mask[:, 0] = 0
    if eos_token:
        attn_mask[:, seq_lengths - 1] = 0

    # Create a mask for the pooling operation (B, SeqLen, HiddenDim)
    pool_mask = attn_mask.unsqueeze(-1).expand(embeddings.shape)

    # Sum the embeddings over the sequence length (use the mask to avoid
    # pad, start, and stop tokens)
    sum_embeds = torch.sum(embeddings * pool_mask, 1)

    # Avoid division by zero for zero length sequences by clamping
    sum_mask = torch.clamp(pool_mask.sum(1), min=1e-9)

    # Compute mean pooled embeddings for each sequence
    return sum_embeds / sum_mask


def compute_embeddings(
    model_id: str,
    sequences: list[str],
    modality: str,
) -> list[list[float]]:
    """Compute embeddings for a list of sequences.

    Parameters
    ----------
    model_id: str
        The model ID to use for embeddings.
    sequences : list[str]
        The sequences to compute embeddings for.
    modality : str
        The modality to use for embeddings. Must be 'codon' or 'aminoacid'.

    Returns
    -------
    list[list[float]]
        The embeddings for the sequences.
    """
    # Validate the modality
    if modality not in ["codon", "aminoacid"]:
        raise ValueError(f'modality must be "codon" or "aminoacid", got {modality}')

    # Load the model
    model, tokenizer, device = load_model(model_id)

    print("All models started successfully.")
    # Get the settings
    settings = get_settings()

    # Decide whether to return codon or amino acid embeddings
    return_codon = modality == "codon"
    return_aminoacid = modality == "aminoacid"

    # The dataset splits the sequences into codons
    dataset = FastaDataset(
        sequences=sequences,
        return_codon=return_codon,
        return_aminoacid=return_aminoacid,
        contains_nucleotide=return_codon,
    )

    # Create the collator
    collator = GenslmEsmcDataCollator(
        return_codon=return_codon,
        return_aminoacid=return_aminoacid,
        tokenizer=tokenizer,
    )

    # Create the dataloader
    dataloader = DataLoader(
        dataset,
        batch_size=settings.batch_size,
        collate_fn=collator,
        num_workers=settings.num_workers,
        pin_memory=settings.pin_memory if device.type != "cpu" else False,
    )

    # Get the attention mask key
    attn_key = f"{modality}_attention_mask"

    # Initialize the embeddings list
    embeddings_list = []

    # Iterate over the dataloader
    with torch.no_grad():
        for batch in tqdm(dataloader):
            # Move the batch to the device
            items = batch.to(device)

            # Run the model
            outputs = cast(GenslmEsmcModelOutput, model(**items))

            # Check that the hidden states are not None
            assert outputs.hidden_states is not None

            # Get the last hidden state
            last_embeddings = outputs.hidden_states[-1]

            # Average pool the embeddings over the sequence length dimension
            embeddings = average_pool(last_embeddings, items[attn_key])

            # Convert from bfloat16 to float32 and move to CPU/numpy
            embeddings = embeddings.to(torch.float32).cpu().numpy()

            # Append the embeddings to the list
            embeddings_list.append(embeddings)

    # Concatenate the embeddings list
    embeddings = np.concatenate(embeddings_list, axis=0)

    # Convert the numpy array to a list of lists for the response
    embeddings = embeddings.tolist()

    # Return the embeddings
    return embeddings


########################################################
# Globus Compute function
########################################################


def embeddings_gc_fn(parameters: dict[str, Any]) -> str:
    """Globus Compute function for generating embeddings.

    Parameters
    ----------
    parameters : dict[str, Any]
        The function parameters containing 'model_params'.
        model_params fields are:
        - 'input' (str or list[str]): The sequences to embed.
        - 'model' (str): The model ID to use for embeddings.
        - 'encoding_format' (Literal['float', 'int']): The encoding format
            to use for the embeddings. Default is 'float' (Unused).
        The 'model' field can be one of the following:
        - 'genslm-test/genslm-esmc-600M-contrastive-aminoacid'
        - 'genslm-test/genslm-esmc-600M-contrastive-codon''
        - 'genslm-test/genslm-esmc-600M-joint-aminoacid'
        - 'genslm-test/genslm-esmc-600M-joint-codon'
        - 'genslm-test/genslm-esmc-600M-aminoacid'
        - 'genslm-test/genslm-esmc-600M-codon'
        - 'genslm-test/genslm-esmc-300M-contrastive-aminoacid'
        - 'genslm-test/genslm-esmc-300M-contrastive-codon'
        - 'genslm-test/genslm-esmc-300M-joint-aminoacid'
        - 'genslm-test/genslm-esmc-300M-joint-codon'
        - 'genslm-test/genslm-esmc-300M-aminoacid'
        - 'genslm-test/genslm-esmc-300M-codon'


        Example request:
        ```json
        {
            'model_params': {
                'input': 'MTPHKGATL...',
                'model': 'genslm-test/genslm-esmc-600M-contrastive-aminoacid',
                'encoding_format': 'float',
            }
        }
        ```

    Returns
    -------
    str
        The embedding response in OpenAI-style format as a JSON string.
        The JSON object contains:
        - 'object' (str): The object type, always 'list'.
        - 'data' (list[dict]): List of embedding objects.
        - 'model' (str): The model ID used for embeddings.
        - 'usage' (dict): Token usage information.
        - 'response_time' (float): Execution time in seconds.
        - 'throughput_tokens_per_second' (float): Processing speed.

    Example response:
    ```json
    {
        'object': 'list',
        'data': [
            {
                'object': 'embedding',
                'embedding': [0.123, -0.456, 0.789, ...],
                'index': 0
            }
        ],
        'model': 'genslm-test/genslm-esmc-600M-contrastive-aminoacid',
        'usage': {
            'prompt_tokens': 8,
            'total_tokens': 8
        }
    }
    ```
    """
    import json
    import time

    # Explicit imports for globus compute
    from genslm_esm_globus_compute.globus_compute_fn import compute_embeddings

    start_time = time.time()

    # Unpack the parameters
    if "model_params" in parameters:
        model_params = parameters["model_params"]
    else:
        # Fallback for backward compatibility
        model_params = parameters

    # Check if this is a health check (simulating vLLM behavior)
    # The gateway may send an 'openai_endpoint' parameter to check health.
    openai_endpoint = model_params.get("openai_endpoint", "")
    if "health" in openai_endpoint.lower():
        end_time = time.time()
        response_time = end_time - start_time
        return json.dumps(
            {
                "status": "healthy",
                "response_time": response_time,
                "throughput_tokens_per_second": 0.0,
            },
            indent=4,
        )

    # Unpack the request parameters
    sequences = model_params["input"]
    model_id = model_params["model"]

    # If the sequences is a single string, convert it to a list
    if isinstance(sequences, str):
        sequences = [sequences]

    # Validate the input sequences
    if not isinstance(sequences, list) or (
        not all(isinstance(seq, str) for seq in sequences)
    ):
        raise ValueError(f"input must be a str or list of str, got {type(sequences)}")

    # Validate the model ID
    valid_model_ids = [
        "genslm-test/genslm-esmc-600M-contrastive-aminoacid",
        "genslm-test/genslm-esmc-600M-contrastive-codon",
        "genslm-test/genslm-esmc-600M-joint-aminoacid",
        "genslm-test/genslm-esmc-600M-joint-codon",
        "genslm-test/genslm-esmc-600M-aminoacid",
        "genslm-test/genslm-esmc-600M-codon",
        "genslm-test/genslm-esmc-300M-contrastive-aminoacid",
        "genslm-test/genslm-esmc-300M-contrastive-codon",
        "genslm-test/genslm-esmc-300M-joint-aminoacid",
        "genslm-test/genslm-esmc-300M-joint-codon",
        "genslm-test/genslm-esmc-300M-aminoacid",
        "genslm-test/genslm-esmc-300M-codon",
    ]
    if model_id not in valid_model_ids:
        raise ValueError(f"model must be one of {valid_model_ids}, got {model_id}")

    # Extract modality from model name
    modality = "aminoacid" if "aminoacid" in model_id.lower() else "codon"

    # Strip the modality from the model name if it exists
    # This occurs for contrastive and joint models, e.g.,
    # 'genslm-test/genslm-esmc-600M-contrastive-aminoacid' ->
    # 'genslm-test/genslm-esmc-600M-contrastive'
    if "contrastive" in model_id.lower() or "joint" in model_id.lower():
        clean_model_id = "-".join(model_id.split("-")[:-1])
    else:
        clean_model_id = model_id

    # Compute the sequence embeddings using the model
    embeddings = compute_embeddings(clean_model_id, sequences, modality)

    # Calculate token counts
    if modality == "aminoacid":
        # If aminoacid modality, each character is a token
        total_tokens = sum(len(seq) for seq in sequences)
    elif modality == "codon":
        # If codon modality, each codon is a token
        total_tokens = sum(len(seq) // 3 for seq in sequences)
    else:
        total_tokens = 0

    # Format response in OpenAI-style structure
    data = [
        {
            "object": "embedding",
            "embedding": embedding,
            "index": idx,
        }
        for idx, embedding in enumerate(embeddings)
    ]

    end_time = time.time()
    response_time = end_time - start_time
    throughput = total_tokens / response_time if response_time > 0 else 0

    # Return the embeddings in OpenAI-style format
    response = {
        "object": "list",
        "data": data,
        "model": model_params["model"],  # Use original model name from params
        "usage": {
            "prompt_tokens": total_tokens,
            "total_tokens": total_tokens,
        },
        "response_time": response_time,
        "throughput_tokens_per_second": throughput,
    }

    return json.dumps(response, indent=4)


if __name__ == "__main__":
    from globus_compute_sdk import Client

    # This will trigger an authentication flow if you aren't already logged in
    gcc = Client()

    # Register the function with the Globus Compute
    func_uuid = gcc.register_function(embeddings_gc_fn)

    # Print the function UUID
    print(f"Function registered with UUID: {func_uuid}")

    # Touch a file to back up the function UUID
    with open("genslm_esm_globus_compute_function_uuid.txt", "w") as f:
        f.write(func_uuid)
