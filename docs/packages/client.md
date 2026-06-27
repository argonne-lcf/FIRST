# ALCF AI Inference Services SDK

This package provides Python client and CLI tools to facilitate usage of the ALCF AI Inference services.

## Command Line Usage

### Quick Start

```bash
# Log in with Globus:
uvx alcf-ai auth login

# Chat with a model
# The default --model is meta-llama/Llama-4-Scout-17B-16E-Instruct
uvx alcf-ai chat "How do I know Pi is irrational? Be concise."
```

### Auth

```bash
# Login for Inference Service only:
uvx alcf-ai auth login

# Login for Inference+Globus data transfers
# (append :data_access only if required for your collection)
SOURCE_COLLECTION="your globus collection UUID"
uvx alcf-ai auth login --authorize-transfers $SOURCE_COLLECTION:data_access

# Get an access token to use externally:
token=$(uvx alcf-ai auth get-access-token)
curl -H "Authorization: Bearer $token" https://inference-api.alcf.anl.gov/resource_server/list-endpoints | jq
```

### Discovering Models

To list the models and corresponding API endpoints that are currently available, use:

```bash
uvx alcf-ai endpoints ls
```

To view the status of models that are currently hot or starting up on a cluster, use:

```bash
# Can substitute "sophia" with "metis"
uvx alcf-ai clusters ls-jobs sophia
```

### Chat with an LLM

```bash
# See detailed options:
uvx alcf-ai chat --help

# For example:
uvx alcf-ai chat --model google/gemma-4-31B-it --stream --temp 0.3 --max-tokens 100 "What is KL divergence? Answer in less than 75 words."
```

### Segment images with SAM3

You can segment your images with the [Meta SAM3](https://github.com/facebookresearch/sam3) model.

Send a single image URI plus prompt in for segmentation:

```bash
uvx alcf-ai sam3 submit-image \
  https://raw.githubusercontent.com/masalim2/sam3-service/refs/heads/main/examples/images/groceries.jpg \
  "Baguette" \
  --save-preview ~/test-baguettes.png
```

#### Batch Processing

For high-throughput, preprocess and bundle your images and prompts in the [WebDataset format](https://github.com/webdataset/webdataset)
using the built-in CLI tool:

```bash
# Bundle all .tiff files in directory with 3 prompts Creates WebDataset tar
# files in --output-dir, with 100 images per .tar.
alcf-ai sam3 create-webdataset \
   /path/to/tiff-stack \
   .tiff \
    "Phloem Fibers" "Hydrated Xylem vessels" "Air-based Pith cells" \
    --output-dir test-wds --shard-size=100 --num-workers=4
```

If the dataset is on a Globus Collection, you can authorize the CLI to send them
to the inference service:

```bash
# Look up the UUID of your collection:
SOURCE_COLLECTION="your globus collection UUID"

# Append ":data_access" if this scope is required:
uvx alcf-ai auth login --authorize-transfers $SOURCE_COLLECTION:data_access
```

Then use the tool to drive data staging and batch inference:

```bash
SAM3_FINETUNE=/eagle/inference_service/sam3-service/weights/synaps-i
SECONDS=0

for f in test-wds/*.tar
do
uvx alcf-ai sam3 submit-batch $f --from-collection-id $SOURCE_COLLECTION --weights-dir-override $SAM3_FINETUNE >> batch-inference.log 2>&1 &
done
wait
echo "Completed in $SECONDS seconds."
```

You can preview the segmentation results in a batch by passing the paths to the input and result tar files:

```bash
uvx  alcf-ai sam3 preview-batch-results shard-00004.tar shard-00004.results.tar
```

### Installing the latest client version

You can force an install of the latest version and verify your local version using:

```bash
uvx alcf-ai@latest version
```


## SDK Usage

You can use `pip install alcf-ai` or `uv run --with-alcf python` to add the SDK to your environment:

```bash
uv run --with alcf-ai python
```

### OpenAI Client

Use `alcf_ai.InferenceClient` to construct an OpenAI client for any ALCF-backed
cluster.  This reuses your auth and ensures that requests are sent to the right
URL:

```python
from alcf_ai import InferenceClient
from rich import print

# Automatically uses cached refresh tokens from previous login:
client = InferenceClient()

# Programmatically discover endpoints:
print(client.endpoints.list()["clusters"]["sophia"])

# Get an OpenAI API client for an ALCF cluster:
oai = client.clusters.get("sophia").openai
print(
    oai.chat.completions.create(
        model="openai/gpt-oss-120b",
        messages=[{"role": "user", "content": "Hello there!"}],
    )
)
```

### Data Movement and SAM3

You can use the same `InferenceClient` to move data in and out of a Globus Guest
Collection that's managed by the service.  Your data is stored in an ephemeral
staging subdirectory, with ACLs that grant *only your* Globus identity
read/write access to it.

```python
from alcf_ai import InferenceClient
from alcf_ai.auth import STAGING_COLLECTION_ROOT
client = InferenceClient()

dataset_path = Path("/path/to/my-dataset.tar")
collection_id="globus collection uuid"

# Stage in data:
stagein = client.staging.stage_in(
    dataset_path, Path(dataset_path.name), from_collection_id=collection_id
)

# Submit SAM3 inference:
resp = client.sam3.submit_batch(
    STAGING_COLLECTION_ROOT + str(stagein.destination_path)
)

# Wait for inference:
result = client.sam3.poll_task_result(resp.task_id)

# Copy results back:
client.staging.stage_out(
    collection_id,
    Path(result.result_path).name,
    dataset_path.with_suffix(".results.tar"),
)
```

## Using an alternate service URL

The client with both programmatic and CLI usage defaults to the ALCF Inference Service production base url of
<https://inference-api.alcf.anl.gov/resource_server/>.  This can be altered in a few ways:

1. By exporting the `inference_base_url` environment variable
2. From the CLI, passing an optional `--base-url` to the `alcf-ai` subcommand.
3. From the Python client, passing the kwarg `InferenceClient(base_url="...")`