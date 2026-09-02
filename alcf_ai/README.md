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
uvx alcf-ai ls-endpoints
```

To view the status of models that are currently hot or starting up on a cluster, use:

```bash
# Can substitute "sophia" with "metis"
uvx alcf-ai ls-jobs sophia
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

### Segment images with DINOv3

You can also segment your images with a [DINOv3](https://github.com/facebookresearch/dinov3)
segmentation model. Unlike SAM3, DINOv3 works over a **whole folder of images at
once**: you stage in a directory, the GPU dataloader batches over every image in
it, and a folder of results (semantic masks, plus optional color overlays) is
written back out.

Because the folder is the unit of work, the input/output paths are staged with
Globus Transfer as recursive directory transfers. Folder transfers require a
source/destination Globus collection (the HTTPS upload path is single-file only),
so first authorize transfers against your collection:

```bash
# Look up the UUID of your collection:
SOURCE_COLLECTION="your globus collection UUID"

# Append ":data_access" if this scope is required:
uvx alcf-ai auth login --authorize-transfers $SOURCE_COLLECTION:data_access
```

Then submit a folder for segmentation with the CLI. It stages the folder in,
runs inference, polls until complete, and stages the results folder back:

```bash
uvx alcf-ai dinov3 submit \
  /path/to/image-folder \
  --from-collection-id $SOURCE_COLLECTION \  # Stage the input folder in from here
  --to-collection-id $SOURCE_COLLECTION \    # Send the results folder back here
  --save-overlay                             # Also render color overlays
```

#### Sharding large datasets

The GPU dataloader batches over all images in a single folder, so **one folder =
one inference task**. To parallelize a large dataset, shard it into subfolders
and submit each concurrently. The SDK is preferred for driving the bulk
transfers and concurrent inference tasks:

```python
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from alcf_ai import InferenceClient
from alcf_ai.auth import STAGING_COLLECTION_ROOT
from rich import print

client = InferenceClient()

collection_id = "your globus collection UUID"

# A dataset pre-sharded into subfolders, e.g. dataset/shard-00000/, shard-00001/, ...
dataset_dir = Path("/path/to/dataset")
shards = sorted(p for p in dataset_dir.iterdir() if p.is_dir())


def run_inference(shard_dir: Path) -> dict:
    """Stage a folder in, run DINOv3 segmentation, and stage the results back."""
    # Recursively stage the input folder in from the source collection:
    stagein = client.stage_in(
        shard_dir,
        Path(shard_dir.name),
        from_collection_id=collection_id,
        recursive=True,
    )
    remote_input = STAGING_COLLECTION_ROOT + str(stagein.destination_path)

    # Submit the inference request and poll for completion. The results
    # directory is derived server-side within your staging area (you don't -- and
    # can't -- choose it) and reported back as `mask_dir` in the result.
    resp = client.dinov3.submit(input_dir=remote_input, save_overlay=True)
    result = client.dinov3.poll_task_result(resp.task_id)

    # Recursively stage the results folder back to the source collection. Its
    # name comes from the service (mask_dir == <results_dir>/semantic_masks):
    results_dirname = Path(result["mask_dir"]).parent.name
    client.stage_out(
        collection_id,
        Path(results_dirname),
        shard_dir.with_name(results_dirname),
        recursive=True,
    )
    return result


with ThreadPoolExecutor(max_workers=8) as pool:
    # Submit all stage_in / inference / stage_out pipelines to run in parallel:
    futures = {pool.submit(run_inference, shard): shard for shard in shards}
    for future in as_completed(futures):
        shard = futures[future]
        result = future.result()
        print(f"[green]{shard.name}[/green] completed: {result}")
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
print(client.list_endpoints()["clusters"]["sophia"])

# Get an OpenAI API client for an ALCF cluster:
oai = client.clusters("sophia").openai
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
stagein = client.stage_in(collection_id, dataset_path, dataset_path.name)

# Submit SAM3 inference:
resp = client.sam3.submit_batch(
    STAGING_COLLECTION_ROOT + str(stagein.destination_path)
)

# Wait for inference:
result = client.sam3.poll_task_result(resp.task_id)

# Copy results back:
client.stage_out(
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
