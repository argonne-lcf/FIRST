# ALCF AI Inference Service SDK

`alcf-ai` is the Python SDK and command line tool for the ALCF Inference
Gateway.  It authenticates with Globus, discovers the models you can use, and
sends requests to the gateway's OpenAI-compatible and Anthropic-compatible
endpoints.

```bash
# Log in with Globus
uvx alcf-tokens login

# See what you can use:
uvx alcf-ai models ls

# Chat (streams by default):
uvx alcf-ai chat -m openai/gpt-oss-120b "How do I know Pi is irrational? Be concise."
```

## Command line usage

### Auth

Logging in is handled by [`alcf-tokens`](https://pypi.org/project/alcf-tokens/),
the shared ALCF CLI: one interactive login issues tokens for several ALCF services, and
`alcf-ai` reads the tokens it cached.

```bash
uvx alcf-tokens login                  # interactive Globus login (cached refresh token)
uvx alcf-tokens test-token inference   # check the gateway accepts your token
token=$(uvx alcf-tokens get-token inference)   # for curl / other SDKs

uvx alcf-ai whoami                     # the identity the gateway sees
```

If you will stage data in or out, authorize those collections in the same
login.  The one-liner that covers everything below is:

```bash
uvx alcf-tokens login \
  --authorize-transfer home \
  --authorize-transfer 96c7390b-a3e8-4dd4-a327-1af7d143283e:https
```

See [Data staging](#data-staging) for what those arguments mean.

Use the token with any OpenAI-compatible tool:

```bash
curl -H "Authorization: Bearer $token" \
  https://inference-api.alcf.anl.gov/federated/v1/models | jq
```

### Discovering models

```bash
uvx alcf-ai models ls                      # endpoints, context window, live state
uvx alcf-ai models ls --endpoint responses # only models supporting an endpoint
uvx alcf-ai models get openai/gpt-oss-120b # limits, capabilities, deployments
uvx alcf-ai deployments ls                 # every deployment with its state
uvx alcf-ai deployments get sophia/openai/gpt-oss-120b   # replicas of a pilot deployment
uvx alcf-ai clusters ls
uvx alcf-ai access-groups ls
```

Every listing command accepts `--json` to print the raw catalog records.

### Cold starts

Models that run in HPC jobs start on demand.  A request to a model with no
running replica gets a `503` that carries a startup ETA; `alcf-ai` shows the
countdown and retries automatically (`--no-wait` to fail fast instead).

To trigger a start and wait for it explicitly:

```bash
uvx alcf-ai models warmup openai/gpt-oss-120b
# ⠋ openai/gpt-oss-120b is starting · ready in ~137s
# openai/gpt-oss-120b is ready (waited 141s)
```

### Chat with an LLM

```bash
uvx alcf-ai chat --help

# Chat Completions (default), Responses, or Anthropic Messages:
uvx alcf-ai chat -m openai/gpt-oss-120b --api responses "Summarize the plot of Dune."
uvx alcf-ai chat -m openai/gpt-oss-120b --api messages --show-reasoning "Why is the sky blue?"

# Non-streaming renders Markdown; --json prints the raw response:
uvx alcf-ai chat -m google/gemma-4-31B-it --no-stream --temp 0.3 -n 200 "What is KL divergence?"

# Pipe a file in, apply an instruction, pin a deployment:
cat notes.md | uvx alcf-ai chat -s "Fix the grammar" -d sophia/openai/gpt-oss-120b

# Embeddings:
uvx alcf-ai embed -m my/embedding-model "first text" "second text"
```

Without `--model`, the `ALCF_AI_MODEL` environment variable or the first
chat-capable model in the catalog is used.  Without `--api`, the first dialect
the model supports is used (Chat Completions, then Responses, then Messages).

### Non-LLM task models

Models such as segmentation services expose task endpoints instead of LLM
endpoints.  They are invoked through the gateway's generic `tasks` route, which
forwards the request body verbatim to the backend's `/v1/{endpoint}` and
returns its result:

```bash
uvx alcf-ai tasks ls
uvx alcf-ai tasks run sam3 promptable-segmentation \
  -p inference_type=single-image \
  -p data_uri=https://example.org/groceries.jpg \
  -p single_image_prompt=Baguette
uvx alcf-ai tasks run sam3 promptable-segmentation --json @request.json
```

### Data staging

Task models read inputs from (and write results to) a per-user directory on
the inference data-staging Globus collection (`IRIBeta`, under
`/eagle/IRIBeta/inference_data_staging/`).  Single files upload over HTTPS;
folders and large datasets move with Globus Transfer.  Both need the relevant
collections authorized at login, with `--authorize-transfer`.

Each entry is a collection UUID (or a known alias) plus the scopes you need on
it: `:https` to read and write files on it directly over HTTPS, and
`:data_access` for collections that require that scope for Transfer.  The
[alcf-tokens README](https://github.com/argonne-lcf/alcf-tokens#authorizing-collections)
documents both in full.

**Uploading local files** only needs the staging collection, with `:https`:

```bash
STAGING=96c7390b-a3e8-4dd4-a327-1af7d143283e   # IRIBeta inference_data_staging
uvx alcf-tokens login --authorize-transfer $STAGING:https
```

**Transferring from your ALCF home directory to IRIBeta** -- the usual way to
stage a folder of inputs -- authorizes both collections in one login.  `home`
is an alias `alcf-tokens` already knows (as are `eagle` and `flare`), and it
expands to the collection UUID with `:data_access`:

```bash
uvx alcf-tokens login \
  --authorize-transfer home \
  --authorize-transfer $STAGING:https
```

Then transfer from it by passing the home collection to `stage_in`:

```python
HOME_COLLECTION = "9032dd3a-e841-4687-a163-2720da731b5b"
stagein = client.staging.stage_in(
    Path("/home/you/inputs"), Path("inputs"),
    from_collection_id=HOME_COLLECTION, recursive=True,
)
```

For any other collection, pass its UUID instead, appending `:data_access` only
if it requires that scope:

```bash
uvx alcf-tokens login \
  --authorize-transfer $SOURCE_COLLECTION:data_access \
  --authorize-transfer $STAGING:https
```

If a consent is missing, `alcf-ai` fails with the exact `alcf-tokens login`
command that would grant it.

Re-running `alcf-tokens login` with more `--authorize-transfer` entries
re-consents with the wider set; list every collection you want authorized in
the same command.

### Segment images with SAM3

Segment a single image by text prompt with [Meta SAM3](https://github.com/facebookresearch/sam3).
Local files are staged in for you; URIs are opened on the cluster:

```bash
uvx alcf-ai sam3 submit-image \
  https://raw.githubusercontent.com/masalim2/sam3-service/refs/heads/main/examples/images/groceries.jpg \
  "Baguette" -o result.json --save-labelmap labels.npy

# Render masks + boxes to a PNG (numpy/matplotlib live in the script, not the SDK):
uv run scripts/sam3_preview.py image groceries.jpg result.json labels.npy preview.png
```

For high throughput, bundle images and prompts into WebDataset shards with
`scripts/sam3_create_webdataset.py`, then submit each shard:

```bash
uv run scripts/sam3_create_webdataset.py /path/to/tiff-stack .tiff \
    "Phloem Fibers" "Hydrated Xylem vessels" "Air-based Pith cells" \
    --output-dir test-wds --shard-size 100 --num-workers 4

SAM3_FINETUNE=/eagle/inference_service/sam3-service/weights/synaps-i
for f in test-wds/*.tar; do
  uvx alcf-ai sam3 submit-batch $f --from-collection-id $SOURCE_COLLECTION \
      --weights-dir-override $SAM3_FINETUNE >> batch-inference.log 2>&1 &
done
wait

uv run scripts/sam3_preview.py batch test-wds/shard-00004.tar test-wds/shard-00004.results.tar
```

### Segment images with DINOv3

[DINOv3](https://github.com/facebookresearch/dinov3) segments a **whole folder
of images at once**: the folder is staged in with Globus Transfer, the GPU
dataloader batches over every image in it, and the results folder (semantic
masks, plus optional overlays) is staged back out.

```bash
uvx alcf-ai dinov3 submit /path/to/image-folder \
  --from-collection-id $SOURCE_COLLECTION \
  --save-overlay
```

One folder is one request, so shard large datasets into subfolders and submit
them concurrently (see the SDK example below).

Both wrappers take `--model` to target a differently named catalog entry, and
any extra request fields pass straight through to the backend.

### Coding agents

Register the gateway as a model provider in opencode, pi, Codex, or Claude
Code.  Model entries carry the context window and capabilities published by
the catalog; the first `-m` model is the agent's default:

```bash
uvx alcf-ai agent configure opencode
uvx alcf-ai agent configure codex -m openai/gpt-oss-120b
uvx alcf-ai agent configure pi
uvx alcf-ai agent configure claude -m openai/gpt-oss-120b
```

Agents refresh their access token by running `alcf-tokens get-token inference`
(or `alcf-ai auth get-access-token`); run `alcf-tokens login` when the refresh
token expires.  If neither command can be found, the current token is embedded
instead and you must re-run `agent configure` when it expires.

Set `ALCF_SESSION_ID` to send an `X-ALCF-Session-ID` header, which lets
backends reuse the prompt cache across a session's requests.  Claude Code
cannot read it from settings, so export
`ANTHROPIC_CUSTOM_HEADERS="X-ALCF-Session-ID: $ALCF_SESSION_ID"` instead.

### Installing the latest client version

```bash
uvx alcf-ai@latest version
```

## SDK usage

The HTTP client allows 900 seconds of inactivity per response read, including
silent model prefill; connect, write and connection-pool waits remain 10 seconds.
This is not a total request deadline. The CLI's `chat --timeout` controls how
long to wait through a **cold start**, not this HTTP read limit. If configuring
the HTTP timeout explicitly, use the supported constructor argument:

```python
from httpx import Timeout
from alcf_ai import InferenceClient

client = InferenceClient(timeout=Timeout(10.0, read=900.0))
```

A gateway, proxy or another SDK can still impose a shorter timeout. One measured
655-second prefill fits this budget; concurrent long prefills need separate
qualification and are not guaranteed to finish prefill before it expires.

```bash
uv run --with alcf-ai python
```

```python
from alcf_ai import InferenceClient

client = InferenceClient()  # uses the cached Globus login

# Catalog:
for model in client.catalog.models():
    print(model.name, model.supported_endpoints, model.max_model_len)

# Chat Completions / Responses / Anthropic Messages, unary or streaming:
body = client.inference.chat_completions(
    "openai/gpt-oss-120b", [{"role": "user", "content": "Hello there!"}]
)
for event in client.inference.responses("openai/gpt-oss-120b", "Hello", stream=True):
    print(event.json())

# Pin a deployment instead of letting the gateway choose:
client.deployment("sophia/openai/gpt-oss-120b").messages(
    "openai/gpt-oss-120b", [{"role": "user", "content": "Hi"}], max_tokens=64
)

# Non-LLM task (any fields pass through to the backend):
result = client.inference.task("sam3", "promptable-segmentation", data_uri="...", inference_type="single-image")

# Wait through a cold start, reporting the ETA:
client.inference.warmup("openai/gpt-oss-120b", on_status=lambda s: print(s.eta_remaining_sec))
```

Errors are raised as the same typed exceptions the gateway uses
(`first_common.errors`): a cold-start `ServiceUnavailable` carries
`info["startup_eta_sec"]`, and `alcf_ai.StartupStatus.from_error(exc)` turns it
into a countdown.

### Data staging and task models

```python
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from alcf_ai import InferenceClient

client = InferenceClient()
collection_id = "your globus collection UUID"

# SAM3 on a single image, uploaded over HTTPS:
stagein = client.staging.stage_in(Path("photo.jpg"), Path("photo.jpg"))
result = client.sam3.submit_image(client.staging.remote_path(stagein), "Baguette")
result.save_labelmap(Path("labels.npy"))

# DINOv3 over pre-sharded subfolders, transferred with Globus and run concurrently:
def run_shard(shard_dir: Path) -> str:
    stagein = client.staging.stage_in(
        shard_dir, Path(shard_dir.name), from_collection_id=collection_id, recursive=True
    )
    result = client.dinov3.submit(client.staging.remote_path(stagein), save_overlay=True)
    results_dirname = Path(result.mask_dir).parent.name
    client.staging.stage_out(
        collection_id, Path(results_dirname), shard_dir.with_name(results_dirname), recursive=True
    )
    return result.mask_dir

shards = sorted(p for p in Path("/path/to/dataset").iterdir() if p.is_dir())
with ThreadPoolExecutor(max_workers=8) as pool:
    for future in as_completed([pool.submit(run_shard, s) for s in shards]):
        print(future.result())
```

### Official SDK clients

```python
oai = client.openai            # openai.OpenAI bound to /federated/v1, reusing your auth
oai.chat.completions.create(model="openai/gpt-oss-120b", messages=[...])

anthropic = client.anthropic   # requires `pip install anthropic`
anthropic.messages.create(model="openai/gpt-oss-120b", max_tokens=64, messages=[...])
```

## Using an alternate service URL

The client defaults to <https://inference-api.alcf.anl.gov>.  Override it with:

1. the `inference_base_url` environment variable (also read from `.env`),
2. `alcf-ai --base-url http://localhost:8000 ...` on the command line, or
3. `InferenceClient(base_url="...")` in Python.
