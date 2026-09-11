# /// script
# requires-python = ">=3.12"
# dependencies = ["numpy", "pillow", "tifffile", "matplotlib"]
# ///
"""
Render SAM3 segmentation results as PNG previews (masks + bounding boxes).

Single image, from `alcf-ai sam3 submit-image ... -o result.json --save-labelmap labels.npy`:

    uv run scripts/sam3_preview.py image ./photo.jpg result.json labels.npy preview.png

Batch results, from `alcf-ai sam3 submit-batch` (input and results tars):

    uv run scripts/sam3_preview.py batch shard-00004.tar shard-00004.results.tar
"""

import argparse
import json
import tarfile
import urllib.request
from io import BytesIO
from pathlib import Path
from typing import Any

import numpy as np
import numpy.typing as npt
from PIL.Image import Image, fromarray
from PIL.Image import open as imopen

NDArray = npt.NDArray[Any]

COLORS = np.array(
    [
        [0.918, 0.159, 0.113],
        [0.243, 0.481, 0.630],
        [0.680, 0.824, 0.177],
        [0.491, 0.125, 0.645],
        [0.699, 0.594, 0.355],
        [0.502, 0.120, 0.286],
        [0.292, 0.924, 0.654],
        [0.259, 0.947, 0.147],
        [0.555, 0.525, 0.643],
        [0.451, 0.372, 0.616],
        [0.209, 0.083, 0.255],
        [0.226, 0.174, 0.893],
        [0.215, 0.720, 0.761],
        [0.168, 0.524, 0.100],
        [0.923, 0.179, 0.676],
        [0.640, 0.260, 0.126],
        [0.884, 0.126, 0.938],
        [0.345, 0.248, 0.170],
        [0.222, 0.374, 0.886],
        [0.651, 0.876, 0.847],
    ]
)


def quantile_norm(image: NDArray | Image) -> NDArray:
    """uint8, channel-first array (see sam3_create_webdataset.py)."""
    if isinstance(image, Image):
        if image.mode == "RGBA":
            image = image.convert("RGB")
        image = np.array(image)
    if image.ndim == 2:
        image = image[np.newaxis, :, :]
    if not np.issubdtype(image.dtype, np.integer):
        lo, hi = np.percentile(image, (1, 99))
        image = np.clip((image - lo) / (hi - lo), 0, 1)
        image = (image * 255).astype(np.uint8)
    return image


def to_pil(arr: NDArray) -> Image:
    if arr.dtype != np.uint8:
        arr = arr.astype(np.uint8)
    if arr.ndim == 2:
        return fromarray(arr)
    if arr.ndim == 3:
        # Channel-first heuristic: first dim is small AND last dim is large
        if arr.shape[0] in (1, 3, 4) and arr.shape[2] not in (1, 3, 4):
            arr = arr.transpose(1, 2, 0)
        if arr.shape[-1] == 1:
            arr = arr.squeeze(-1)
        return fromarray(arr)
    raise ValueError(f"Unexpected array shape: {arr.shape}")


def load_image(source: str) -> NDArray:
    """Local path (any PIL/TIFF format) or http(s) URL."""
    if source.startswith(("http://", "https://")):
        with urllib.request.urlopen(source) as resp:
            return quantile_norm(imopen(BytesIO(resp.read())))
    path = Path(source)
    if path.suffix.lower() in (".tif", ".tiff"):
        import tifffile

        return quantile_norm(tifffile.imread(path))
    if path.suffix.lower() == ".npy":
        return quantile_norm(np.load(path))
    return quantile_norm(imopen(path))


def render(
    image: NDArray, boxes: list[list[float]], labelmap: NDArray, out: Path
) -> None:
    import matplotlib.patches as patches
    import matplotlib.pyplot as plt

    pil = to_pil(image)
    width, height = pil.size
    assert labelmap.shape == (height, width), f"{labelmap.shape=} vs {pil.size=}"

    plt.figure(figsize=(12, 8))
    plt.imshow(pil, cmap="gray")
    overlay = np.zeros((height, width, 4), dtype=np.float32)
    ax = plt.gca()
    for i, box in enumerate(boxes):
        color = COLORS[i % len(COLORS)]
        mask = labelmap == (i + 1)
        overlay[mask, :3] = color
        overlay[mask, 3] = 0.5
        x1, y1, x2, y2 = box
        ax.add_patch(
            patches.Rectangle(
                (x1, y1),
                x2 - x1,
                y2 - y1,
                linewidth=1.5,
                edgecolor=color,
                facecolor="none",
            )
        )
    ax.imshow(overlay)
    plt.savefig(out)
    plt.close()
    print(f"Saved {out}")


def preview_image(args: argparse.Namespace) -> None:
    result = json.loads(Path(args.result_json).read_text())
    labelmap = np.load(args.labelmap_npy)
    render(load_image(args.image), result["boxes"], labelmap, Path(args.out))


def preview_batch(args: argparse.Namespace) -> None:
    preview_dir = Path(args.result_tar).with_suffix(".preview")
    preview_dir.mkdir(exist_ok=True)
    with tarfile.open(args.result_tar) as tf, tarfile.open(args.input_tar) as img_tf:
        for member in tf:
            if not (member.isfile() and member.name.endswith(".json")):
                continue
            assert (fp := tf.extractfile(member)) is not None
            result = json.load(fp)
            if result["num_objects"] < 1:
                print(f"Skipping {member.name}: no objects detected.")
                continue
            assert (img_fp := img_tf.extractfile(result["input_image"])) is not None
            image = quantile_norm(np.load(BytesIO(img_fp.read())))
            labels_name = member.name.replace(".json", ".labels.npy")
            assert (label_fp := tf.extractfile(labels_name)) is not None
            labelmap = np.load(BytesIO(label_fp.read()))
            out = preview_dir / Path(member.name).with_suffix(".png").name
            render(image, result["boxes"], labelmap, out)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(required=True)

    image = sub.add_parser("image", help="Preview one submit-image result")
    image.add_argument("image", help="The image that was segmented (path or URL)")
    image.add_argument("result_json", help="Result written by `sam3 submit-image -o`")
    image.add_argument("labelmap_npy", help="Label map from `--save-labelmap`")
    image.add_argument("out", help="Output PNG path")
    image.set_defaults(func=preview_image)

    batch = sub.add_parser("batch", help="Preview every result in a batch shard")
    batch.add_argument("input_tar")
    batch.add_argument("result_tar")
    batch.set_defaults(func=preview_batch)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
