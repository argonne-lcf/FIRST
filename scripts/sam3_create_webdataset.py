# /// script
# requires-python = ">=3.12"
# dependencies = ["numpy", "pillow", "tifffile"]
# ///
"""
Bundle images and text prompts into WebDataset tar shards for SAM3 batch
inference (`alcf-ai sam3 submit-batch`).

Each image is stored as a uint8 .npy array next to a .json listing the prompts.
Floating-point images (e.g. float32 TIFFs) are percentile-normalized to uint8
here, on the client, so the shards stay small on the wire.

    uv run scripts/sam3_create_webdataset.py ./images .tiff "granule" "white shape" \\
        --output-dir ./images-wds --shard-size 100 --num-workers 4
"""

import argparse
import io
import json
import tarfile
from concurrent.futures import ProcessPoolExecutor, wait
from io import BytesIO
from math import ceil
from pathlib import Path
from typing import Any

import numpy as np
import numpy.typing as npt
from PIL.Image import Image
from PIL.Image import open as imopen

NDArray = npt.NDArray[Any]


def quantile_norm(image: NDArray | Image) -> NDArray:
    """
    Convert an image to a uint8 array with a leading channel dimension.

    SAM3 preprocessing expects 1 or 3 channels and squashes small dynamic
    ranges, so floating dtypes are clipped to the 1st..99th percentile and
    scaled to 0..255.  Compute percentiles over a whole volume beforehand if
    per-slice normalization is not what you want.
    """
    if isinstance(image, Image):
        if image.mode == "RGBA":
            image = image.convert("RGB")
        image = np.array(image)

    if image.ndim == 2:
        image = image[np.newaxis, :, :]
    assert image.ndim == 3, f"Expected 2D or 3D image, got shape {image.shape}"

    if not np.issubdtype(image.dtype, np.integer):
        lo, hi = np.percentile(image, (1, 99))
        image = np.clip((image - lo) / (hi - lo), 0, 1)
        image = (image * 255).astype(np.uint8)
    return image


def load_image(path: Path) -> NDArray:
    import tifffile

    image: NDArray | Image
    if path.suffix.lower() in (".tif", ".tiff"):
        image = tifffile.imread(path)
    else:
        image = imopen(path)
    return quantile_norm(image)


def add_member(tf: tarfile.TarFile, filename: str, buf: BytesIO) -> None:
    info = tarfile.TarInfo(filename)
    buf.seek(0, io.SEEK_END)
    info.size = buf.tell()
    buf.seek(0)
    tf.addfile(info, buf)


def to_npy(arr: NDArray) -> BytesIO:
    buf = BytesIO()
    np.save(buf, arr)
    buf.seek(0)
    return buf


def write_shard(tar_path: Path, image_paths: list[Path], prompts: list[str]) -> None:
    prompt_json = json.dumps({"prompts": [{"text": p} for p in prompts]}).encode()
    print(f"Writing {len(image_paths)} images to {tar_path}")
    with tarfile.open(tar_path, mode="w") as writer:
        for path in sorted(image_paths):
            add_member(writer, path.with_suffix(".npy").name, to_npy(load_image(path)))
            add_member(writer, path.with_suffix(".json").name, BytesIO(prompt_json))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("image_dir", type=Path)
    parser.add_argument("image_ext", help="e.g. .tiff or .png")
    parser.add_argument(
        "prompts", nargs="+", help="Text prompts applied to every image"
    )
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--shard-size", type=int, default=100)
    parser.add_argument("--num-workers", type=int, default=4)
    args = parser.parse_args()

    output_dir = args.output_dir or args.image_dir.with_name(
        f"{args.image_dir.name}-webdataset-shards"
    )
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    ext = args.image_ext.strip(".")
    sources = sorted(Path(args.image_dir).glob(f"*.{ext}"))
    if not sources:
        raise SystemExit(f"No '.{ext}' files found in {args.image_dir}")

    num_shards = ceil(len(sources) / args.shard_size)
    shards = [
        sources[i * args.shard_size : (i + 1) * args.shard_size]
        for i in range(num_shards)
    ]
    print(f"Writing {num_shards} shard(s) with {args.num_workers} worker(s)")
    with ProcessPoolExecutor(max_workers=args.num_workers) as pool:
        futures = [
            pool.submit(
                write_shard, output_dir / f"shard-{i:05d}.tar", shard, args.prompts
            )
            for i, shard in enumerate(shards)
        ]
        wait(futures)
        for f in futures:
            f.result()
    print(f"Done: {output_dir}")


if __name__ == "__main__":
    main()
