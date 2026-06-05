import io
import json
import logging
import tarfile
from concurrent.futures import ProcessPoolExecutor, wait
from io import BytesIO
from math import ceil
from pathlib import Path
from typing import Any

import numpy as np
import numpy.typing as npt
import smart_open
import typer
from PIL.Image import Image, fromarray
from PIL.Image import open as imopen

from .auth import STAGING_COLLECTION_ROOT
from .resources.sam3 import Sam3BatchResult, Sam3ImageResult

NDArray = npt.NDArray[Any]

logger = logging.getLogger(__name__)

cli = typer.Typer(no_args_is_help=True)

COLORS = np.array(
    [
        [0.9181167412189333, 0.15919377061073092, 0.11266724590330243],
        [0.2426219269955917, 0.48112269889334175, 0.6300907349521442],
        [0.6801395198918061, 0.8243199236330475, 0.1774045944122353],
        [0.49107573382331965, 0.12463080139992075, 0.6454314740086482],
        [0.6987104631708523, 0.5937358967283586, 0.3546217817852228],
        [0.5019010773320838, 0.11971558737792043, 0.28576675725477],
        [0.29243644808061936, 0.9238077273791944, 0.6538220475380706],
        [0.2586205918865394, 0.9470123824979256, 0.14729039742890707],
        [0.5550243221210623, 0.5246700043349789, 0.6434082461812896],
        [0.451148867503468, 0.3717198279094181, 0.6157734521389989],
        [0.20944722494667917, 0.08279581671195499, 0.2551450164828858],
        [0.22621543260836652, 0.17443115982799293, 0.8931335183945011],
        [0.2151999962505184, 0.7196045516977536, 0.7610355803084937],
        [0.16802189556009375, 0.5235718347378724, 0.10018495359215421],
        [0.923169872253808, 0.17919414345005819, 0.6756018070768858],
        [0.6398636341016658, 0.26039053450112304, 0.12587290510733998],
        [0.883578524489999, 0.1258989287053571, 0.9381302231143882],
        [0.34475212142314654, 0.2476343873941264, 0.16974966534928532],
        [0.2215159259189658, 0.3735720552568507, 0.8860350713062034],
        [0.65092947010467, 0.8755035668138884, 0.8467078897265947],
    ]
)


def add_member(tf: tarfile.TarFile, filename: str, buf: BytesIO) -> None:
    info = tarfile.TarInfo(filename)
    buf.seek(0, io.SEEK_END)
    info.size = buf.tell()

    buf.seek(0)
    tf.addfile(info, buf)


def quantile_norm(image: NDArray | Image) -> NDArray:
    """
    Converts floating dtype images to uint8-quantized npy format with a channel
    dimension.

    SAM3 preprocessing expects 1 or 3 channel dims and will squash intensities
    in a smaller dynamic range.
    """
    if isinstance(image, Image):
        if image.mode == "RGBA":
            image = image.convert("RGB")
        image = np.array(image)

    # Vision models expect a color channel:
    if image.ndim == 2:
        image = image[np.newaxis, :, :]

    assert image.ndim == 3

    # If floating dtype, normalize and clamp pixel intensities such that
    # the 1st percentile is 0 and 99th percentile is 255

    # N.B. this is required to get any results out of float32 TIFFs but can
    # become a CPU-intensive bottleneck. Handle it here to be robust, but
    # suggest quantizing on the client to cut the size down BEFORE
    # sending over the network!

    # Also, clients may wish to use the intensity range over an entire volume,
    # not per-slice, which can only be done by computing percentiles ahead of
    # time.
    if not np.issubdtype(image.dtype, np.integer):
        lo, hi = np.percentile(image, (1, 99))
        image = np.clip((image - lo) / (hi - lo), 0, 1)
        assert isinstance(image, np.ndarray)
        image = (image * 255).astype(np.uint8)

    return image


def plot_bbox(
    img_height: int | float,
    img_width: int | float,
    box: list[float],
    box_format: str = "XYXY",
    relative_coords: bool = True,
    color: str = "r",
    linestyle: str = "solid",
    text: str | None = None,
    ax: Any = None,
) -> None:
    import matplotlib.patches as patches
    import matplotlib.pyplot as plt

    if box_format == "XYXY":
        x, y, x2, y2 = box
        w = x2 - x
        h = y2 - y
    elif box_format == "XYWH":
        x, y, w, h = box
    elif box_format == "CxCyWH":
        cx, cy, w, h = box
        x = cx - w / 2
        y = cy - h / 2
    else:
        raise RuntimeError(f"Invalid box_format {box_format}")

    if relative_coords:
        x *= img_width
        w *= img_width
        y *= img_height
        h *= img_height

    if ax is None:
        ax = plt.gca()
    rect = patches.Rectangle(
        (x, y),
        w,
        h,
        linewidth=1.5,
        edgecolor=color,
        facecolor="none",
        linestyle=linestyle,
    )
    ax.add_patch(rect)
    if text is not None:
        facecolor = "w"
        ax.text(
            x,
            y - 5,
            text,
            color=color,
            fontsize=8,
            bbox={"facecolor": facecolor, "alpha": 0.75, "pad": 2},
        )


def to_pil(arr: NDArray) -> Image:
    # Handle dtype first
    if arr.dtype != np.uint8:
        arr = arr.astype(np.uint8)

    if arr.ndim == 2:
        return fromarray(arr)  # (H, W) grayscale

    if arr.ndim == 3:
        # Channel-first heuristic: first dim is small AND last dim is large
        if arr.shape[0] in (1, 3, 4) and arr.shape[2] not in (1, 3, 4):
            arr = arr.transpose(1, 2, 0)

        # Squeeze single-channel
        if arr.shape[-1] == 1:
            arr = arr.squeeze(-1)

        return fromarray(arr)

    raise ValueError(f"Unexpected array shape: {arr.shape}")


def preview_sam3_result(arr: NDArray, result: Sam3ImageResult, path: Path) -> None:
    import matplotlib.pyplot as plt
    from matplotlib.colors import to_rgb

    plt.figure(figsize=(12, 8))

    image = to_pil(arr)
    width, height = image.size
    plt.imshow(image, cmap="gray")

    labelmap = result.labelmap_npy
    assert labelmap.shape == (height, width), f"{labelmap.shape=} {image.size=}"
    overlay = np.zeros((height, width, 4), dtype=np.float32)

    for i in range(result.num_objects):
        color = COLORS[i % len(COLORS)]

        mask = labelmap == (i + 1)
        rgb = to_rgb(color)
        overlay[mask, :3] = rgb
        overlay[mask, 3] = 0.5

        plot_bbox(
            height,
            width,
            result.boxes[i],
            text=None,
            box_format="XYXY",
            color=color,
            relative_coords=False,
        )

    ax = plt.gca()
    ax.imshow(overlay)
    plt.savefig(path)
    plt.close()


def load_image(img_path: Path) -> NDArray:
    import tifffile

    image: NDArray | Image
    if img_path.suffix in (".tif", ".tiff"):
        image = tifffile.imread(img_path)
    else:
        image = imopen(img_path)
    return quantile_norm(image)


def to_npy(arr: NDArray) -> BytesIO:
    buf = BytesIO()
    np.save(buf, arr)
    buf.seek(0)
    return buf


def write_wds_shard(
    tar_path: Path, image_paths: list[Path], text_prompts: list[str]
) -> None:
    """
    Create a tar (aka WebDataset) of paired .npy/.json prompts to SAM3.
    """
    prompt_json = BytesIO(
        json.dumps({"prompts": [{"text": tp} for tp in text_prompts]}).encode()
    )

    logger.info(f"Writing {len(image_paths)} images to shard: {tar_path}")
    with tarfile.open(tar_path, mode="w") as writer:
        for path in sorted(image_paths):
            add_member(writer, path.with_suffix(".npy").name, to_npy(load_image(path)))
            add_member(writer, path.with_suffix(".json").name, prompt_json)


@cli.command()
def submit_batch(
    dataset_path: Path,
    from_collection_id: str | None = None,
    weights_dir_override: Path | None = None,
) -> None:
    """
    Submit a WebDataset-structured tar file for batch inference.
    """
    from .cli import _cli_state

    client = _cli_state["client"]

    dataset_path = dataset_path.expanduser().resolve()

    logger.info(f"Staging in {dataset_path}")
    stagein = client.stage_in(
        dataset_path, Path(dataset_path.name), from_collection_id=from_collection_id
    )
    logger.info(f"Stage in complete: {stagein}")

    logger.info("Sending inference request...")
    resp = client.sam3.submit_batch(
        STAGING_COLLECTION_ROOT + str(stagein.destination_path),
        weights_dir_override=weights_dir_override,
    )

    logger.info(f"Polling on inference task {resp.task_id!r}...")
    result = client.sam3.poll_task_result(resp.task_id)
    assert isinstance(result, Sam3BatchResult)
    logger.info(f"Inference completed: {result}")

    logger.info(f"Staging out result file: {result.result_path}")
    if from_collection_id:
        stageout = client.stage_out(
            from_collection_id,
            Path(Path(result.result_path).name),
            dataset_path.with_suffix(".results.tar"),
        )
        logger.info(f"Stage out complete: {stageout}")


@cli.command()
def submit_image(
    image_uri: str,
    prompt: str,
    save_preview: Path | None = None,
) -> None:
    from .cli import _cli_state

    client = _cli_state["client"]
    logger.info("Sending request...")

    if "://" not in image_uri and Path(image_uri).is_file():
        logger.info(f"{image_uri} is a local file; staging in...")
        stagein = client.stage_in(Path(image_uri), Path(Path(image_uri).name))
        process_uri = STAGING_COLLECTION_ROOT + str(stagein.destination_path)
    else:
        process_uri = image_uri

    resp = client.sam3.submit_image(process_uri, prompt)
    result = client.sam3.poll_task_result(resp.task_id)
    assert isinstance(result, Sam3ImageResult)
    logger.info(result.model_dump(exclude={"labelmap_npy"}))

    if save_preview and result.num_objects > 0:
        logger.info("Generating local preview of segmentation results...")
        with smart_open.open(image_uri, "rb") as fp:
            image = quantile_norm(imopen(fp))
            preview_sam3_result(image, result, save_preview)
            logger.info(f"Saved segmentation result preview to {save_preview}")


@cli.command()
def preview_batch_results(input_tar: Path, result_tar: Path) -> None:
    preview_dir = result_tar.with_suffix(".preview")
    preview_dir.mkdir(exist_ok=True)

    with tarfile.open(result_tar) as tf, tarfile.open(input_tar) as img_tf:
        jsons = [f.name for f in tf if f.isfile() and f.name.endswith(".json")]
        for fname in jsons:
            assert (result_fp := tf.extractfile(fname)) is not None

            result = json.load(result_fp)

            if result["num_objects"] < 1:
                logger.info(f"Skipping {fname}: no objects detected.")
                continue

            assert (img_fp := img_tf.extractfile(result["input_image"])) is not None
            image = quantile_norm(np.load(BytesIO(img_fp.read())))

            assert (
                label_fp := tf.extractfile(fname.replace(".json", ".labels.npy"))
            ) is not None
            labelmap = np.load(BytesIO(label_fp.read()))

            result = Sam3ImageResult(**result, labelmap_npy=labelmap)

            png_path = preview_dir / Path(fname).with_suffix(".png").name
            logger.info(f"Generating preview: {png_path.name} ...")

            preview_sam3_result(image, result, png_path)
            logger.info(f"Saved preview: {png_path}")


@cli.command()
def create_webdataset(
    image_dir: Path,
    image_ext: str,
    text_prompts: list[str],
    *,
    output_dir: Path | None = None,
    shard_size: int = 100,
    num_workers: int = 4,
) -> None:
    """
    Bundle images+text prompts into WebDataset tar archives.
    Example:
    sam3-service create-webdataset ./images/ .tiff "granule" "white shape"
    """
    if output_dir is None:
        output_dir = image_dir.with_name(f"{image_dir.name}-webdataset-shards")

    output_dir = Path(output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    image_ext = image_ext.strip(".")
    source_paths = list(Path(image_dir).glob(f"*.{image_ext}"))
    if not source_paths:
        raise RuntimeError(f"No '.{image_ext}' files found in {image_dir}")

    assert len(text_prompts) > 0

    num_shards = ceil(len(source_paths) / shard_size)
    shards = [
        source_paths[i * shard_size : (i + 1) * shard_size] for i in range(num_shards)
    ]

    logger.info(f"Writing {num_shards=} using {num_workers=}")
    with ProcessPoolExecutor(max_workers=num_workers) as pool:
        futs = []
        for i, shard in enumerate(shards):
            tar_path = output_dir / f"shard-{i:05d}.tar"
            futs.append(pool.submit(write_wds_shard, tar_path, shard, text_prompts))

        logger.info("Waiting for shard writes to finish...")
        wait(futs)
        logger.info("Done!")
