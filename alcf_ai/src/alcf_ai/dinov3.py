import logging
from pathlib import Path

import typer

from .auth import STAGING_COLLECTION_ROOT

logger = logging.getLogger(__name__)

cli = typer.Typer(no_args_is_help=True)


@cli.command()
def submit(
    input_dir: Path = typer.Argument(..., help="Path to a folder of images to segment"),
    origin_collection_id: str = typer.Option(
        ...,
        help="Globus collection ID to stage the input folder in from "
        "(required: folder transfers use Globus, not HTTPS upload)",
    ),
    output_dir: Path | None = typer.Option(
        None,
        help="Local destination to stage the results folder into "
        "(default: the service-named results folder, next to the input)",
    ),
    project: str | None = typer.Option(None, help="DINOv3 project/class set"),
    checkpoint: str | None = typer.Option(
        None, help="Override the server's default model checkpoint"
    ),
    save_overlay: bool = typer.Option(
        False, help="Also render color overlay images alongside the masks"
    ),
    batch_size: int | None = typer.Option(None, help="GPU dataloader batch size"),
    num_workers: int | None = typer.Option(None, help="Dataloader worker count"),
    timeout: int = typer.Option(300, help="Seconds to poll for inference completion"),
) -> None:
    """
    Stage in a folder of images, run DINOv3 segmentation, and stage out the
    results folder (semantic masks, and overlays if requested).

    The GPU dataloader batches over every image in the folder, so a single
    submission is the unit of parallelism -- shard large datasets into
    subfolders and submit them concurrently for higher throughput.
    """
    from .cli import _cli_state

    client = _cli_state["client"]

    input_dir = input_dir.expanduser().resolve()

    logger.info(f"Staging in folder {input_dir}")
    stagein = client.stage_in(
        input_dir,
        Path(input_dir.name),
        from_collection_id=origin_collection_id,
        recursive=True,
    )
    logger.info(f"Stage in complete: {stagein}")

    remote_input = STAGING_COLLECTION_ROOT + str(stagein.destination_path)

    logger.info("Submitting DINOv3 inference request...")
    resp = client.dinov3.submit(
        input_dir=remote_input,
        project=project,
        checkpoint=checkpoint,
        save_overlay=save_overlay or None,
        batch_size=batch_size,
        num_workers=num_workers,
    )

    logger.info(f"Polling on inference task {resp.task_id!r}...")
    result = client.dinov3.poll_task_result(resp.task_id, timeout=timeout)
    logger.info(f"Inference completed: {result}")

    # The service derives the results directory itself and reports it back as
    # mask_dir (== <results_dir>/semantic_masks). Stage out that directory.
    results_dirname = Path(result["mask_dir"]).parent.name
    if output_dir is None:
        output_dir = input_dir.with_name(results_dirname)
    output_dir = output_dir.expanduser().resolve()
    logger.info(f"Staging out results folder to {output_dir}")
    stageout = client.stage_out(
        origin_collection_id,
        Path(results_dirname),
        output_dir,
        recursive=True,
    )
    logger.info(f"Stage out complete: {stageout}")
