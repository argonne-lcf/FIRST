"""
`alcf-ai dinov3`: DINOv3 folder segmentation through the generic tasks route.
"""

import logging
from pathlib import Path
from typing import Annotated

import typer

from ..dinov3 import DEFAULT_MODEL
from ._shared import get_client, print_json, with_startup_wait

logger = logging.getLogger(__name__)

cli = typer.Typer(no_args_is_help=True)


@cli.command()
def submit(
    ctx: typer.Context,
    input_dir: Annotated[Path, typer.Argument(help="Folder of images to segment.")],
    from_collection_id: Annotated[
        str,
        typer.Option(
            help="Globus collection to stage the folder in from "
            "(required: folder transfers use Globus, not HTTPS upload)."
        ),
    ],
    to_collection_id: Annotated[
        str | None,
        typer.Option(help="Collection to stage results to (default: same as input)."),
    ] = None,
    output_dir: Annotated[
        Path | None,
        typer.Option(
            help="Destination for the results folder "
            "(default: the service-named results folder, next to the input)."
        ),
    ] = None,
    model: Annotated[
        str, typer.Option("--model", "-m", help="Catalog name of the DINOv3 model.")
    ] = DEFAULT_MODEL,
    project: Annotated[
        str | None, typer.Option(help="DINOv3 project/class set.")
    ] = None,
    checkpoint: Annotated[
        str | None, typer.Option(help="Override the server's default checkpoint.")
    ] = None,
    save_overlay: Annotated[
        bool, typer.Option(help="Also render color overlays alongside the masks.")
    ] = False,
    batch_size: Annotated[
        int | None, typer.Option(help="GPU dataloader batch size.")
    ] = None,
    num_workers: Annotated[
        int | None, typer.Option(help="Dataloader worker count.")
    ] = None,
    wait: Annotated[
        bool,
        typer.Option(
            "--wait/--no-wait", help="Wait through a cold start (shows the ETA)."
        ),
    ] = True,
) -> None:
    """
    Stage in a folder of images, run DINOv3 segmentation, and stage out the
    results folder (semantic masks, and overlays if requested).

    One folder is one inference request; shard large datasets into subfolders
    and submit them concurrently for higher throughput.
    """
    client = get_client(ctx)
    input_dir = input_dir.expanduser().resolve()

    logger.info(f"Staging in folder {input_dir}")
    stagein = client.staging.stage_in(
        input_dir,
        Path(input_dir.name),
        from_collection_id=from_collection_id,
        recursive=True,
    )
    logger.info(f"Stage in complete: {stagein}")

    result = with_startup_wait(
        lambda: client.dinov3.submit(
            client.staging.remote_path(stagein),
            project=project,
            checkpoint=checkpoint,
            save_overlay=save_overlay or None,
            batch_size=batch_size,
            num_workers=num_workers,
            model=model,
        ),
        model=model,
        wait=wait,
        timeout=1800,
    )
    logger.info(f"Inference completed: {result}")

    # The service derives the results directory itself and reports it back as
    # mask_dir (== <results_dir>/semantic_masks). Stage out that directory.
    results_dirname = Path(result.mask_dir).parent.name
    if output_dir is None:
        output_dir = input_dir.with_name(results_dirname)
    output_dir = output_dir.expanduser().resolve()
    logger.info(f"Staging out results folder to {output_dir}")
    stageout = client.staging.stage_out(
        to_collection_id or from_collection_id,
        Path(results_dirname),
        output_dir,
        recursive=True,
    )
    logger.info(f"Stage out complete: {stageout}")
    print_json(result)
