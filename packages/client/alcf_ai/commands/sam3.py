"""
`alcf-ai sam3`: SAM3 promptable segmentation through the generic tasks route.
"""

import logging
from pathlib import Path
from typing import Annotated

import typer
from rich.table import Table

from ..sam3 import DEFAULT_MODEL
from ._shared import console, err_console, get_client, print_json, with_startup_wait

logger = logging.getLogger(__name__)

cli = typer.Typer(no_args_is_help=True)

ModelOpt = Annotated[
    str, typer.Option("--model", "-m", help="Catalog name of the SAM3 model.")
]
WeightsOpt = Annotated[
    Path | None,
    typer.Option("--weights-dir-override", help="Fine-tuned weights on the cluster."),
]
WaitOpt = Annotated[
    bool,
    typer.Option("--wait/--no-wait", help="Wait through a cold start (shows the ETA)."),
]


@cli.command("submit-image")
def submit_image(
    ctx: typer.Context,
    image_uri: Annotated[
        str, typer.Argument(help="Local file, https/s3 URI, or cluster path.")
    ],
    prompt: Annotated[str, typer.Argument(help="Text prompt naming what to segment.")],
    model: ModelOpt = DEFAULT_MODEL,
    weights_dir_override: WeightsOpt = None,
    save_labelmap: Annotated[
        Path | None,
        typer.Option("--save-labelmap", help="Write the label map as a .npy file."),
    ] = None,
    output: Annotated[
        Path | None,
        typer.Option(
            "--output", "-o", help="Write the result (boxes, scores) as JSON."
        ),
    ] = None,
    as_json: Annotated[
        bool, typer.Option("--json", help="Print the result JSON.")
    ] = False,
    wait: WaitOpt = True,
) -> None:
    """
    Segment one image by text prompt.

    A local file is uploaded to your staging area first; URIs are opened on the
    serving cluster.  Preview the result with scripts/sam3_preview.py.
    """
    client = get_client(ctx)

    if "://" not in image_uri and Path(image_uri).is_file():
        logger.info(f"{image_uri} is a local file; staging in...")
        stagein = client.staging.stage_in(Path(image_uri), Path(Path(image_uri).name))
        data_uri = client.staging.remote_path(stagein)
    else:
        data_uri = image_uri

    result = with_startup_wait(
        lambda: client.sam3.submit_image(
            data_uri, prompt, weights_dir_override=weights_dir_override, model=model
        ),
        model=model,
        wait=wait,
        timeout=1800,
    )

    if save_labelmap is not None:
        result.save_labelmap(save_labelmap)
        err_console.print(f"[dim]Saved label map to {save_labelmap}[/]")
    if output is not None:
        output.write_text(result.model_dump_json(exclude={"labelmap_npy"}, indent=2))
        err_console.print(f"[dim]Saved result to {output}[/]")

    if as_json:
        print_json(result.model_dump(exclude={"labelmap_npy"}))
        return

    table = Table(title=f"{result.num_objects} object(s) for {prompt!r}")
    table.add_column("#", justify="right")
    table.add_column("Score", justify="right")
    table.add_column("Box (x1, y1, x2, y2)")
    for i, (score, box) in enumerate(zip(result.scores, result.boxes), start=1):
        table.add_row(str(i), f"{score:.3f}", ", ".join(f"{v:.0f}" for v in box))
    console.print(table)


@cli.command("submit-batch")
def submit_batch(
    ctx: typer.Context,
    dataset_path: Annotated[
        Path,
        typer.Argument(help="WebDataset tar (see scripts/sam3_create_webdataset.py)."),
    ],
    from_collection_id: Annotated[
        str | None,
        typer.Option(help="Globus collection holding the tar (default: HTTPS upload)."),
    ] = None,
    model: ModelOpt = DEFAULT_MODEL,
    weights_dir_override: WeightsOpt = None,
    wait: WaitOpt = True,
) -> None:
    """
    Stage in a WebDataset tar, run batch segmentation, and stage the results
    tar back next to the input (when a collection id is given).
    """
    client = get_client(ctx)
    dataset_path = dataset_path.expanduser().resolve()

    logger.info(f"Staging in {dataset_path}")
    stagein = client.staging.stage_in(
        dataset_path, Path(dataset_path.name), from_collection_id=from_collection_id
    )
    logger.info(f"Stage in complete: {stagein}")

    result = with_startup_wait(
        lambda: client.sam3.submit_batch(
            client.staging.remote_path(stagein),
            weights_dir_override=weights_dir_override,
            model=model,
        ),
        model=model,
        wait=wait,
        timeout=1800,
    )
    logger.info(f"Inference completed: {result}")

    if from_collection_id:
        stageout = client.staging.stage_out(
            from_collection_id,
            Path(Path(result.result_path).name),
            dataset_path.with_suffix(".results.tar"),
        )
        logger.info(f"Stage out complete: {stageout}")
    print_json(result)
