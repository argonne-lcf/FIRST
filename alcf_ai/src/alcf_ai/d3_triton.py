import logging
from pathlib import Path

import typer

from .auth import STAGING_COLLECTION_ROOT

logger = logging.getLogger(__name__)

cli = typer.Typer(no_args_is_help=True)


@cli.command()
def submit(
    model_name: str = typer.Argument(..., help="Triton model name (e.g. DoubleMetricLearning)"),
    input_path: Path = typer.Argument(..., help="Path to the input .npz file"),
    from_collection_id: str | None = typer.Option(
        None, help="Globus collection ID to stage in from (default: HTTPS upload)"
    ),
    to_collection_id: str | None = typer.Option(
        None, help="Globus collection ID to stage out results to"
    ),
) -> None:
    """
    Stage in an .npz input file, run Triton HEP inference, and stage out the result.
    """
    from .cli import _cli_state

    client = _cli_state["client"]

    input_path = input_path.expanduser().resolve()

    logger.info(f"Staging in {input_path}")
    stagein = client.stage_in(
        input_path, Path(input_path.name), from_collection_id=from_collection_id
    )
    logger.info(f"Stage in complete: {stagein}")

    remote_input = STAGING_COLLECTION_ROOT + str(stagein.destination_path)
    remote_output = remote_input.rsplit(".", 1)[0] + ".output.npz"

    logger.info(f"Submitting inference request for model {model_name!r}...")
    resp = client.d3_triton.submit(
        model_name=model_name,
        input_path=remote_input,
        output_path=remote_output,
    )

    logger.info(f"Polling on inference task {resp.task_id!r}...")
    result = client.d3_triton.poll_task_result(resp.task_id)
    logger.info(f"Inference completed: {result}")

    if to_collection_id:
        output_filename = Path(result["output_path"]).name
        local_output = input_path.with_suffix(".output.npz")
        logger.info(f"Staging out result file: {output_filename}")
        stageout = client.stage_out(
            to_collection_id,
            Path(output_filename),
            local_output,
        )
        logger.info(f"Stage out complete: {stageout}")
