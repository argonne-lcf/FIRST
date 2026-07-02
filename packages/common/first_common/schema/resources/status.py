from pydantic import BaseModel


class DeploymentStatus(BaseModel):
    """Redis-backed observational status for PilotDeployment and StaticDeployment.

    Every field must have a default so the model can be materialized when
    Redis is cold.
    """

    load_avg_1m: float = 0.0
    load_avg_5m: float = 0.0
    load_max_1m: float = 0.0
    load_max_5m: float = 0.0
