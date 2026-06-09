import os

from dotenv import load_dotenv
from globus_compute_sdk import Executor

load_dotenv()

TEMPLATE_ENDPOINT_ID = os.environ.get("TEMPLATE_ENDPOINT_ID")
SEC_PER_HEARTBEAT = 30


def hello():
    return "Hello from the templated endpoint!"


models = "meta-llama/Llama-4-Maverick-17B-128E-Instruct"

# Define heart beats to prevent the UEP shutting down before walltime
walltime = "01:00:00"
w_split = walltime.split(":")
walltime_sec = int(w_split[0]) * 3600 + int(w_split[1]) * 60 + int(w_split[2])
idle_heartbeats_soft = walltime_sec / SEC_PER_HEARTBEAT + 1

endpoint_config = {
    "display_name": models,
    "max_idletime": 1800,
    "account": "inference_service",
    "queue": "by-node",
    "walltime": walltime,
    "worker_init": f"echo 'starting {models}'; source /home/bcote/test-template/.venv/bin/activate",
    "idle_heartbeats_soft": idle_heartbeats_soft,
}

with Executor(
    endpoint_id=TEMPLATE_ENDPOINT_ID, user_endpoint_config=endpoint_config
) as ex:
    future = ex.submit(hello)
    result = future.result()
    print("result:", result)
