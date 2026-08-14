import asyncio
import logging

from rich import print

from first_common.schema.base_scheduler import JobSubmitPayload
from first_gateway.platforms.schedulers.graphql_pbs import GraphQLPBSAdapter
from first_gateway.settings import Settings

logging.basicConfig(level="INFO")
test_script = """#!/bin/bash

echo Test script starting

for node in $(cat $PBS_NODEFILE)
do
echo $node
ssh $node nvidia-smi -L
done
sleep 30
echo Test script done!
"""


async def main():
    s = Settings()
    async with s.build_clients() as cs:
        adapter = await GraphQLPBSAdapter.build(
            cs,
            dict(
                keycloak_client_name="tara-pbs",
                job_owner="openinference_svc",
                graphql_url="https://graphql-bridge-dev-01.lab.alcf.anl.gov/tara/graphql",
            ),
        )
        job = await adapter.submit_job(
            JobSubmitPayload(
                name="TEST-JOB",
                queue="workq",
                account="inference_service",
                scheduler_flags="",
                num_nodes=3,
                gpus_per_node=4,
                walltime_min=5,
                log_path="/home/openinference_svc/test123.log",
                script=test_script,
            )
        )
        print("Submitted job:", job)

        print("Job Statuses:")
        stats = await adapter.get_job_statuses()
        print(stats)

        print("OK!")


asyncio.run(main())
