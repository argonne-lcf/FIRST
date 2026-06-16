from first_common.schema.scheduler import (
    PilotJobStatusResult,
    PilotJobSubmitPayload,
    PilotJobSubmitResult,
    PilotJobTerminateResult,
    SchedulerInterface,
)


class GlobusComputePBSWrapper(SchedulerInterface):
    async def submit_job(
        self, _job_spec: PilotJobSubmitPayload
    ) -> PilotJobSubmitResult:
        return PilotJobSubmitResult()

    async def get_job_status(self, _job_id: str) -> PilotJobStatusResult:
        return PilotJobStatusResult()

    async def terminate_job(self, _job_id: str) -> PilotJobTerminateResult:
        return PilotJobTerminateResult()
