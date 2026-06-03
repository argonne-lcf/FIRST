from abc import ABC, abstractmethod


class PilotJobSubmitPayload: ...


class PilotJobSubmitResult: ...


class PilotJobStatusResult: ...


class PilotJobTerminateResult: ...


class SchedulerInterface(ABC):
    @abstractmethod
    async def submit_job(
        self, job_spec: PilotJobSubmitPayload
    ) -> PilotJobSubmitResult: ...

    @abstractmethod
    async def get_job_status(self, job_id: str) -> PilotJobStatusResult: ...

    @abstractmethod
    async def terminate_job(self, job_id: str) -> PilotJobTerminateResult: ...
