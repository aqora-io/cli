from __future__ import annotations

from typing import TYPE_CHECKING, Any, Mapping

from aqora import Client
from aqora._provider.jobs import (
    ProviderJob,
    _resolve_graphql,
    is_provider_failure,
    qualified_platform_name,
)
from aqora._provider.results import ProviderResult

from ._deps import GateModelQuantumTaskResult, QuantumTask

if TYPE_CHECKING:
    from .device import QPU

# Aqora job statuses as Braket task states. A job with no status yet has been
# accepted but not handed to the provider.
_STATE_MAPPING = {
    "WAITING": "QUEUED",
    "RUNNING": "RUNNING",
    "COMPLETED": "COMPLETED",
    "CANCELLING": "CANCELLING",
    "CANCELLED": "CANCELLED",
    None: "CREATED",
}


class QPUTask(ProviderJob, QuantumTask):
    def __init__(
        self,
        device: "QPU",
        job_id: str,
        *,
        payload: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(device._graphql, job_id, payload=payload)
        self._qpu = device

    @classmethod
    def from_id(
        cls,
        job_id: str,
        *,
        client: Client | None = None,
        url: str | None = None,
        allow_insecure_host: bool | None = None,
    ) -> "QPUTask":
        """Load a task by id, building its `QPU` from the job's platform."""
        from .device import QPU

        graphql = _resolve_graphql(client, url=url, allow_insecure_host=allow_insecure_host)
        graphql.ensure_authenticated()
        payload = graphql.get_provider_job(job_id)
        device = QPU(
            qualified_platform_name(payload.get("platform")),
            client=graphql.client,
        )
        return cls(device, job_id, payload=payload)

    @property
    def id(self) -> str:
        return self.job_id

    def device(self) -> "QPU":
        return self._qpu

    def state(self) -> str:
        payload = self.refresh()
        status = payload.get("status")
        if is_provider_failure(status, payload.get("error")):
            return "FAILED"
        return _STATE_MAPPING.get(status, str(status))

    def result(
        self,
        timeout: float | None = None,
        wait: float = 5.0,
    ) -> GateModelQuantumTaskResult:
        """Wait for the task to complete and decode its Braket result."""
        self.wait(timeout=timeout, poll_interval=wait)
        items = self.results()
        if len(items) != 1:
            raise RuntimeError(
                f"aqora provider job {self.job_id!r} returned {len(items)} result "
                "payloads; use `result_items()` to inspect them"
            )
        return items[0].to_braket_result()

    def result_items(self) -> list[ProviderResult]:
        return self.results()

    def cancel(self) -> None:
        raise NotImplementedError("aqora provider jobs cannot be cancelled")

    def async_result(self) -> Any:
        raise NotImplementedError("aqora.braket.QPUTask has no async results; use `result`")
