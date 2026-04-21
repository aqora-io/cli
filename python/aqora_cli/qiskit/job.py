from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Mapping, Sequence

from . import client as client_ops
from ._deps import JobStatus, JobV1, QuantumProgramResult, Result

if TYPE_CHECKING:
    from .backend import AqoraBackend

FINAL_JOB_STATES = {
    JobStatus.DONE,
    JobStatus.ERROR,
    JobStatus.CANCELLED,
}

_STATUS_MAPPING = {
    "WAITING": JobStatus.QUEUED,
    "RUNNING": JobStatus.RUNNING,
    "COMPLETED": JobStatus.DONE,
    "CANCELLING": JobStatus.RUNNING,
    "CANCELLED": JobStatus.CANCELLED,
    None: JobStatus.INITIALIZING,
}


@dataclass(frozen=True)
class ProviderJobResultItem:
    index: int
    error: str | None
    result_url: str | None

    @classmethod
    def from_graphql(cls, data: Mapping[str, Any]) -> "ProviderJobResultItem":
        return cls(
            index=int(data["index"]),
            error=data.get("error"),
            result_url=data.get("result"),
        )

    def to_qiskit_result(self) -> Result:
        if self.error:
            raise RuntimeError(f"Provider job result {self.index} failed: {self.error}")
        if not self.result_url:
            raise RuntimeError(f"Provider job result {self.index} is missing a result URL")
        payload = client_ops._download_text(self.result_url)
        return QuantumProgramResult.from_json_str(payload).to_qiskit_result()


def _merge_qiskit_results(results: Sequence[Result], *, job_id: str | None = None) -> Result:
    if not results:
        raise RuntimeError("Provider job completed without any result payloads")
    if len(results) == 1:
        return results[0]

    merged = dict(results[0].to_dict())
    merged["results"] = []
    merged["success"] = True
    if job_id is not None:
        merged["job_id"] = job_id

    for result in results:
        payload = result.to_dict()
        merged["results"].extend(payload.get("results", []))
        merged["success"] = bool(merged["success"]) and bool(payload.get("success", True))

    return Result.from_dict(merged)


class AqoraJob(JobV1):
    def __init__(
        self,
        backend: "AqoraBackend",
        job_id: str,
        *,
        payload: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(backend, job_id)
        self._payload = dict(payload or {})

    @property
    def backend(self) -> "AqoraBackend":
        return self._backend  # type: ignore[return-value]

    def submit(self) -> None:
        return None

    def cancel(self) -> bool:
        return False

    def cancelled(self) -> bool:
        return self.status() == JobStatus.CANCELLED

    def status(self) -> JobStatus:
        payload = self.backend._fetch_job(self.job_id())
        self._payload = dict(payload)
        return self._job_status(payload.get("status"), payload.get("error"))

    def wait_for_final_state(
        self,
        timeout: float | None = None,
        wait: float = 5,
    ) -> None:
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            status = self.status()
            if status in FINAL_JOB_STATES:
                return
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(f"Timed out waiting for aqora job {self.job_id()!r}")
            time.sleep(wait)

    def result(
        self,
        timeout: float | None = None,
        wait: float = 5,
    ) -> Any:
        self.wait_for_final_state(timeout=timeout, wait=wait)
        status = self._job_status(self._payload.get("status"), self._payload.get("error"))
        if status == JobStatus.CANCELLED:
            raise RuntimeError(f"aqora job {self.job_id()!r} was cancelled")
        if status == JobStatus.ERROR:
            message = self._payload.get("error") or f"aqora job {self.job_id()!r} failed"
            raise RuntimeError(message)
        results = self.result_items()
        return _merge_qiskit_results(
            [result.to_qiskit_result() for result in results],
            job_id=self.job_id(),
        )

    def result_items(self) -> list[ProviderJobResultItem]:
        return self.backend._fetch_job_results(self.job_id())

    def _job_status(self, status: str | None, error: str | None) -> JobStatus:
        if error:
            return JobStatus.ERROR
        # Unknown statuses fall back to RUNNING so that `wait_for_final_state`
        # keeps polling rather than reporting a spurious ERROR if the server
        # introduces a new status value.
        return _STATUS_MAPPING.get(status, JobStatus.RUNNING)
