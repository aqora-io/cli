from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Mapping, Sequence

from ._deps import JobError, JobStatus, JobV1, QuantumProgramResult, Result

if TYPE_CHECKING:
    from . import client as client_ops
    from .backend import QPU

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

    def to_qiskit_result(self, graphql: "client_ops.AqoraGraphQLClient") -> Result:
        if self.error is not None:
            raise JobError(f"Provider job result {self.index} failed: {self.error}")
        if not self.result_url:
            raise JobError(f"Provider job result {self.index} is missing a result URL")
        payload = graphql.download_text(self.result_url)
        return QuantumProgramResult.from_json_str(payload).to_qiskit_result()


def _merge_qiskit_results(results: Sequence[Result], *, job_id: str | None = None) -> Result:
    if not results:
        raise JobError("Provider job completed without any result payloads")

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


class QPUJob(JobV1):
    def __init__(
        self,
        backend: "QPU",
        job_id: str,
        *,
        payload: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(backend, job_id)
        self._payload = dict(payload or {})

    def backend(self) -> "QPU":
        return self._backend  # type: ignore[return-value]

    def submit(self) -> None:
        return None

    def cancel(self) -> bool:
        return False

    def status(self) -> JobStatus:
        payload = self._backend._fetch_job(self.job_id())
        self._payload = dict(payload)
        return self._job_status(payload.get("status"), payload.get("error"))

    def result(
        self,
        timeout: float | None = None,
        wait: float = 5,
    ) -> Any:
        # `wait_for_final_state` is inherited from JobV1, which polls
        # `status()` (refreshing `self._payload`), supports the standard
        # `callback` argument, and raises JobTimeoutError on timeout.
        self.wait_for_final_state(timeout=timeout, wait=wait)
        status = self._job_status(self._payload.get("status"), self._payload.get("error"))
        if status == JobStatus.CANCELLED:
            raise JobError(f"aqora QPU job {self.job_id()!r} was cancelled")
        if status == JobStatus.ERROR:
            message = self._payload.get("error") or f"aqora QPU job {self.job_id()!r} failed"
            raise JobError(message)
        items = self.result_items()
        expected = self._payload.get("resultCount")
        if expected is not None and len(items) != expected:
            raise JobError(
                f"aqora QPU job {self.job_id()!r} returned {len(items)} of "
                f"{expected} result payloads; results may still be uploading"
            )
        graphql = self.backend()._graphql
        return _merge_qiskit_results(
            [item.to_qiskit_result(graphql) for item in items],
            job_id=self.job_id(),
        )

    def result_items(self) -> list[ProviderJobResultItem]:
        return self.backend()._fetch_job_results(self.job_id())

    def _job_status(self, status: str | None, error: str | None) -> JobStatus:
        # Truthiness, not `is not None`: the server has returned `""` for
        # healthy jobs (the field mirrors the provider's progress message).
        if error:
            return JobStatus.ERROR
        # Unknown statuses fall back to RUNNING so that `wait_for_final_state`
        # keeps polling rather than reporting a spurious ERROR if the server
        # introduces a new status value.
        return _STATUS_MAPPING.get(status, JobStatus.RUNNING)
