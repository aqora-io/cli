from __future__ import annotations

from typing import TYPE_CHECKING, Any, Mapping

from aqora import Client
from aqora._provider import wire
from aqora._provider.jobs import ProviderJob, _resolve_graphql, qualified_platform_name
from aqora._provider.results import ProviderResult, QirLabeledResult

from ._deps import QsysResult

if TYPE_CHECKING:
    from .backend import QPU


class QPUJob(ProviderJob):
    def __init__(
        self,
        backend: "QPU",
        job_id: str,
        *,
        payload: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(backend._graphql, job_id, payload=payload)
        self._qpu = backend

    @classmethod
    def from_id(
        cls,
        job_id: str,
        *,
        client: Client | None = None,
        url: str | None = None,
        allow_insecure_host: bool | None = None,
    ) -> "QPUJob":
        """Load a job by id, building its `QPU` from the job's platform."""
        from .backend import QPU

        graphql = _resolve_graphql(client, url=url, allow_insecure_host=allow_insecure_host)
        graphql.ensure_authenticated()
        payload = graphql.get_provider_job(job_id)
        backend = QPU(
            graphql.client,
            platform=qualified_platform_name(payload.get("platform")),
        )
        return cls(backend, job_id, payload=payload)

    def backend(self) -> "QPU":
        return self._qpu

    def result(
        self,
        timeout: float | None = None,
        wait: float = 5.0,
    ) -> QsysResult | QirLabeledResult:
        """Wait for the job to complete and decode its result.

        QSYS shot arrays decode to a hugr `QsysResult`; labeled QIR text
        decodes to a `QirLabeledResult`.
        """
        self.wait(timeout=timeout, poll_interval=wait)
        items = self.results()
        if len(items) != 1:
            raise RuntimeError(
                f"aqora provider job {self.job_id!r} returned {len(items)} result "
                "payloads; use `result_items()` to inspect them"
            )
        return self._decode(items[0])

    def result_items(self) -> list[ProviderResult]:
        return self.results()

    def _decode(self, item: ProviderResult) -> QsysResult | QirLabeledResult:
        if item.serialization_format == wire.RESULT_QSYS_RESULT_JSON_V1:
            return QsysResult(item.qsys_shots())
        if item.serialization_format == wire.RESULT_QIR_LABELED_RESULT_V1:
            return item.qir_labeled()
        raise ValueError(
            f"Unexpected guppy result serialization format {item.serialization_format}"
        )
