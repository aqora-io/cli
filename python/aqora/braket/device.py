from __future__ import annotations

from typing import Any

from aqora import Client
from aqora._provider import jobs, wire
from aqora._provider.formats import braket_qasm3

from ._deps import Circuit, Device
from .task import QPUTask


class QPU(Device):
    """Braket device that submits circuits to the QPU bound to the current runner session.

    `platform` selects the provider platform tasks are submitted to, by name or
    id (the schema's `ProviderPlatformNameOrID`), e.g. `"braket:SV1"`. When
    omitted, the server chooses its default platform.

    `as_entity` is the username or id of an organization you belong to; tasks
    are attributed to it, which is how provider usage is tracked per team. When
    it is omitted tasks are attributed to you personally.
    """

    def __init__(
        self,
        platform: str | None = None,
        *,
        client: Client | None = None,
        url: str | None = None,
        allow_insecure_host: bool | None = None,
        as_entity: str | None = None,
        compress: bool = True,
    ) -> None:
        # The platform's live status lives server-side; Braket's `status` is
        # only informational here.
        super().__init__(name=platform or "aqora-qpu", status="UNKNOWN")
        self._graphql = jobs._resolve_graphql(
            client, url=url, allow_insecure_host=allow_insecure_host
        )
        self._platform = platform
        self._as_entity = as_entity
        self._compress = compress

    @property
    def client(self) -> Client:
        return self._graphql.client

    @property
    def platform(self) -> str | None:
        return self._platform

    @property
    def as_entity(self) -> str | None:
        return self._as_entity

    def run(
        self,
        task_specification: Circuit,
        shots: int | None = None,
        inputs: dict[str, float] | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> QPUTask:
        """Submit a Braket `Circuit` as a provider job.

        `shots` is required. Parametric `inputs` and Braket's other run
        arguments are not supported.
        """
        if not isinstance(task_specification, Circuit):
            raise TypeError(
                "aqora.braket.QPU only runs Braket `Circuit`s, "
                f"got {type(task_specification).__name__}"
            )
        if inputs:
            raise NotImplementedError("aqora.braket.QPU does not support `inputs`")
        unsupported = [
            *(repr(arg) for arg in args if arg is not None),
            *(key for key, value in kwargs.items() if value is not None),
        ]
        if unsupported:
            raise NotImplementedError(
                "The aqora provider GraphQL API only supports `shots` as a per-run "
                f"parameter (unsupported arguments: {', '.join(unsupported)})"
            )
        shots = jobs.normalize_shots(shots)

        payload = wire.build_model_payload(
            [(braket_qasm3(task_specification), wire.PROGRAM_QASM_V3)],
            compress=self._compress,
        )
        job = jobs.submit_model(
            self._graphql,
            payload,
            shots=shots,
            platform=self._platform,
            as_entity=self._as_entity,
        )
        return QPUTask(self, job.job_id, payload=job._payload)

    def run_batch(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError(
            "aqora.braket.QPU does not support batches; call `run` per circuit"
        )
