from __future__ import annotations

import json
import time
from typing import Any, Sequence

from aqora import Client
from aqora._provider import jobs, wire
from aqora._provider.client import AqoraGraphQLClient, _package_version

from ._deps import (
    AutoRebase,
    Backend,
    BackendInfo,
    BackendResult,
    BasePass,
    Circuit,
    CircuitNotRunError,
    CircuitStatus,
    DecomposeBoxes,
    FullPeepholeOptimise,
    MaxNQubitsPredicate,
    OpType,
    Predicate,
    ResultHandle,
    SequencePass,
    StatusEnum,
)

# Quantinuum-style gateset; the server owns device-level compilation, so this
# is a client-side convenience for `rebase_pass`/`backend_info` only.
GATESET = {
    OpType.PhasedX,
    OpType.Rz,
    OpType.ZZPhase,
    OpType.ZZMax,
    OpType.Measure,
    OpType.Reset,
}

_STATUS_MAPPING = {
    "WAITING": StatusEnum.QUEUED,
    "RUNNING": StatusEnum.RUNNING,
    "COMPLETED": StatusEnum.COMPLETED,
    "CANCELLING": StatusEnum.CANCELLING,
    "CANCELLED": StatusEnum.CANCELLED,
    None: StatusEnum.SUBMITTED,
}


class QPU(Backend):
    """pytket backend that submits circuits to the QPU bound to the current runner session.

    `platform` selects the provider platform jobs are submitted to, by name or
    id (the schema's `ProviderPlatformNameOrID`). When omitted, the server
    chooses its default platform.
    """

    _supports_shots = True
    _supports_counts = True
    _persistent_handles = True

    def __init__(
        self,
        client: Client | None = None,
        *,
        url: str | None = None,
        allow_insecure_host: bool | None = None,
        platform: str | None = None,
        compress: bool = True,
    ) -> None:
        if client is not None and (url is not None or allow_insecure_host is not None):
            raise ValueError(
                "`url` and `allow_insecure_host` cannot be combined with an explicit `client`"
            )
        raw_client = client or Client(url, allow_insecure_host=allow_insecure_host)
        self._graphql = AqoraGraphQLClient(raw_client)
        self._platform = platform
        self._compress = compress
        self._max_qubits: int | None = None
        self._backend_info: BackendInfo | None = None
        super().__init__()

    @property
    def client(self) -> Client:
        return self._graphql.client

    @property
    def platform(self) -> str | None:
        return self._platform

    @property
    def _result_id_type(self) -> tuple[type, ...]:
        return (str, int)

    @property
    def backend_info(self) -> BackendInfo:
        if self._backend_info is None:
            self._backend_info = BackendInfo(
                name=type(self).__name__,
                device_name=self._platform,
                version=_package_version(),
                architecture=None,
                gate_set=GATESET,
                misc={"max_qubits": self._platform_max_qubits()},
            )
        return self._backend_info

    @property
    def required_predicates(self) -> list[Predicate]:
        return [MaxNQubitsPredicate(self._platform_max_qubits())]

    def default_compilation_pass(self, optimisation_level: int = 2) -> BasePass:
        passes: list[BasePass] = [DecomposeBoxes()]
        if optimisation_level >= 2:
            passes.append(FullPeepholeOptimise())
        return SequencePass(passes)

    def rebase_pass(self) -> BasePass:
        return AutoRebase(GATESET)

    def process_circuits(
        self,
        circuits: Sequence[Circuit],
        n_shots: int | Sequence[int] | None = None,
        valid_check: bool = True,
        **kwargs: Any,
    ) -> list[ResultHandle]:
        circuits = list(circuits)
        if not circuits:
            raise ValueError("At least one circuit is required")
        shots = self._normalize_shots(n_shots)
        if valid_check:
            self._check_all_circuits(circuits)
        programs = [
            (json.dumps(circuit.to_dict()), wire.PROGRAM_TKET_CIRCUIT_JSON_V1)
            for circuit in circuits
        ]
        job = jobs.submit_model(
            self._graphql,
            wire.build_model_payload(programs, compress=self._compress),
            shots=shots,
            platform=self._platform,
        )
        return [ResultHandle(job.job_id, index) for index in range(len(circuits))]

    def circuit_status(self, handle: ResultHandle) -> CircuitStatus:
        self._graphql.ensure_authenticated()
        payload = self._graphql.get_provider_job(str(handle[0]))
        # The server mirrors the provider's progress message into `error` for
        # live jobs ("The job is queued.") and nulls `status` when the
        # provider reports an error state; only that combination is a failure.
        status = payload.get("status")
        error = payload.get("error")
        if status is None and error:
            return CircuitStatus(StatusEnum.ERROR, error)
        # Unknown statuses fall back to RUNNING so that callers keep polling
        # rather than reporting a spurious failure if the server introduces a
        # new status value.
        return CircuitStatus(_STATUS_MAPPING.get(status, StatusEnum.RUNNING), error or "")

    def get_result(self, handle: ResultHandle, **kwargs: Any) -> BackendResult:
        try:
            return super().get_result(handle)
        except CircuitNotRunError:
            pass

        timeout = kwargs.get("timeout")
        wait = kwargs.get("wait", 5.0)
        deadline = None if timeout is None else time.monotonic() + timeout
        job_id = str(handle[0])
        while True:
            status = self.circuit_status(handle)
            if status.status == StatusEnum.COMPLETED:
                break
            if status.status == StatusEnum.ERROR:
                raise RuntimeError(
                    f"aqora provider job {job_id!r} failed: {status.message}"
                )
            if status.status == StatusEnum.CANCELLED:
                raise RuntimeError(f"aqora provider job {job_id!r} was cancelled")
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Timeout while waiting for aqora provider job {job_id!r}"
                )
            time.sleep(wait)

        job = jobs.ProviderJob(self._graphql, job_id)
        for item in job.results():
            item_handle = ResultHandle(job_id, item.index)
            self._cache.setdefault(item_handle, {})["result"] = item.to_backend_result()
        return super().get_result(handle)

    def cancel(self, handle: ResultHandle) -> None:
        raise NotImplementedError("The aqora provider GraphQL API does not support cancellation")

    def _normalize_shots(self, n_shots: int | Sequence[int] | None) -> int | None:
        if n_shots is None or isinstance(n_shots, int):
            return n_shots
        shots = list(n_shots)
        if len(set(shots)) > 1:
            raise NotImplementedError(
                "The aqora provider GraphQL API does not support per-circuit "
                "shot counts; all circuits in a job share one `shots` value"
            )
        return shots[0] if shots else None

    def _platform_max_qubits(self) -> int:
        if self._max_qubits is None:
            self._graphql.ensure_authenticated()
            platforms = self._graphql.get_provider_platforms()
            if self._platform is not None:
                platforms = [
                    platform
                    for platform in platforms
                    if jobs.platform_matches(self._platform, platform)
                ]
                if not platforms:
                    raise LookupError(f"Provider platform {self._platform!r} was not found")
            max_qubits = [
                platform["meta"]["maxQubits"]
                for platform in platforms
                if (platform.get("meta") or {}).get("maxQubits") is not None
            ]
            if not max_qubits:
                raise RuntimeError(
                    "No provider platform reported a qubit count; "
                    "cannot build circuit predicates"
                )
            self._max_qubits = max(max_qubits)
        return self._max_qubits
