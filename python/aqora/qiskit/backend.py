from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterable, Mapping, Sequence

from aqora import Client

from . import client as client_ops
from ._deps import (
    BackendData,
    BackendEstimatorV2,
    BackendSamplerV2,
    BackendV2,
    ClientData,
    Options,
    QuantumCircuit,
    QuantumComputationModel,
    QuantumProgram,
    Target,
)

if TYPE_CHECKING:
    from .job import ProviderJobResultItem, QPUJob

GENERIC_BASIS_GATES = [
    "id",
    "x",
    "sx",
    "rz",
    "cx",
    "measure",
    "reset",
    "delay",
]


class QPU(BackendV2):
    """Qiskit backend that submits circuits to the QPU bound to the current runner session.

    `platform` selects the provider platform jobs are submitted to, by name or
    id (the schema's `ProviderPlatformNameOrID`). When omitted, the server
    chooses its default platform.
    """

    def __init__(
        self,
        client: Client | None = None,
        *,
        url: str | None = None,
        allow_insecure_host: bool | None = None,
        platform: str | None = None,
    ) -> None:
        if client is not None and (url is not None or allow_insecure_host is not None):
            raise ValueError(
                "`url` and `allow_insecure_host` cannot be combined with an explicit `client`"
            )
        raw_client = client or Client(url, allow_insecure_host=allow_insecure_host)
        self._graphql = client_ops.AqoraGraphQLClient(raw_client)
        self._platform = platform
        self._target: Target | None = None
        super().__init__(provider=None, name="aqora-qpu")

    @classmethod
    def _default_options(cls) -> Options:
        return Options(
            shots=None,
            memory=False,
        )

    @property
    def client(self) -> Client:
        return self._graphql.client

    @property
    def platform(self) -> str | None:
        return self._platform

    @property
    def target(self) -> Target:
        """Transpilation target built from the provider platforms' capabilities.

        The qubit count is fetched lazily from the server on first access and
        cached. With a `platform` selected it is that platform's `maxQubits`;
        otherwise it is the maximum across the available platforms, so the
        server remains the authority on per-platform limits.
        """
        if self._target is None:
            self._target = self._build_target()
        return self._target

    @property
    def max_circuits(self) -> int | None:
        return None

    def run(
        self,
        run_input: QuantumCircuit | Iterable[QuantumCircuit],
        **options: Any,
    ) -> "QPUJob":
        """Submit circuits as a provider job.

        Only `shots` is forwarded to the provider API. `memory` is accepted for
        compatibility with `BackendSamplerV2` (which always requests it); whether
        per-shot memory is present in the returned results depends on the
        provider. Any other option set to a non-None value is rejected.
        """
        from .job import QPUJob

        circuits = self._normalize_run_input(run_input)

        shots = self._effective_shots(options)
        unsupported = self._unsupported_run_options(options)
        if unsupported:
            raise NotImplementedError(
                "The aqora provider GraphQL API only supports `shots` as a per-run "
                f"parameter (unsupported options: {', '.join(sorted(unsupported))})"
            )
        if shots is not None and shots < 1:
            raise ValueError("`shots` must be at least 1")

        self._graphql.ensure_authenticated()

        model_payload = self._build_model_payload(circuits)
        upload_info = self._graphql.start_provider_model_upload()
        etag = self._graphql.upload_payload(upload_info["uploadUrl"], model_payload)

        model = self._graphql.create_provider_model(
            provider_model_upload_id=upload_info["providerModelUploadId"],
            etag=etag,
        )
        job = self._graphql.create_provider_job(
            provider_model_id=model["id"],
            shots=shots,
            provider_platform=self._platform,
        )
        return QPUJob(self, str(job["id"]), payload=job)

    def sampler(self, *, options: Mapping[str, Any] | None = None) -> BackendSamplerV2:
        return BackendSamplerV2(backend=self, options=dict(options) if options else None)

    def estimator(self, *, options: Mapping[str, Any] | None = None) -> BackendEstimatorV2:
        return BackendEstimatorV2(backend=self, options=dict(options) if options else None)

    def _normalize_run_input(
        self,
        run_input: QuantumCircuit | Iterable[QuantumCircuit],
    ) -> list[QuantumCircuit]:
        if isinstance(run_input, QuantumCircuit):
            circuits = [run_input]
        else:
            circuits = list(run_input)
        if not circuits:
            raise ValueError("At least one circuit is required")
        if not all(isinstance(circuit, QuantumCircuit) for circuit in circuits):
            raise TypeError("aqora QPU only accepts qiskit.QuantumCircuit inputs")
        return circuits

    def _effective_shots(self, overrides: Mapping[str, Any]) -> int | None:
        shots = overrides.get("shots", getattr(self.options, "shots", None))
        if shots is None:
            return None
        if isinstance(shots, bool):
            raise TypeError("`shots` must be an integer")
        try:
            as_int = int(shots)
        except (TypeError, ValueError) as exc:
            raise TypeError("`shots` must be an integer") from exc
        if as_int != shots:
            raise TypeError("`shots` must be an integer")
        return as_int

    def _unsupported_run_options(self, overrides: Mapping[str, Any]) -> list[str]:
        # `is not None` (not truthiness) so that falsy-but-meaningful values
        # like seed_simulator=0 or rep_delay=0.0 are rejected rather than
        # silently dropped. `memory` is accepted for BackendSamplerV2.
        unsupported = []
        for key, value in overrides.items():
            if key in ("shots", "memory"):
                continue
            if value is not None:
                unsupported.append(key)
        return unsupported

    def _build_target(self) -> Target:
        self._graphql.ensure_authenticated()
        platforms = self._graphql.get_provider_platforms()
        if self._platform is not None:
            platforms = [
                platform
                for platform in platforms
                if self._platform in (platform.get("name"), platform.get("id"))
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
                "cannot build a transpilation target"
            )
        return Target.from_configuration(
            basis_gates=GENERIC_BASIS_GATES,
            num_qubits=max(max_qubits),
        )

    def _build_model_payload(self, circuits: Sequence[QuantumCircuit]) -> str:
        model = QuantumComputationModel(
            programs=[QuantumProgram.from_qiskit_circuit(circuit) for circuit in circuits],
            client=ClientData(user_agent=f"aqora/{client_ops._package_version()}"),
            backend=BackendData(
                name="aqora-qpu",
                version=None,
                options=None,
            ),
        )
        return model.to_json_str()

    def _fetch_job(self, job_id: str) -> Mapping[str, Any]:
        return self._graphql.get_provider_job(job_id)

    def _fetch_job_results(
        self,
        job_id: str,
        *,
        page_size: int = 100,
    ) -> list["ProviderJobResultItem"]:
        from .job import ProviderJobResultItem

        items = [
            ProviderJobResultItem.from_graphql(item)
            for item in self._graphql.get_provider_job_results(
                job_id,
                page_size=page_size,
            )
        ]
        items.sort(key=lambda item: item.index)
        return items
