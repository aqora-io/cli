from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Iterable, Mapping, Sequence

from aqora_cli import Client

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
    from .job import AqoraJob, ProviderJobResultItem

RUN_PARAMETERS_UNSUPPORTED = (
    "The current aqora provider GraphQL API only supports `shots` as a per-run "
    "parameter. `memory` and other backend run options cannot be customized "
    "through `createProviderJob`."
)

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


@dataclass(frozen=True)
class ProviderPlatform:
    provider: str
    name: str
    id: str
    vendor: str | None = None
    backend: str | None = None
    max_qubits: int | None = None
    max_shots: int | None = None
    max_circuits: int | None = None
    technology: str | None = None
    is_qpu: bool | None = None
    status: str | None = None

    @classmethod
    def from_graphql(cls, data: Mapping[str, Any]) -> "ProviderPlatform":
        return cls(
            provider=str(data["provider"]),
            name=str(data["name"]),
            id=str(data["id"]),
            vendor=data.get("vendor"),
            backend=data.get("backend"),
            max_qubits=data.get("maxQubits"),
            max_shots=data.get("maxShots"),
            max_circuits=data.get("maxCircuits"),
            technology=data.get("technology"),
            is_qpu=data.get("isQpu"),
            status=data.get("status"),
        )


class AqoraProvider:
    def __init__(
        self,
        client: Client | None = None,
        *,
        url: str | None = None,
        allow_insecure_host: bool | None = None,
    ) -> None:
        raw_client = client or Client(url, allow_insecure_host=allow_insecure_host)
        self._graphql = client_ops.AqoraGraphQLClient(raw_client)
        self._platforms: list[ProviderPlatform] | None = None

    @property
    def client(self) -> Client:
        return self._graphql.client

    def backends(
        self,
        name: str | None = None,
        *,
        provider: str | None = None,
        refresh: bool = False,
    ) -> list["AqoraBackend"]:
        platforms = self._list_platforms(refresh=refresh)
        if provider is not None:
            platforms = [platform for platform in platforms if platform.provider == provider]
        backends = [AqoraBackend(self, platform) for platform in platforms]
        if name is None:
            return backends
        return [
            backend
            for backend in backends
            if backend.name == name
            or backend.platform_id == name
            or backend.platform.name == name
        ]

    def get_backend(
        self,
        name: str,
        *,
        provider: str | None = None,
        refresh: bool = False,
    ) -> "AqoraBackend":
        matches = self.backends(name, provider=provider, refresh=refresh)
        if not matches:
            raise LookupError(f"No aqora backend matched {name!r}")
        if len(matches) > 1:
            raise LookupError(
                f"Multiple aqora backends matched {name!r}; specify `provider=` explicitly"
            )
        return matches[0]

    def _list_platforms(self, *, refresh: bool = False) -> list[ProviderPlatform]:
        if self._platforms is not None and not refresh:
            return list(self._platforms)

        platforms = [
            ProviderPlatform.from_graphql(platform)
            for platform in self._graphql.list_provider_platforms()
        ]
        self._platforms = platforms
        return list(platforms)

    def _ensure_authenticated(self) -> None:
        self._graphql.ensure_authenticated()


class AqoraBackend(BackendV2):
    def __init__(self, provider: AqoraProvider, platform: ProviderPlatform) -> None:
        self._provider = provider
        self._platform = platform
        self._target = Target.from_configuration(
            basis_gates=GENERIC_BASIS_GATES,
            num_qubits=platform.max_qubits or 0,
        )
        super().__init__(
            provider=provider,
            name=f"{platform.provider}:{platform.id}",
            description=platform.name,
            backend_version=platform.backend or "unknown",
        )

    @classmethod
    def _default_options(cls) -> Options:
        return Options(
            shots=None,
            memory=False,
            seed_simulator=None,
        )

    @property
    def platform(self) -> ProviderPlatform:
        return self._platform

    @property
    def platform_id(self) -> str:
        return self._platform.id

    @property
    def provider_name(self) -> str:
        return self._platform.provider

    @property
    def target(self) -> Target:
        return self._target

    @property
    def max_circuits(self) -> int | None:
        return self._platform.max_circuits

    @property
    def num_qubits(self) -> int:
        return self._platform.max_qubits or 0

    def run(
        self,
        run_input: QuantumCircuit | Iterable[QuantumCircuit],
        **options: Any,
    ) -> "AqoraJob":
        from .job import AqoraJob

        circuits = self._normalize_run_input(run_input)
        self._validate_circuits(circuits)

        shots = self._effective_shots(options)
        unsupported_options = self._unsupported_run_options(options)
        if unsupported_options:
            raise NotImplementedError(RUN_PARAMETERS_UNSUPPORTED)
        self._validate_shots(shots)

        self._provider._ensure_authenticated()

        model_payload = self._build_model_payload(circuits)
        upload_info = self._provider._graphql.start_provider_model_upload()
        etag = client_ops._put_json_payload(upload_info["uploadUrl"], model_payload)

        model = self._provider._graphql.create_provider_model(
            provider_model_upload_id=upload_info["providerModelUploadId"],
            etag=etag,
            provider=self.provider_name,
        )
        job = self._provider._graphql.create_provider_job(
            provider_model_id=model["id"],
            provider=self.provider_name,
            platform_id=self.platform_id,
            shots=shots,
        )
        return AqoraJob(self, str(job["id"]), payload=job)

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
            raise TypeError("aqora backends only accept qiskit.QuantumCircuit inputs")
        return circuits

    def _validate_circuits(self, circuits: Sequence[QuantumCircuit]) -> None:
        if self.max_circuits is not None and len(circuits) > self.max_circuits:
            raise ValueError(
                f"Backend {self.name} accepts at most {self.max_circuits} circuits per job"
            )
        if self.num_qubits and any(circuit.num_qubits > self.num_qubits for circuit in circuits):
            raise ValueError(
                f"Backend {self.name} accepts at most {self.num_qubits} qubits per circuit"
            )

    def _effective_shots(self, overrides: Mapping[str, Any]) -> int | None:
        shots = overrides.get("shots", getattr(self.options, "shots", None))
        if shots is None:
            return None
        try:
            shots = int(shots)
        except (TypeError, ValueError) as exc:
            raise TypeError("`shots` must be an integer") from exc
        return shots

    def _unsupported_run_options(self, overrides: Mapping[str, Any]) -> dict[str, Any]:
        unsupported = {
            "memory": overrides.get("memory", getattr(self.options, "memory", False)),
            "seed_simulator": overrides.get(
                "seed_simulator",
                getattr(self.options, "seed_simulator", None),
            ),
        }
        extras = {
            key: value
            for key, value in overrides.items()
            if key not in {"shots", "memory", "seed_simulator"} and value is not None
        }
        if extras:
            unsupported.update(extras)
        return {key: value for key, value in unsupported.items() if value not in (None, False)}

    def _validate_shots(self, shots: int | None) -> None:
        if shots is None:
            return
        if shots < 1:
            raise ValueError("`shots` must be at least 1")
        if self.platform.max_shots is not None and shots > self.platform.max_shots:
            raise ValueError(
                f"Backend {self.name} accepts at most {self.platform.max_shots} shots per job"
            )

    def _build_model_payload(self, circuits: Sequence[QuantumCircuit]) -> str:
        model = QuantumComputationModel(
            programs=[QuantumProgram.from_qiskit_circuit(circuit) for circuit in circuits],
            client=ClientData(user_agent=f"aqora-cli/{client_ops._package_version()}"),
            backend=BackendData(
                name=self.platform.backend or self.platform.name,
                version=self.backend_version,
                options=client_ops._json_options({}),
            ),
        )
        return model.to_json_str()

    def _fetch_job(self, job_id: str) -> Mapping[str, Any]:
        return self._provider._graphql.get_provider_job(job_id)

    def _fetch_job_results(
        self,
        job_id: str,
        *,
        page_size: int = 100,
    ) -> list["ProviderJobResultItem"]:
        from .job import ProviderJobResultItem

        items = [
            ProviderJobResultItem.from_graphql(item)
            for item in self._provider._graphql.get_provider_job_results(
                job_id,
                page_size=page_size,
            )
        ]
        items.sort(key=lambda item: item.index)
        return items
