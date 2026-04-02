from __future__ import annotations

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
    from .job import ProviderJobResultItem, QPUJob

RUN_PARAMETERS_UNSUPPORTED = (
    "The aqora provider GraphQL API only supports `shots` as a per-run parameter."
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


class QPU(BackendV2):
    """Qiskit backend that submits circuits to the QPU bound to the current runner session."""

    def __init__(
        self,
        client: Client | None = None,
        *,
        url: str | None = None,
        allow_insecure_host: bool | None = None,
    ) -> None:
        raw_client = client or Client(url, allow_insecure_host=allow_insecure_host)
        self._graphql = client_ops.AqoraGraphQLClient(raw_client)
        self._target = Target.from_configuration(
            basis_gates=GENERIC_BASIS_GATES,
            num_qubits=0,
        )
        super().__init__(provider=None, name="aqora-qpu")

    @classmethod
    def _default_options(cls) -> Options:
        return Options(
            shots=None,
            memory=False,
            seed_simulator=None,
        )

    @property
    def client(self) -> Client:
        return self._graphql.client

    @property
    def target(self) -> Target:
        return self._target

    @property
    def max_circuits(self) -> int | None:
        return None

    def run(
        self,
        run_input: QuantumCircuit | Iterable[QuantumCircuit],
        **options: Any,
    ) -> "QPUJob":
        from .job import QPUJob

        circuits = self._normalize_run_input(run_input)

        shots = self._effective_shots(options)
        if self._unsupported_run_options(options):
            raise NotImplementedError(RUN_PARAMETERS_UNSUPPORTED)
        if shots is not None and shots < 1:
            raise ValueError("`shots` must be at least 1")

        self._graphql.ensure_authenticated()

        model_payload = self._build_model_payload(circuits)
        upload_info = self._graphql.start_provider_model_upload()
        etag = client_ops._put_json_payload(upload_info["uploadUrl"], model_payload)

        model = self._graphql.create_provider_model(
            provider_model_upload_id=upload_info["providerModelUploadId"],
            etag=etag,
        )
        job = self._graphql.create_provider_job(
            provider_model_id=model["id"],
            shots=shots,
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

    def _build_model_payload(self, circuits: Sequence[QuantumCircuit]) -> str:
        model = QuantumComputationModel(
            programs=[QuantumProgram.from_qiskit_circuit(circuit) for circuit in circuits],
            client=ClientData(user_agent=f"aqora-cli/{client_ops._package_version()}"),
            backend=BackendData(
                name="aqora-qpu",
                version=None,
                options=client_ops._json_options({}),
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
