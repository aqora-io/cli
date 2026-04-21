from __future__ import annotations

import importlib.util
import json
import sys
import types
from enum import Enum
from pathlib import Path

import pytest


MODULE_PATH = (
    Path(__file__).resolve().parents[1] / "python" / "aqora_cli" / "qiskit" / "__init__.py"
)


def load_qiskit_module(fake_client_cls: type):
    for name in list(sys.modules):
        if name == "aqora_cli" or name.startswith(("aqora_cli.qiskit", "qiskit", "qio")):
            sys.modules.pop(name, None)

    aqora_cli = types.ModuleType("aqora_cli")
    aqora_cli.__path__ = [str(MODULE_PATH.parents[1])]
    aqora_cli.Client = fake_client_cls
    sys.modules["aqora_cli"] = aqora_cli

    qiskit = types.ModuleType("qiskit")

    class QuantumCircuit:
        def __init__(self, num_qubits: int) -> None:
            self.num_qubits = num_qubits

    qiskit.QuantumCircuit = QuantumCircuit
    sys.modules["qiskit"] = qiskit

    qiskit_primitives = types.ModuleType("qiskit.primitives")

    class BackendSamplerV2:
        def __init__(self, *, backend, options=None) -> None:
            self.backend = backend
            self.options = options

    class BackendEstimatorV2:
        def __init__(self, *, backend, options=None) -> None:
            self.backend = backend
            self.options = options

    qiskit_primitives.BackendSamplerV2 = BackendSamplerV2
    qiskit_primitives.BackendEstimatorV2 = BackendEstimatorV2
    sys.modules["qiskit.primitives"] = qiskit_primitives

    qiskit_providers = types.ModuleType("qiskit.providers")

    class Options:
        def __init__(self, **fields) -> None:
            self.__dict__.update(fields)

    class BackendV2:
        version = 2

        def __init__(
            self,
            provider=None,
            name=None,
            description=None,
            online_date=None,
            backend_version=None,
            **fields,
        ) -> None:
            self.provider = provider
            self.name = name
            self.description = description
            self.online_date = online_date
            self.backend_version = backend_version
            self.options = type(self)._default_options()
            self.options.__dict__.update(fields)

    class JobV1:
        def __init__(self, backend, job_id) -> None:
            self._backend = backend
            self._job_id = job_id

        def job_id(self) -> str:
            return self._job_id

    qiskit_providers.Options = Options
    qiskit_providers.BackendV2 = BackendV2
    qiskit_providers.JobV1 = JobV1
    sys.modules["qiskit.providers"] = qiskit_providers

    qiskit_jobstatus = types.ModuleType("qiskit.providers.jobstatus")

    class JobStatus(Enum):
        INITIALIZING = "INITIALIZING"
        QUEUED = "QUEUED"
        RUNNING = "RUNNING"
        DONE = "DONE"
        ERROR = "ERROR"
        CANCELLED = "CANCELLED"

    qiskit_jobstatus.JobStatus = JobStatus
    sys.modules["qiskit.providers.jobstatus"] = qiskit_jobstatus

    qiskit_result = types.ModuleType("qiskit.result")

    class Result:
        def __init__(self, payload: dict[str, object]) -> None:
            self._payload = payload

        @classmethod
        def from_dict(cls, payload: dict[str, object]) -> "Result":
            return cls(payload)

        def to_dict(self) -> dict[str, object]:
            return self._payload

    qiskit_result.Result = Result
    sys.modules["qiskit.result"] = qiskit_result

    qiskit_transpiler = types.ModuleType("qiskit.transpiler")

    class Target:
        def __init__(self, *, basis_gates=None, num_qubits=0) -> None:
            self.basis_gates = basis_gates
            self.num_qubits = num_qubits

        @classmethod
        def from_configuration(cls, basis_gates, num_qubits=None, coupling_map=None):
            return cls(basis_gates=basis_gates, num_qubits=num_qubits or 0)

    qiskit_transpiler.Target = Target
    sys.modules["qiskit.transpiler"] = qiskit_transpiler

    qio = types.ModuleType("qio")
    qio_core = types.ModuleType("qio.core")

    class QuantumProgram:
        def __init__(self, serialization: str) -> None:
            self.serialization = serialization

        @classmethod
        def from_qiskit_circuit(cls, circuit) -> "QuantumProgram":
            return cls(serialization=f"qasm://{circuit.num_qubits}")

        def to_json_dict(self) -> dict[str, str]:
            return {"serialization": self.serialization}

    class ClientData:
        def __init__(self, user_agent: str) -> None:
            self.user_agent = user_agent

        def to_json_dict(self) -> dict[str, str]:
            return {"user_agent": self.user_agent}

    class BackendData:
        def __init__(self, name: str, version: str | None = None, options=None) -> None:
            self.name = name
            self.version = version
            self.options = options

        def to_json_dict(self) -> dict[str, object]:
            return {
                "name": self.name,
                "version": self.version,
                "options": self.options,
            }

    class QuantumComputationModel:
        def __init__(self, programs, noise_model=None, client=None, backend=None) -> None:
            self.programs = programs
            self.noise_model = noise_model
            self.client = client
            self.backend = backend

        def to_json_str(self) -> str:
            return json.dumps(
                {
                    "programs": [program.to_json_dict() for program in self.programs],
                    "noise_model": self.noise_model,
                    "client": None if self.client is None else self.client.to_json_dict(),
                    "backend": None if self.backend is None else self.backend.to_json_dict(),
                }
            )

    class QuantumProgramResult:
        def __init__(self, payload: dict[str, object]) -> None:
            self.payload = payload

        @classmethod
        def from_json_str(cls, payload: str) -> "QuantumProgramResult":
            return cls(json.loads(payload))

        def to_qiskit_result(self):
            return Result.from_dict(self.payload)

    qio_core.BackendData = BackendData
    qio_core.ClientData = ClientData
    qio_core.QuantumComputationModel = QuantumComputationModel
    qio_core.QuantumProgram = QuantumProgram
    qio_core.QuantumProgramResult = QuantumProgramResult
    sys.modules["qio"] = qio
    sys.modules["qio.core"] = qio_core

    spec = importlib.util.spec_from_file_location(
        "aqora_cli.qiskit",
        MODULE_PATH,
        submodule_search_locations=[str(MODULE_PATH.parent)],
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules["aqora_cli.qiskit"] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class FakeClient:
    def __init__(self, *_args, **_kwargs) -> None:
        self.authenticated = False
        self.calls: list[tuple[str, dict[str, object]]] = []

    async def authenticate(self) -> None:
        self.authenticated = True

    async def send(self, query: str, **variables):
        self.calls.append((query, variables))
        if "providerPlatforms" in query:
            return {
                "providerPlatforms": {
                    "pageInfo": {
                        "hasNextPage": False,
                        "endCursor": None,
                    },
                    "nodes": [
                        {
                            "provider": "scaleway",
                            "name": "Scaleway QPU",
                            "id": "scw-qpu",
                            "vendor": "Scaleway",
                            "backend": "1.0",
                            "maxQubits": 12,
                            "maxShots": 1000,
                            "maxCircuits": 4,
                            "technology": "NEUTRAL_ATOM",
                            "isQpu": True,
                            "status": "AVAILABLE",
                        }
                    ],
                }
            }
        if "uploadProviderModelPayload" in query:
            return {
                "uploadProviderModelPayload": {
                    "providerModelUploadId": "upload-1",
                    "uploadUrl": "https://example.invalid/upload",
                }
            }
        if "createProviderModel" in query:
            return {"createProviderModel": {"id": "model-1", "provider": "scaleway"}}
        if "createProviderJob" in query:
            return {
                "createProviderJob": {
                    "id": "ProviderJob:job-1",
                    "provider": "scaleway",
                    "status": "WAITING",
                    "error": None,
                    "resultCount": None,
                    "createdAt": "2026-04-02T00:00:00Z",
                }
            }
        if "query ProviderJobResults" in query:
            return {
                "node": {
                    "__typename": "ProviderJob",
                    "results": {
                        "pageInfo": {
                            "hasNextPage": False,
                            "endCursor": None,
                        },
                        "nodes": [
                            {
                                "index": 1,
                                "error": None,
                                "result": "https://example.invalid/result-1",
                            },
                            {
                                "index": 0,
                                "error": None,
                                "result": "https://example.invalid/result-0",
                            },
                        ],
                    },
                }
            }
        if "query ProviderJob" in query:
            return {
                "node": {
                    "__typename": "ProviderJob",
                    "id": variables["id"],
                    "provider": "scaleway",
                    "status": "COMPLETED",
                    "error": None,
                    "duration": 1,
                    "resultCount": 1,
                    "createdAt": "2026-04-02T00:00:00Z",
                }
            }
        raise AssertionError(f"Unexpected query: {query}")


def test_provider_lists_platforms():
    mod = load_qiskit_module(FakeClient)

    provider = mod.AqoraProvider()
    backends = provider.backends()

    assert len(backends) == 1
    assert backends[0].name == "scaleway:scw-qpu"
    assert backends[0].platform.name == "Scaleway QPU"
    assert provider.get_backend("scw-qpu").name == "scaleway:scw-qpu"


def test_backend_run_uploads_qio_model(monkeypatch: pytest.MonkeyPatch):
    mod = load_qiskit_module(FakeClient)
    provider = mod.AqoraProvider()
    backend = provider.get_backend("scw-qpu")
    circuit = mod.QuantumCircuit(2)
    captured: dict[str, str] = {}

    def fake_put_json_payload(upload_url: str, payload: str) -> str:
        captured["upload_url"] = upload_url
        captured["payload"] = payload
        return '"etag-1"'

    monkeypatch.setattr(mod.client, "_put_json_payload", fake_put_json_payload)
    monkeypatch.setattr(mod.client, "_package_version", lambda: "9.9.9")

    job = backend.run(circuit)

    assert provider.client.authenticated
    assert job.job_id() == "ProviderJob:job-1"
    assert captured["upload_url"] == "https://example.invalid/upload"

    payload = json.loads(captured["payload"])
    assert payload["programs"] == [{"serialization": "qasm://2"}]
    assert payload["client"] == {"user_agent": "aqora-cli/9.9.9"}
    assert payload["backend"] == {"name": "1.0", "version": "1.0", "options": None}
    create_provider_job_calls = [
        variables
        for query, variables in provider.client.calls
        if "createProviderJob" in query
    ]
    assert create_provider_job_calls == [
        {
            "providerModelId": "model-1",
            "provider": "scaleway",
            "platformId": "scw-qpu",
            "shots": None,
        }
    ]


def test_backend_run_passes_shots(monkeypatch: pytest.MonkeyPatch):
    mod = load_qiskit_module(FakeClient)
    provider = mod.AqoraProvider()
    backend = provider.get_backend("scw-qpu")

    monkeypatch.setattr(mod.client, "_put_json_payload", lambda *_args, **_kwargs: '"etag-1"')

    backend.run(mod.QuantumCircuit(1), shots=512)

    create_provider_job_calls = [
        variables
        for query, variables in provider.client.calls
        if "createProviderJob" in query
    ]
    assert create_provider_job_calls[-1]["shots"] == 512


def test_backend_run_rejects_unsupported_run_options():
    mod = load_qiskit_module(FakeClient)
    backend = mod.AqoraProvider().get_backend("scw-qpu")

    with pytest.raises(NotImplementedError, match="only supports `shots` as a per-run parameter"):
        backend.run(mod.QuantumCircuit(1), memory=True)


def test_job_result_downloads_and_merges_backend_results(monkeypatch: pytest.MonkeyPatch):
    mod = load_qiskit_module(FakeClient)
    backend = mod.AqoraProvider().get_backend("scw-qpu")
    job = mod.AqoraJob(backend, "ProviderJob:job-1")
    payloads = {
        "https://example.invalid/result-0": json.dumps(
            {
                "results": [{"header": {"name": "circuit-0"}}],
                "success": True,
                "job_id": "provider-result-0",
            }
        ),
        "https://example.invalid/result-1": json.dumps(
            {
                "results": [{"header": {"name": "circuit-1"}}],
                "success": True,
                "job_id": "provider-result-1",
            }
        ),
    }

    monkeypatch.setattr(mod.client, "_download_text", payloads.__getitem__)

    result = job.result(timeout=0.01, wait=0)

    assert result.to_dict()["job_id"] == "ProviderJob:job-1"
    assert [entry["header"]["name"] for entry in result.to_dict()["results"]] == [
        "circuit-0",
        "circuit-1",
    ]


def test_result_item_with_error_raises():
    mod = load_qiskit_module(FakeClient)

    item = mod.ProviderJobResultItem(index=0, error="boom", result_url=None)
    with pytest.raises(RuntimeError, match="boom"):
        item.to_qiskit_result()


def test_unknown_status_falls_back_to_running():
    mod = load_qiskit_module(FakeClient)
    backend = mod.AqoraProvider().get_backend("scw-qpu")
    job = mod.AqoraJob(backend, "ProviderJob:job-1")

    status = job._job_status("SOME_NEW_SERVER_STATUS", None)

    from qiskit.providers.jobstatus import JobStatus

    assert status == JobStatus.RUNNING


def test_put_json_payload_rejects_non_http_url():
    mod = load_qiskit_module(FakeClient)

    with pytest.raises(ValueError, match="http or https"):
        mod.client._put_json_payload("file:///etc/passwd", "{}")


def test_download_text_rejects_non_http_url():
    mod = load_qiskit_module(FakeClient)

    with pytest.raises(ValueError, match="http or https"):
        mod.client._download_text("file:///etc/passwd")
