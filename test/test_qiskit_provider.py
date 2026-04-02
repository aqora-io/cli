from __future__ import annotations

import importlib.util
import json
import sys
import time
import types
from enum import Enum
from pathlib import Path

import pytest


MODULE_PATH = (
    Path(__file__).resolve().parents[1] / "python" / "aqora" / "qiskit" / "__init__.py"
)

_FAKE_MODULE_PREFIXES = ("aqora.", "qiskit", "qio")


def load_qiskit_module(fake_client_cls: type):
    for name in list(sys.modules):
        if name == "aqora" or name.startswith(_FAKE_MODULE_PREFIXES):
            sys.modules.pop(name, None)

    aqora = types.ModuleType("aqora")
    aqora.__path__ = [str(MODULE_PATH.parents[1])]
    aqora.Client = fake_client_cls
    sys.modules["aqora"] = aqora

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

    qiskit_jobstatus = types.ModuleType("qiskit.providers.jobstatus")

    class JobStatus(Enum):
        INITIALIZING = "INITIALIZING"
        QUEUED = "QUEUED"
        RUNNING = "RUNNING"
        DONE = "DONE"
        ERROR = "ERROR"
        CANCELLED = "CANCELLED"

    JOB_FINAL_STATES = (JobStatus.DONE, JobStatus.CANCELLED, JobStatus.ERROR)

    qiskit_jobstatus.JobStatus = JobStatus
    qiskit_jobstatus.JOB_FINAL_STATES = JOB_FINAL_STATES
    sys.modules["qiskit.providers.jobstatus"] = qiskit_jobstatus

    qiskit_providers = types.ModuleType("qiskit.providers")

    class JobError(Exception):
        pass

    class JobTimeoutError(JobError):
        pass

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
        version = 1
        _async = True

        def __init__(self, backend, job_id) -> None:
            self._backend = backend
            self._job_id = job_id

        def job_id(self) -> str:
            return self._job_id

        def backend(self):
            return self._backend

        def cancelled(self) -> bool:
            return self.status() == JobStatus.CANCELLED

        # Mirrors qiskit's JobV1.wait_for_final_state so that tests exercise
        # the same inherited behavior the real base class provides.
        def wait_for_final_state(self, timeout=None, wait=5, callback=None) -> None:
            start_time = time.time()
            status = self.status()
            while status not in JOB_FINAL_STATES:
                elapsed_time = time.time() - start_time
                if timeout is not None and elapsed_time >= timeout:
                    raise JobTimeoutError(f"Timeout while waiting for job {self.job_id()}.")
                if callback:
                    callback(self.job_id(), status, self)
                time.sleep(wait)
                status = self.status()

    qiskit_providers.JobError = JobError
    qiskit_providers.JobTimeoutError = JobTimeoutError
    qiskit_providers.Options = Options
    qiskit_providers.BackendV2 = BackendV2
    qiskit_providers.JobV1 = JobV1
    sys.modules["qiskit.providers"] = qiskit_providers

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
        "aqora.qiskit",
        MODULE_PATH,
        submodule_search_locations=[str(MODULE_PATH.parent)],
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules["aqora.qiskit"] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class FakeClient:
    def __init__(self, *_args, **_kwargs) -> None:
        self.authenticated = False
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.uploads: list[tuple[str, bytes]] = []
        self.job_status: str | None = "COMPLETED"
        self.job_error: str | None = None
        # `result_count` of None means "match the number of result nodes".
        self.result_count: int | None = None
        self.result_pages: list[list[dict[str, object]]] = [
            [
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
            ]
        ]
        self.payloads: dict[str, str] = {
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
        self.platform_page_info: dict[str, object] = {
            "hasNextPage": False,
            "endCursor": None,
        }
        self.platforms: list[dict[str, object]] = [
            {
                "id": "ProviderPlatform:a",
                "name": "platform-a",
                "meta": {"maxQubits": 3, "maxShots": 1000, "maxCircuits": 10},
            },
            {
                "id": "ProviderPlatform:b",
                "name": "platform-b",
                "meta": {"maxQubits": 7, "maxShots": None, "maxCircuits": None},
            },
        ]

    async def authenticate(self) -> None:
        self.authenticated = True

    async def s3_put(self, url: str, body: bytes) -> str:
        self.uploads.append((url, body))
        return '"etag-1"'

    async def s3_get(self, url: str) -> bytes:
        return self.payloads[url].encode("utf-8")

    def _effective_result_count(self) -> int:
        if self.result_count is not None:
            return self.result_count
        return sum(len(page) for page in self.result_pages)

    async def send(self, query: str, **variables):
        self.calls.append((query, variables))
        if "uploadProviderModelPayload" in query:
            return {
                "uploadProviderModelPayload": {
                    "providerModelUploadId": "upload-1",
                    "uploadUrl": "https://example.invalid/upload",
                }
            }
        if "createProviderModel" in query:
            return {"createProviderModel": {"id": "model-1"}}
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
            after = variables.get("after")
            page_index = 0 if after is None else int(str(after).rsplit("-", 1)[1]) + 1
            has_next = page_index < len(self.result_pages) - 1
            return {
                "node": {
                    "__typename": "ProviderJob",
                    "results": {
                        "pageInfo": {
                            "hasNextPage": has_next,
                            "endCursor": f"cursor-{page_index}" if has_next else None,
                        },
                        "nodes": self.result_pages[page_index],
                    },
                }
            }
        if "query ProviderPlatforms" in query:
            return {
                "providerPlatforms": {
                    "pageInfo": self.platform_page_info,
                    "nodes": self.platforms,
                }
            }
        if "query ProviderJob" in query:
            return {
                "node": {
                    "__typename": "ProviderJob",
                    "id": variables["id"],
                    "provider": "scaleway",
                    "status": self.job_status,
                    "error": self.job_error,
                    "duration": 1,
                    "resultCount": self._effective_result_count(),
                    "createdAt": "2026-04-02T00:00:00Z",
                }
            }
        raise AssertionError(f"Unexpected query: {query}")


@pytest.fixture
def mod():
    saved = {
        name: module
        for name, module in sys.modules.items()
        if name == "aqora" or name.startswith(_FAKE_MODULE_PREFIXES)
    }
    try:
        yield load_qiskit_module(FakeClient)
    finally:
        for name in list(sys.modules):
            if name == "aqora" or name.startswith(_FAKE_MODULE_PREFIXES):
                sys.modules.pop(name, None)
        sys.modules.update(saved)


def test_qpu_run_uploads_qio_model(mod, monkeypatch: pytest.MonkeyPatch):
    qpu = mod.QPU()
    circuit = mod.QuantumCircuit(2)

    monkeypatch.setattr(mod.client, "_package_version", lambda: "9.9.9")

    job = qpu.run(circuit)

    assert qpu.client.authenticated
    assert job.job_id() == "ProviderJob:job-1"

    assert len(qpu.client.uploads) == 1
    upload_url, payload_bytes = qpu.client.uploads[0]
    assert upload_url == "https://example.invalid/upload"

    payload = json.loads(payload_bytes)
    assert payload["programs"] == [{"serialization": "qasm://2"}]
    assert payload["client"] == {"user_agent": "aqora/9.9.9"}
    assert payload["backend"] == {"name": "aqora-qpu", "version": None, "options": None}

    create_provider_model_calls = [
        variables
        for query, variables in qpu.client.calls
        if "createProviderModel" in query
    ]
    assert create_provider_model_calls == [
        {"providerModelUploadId": "upload-1", "etag": '"etag-1"'}
    ]

    create_provider_job_calls = [
        variables
        for query, variables in qpu.client.calls
        if "createProviderJob" in query
    ]
    assert create_provider_job_calls == [
        {
            "providerModelId": "model-1",
            "shots": None,
            "providerPlatform": None,
        }
    ]


def test_qpu_run_passes_shots(mod):
    qpu = mod.QPU()

    qpu.run(mod.QuantumCircuit(1), shots=512)

    create_provider_job_calls = [
        variables
        for query, variables in qpu.client.calls
        if "createProviderJob" in query
    ]
    assert create_provider_job_calls[-1]["shots"] == 512


def test_qpu_run_passes_provider_platform(mod):
    qpu = mod.QPU(platform="platform-b")

    qpu.run(mod.QuantumCircuit(1))

    create_provider_job_calls = [
        variables
        for query, variables in qpu.client.calls
        if "createProviderJob" in query
    ]
    assert create_provider_job_calls[-1]["providerPlatform"] == "platform-b"


def test_qpu_run_accepts_sampler_run_options(mod):
    # BackendSamplerV2 always calls backend.run(..., memory=True,
    # seed_simulator=None); neither may be rejected.
    qpu = mod.QPU()

    job = qpu.run(mod.QuantumCircuit(1), shots=8, memory=True, seed_simulator=None)

    assert job.job_id() == "ProviderJob:job-1"


def test_qpu_run_rejects_unsupported_run_options(mod):
    qpu = mod.QPU()

    with pytest.raises(NotImplementedError, match="seed_simulator"):
        qpu.run(mod.QuantumCircuit(1), seed_simulator=1234)


def test_qpu_run_rejects_falsy_unsupported_options(mod):
    qpu = mod.QPU()

    with pytest.raises(NotImplementedError, match="seed_simulator"):
        qpu.run(mod.QuantumCircuit(1), seed_simulator=0)
    with pytest.raises(NotImplementedError, match="rep_delay"):
        qpu.run(mod.QuantumCircuit(1), rep_delay=0.0)


def test_qpu_run_rejects_non_integer_shots(mod):
    qpu = mod.QPU()

    with pytest.raises(TypeError, match="integer"):
        qpu.run(mod.QuantumCircuit(1), shots=True)
    with pytest.raises(TypeError, match="integer"):
        qpu.run(mod.QuantumCircuit(1), shots=512.5)


def test_job_backend_is_a_method(mod):
    qpu = mod.QPU()
    job = mod.QPUJob(qpu, "ProviderJob:job-1")

    assert job.backend() is qpu


def test_job_result_downloads_and_merges_backend_results(mod):
    qpu = mod.QPU()
    job = mod.QPUJob(qpu, "ProviderJob:job-1")

    result = job.result(timeout=0.01, wait=0)

    assert result.to_dict()["job_id"] == "ProviderJob:job-1"
    assert [entry["header"]["name"] for entry in result.to_dict()["results"]] == [
        "circuit-0",
        "circuit-1",
    ]


def test_job_result_single_result_sets_job_id(mod):
    qpu = mod.QPU()
    qpu.client.result_pages = [
        [
            {
                "index": 0,
                "error": None,
                "result": "https://example.invalid/result-0",
            }
        ]
    ]
    job = mod.QPUJob(qpu, "ProviderJob:job-1")

    result = job.result(timeout=0.01, wait=0)

    assert result.to_dict()["job_id"] == "ProviderJob:job-1"


def test_job_result_paginates_all_result_pages(mod):
    qpu = mod.QPU()
    qpu.client.result_pages = [
        [
            {
                "index": 1,
                "error": None,
                "result": "https://example.invalid/result-1",
            }
        ],
        [
            {
                "index": 0,
                "error": None,
                "result": "https://example.invalid/result-0",
            }
        ],
    ]
    job = mod.QPUJob(qpu, "ProviderJob:job-1")

    result = job.result(timeout=0.01, wait=0)

    assert [entry["header"]["name"] for entry in result.to_dict()["results"]] == [
        "circuit-0",
        "circuit-1",
    ]


def test_job_result_count_mismatch_raises(mod):
    qpu = mod.QPU()
    qpu.client.result_count = 3
    job = mod.QPUJob(qpu, "ProviderJob:job-1")

    with pytest.raises(mod.job.JobError, match="2 of 3"):
        job.result(timeout=0.01, wait=0)


def test_job_result_failed_job_raises_job_error(mod):
    qpu = mod.QPU()
    qpu.client.job_error = "device on fire"
    job = mod.QPUJob(qpu, "ProviderJob:job-1")

    with pytest.raises(mod.job.JobError, match="device on fire"):
        job.result(timeout=0.01, wait=0)


def test_job_result_cancelled_job_raises_job_error(mod):
    qpu = mod.QPU()
    qpu.client.job_status = "CANCELLED"
    job = mod.QPUJob(qpu, "ProviderJob:job-1")

    with pytest.raises(mod.job.JobError, match="cancelled"):
        job.result(timeout=0.01, wait=0)


def test_result_item_with_error_raises(mod):
    qpu = mod.QPU()

    item = mod.ProviderJobResultItem(index=0, error="boom", result_url=None)
    with pytest.raises(mod.job.JobError, match="boom"):
        item.to_qiskit_result(qpu._graphql)


def test_unknown_status_falls_back_to_running(mod):
    qpu = mod.QPU()
    job = mod.QPUJob(qpu, "ProviderJob:job-1")

    status = job._job_status("SOME_NEW_SERVER_STATUS", None)

    from qiskit.providers.jobstatus import JobStatus

    assert status == JobStatus.RUNNING


def test_target_uses_max_platform_qubits(mod):
    qpu = mod.QPU()

    assert qpu.target.num_qubits == 7

    # The target is cached: a second access must not query the server again.
    assert qpu.target.num_qubits == 7
    platform_queries = [
        query for query, _ in qpu.client.calls if "query ProviderPlatforms" in query
    ]
    assert len(platform_queries) == 1


def test_target_uses_selected_platform_qubits(mod):
    qpu = mod.QPU(platform="platform-a")

    assert qpu.target.num_qubits == 3


def test_target_selects_platform_by_id(mod):
    qpu = mod.QPU(platform="ProviderPlatform:b")

    assert qpu.target.num_qubits == 7


def test_target_unknown_platform_raises(mod):
    qpu = mod.QPU(platform="no-such-platform")

    with pytest.raises(LookupError, match="no-such-platform"):
        _ = qpu.target


def test_target_raises_without_platform_metadata(mod):
    qpu = mod.QPU()
    qpu.client.platforms = [{"name": "platform-a", "meta": {"maxQubits": None}}]

    with pytest.raises(RuntimeError, match="qubit count"):
        _ = qpu.target


def test_qpu_rejects_client_combined_with_url_options(mod):
    with pytest.raises(ValueError, match="explicit `client`"):
        mod.QPU(FakeClient(), url="https://example.invalid")
    with pytest.raises(ValueError, match="explicit `client`"):
        mod.QPU(FakeClient(), allow_insecure_host=True)


def test_run_sync_times_out(mod):
    import asyncio

    with pytest.raises(TimeoutError, match="did not complete"):
        mod.client._run_sync(lambda: asyncio.sleep(60), timeout=0.05)


def test_empty_string_error_is_treated_as_error(mod):
    qpu = mod.QPU()
    job = mod.QPUJob(qpu, "ProviderJob:job-1")

    from qiskit.providers.jobstatus import JobStatus

    assert job._job_status("COMPLETED", "") == JobStatus.ERROR

    item = mod.ProviderJobResultItem(index=0, error="", result_url=None)
    with pytest.raises(mod.job.JobError, match="failed"):
        item.to_qiskit_result(qpu._graphql)


def test_platform_pagination_must_advance(mod):
    qpu = mod.QPU()
    qpu.client.platform_page_info = {"hasNextPage": True, "endCursor": None}

    with pytest.raises(RuntimeError, match="did not advance"):
        _ = qpu.target


def test_upload_payload_rejects_non_http_url(mod):
    graphql = mod.client.AqoraGraphQLClient(FakeClient())

    with pytest.raises(ValueError, match="http or https"):
        graphql.upload_payload("file:///etc/passwd", "{}")


def test_download_text_rejects_non_http_url(mod):
    graphql = mod.client.AqoraGraphQLClient(FakeClient())

    with pytest.raises(ValueError, match="http or https"):
        graphql.download_text("file:///etc/passwd")
