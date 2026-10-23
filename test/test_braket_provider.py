from __future__ import annotations

import abc
import base64
import importlib.util
import json
import sys
import types
import zlib
from pathlib import Path

import pytest

# The real native extension (built by `uv sync` / maturin): the provider wire
# format is implemented in Rust, so it is kept registered while the rest of
# the `aqora` package is faked.
import aqora._aqora as _AQORA_NATIVE


MODULE_PATH = (
    Path(__file__).resolve().parents[1] / "python" / "aqora" / "braket" / "__init__.py"
)

_FAKE_MODULE_PREFIXES = ("aqora.", "braket")

QASM3_SOURCE = "OPENQASM 3.0;\nbit[2] b;\nqubit[2] q;\nh q[0];\ncnot q[0], q[1];\nb = measure q;"

BRAKET_RESULT = {
    "braketSchemaHeader": {
        "name": "braket.task_result.gate_model_task_result",
        "version": "1",
    },
    "measuredQubits": [0, 1],
    "measurements": [[0, 0], [1, 1], [1, 1]],
    "taskMetadata": {"shots": 3},
}


def _install_braket() -> None:
    """Register fakes of the Braket SDK pieces `aqora.braket` touches.

    `Device` and `QuantumTask` keep the real SDK's abstract methods, so a shim
    class that misses one fails to instantiate here just as it would for real.
    """
    for name in ("braket", "braket.circuits", "braket.devices", "braket.tasks"):
        module = types.ModuleType(name)
        module.__path__ = []
        sys.modules[name] = module

    class Circuit:
        def to_ir(self, ir_type):
            assert ir_type == "OPENQASM"
            return types.SimpleNamespace(source=QASM3_SOURCE)

    class Device(abc.ABC):
        def __init__(self, name: str, status: str) -> None:
            self._name = name
            self._status = status

        @abc.abstractmethod
        def run(self, task_specification, shots, inputs, *args, **kwargs): ...

        @abc.abstractmethod
        def run_batch(self, task_specifications, shots, max_parallel, inputs, *args, **kwargs): ...

        @property
        def name(self) -> str:
            return self._name

    class QuantumTask(abc.ABC):
        @property
        @abc.abstractmethod
        def id(self) -> str: ...

        @abc.abstractmethod
        def cancel(self) -> None: ...

        @abc.abstractmethod
        def state(self) -> str: ...

        @abc.abstractmethod
        def result(self): ...

        @abc.abstractmethod
        def async_result(self): ...

    class GateModelQuantumTaskResult:
        def __init__(self, data) -> None:
            self.data = data

        @classmethod
        def from_string(cls, raw: str) -> "GateModelQuantumTaskResult":
            return cls(json.loads(raw))

    sys.modules["braket.circuits"].Circuit = Circuit
    sys.modules["braket.devices"].Device = Device
    sys.modules["braket.tasks"].QuantumTask = QuantumTask
    sys.modules["braket.tasks"].GateModelQuantumTaskResult = GateModelQuantumTaskResult
    serialization = types.ModuleType("braket.circuits.serialization")
    serialization.IRType = types.SimpleNamespace(OPENQASM="OPENQASM")
    sys.modules["braket.circuits.serialization"] = serialization


def load_braket_module(fake_client_cls: type):
    for name in list(sys.modules):
        if name == "aqora" or name.startswith(_FAKE_MODULE_PREFIXES):
            sys.modules.pop(name, None)

    aqora = types.ModuleType("aqora")
    aqora.__path__ = [str(MODULE_PATH.parents[1])]
    aqora.Client = fake_client_cls
    aqora._aqora = _AQORA_NATIVE
    sys.modules["aqora"] = aqora
    sys.modules["aqora._aqora"] = _AQORA_NATIVE

    _install_braket()

    spec = importlib.util.spec_from_file_location(
        "aqora.braket",
        MODULE_PATH,
        submodule_search_locations=[str(MODULE_PATH.parent)],
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules["aqora.braket"] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _result_payload(serialization: str, serialization_format: int) -> str:
    return json.dumps(
        {
            "compression_format": 1,
            "serialization_format": serialization_format,
            "serialization": serialization,
        }
    )


class FakeClient:
    def __init__(self, *_args, **_kwargs) -> None:
        self.authenticated = False
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.uploads: list[tuple[str, bytes, str | None]] = []
        self.job_status: str | None = "COMPLETED"
        self.job_error: str | None = None
        self.payloads: dict[str, str] = {
            "https://example.invalid/result-0": _result_payload(
                json.dumps(BRAKET_RESULT), 1003
            ),
        }

    async def authenticate(self) -> None:
        self.authenticated = True

    async def s3_put(self, url: str, body: bytes, *, content_type: str | None = None) -> str:
        self.uploads.append((url, body, content_type))
        return '"etag-1"'

    async def s3_get(self, url: str) -> bytes:
        return self.payloads[url].encode("utf-8")

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
                    "provider": "braket",
                    "status": "WAITING",
                    "error": None,
                    "resultCount": None,
                    "createdAt": "2026-10-22T00:00:00Z",
                }
            }
        if "query ProviderJobResults" in query:
            return {
                "node": {
                    "__typename": "ProviderJob",
                    "results": {
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                        "nodes": [
                            {
                                "index": 0,
                                "error": None,
                                "result": "https://example.invalid/result-0",
                            }
                        ],
                    },
                }
            }
        if "query ProviderJob" in query:
            return {
                "node": {
                    "__typename": "ProviderJob",
                    "id": variables["id"],
                    "provider": "braket",
                    "platform": {
                        "id": "ProviderPlatform:sv1",
                        "name": "SV1",
                        "provider": "BRAKET",
                    },
                    "status": self.job_status,
                    "error": self.job_error,
                    "duration": 1,
                    "resultCount": 1,
                    "createdAt": "2026-10-22T00:00:00Z",
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
        yield load_braket_module(FakeClient)
    finally:
        for name in list(sys.modules):
            if name == "aqora" or name.startswith(_FAKE_MODULE_PREFIXES):
                sys.modules.pop(name, None)
        sys.modules.update(saved)


def _circuit():
    return sys.modules["braket.circuits"].Circuit()


def _uploaded_program(qpu) -> dict[str, object]:
    (upload,) = qpu.client.uploads
    (program,) = json.loads(upload[1])["programs"]
    return program


def _job_variables(qpu) -> dict[str, object]:
    (variables,) = [v for q, v in qpu.client.calls if "createProviderJob" in q]
    return variables


def test_run_submits_the_circuit_as_openqasm3(mod):
    qpu = mod.QPU("braket:SV1", compress=False)

    task = qpu.run(_circuit(), shots=3)

    program = _uploaded_program(qpu)
    assert program["serialization_format"] == 3  # QASM_V3
    assert program["serialization"] == QASM3_SOURCE
    assert _job_variables(qpu)["shots"] == 3
    assert task.id == "ProviderJob:job-1"
    assert task.device() is qpu


def test_run_compresses_by_default(mod):
    qpu = mod.QPU("braket:SV1")
    qpu.run(_circuit(), shots=3)

    program = _uploaded_program(qpu)
    assert program["compression_format"] == 2
    assert (
        zlib.decompress(base64.b64decode(program["serialization"])).decode("utf-8")
        == QASM3_SOURCE
    )


def test_shim_classes_are_braket_types(mod):
    qpu = mod.QPU("braket:SV1")
    task = qpu.run(_circuit(), shots=1)

    assert isinstance(qpu, sys.modules["braket.devices"].Device)
    assert isinstance(task, sys.modules["braket.tasks"].QuantumTask)
    assert qpu.name == "braket:SV1"


def test_result_decodes_the_braket_task_result(mod):
    task = mod.QPU("braket:SV1").run(_circuit(), shots=3)

    result = task.result(wait=0)

    assert isinstance(result, sys.modules["braket.tasks"].GateModelQuantumTaskResult)
    assert result.data == BRAKET_RESULT
    (item,) = task.result_items()
    assert item.counts() == {"00": 1, "11": 2}


def test_result_raises_on_failure(mod):
    qpu = mod.QPU("braket:SV1")
    task = qpu.run(_circuit(), shots=3)
    qpu.client.job_status = None
    qpu.client.job_error = "device offline"

    with pytest.raises(RuntimeError, match="device offline"):
        task.result(wait=0)


@pytest.mark.parametrize(
    ("status", "error", "state"),
    [
        (None, None, "CREATED"),
        ("WAITING", None, "QUEUED"),
        ("RUNNING", None, "RUNNING"),
        ("COMPLETED", None, "COMPLETED"),
        ("CANCELLED", None, "CANCELLED"),
        (None, "boom", "FAILED"),
    ],
)
def test_state_maps_job_status(mod, status, error, state):
    qpu = mod.QPU("braket:SV1")
    task = qpu.run(_circuit(), shots=1)
    qpu.client.job_status = status
    qpu.client.job_error = error

    assert task.state() == state


def test_run_requires_shots(mod):
    with pytest.raises(ValueError):
        mod.QPU("braket:SV1").run(_circuit())


def test_run_rejects_non_circuits(mod):
    with pytest.raises(TypeError, match="Circuit"):
        mod.QPU("braket:SV1").run("OPENQASM 3.0;", shots=1)


def test_run_rejects_inputs_and_unknown_arguments(mod):
    qpu = mod.QPU("braket:SV1")
    with pytest.raises(NotImplementedError, match="inputs"):
        qpu.run(_circuit(), shots=1, inputs={"theta": 0.1})
    with pytest.raises(NotImplementedError, match="poll_timeout_seconds"):
        qpu.run(_circuit(), shots=1, poll_timeout_seconds=10)
    assert qpu.client.uploads == []


def test_run_batch_is_unsupported(mod):
    with pytest.raises(NotImplementedError):
        mod.QPU("braket:SV1").run_batch([_circuit()], shots=1)


def test_from_id_builds_the_device_from_the_job_platform(mod):
    task = mod.QPUTask.from_id("ProviderJob:job-1")

    assert task.id == "ProviderJob:job-1"
    assert task.device().platform == "braket:SV1"


def test_to_braket_result_rejects_other_formats(mod):
    results = sys.modules["aqora._provider.results"]
    item = results.ProviderResult(index=0, serialization_format=2, raw="{}")

    with pytest.raises(ValueError, match="Braket gate-model task result"):
        item.to_braket_result()
