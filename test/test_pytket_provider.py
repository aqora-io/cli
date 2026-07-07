from __future__ import annotations

import base64
import importlib.util
import json
import sys
import types
import zlib
from enum import Enum
from pathlib import Path

import pytest

# The real native extension (built by `uv sync` / maturin): the provider wire
# format is implemented in Rust, so it is kept registered while the rest of
# the `aqora` package is faked.
import aqora._aqora as _AQORA_NATIVE


MODULE_PATH = (
    Path(__file__).resolve().parents[1] / "python" / "aqora" / "pytket" / "__init__.py"
)

_FAKE_MODULE_PREFIXES = ("aqora.", "pytket")


def load_pytket_module(fake_client_cls: type):
    for name in list(sys.modules):
        if name == "aqora" or name.startswith(_FAKE_MODULE_PREFIXES):
            sys.modules.pop(name, None)

    aqora = types.ModuleType("aqora")
    aqora.__path__ = [str(MODULE_PATH.parents[1])]
    aqora.Client = fake_client_cls
    aqora._aqora = _AQORA_NATIVE
    sys.modules["aqora"] = aqora
    sys.modules["aqora._aqora"] = _AQORA_NATIVE

    pytket = types.ModuleType("pytket")
    sys.modules["pytket"] = pytket

    pytket_circuit = types.ModuleType("pytket.circuit")

    class Circuit:
        def __init__(self, n_qubits: int = 0, name: str | None = None) -> None:
            self.n_qubits = n_qubits
            self.name = name

        def to_dict(self) -> dict[str, object]:
            return {"phase": "0.0", "qubits": self.n_qubits}

    class OpType(Enum):
        PhasedX = "PhasedX"
        Rz = "Rz"
        ZZPhase = "ZZPhase"
        ZZMax = "ZZMax"
        Measure = "Measure"
        Reset = "Reset"

    pytket_circuit.Circuit = Circuit
    pytket_circuit.OpType = OpType
    pytket.Circuit = Circuit
    pytket.OpType = OpType
    sys.modules["pytket.circuit"] = pytket_circuit

    pytket_backend_exceptions = types.ModuleType("pytket.backends.backend_exceptions")

    class CircuitNotRunError(Exception):
        def __init__(self, handle) -> None:
            super().__init__(f"Circuit corresponding to {handle!r} has not been run")

    pytket_backend_exceptions.CircuitNotRunError = CircuitNotRunError
    sys.modules["pytket.backends.backend_exceptions"] = pytket_backend_exceptions

    pytket_backendresult = types.ModuleType("pytket.backends.backendresult")

    class BackendResult:
        def __init__(self, payload: dict[str, object]) -> None:
            self._payload = payload

        @classmethod
        def from_dict(cls, payload: dict[str, object]) -> "BackendResult":
            allowed = {
                "qubits",
                "bits",
                "shots",
                "counts",
                "state",
                "unitary",
                "density_matrix",
                "ppcirc",
            }
            unexpected = set(payload) - allowed
            if unexpected:
                raise KeyError(f"Unexpected keys: {sorted(unexpected)}")
            return cls(payload)

        def to_dict(self) -> dict[str, object]:
            return self._payload

    pytket_backendresult.BackendResult = BackendResult
    sys.modules["pytket.backends.backendresult"] = pytket_backendresult

    pytket_backendinfo = types.ModuleType("pytket.backends.backendinfo")

    class BackendInfo:
        def __init__(
            self,
            name,
            device_name,
            version,
            architecture,
            gate_set,
            n_cl_reg=None,
            **kwargs,
        ) -> None:
            self.name = name
            self.device_name = device_name
            self.version = version
            self.architecture = architecture
            self.gate_set = gate_set
            self.n_cl_reg = n_cl_reg
            self.__dict__.update(kwargs)

    pytket_backendinfo.BackendInfo = BackendInfo
    sys.modules["pytket.backends.backendinfo"] = pytket_backendinfo

    pytket_backends = types.ModuleType("pytket.backends")

    class StatusEnum(Enum):
        COMPLETED = "COMPLETED"
        QUEUED = "QUEUED"
        SUBMITTED = "SUBMITTED"
        RUNNING = "RUNNING"
        CANCELLING = "CANCELLING"
        CANCELLED = "CANCELLED"
        ERROR = "ERROR"

    class CircuitStatus:
        def __init__(self, status: StatusEnum, message: str = "") -> None:
            self.status = status
            self.message = message

        def __eq__(self, other) -> bool:
            return (
                isinstance(other, CircuitStatus)
                and self.status == other.status
                and self.message == other.message
            )

    class ResultHandle(tuple):
        def __new__(cls, *args):
            return super().__new__(cls, args)

    class Backend:
        _supports_shots = False
        _supports_counts = False
        _persistent_handles = False

        def __init__(self) -> None:
            self._cache: dict[ResultHandle, dict[str, object]] = {}

        def _check_all_circuits(self, circuits, nomeasure_warn=None) -> bool:
            self.checked_circuits = list(circuits)
            for circuit in circuits:
                for predicate in self.required_predicates:
                    if not predicate.verify(circuit):
                        raise ValueError(f"Circuit fails predicate: {predicate!r}")
            return True

        def get_result(self, handle, **kwargs):
            if handle in self._cache and "result" in self._cache[handle]:
                return self._cache[handle]["result"]
            raise CircuitNotRunError(handle)

    pytket_backends.Backend = Backend
    pytket_backends.CircuitStatus = CircuitStatus
    pytket_backends.ResultHandle = ResultHandle
    pytket_backends.StatusEnum = StatusEnum
    sys.modules["pytket.backends"] = pytket_backends

    pytket_passes = types.ModuleType("pytket.passes")

    class BasePass:
        pass

    class SequencePass(BasePass):
        def __init__(self, passes) -> None:
            self.passes = list(passes)

    class DecomposeBoxes(BasePass):
        pass

    class FullPeepholeOptimise(BasePass):
        pass

    class AutoRebase(BasePass):
        def __init__(self, gateset) -> None:
            self.gateset = set(gateset)

    pytket_passes.BasePass = BasePass
    pytket_passes.SequencePass = SequencePass
    pytket_passes.DecomposeBoxes = DecomposeBoxes
    pytket_passes.FullPeepholeOptimise = FullPeepholeOptimise
    pytket_passes.AutoRebase = AutoRebase
    sys.modules["pytket.passes"] = pytket_passes

    pytket_predicates = types.ModuleType("pytket.predicates")

    class Predicate:
        def verify(self, circuit) -> bool:
            return True

    class MaxNQubitsPredicate(Predicate):
        def __init__(self, n_qubits: int) -> None:
            self.n_qubits = n_qubits

        def verify(self, circuit) -> bool:
            return circuit.n_qubits <= self.n_qubits

    pytket_predicates.Predicate = Predicate
    pytket_predicates.MaxNQubitsPredicate = MaxNQubitsPredicate
    sys.modules["pytket.predicates"] = pytket_predicates

    spec = importlib.util.spec_from_file_location(
        "aqora.pytket",
        MODULE_PATH,
        submodule_search_locations=[str(MODULE_PATH.parent)],
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules["aqora.pytket"] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _zlib_b64(text: str) -> str:
    return base64.b64encode(zlib.compress(text.encode("utf-8"))).decode("ascii")


def _un_zlib_b64(text: str) -> str:
    return zlib.decompress(base64.b64decode(text)).decode("utf-8")


def _result_payload(
    serialization: str,
    serialization_format: int,
    *,
    compression_format: int = 1,
) -> str:
    if compression_format == 2:
        serialization = _zlib_b64(serialization)
    return json.dumps(
        {
            "compression_format": compression_format,
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
            "https://example.invalid/result-0": _result_payload(
                json.dumps({"qubits": [["q", [0]]], "shots": [[0]]}),
                1000,
            ),
            "https://example.invalid/result-1": _result_payload(
                json.dumps({"qubits": [["q", [1]]], "shots": [[1]]}),
                1000,
                compression_format=2,
            ),
        }
        self.platform_page_info: dict[str, object] = {
            "hasNextPage": False,
            "endCursor": None,
        }
        self.platforms: list[dict[str, object]] = [
            {
                "id": "ProviderPlatform:a",
                "name": "aer_simulator_statevector",
                "provider": "NEXUS",
                "meta": {"maxQubits": 40, "maxShots": 1000, "maxCircuits": 10},
            },
            {
                "id": "ProviderPlatform:b",
                "name": "Selene",
                "provider": "NEXUS",
                "meta": {"maxQubits": 26, "maxShots": None, "maxCircuits": None},
            },
        ]

    async def authenticate(self) -> None:
        self.authenticated = True

    async def s3_put(self, url: str, body: bytes, *, content_type: str | None = None) -> str:
        self.uploads.append((url, body, content_type))
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
                    "provider": "nexus",
                    "status": "WAITING",
                    "error": None,
                    "resultCount": None,
                    "createdAt": "2026-07-06T00:00:00Z",
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
                    "provider": "nexus",
                    "status": self.job_status,
                    "error": self.job_error,
                    "duration": 1,
                    "resultCount": self._effective_result_count(),
                    "createdAt": "2026-07-06T00:00:00Z",
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
        yield load_pytket_module(FakeClient)
    finally:
        for name in list(sys.modules):
            if name == "aqora" or name.startswith(_FAKE_MODULE_PREFIXES):
                sys.modules.pop(name, None)
        sys.modules.update(saved)


def test_qpu_process_circuits_uploads_and_returns_handles(mod):
    from pytket.circuit import Circuit

    qpu = mod.QPU(platform="aer_simulator_statevector")

    handles = qpu.process_circuits([Circuit(1), Circuit(2)], n_shots=100)

    assert [tuple(handle) for handle in handles] == [
        ("ProviderJob:job-1", 0),
        ("ProviderJob:job-1", 1),
    ]

    (upload,) = qpu.client.uploads
    payload = json.loads(upload[1])
    (program_0, program_1) = payload["programs"]
    assert program_0["serialization_format"] == 1000
    assert program_0["compression_format"] == 2
    assert json.loads(_un_zlib_b64(program_1["serialization"])) == {
        "phase": "0.0",
        "qubits": 2,
    }

    create_provider_job_calls = [
        variables for query, variables in qpu.client.calls if "createProviderJob" in query
    ]
    assert create_provider_job_calls == [
        {
            "providerModelId": "model-1",
            "shots": 100,
            "providerPlatform": "aer_simulator_statevector",
        }
    ]


def test_qpu_accepts_uniform_shot_sequence(mod):
    from pytket.circuit import Circuit

    qpu = mod.QPU()

    qpu.process_circuits([Circuit(1), Circuit(1)], n_shots=[64, 64])

    create_provider_job_calls = [
        variables for query, variables in qpu.client.calls if "createProviderJob" in query
    ]
    assert create_provider_job_calls[-1]["shots"] == 64


def test_qpu_rejects_non_uniform_shot_sequence(mod):
    from pytket.circuit import Circuit

    qpu = mod.QPU()

    with pytest.raises(NotImplementedError, match="per-circuit"):
        qpu.process_circuits([Circuit(1), Circuit(1)], n_shots=[64, 128])


def test_qpu_valid_check_enforces_max_qubits(mod):
    from pytket.circuit import Circuit

    qpu = mod.QPU(platform="Selene")

    with pytest.raises(ValueError, match="predicate"):
        qpu.process_circuits([Circuit(30)], n_shots=10)

    handles = qpu.process_circuits([Circuit(30)], n_shots=10, valid_check=False)
    assert len(handles) == 1


def test_qpu_circuit_status_mapping(mod):
    from pytket.backends import CircuitStatus, ResultHandle, StatusEnum

    qpu = mod.QPU()
    handle = ResultHandle("ProviderJob:job-1", 0)

    for server_status, expected in [
        ("WAITING", StatusEnum.QUEUED),
        ("RUNNING", StatusEnum.RUNNING),
        ("COMPLETED", StatusEnum.COMPLETED),
        ("CANCELLING", StatusEnum.CANCELLING),
        ("CANCELLED", StatusEnum.CANCELLED),
        (None, StatusEnum.SUBMITTED),
        ("SOME_NEW_SERVER_STATUS", StatusEnum.RUNNING),
    ]:
        qpu.client.job_status = server_status
        assert qpu.circuit_status(handle) == CircuitStatus(expected)


def test_qpu_circuit_status_error(mod):
    # The platform nulls the job status when the provider reports an error
    # state; the error field holds the provider's message.
    from pytket.backends import CircuitStatus, ResultHandle, StatusEnum

    qpu = mod.QPU()
    qpu.client.job_status = None
    qpu.client.job_error = "device on fire"

    status = qpu.circuit_status(ResultHandle("ProviderJob:job-1", 0))

    assert status == CircuitStatus(StatusEnum.ERROR, "device on fire")


def test_qpu_circuit_status_progress_message_is_not_an_error(mod):
    # The error field mirrors the provider's progress message for healthy
    # jobs; it is surfaced as the CircuitStatus message.
    from pytket.backends import CircuitStatus, ResultHandle, StatusEnum

    qpu = mod.QPU()
    qpu.client.job_status = "WAITING"
    qpu.client.job_error = "The job is queued."

    status = qpu.circuit_status(ResultHandle("ProviderJob:job-1", 0))

    assert status == CircuitStatus(StatusEnum.QUEUED, "The job is queued.")


def test_qpu_circuit_status_empty_error_is_not_an_error(mod):
    from pytket.backends import CircuitStatus, ResultHandle, StatusEnum

    qpu = mod.QPU()
    qpu.client.job_error = ""

    status = qpu.circuit_status(ResultHandle("ProviderJob:job-1", 0))

    assert status == CircuitStatus(StatusEnum.COMPLETED)


def test_qpu_get_result_populates_cache_for_all_indices(mod):
    from pytket.circuit import Circuit

    qpu = mod.QPU()
    handles = qpu.process_circuits([Circuit(1), Circuit(1)], n_shots=10)

    result_0 = qpu.get_result(handles[0], timeout=0.01, wait=0)
    result_1 = qpu.get_result(handles[1], timeout=0.01, wait=0)

    assert result_0.to_dict() == {"qubits": [["q", [0]]], "shots": [[0]]}
    assert result_1.to_dict() == {"qubits": [["q", [1]]], "shots": [[1]]}

    # The second handle must be served from the cache without re-fetching.
    result_queries = [
        query for query, _ in qpu.client.calls if "query ProviderJobResults" in query
    ]
    assert len(result_queries) == 1


def test_qpu_get_result_tolerates_sibling_error(mod):
    # One failed circuit must not block retrieving a successful sibling sharing
    # the job; only requesting the failed handle raises.
    from pytket.circuit import Circuit

    qpu = mod.QPU()
    handles = qpu.process_circuits([Circuit(1), Circuit(1)], n_shots=10)
    qpu.client.result_pages = [
        [
            {"index": 0, "error": None, "result": "https://example.invalid/result-0"},
            {"index": 1, "error": "circuit 1 failed", "result": None},
        ]
    ]

    result_0 = qpu.get_result(handles[0], timeout=0.01, wait=0)
    assert result_0.to_dict() == {"qubits": [["q", [0]]], "shots": [[0]]}

    with pytest.raises(RuntimeError, match="circuit 1 failed"):
        qpu.get_result(handles[1], timeout=0.01, wait=0)


def test_qpu_rejects_non_positive_shots(mod):
    # Shots normalization is shared with the guppy backend: bools and values
    # below 1 are rejected consistently.
    from pytket.circuit import Circuit

    qpu = mod.QPU()

    with pytest.raises(ValueError, match="at least 1"):
        qpu.process_circuits([Circuit(1)], n_shots=0)
    with pytest.raises(TypeError, match="integer"):
        qpu.process_circuits([Circuit(1)], n_shots=True)


def test_qpu_get_result_raises_on_error(mod):
    from pytket.circuit import Circuit

    qpu = mod.QPU()
    (handle,) = qpu.process_circuits([Circuit(1)], n_shots=10)
    qpu.client.job_status = None
    qpu.client.job_error = "device on fire"

    with pytest.raises(RuntimeError, match="device on fire"):
        qpu.get_result(handle, timeout=0.01, wait=0)


def test_qpu_get_result_raises_on_cancelled(mod):
    from pytket.circuit import Circuit

    qpu = mod.QPU()
    (handle,) = qpu.process_circuits([Circuit(1)], n_shots=10)
    qpu.client.job_status = "CANCELLED"

    with pytest.raises(RuntimeError, match="cancelled"):
        qpu.get_result(handle, timeout=0.01, wait=0)


def test_qpu_get_result_times_out(mod):
    from pytket.circuit import Circuit

    qpu = mod.QPU()
    (handle,) = qpu.process_circuits([Circuit(1)], n_shots=10)
    qpu.client.job_status = "RUNNING"

    with pytest.raises(TimeoutError):
        qpu.get_result(handle, timeout=0.01, wait=0)


def test_qpu_reattached_handle_authenticates(mod):
    # Handles are persistent; polling one from a fresh backend must
    # authenticate on first use.
    from pytket.backends import ResultHandle, StatusEnum

    qpu = mod.QPU()
    handle = ResultHandle("ProviderJob:job-1", 0)

    assert not qpu.client.authenticated
    assert qpu.circuit_status(handle).status == StatusEnum.COMPLETED
    assert qpu.client.authenticated


def test_qpu_backend_info_uses_platform_meta(mod):
    qpu = mod.QPU(platform="Selene")

    info = qpu.backend_info

    assert info.device_name == "Selene"

    # Cached: a second access must not query the server again.
    _ = qpu.backend_info
    platform_queries = [
        query for query, _ in qpu.client.calls if "query ProviderPlatforms" in query
    ]
    assert len(platform_queries) == 1


def test_qpu_required_predicates_use_selected_platform(mod):
    qpu = mod.QPU(platform="Selene")

    (predicate,) = qpu.required_predicates

    assert predicate.n_qubits == 26


def test_qpu_required_predicates_defer_when_no_platform_selected(mod):
    # With no platform chosen the server picks one we cannot identify here, so
    # no client-side qubit predicate is emitted; the server enforces its limits
    # rather than the client guessing from the max across all platforms.
    qpu = mod.QPU()

    assert qpu.required_predicates == []


def test_qpu_required_predicates_skip_platform_without_qubit_count(mod):
    # A matched, runnable platform that advertises no maxQubits must not fail
    # predicate construction; the check is simply skipped.
    qpu = mod.QPU(platform="Selene")
    qpu.client.platforms = [
        {
            "id": "ProviderPlatform:b",
            "name": "Selene",
            "provider": "NEXUS",
            "meta": {"maxQubits": None, "maxShots": None, "maxCircuits": None},
        },
    ]

    assert qpu.required_predicates == []


def test_qpu_matches_provider_qualified_platform_name(mod):
    # The server's ProviderPlatformNameOrID scalar requires `provider:name`
    # for name strings, so client-side matching must accept it too.
    qpu = mod.QPU(platform="nexus:Selene")

    (predicate,) = qpu.required_predicates

    assert predicate.n_qubits == 26


def test_qpu_unknown_platform_raises(mod):
    qpu = mod.QPU(platform="no-such-platform")

    with pytest.raises(LookupError, match="no-such-platform"):
        _ = qpu.required_predicates


def test_qpu_rejects_client_combined_with_url_options(mod):
    with pytest.raises(ValueError, match="explicit `client`"):
        mod.QPU(FakeClient(), url="https://example.invalid")


def test_qpu_compilation_passes(mod):
    from pytket.circuit import OpType
    from pytket.passes import AutoRebase, SequencePass

    qpu = mod.QPU()

    assert isinstance(qpu.default_compilation_pass(), SequencePass)
    assert isinstance(qpu.default_compilation_pass(0), SequencePass)

    rebase = qpu.rebase_pass()
    assert isinstance(rebase, AutoRebase)
    assert OpType.Measure in rebase.gateset


def test_qpu_result_id_type(mod):
    qpu = mod.QPU()

    assert qpu._result_id_type == (str, int)


def test_qpu_cancel_not_implemented(mod):
    from pytket.backends import ResultHandle

    qpu = mod.QPU()

    with pytest.raises(NotImplementedError):
        qpu.cancel(ResultHandle("ProviderJob:job-1", 0))


def test_provider_result_to_backend_result_filters_extra_keys(mod):
    result = mod.ProviderResult(
        index=0,
        serialization_format=1000,
        raw=json.dumps(
            {
                "qubits": [["q", [0]]],
                "shots": [[0]],
                "extra_nexus_field": True,
            }
        ),
    )

    backend_result = result.to_backend_result()

    assert backend_result.to_dict() == {"qubits": [["q", [0]]], "shots": [[0]]}


def test_provider_result_to_backend_result_wrong_format_raises(mod):
    result = mod.ProviderResult(index=0, serialization_format=1002, raw="[]")

    with pytest.raises(ValueError, match="pytket"):
        result.to_backend_result()
