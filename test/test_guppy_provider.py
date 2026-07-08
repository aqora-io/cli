from __future__ import annotations

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
    Path(__file__).resolve().parents[1] / "python" / "aqora" / "guppy" / "__init__.py"
)

_FAKE_MODULE_PREFIXES = ("aqora.", "hugr")

HUGR_ENVELOPE = b"HUGR\x00fake-envelope"
QIR_BITCODE = b"BC\xc0\xdefake-bitcode"


def load_guppy_module(fake_client_cls: type):
    for name in list(sys.modules):
        if name == "aqora" or name.startswith(_FAKE_MODULE_PREFIXES):
            sys.modules.pop(name, None)

    aqora = types.ModuleType("aqora")
    aqora.__path__ = [str(MODULE_PATH.parents[1])]
    aqora.Client = fake_client_cls
    aqora._aqora = _AQORA_NATIVE
    sys.modules["aqora"] = aqora
    sys.modules["aqora._aqora"] = _AQORA_NATIVE

    hugr = types.ModuleType("hugr")
    sys.modules["hugr"] = hugr

    hugr_qsystem = types.ModuleType("hugr.qsystem")
    sys.modules["hugr.qsystem"] = hugr_qsystem

    hugr_qsystem_result = types.ModuleType("hugr.qsystem.result")

    class QsysResult:
        # Mirrors hugr.qsystem.result.QsysResult, which exposes the decoded
        # shots as `.results` (the attribute `job.py._decode` feeds and callers
        # read), not `.shots`.
        def __init__(self, results=None) -> None:
            self.results = list(results or [])

    hugr_qsystem_result.QsysResult = QsysResult
    sys.modules["hugr.qsystem.result"] = hugr_qsystem_result

    spec = importlib.util.spec_from_file_location(
        "aqora.guppy",
        MODULE_PATH,
        submodule_search_locations=[str(MODULE_PATH.parent)],
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules["aqora.guppy"] = module
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


QSYS_SHOTS = [[["c0", 1], ["c1", 1]], [["c0", 0], ["c1", 0]]]

QIR_LABELED_FIXTURE = "\n".join(
    [
        "HEADER\tschema_name\tlabeled",
        "HEADER\tschema_version\t1.0",
        "START",
        "METADATA\tentry_point",
        "OUTPUT\tRESULT\t0\tr0",
        "OUTPUT\tRESULT\t1\tr1",
        "END\t0",
        "START",
        "OUTPUT\tRESULT\t1\tr0",
        "OUTPUT\tRESULT\t1\tr1",
        "END\t0",
    ]
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
                    "index": 0,
                    "error": None,
                    "result": "https://example.invalid/result-0",
                }
            ]
        ]
        self.payloads: dict[str, str] = {
            "https://example.invalid/result-0": _result_payload(
                json.dumps(QSYS_SHOTS),
                1002,
                compression_format=2,
            ),
        }
        self.platform_page_info: dict[str, object] = {
            "hasNextPage": False,
            "endCursor": None,
        }
        self.platforms: list[dict[str, object]] = [
            {
                "id": "ProviderPlatform:b",
                "name": "Selene",
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
                    "platform": {
                        "id": "ProviderPlatform:b",
                        "name": "Selene",
                        "provider": "NEXUS",
                    },
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
        yield load_guppy_module(FakeClient)
    finally:
        for name in list(sys.modules):
            if name == "aqora" or name.startswith(_FAKE_MODULE_PREFIXES):
                sys.modules.pop(name, None)
        sys.modules.update(saved)


def _uploaded_program(qpu) -> dict[str, object]:
    (upload,) = qpu.client.uploads
    (program,) = json.loads(upload[1])["programs"]
    return program


class FakePackage:
    """Duck-types hugr's Package."""

    def to_bytes(self) -> bytes:
        return HUGR_ENVELOPE


class FakeGuppyFunction:
    """Duck-types a @guppy-decorated function."""

    def compile(self) -> FakePackage:
        return FakePackage()


# --- wire format (aqora._provider.wire over the Rust qio crate) ---


def test_wire_build_model_payload_shape(mod, monkeypatch: pytest.MonkeyPatch):
    wire = sys.modules["aqora._provider.wire"]
    monkeypatch.setattr(wire, "_package_version", lambda: "9.9.9")

    payload = json.loads(wire.build_model_payload([("{}", 1001)], compress=False))

    assert payload == {
        "programs": [
            {
                "compression_format": 1,
                "serialization_format": 1001,
                "serialization": "{}",
            }
        ],
        "noise_model": None,
        "client": {"user_agent": "aqora/9.9.9"},
        "backend": {"name": "aqora-qpu", "version": None, "options": None},
    }


def test_wire_build_model_payload_compresses_with_qio_scheme(mod):
    wire = sys.modules["aqora._provider.wire"]
    text = json.dumps({"phase": "0.0"})

    payload = json.loads(wire.build_model_payload([(text, 1000)]))

    (program,) = payload["programs"]
    assert program["compression_format"] == 2
    assert program["serialization_format"] == 1000
    # Must match qio's scheme: base64(zlib.compress(utf8)).
    assert _un_zlib_b64(program["serialization"]) == text


def test_wire_build_model_payload_rejects_unknown_program_format(mod):
    wire = sys.modules["aqora._provider.wire"]

    with pytest.raises(ValueError, match="serialization format"):
        wire.build_model_payload([("{}", 999)])


def test_wire_parse_result_payload_uncompressed(mod):
    wire = sys.modules["aqora._provider.wire"]
    payload = _result_payload('{"shots": [[0]]}', 1002)

    serialization_format, raw = wire.parse_result_payload(payload)

    assert serialization_format == 1002
    assert raw == '{"shots": [[0]]}'


def test_wire_parse_result_payload_zlib(mod):
    wire = sys.modules["aqora._provider.wire"]
    payload = _result_payload("HEADER\tschema\tlabeled", 1001, compression_format=2)

    serialization_format, raw = wire.parse_result_payload(payload)

    assert serialization_format == 1001
    assert raw == "HEADER\tschema\tlabeled"


def test_wire_parse_result_payload_double_encoded(mod):
    wire = sys.modules["aqora._provider.wire"]
    payload = json.dumps(_result_payload('{"shots": []}', 1002))

    serialization_format, raw = wire.parse_result_payload(payload)

    assert serialization_format == 1002
    assert raw == '{"shots": []}'


def test_wire_parse_result_payload_unknown_compression(mod):
    wire = sys.modules["aqora._provider.wire"]
    payload = json.dumps(
        {"compression_format": 99, "serialization_format": 1002, "serialization": ""}
    )

    with pytest.raises(ValueError, match="result payload"):
        wire.parse_result_payload(payload)


def test_wire_parse_result_payload_passes_unknown_formats_through(mod):
    # Result formats newer than this build must not break parsing; they
    # surface as the raw discriminant.
    wire = sys.modules["aqora._provider.wire"]
    payload = _result_payload('{"future": true}', 1003, compression_format=2)

    serialization_format, raw = wire.parse_result_payload(payload)

    assert serialization_format == 1003
    assert raw == '{"future": true}'


# --- QPU.run input handling ---


def test_qpu_run_uploads_hugr_from_guppy_function(mod, monkeypatch: pytest.MonkeyPatch):
    wire = sys.modules["aqora._provider.wire"]
    monkeypatch.setattr(wire, "_package_version", lambda: "9.9.9")

    qpu = mod.QPU(platform="Selene")
    job = qpu.run(FakeGuppyFunction(), shots=100)

    assert qpu.client.authenticated
    assert job.job_id == "ProviderJob:job-1"

    (upload_url, payload_bytes, content_type) = qpu.client.uploads[0]
    assert upload_url == "https://example.invalid/upload"
    assert content_type == "application/json"

    payload = json.loads(payload_bytes)
    assert payload["client"] == {"user_agent": "aqora/9.9.9"}
    assert payload["backend"] == {"name": "aqora-qpu", "version": None, "options": None}
    (program,) = payload["programs"]
    assert program["serialization_format"] == 1001
    assert base64.b64decode(_un_zlib_b64(program["serialization"])) == HUGR_ENVELOPE

    create_provider_job_calls = [
        variables for query, variables in qpu.client.calls if "createProviderJob" in query
    ]
    assert create_provider_job_calls == [
        {
            "providerModelId": "model-1",
            "shots": 100,
            "providerPlatform": "Selene",
        }
    ]


def test_qpu_run_accepts_hugr_package(mod):
    qpu = mod.QPU()

    qpu.run(FakePackage())

    program = _uploaded_program(qpu)
    assert program["serialization_format"] == 1001
    assert base64.b64decode(_un_zlib_b64(program["serialization"])) == HUGR_ENVELOPE


def test_qpu_run_accepts_hugr_envelope_bytes(mod):
    qpu = mod.QPU()

    qpu.run(HUGR_ENVELOPE)

    program = _uploaded_program(qpu)
    assert program["serialization_format"] == 1001


def test_qpu_run_accepts_qir_bitcode_bytes(mod):
    qpu = mod.QPU()

    qpu.run(QIR_BITCODE, shots=50)

    program = _uploaded_program(qpu)
    assert program["serialization_format"] == 4
    assert base64.b64decode(_un_zlib_b64(program["serialization"])) == QIR_BITCODE


def test_qpu_run_accepts_base64_strings(mod):
    qpu = mod.QPU()
    qpu.run(base64.b64encode(HUGR_ENVELOPE).decode("ascii"))
    assert _uploaded_program(qpu)["serialization_format"] == 1001

    qpu = mod.QPU()
    qpu.run(base64.b64encode(QIR_BITCODE).decode("ascii"))
    assert _uploaded_program(qpu)["serialization_format"] == 4


def test_qpu_run_rejects_unrecognized_bytes(mod):
    qpu = mod.QPU()

    with pytest.raises(ValueError, match="HUGR"):
        qpu.run(b"ELF\x00not-a-program")


def test_qpu_run_rejects_non_base64_string(mod):
    qpu = mod.QPU()

    with pytest.raises(ValueError, match="base64"):
        qpu.run("not base64!!!")


def test_qpu_run_rejects_non_program_input(mod):
    qpu = mod.QPU()

    # `int` has a builtin `to_bytes()`; it must not be treated as a package.
    with pytest.raises(TypeError, match="guppy"):
        qpu.run(12345)


def test_qpu_run_rejects_unsupported_options(mod):
    qpu = mod.QPU()

    with pytest.raises(NotImplementedError, match="seed"):
        qpu.run(HUGR_ENVELOPE, seed=42)


def test_qpu_run_rejects_non_integer_shots(mod):
    qpu = mod.QPU()

    with pytest.raises(TypeError, match="integer"):
        qpu.run(HUGR_ENVELOPE, shots=True)
    with pytest.raises(TypeError, match="integer"):
        qpu.run(HUGR_ENVELOPE, shots=12.5)


def test_qpu_rejects_client_combined_with_url_options(mod):
    with pytest.raises(ValueError, match="explicit `client`"):
        mod.QPU(FakeClient(), url="https://example.invalid")


# --- QPUJob ---


def test_job_result_decodes_qsys_result(mod):
    qpu = mod.QPU(platform="Selene")
    job = qpu.run(FakeGuppyFunction(), shots=100)

    result = job.result(timeout=0.01, wait=0)

    from hugr.qsystem.result import QsysResult

    assert isinstance(result, QsysResult)
    assert result.results == QSYS_SHOTS


def test_job_result_decodes_qir_labeled_result(mod):
    qpu = mod.QPU()
    qpu.client.payloads["https://example.invalid/result-0"] = _result_payload(
        QIR_LABELED_FIXTURE, 1001, compression_format=2
    )
    job = qpu.run(QIR_BITCODE)

    result = job.result(timeout=0.01, wait=0)

    assert isinstance(result, mod.QirLabeledResult)
    assert result.shots == [
        [("RESULT", "0", "r0"), ("RESULT", "1", "r1")],
        [("RESULT", "1", "r0"), ("RESULT", "1", "r1")],
    ]
    assert result.register_counts() == {
        "r0": {"0": 1, "1": 1},
        "r1": {"1": 2},
    }


def test_job_result_rejects_unexpected_format(mod):
    qpu = mod.QPU()
    qpu.client.payloads["https://example.invalid/result-0"] = _result_payload(
        "{}", 1000
    )
    job = qpu.run(HUGR_ENVELOPE)

    with pytest.raises(ValueError, match="1000"):
        job.result(timeout=0.01, wait=0)


def test_job_result_raises_on_error(mod):
    # The platform nulls the job status when the provider reports an error
    # state; the error field holds the provider's message.
    qpu = mod.QPU()
    job = qpu.run(HUGR_ENVELOPE)
    qpu.client.job_status = None
    qpu.client.job_error = "device on fire"

    with pytest.raises(RuntimeError, match="device on fire"):
        job.result(timeout=0.01, wait=0)


def test_job_result_ignores_progress_message_on_live_job(mod):
    # The error field mirrors the provider's progress message ("The job is
    # queued.", "Job has been submitted to Nexus.") for healthy jobs.
    qpu = mod.QPU()
    job = qpu.run(HUGR_ENVELOPE)
    qpu.client.job_error = "Job has been submitted to Nexus."

    result = job.result(timeout=0.01, wait=0)

    assert result.results == QSYS_SHOTS


def test_job_wait_keeps_polling_null_status_without_error(mod):
    qpu = mod.QPU()
    job = qpu.run(HUGR_ENVELOPE)
    qpu.client.job_status = None

    with pytest.raises(TimeoutError):
        job.result(timeout=0.01, wait=0)


def test_job_result_raises_on_cancelled(mod):
    qpu = mod.QPU()
    job = qpu.run(HUGR_ENVELOPE)
    qpu.client.job_status = "CANCELLED"

    with pytest.raises(RuntimeError, match="cancelled"):
        job.result(timeout=0.01, wait=0)


def test_job_result_times_out(mod):
    qpu = mod.QPU()
    job = qpu.run(HUGR_ENVELOPE)
    qpu.client.job_status = "RUNNING"

    with pytest.raises(TimeoutError):
        job.result(timeout=0.01, wait=0)


def test_job_result_empty_string_error_is_not_an_error(mod):
    qpu = mod.QPU()
    job = qpu.run(HUGR_ENVELOPE)
    qpu.client.job_error = ""

    result = job.result(timeout=0.01, wait=0)

    assert result.results == QSYS_SHOTS


def test_job_result_with_multiple_items_raises(mod):
    qpu = mod.QPU()
    job = qpu.run(HUGR_ENVELOPE)
    qpu.client.result_pages = [
        [
            {"index": 0, "error": None, "result": "https://example.invalid/result-0"},
            {"index": 1, "error": None, "result": "https://example.invalid/result-0"},
        ]
    ]

    with pytest.raises(RuntimeError, match="result_items"):
        job.result(timeout=0.01, wait=0)


def test_job_result_items_paginates_and_sorts(mod):
    qpu = mod.QPU()
    job = qpu.run(HUGR_ENVELOPE)
    qpu.client.payloads["https://example.invalid/result-1"] = _result_payload(
        json.dumps(QSYS_SHOTS), 1002
    )
    qpu.client.result_pages = [
        [{"index": 1, "error": None, "result": "https://example.invalid/result-1"}],
        [{"index": 0, "error": None, "result": "https://example.invalid/result-0"}],
    ]

    items = job.result_items()

    assert [item.index for item in items] == [0, 1]
    assert [item.serialization_format for item in items] == [1002, 1002]


def test_job_result_count_mismatch_raises(mod):
    qpu = mod.QPU()
    job = qpu.run(HUGR_ENVELOPE)
    qpu.client.result_count = 3

    with pytest.raises(RuntimeError, match="1 of 3"):
        job.result(timeout=0.01, wait=0)


def test_job_result_item_error_raises(mod):
    qpu = mod.QPU()
    job = qpu.run(HUGR_ENVELOPE)
    qpu.client.result_pages = [[{"index": 0, "error": "boom", "result": None}]]

    with pytest.raises(RuntimeError, match="boom"):
        job.result(timeout=0.01, wait=0)


def test_generic_provider_job_from_id(mod):
    # The dependency-free base class must support reattach-by-id too; it is
    # exported from top-level `aqora`.
    from aqora._provider.jobs import ProviderJob

    job = ProviderJob.from_id("ProviderJob:job-1")

    assert type(job) is ProviderJob
    assert job.client.authenticated
    assert job.status() == "COMPLETED"
    (item,) = job.results()
    assert item.qsys_shots() == QSYS_SHOTS


def test_job_from_id_builds_backend_from_job(mod):
    # Uses the default client (url from env) and binds the backend to the
    # job's platform in the server's `provider:name` form.
    job = mod.QPUJob.from_id("ProviderJob:job-1")

    assert isinstance(job.backend(), mod.QPU)
    assert job.backend().platform == "nexus:Selene"
    assert job.backend().client.authenticated
    assert job.status() == "COMPLETED"

    result = job.result(timeout=0.01, wait=0)
    assert result.results == QSYS_SHOTS


def test_job_from_id_accepts_explicit_client(mod):
    client = FakeClient()

    job = mod.QPUJob.from_id("ProviderJob:job-1", client=client)

    assert job.backend().client is client
    with pytest.raises(ValueError, match="explicit `client`"):
        mod.QPUJob.from_id("ProviderJob:job-1", client=client, url="https://x.invalid")


def test_reattached_job_authenticates(mod):
    # A job handle built from a stored id must authenticate on first use;
    # only the submit path did before.
    qpu = mod.QPU()
    job = mod.QPUJob(qpu, "ProviderJob:job-1")

    assert not qpu.client.authenticated
    assert job.status() == "COMPLETED"
    assert qpu.client.authenticated

    results = job.result_items()
    assert [item.index for item in results] == [0]


def test_job_status_and_backend(mod):
    qpu = mod.QPU()
    job = qpu.run(HUGR_ENVELOPE)

    assert job.backend() is qpu
    assert job.status() == "COMPLETED"


def test_provider_result_qsys_shots_raw_passthrough(mod):
    result = mod.ProviderResult(
        index=0, serialization_format=1002, raw=json.dumps(QSYS_SHOTS)
    )

    assert result.qsys_shots() == QSYS_SHOTS


def test_parse_qir_labeled_fixture(mod):
    labeled = mod.parse_qir_labeled(QIR_LABELED_FIXTURE)

    assert labeled.text == QIR_LABELED_FIXTURE
    assert len(labeled.shots) == 2
