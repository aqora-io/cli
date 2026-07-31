from __future__ import annotations

import base64
import importlib.util
import json
import sys
import types
from collections import Counter, namedtuple
from pathlib import Path

import pytest

# The real native extension (built by `uv sync` / maturin): the format constants
# come from the Rust qio crate, so it stays registered while the rest of the
# `aqora` package and every quantum framework is faked.
import aqora._aqora as _AQORA_NATIVE

PACKAGE_ROOT = Path(__file__).resolve().parents[1] / "python" / "aqora"

_FAKE_MODULE_PREFIXES = ("aqora.", "qiskit", "pytket", "hugr", "cirq")

HUGR_ENVELOPE = b"HUGR\x00fake-envelope"
QIR_BITCODE = b"BC\xc0\xdefake-bitcode"
QASM2_SOURCE = "OPENQASM 2.0;\ninclude \"qelib1.inc\";\nqreg q[1];"
QASM3_SOURCE = "OPENQASM 3.0;\nqubit[1] q;"


# --- fake frameworks -------------------------------------------------------


class FakeQiskitCircuit:
    def __init__(self, name: str = "qc") -> None:
        self.name = name


class FakePytketCircuit:
    def __init__(self, name: str = "tk") -> None:
        self.name = name

    def to_dict(self) -> dict[str, object]:
        return {"circuit": self.name}


class FakePackage:
    """Duck-types hugr's Package."""

    def to_bytes(self) -> bytes:
        return HUGR_ENVELOPE


class FakeGuppyFunction:
    """Duck-types a @guppy-decorated function."""

    def compile(self) -> FakePackage:
        return FakePackage()


def _install_qiskit(*, qasm2=None, qasm3=None) -> None:
    """Register a fake `qiskit`, optionally with its QASM serializers.

    Leaving a serializer out keeps `qiskit.qasm2`/`qiskit.qasm3` out of
    `sys.modules`, which is how the "optional dependency missing" path is
    exercised: the fake `qiskit` module has no `__path__`, so importing the
    submodule fails the way an uninstalled package would.
    """
    qiskit = types.ModuleType("qiskit")
    qiskit.QuantumCircuit = FakeQiskitCircuit
    sys.modules["qiskit"] = qiskit
    if qasm2 is not None:
        module = types.ModuleType("qiskit.qasm2")
        module.dumps = qasm2
        sys.modules["qiskit.qasm2"] = module
    if qasm3 is not None:
        module = types.ModuleType("qiskit.qasm3")
        module.dumps = qasm3
        sys.modules["qiskit.qasm3"] = module


def _install_pytket(*, qasm=True, qir=None) -> None:
    pytket = types.ModuleType("pytket")
    pytket.Circuit = FakePytketCircuit
    sys.modules["pytket"] = pytket
    if qasm:
        module = types.ModuleType("pytket.qasm")
        module.circuit_to_qasm_str = lambda circuit, **_: f"OPENQASM 2.0; // {circuit.name}"
        sys.modules["pytket.qasm"] = module
    if qir is not None:
        module = types.ModuleType("pytket.qir")

        class QIRFormat:
            BINARY = 0
            STRING = 1

        module.QIRFormat = QIRFormat
        module.pytket_to_qir = qir
        sys.modules["pytket.qir"] = module


def _install_qiskit_to_tket(convert=None) -> None:
    extensions = types.ModuleType("pytket.extensions")
    extensions.__path__ = []
    sys.modules["pytket.extensions"] = extensions
    module = types.ModuleType("pytket.extensions.qiskit")
    module.qiskit_to_tk = convert or (lambda circuit: FakePytketCircuit(circuit.name))
    sys.modules["pytket.extensions.qiskit"] = module


def _install_hugr_qir(convert=None) -> None:
    hugr_qir = types.ModuleType("hugr_qir")
    hugr_qir.__path__ = []
    sys.modules["hugr_qir"] = hugr_qir
    module = types.ModuleType("hugr_qir.hugr_to_qir")
    module.to_qir_bytes = convert or (lambda envelope, **_: QIR_BITCODE)
    sys.modules["hugr_qir.hugr_to_qir"] = module


# --- fake GraphQL client ---------------------------------------------------


class FakeClient:
    def __init__(self, *_args, **_kwargs) -> None:
        self.authenticated = False
        self.uploads: list[tuple[str, bytes, str | None]] = []
        self.job_status: str | None = "COMPLETED"
        self.job_error: str | None = None
        self.result_pages: list[list[dict[str, object]]] = [
            [{"index": 0, "error": None, "result": "https://example.invalid/result-0"}]
        ]
        self.payloads: dict[str, str] = {}
        self.platforms: list[dict[str, object]] = [
            {
                "id": "ProviderPlatform:b",
                "name": "Selene",
                "provider": "NEXUS",
                "meta": {
                    "maxQubits": 26,
                    "maxShots": None,
                    "maxCircuits": None,
                    "inputFormats": ["HUGR_V1", "QIR_V1", "QASM_V2"],
                },
            },
        ]

    async def authenticate(self) -> None:
        self.authenticated = True

    async def s3_put(self, url: str, body: bytes, *, content_type: str | None = None) -> str:
        self.uploads.append((url, body, content_type))
        return '"etag-1"'

    async def s3_get(self, url: str) -> bytes:
        return self.payloads[url].encode("utf-8")

    async def send(self, query: str, **variables):
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
            page = 0 if variables.get("after") is None else int(variables["after"])
            has_next = page + 1 < len(self.result_pages)
            return {
                "node": {
                    "__typename": "ProviderJob",
                    "results": {
                        "pageInfo": {
                            "hasNextPage": has_next,
                            "endCursor": str(page + 1) if has_next else None,
                        },
                        "nodes": self.result_pages[page],
                    },
                }
            }
        if "query ProviderPlatforms" in query:
            return {
                "providerPlatforms": {
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
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
                    "resultCount": sum(len(page) for page in self.result_pages),
                    "createdAt": "2026-07-06T00:00:00Z",
                }
            }
        raise AssertionError(f"Unexpected query: {query}")


def _purge() -> None:
    for name in list(sys.modules):
        if name == "aqora" or name.startswith(_FAKE_MODULE_PREFIXES):
            sys.modules.pop(name, None)


def load_aqora() -> types.ModuleType:
    _purge()
    aqora = types.ModuleType("aqora")
    aqora.__path__ = [str(PACKAGE_ROOT)]
    aqora.Client = FakeClient
    aqora._aqora = _AQORA_NATIVE
    sys.modules["aqora"] = aqora
    sys.modules["aqora._aqora"] = _AQORA_NATIVE

    # No `submodule_search_locations`: `qpu.py` is a plain module, and making it
    # a package would resolve its `from ._provider import ...` relative to
    # `aqora.qpu` instead of `aqora`.
    spec = importlib.util.spec_from_file_location("aqora.qpu", PACKAGE_ROOT / "qpu.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules["aqora.qpu"] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    aqora.QPU = module.QPU
    aqora.QPUJob = module.QPUJob
    return module


@pytest.fixture
def qpu_mod():
    saved = {
        name: module
        for name, module in sys.modules.items()
        if name == "aqora" or name.startswith(_FAKE_MODULE_PREFIXES)
    }
    try:
        yield load_aqora()
    finally:
        _purge()
        sys.modules.update(saved)


@pytest.fixture
def formats(qpu_mod):
    return importlib.import_module("aqora._provider.formats")


@pytest.fixture
def wire(qpu_mod):
    return importlib.import_module("aqora._provider.wire")


@pytest.fixture
def results(qpu_mod):
    return importlib.import_module("aqora._provider.results")


# --- GraphQL enum names ----------------------------------------------------


def test_formats_from_graphql_preserves_platform_order(formats, wire):
    assert formats.formats_from_graphql(["HUGR_V1", "QASM_V3", "QIR_V1"]) == [
        wire.PROGRAM_HUGR_V1,
        wire.PROGRAM_QASM_V3,
        wire.PROGRAM_QIR_V1,
    ]


def test_formats_from_graphql_skips_unknown_names(formats, wire):
    # A server may advertise a format newer than this build; that must not
    # break submission, only narrow the choices.
    assert formats.formats_from_graphql(["SOME_FUTURE_V9", "QASM_V3"]) == [
        wire.PROGRAM_QASM_V3
    ]


def test_formats_from_graphql_deduplicates(formats, wire):
    assert formats.formats_from_graphql(["QASM_V3", "QASM_V3"]) == [
        wire.PROGRAM_QASM_V3
    ]


# --- source detection ------------------------------------------------------


def test_detect_qiskit_circuit(formats, wire):
    _install_qiskit(qasm3=lambda circuit: QASM3_SOURCE)
    source = formats.detect(FakeQiskitCircuit())
    assert source.kind == "qiskit"
    assert source.native_format == wire.PROGRAM_QASM_V3


def test_detect_pytket_circuit(formats, wire):
    _install_pytket()
    source = formats.detect(FakePytketCircuit())
    assert source.kind == "pytket"
    assert source.native_format == wire.PROGRAM_TKET_CIRCUIT_JSON_V1


@pytest.mark.parametrize(
    "program",
    [FakeGuppyFunction(), FakePackage(), HUGR_ENVELOPE],
    ids=["guppy-function", "hugr-package", "envelope-bytes"],
)
def test_detect_hugr_sources_collapse_to_envelope_bytes(formats, wire, program):
    source = formats.detect(program)
    assert source.kind == "hugr"
    assert source.program == HUGR_ENVELOPE
    assert source.native_format == wire.PROGRAM_HUGR_V1


def test_detect_qir_bitcode_is_terminal(formats, wire):
    source = formats.detect(QIR_BITCODE)
    assert source.kind == "raw"
    assert source.native_format == wire.PROGRAM_QIR_V1
    assert base64.b64decode(source.program) == QIR_BITCODE


@pytest.mark.parametrize(
    ("source_text", "expected"),
    [(QASM2_SOURCE, "PROGRAM_QASM_V2"), (QASM3_SOURCE, "PROGRAM_QASM_V3")],
)
def test_detect_qasm_source(formats, wire, source_text, expected):
    source = formats.detect(source_text)
    assert source.kind == "raw"
    assert source.native_format == getattr(wire, expected)
    assert source.program == source_text


def test_detect_qasm_source_after_block_comment(formats, wire):
    source_text = "/* vendor\n   header */\n// note\nOPENQASM 3.0;\nqubit[1] q;"
    source = formats.detect(source_text)
    assert source.kind == "raw"
    assert source.native_format == wire.PROGRAM_QASM_V3


def test_detect_base64_envelope(formats, wire):
    encoded = base64.b64encode(HUGR_ENVELOPE).decode("ascii")
    assert formats.detect(encoded).native_format == wire.PROGRAM_HUGR_V1


def test_detect_rejects_unknown_program(formats):
    with pytest.raises(TypeError):
        formats.detect(object())


def test_detect_rejects_unrecognised_bytes(formats):
    with pytest.raises(TypeError, match="HUGR"):
        formats.detect(b"not-a-known-encoding")


# --- negotiation -----------------------------------------------------------


def test_first_reachable_platform_format_wins(formats, wire):
    _install_qiskit(qasm2=lambda c: QASM2_SOURCE, qasm3=lambda c: QASM3_SOURCE)
    _install_qiskit_to_tket()
    source = formats.detect(FakeQiskitCircuit())

    programs, chosen = formats.encode(
        [source],
        [wire.PROGRAM_TKET_CIRCUIT_JSON_V1, wire.PROGRAM_QASM_V3],
    )

    assert chosen == wire.PROGRAM_TKET_CIRCUIT_JSON_V1
    assert json.loads(programs[0][0]) == {"circuit": "qc"}


def test_missing_optional_dependency_falls_through(formats, wire):
    # `pytket.extensions.qiskit` is deliberately not registered, so the tket hop
    # is unavailable and negotiation must move on rather than fail.
    _install_qiskit(qasm3=lambda c: QASM3_SOURCE)
    source = formats.detect(FakeQiskitCircuit())

    programs, chosen = formats.encode(
        [source],
        [wire.PROGRAM_TKET_CIRCUIT_JSON_V1, wire.PROGRAM_QASM_V3],
    )

    assert chosen == wire.PROGRAM_QASM_V3
    assert programs == [(QASM3_SOURCE, wire.PROGRAM_QASM_V3)]


def test_failing_conversion_falls_through(formats, wire):
    def unrepresentable(_circuit):
        raise ValueError("gate 'foo' is not in the QASM2 gateset")

    _install_qiskit(qasm2=unrepresentable, qasm3=lambda c: QASM3_SOURCE)
    source = formats.detect(FakeQiskitCircuit())

    programs, chosen = formats.encode(
        [source],
        [wire.PROGRAM_QASM_V2, wire.PROGRAM_QASM_V3],
    )

    assert chosen == wire.PROGRAM_QASM_V3
    assert programs == [(QASM3_SOURCE, wire.PROGRAM_QASM_V3)]


def test_failing_conversion_is_replayed_when_nothing_works(formats, wire):
    def unrepresentable(_circuit):
        raise ValueError("gate 'foo' is not in the QASM2 gateset")

    _install_qiskit(qasm2=unrepresentable)
    source = formats.detect(FakeQiskitCircuit())

    with pytest.raises(ValueError) as excinfo:
        formats.encode([source], [wire.PROGRAM_QASM_V2])

    message = str(excinfo.value)
    assert "QASM_V2" in message
    assert "gate 'foo' is not in the QASM2 gateset" in message


def test_missing_dependency_named_when_nothing_works(formats, wire):
    _install_qiskit(qasm3=lambda c: QASM3_SOURCE)
    source = formats.detect(FakeQiskitCircuit())

    with pytest.raises(ValueError) as excinfo:
        formats.encode([source], [wire.PROGRAM_QIR_V1])

    message = str(excinfo.value)
    assert "aqora[qiskit-tket]" in message
    assert "qiskit" in message


def test_unreachable_format_reports_platform_list(formats, wire):
    _install_qiskit(qasm3=lambda c: QASM3_SOURCE)
    source = formats.detect(FakeQiskitCircuit())

    with pytest.raises(ValueError, match="PERCEVAL_CIRCUIT_JSON_V1"):
        formats.encode([source], [wire.PROGRAM_PERCEVAL_CIRCUIT_JSON_V1])


def test_empty_platform_list_uses_native_format(formats, wire):
    _install_qiskit(qasm3=lambda c: QASM3_SOURCE)
    source = formats.detect(FakeQiskitCircuit())

    programs, chosen = formats.encode([source], [])

    assert chosen == wire.PROGRAM_QASM_V3
    assert programs == [(QASM3_SOURCE, wire.PROGRAM_QASM_V3)]


def test_empty_platform_list_uses_native_format_for_hugr(formats, wire):
    programs, chosen = formats.encode([formats.detect(FakeGuppyFunction())], [])

    assert chosen == wire.PROGRAM_HUGR_V1
    assert base64.b64decode(programs[0][0]) == HUGR_ENVELOPE


def test_no_preference_error_does_not_claim_a_platform_preference(formats, wire):
    # The platform advertised nothing, so the error must not say it "accepts"
    # the fallback candidates.
    _install_qiskit()
    source = formats.detect(FakeQiskitCircuit())

    with pytest.raises(ValueError, match="no preference"):
        formats.encode([source], [])


def test_mixed_sources_resolve_to_a_shared_format(formats, wire):
    # A qiskit circuit cannot produce HUGR and a guppy program cannot produce
    # QASM, so the whole job has to land on QIR.
    _install_qiskit(qasm3=lambda c: QASM3_SOURCE)
    _install_qiskit_to_tket()
    _install_pytket(qir=lambda circuit, **_: QIR_BITCODE)
    _install_hugr_qir()

    sources = [formats.detect(FakeQiskitCircuit()), formats.detect(FakeGuppyFunction())]
    programs, chosen = formats.encode(
        sources,
        [wire.PROGRAM_HUGR_V1, wire.PROGRAM_QASM_V3, wire.PROGRAM_QIR_V1],
    )

    assert chosen == wire.PROGRAM_QIR_V1
    assert [base64.b64decode(program) for program, _ in programs] == [
        QIR_BITCODE,
        QIR_BITCODE,
    ]


def test_pytket_qir_none_is_a_conversion_failure(formats, wire):
    # `pytket_to_qir` returns None for circuits it cannot lower.
    _install_pytket(qir=lambda circuit, **_: None)
    source = formats.detect(FakePytketCircuit())

    with pytest.raises(ValueError, match="did not produce QIR bitcode"):
        formats.encode([source], [wire.PROGRAM_QIR_V1])


def test_encode_requires_a_program(formats):
    with pytest.raises(ValueError, match="At least one program"):
        formats.encode([], [])


# --- QPU -------------------------------------------------------------------


def _uploaded_programs(qpu) -> list[dict[str, object]]:
    (upload,) = qpu.client.uploads
    return json.loads(upload[1])["programs"]


def test_run_submits_in_the_platform_preferred_format(qpu_mod, wire):
    qpu = qpu_mod.QPU(platform="nexus:Selene")
    qpu.run(FakeGuppyFunction(), shots=10)

    (program,) = _uploaded_programs(qpu)
    assert program["serialization_format"] == wire.PROGRAM_HUGR_V1


def test_run_negotiates_past_formats_the_program_cannot_reach(qpu_mod, wire):
    _install_qiskit(qasm2=lambda c: QASM2_SOURCE, qasm3=lambda c: QASM3_SOURCE)
    qpu = qpu_mod.QPU(platform="nexus:Selene")

    # The platform prefers HUGR then QIR, neither of which a qiskit circuit can
    # produce without the cross-hop extras, so it lands on QASM_V2.
    qpu.run(FakeQiskitCircuit())

    (program,) = _uploaded_programs(qpu)
    assert program["serialization_format"] == wire.PROGRAM_QASM_V2


def test_input_formats_is_empty_without_a_selected_platform(qpu_mod):
    assert qpu_mod.QPU().input_formats == []


def test_input_formats_reads_the_selected_platform(qpu_mod, wire):
    qpu = qpu_mod.QPU(platform="nexus:Selene")
    assert qpu.input_formats == [
        wire.PROGRAM_HUGR_V1,
        wire.PROGRAM_QIR_V1,
        wire.PROGRAM_QASM_V2,
    ]


def test_input_formats_is_cached(qpu_mod):
    qpu = qpu_mod.QPU(platform="nexus:Selene")
    assert qpu.input_formats is qpu.input_formats


def test_missing_input_formats_field_degrades_to_native(qpu_mod, wire):
    qpu = qpu_mod.QPU(platform="nexus:Selene")
    qpu.client.platforms[0]["meta"] = {"maxQubits": 26}

    assert qpu.input_formats == []
    qpu.run(FakeGuppyFunction())
    (program,) = _uploaded_programs(qpu)
    assert program["serialization_format"] == wire.PROGRAM_HUGR_V1


def test_unknown_platform_raises(qpu_mod):
    qpu = qpu_mod.QPU(platform="nexus:Nope")
    with pytest.raises(LookupError, match="nexus:Nope"):
        _ = qpu.input_formats


def test_run_accepts_a_list_of_programs(qpu_mod):
    qpu = qpu_mod.QPU(platform="nexus:Selene")
    qpu.run([FakeGuppyFunction(), FakePackage()])
    assert len(_uploaded_programs(qpu)) == 2


def test_run_does_not_split_an_iterable_program(qpu_mod):
    # QASM source is a string, and a string is iterable; it must stay one
    # program rather than becoming one program per character.
    qpu = qpu_mod.QPU(platform="nexus:Selene")
    qpu.client.platforms[0]["meta"]["inputFormats"] = ["QASM_V2"]
    qpu.run(QASM2_SOURCE)
    assert len(_uploaded_programs(qpu)) == 1


def test_job_reports_the_negotiated_format(qpu_mod, wire):
    qpu = qpu_mod.QPU(platform="nexus:Selene")
    job = qpu.run(FakeGuppyFunction())
    assert job.serialization_format == wire.PROGRAM_HUGR_V1


def test_from_id_builds_the_qpu_from_the_job_platform(qpu_mod):
    job = qpu_mod.QPUJob.from_id("ProviderJob:job-1")
    assert job.backend().platform == "nexus:Selene"
    assert job.serialization_format is None


def test_counts_maps_every_result(qpu_mod, wire):
    qpu = qpu_mod.QPU(platform="nexus:Selene")
    job = qpu.run(FakeGuppyFunction())
    qpu.client.result_pages = [
        [
            {"index": 0, "error": None, "result": "https://example.invalid/result-0"},
            {"index": 1, "error": None, "result": "https://example.invalid/result-1"},
        ]
    ]
    for index, value in enumerate("01"):
        text = "\n".join(["START", f"OUTPUT\tRESULT\t{value}\tr0", "END\t0"])
        qpu.client.payloads[f"https://example.invalid/result-{index}"] = json.dumps(
            {
                "compression_format": 1,
                "serialization_format": wire.RESULT_QIR_LABELED_RESULT_V1,
                "serialization": text,
            }
        )
    assert job.counts() == [{"0": 1}, {"1": 1}]


def test_counts_follows_result_pagination(qpu_mod, wire):
    qpu = qpu_mod.QPU(platform="nexus:Selene")
    job = qpu.run(FakeGuppyFunction())
    qpu.client.result_pages = [
        [{"index": 0, "error": None, "result": "https://example.invalid/result-0"}],
        [{"index": 1, "error": None, "result": "https://example.invalid/result-1"}],
    ]
    for index, value in enumerate("01"):
        text = "\n".join(["START", f"OUTPUT\tRESULT\t{value}\tr0", "END\t0"])
        qpu.client.payloads[f"https://example.invalid/result-{index}"] = json.dumps(
            {
                "compression_format": 1,
                "serialization_format": wire.RESULT_QIR_LABELED_RESULT_V1,
                "serialization": text,
            }
        )
    assert job.counts() == [{"0": 1}, {"1": 1}]


# --- counts normalization --------------------------------------------------


def _result(results, serialization_format: int, raw: str):
    return results.ProviderResult(index=0, serialization_format=serialization_format, raw=raw)


def test_counts_from_qir_labeled_is_joint_not_marginal(results, wire):
    # Two shots that share no joint outcome: "01" and "10". Multiplying the
    # per-register marginals would invent "00" and "11" as well.
    text = "\n".join(
        [
            "START",
            "OUTPUT\tRESULT\t0\tr0",
            "OUTPUT\tRESULT\t1\tr1",
            "END\t0",
            "START",
            "OUTPUT\tRESULT\t1\tr0",
            "OUTPUT\tRESULT\t0\tr1",
            "END\t0",
        ]
    )
    result = _result(results, wire.RESULT_QIR_LABELED_RESULT_V1, text)
    assert result.counts() == {"0 1": 1, "1 0": 1}


def test_counts_from_cudaq_global_register(results, wire):
    # name "__global__" as character codes, then one (packed, width, count)
    # triplet per outcome.
    name = "__global__"
    payload = [len(name), *[ord(char) for char in name], 2, 0b01, 2, 3, 0b10, 2, 5]
    result = _result(
        results, wire.RESULT_CUDAQ_SAMPLE_RESULT_JSON_V1, json.dumps(payload)
    )
    assert result.counts() == {"01": 3, "10": 5}


def test_counts_from_cudaq_without_global_register_refuses_to_guess(results, wire):
    payload: list[int] = []
    for name in ("a", "b"):
        payload += [len(name), ord(name), 1, 0b1, 1, 4]
    result = _result(
        results, wire.RESULT_CUDAQ_SAMPLE_RESULT_JSON_V1, json.dumps(payload)
    )
    with pytest.raises(ValueError, match="no joint counts"):
        result.counts()


def test_counts_from_cudaq_rejects_a_non_array_payload(results, wire):
    result = _result(
        results, wire.RESULT_CUDAQ_SAMPLE_RESULT_JSON_V1, json.dumps({"register": 1})
    )
    with pytest.raises(ValueError, match="not a valid CUDA-Q sample payload"):
        result.counts()


def test_counts_from_cudaq_rejects_an_outcome_wider_than_declared(results, wire):
    # 0b100 needs three bits but the record declares a width of 2.
    payload = [1, ord("a"), 1, 0b100, 2, 4]
    result = _result(
        results, wire.RESULT_CUDAQ_SAMPLE_RESULT_JSON_V1, json.dumps(payload)
    )
    with pytest.raises(ValueError, match="width"):
        result.counts()


def test_counts_from_cudaq_rejects_a_truncated_payload(results, wire):
    # The register promises two outcome triplets but carries only one.
    payload = [1, ord("a"), 2, 0b1, 1, 4]
    result = _result(
        results, wire.RESULT_CUDAQ_SAMPLE_RESULT_JSON_V1, json.dumps(payload)
    )
    with pytest.raises(ValueError, match="truncated"):
        result.counts()


def test_counts_from_qsys(results, wire):
    hugr = types.ModuleType("hugr")
    hugr.__path__ = []
    sys.modules["hugr"] = hugr
    qsystem = types.ModuleType("hugr.qsystem")
    qsystem.__path__ = []
    sys.modules["hugr.qsystem"] = qsystem
    module = types.ModuleType("hugr.qsystem.result")

    class QsysResult:
        def __init__(self, shots) -> None:
            self.shots = shots

        def collated_counts(self):
            return Counter({(("c0", "1"), ("c1", "0")): 2, (("c0", "0"), ("c1", "1")): 1})

    module.QsysResult = QsysResult
    sys.modules["hugr.qsystem.result"] = module

    result = _result(results, wire.RESULT_QSYS_RESULT_JSON_V1, json.dumps([]))
    assert result.counts() == {"1 0": 2, "0 1": 1}


def test_counts_from_qiskit(results, wire):
    qiskit_result = types.ModuleType("qiskit.result")

    class Result:
        def __init__(self, payload) -> None:
            self.payload = payload

        @classmethod
        def from_dict(cls, payload):
            return cls(payload)

        def get_counts(self):
            return {"00": 4, "11": 6}

    qiskit_result.Result = Result
    sys.modules["qiskit"] = types.ModuleType("qiskit")
    sys.modules["qiskit"].__path__ = []
    sys.modules["qiskit.result"] = qiskit_result

    result = _result(results, wire.RESULT_QISKIT_RESULT_JSON_V1, json.dumps({}))
    assert result.counts() == {"00": 4, "11": 6}


def test_counts_from_qiskit_rejects_multi_experiment_payload(results, wire):
    qiskit_result = types.ModuleType("qiskit.result")

    class Result:
        @classmethod
        def from_dict(cls, _payload):
            return cls()

        def get_counts(self):
            return [{"0": 1}, {"1": 1}]

    qiskit_result.Result = Result
    sys.modules["qiskit"] = types.ModuleType("qiskit")
    sys.modules["qiskit"].__path__ = []
    sys.modules["qiskit.result"] = qiskit_result

    result = _result(results, wire.RESULT_QISKIT_RESULT_JSON_V1, json.dumps({}))
    with pytest.raises(ValueError, match="2 experiments"):
        result.counts()


def test_counts_from_cirq(results, wire):
    cirq = types.ModuleType("cirq")

    class CirqResult:
        measurements = {"m": [[0, 1], [1, 0], [0, 1]]}

    cirq.read_json = lambda json_text=None: CirqResult()
    sys.modules["cirq"] = cirq

    result = _result(results, wire.RESULT_CIRQ_RESULT_JSON_V1, json.dumps({}))
    assert result.counts() == {"01": 2, "10": 1}


# Sorts and hashes like a pytket `Bit`: register name first, then index.
FakeBit = namedtuple("FakeBit", ["reg_name", "index"])


def test_counts_from_pytket(results, wire):
    aqora_pytket = types.ModuleType("aqora.pytket")
    aqora_pytket.__path__ = []
    sys.modules["aqora.pytket"] = aqora_pytket
    deps = types.ModuleType("aqora.pytket._deps")

    class BackendResult:
        c_bits = {FakeBit("c", 0): 0, FakeBit("c", 1): 1}

        @classmethod
        def from_dict(cls, _payload):
            return cls()

        def get_counts(self):
            return Counter({(0, 1): 3, (1, 1): 7})

    deps.BackendResult = BackendResult
    sys.modules["aqora.pytket._deps"] = deps

    result = _result(
        results, wire.RESULT_PYTKET_BACKEND_RESULT_JSON_V1, json.dumps({"counts": {}})
    )
    assert result.counts() == {"01": 3, "11": 7}


def test_counts_from_pytket_joins_registers_with_a_space(results, wire):
    aqora_pytket = types.ModuleType("aqora.pytket")
    aqora_pytket.__path__ = []
    sys.modules["aqora.pytket"] = aqora_pytket
    deps = types.ModuleType("aqora.pytket._deps")

    class BackendResult:
        # Two single-bit registers: the joint outcome must keep them apart
        # ("0 1"), not fuse into one bitstring ("01").
        c_bits = {FakeBit("a", 0): 0, FakeBit("b", 0): 1}

        @classmethod
        def from_dict(cls, _payload):
            return cls()

        def get_counts(self):
            return Counter({(0, 1): 3, (1, 0): 2})

    deps.BackendResult = BackendResult
    sys.modules["aqora.pytket._deps"] = deps

    result = _result(
        results, wire.RESULT_PYTKET_BACKEND_RESULT_JSON_V1, json.dumps({"counts": {}})
    )
    assert result.counts() == {"0 1": 3, "1 0": 2}


def test_counts_rejects_an_errored_result(results, wire):
    result = results.ProviderResult(
        index=2, serialization_format=-1, raw="", error="provider exploded"
    )
    with pytest.raises(RuntimeError, match="provider exploded"):
        result.counts()


def test_counts_rejects_a_format_without_a_counts_view(results):
    result = _result(results, 0, "")
    with pytest.raises(ValueError, match="no counts representation"):
        result.counts()
