"""Negotiating a program serialization format with a provider platform.

A platform advertises the formats it accepts as `ProviderPlatformMeta.inputFormats`,
most preferred first. This module turns those GraphQL enum names into the qio
format constants and knows how to encode a program from each supported framework
into every format it can reach.

Nothing here imports a quantum framework at module scope: every conversion pulls
its dependency in lazily, so an uninstalled optional extra disables just that one
hop instead of the whole module.
"""

from __future__ import annotations

import base64
import binascii
import importlib
import json
import re
import sys
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping, Sequence

from . import wire

# `QuantumProgramFormat` variants as GraphQL renders them. Formats no source can
# produce are listed too: negotiation has to recognise them and move on, so that
# a "no reachable format" error can name what the platform actually asked for
# rather than reporting it as an unknown name.
FORMAT_BY_GRAPHQL_NAME: dict[str, int] = {
    "UNKNOWN_SERIALIZATION_FORMAT": wire.PROGRAM_UNKNOWN_SERIALIZATION_FORMAT,
    "QASM_V1": wire.PROGRAM_QASM_V1,
    "QASM_V2": wire.PROGRAM_QASM_V2,
    "QASM_V3": wire.PROGRAM_QASM_V3,
    "QIR_V1": wire.PROGRAM_QIR_V1,
    "CIRQ_CIRCUIT_JSON_V1": wire.PROGRAM_CIRQ_CIRCUIT_JSON_V1,
    "PERCEVAL_CIRCUIT_JSON_V1": wire.PROGRAM_PERCEVAL_CIRCUIT_JSON_V1,
    "PULSER_SEQUENCE_JSON_V1": wire.PROGRAM_PULSER_SEQUENCE_JSON_V1,
    "TKET_CIRCUIT_JSON_V1": wire.PROGRAM_TKET_CIRCUIT_JSON_V1,
    "HUGR_V1": wire.PROGRAM_HUGR_V1,
}

GRAPHQL_NAME_BY_FORMAT: dict[int, str] = {
    value: key for key, value in FORMAT_BY_GRAPHQL_NAME.items()
}

# Magic prefixes of the two binary program encodings, as in `aqora.guppy`.
_HUGR_MAGIC = b"HUGR"
_QIR_MAGIC = b"BC\xc0\xde"


class ConversionUnavailable(Exception):
    """A conversion exists but the optional dependency it needs is missing.

    Distinct from a conversion that ran and failed: this one says nothing about
    whether the program *could* reach the format, so negotiation moves to the
    next candidate without treating it as evidence of a bad program.
    """


def formats_from_graphql(names: Iterable[str]) -> list[int]:
    """Map `inputFormats` enum names onto qio format constants, in order.

    Names this build does not know are dropped rather than raising: the server
    may advertise a format newer than the CLI, and that must not break
    submission.
    """
    formats: list[int] = []
    for name in names:
        serialization_format = FORMAT_BY_GRAPHQL_NAME.get(str(name).upper())
        if serialization_format is not None and serialization_format not in formats:
            formats.append(serialization_format)
    return formats


def format_name(serialization_format: int) -> str:
    return GRAPHQL_NAME_BY_FORMAT.get(serialization_format, str(serialization_format))


@dataclass(frozen=True)
class Source:
    """A program normalized into the shape its encoders expect.

    `kind` selects the encoder table. `program` is a framework circuit for
    `qiskit`/`pytket`, HUGR envelope bytes for `hugr`, and an already-encoded
    serialization string for `raw` (whose only reachable format is
    `native_format`).
    """

    kind: str
    program: Any
    native_format: int


def _imported_attr(module: str, attribute: str) -> Any:
    """`module.attribute`, but only if the module is already imported.

    A framework's circuit object cannot exist unless that framework has been
    imported, so this recognises qiskit and pytket inputs without importing
    either one.
    """
    imported = sys.modules.get(module)
    return None if imported is None else getattr(imported, attribute, None)


def _is_instance_of(program: Any, module: str, attribute: str) -> bool:
    cls = _imported_attr(module, attribute)
    return isinstance(cls, type) and isinstance(program, cls)


# Line and block comments as QASM tokenizes them, stripped before looking for
# the version header so a leading `/* ... */` does not mask it.
_QASM_COMMENT = re.compile(r"/\*.*?\*/|//[^\n]*", re.DOTALL)


def _qasm_format(text: str) -> int | None:
    """The QASM format of a source string, or None if it is not QASM."""
    for line in _QASM_COMMENT.sub(" ", text).splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if not stripped.startswith("OPENQASM"):
            return None
        version = stripped[len("OPENQASM") :].strip().rstrip(";").strip()
        if version.startswith("2"):
            return wire.PROGRAM_QASM_V2
        if version.startswith("3"):
            return wire.PROGRAM_QASM_V3
        return None
    return None


def detect(program: Any) -> Source:
    """Normalize any accepted program into a `Source`.

    Accepts a qiskit `QuantumCircuit`, a pytket `Circuit`, a `@guppy`-decorated
    function, a hugr `Package`, raw HUGR envelope or QIR bitcode bytes, QASM
    source text, or a base64 string of either binary encoding.
    """
    if _is_instance_of(program, "qiskit", "QuantumCircuit"):
        return Source("qiskit", program, wire.PROGRAM_QASM_V3)
    if _is_instance_of(program, "pytket", "Circuit"):
        return Source("pytket", program, wire.PROGRAM_TKET_CIRCUIT_JSON_V1)

    # A `@guppy`-decorated function compiles to a hugr `Package`, which
    # serializes to a HUGR envelope; both collapse to the same envelope bytes.
    compile_ = getattr(program, "compile", None)
    if callable(compile_):
        program = compile_()
    # Exclude int: its builtin `to_bytes()` is not a package serializer.
    to_bytes = None if isinstance(program, int) else getattr(program, "to_bytes", None)
    if callable(to_bytes):
        program = to_bytes()

    if isinstance(program, (bytes, bytearray)):
        raw = bytes(program)
    elif isinstance(program, str):
        qasm = _qasm_format(program)
        if qasm is not None:
            return Source("raw", program, qasm)
        try:
            raw = base64.b64decode(program, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise TypeError(
                "string programs must be QASM source or a base64-encoded HUGR "
                "envelope or QIR bitcode"
            ) from exc
    else:
        raise TypeError(
            "`program` must be a qiskit QuantumCircuit, a pytket Circuit, a guppy "
            "function, a hugr Package, HUGR envelope bytes, QIR bitcode bytes, "
            "QASM source, or a base64 string of either binary encoding"
        )

    if raw.startswith(_HUGR_MAGIC):
        # Kept as bytes: a HUGR envelope can still be lowered to QIR.
        return Source("hugr", raw, wire.PROGRAM_HUGR_V1)
    if raw.startswith(_QIR_MAGIC):
        return Source(
            "raw", base64.b64encode(raw).decode("ascii"), wire.PROGRAM_QIR_V1
        )
    raise TypeError(
        "program bytes are neither a HUGR package envelope (`HUGR` magic) "
        "nor QIR bitcode (`BC\\xc0\\xde` magic)"
    )


def _require(module: str, extra: str) -> Any:
    try:
        return importlib.import_module(module)
    except ImportError as exc:
        raise ConversionUnavailable(
            f"{module} is not installed; install `aqora[{extra}]`"
        ) from exc


def _qiskit_qasm3(circuit: Any) -> str:
    return _require("qiskit.qasm3", "qiskit").dumps(circuit)


def _qiskit_qasm2(circuit: Any) -> str:
    return _require("qiskit.qasm2", "qiskit").dumps(circuit)


def _qiskit_to_tket(circuit: Any) -> Any:
    return _require("pytket.extensions.qiskit", "qiskit-tket").qiskit_to_tk(circuit)


def _qiskit_tket_json(circuit: Any) -> str:
    return _pytket_tket_json(_qiskit_to_tket(circuit))


def _qiskit_qir(circuit: Any) -> str:
    return _pytket_qir(_qiskit_to_tket(circuit))


def _pytket_tket_json(circuit: Any) -> str:
    return json.dumps(circuit.to_dict())


def _pytket_qasm2(circuit: Any) -> str:
    return _require("pytket.qasm", "pytket").circuit_to_qasm_str(circuit)


def _pytket_qir(circuit: Any) -> str:
    module = _require("pytket.qir", "pytket-qir")
    bitcode = module.pytket_to_qir(circuit, qir_format=module.QIRFormat.BINARY)
    if not isinstance(bitcode, bytes):
        # `pytket_to_qir` returns None for circuits it cannot lower.
        raise ValueError("pytket-qir did not produce QIR bitcode for this circuit")
    return base64.b64encode(bitcode).decode("ascii")


def _hugr_envelope(envelope: bytes) -> str:
    return base64.b64encode(envelope).decode("ascii")


def _hugr_qir(envelope: bytes) -> str:
    # `to_qir_bytes` tests `type(hugr) is bytes`, so a bytearray would not do.
    to_qir_bytes = _require("hugr_qir.hugr_to_qir", "guppy-qir").to_qir_bytes
    return base64.b64encode(to_qir_bytes(bytes(envelope))).decode("ascii")


ENCODERS: dict[str, dict[int, Callable[[Any], str]]] = {
    "qiskit": {
        wire.PROGRAM_QASM_V3: _qiskit_qasm3,
        wire.PROGRAM_QASM_V2: _qiskit_qasm2,
        wire.PROGRAM_TKET_CIRCUIT_JSON_V1: _qiskit_tket_json,
        wire.PROGRAM_QIR_V1: _qiskit_qir,
    },
    "pytket": {
        wire.PROGRAM_TKET_CIRCUIT_JSON_V1: _pytket_tket_json,
        wire.PROGRAM_QASM_V2: _pytket_qasm2,
        wire.PROGRAM_QIR_V1: _pytket_qir,
    },
    "hugr": {
        wire.PROGRAM_HUGR_V1: _hugr_envelope,
        wire.PROGRAM_QIR_V1: _hugr_qir,
    },
}


def _encoders(source: Source) -> Mapping[int, Callable[[Any], str]]:
    if source.kind == "raw":
        # Already serialized; it reaches exactly the format it arrived in.
        return {source.native_format: lambda program: program}
    return ENCODERS[source.kind]


def _fallback_formats(sources: Sequence[Source]) -> list[int]:
    """Candidates to try when the platform advertises no preference.

    Each source's own native format, in order, so a single-framework job always
    uses its own encoding and a mixed one can still find common ground.
    """
    formats: list[int] = []
    for source in sources:
        if source.native_format not in formats:
            formats.append(source.native_format)
    return formats


def encode(
    sources: Sequence[Source],
    accepted: Sequence[int],
) -> tuple[list[tuple[str, int]], int]:
    """Encode every program into the first format the whole job can reach.

    `accepted` is the platform's preference order. An empty one means "no
    preference" — an old server, or the platform's provider RPC degrading to an
    empty list — and falls back to the sources' native formats.

    Returns the `(serialization, serialization_format)` pairs that
    `wire.build_model_payload` takes, plus the format that was chosen.
    """
    if not sources:
        raise ValueError("At least one program is required")
    candidates = list(accepted) or _fallback_formats(sources)
    unavailable: list[tuple[int, Exception]] = []
    failed: list[tuple[int, Exception]] = []

    for serialization_format in candidates:
        encoders = [_encoders(source).get(serialization_format) for source in sources]
        if any(encoder is None for encoder in encoders):
            continue
        try:
            programs = [
                encoder(source.program)
                for encoder, source in zip(encoders, sources)
                if encoder is not None
            ]
        except ConversionUnavailable as error:
            unavailable.append((serialization_format, error))
            continue
        except Exception as error:  # noqa: BLE001 - replayed below if nothing works
            # A conversion that runs and fails is a legitimate "this program
            # cannot reach this format" signal (`qiskit.qasm2.dumps` rejects
            # gates outside the QASM2 gateset), so keep negotiating. The error
            # is kept so a real bug does not vanish behind "no reachable format".
            failed.append((serialization_format, error))
            continue
        return [
            (program, serialization_format) for program in programs
        ], serialization_format

    raise _no_reachable_format(sources, candidates, bool(accepted), unavailable, failed)


def _no_reachable_format(
    sources: Sequence[Source],
    candidates: Sequence[int],
    platform_stated: bool,
    unavailable: Sequence[tuple[int, Exception]],
    failed: Sequence[tuple[int, Exception]],
) -> ValueError:
    kinds = ", ".join(sorted({source.kind for source in sources}))
    wanted = ", ".join(format_name(candidate) for candidate in candidates)
    if platform_stated:
        message = (
            f"No usable serialization format: the platform accepts {wanted}, and "
            f"this job's programs ({kinds}) cannot produce any of them."
        )
    else:
        message = (
            f"No usable serialization format: the platform states no preference, "
            f"and this job's programs ({kinds}) cannot produce their native "
            f"formats ({wanted})."
        )
    if unavailable:
        detail = "; ".join(
            f"{format_name(serialization_format)}: {error}"
            for serialization_format, error in unavailable
        )
        message += f" Missing optional dependencies: {detail}."
    if failed:
        detail = "; ".join(
            f"{format_name(serialization_format)}: {error}"
            for serialization_format, error in failed
        )
        message += f" Conversions attempted that failed: {detail}."
    return ValueError(message)
