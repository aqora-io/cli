"""The qio wire format for provider programs and results.

Building and parsing are implemented in Rust by the `qio` crate
(github.com/aqora-io/qio-rs) and exposed through `aqora._aqora`, so the wire
contract (JSON shape, format discriminants, zlib+base64 compression) has a
single source of truth shared with the platform. That includes a WASM module
the programs call into, which the model embeds.
"""

from __future__ import annotations

import base64
import importlib.metadata
import os
from typing import Any

from aqora._aqora import (
    QIO_PROGRAM_CIRQ_CIRCUIT_JSON_V1,
    QIO_PROGRAM_HUGR_V1,
    QIO_PROGRAM_PERCEVAL_CIRCUIT_JSON_V1,
    QIO_PROGRAM_PULSER_SEQUENCE_JSON_V1,
    QIO_PROGRAM_QASM_V1,
    QIO_PROGRAM_QASM_V2,
    QIO_PROGRAM_QASM_V3,
    QIO_PROGRAM_QIR_V1,
    QIO_PROGRAM_TKET_CIRCUIT_JSON_V1,
    QIO_PROGRAM_UNKNOWN_SERIALIZATION_FORMAT,
    QIO_RESULT_CIRQ_RESULT_JSON_V1,
    QIO_RESULT_CUDAQ_SAMPLE_RESULT_JSON_V1,
    QIO_RESULT_PYTKET_BACKEND_RESULT_JSON_V1,
    QIO_RESULT_QIR_LABELED_RESULT_V1,
    QIO_RESULT_QISKIT_RESULT_JSON_V1,
    QIO_RESULT_QSYS_RESULT_JSON_V1,
    qio_build_model_payload,
    qio_parse_result_payload,
)


def _package_version() -> str:
    try:
        return importlib.metadata.version("aqora")
    except importlib.metadata.PackageNotFoundError:
        return "unknown"


PROGRAM_UNKNOWN_SERIALIZATION_FORMAT = QIO_PROGRAM_UNKNOWN_SERIALIZATION_FORMAT
PROGRAM_QASM_V1 = QIO_PROGRAM_QASM_V1
PROGRAM_QASM_V2 = QIO_PROGRAM_QASM_V2
PROGRAM_QASM_V3 = QIO_PROGRAM_QASM_V3
PROGRAM_QIR_V1 = QIO_PROGRAM_QIR_V1
PROGRAM_CIRQ_CIRCUIT_JSON_V1 = QIO_PROGRAM_CIRQ_CIRCUIT_JSON_V1
PROGRAM_PERCEVAL_CIRCUIT_JSON_V1 = QIO_PROGRAM_PERCEVAL_CIRCUIT_JSON_V1
PROGRAM_PULSER_SEQUENCE_JSON_V1 = QIO_PROGRAM_PULSER_SEQUENCE_JSON_V1
PROGRAM_TKET_CIRCUIT_JSON_V1 = QIO_PROGRAM_TKET_CIRCUIT_JSON_V1
PROGRAM_HUGR_V1 = QIO_PROGRAM_HUGR_V1

RESULT_CIRQ_RESULT_JSON_V1 = QIO_RESULT_CIRQ_RESULT_JSON_V1
RESULT_QISKIT_RESULT_JSON_V1 = QIO_RESULT_QISKIT_RESULT_JSON_V1
RESULT_CUDAQ_SAMPLE_RESULT_JSON_V1 = QIO_RESULT_CUDAQ_SAMPLE_RESULT_JSON_V1
RESULT_PYTKET_BACKEND_RESULT_JSON_V1 = QIO_RESULT_PYTKET_BACKEND_RESULT_JSON_V1
RESULT_QIR_LABELED_RESULT_V1 = QIO_RESULT_QIR_LABELED_RESULT_V1
RESULT_QSYS_RESULT_JSON_V1 = QIO_RESULT_QSYS_RESULT_JSON_V1


def build_model_payload(
    programs: list[tuple[str, int]],
    *,
    backend_name: str = "aqora-qpu",
    compress: bool = True,
    wasm: Any = None,
) -> str:
    """Build a `QuantumComputationModel` JSON payload.

    `programs` holds `(serialization, serialization_format)` pairs. `wasm` is
    a WASM module the programs call into (see `encode_wasm_module`), embedded
    in the model.
    """
    return qio_build_model_payload(
        programs,
        user_agent=f"aqora/{_package_version()}",
        backend_name=backend_name,
        compress=compress,
        wasm_module=None if wasm is None else encode_wasm_module(wasm),
    )


def parse_result_payload(payload: str) -> tuple[int, str]:
    """Decode a result payload into `(serialization_format, raw serialization)`."""
    return qio_parse_result_payload(payload)


_WASM_MAGIC = b"\0asm"


def encode_wasm_module(wasm: Any) -> str:
    """The base64 bytecode of a WASM module.

    `wasm` is a pytket `WasmModuleHandler` or `WasmFileHandler` (anything with
    a `bytecode_base64` attribute), the module's raw bytecode, or a path to a
    `.wasm` file. Whichever it is, the module must start with the `\\0asm`
    header: pytket's handlers skip their own check when built with
    `check=False`/`check_file=False`.
    """
    encoded = getattr(wasm, "bytecode_base64", None)
    if encoded is not None:
        if isinstance(encoded, (bytes, bytearray)):
            encoded = bytes(encoded).decode("ascii")
        try:
            bytecode = base64.b64decode(encoded, validate=True)
        except ValueError as err:
            raise ValueError(f"WASM module handler holds invalid base64: {err}") from err
        _check_wasm_bytecode(bytecode, "WASM module handler")
    elif isinstance(wasm, (bytes, bytearray, memoryview)):
        encoded = _encode_wasm_bytecode(bytes(wasm), "WASM bytecode")
    elif isinstance(wasm, (str, os.PathLike)):
        with open(wasm, "rb") as file:
            encoded = _encode_wasm_bytecode(file.read(), f"WASM file {os.fspath(wasm)!r}")
    else:
        raise TypeError(
            "`wasm` must be a pytket WasmModuleHandler/WasmFileHandler, WASM "
            f"bytecode, or a path to a .wasm file, got {type(wasm).__name__}"
        )
    return encoded


def _encode_wasm_bytecode(bytecode: bytes, what: str) -> str:
    _check_wasm_bytecode(bytecode, what)
    return base64.b64encode(bytecode).decode("ascii")


def _check_wasm_bytecode(bytecode: bytes, what: str) -> None:
    if not bytecode.startswith(_WASM_MAGIC):
        raise ValueError(f"{what} is not a WASM module: it lacks the \\0asm header")
