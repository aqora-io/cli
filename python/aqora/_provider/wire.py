"""The qio wire format for provider programs and results.

Building and parsing are implemented in Rust by the `qio` crate
(github.com/aqora-io/qio-rs) and exposed through `aqora._aqora`, so the wire
contract (JSON shape, format discriminants, zlib+base64 compression) has a
single source of truth shared with the platform.
"""

from __future__ import annotations

import importlib.metadata

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
) -> str:
    """Build a `QuantumComputationModel` JSON payload.

    `programs` holds `(serialization, serialization_format)` pairs.
    """
    return qio_build_model_payload(
        programs,
        user_agent=f"aqora/{_package_version()}",
        backend_name=backend_name,
        compress=compress,
    )


def parse_result_payload(payload: str) -> tuple[int, str]:
    """Decode a result payload into `(serialization_format, raw serialization)`."""
    return qio_parse_result_payload(payload)
