from __future__ import annotations

try:
    from qio.core import (
        BackendData,
        ClientData,
        QuantumComputationModel,
        QuantumProgram,
        QuantumProgramResult,
    )
    from qiskit import QuantumCircuit
    from qiskit.primitives import BackendEstimatorV2, BackendSamplerV2
    from qiskit.providers import BackendV2, JobV1, Options
    from qiskit.providers.jobstatus import JobStatus
    from qiskit.result import Result
    from qiskit.transpiler import Target
except ImportError as exc:  # pragma: no cover - depends on optional deps
    raise ImportError(
        "aqora_cli.qiskit requires the optional Qiskit dependencies. "
        "Install `aqora-cli[qiskit]` to use this module."
    ) from exc

__all__ = [
    "BackendData",
    "BackendEstimatorV2",
    "BackendSamplerV2",
    "BackendV2",
    "ClientData",
    "JobStatus",
    "JobV1",
    "Options",
    "QuantumCircuit",
    "QuantumComputationModel",
    "QuantumProgram",
    "QuantumProgramResult",
    "Result",
    "Target",
]
