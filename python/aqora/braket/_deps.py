from __future__ import annotations

try:
    from braket.circuits import Circuit
    from braket.devices import Device
    from braket.tasks import GateModelQuantumTaskResult, QuantumTask
except ImportError as exc:  # pragma: no cover - depends on optional deps
    raise ImportError(
        "aqora.braket requires the optional Braket dependencies. "
        "Install `aqora[braket]` to use this module."
    ) from exc

__all__ = [
    "Circuit",
    "Device",
    "GateModelQuantumTaskResult",
    "QuantumTask",
]
