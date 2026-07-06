from __future__ import annotations

try:
    from hugr.qsystem.result import QsysResult
except ImportError as exc:  # pragma: no cover - depends on optional deps
    raise ImportError(
        "aqora.guppy requires the optional guppy dependencies. "
        "Install `aqora[guppy]` to use this module."
    ) from exc

__all__ = [
    "QsysResult",
]
