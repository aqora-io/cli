from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from . import wire

if TYPE_CHECKING:
    from pytket.backends.backendresult import BackendResult

# The keys `BackendResult.from_dict` accepts; the provider's result attributes
# can carry extra provider-side fields that must be dropped before decoding.
_BACKEND_RESULT_KEYS = {
    "qubits",
    "bits",
    "shots",
    "counts",
    "state",
    "unitary",
    "density_matrix",
    "ppcirc",
}


@dataclass(frozen=True)
class QirLabeledResult:
    """Quantinuum labeled QIR output (tab-separated line protocol).

    `text` is the raw payload; `shots` holds the `OUTPUT` records of each
    `START`..`END` block as `(type, value, label)` tuples.
    """

    text: str
    shots: list[list[tuple[str, str, str]]]

    def register_counts(self) -> dict[str, Counter[str]]:
        counts: dict[str, Counter[str]] = {}
        for shot in self.shots:
            for _type, value, label in shot:
                counts.setdefault(label, Counter())[value] += 1
        return counts


def parse_qir_labeled(text: str) -> QirLabeledResult:
    shots: list[list[tuple[str, str, str]]] = []
    current: list[tuple[str, str, str]] | None = None
    for line in text.splitlines():
        fields = line.split("\t")
        tag = fields[0]
        if tag == "START":
            current = []
        elif tag == "END":
            if current is not None:
                shots.append(current)
                current = None
        elif tag == "OUTPUT" and current is not None and len(fields) >= 3:
            output_type = fields[1]
            value = fields[2]
            label = fields[3] if len(fields) > 3 else ""
            current.append((output_type, value, label))
    return QirLabeledResult(text=text, shots=shots)


@dataclass(frozen=True)
class ProviderResult:
    index: int
    serialization_format: int
    raw: str
    # Set when the provider failed this individual result; `serialization_format`
    # and `raw` are then unset (sentinel `-1`/`""`).
    error: str | None = None

    def _require_format(self, expected: int, description: str) -> None:
        if self.serialization_format != expected:
            raise ValueError(
                f"Result {self.index} is not {description} "
                f"(serialization format {self.serialization_format})"
            )

    def to_backend_result(self) -> "BackendResult":
        self._require_format(
            wire.RESULT_PYTKET_BACKEND_RESULT_JSON_V1, "a pytket backend result"
        )
        from aqora.pytket._deps import BackendResult

        data = json.loads(self.raw)
        filtered = {key: value for key, value in data.items() if key in _BACKEND_RESULT_KEYS}
        return BackendResult.from_dict(filtered)

    def qsys_shots(self) -> list[Any]:
        self._require_format(wire.RESULT_QSYS_RESULT_JSON_V1, "a QSYS result")
        return json.loads(self.raw)

    def to_qsys_result(self) -> Any:
        shots = self.qsys_shots()
        try:
            from hugr.qsystem.result import QsysResult
        except ImportError as exc:
            raise ImportError(
                "Parsing QSYS results requires the `hugr` package. "
                "Install `aqora[guppy]` (guppylang) to use this method; "
                "`qsys_shots()` returns the raw shot array without it."
            ) from exc
        return QsysResult(shots)

    def qir_labeled(self) -> QirLabeledResult:
        self._require_format(wire.RESULT_QIR_LABELED_RESULT_V1, "a labeled QIR result")
        return parse_qir_labeled(self.raw)
