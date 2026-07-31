from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable

from . import wire

if TYPE_CHECKING:
    from pytket.backends.backendresult import BackendResult
    from qiskit.result import Result

# CUDA-Q's own name for the register holding the joint distribution over every
# measured qubit; the named registers beside it are per-register marginals.
_CUDAQ_GLOBAL_REGISTER = "__global__"

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


def _cudaq_register_counts(data: Any) -> dict[str, dict[str, int]]:
    """Decode a CUDA-Q `SampleResult.serialize()` array into per-register counts.

    The payload is a flat integer array: for each register, its name as
    character codes, then a triplet of (packed bitstring, bit width, count) per
    distinct outcome. Decoding it here keeps the heavyweight `cudaq` package out
    of the dependency tree just to read a result back.
    """
    if not isinstance(data, list) or not all(
        isinstance(item, int) and item >= 0 for item in data
    ):
        raise ValueError("payload is not an array of non-negative integers")
    registers: dict[str, dict[str, int]] = {}
    stride = 0
    while stride < len(data):
        name_length = data[stride]
        stride += 1
        # The register record must still hold the full name plus an outcome
        # count after it.
        if stride + name_length >= len(data):
            raise ValueError("payload is truncated mid-record")
        name = "".join(chr(code) for code in data[stride : stride + name_length])
        stride += name_length

        outcomes = data[stride]
        stride += 1
        if stride + 3 * outcomes > len(data):
            raise ValueError("payload is truncated mid-record")
        counts: dict[str, int] = {}
        for _ in range(outcomes):
            packed, width, count = data[stride], data[stride + 1], data[stride + 2]
            if packed.bit_length() > width:
                raise ValueError(
                    f"outcome {packed} does not fit its declared width of {width} bits"
                )
            counts[bin(packed)[2:].zfill(width)] = count
            stride += 3
        registers[name] = counts
    return registers


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
        # The provider emits every result slot and populates only the ones the
        # job produced: a shots job returns real `shots` beside `counts: []`
        # and `qubits: []`. `BackendResult.from_dict` keys on presence, not
        # truthiness, and rejects counts alongside shots, so the empty slots
        # have to go too, not just the unknown keys.
        filtered = {
            key: value
            for key, value in data.items()
            if key in _BACKEND_RESULT_KEYS and value not in ([], None)
        }
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

    def to_qiskit_result(self) -> "Result":
        self._require_format(wire.RESULT_QISKIT_RESULT_JSON_V1, "a qiskit result")
        try:
            from qiskit.result import Result
        except ImportError as exc:
            raise ImportError(
                "Parsing qiskit results requires the `qiskit` package. "
                "Install `aqora[qiskit]` to use this method."
            ) from exc
        return Result.from_dict(json.loads(self.raw))

    def to_cirq_result(self) -> Any:
        self._require_format(wire.RESULT_CIRQ_RESULT_JSON_V1, "a cirq result")
        try:
            import cirq
        except ImportError as exc:
            raise ImportError(
                "Parsing cirq results requires the `cirq` package. "
                "Install `aqora[cirq]` to use this method."
            ) from exc
        return cirq.read_json(json_text=self.raw)

    def cudaq_register_counts(self) -> dict[str, dict[str, int]]:
        """Per-register counts of a CUDA-Q sample result. Needs no `cudaq`."""
        self._require_format(
            wire.RESULT_CUDAQ_SAMPLE_RESULT_JSON_V1, "a CUDA-Q sample result"
        )
        # `json.JSONDecodeError` is a `ValueError`, so undecodable raw text is
        # wrapped the same way as a well-formed but invalid array.
        try:
            return _cudaq_register_counts(json.loads(self.raw))
        except ValueError as error:
            raise ValueError(
                f"Result {self.index} is not a valid CUDA-Q sample payload: {error}"
            ) from error

    def counts(self) -> dict[str, int]:
        """Measurement counts, normalized across every result format.

        Keys are bitstrings; a result carrying several named registers joins
        their values with a space in label order, matching qiskit's convention
        for multiple classical registers. Counts are always joint over the
        registers of a shot, never a product of per-register marginals.
        """
        if self.error is not None:
            raise RuntimeError(f"Result {self.index} failed: {self.error}")
        to_counts = _COUNTS_BY_FORMAT.get(self.serialization_format)
        if to_counts is None:
            raise ValueError(
                f"Result {self.index} has no counts representation "
                f"(serialization format {self.serialization_format})"
            )
        return to_counts(self)


def _pytket_counts(result: ProviderResult) -> dict[str, int]:
    backend_result = result.to_backend_result()
    # `get_counts()` orders outcome columns by `sorted(c_bits)` — register name
    # first, then bit index — so same-register columns are contiguous; a space
    # between register groups keeps multi-register keys on the `counts()`
    # convention.
    registers = [bit.reg_name for bit in sorted(backend_result.c_bits)]
    counts: dict[str, int] = {}
    for outcome, count in backend_result.get_counts().items():
        parts: list[str] = []
        for position, bit in enumerate(outcome):
            if position and registers[position] != registers[position - 1]:
                parts.append(" ")
            parts.append(str(bit))
        counts["".join(parts)] = count
    return counts


def _qsys_counts(result: ProviderResult) -> dict[str, int]:
    # `collated_counts` is joint across tags per shot; `register_counts` is not.
    return {
        " ".join(bits for _tag, bits in entries): count
        for entries, count in result.to_qsys_result().collated_counts().items()
    }


def _qir_labeled_counts(result: ProviderResult) -> dict[str, int]:
    # Built from the shots rather than `register_counts()`: those are per-label
    # marginals, and combining them would invent a joint distribution the shots
    # never actually showed.
    counts: Counter[str] = Counter()
    for shot in result.qir_labeled().shots:
        outputs = sorted(shot, key=lambda output: output[2])
        counts[" ".join(value for _type, value, _label in outputs)] += 1
    return dict(counts)


def _qiskit_counts(result: ProviderResult) -> dict[str, int]:
    counts = result.to_qiskit_result().get_counts()
    # One `ProviderResult` is one program, so a multi-experiment payload means
    # the provider merged results we cannot attribute to a single circuit.
    if isinstance(counts, list):
        if len(counts) != 1:
            raise ValueError(
                f"Result {result.index} carries {len(counts)} experiments; "
                "use `to_qiskit_result()` to inspect them"
            )
        counts = counts[0]
    return dict(counts)


def _cirq_counts(result: ProviderResult) -> dict[str, int]:
    measurements = result.to_cirq_result().measurements
    if not measurements:
        return {}
    keys = sorted(measurements)
    counts: Counter[str] = Counter()
    for shot in range(len(measurements[keys[0]])):
        counts[
            " ".join(
                "".join(str(int(bit)) for bit in measurements[key][shot])
                for key in keys
            )
        ] += 1
    return dict(counts)


def _cudaq_counts(result: ProviderResult) -> dict[str, int]:
    registers = result.cudaq_register_counts()
    if _CUDAQ_GLOBAL_REGISTER in registers:
        return registers[_CUDAQ_GLOBAL_REGISTER]
    if len(registers) == 1:
        return next(iter(registers.values()))
    # Named registers hold marginals; without the global one there is no joint
    # distribution to report, and multiplying them out would be a fiction.
    raise ValueError(
        f"Result {result.index} has no `{_CUDAQ_GLOBAL_REGISTER}` register and "
        f"{len(registers)} named registers, so it carries no joint counts; "
        "use `cudaq_register_counts()` to read them individually"
    )


_COUNTS_BY_FORMAT: dict[int, Callable[[ProviderResult], dict[str, int]]] = {
    wire.RESULT_PYTKET_BACKEND_RESULT_JSON_V1: _pytket_counts,
    wire.RESULT_QSYS_RESULT_JSON_V1: _qsys_counts,
    wire.RESULT_QIR_LABELED_RESULT_V1: _qir_labeled_counts,
    wire.RESULT_QISKIT_RESULT_JSON_V1: _qiskit_counts,
    wire.RESULT_CIRQ_RESULT_JSON_V1: _cirq_counts,
    wire.RESULT_CUDAQ_SAMPLE_RESULT_JSON_V1: _cudaq_counts,
}
