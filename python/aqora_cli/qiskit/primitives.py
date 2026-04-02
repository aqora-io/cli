from __future__ import annotations

from typing import Any, Mapping

from ._deps import BackendEstimatorV2, BackendSamplerV2
from .backend import QPU


def sampler(
    qpu: QPU,
    *,
    options: Mapping[str, Any] | None = None,
) -> BackendSamplerV2:
    return qpu.sampler(options=options)


def estimator(
    qpu: QPU,
    *,
    options: Mapping[str, Any] | None = None,
) -> BackendEstimatorV2:
    return qpu.estimator(options=options)
