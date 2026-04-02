from __future__ import annotations

from typing import Any, Mapping

from ._deps import BackendEstimatorV2, BackendSamplerV2
from .backend import AqoraBackend


def sampler(
    backend: AqoraBackend,
    *,
    options: Mapping[str, Any] | None = None,
) -> BackendSamplerV2:
    return backend.sampler(options=options)


def estimator(
    backend: AqoraBackend,
    *,
    options: Mapping[str, Any] | None = None,
) -> BackendEstimatorV2:
    return backend.estimator(options=options)
