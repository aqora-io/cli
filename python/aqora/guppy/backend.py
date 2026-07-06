from __future__ import annotations

import base64
import binascii
from typing import Any, Mapping

from aqora import Client
from aqora._provider import jobs, wire
from aqora._provider.client import AqoraGraphQLClient

from .job import QPUJob

# Magic prefixes of the two program encodings the provider accepts for guppy
# workflows: a HUGR package envelope (what guppy compiles to) and LLVM/QIR
# bitcode.
_HUGR_MAGIC = b"HUGR"
_QIR_MAGIC = b"BC\xc0\xde"


def _encode_program(program: Any) -> tuple[str, int]:
    """Normalize a program into `(base64 serialization, serialization_format)`.

    Accepts a `@guppy`-decorated function (compiled via `.compile()`), a hugr
    `Package` (serialized via `.to_bytes()`), raw HUGR envelope or QIR bitcode
    bytes, or a base64 string of either.
    """
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
        try:
            raw = base64.b64decode(program, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise ValueError(
                "string programs must be base64-encoded HUGR envelopes or QIR bitcode"
            ) from exc
    else:
        raise TypeError(
            "`program` must be a guppy function, a hugr Package, HUGR envelope "
            "bytes, QIR bitcode bytes, or a base64 string of either"
        )
    if raw.startswith(_HUGR_MAGIC):
        serialization_format = wire.PROGRAM_HUGR_V1
    elif raw.startswith(_QIR_MAGIC):
        serialization_format = wire.PROGRAM_QIR_V1
    else:
        raise ValueError(
            "program bytes are neither a HUGR package envelope (`HUGR` magic) "
            "nor QIR bitcode (`BC\\xc0\\xde` magic)"
        )
    return base64.b64encode(raw).decode("ascii"), serialization_format


class QPU:
    """Guppy backend that submits programs to the QPU bound to the current runner session.

    `platform` selects the provider platform jobs are submitted to, by name or
    id (the schema's `ProviderPlatformNameOrID`). When omitted, the server
    chooses its default platform.
    """

    def __init__(
        self,
        client: Client | None = None,
        *,
        url: str | None = None,
        allow_insecure_host: bool | None = None,
        platform: str | None = None,
        compress: bool = True,
    ) -> None:
        if client is not None and (url is not None or allow_insecure_host is not None):
            raise ValueError(
                "`url` and `allow_insecure_host` cannot be combined with an explicit `client`"
            )
        raw_client = client or Client(url, allow_insecure_host=allow_insecure_host)
        self._graphql = AqoraGraphQLClient(raw_client)
        self._platform = platform
        self._compress = compress

    @property
    def client(self) -> Client:
        return self._graphql.client

    @property
    def platform(self) -> str | None:
        return self._platform

    def run(self, program: Any, **options: Any) -> QPUJob:
        """Submit a guppy program as a provider job.

        Only `shots` is forwarded to the provider API; any other option set to
        a non-None value is rejected.
        """
        shots = self._effective_shots(options)
        unsupported = self._unsupported_run_options(options)
        if unsupported:
            raise NotImplementedError(
                "The aqora provider GraphQL API only supports `shots` as a per-run "
                f"parameter (unsupported options: {', '.join(sorted(unsupported))})"
            )
        if shots is not None and shots < 1:
            raise ValueError("`shots` must be at least 1")

        payload = wire.build_model_payload(
            [_encode_program(program)],
            compress=self._compress,
        )
        job = jobs.submit_model(
            self._graphql,
            payload,
            shots=shots,
            platform=self._platform,
        )
        return QPUJob(self, job.job_id, payload=job._payload)

    def _effective_shots(self, overrides: Mapping[str, Any]) -> int | None:
        shots = overrides.get("shots")
        if shots is None:
            return None
        if isinstance(shots, bool):
            raise TypeError("`shots` must be an integer")
        try:
            as_int = int(shots)
        except (TypeError, ValueError) as exc:
            raise TypeError("`shots` must be an integer") from exc
        if as_int != shots:
            raise TypeError("`shots` must be an integer")
        return as_int

    def _unsupported_run_options(self, overrides: Mapping[str, Any]) -> list[str]:
        # `is not None` (not truthiness) so that falsy-but-meaningful values
        # like seed=0 are rejected rather than silently dropped.
        unsupported = []
        for key, value in overrides.items():
            if key == "shots":
                continue
            if value is not None:
                unsupported.append(key)
        return unsupported
