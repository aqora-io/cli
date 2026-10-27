"""Provider models: programs uploaded once and run as many jobs as needed."""

from __future__ import annotations

import json
from typing import Any

from aqora import Client

from .client import AqoraGraphQLClient


class ProviderModel:
    """Programs uploaded to aqora, ready to run as provider jobs.

    A QPU's `upload` returns one; reload one by its `id` with the QPU's `model`
    or `from_id`, e.g. in a later session. A WASM module uploaded with the
    programs is part of the model, so every job run from it uses the module.

    `serialization_format` and `program_count` are known right away for a
    model uploaded in this session. For one reloaded by id they are read from
    the uploaded payload, which is downloaded once, on first use.
    """

    def __init__(
        self,
        graphql: AqoraGraphQLClient,
        model_id: str,
        *,
        serialization_format: int | None = None,
        program_count: int | None = None,
        has_wasm_module: bool | None = None,
    ) -> None:
        self._graphql = graphql
        self._id = model_id
        self._serialization_format = serialization_format
        self._program_count = program_count
        self._has_wasm_module = has_wasm_module

    @classmethod
    def from_id(
        cls,
        model_id: str,
        *,
        client: Client | None = None,
        url: str | None = None,
        allow_insecure_host: bool | None = None,
    ) -> "ProviderModel":
        """Load a model by id using the default client when none is given."""
        # Imported here: `jobs` builds models, so it imports this module.
        from .jobs import _resolve_graphql

        graphql = _resolve_graphql(client, url=url, allow_insecure_host=allow_insecure_host)
        return cls(graphql, model_id)

    @property
    def id(self) -> str:
        return self._id

    @property
    def serialization_format(self) -> int:
        """The format the programs are serialized in, shared by all of them."""
        if self._serialization_format is None:
            self._load()
        assert self._serialization_format is not None
        return self._serialization_format

    @property
    def program_count(self) -> int:
        """How many programs the model holds; a job returns one result each."""
        if self._program_count is None:
            self._load()
        assert self._program_count is not None
        return self._program_count

    @property
    def has_wasm_module(self) -> bool:
        """Whether the model embeds a WASM module the programs call into."""
        if self._has_wasm_module is None:
            self._load()
        assert self._has_wasm_module is not None
        return self._has_wasm_module

    def _load(self) -> None:
        self._graphql.ensure_authenticated()
        payload: Any = json.loads(self._graphql.download_provider_model(self._id))
        programs = payload.get("programs") if isinstance(payload, dict) else None
        # A WASM module uploaded on its own, for the deprecated `wasmModuleId`,
        # is a provider model too, but holds no programs.
        if not isinstance(programs, list) or not programs:
            raise ValueError(f"Provider model {self._id!r} holds no programs")
        formats = {program.get("serialization_format") for program in programs}
        if len(formats) != 1:
            raise ValueError(
                f"Provider model {self._id!r} mixes serialization formats; "
                "a job runs programs of a single format"
            )
        serialization_format = formats.pop()
        if not isinstance(serialization_format, int):
            raise ValueError(
                f"Provider model {self._id!r} doesn't record its programs' serialization format"
            )
        self._serialization_format = serialization_format
        self._program_count = len(programs)
        self._has_wasm_module = payload.get("wasm_module") is not None

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._id!r})"
