"""A provider backend that negotiates its wire format with the platform."""

from __future__ import annotations

from typing import Any, Mapping

from aqora import Client

from ._provider import formats, jobs, wire
from ._provider.jobs import ProviderJob, _resolve_graphql, qualified_platform_name
from ._provider.results import ProviderResult


def _as_programs(programs: Any) -> list[Any]:
    """Normalize the `run` argument into a list of programs.

    Only a list or tuple means "several programs": strings, bytes and framework
    circuits are all iterable in their own right, so duck-typing the argument
    would silently tear a single circuit into its instructions.
    """
    if isinstance(programs, (list, tuple)):
        return list(programs)
    return [programs]


class QPU:
    """Submits programs from any supported framework to a provider platform.

    The framework backends in `aqora.qiskit`, `aqora.pytket` and `aqora.guppy`
    each speak one fixed serialization format. This one instead reads the
    formats the platform accepts (`meta.inputFormats`, most preferred first) and
    encodes into the first of them the submitted programs can produce, so a
    qiskit circuit can reach a HUGR-preferring platform and a guppy program a
    QASM-preferring one.

    `platform` selects the provider platform by name or id (the schema's
    `ProviderPlatformNameOrID`). When it is omitted the server chooses a default
    platform that this client cannot identify, so there is nothing to negotiate
    against and programs are submitted in their native format.
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
        self._graphql = _resolve_graphql(
            client, url=url, allow_insecure_host=allow_insecure_host
        )
        self._platform = platform
        self._compress = compress
        self._input_formats: list[int] | None = None

    @property
    def client(self) -> Client:
        return self._graphql.client

    @property
    def platform(self) -> str | None:
        return self._platform

    @property
    def input_formats(self) -> list[int]:
        """The formats the platform accepts, most preferred first.

        Fetched lazily on first use and cached. Empty when no platform is
        selected, and empty too if the platform advertises none — the server's
        provider RPC degrades to an empty list the same way `maxQubits` degrades
        to null. Either way encoding falls back to the programs' native formats
        and the server remains the authority on what it will accept.
        """
        if self._input_formats is None:
            self._input_formats = self._load_input_formats()
        return self._input_formats

    def run(self, programs: Any, *, shots: int | None = None) -> "QPUJob":
        """Submit one or more programs as a provider job.

        `programs` is a single program, or a list or tuple of them. Accepted
        programs are qiskit `QuantumCircuit`s, pytket `Circuit`s,
        `@guppy`-decorated functions, hugr `Package`s, raw HUGR envelope or QIR
        bitcode bytes, and QASM source.

        Every program in a job shares one serialization format: the first the
        platform accepts that all of them can produce.
        """
        sources = [formats.detect(program) for program in _as_programs(programs)]
        encoded, serialization_format = formats.encode(sources, self.input_formats)
        job = jobs.submit_model(
            self._graphql,
            wire.build_model_payload(encoded, compress=self._compress),
            shots=jobs.normalize_shots(shots),
            platform=self._platform,
        )
        return QPUJob(
            self,
            job.job_id,
            payload=job._payload,
            serialization_format=serialization_format,
        )

    def _load_input_formats(self) -> list[int]:
        # Without a selected platform the server picks one we cannot identify
        # here, so there is nothing to read formats from; defer to the server
        # rather than guess from the union across all platforms.
        if self._platform is None:
            return []
        self._graphql.ensure_authenticated()
        platforms = [
            platform
            for platform in self._graphql.get_provider_platforms()
            if jobs.platform_matches(self._platform, platform)
        ]
        if not platforms:
            raise LookupError(f"Provider platform {self._platform!r} was not found")
        meta = platforms[0].get("meta") or {}
        return formats.formats_from_graphql(meta.get("inputFormats") or [])


class QPUJob(ProviderJob):
    def __init__(
        self,
        backend: QPU,
        job_id: str,
        *,
        payload: Mapping[str, Any] | None = None,
        serialization_format: int | None = None,
    ) -> None:
        super().__init__(backend._graphql, job_id, payload=payload)
        self._qpu = backend
        self._serialization_format = serialization_format

    @classmethod
    def from_id(
        cls,
        job_id: str,
        *,
        client: Client | None = None,
        url: str | None = None,
        allow_insecure_host: bool | None = None,
    ) -> "QPUJob":
        """Load a job by id, building its `QPU` from the job's platform."""
        graphql = _resolve_graphql(client, url=url, allow_insecure_host=allow_insecure_host)
        graphql.ensure_authenticated()
        payload = graphql.get_provider_job(job_id)
        backend = QPU(
            graphql.client,
            platform=qualified_platform_name(payload.get("platform")),
        )
        return cls(backend, job_id, payload=payload)

    def backend(self) -> QPU:
        return self._qpu

    @property
    def serialization_format(self) -> int | None:
        """The format the programs were submitted in.

        `None` for a job loaded with `from_id`, which never saw the submission.
        """
        return self._serialization_format

    def result(self, timeout: float | None = None, wait: float = 5.0) -> ProviderResult:
        """Wait for a single-program job to complete and return its result."""
        self.wait(timeout=timeout, poll_interval=wait)
        items = self.results()
        if len(items) != 1:
            raise RuntimeError(
                f"aqora provider job {self.job_id!r} returned {len(items)} result "
                "payloads; use `result_items()` to inspect them"
            )
        return items[0]

    def result_items(self) -> list[ProviderResult]:
        return self.results()

    def counts(
        self,
        timeout: float | None = None,
        wait: float = 5.0,
    ) -> list[dict[str, int]]:
        """Wait for the job and normalize each result to counts.

        One mapping per program, in submission order, whatever format the
        provider returned.
        """
        self.wait(timeout=timeout, poll_interval=wait)
        return [item.counts() for item in self.results()]
