"""A provider backend that negotiates its wire format with the platform."""

from __future__ import annotations

from typing import Any, Mapping

from aqora import Client

from ._provider import formats, jobs, wire
from ._provider.jobs import ProviderJob, _resolve_graphql, qualified_platform_name
from ._provider.models import ProviderModel
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

    `as_entity` is the username or id of an organization you belong to; jobs
    are attributed to it, which is how provider quota is tracked per team. When
    it is omitted jobs are attributed to you personally.
    """

    def __init__(
        self,
        client: Client | None = None,
        *,
        url: str | None = None,
        allow_insecure_host: bool | None = None,
        platform: str | None = None,
        as_entity: str | None = None,
        compress: bool = True,
    ) -> None:
        self._graphql = _resolve_graphql(
            client, url=url, allow_insecure_host=allow_insecure_host
        )
        self._platform = platform
        self._as_entity = as_entity
        self._compress = compress
        self._input_formats: list[int] | None = None

    @property
    def client(self) -> Client:
        return self._graphql.client

    @property
    def platform(self) -> str | None:
        return self._platform

    @property
    def as_entity(self) -> str | None:
        return self._as_entity

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
            self._input_formats = jobs.load_input_formats(self._graphql, self._platform)
        return self._input_formats

    def upload(self, programs: Any, *, wasm: Any = None) -> ProviderModel:
        """Upload one or more programs as a model, to run as many jobs as needed.

        `programs` is a single program, or a list or tuple of them. Accepted
        programs are qiskit `QuantumCircuit`s, pytket `Circuit`s,
        `@guppy`-decorated functions, hugr `Package`s, raw HUGR envelope or QIR
        bitcode bytes, and QASM source.

        Every program in a model shares one serialization format: the first the
        platform accepts that all of them can produce. A job can run the model
        on any platform that accepts that format.

        `wasm` is a WASM module the programs call into: a pytket
        `WasmFileHandler`/`WasmModuleHandler`, the module's bytecode, or a path
        to a `.wasm` file. It is part of the model, so every job run from it
        uses the module. Only Nexus platforms accept one.

        The model's `id` reloads it later, e.g. in another session, with
        `model`.
        """
        sources = [formats.detect(program) for program in _as_programs(programs)]
        encoded, serialization_format = formats.encode(sources, self.input_formats)
        return jobs.upload_programs(
            self._graphql,
            wire.build_model_payload(encoded, compress=self._compress, wasm=wasm),
            serialization_format=serialization_format,
            program_count=len(encoded),
            has_wasm_module=wasm is not None,
            as_entity=self._as_entity,
        )

    def model(self, model_id: str) -> ProviderModel:
        """The model `model_id`, as uploaded earlier with `upload`."""
        return ProviderModel(self._graphql, model_id)

    def run(
        self,
        programs: Any,
        *,
        shots: int,
        wasm: Any = None,
        options: Mapping[str, Any] | None = None,
    ) -> "QPUJob":
        """Submit a model, or one or more programs, as a provider job.

        `shots` is required: every program in the job runs that many times.

        `programs` is a model from `upload` or `model`, which runs without
        being uploaded again, or programs as `upload` takes them, which are
        uploaded for this job alone. A model must be in a format the platform
        accepts, and must be owned by the job's `as_entity`.

        `wasm` attaches a WASM module the programs call into, as for `upload`.
        A model already holds its WASM module, so it can't be given one here.

        `options` are backend options for the job. Nexus H-series devices and
        emulators accept `noisy_simulation`, `simulator`, `error_params`,
        `compiler_options`, `no_opt`, `allow_2q_gate_rebase`,
        `target_2qb_gate`, `leakage_detection` and `simplify_initial`. Nexus
        Selene accepts `n_qubits` (the qubits to simulate, by default the
        device's width), `simulator` and `error_model`, and Selene Plus also
        `runtime`, e.g. `{"n_qubits": 8, "runtime": {"type": "HeliosRuntime"}}`.
        Other platforms accept none, and the server rejects the job before it
        runs.
        """
        shots = jobs.normalize_shots(shots)
        if isinstance(programs, ProviderModel):
            jobs.reject_wasm_for_model(wasm)
            model = programs
        else:
            if isinstance(programs, (list, tuple)) and any(
                isinstance(program, ProviderModel) for program in programs
            ):
                raise TypeError(
                    "A job runs a single model: pass it on its own, not in a list"
                )
            # Checked first, so a bad argument fails before anything is uploaded.
            jobs.normalize_options(options)
            model = self.upload(programs, wasm=wasm)
        job = jobs.submit_job(
            self._graphql,
            model,
            shots=shots,
            platform=self._platform,
            as_entity=self._as_entity,
            options=options,
            input_formats=self.input_formats,
        )
        return QPUJob(
            self,
            job.job_id,
            payload=job._payload,
            serialization_format=model.serialization_format,
        )


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
