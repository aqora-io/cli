from __future__ import annotations

import json
import time
from typing import Any, Mapping, Sequence

from aqora import Client

from . import formats, wire
from .client import AqoraGraphQLClient
from .models import ProviderModel
from .results import ProviderResult


def platform_matches(requested: str, platform: Mapping[str, Any]) -> bool:
    """Whether a platform listing entry matches a requested platform string.

    Accepts the platform's global id, its bare name, or the server's
    `provider:name` form (the format the `ProviderPlatformNameOrID` scalar
    requires for name strings; the provider segment is the lowercase enum
    value, e.g. `nexus:Selene`).
    """
    candidates = {platform.get("id"), platform.get("name")}
    provider = platform.get("provider")
    name = platform.get("name")
    if provider and name:
        candidates.add(f"{str(provider).lower()}:{name}")
    return requested in candidates


def qualified_platform_name(platform: Mapping[str, Any] | None) -> str | None:
    """The `provider:name` string for a platform entry, e.g. `nexus:Selene`.

    This is the canonical form the server's `ProviderPlatformNameOrID` scalar
    requires for name strings.
    """
    if not platform:
        return None
    provider = platform.get("provider")
    name = platform.get("name")
    if provider and name:
        return f"{str(provider).lower()}:{name}"
    return name or platform.get("id")


def _resolve_graphql(
    client: Client | None = None,
    *,
    url: str | None = None,
    allow_insecure_host: bool | None = None,
) -> AqoraGraphQLClient:
    if client is not None and (url is not None or allow_insecure_host is not None):
        raise ValueError(
            "`url` and `allow_insecure_host` cannot be combined with an explicit `client`"
        )
    return AqoraGraphQLClient(client or Client(url, allow_insecure_host=allow_insecure_host))


def is_provider_failure(status: str | None, error: str | None) -> bool:
    """Whether a job payload's `(status, error)` signals a provider failure.

    The server nulls `status` and puts the provider's message in `error` when
    the provider reports an error state. For live jobs `error` instead mirrors
    the provider's progress message ("The job is queued."), so a failure is
    only the combination of a null status with a non-empty error.
    """
    return status is None and bool(error)


def normalize_shots(shots: Any) -> int:
    """Coerce a user-supplied shot count into a positive int.

    The provider API takes a single `shots` value shared by every circuit in a
    job. Providers do not agree on a default (some fail the job, others return
    a result without counts), so it is required. Rejects bools and
    non-integral values; requires `>= 1`.
    """
    if shots is None:
        raise ValueError(
            "`shots` is required: pass the number of shots to run, e.g. `run(program, shots=1000)`"
        )
    if isinstance(shots, bool):
        raise TypeError("`shots` must be an integer")
    try:
        as_int = int(shots)
    except (TypeError, ValueError) as exc:
        raise TypeError("`shots` must be an integer") from exc
    if as_int != shots:
        raise TypeError("`shots` must be an integer")
    if as_int < 1:
        raise ValueError("`shots` must be at least 1")
    return as_int


def _errored_result(index: int, error: str) -> ProviderResult:
    # An errored item carries no serialization; callers must check `.error`
    # before reading `serialization_format`/`raw`.
    return ProviderResult(index=index, serialization_format=-1, raw="", error=error)


def upload_model(
    graphql: AqoraGraphQLClient,
    payload: str,
    *,
    as_entity: str | None = None,
) -> str:
    """Upload `payload` as a provider model owned by `as_entity` and return its id."""
    upload_info = graphql.start_provider_model_upload(as_entity=as_entity)
    etag = graphql.upload_payload(upload_info["uploadUrl"], payload)
    model = graphql.create_provider_model(
        provider_model_upload_id=upload_info["providerModelUploadId"],
        etag=etag,
    )
    return str(model["id"])


def load_input_formats(graphql: AqoraGraphQLClient, platform: str | None) -> list[int]:
    """The formats `platform` accepts, most preferred first.

    Empty when no platform is selected: the server then picks one we cannot
    identify here, so there is nothing to read formats from, and we defer to
    the server rather than guess from the union across all platforms. Empty
    too if the platform advertises none — the server's provider RPC degrades
    to an empty list the same way `maxQubits` degrades to null.
    """
    if platform is None:
        return []
    graphql.ensure_authenticated()
    platforms = [
        entry for entry in graphql.get_provider_platforms() if platform_matches(platform, entry)
    ]
    if not platforms:
        raise LookupError(f"Provider platform {platform!r} was not found")
    meta = platforms[0].get("meta") or {}
    return formats.formats_from_graphql(meta.get("inputFormats") or [])


def normalize_options(options: Any) -> Mapping[str, Any] | None:
    if options is None:
        return None
    if not isinstance(options, Mapping):
        raise TypeError(
            "`options` must be a mapping of option names to values, got "
            f"{type(options).__name__}"
        )
    # The round trip also snapshots nested values, so the job sends the
    # options as they were when it was submitted.
    try:
        return json.loads(json.dumps(dict(options), allow_nan=False))
    except (TypeError, ValueError) as err:
        raise type(err)(f"`options` must be JSON-serializable: {err}") from err


def reject_wasm_for_model(wasm: Any, argument: str = "wasm") -> None:
    """Refuse a WASM module given for a job that runs an uploaded model."""
    if wasm is not None:
        raise TypeError(
            f"`{argument}` cannot be given when running an uploaded model: its "
            "WASM module is part of the model, so pass it when uploading"
        )


def upload_programs(
    graphql: AqoraGraphQLClient,
    payload: str,
    *,
    serialization_format: int | None,
    program_count: int,
    has_wasm_module: bool,
    as_entity: str | None = None,
) -> ProviderModel:
    """Upload a model `payload` (see `wire.build_model_payload`) owned by
    `as_entity`, returning it as a model to run jobs from.

    The other arguments describe what `payload` holds, so the model needn't
    download it back to find out; a `serialization_format` of None is read
    back from the upload if it is ever needed."""
    graphql.ensure_authenticated()
    return ProviderModel(
        graphql,
        upload_model(graphql, payload, as_entity=as_entity),
        serialization_format=serialization_format,
        program_count=program_count,
        has_wasm_module=has_wasm_module,
    )


def submit_job(
    graphql: AqoraGraphQLClient,
    model: ProviderModel,
    *,
    shots: int | None = None,
    platform: str | None = None,
    as_entity: str | None = None,
    options: Mapping[str, Any] | None = None,
    input_formats: Sequence[int] = (),
) -> "ProviderJob":
    """Submit a job running `model` on `platform`.

    `as_entity` attributes the job to that entity (an organization the user
    belongs to), which must own the model; the backend rejects the job
    otherwise.

    `options` are backend options for the job, e.g. emulator settings. The
    platform decides which it accepts and rejects the job otherwise.

    `input_formats` are the formats `platform` accepts (see
    `load_input_formats`). A model in none of them is refused before the job
    is submitted; when empty, the server is left to decide."""
    # Checked first, so a bad argument fails before the model is inspected.
    options = normalize_options(options)
    _check_model_format(model, platform, input_formats)
    graphql.ensure_authenticated()
    return _create_job(
        graphql, model.id, shots=shots, platform=platform, as_entity=as_entity, options=options
    )


def estimate_job(
    graphql: AqoraGraphQLClient,
    model: ProviderModel,
    *,
    shots: int,
    platform: str | None = None,
    as_entity: str | None = None,
    input_formats: Sequence[int] = (),
) -> "ProviderJob":
    """Submit a job estimating what running `model` for `shots` shots on
    `platform` would cost, in HQC: the server runs it on the platform's syntax
    checker, and the job's `hqc_cost` is the estimate once it completes.

    `as_entity` and `input_formats` are as for `submit_job`."""
    _check_model_format(model, platform, input_formats)
    graphql.ensure_authenticated()
    job = graphql.estimate_provider_job(
        provider_model_id=model.id,
        shots=shots,
        provider_platform=platform,
        as_entity=as_entity,
    )
    return ProviderJob(graphql, str(job["id"]), payload=job)


def _check_model_format(
    model: ProviderModel, platform: str | None, input_formats: Sequence[int]
) -> None:
    """Refuse a model in none of `input_formats`; when empty, the server is
    left to decide."""
    if input_formats and model.serialization_format not in input_formats:
        accepted = ", ".join(formats.format_name(accepted) for accepted in input_formats)
        raise ValueError(
            f"Provider model {model.id!r} holds "
            f"{formats.format_name(model.serialization_format)} programs, which "
            f"platform {platform!r} doesn't accept (it accepts {accepted})"
        )


def submit_model(
    graphql: AqoraGraphQLClient,
    payload: str,
    *,
    shots: int | None = None,
    platform: str | None = None,
    as_entity: str | None = None,
    options: Mapping[str, Any] | None = None,
) -> "ProviderJob":
    """Upload `payload` and submit it as a job.

    `as_entity` attributes both the upload and the job to that entity (an
    organization the user belongs to); the backend rejects a job whose model
    is owned by a different entity.

    `options` are backend options for the job, e.g. emulator settings. The
    platform decides which it accepts and rejects the job otherwise."""
    # Checked first, so a bad argument fails before anything is uploaded.
    options = normalize_options(options)
    graphql.ensure_authenticated()
    model_id = upload_model(graphql, payload, as_entity=as_entity)
    return _create_job(
        graphql, model_id, shots=shots, platform=platform, as_entity=as_entity, options=options
    )


def _create_job(
    graphql: AqoraGraphQLClient,
    model_id: str,
    *,
    shots: int | None,
    platform: str | None,
    as_entity: str | None,
    options: Mapping[str, Any] | None,
) -> "ProviderJob":
    job = graphql.create_provider_job(
        provider_model_id=model_id,
        shots=shots,
        provider_platform=platform,
        as_entity=as_entity,
        options=options,
    )
    return ProviderJob(graphql, str(job["id"]), payload=job)


class ProviderJob:
    def __init__(
        self,
        graphql: AqoraGraphQLClient,
        job_id: str,
        *,
        payload: Mapping[str, Any] | None = None,
    ) -> None:
        self._graphql = graphql
        self.job_id = job_id
        self._payload = dict(payload or {})

    @classmethod
    def from_id(
        cls,
        job_id: str,
        *,
        client: Client | None = None,
        url: str | None = None,
        allow_insecure_host: bool | None = None,
    ) -> "ProviderJob":
        """Load a job by id using the default client when none is given."""
        graphql = _resolve_graphql(client, url=url, allow_insecure_host=allow_insecure_host)
        graphql.ensure_authenticated()
        payload = graphql.get_provider_job(job_id)
        # Construct the base class explicitly: subclasses (aqora.guppy.QPUJob)
        # take a backend in `__init__` and override `from_id`.
        return ProviderJob(graphql, job_id, payload=payload)

    @property
    def client(self) -> Client:
        return self._graphql.client

    def refresh(self) -> Mapping[str, Any]:
        self._graphql.ensure_authenticated()
        self._payload = dict(self._graphql.get_provider_job(self.job_id))
        return self._payload

    def status(self) -> str | None:
        return self.refresh().get("status")

    def error(self) -> str | None:
        return self.refresh().get("error")

    def hqc_cost(self) -> float | None:
        """What the job costs in HQC (Quantinuum credits), as Nexus reports it.

        For a job from `estimate`, the estimated cost. `None` until Nexus
        reports it, usually once the job completes, and for other providers.
        """
        self._graphql.ensure_authenticated()
        return self._graphql.get_provider_job_hqc_cost(self.job_id)

    def wait(self, timeout: float | None = None, poll_interval: float = 5.0) -> None:
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            payload = self.refresh()
            self._raise_on_failure(payload)
            if payload.get("status") == "COMPLETED":
                return
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Timeout while waiting for aqora provider job {self.job_id!r}"
                )
            time.sleep(poll_interval)

    def results(self, *, raise_on_item_error: bool = True) -> list[ProviderResult]:
        """Download and decode every result payload of the job.

        By default a per-item error aborts the whole call. Pass
        `raise_on_item_error=False` to instead surface each failed item as a
        `ProviderResult` carrying its `error`, so that a single bad result does
        not poison the successful siblings in a multi-program job.
        """
        self._graphql.ensure_authenticated()
        payload = self.refresh()
        self._raise_on_failure(payload)
        items = self._graphql.get_provider_job_results(self.job_id)
        expected = payload.get("resultCount")
        if expected is not None and len(items) != expected:
            raise RuntimeError(
                f"aqora provider job {self.job_id!r} returned {len(items)} of "
                f"{expected} result payloads; results may still be uploading"
            )
        results = []
        for item in items:
            index = int(item["index"])
            error = item.get("error")
            if error:
                if raise_on_item_error:
                    raise RuntimeError(f"Provider job result {index} failed: {error}")
                results.append(_errored_result(index, str(error)))
                continue
            result_url = item.get("result")
            if not result_url:
                if raise_on_item_error:
                    raise RuntimeError(
                        f"Provider job result {index} is missing a result URL"
                    )
                results.append(_errored_result(index, "missing a result URL"))
                continue
            serialization_format, raw = wire.parse_result_payload(
                self._graphql.download_text(result_url)
            )
            results.append(
                ProviderResult(
                    index=index,
                    serialization_format=serialization_format,
                    raw=raw,
                )
            )
        results.sort(key=lambda result: result.index)
        return results

    def _raise_on_failure(self, payload: Mapping[str, Any]) -> None:
        status = payload.get("status")
        error = payload.get("error")
        if is_provider_failure(status, error):
            raise RuntimeError(f"aqora provider job {self.job_id!r} failed: {error}")
        if status == "CANCELLED":
            raise RuntimeError(f"aqora provider job {self.job_id!r} was cancelled")
