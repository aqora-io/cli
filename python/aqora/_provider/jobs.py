from __future__ import annotations

import time
from typing import Any, Mapping

from aqora import Client

from . import wire
from .client import AqoraGraphQLClient
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


def normalize_shots(shots: Any) -> int | None:
    """Coerce a user-supplied shot count into a positive int, or None.

    The provider API takes a single optional `shots` value shared by every
    circuit in a job. Rejects bools and non-integral values; requires `>= 1`.
    """
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
    if as_int < 1:
        raise ValueError("`shots` must be at least 1")
    return as_int


def _errored_result(index: int, error: str) -> ProviderResult:
    # An errored item carries no serialization; callers must check `.error`
    # before reading `serialization_format`/`raw`.
    return ProviderResult(index=index, serialization_format=-1, raw="", error=error)


def submit_model(
    graphql: AqoraGraphQLClient,
    payload: str,
    *,
    shots: int | None = None,
    platform: str | None = None,
) -> "ProviderJob":
    graphql.ensure_authenticated()
    upload_info = graphql.start_provider_model_upload()
    etag = graphql.upload_payload(upload_info["uploadUrl"], payload)
    model = graphql.create_provider_model(
        provider_model_upload_id=upload_info["providerModelUploadId"],
        etag=etag,
    )
    job = graphql.create_provider_job(
        provider_model_id=model["id"],
        shots=shots,
        provider_platform=platform,
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
