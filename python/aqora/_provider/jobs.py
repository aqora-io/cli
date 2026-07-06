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

    def results(self) -> list[ProviderResult]:
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
            if item.get("error"):
                raise RuntimeError(
                    f"Provider job result {item['index']} failed: {item['error']}"
                )
            result_url = item.get("result")
            if not result_url:
                raise RuntimeError(
                    f"Provider job result {item['index']} is missing a result URL"
                )
            serialization_format, raw = wire.parse_result_payload(
                self._graphql.download_text(result_url)
            )
            results.append(
                ProviderResult(
                    index=int(item["index"]),
                    serialization_format=serialization_format,
                    raw=raw,
                )
            )
        results.sort(key=lambda result: result.index)
        return results

    def _raise_on_failure(self, payload: Mapping[str, Any]) -> None:
        # The server mirrors the provider's progress message into `error` for
        # live jobs ("The job is queued.") and nulls `status` when the
        # provider reports an error state; only that combination is a failure.
        status = payload.get("status")
        error = payload.get("error")
        if status is None and error:
            raise RuntimeError(f"aqora provider job {self.job_id!r} failed: {error}")
        if status == "CANCELLED":
            raise RuntimeError(f"aqora provider job {self.job_id!r} was cancelled")
