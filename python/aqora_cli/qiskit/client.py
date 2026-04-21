from __future__ import annotations

import asyncio
import importlib.metadata
import json
import threading
import urllib.parse
import urllib.request
from typing import Any, Awaitable, Callable, Mapping

from aqora_cli import Client

PROVIDER_PLATFORMS_QUERY = """
query ProviderPlatforms($first: Int, $after: String) {
  providerPlatforms(first: $first, after: $after) {
    pageInfo {
      hasNextPage
      endCursor
    }
    nodes {
      provider
      name
      id
      vendor
      backend
      maxQubits
      maxShots
      maxCircuits
      technology
      isQpu
      status
    }
  }
}
"""

UPLOAD_PROVIDER_MODEL_PAYLOAD_MUTATION = """
mutation UploadProviderModelPayload {
  uploadProviderModelPayload {
    providerModelUploadId
    uploadUrl
  }
}
"""

CREATE_PROVIDER_MODEL_MUTATION = """
mutation CreateProviderModel(
  $providerModelUploadId: UUID!,
  $etag: String!,
  $provider: String!,
) {
  createProviderModel(
    providerModelUploadId: $providerModelUploadId,
    etag: $etag,
    provider: $provider,
  ) {
    id
    provider
  }
}
"""

CREATE_PROVIDER_JOB_MUTATION = """
mutation CreateProviderJob(
  $providerModelId: UUID!,
  $provider: String!,
  $platformId: String!,
  $shots: Int,
) {
  createProviderJob(
    providerModelId: $providerModelId,
    provider: $provider,
    platformId: $platformId,
    shots: $shots,
  ) {
    id
    provider
    status
    error
    resultCount
    createdAt
  }
}
"""

PROVIDER_JOB_QUERY = """
query ProviderJob($id: ID!) {
  node(id: $id) {
    __typename
    ... on ProviderJob {
      id
      provider
      status
      error
      duration
      resultCount
      createdAt
    }
  }
}
"""

PROVIDER_JOB_RESULTS_QUERY = """
query ProviderJobResults($id: ID!, $first: Int, $after: String) {
  node(id: $id) {
    __typename
    ... on ProviderJob {
      results(first: $first, after: $after) {
        pageInfo {
          hasNextPage
          endCursor
        }
        nodes {
          index
          error
          result
        }
      }
    }
  }
}
"""


def _run_sync(factory: Callable[[], Awaitable[Any]]) -> Any:
    async def run_factory() -> Any:
        return await factory()

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(run_factory())

    result: dict[str, Any] = {}
    error: dict[str, BaseException] = {}

    def runner() -> None:
        try:
            result["value"] = asyncio.run(run_factory())
        except BaseException as exc:  # pragma: no cover - thread bridge
            error["value"] = exc

    thread = threading.Thread(target=runner, daemon=True)
    thread.start()
    thread.join()

    if "value" in error:
        raise error["value"]
    return result.get("value")


def _package_version() -> str:
    try:
        return importlib.metadata.version("aqora-cli")
    except importlib.metadata.PackageNotFoundError:
        return "unknown"


def _json_options(options: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not options:
        return None
    try:
        return json.loads(json.dumps(dict(options)))
    except TypeError as exc:
        raise TypeError("Backend options must be JSON-serializable") from exc


def _require_http_scheme(url: str) -> None:
    scheme = urllib.parse.urlparse(url).scheme.lower()
    if scheme not in ("https", "http"):
        raise ValueError(f"URL must use http or https scheme: {url!r}")


def _put_json_payload(upload_url: str, payload: str) -> str:
    _require_http_scheme(upload_url)
    request = urllib.request.Request(
        upload_url,
        data=payload.encode("utf-8"),
        method="PUT",
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request) as response:  # noqa: S310
        etag = response.headers.get("ETag") or response.headers.get("etag")
    if not etag:
        raise RuntimeError("Provider model upload completed without returning an ETag")
    return etag


def _download_text(url: str) -> str:
    _require_http_scheme(url)
    with urllib.request.urlopen(url) as response:  # noqa: S310
        return response.read().decode("utf-8")


class AqoraGraphQLClient:
    def __init__(self, client: Client) -> None:
        self._client = client

    @property
    def client(self) -> Client:
        return self._client

    def ensure_authenticated(self) -> None:
        if self._client.authenticated:
            return
        _run_sync(self._client.authenticate)

    def list_provider_platforms(self, *, page_size: int = 100) -> list[Mapping[str, Any]]:
        after: str | None = None
        items: list[Mapping[str, Any]] = []

        while True:
            response = _run_sync(
                lambda: self._client.send(
                    PROVIDER_PLATFORMS_QUERY,
                    first=page_size,
                    after=after,
                )
            )
            connection = response["providerPlatforms"]
            items.extend(connection.get("nodes", []))
            page_info = connection["pageInfo"]
            if not page_info["hasNextPage"]:
                return items
            after = page_info["endCursor"]

    def start_provider_model_upload(self) -> Mapping[str, Any]:
        response = _run_sync(lambda: self._client.send(UPLOAD_PROVIDER_MODEL_PAYLOAD_MUTATION))
        return response["uploadProviderModelPayload"]

    def create_provider_model(
        self,
        *,
        provider_model_upload_id: str,
        etag: str,
        provider: str,
    ) -> Mapping[str, Any]:
        response = _run_sync(
            lambda: self._client.send(
                CREATE_PROVIDER_MODEL_MUTATION,
                providerModelUploadId=provider_model_upload_id,
                etag=etag,
                provider=provider,
            )
        )
        return response["createProviderModel"]

    def create_provider_job(
        self,
        *,
        provider_model_id: str,
        provider: str,
        platform_id: str,
        shots: int | None,
    ) -> Mapping[str, Any]:
        response = _run_sync(
            lambda: self._client.send(
                CREATE_PROVIDER_JOB_MUTATION,
                providerModelId=provider_model_id,
                provider=provider,
                platformId=platform_id,
                shots=shots,
            )
        )
        return response["createProviderJob"]

    def get_provider_job(self, job_id: str) -> Mapping[str, Any]:
        node = _run_sync(lambda: self._client.send(PROVIDER_JOB_QUERY, id=job_id))["node"]
        if not node or node.get("__typename") != "ProviderJob":
            raise LookupError(f"Provider job {job_id!r} was not found")
        return node

    def get_provider_job_results(
        self,
        job_id: str,
        *,
        page_size: int = 100,
    ) -> list[Mapping[str, Any]]:
        after: str | None = None
        items: list[Mapping[str, Any]] = []

        while True:
            node = _run_sync(
                lambda: self._client.send(
                    PROVIDER_JOB_RESULTS_QUERY,
                    id=job_id,
                    first=page_size,
                    after=after,
                )
            )["node"]
            if not node or node.get("__typename") != "ProviderJob":
                raise LookupError(f"Provider job {job_id!r} was not found")

            results = node["results"]
            items.extend(results.get("nodes", []))
            page_info = results["pageInfo"]
            if not page_info["hasNextPage"]:
                return items
            after = page_info["endCursor"]
