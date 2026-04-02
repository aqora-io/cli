from __future__ import annotations

import asyncio
import concurrent.futures
import importlib.metadata
import threading
import urllib.parse
from typing import Any, Awaitable, Callable, Mapping

from aqora import Client

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
) {
  createProviderModel(
    providerModelUploadId: $providerModelUploadId,
    etag: $etag,
  ) {
    id
  }
}
"""

CREATE_PROVIDER_JOB_MUTATION = """
mutation CreateProviderJob(
  $providerModelId: UUID!,
  $shots: Int,
  $providerPlatform: ProviderPlatformNameOrID,
) {
  createProviderJob(
    providerModelId: $providerModelId,
    shots: $shots,
    providerPlatform: $providerPlatform,
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

PROVIDER_PLATFORMS_QUERY = """
query ProviderPlatforms($first: Int, $after: String) {
  providerPlatforms(first: $first, after: $after) {
    pageInfo {
      hasNextPage
      endCursor
    }
    nodes {
      id
      name
      meta {
        maxQubits
        maxShots
        maxCircuits
      }
    }
  }
}
"""

_loop_lock = threading.Lock()
_loop: asyncio.AbstractEventLoop | None = None


def _background_loop() -> asyncio.AbstractEventLoop:
    """Lazily start a single event loop thread shared by all sync bridges.

    The aqora Client only exposes async methods; dispatching them to one
    persistent loop avoids spawning a thread and event loop per call.
    """
    global _loop
    with _loop_lock:
        if _loop is None or _loop.is_closed():
            loop = asyncio.new_event_loop()
            threading.Thread(
                target=loop.run_forever,
                name="aqora-qiskit-client",
                daemon=True,
            ).start()
            _loop = loop
        return _loop


# Transport-level guard: the underlying client retries but sets no HTTP
# timeout, so without this a hung connection would block callers forever
# (including `QPUJob.result(timeout=...)`, which only checks its deadline
# between status polls).
DEFAULT_SYNC_TIMEOUT = 300.0


def _run_sync(
    factory: Callable[[], Awaitable[Any]],
    *,
    timeout: float | None = DEFAULT_SYNC_TIMEOUT,
) -> Any:
    async def run_factory() -> Any:
        return await factory()

    future = asyncio.run_coroutine_threadsafe(run_factory(), _background_loop())
    try:
        return future.result(timeout=timeout)
    except concurrent.futures.TimeoutError:
        future.cancel()
        raise TimeoutError(
            f"aqora client call did not complete within {timeout} seconds"
        ) from None


def _package_version() -> str:
    try:
        return importlib.metadata.version("aqora")
    except importlib.metadata.PackageNotFoundError:
        return "unknown"


def _require_http_scheme(url: str) -> None:
    scheme = urllib.parse.urlparse(url).scheme.lower()
    if scheme not in ("https", "http"):
        raise ValueError(f"URL must use http or https scheme: {url!r}")


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

    def upload_payload(self, upload_url: str, payload: str) -> str:
        _require_http_scheme(upload_url)
        body = payload.encode("utf-8")
        # The server presigns the upload URL with `Content-Type: application/json`
        # in the signature, so the PUT must send the same header.
        return _run_sync(
            lambda: self._client.s3_put(upload_url, body, content_type="application/json")
        )

    def download_text(self, url: str) -> str:
        _require_http_scheme(url)
        return _run_sync(lambda: self._client.s3_get(url)).decode("utf-8")

    def start_provider_model_upload(self) -> Mapping[str, Any]:
        response = _run_sync(lambda: self._client.send(UPLOAD_PROVIDER_MODEL_PAYLOAD_MUTATION))
        return response["uploadProviderModelPayload"]

    def create_provider_model(
        self,
        *,
        provider_model_upload_id: str,
        etag: str,
    ) -> Mapping[str, Any]:
        response = _run_sync(
            lambda: self._client.send(
                CREATE_PROVIDER_MODEL_MUTATION,
                providerModelUploadId=provider_model_upload_id,
                etag=etag,
            )
        )
        return response["createProviderModel"]

    def create_provider_job(
        self,
        *,
        provider_model_id: str,
        shots: int | None,
        provider_platform: str | None = None,
    ) -> Mapping[str, Any]:
        response = _run_sync(
            lambda: self._client.send(
                CREATE_PROVIDER_JOB_MUTATION,
                providerModelId=provider_model_id,
                shots=shots,
                providerPlatform=provider_platform,
            )
        )
        return response["createProviderJob"]

    def get_provider_job(self, job_id: str) -> Mapping[str, Any]:
        # `node(id:)` is non-null in the schema, so an unknown id surfaces as a
        # GraphQL error (aqora.ClientError) from `send` rather than a null node;
        # this guard only catches ids that point at a different node type.
        node = _run_sync(lambda: self._client.send(PROVIDER_JOB_QUERY, id=job_id))["node"]
        if not node or node.get("__typename") != "ProviderJob":
            raise LookupError(f"Node {job_id!r} is not a provider job")
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
                raise LookupError(f"Node {job_id!r} is not a provider job")

            results = node["results"]
            items.extend(results.get("nodes", []))
            page_info = results["pageInfo"]
            if not page_info["hasNextPage"]:
                return items
            cursor = page_info["endCursor"]
            if cursor is None or cursor == after:
                raise RuntimeError(
                    f"Pagination of provider job {job_id!r} results did not advance"
                )
            after = cursor

    def get_provider_platforms(
        self,
        *,
        page_size: int = 100,
    ) -> list[Mapping[str, Any]]:
        after: str | None = None
        platforms: list[Mapping[str, Any]] = []

        while True:
            connection = _run_sync(
                lambda: self._client.send(
                    PROVIDER_PLATFORMS_QUERY,
                    first=page_size,
                    after=after,
                )
            )["providerPlatforms"]
            platforms.extend(connection.get("nodes", []))
            page_info = connection["pageInfo"]
            if not page_info["hasNextPage"]:
                return platforms
            cursor = page_info["endCursor"]
            if cursor is None or cursor == after:
                raise RuntimeError("Pagination of provider platforms did not advance")
            after = cursor
