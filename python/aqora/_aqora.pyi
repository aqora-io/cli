# pyright: reportExplicitAny=false, reportAny=false

import datetime
from pathlib import Path
from typing_extensions import Any, Never, Sequence

class PipelineConfig:
    data: Path

class LayerEvaluation:
    transform: Any
    context: Any
    metric: Any | None
    branch: Any | None

def main() -> Never: ...

QIO_PROGRAM_QIR_V1: int
QIO_PROGRAM_TKET_CIRCUIT_JSON_V1: int
QIO_PROGRAM_HUGR_V1: int
QIO_RESULT_PYTKET_BACKEND_RESULT_JSON_V1: int
QIO_RESULT_QIR_LABELED_RESULT_V1: int
QIO_RESULT_QSYS_RESULT_JSON_V1: int

def qio_build_model_payload(
    programs: list[tuple[str, int]],
    *,
    user_agent: str,
    backend_name: str,
    compress: bool,
) -> str: ...
def qio_parse_result_payload(payload: str) -> tuple[int, str]: ...

class ClientError(Exception): ...

class Client:
    authenticated: bool
    granted_scopes: list[str] | None
    def __init__(
        self, url: str | None = None, *, allow_insecure_host: bool | None = None
    ) -> None: ...
    async def authenticate(self) -> None: ...
    async def store_credentials(
        self, duration: int | None = None
    ) -> dict[str, Any]: ...
    async def send(self, query: str, **variables: Any) -> Any: ...
    async def s3_get(
        self, url: str, *, range: tuple[int | None, int | None] | None = None
    ) -> bytes: ...
    async def s3_put(
        self, url: str, body: bytes, *, content_type: str | None = None
    ) -> str: ...
    async def _download_workspace_notebook(
        self,
        owner: str,
        slug: str,
        dest_dir: str | Path,
        notebook: str | None = None,
        version: str | None = None,
        force: bool = False,
    ) -> str: ...
    def with_token(self, token: str) -> Client: ...
    async def authorize_viewer(
        self, scope: Sequence[str] | None = None
    ) -> ViewerAuthorization | None: ...
    async def viewer_grant_active(self) -> bool: ...

class ViewerAuthorization:
    url: str
    client_id: str
    async def wait(self, *, timeout: float | None = None) -> Client: ...

class StoreCredentials:
    access_key_id: str
    secret_access_key: str
    expires_at: datetime.datetime
    endpoint: str
    bucket: str
    region: str
    def __init__(
        self,
        *,
        access_key_id: str,
        secret_access_key: str,
        expires_at: datetime.datetime,
        endpoint: str,
        bucket: str,
        region: str,
    ) -> None: ...
    @property
    def use_ssl(self) -> bool: ...
    @property
    def host(self) -> str: ...
    def url(self, key: str = "") -> str: ...
    def remaining(self, now: datetime.datetime | None = None) -> float: ...
    def duckdb_sql(self, name: str) -> str: ...

class _Store:
    client: Client
    duration: int | None
    refresh_margin: float
    def __init__(
        self,
        client: Client | None = None,
        *,
        duration: int | None = None,
        refresh_margin: float = 60.0,
        url: str | None = None,
        allow_insecure_host: bool | None = None,
    ) -> None: ...
    async def credentials_async(self, *, force: bool = False) -> StoreCredentials: ...

class _KV:
    def __init__(
        self, path: str, store: _Store | None = None, *, stale_after: float = 5.0
    ) -> None: ...
    def set(self, key: str, value: Any) -> None: ...
    def delete(self, key: str) -> None: ...
    async def get_async(self, key: str) -> Any | None: ...
    async def list_async(self) -> list[str]: ...
    async def flush_async(self) -> None: ...
    async def close_async(self) -> None: ...
