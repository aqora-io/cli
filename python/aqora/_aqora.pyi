# pyright: reportExplicitAny=false, reportAny=false

from pathlib import Path
from typing_extensions import Any, Never

class PipelineConfig:
    data: Path

class LayerEvaluation:
    transform: Any
    context: Any
    metric: Any | None
    branch: Any | None

def main() -> Never: ...

class ClientError(Exception): ...

class Client:
    authenticated: bool
    def __init__(
        self, url: str | None = None, *, allow_insecure_host: bool | None = None
    ) -> None: ...
    async def authenticate(self) -> None: ...
    async def send(self, query: str, **variables: Any) -> Any: ...
    async def s3_get(
        self, url: str, *, range: tuple[int | None, int | None] | None = None
    ) -> bytes: ...
    async def s3_put(self, url: str, body: bytes) -> str: ...
    async def _download_workspace_notebook(
        self,
        owner: str,
        slug: str,
        dest_dir: str | Path,
        notebook: str | None = None,
        force: bool = False,
    ) -> str: ...
