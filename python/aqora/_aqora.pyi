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
    def __init__(
        self, url: str | None = None, *, allow_insecure_host: bool | None = None
    ) -> None: ...
    async def authenticate(self) -> None: ...
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
