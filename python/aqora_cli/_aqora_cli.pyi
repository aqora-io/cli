# pyright: reportExplicitAny=false, reportAny=false

from pathlib import Path
from typing import Any, Never

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
    def __init__(self, url: str | None = None) -> None: ...
    async def authenticate(self) -> None: ...
    async def send(self, query: str, **variables: Any) -> Any: ...
    async def s3_get(
        self, url: str, *, range: tuple[int | None, int | None] | None = None
    ) -> bytes: ...
