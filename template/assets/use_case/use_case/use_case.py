from types import NoneType
from typing import Dict, AsyncIterator, List, Any
from aqora_cli import PipelineConfig, LayerEvaluation

Input = NoneType
Output = Any
EvaluationResults = Dict[str, List[LayerEvaluation]]


async def generator(_: PipelineConfig) -> AsyncIterator[Input]:
    yield None


async def aggregate(outputs: AsyncIterator[EvaluationResults]) -> float:
    async for _ in outputs:
        pass
    return 0
