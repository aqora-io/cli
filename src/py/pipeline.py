from copy import deepcopy
from typing import (
    AsyncIterator,
    Optional,
    Tuple,
    TypeVar,
    Generic,
    Callable,
    Awaitable,
    List,
)
import asyncio

I = TypeVar("I")
O = TypeVar("O")
M = TypeVar("M")
S = TypeVar("S")


Result = Tuple[I, O, Optional[M]]
EvaluateFn = Callable[[I], Awaitable[O]]
MetricFn = Optional[Callable[[I, O], Awaitable[Optional[M]]]]


class Layer(Generic[I, O, M]):
    evaluate: EvaluateFn[I, O]
    metric: MetricFn[I, O, M]

    def __init__(
        self,
        evaluate: EvaluateFn,
        metric: MetricFn = None,
    ):
        self.evaluate = evaluate
        self.metric = metric

    async def pipe(self, input: I) -> Result[I, O, M]:
        output = await self.evaluate(deepcopy(input))
        metric = None
        if self.metric is not None:
            metric = await self.metric(deepcopy(input), deepcopy(output))
        return (input, output, metric)


AggregateFn = Callable[[AsyncIterator[List[Result[I, O, M]]]], Awaitable[S]]


class Pipeline(Generic[I, O, M, S]):
    generator: AsyncIterator[I]
    layers: List[Layer[I, O, M]]
    aggregate: AggregateFn[I, O, M, S]

    def __init__(
        self,
        generator: AsyncIterator[I],
        layers: List[Layer[I, O, M]],
        aggregate: AggregateFn,
    ):
        self.generator = generator
        self.layers = layers
        self.aggregate = aggregate

    async def evaluate(self, input: I) -> List[Result[I, O, M]]:
        return [await layer.pipe(input) for layer in self.layers]

    async def results(self) -> AsyncIterator[List[Result[I, O, M]]]:
        for task in asyncio.as_completed(
            [self.evaluate(input) async for input in self.generator]
        ):
            yield await task

    async def run(self) -> S:
        return await self.aggregate(self.results())
