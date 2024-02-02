import random
from typing import AsyncIterator, List, Optional, Tuple
from pyzx.graph.base import BaseGraph
import pyzx as zx


async def cases() -> AsyncIterator[BaseGraph]:
    random.seed(123)
    state = random.getstate()
    for _ in range(10):
        random.setstate(state)
        generated = zx.generate.cliffordT(5, 5)
        state = random.getstate()
        yield generated


async def metric(input: BaseGraph, output: BaseGraph) -> float:
    print(input)
    print(output)
    input_circuit = zx.Circuit.from_graph(input)
    output_circuit = zx.extract_circuit(output)
    if not input_circuit.verify_equality(output_circuit):
        raise Exception("Circuits are not equal")
    return (input.num_vertices() - output.num_vertices()) / input.num_vertices()


async def aggregate(
    outputs: AsyncIterator[List[Tuple[BaseGraph, BaseGraph, Optional[float]]]]
) -> float:
    total: float = 0
    count = 0
    async for output in outputs:
        _, _, metric = output[0]
        if metric is not None:
            total += metric
            count += 1
    return total / count
