import random
from typing import AsyncIterator, List, Dict, Any
from pyzx.graph.base import BaseGraph
import pyzx as zx
from pathlib import Path


class Result:
    transform: BaseGraph
    metric: float


def init_random(config: Dict[str, str]) -> Any:
    seed_path = Path(config["data"]) / "seed"
    with open(seed_path, "r") as f:
        seed = int(f.read())
    random.seed(seed)
    return random.getstate()


async def cases(config: Dict[str, str]) -> AsyncIterator[BaseGraph]:
    random_state = init_random(config)
    for _ in range(10):
        random.setstate(random_state)
        generated = zx.generate.cliffordT(5, 5)
        random_state = random.getstate()
        yield generated


async def metric(transformed: BaseGraph, context: BaseGraph) -> float:
    input_circuit = zx.Circuit.from_graph(context)
    output_circuit = zx.extract_circuit(transformed)
    if not input_circuit.verify_equality(output_circuit):
        raise Exception("Circuits are not equal")
    return (
        context.num_vertices() - transformed.num_vertices()
    ) / context.num_vertices()


async def aggregate(outputs: AsyncIterator[Dict[str, List[Dict[str, Any]]]]) -> float:
    total = 0
    count = 0
    async for output in outputs:
        total += output["reduce"][0]["metric"]
        count += 1
    return total / count
