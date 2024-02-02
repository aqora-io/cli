from pyzx.graph.base import BaseGraph
import pyzx as zx


async def reduce(input: BaseGraph) -> BaseGraph:
    zx.full_reduce(input)
    return input


def test() -> str:
    return "Hello, World!"
