from dataclasses import dataclass, field
from typing import List, Set, Tuple

@dataclass
class Node:
    id: str
    join_keys: frozenset[str]
    local_preds: List[Tuple[str, str, object]]  # (col, op, val)
    pred_origins: Set[str] = field(default_factory=set)

@dataclass
class Edge:
    src: str
    dst: str
    prune: bool | None = None

@dataclass
class Graph:
    nodes: dict[str, Node]
    edges: List[Edge]  # irányított