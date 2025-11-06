from dataclasses import dataclass, field
from typing import List, Set, Tuple

LOCAL = "__LOCAL__"

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
    
def has_local_pred(v: Node) -> bool:
    return bool(v.local_preds)

def join_key_containment(src: Node, dst: Node) -> bool:
    return dst.join_keys.issubset(src.join_keys)

def estimate_selectivity(preds: List[Tuple[str,str,object]]) -> float:
    if not preds: return 1.0
    s = 1.0
    for col, op, val in preds:
        if op == '=': s *= 0.1
        elif op.lower() == 'in': s *= min(0.2, max(0.02, 0.02*len(val)))
        elif op in ('<','>','<=','>=','between'): s *= 0.5
    return max(0.0, min(1.0, s))

def predicate_containment(src_node: Node, dst_node: Node) -> bool:
    s_src = estimate_selectivity(src_node.local_preds)
    s_dst = estimate_selectivity(dst_node.local_preds)
    return s_dst <= s_src


def prune_transfers(g: Graph) -> None:
    # init
    for v in g.nodes.values():
        v.pred_origins = set()
        if has_local_pred(v):
            v.pred_origins.add(LOCAL)

    edges_forward = topo_sort(g.nodes, g.edges)
    def iterate(edges_order: List[Edge]):
        for e in edges_order:
            e.prune = True
            src, dst = g.nodes[e.src], g.nodes[e.dst]
            if not join_key_containment(src, dst):
                e.prune = False
                dst.pred_origins.add(src.id)  # továbbítsd az eredetet, mert nem prunelünk
                continue
            # Jelöld, hogy src-ből érkezhet predikátum
            dst.pred_origins.add(src.id)
            # Ha JK oké, nézd pred containmentet
            if predicate_containment(src, dst):
                e.prune = True  # fölösleges átvitel
            else:
                e.prune = False
                # gazdagítsd dst eredetét src eredeteivel is
                dst.pred_origins.update(src.pred_origins)

    # forward pass
    iterate(edges_forward)
    # backward pass: élek megfordítva + topo
    edges_rev = [Edge(src=e.dst, dst=e.src) for e in g.edges]
    edges_backward = topo_sort(g.nodes, edges_rev)
    iterate(edges_backward)
    
    
# Input: Predicate Transfer Graph 𝐺 = (𝑉 , 𝐸)
# 1 forward ← true
# 2 foreach v in V do
# 3 v.predOrigins ← ∅
# 4 if HasLocalPred(v) then
# 5 v.predOrigins.AddLocal()
# 6 Esort ← TopologicalSort(E)
# 7 foreach e = (src, dst) in Esort do
# 8 e.prune ← true
# 9 if not JoinKeyContainment(src, dst) then
# 10 e.prune ← false
# 11 dst.predOrigins.AddTransferred(src)
# 12 if e.prune then
# 13 e.prune ← PredicateContainment(src.predOrigins, dst.predOrigins)
# 14 if not 𝑒.prune then
# 15 dst.predOrigins.AddTransferred(src.predOrigins)
# 16 if forward then
# 17 Erev ← Reverse(E)
# 18 Esort ← TopologicalSort(Erev)
# 19 forward ← false
# 20 goto line 7
