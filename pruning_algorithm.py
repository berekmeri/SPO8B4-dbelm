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

def topo_sort_dfs(nodes: dict[str, "Node"], edges: List["Edge"]) -> List["Edge"]:
    # szomszédsági lista (csúcs -> kijövő élek)
    adj: dict[str, List["Edge"]] = {nid: [] for nid in nodes}
    for e in edges:
        adj[e.src].append(e)

    visited: set[str] = set()
    temp_mark: set[str] = set()  
    node_order: List[str] = []

    def dfs(u: str) -> None:
        if u in temp_mark:
            raise ValueError("Ciklust találtam, nem DAG.")
        if u in visited:
            return
        temp_mark.add(u)
        for e in adj[u]:
            dfs(e.dst)
        temp_mark.remove(u)
        visited.add(u)
        node_order.append(u)

    # indítsuk el a DFS-t minden komponensre
    for nid in nodes:
        if nid not in visited:
            dfs(nid)

    # DFS után node_order fordítottja a topologikus sorrend
    node_order.reverse()

    # él-sorrend előállítása a csúcssorrend alapján
    pos = {nid: i for i, nid in enumerate(node_order)}  # csúcs -> pozíció
    ordered_edges = sorted(edges, key=lambda e: (pos[e.src], pos[e.dst]))

    return ordered_edges

def prune_transfers(g: Graph) -> None:
    # init
    for v in g.nodes.values():
        v.pred_origins = set()
        if has_local_pred(v):
            v.pred_origins.add(LOCAL)

    edges_forward = topo_sort_dfs(g.nodes, g.edges)
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
    edges_backward = topo_sort_dfs(g.nodes, edges_rev)
    iterate(edges_backward)
    



# --- Vonalas lánc (A -> B -> C)
nodes_line = {
    "A": Node(id="A", join_keys=frozenset({"a_id"}), local_preds=[("a_id","IN",{10,11,12})]),
    "B": Node(id="B", join_keys=frozenset({"a_id","b_id"}), local_preds=[]),
    "C": Node(id="C", join_keys=frozenset({"b_id"}), local_preds=[("b_id","=",7)]),
}
edges_line = [Edge("A","B"), Edge("B","C")]
g_line = Graph(nodes=nodes_line, edges=edges_line)

# --- Gyémánt (A -> B, A -> C, B -> D, C -> D)
nodes_diamond = {
    "A": Node(id="A", join_keys=frozenset({"a_id"}), local_preds=[("a_id","IN",{1,2,3,4})]),
    "B": Node(id="B", join_keys=frozenset({"a_id","b_id"}), local_preds=[("b_id",">",1000)]),
    "C": Node(id="C", join_keys=frozenset({"a_id","c_id"}), local_preds=[]),
    "D": Node(id="D", join_keys=frozenset({"b_id","c_id"}), local_preds=[]),
}
edges_diamond = [Edge("A","B"), Edge("A","C"), Edge("B","D"), Edge("C","D")]
g_diamond = Graph(nodes=nodes_diamond, edges=edges_diamond)

# --- Vegyes (A -> B -> D és A -> C -> D, + B -> C)
nodes_mix = {
    "A": Node(id="A", join_keys=frozenset({"a_id"}), local_preds=[("a_id","=",42)]),
    "B": Node(id="B", join_keys=frozenset({"a_id","b_id"}), local_preds=[("b_id","<",500)]),
    "C": Node(id="C", join_keys=frozenset({"a_id","c_id"}), local_preds=[]),
    "D": Node(id="D", join_keys=frozenset({"b_id","c_id"}), local_preds=[("c_id","IN",{7,8})]),
}
edges_mix = [Edge("A","B"), Edge("B","D"), Edge("A","C"), Edge("C","D"), Edge("B","C")]
g_mix = Graph(nodes=nodes_mix, edges=edges_mix)

# Futtatás:
prune_transfers(g_line);  print([(e.src,e.dst,e.prune) for e in g_line.edges])
prune_transfers(g_diamond);print([(e.src,e.dst,e.prune) for e in g_diamond.edges])
prune_transfers(g_mix);    print([(e.src,e.dst,e.prune) for e in g_mix.edges])
