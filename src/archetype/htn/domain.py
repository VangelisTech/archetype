# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
HTN Domain
==========

The STATIC planning domain: STRIPS ``OperatorSpec`` (primitive tasks) and
``MethodSpec`` (ways to decompose a compound task), grouped into an ``HtnDomain``.

This module is consulted ONLY by the driver, in plain Python, to BUILD the per-node
``op`` / ``candidate_methods`` JSON that gets embedded into ``Branch.network_json`` at
spawn time (Inv HTN-V2.15). The domain object NEVER enters a ``@daft.func`` — every
UDF reads only the pre-denormalized JSON columns. None of these helpers touch Daft or
force eager materialization, so the module is lazy-audit clean and lives in ``src/``.

Parameterization is light and positional: operator/method ``params`` bind to a node's
``args`` and substitute into ``{name}`` placeholders in precondition / effect / subtask
templates via ``str.format``. Propositional domains simply use empty ``params`` and
literal templates.
"""

from __future__ import annotations

from dataclasses import dataclass, field


def _subst(templates: list[str], binding: dict[str, str]) -> list[str]:
    """Substitute a positional binding into ``{var}`` placeholders.

    With an empty binding the templates pass through verbatim (propositional case).
    """
    if not binding:
        return list(templates)
    return [t.format(**binding) for t in templates]


@dataclass(frozen=True)
class OperatorSpec:
    """A primitive task: STRIPS ``(pre+, pre-, add, del)`` over ground atoms.

    ``applicable(a, s) <=> pre+ ⊆ s ∧ pre- ∩ s = ∅``; the transition is
    ``gamma(s, a) = (s \\ del) ∪ add`` with delete strictly before add.
    """

    name: str
    params: tuple[str, ...] = ()
    pre_pos: tuple[str, ...] = ()
    pre_neg: tuple[str, ...] = ()
    add: tuple[str, ...] = ()
    delete: tuple[str, ...] = ()

    def ground(self, args: list[str]) -> dict:
        """Bind ``args`` to ``params`` and substitute, yielding a ground op-spec dict
        embedded per-node by the driver (read later by the pure UDFs)."""
        binding = dict(zip(self.params, args, strict=False))
        return {
            "pre_pos": _subst(list(self.pre_pos), binding),
            "pre_neg": _subst(list(self.pre_neg), binding),
            "add": _subst(list(self.add), binding),
            "del": _subst(list(self.delete), binding),
        }


@dataclass(frozen=True)
class MethodSpec:
    """A way to decompose a compound task: ``(task, pre+, pre-, subtasks)``.

    ``subtasks`` is an ORDERED list of ``(name, arg_templates)`` — total-order
    decomposition (each subtask depends on the previous). The method applies when its
    precondition holds in the branch's CURRENT state.
    """

    task: str
    method_id: str
    params: tuple[str, ...] = ()
    pre_pos: tuple[str, ...] = ()
    pre_neg: tuple[str, ...] = ()
    subtasks: tuple[tuple[str, tuple[str, ...]], ...] = ()

    def precond(self, args: list[str]) -> dict:
        """Ground precondition for the ``candidate_methods`` filter UDF."""
        binding = dict(zip(self.params, args, strict=False))
        return {
            "id": self.method_id,
            "pre_pos": _subst(list(self.pre_pos), binding),
            "pre_neg": _subst(list(self.pre_neg), binding),
        }

    def ground_subtasks(self, args: list[str]) -> list[tuple[str, list[str]]]:
        binding = dict(zip(self.params, args, strict=False))
        return [(name, _subst(list(arg_t), binding)) for name, arg_t in self.subtasks]


@dataclass
class HtnDomain:
    """Operators + methods. The driver's only source of decomposition knowledge."""

    operators: dict[str, OperatorSpec] = field(default_factory=dict)
    methods: dict[str, list[MethodSpec]] = field(default_factory=dict)
    _method_index: dict[str, MethodSpec] = field(default_factory=dict, init=False, repr=False)

    def __post_init__(self) -> None:
        for ms in self.methods.values():
            for m in ms:
                self._method_index[m.method_id] = m

    @classmethod
    def of(cls, operators: list[OperatorSpec], methods: list[MethodSpec]) -> HtnDomain:
        ops = {o.name: o for o in operators}
        meth: dict[str, list[MethodSpec]] = {}
        for m in methods:
            meth.setdefault(m.task, []).append(m)
        return cls(operators=ops, methods=meth)

    def is_primitive(self, name: str) -> bool:
        return name in self.operators

    def method_by_id(self, method_id: str) -> MethodSpec:
        return self._method_index[method_id]

    # ── per-node embedding ────────────────────────────────────────────────

    def embed_node(
        self,
        node_id: int,
        name: str,
        args: list[str],
        *,
        preds: list[int],
        order_index: int,
        depth: int,
    ) -> dict:
        """Build a network node with its op-spec OR candidate-method preconditions
        embedded, so the pure UDFs never need the domain object."""
        if self.is_primitive(name):
            kind = "primitive"
            op = self.operators[name].ground(args)
            candidates: list[dict] = []
        else:
            kind = "compound"
            op = {}
            candidates = [m.precond(args) for m in self.methods.get(name, [])]
        return {
            "node_id": node_id,
            "name": name,
            "args": list(args),
            "kind": kind,
            "preds": list(preds),
            "order_index": order_index,
            "depth": depth,
            "status": "open",
            "chosen_method": "",
            "op": op,
            "candidate_methods": candidates,
        }


# ============================================================================
# Network construction & decomposition (pure Python, driver-side)
# ============================================================================


def _topo_order_index(network: list[dict]) -> None:
    """Assign each node a dense ``order_index`` that is a topological rank over the
    ``preds`` edges (Kahn). Guarantees the min-``order_index`` open-with-preds-resolved
    pick respects the partial order without collisions or gaps (Inv HTN-V2.13)."""
    by_id = {n["node_id"]: n for n in network}
    indeg = {nid: 0 for nid in by_id}
    succ: dict[int, list[int]] = {nid: [] for nid in by_id}
    for n in network:
        for p in n["preds"]:
            if p in by_id:
                indeg[n["node_id"]] += 1
                succ[p].append(n["node_id"])
    # deterministic Kahn: pop smallest node_id among the ready set
    ready = sorted(nid for nid, d in indeg.items() if d == 0)
    rank = 0
    while ready:
        nid = ready.pop(0)
        by_id[nid]["order_index"] = rank
        rank += 1
        for s in sorted(succ[nid]):
            indeg[s] -= 1
            if indeg[s] == 0:
                ready.append(s)
        ready.sort()
    # any node left (cycle) keeps a large index so it never blocks live picks
    for n in network:
        if n["node_id"] not in by_id or indeg.get(n["node_id"], 0) > 0:
            n["order_index"] = 10_000 + n["node_id"]


def build_initial_network(domain: HtnDomain, tn0: list[tuple[str, list[str]]]) -> list[dict]:
    """Build the initial task network from ``tn0`` — an ordered list of top-level
    ``(task_name, args)``. Tasks are chained total-order (node i depends on i-1)."""
    network: list[dict] = []
    prev: int | None = None
    for i, (name, args) in enumerate(tn0):
        preds = [prev] if prev is not None else []
        network.append(domain.embed_node(i, name, args, preds=preds, order_index=i, depth=0))
        prev = i
    _topo_order_index(network)
    return network


def splice_method(
    domain: HtnDomain,
    network: list[dict],
    compound_node_id: int,
    method_id: str,
) -> list[dict]:
    """Return a NEW network (deep value-copy) for a child branch, decomposing the
    compound node by ``method_id`` (Inv HTN-V2.13):

      1. instantiate the method's subtasks with fresh node_ids, args bound, op /
         candidate_methods embedded, chained total-order;
      2. inherit the compound node's preds onto the method's INITIAL subtask;
      3. rewire every successor of the compound (a node whose preds contained it) to
         depend instead on the method's TERMINAL subtask;
      4. mark the compound node ``resolved`` with ``chosen_method``;
      5. densely renumber ``order_index`` as a topological sort.
    """
    import copy

    net = copy.deepcopy(network)
    by_id = {n["node_id"]: n for n in net}
    compound = by_id[compound_node_id]
    method = domain.method_by_id(method_id)
    next_id = max(n["node_id"] for n in net) + 1
    child_depth = compound["depth"] + 1

    subtask_specs = method.ground_subtasks(compound["args"])
    sub_ids: list[int] = []
    for k, (name, args) in enumerate(subtask_specs):
        nid = next_id
        next_id += 1
        # (2) initial subtask inherits the compound's preds; later subtasks chain.
        preds = list(compound["preds"]) if k == 0 else [sub_ids[-1]]
        node = domain.embed_node(nid, name, args, preds=preds, order_index=0, depth=child_depth)
        net.append(node)
        sub_ids.append(nid)

    # (3) rewire successors of the compound to the method's terminal subtask. If the
    # method is empty (no subtasks), successors fall back onto the compound's own preds.
    terminal = sub_ids[-1] if sub_ids else None
    replacement = [terminal] if terminal is not None else list(compound["preds"])
    for n in net:
        if n["node_id"] == compound_node_id:
            continue
        if compound_node_id in n["preds"]:
            rewired = [p for p in n["preds"] if p != compound_node_id] + replacement
            # dedup preserving determinism
            n["preds"] = sorted(set(rewired))

    # (4) retire the compound node within the child network.
    compound["status"] = "resolved"
    compound["chosen_method"] = method_id

    # (5) topological renumber over the whole spliced network.
    _topo_order_index(net)
    return net
