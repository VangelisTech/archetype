# Copyright 2025 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
HTN UDFs
========

Pure, total ``@daft.func`` row transforms over the ``Branch`` JSON columns. None
closes over any non-serializable state — the operator / method spec each needs is
ALREADY embedded per-node in ``network_json`` by the driver at spawn time (Inv
HTN-V2.15). No UDF raises on any input, so ``when(...)`` gating selects the right
branch without partial-failure (a flag column tells effectful UDFs whether to act).

Daft 0.7.x notes:
  * struct field access is ``col("s")["field"]`` getitem — ``.struct.get()`` was
    removed in 0.7.0 (LEARNINGS.md:173);
  * a ``@daft.func`` returning a dict with ``return_dtype=DataType.struct({...})``
    yields a struct column.
"""

from __future__ import annotations

import hashlib
import json

import daft
from daft import DataType

# Struct returned by ``pick_ready`` — must be a struct (not a JSON string), because the
# FrontierProcessor reads its fields with ``col(...)[k]`` getitem.
READY_DTYPE = DataType.struct(
    {
        "ready_node_id": DataType.int64(),
        "ready_kind": DataType.string(),
        "ready_op_name": DataType.string(),
        "ready_args_json": DataType.string(),
        "pre_pos_json": DataType.string(),
        "pre_neg_json": DataType.string(),
        "add_json": DataType.string(),
        "del_json": DataType.string(),
        "candidate_methods_json": DataType.string(),
    }
)

_EMPTY_READY = {
    "ready_node_id": -1,
    "ready_kind": "none",
    "ready_op_name": "",
    "ready_args_json": "[]",
    "pre_pos_json": "[]",
    "pre_neg_json": "[]",
    "add_json": "[]",
    "del_json": "[]",
    "candidate_methods_json": "[]",
}


@daft.func(return_dtype=READY_DTYPE)
def pick_ready(network_json: str) -> dict:
    """Pick the min-``order_index`` OPEN node whose ``preds`` are all resolved, and
    denormalize its op / candidate-method spec. The spec is already embedded per-node
    (Inv HTN-V2.15) — NO domain object here."""
    net = json.loads(network_json)
    resolved = {n["node_id"] for n in net if n["status"] == "resolved"}
    ready = [n for n in net if n["status"] == "open" and set(n.get("preds", [])) <= resolved]
    if not ready:
        return dict(_EMPTY_READY)
    n = min(ready, key=lambda x: x["order_index"])
    op = n.get("op") or {}
    return {
        "ready_node_id": n["node_id"],
        "ready_kind": n["kind"],
        "ready_op_name": n["name"],
        "ready_args_json": json.dumps(n.get("args", [])),
        "pre_pos_json": json.dumps(op.get("pre_pos", [])),
        "pre_neg_json": json.dumps(op.get("pre_neg", [])),
        "add_json": json.dumps(op.get("add", [])),
        "del_json": json.dumps(op.get("del", [])),
        "candidate_methods_json": json.dumps(n.get("candidate_methods", [])),
    }


@daft.func(return_dtype=DataType.bool())
def precond_holds(atoms_json: str, pre_pos_json: str, pre_neg_json: str) -> bool:
    """STRIPS applicability: ``pre+ ⊆ atoms ∧ pre- ∩ atoms = ∅``."""
    atoms = set(json.loads(atoms_json))
    pre_pos = set(json.loads(pre_pos_json))
    pre_neg = set(json.loads(pre_neg_json))
    return pre_pos <= atoms and not (pre_neg & atoms)


@daft.func(return_dtype=DataType.string())
def methods_applicable(atoms_json: str, candidate_methods_json: str) -> str:
    """Filter the embedded candidate methods to those applicable in CURRENT atoms.
    Returns the COMPLETE applicable set as a JSON list (Inv HTN-V2.7)."""
    atoms = set(json.loads(atoms_json))
    out = [
        m
        for m in json.loads(candidate_methods_json)
        if set(m.get("pre_pos", [])) <= atoms and not (set(m.get("pre_neg", [])) & atoms)
    ]
    return json.dumps(out)


@daft.func(return_dtype=DataType.bool())
def json_list_nonempty(json_list: str) -> bool:
    return len(json.loads(json_list)) > 0


@daft.func(return_dtype=DataType.string())
def state_sig(atoms_json: str) -> str:
    """Hash of the (already-sorted) ground-atom state. Recorded as the executability
    witness's ``pre_state_sig`` BEFORE atoms are mutated (Inv HTN-V2.4 / HTN-V2.4a)."""
    return hashlib.sha256(atoms_json.encode()).hexdigest()[:16]


@daft.func(return_dtype=DataType.string())
def apply_strips(atoms_json: str, add_json: str, del_json: str, apply: bool) -> str:
    """``gamma(s, a) = (s \\ del) ∪ add`` (delete strictly before add) when ``apply``,
    else atoms unchanged. Result is re-sorted for a canonical state encoding."""
    if not apply:
        return atoms_json
    atoms = set(json.loads(atoms_json))
    atoms -= set(json.loads(del_json))
    atoms |= set(json.loads(add_json))
    return json.dumps(sorted(atoms))


@daft.func(return_dtype=DataType.string())
def mark_resolved(network_json: str, node_id: int, apply: bool) -> str:
    """Mark the applied node ``resolved`` in the network when ``apply``, else unchanged."""
    if not apply:
        return network_json
    net = json.loads(network_json)
    for n in net:
        if n["node_id"] == node_id:
            n["status"] = "resolved"
            break
    return json.dumps(net)


@daft.func(return_dtype=DataType.string())
def append_plan(
    plan_json: str,
    seq: int,
    op_name: str,
    args_json: str,
    pre_sig: str,
    node_id: int,
    apply: bool,
) -> str:
    """Append the executability-witnessed plan step at application time (Inv HTN-V2.4)."""
    if not apply:
        return plan_json
    plan = json.loads(plan_json)
    plan.append(
        {
            "seq": seq,
            "op": op_name,
            "args": json.loads(args_json),
            "pre_sig": pre_sig,
            "node_id": node_id,
        }
    )
    return json.dumps(plan)


@daft.func(return_dtype=DataType.int64())
def count_open(network_json: str) -> int:
    """Authoritative post-Effect open-node count, same row (Inv HTN-V2.4b)."""
    return sum(1 for n in json.loads(network_json) if n["status"] == "open")


# The whole guarded STRIPS application, consolidated into ONE struct-returning UDF.
#
# Daft folds a UDF's column arguments to that column's FINAL definition in the plan
# (verified: a separate `state_sig(atoms_json)` written before `atoms_json` is reassigned
# reads the POST-mutation atoms, breaking Inv HTN-V2.4a). Splitting the effect across
# several UDFs that each read `atoms_json`/`plan_json`/`seq_next` and then overwrite them
# therefore corrupts the witness and the seq. Consolidating into one UDF that reads each
# input ONCE and returns every result as a struct field is the only aliasing-proof shape:
# because the output columns are derived FROM this UDF, Daft cannot fold the UDF's own
# inputs to those outputs (that would be a cycle), so it reads the genuine pre-mutation
# values. ``EffectProcessor`` then splits the struct back into the Branch columns.
EFFECT_DTYPE = DataType.struct(
    {
        "atoms_json": DataType.string(),
        "network_json": DataType.string(),
        "plan_json": DataType.string(),
        "pre_state_sig": DataType.string(),
        "seq_next": DataType.int64(),
        "status": DataType.string(),
        "fail_reason": DataType.string(),
        "applicable": DataType.bool(),
    }
)


@daft.func(return_dtype=EFFECT_DTYPE)
def apply_effect(
    atoms_json: str,
    network_json: str,
    plan_json: str,
    seq_next: int,
    status: str,
    fail_reason: str,
    pre_state_sig: str,
    depth: int,
    max_depth: int,
    ready_kind: str,
    ready_node_id: int,
    ready_op_name: str,
    ready_args_json: str,
    pre_pos_json: str,
    pre_neg_json: str,
    add_json: str,
    del_json: str,
) -> dict:
    """Guarded, self-recording STRIPS application for the ready node (Inv HTN-V2.3/2.4).

    Enforces the mandatory depth cap before any mutation, then acts ONLY when
    the branch is live and its ready node is a PRIMITIVE. Re-checks
    applicability against the CURRENT (pre-mutation) atoms in-row — never trusting a
    possibly-stale ``applicable`` column. On success: records the witness ``pre_state_sig``
    from the pre-mutation atoms, appends the plan step (``seq = len(plan)`` — derived, so it
    cannot drift), applies ``gamma`` (delete-then-add), and marks the node resolved. On a
    primitive precondition failure: the branch dies ``precond_unsat`` with atoms untouched.
    Otherwise (compound / no-ready / not-live): a clean pass-through.
    """
    unchanged = {
        "atoms_json": atoms_json,
        "network_json": network_json,
        "plan_json": plan_json,
        "pre_state_sig": pre_state_sig,
        "seq_next": seq_next,
        "status": status,
        "fail_reason": fail_reason,
        "applicable": False,
    }
    if status == "live" and depth > max_depth:
        return {**unchanged, "status": "failed", "fail_reason": "depth_exceeded"}
    if ready_kind != "primitive" or status != "live":
        return unchanged

    atoms = set(json.loads(atoms_json))
    pre_pos = set(json.loads(pre_pos_json))
    pre_neg = set(json.loads(pre_neg_json))
    if not (pre_pos <= atoms and not (pre_neg & atoms)):
        return {**unchanged, "status": "failed", "fail_reason": "precond_unsat"}

    pre_sig = hashlib.sha256(atoms_json.encode()).hexdigest()[:16]
    plan = json.loads(plan_json)
    seq = len(plan)  # derived from the append-only plan — monotone by construction
    plan.append(
        {
            "seq": seq,
            "op": ready_op_name,
            "args": json.loads(ready_args_json),
            "pre_sig": pre_sig,
            "node_id": ready_node_id,
        }
    )
    new_atoms = sorted((atoms - set(json.loads(del_json))) | set(json.loads(add_json)))
    net = json.loads(network_json)
    for n in net:
        if n["node_id"] == ready_node_id:
            n["status"] = "resolved"
            break
    return {
        "atoms_json": json.dumps(new_atoms),
        "network_json": json.dumps(net),
        "plan_json": json.dumps(plan),
        "pre_state_sig": pre_sig,
        "seq_next": seq + 1,
        "status": status,
        "fail_reason": fail_reason,
        "applicable": True,
    }


@daft.func(return_dtype=DataType.string())
def frontier_sig(atoms_json: str, network_json: str) -> str:
    """Memo key encoding BOTH state AND remaining task network (Inv HTN-V2.12) — a
    state-only key would silently prune an applicable continuation with a distinct
    remaining network, violating completeness."""
    net = json.loads(network_json)
    rem = sorted((n["name"], json.dumps(n.get("args", []))) for n in net if n["status"] == "open")
    return hashlib.sha256((atoms_json + "||" + json.dumps(rem)).encode()).hexdigest()[:16]
