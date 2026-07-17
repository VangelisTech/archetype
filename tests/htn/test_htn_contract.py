# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contract tests for the HTN fan-out plan-resolution pattern.

Each test pins a numbered invariant from ``.context/htn/PATTERN.md``. The end-to-end
tests drive the real ``examples/08_htn_resolution.py`` driver through a live Archetype
world and then independently REPLAY the extracted plans against the domain operators —
the strongest check that resolution is sound (executable by construction, Inv HTN-V2.4).
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import sys
from pathlib import Path

import pytest

# The runtime auto-configures Logfire; keep it offline and non-interactive in tests.
os.environ.setdefault("LOGFIRE_SEND_TO_LOGFIRE", "false")
os.environ.setdefault("LOGFIRE_IGNORE_NO_CONFIG", "1")

from archetype import ArchetypeRuntime  # noqa: E402
from archetype.core.config import StorageConfig  # noqa: E402
from archetype.htn import (  # noqa: E402
    Branch,
    EffectProcessor,
    HtnDomain,
    MethodSpec,
    OperatorSpec,
    build_initial_network,
    htn_processors,
    splice_method,
    udfs,  # noqa: E402
)

# Load the example driver directly — examples/ isn't a package on sys.path.
_EXAMPLE = Path(__file__).resolve().parents[2] / "examples" / "08_htn_resolution.py"
_spec = importlib.util.spec_from_file_location("htn_example", _EXAMPLE)
htn_example = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = htn_example
_spec.loader.exec_module(htn_example)

resolve_htn = htn_example.resolve_htn
resolve_issue_domain = htn_example.resolve_issue_domain


# ── helpers ───────────────────────────────────────────────────────────────


def _run(domain, s0, tn0, tmp_path, **kw) -> dict:
    """Resolve in a fresh, isolated runtime/world and return the driver result."""

    async def _go():
        async with ArchetypeRuntime() as runtime:
            world = runtime.world(
                "htn-test",
                storage=StorageConfig(uri=str(tmp_path), namespace="htn_test"),
                processors=htn_processors(),
            )
            return await resolve_htn(world, domain, s0, tn0, verbose=False, **kw)

    return asyncio.run(_go())


def _replay(domain: HtnDomain, s0: list[str], plan: list[dict]) -> set[str]:
    """Independently execute a plan from s0, re-checking STRIPS applicability at every
    step. Raises if any operator is applied to a state where its precondition fails —
    so a passing replay witnesses executability-by-construction (Inv HTN-V2.4.1/4.3)."""
    s = set(s0)
    for step in sorted(plan, key=lambda x: x["seq"]):
        op = domain.operators[step["op"]]
        g = op.ground(step["args"])
        assert set(g["pre_pos"]) <= s, f"{step['op']} pre+ not in state {sorted(s)}"
        assert not (set(g["pre_neg"]) & s), f"{step['op']} pre- intersects state {sorted(s)}"
        s = (s - set(g["del"])) | set(g["add"])
    return s


# ── pure UDF unit tests ─────────────────────────────────────────────────────


class TestUDFs:
    def test_precond_holds_strips(self):
        import daft
        from daft import col

        df = daft.from_pydict(
            {"a": [json.dumps(["x", "y"])], "pp": [json.dumps(["x"])], "pn": [json.dumps(["z"])]}
        )
        out = (
            df.with_column("ok", udfs.precond_holds(col("a"), col("pp"), col("pn")))
            .collect()
            .to_pydict()
        )
        assert out["ok"][0] is True
        # pre- now intersects -> not applicable
        df2 = daft.from_pydict(
            {"a": [json.dumps(["x", "z"])], "pp": [json.dumps(["x"])], "pn": [json.dumps(["z"])]}
        )
        out2 = (
            df2.with_column("ok", udfs.precond_holds(col("a"), col("pp"), col("pn")))
            .collect()
            .to_pydict()
        )
        assert out2["ok"][0] is False

    def test_apply_strips_delete_then_add(self):
        import daft
        from daft import col, lit

        # delete-then-add: an atom in both del and add survives (add wins).
        df = daft.from_pydict(
            {
                "a": [json.dumps(["p", "q"])],
                "add": [json.dumps(["q", "r"])],
                "del": [json.dumps(["q"])],
            }
        )
        out = (
            df.with_column("s", udfs.apply_strips(col("a"), col("add"), col("del"), lit(True)))
            .collect()
            .to_pydict()
        )
        assert json.loads(out["s"][0]) == ["p", "q", "r"]
        # apply=False leaves atoms untouched
        out2 = (
            df.with_column("s", udfs.apply_strips(col("a"), col("add"), col("del"), lit(False)))
            .collect()
            .to_pydict()
        )
        assert json.loads(out2["s"][0]) == ["p", "q"]

    def test_frontier_sig_encodes_network_not_just_state(self):
        """Inv HTN-V2.12: the memo key must distinguish two branches with identical
        state but different remaining task networks."""
        import daft
        from daft import col

        atoms = json.dumps(["x"])
        net_a = json.dumps([{"node_id": 0, "name": "a", "args": [], "status": "open"}])
        net_b = json.dumps([{"node_id": 0, "name": "b", "args": [], "status": "open"}])
        df = daft.from_pydict({"a": [atoms, atoms], "n": [net_a, net_b]})
        sigs = (
            df.with_column("sig", udfs.frontier_sig(col("a"), col("n")))
            .collect()
            .to_pydict()["sig"]
        )
        assert sigs[0] != sigs[1]


class TestProcessors:
    @pytest.mark.asyncio
    async def test_effect_processor_checks_depth_before_applying_primitive(self):
        """Inv HTN-V2.11: a branch over the cap cannot mutate state first."""
        import daft

        branch = Branch(
            atoms_json=json.dumps(["before"]),
            network_json=json.dumps([{"node_id": 7, "name": "mutate", "status": "open"}]),
            depth=1,
            max_depth=0,
            ready_node_id=7,
            ready_kind="primitive",
            ready_op_name="mutate",
            ready_args_json="[]",
            pre_pos_json=json.dumps(["before"]),
            add_json=json.dumps(["after"]),
            del_json=json.dumps(["before"]),
        )

        out = await EffectProcessor().process(daft.from_pylist([branch.to_row_dict()]))
        row = out.collect().to_pylist()[0]

        assert row["branch__status"] == "failed"
        assert row["branch__fail_reason"] == "depth_exceeded"
        assert json.loads(row["branch__atoms_json"]) == ["before"]
        assert json.loads(row["branch__plan_json"]) == []


# ── domain build / decomposition ─────────────────────────────────────────────


class TestDomain:
    def test_splice_resolves_compound_and_chains_subtasks(self):
        """Inv HTN-V2.13: splice marks the compound resolved, inserts subtasks chained
        in total order, and renumbers order_index topologically."""
        dom = resolve_issue_domain()
        net0 = build_initial_network(dom, [("resolve_issue", [])])
        net1 = splice_method(dom, net0, 0, "resolve_issue#0")
        by_name = {n["name"]: n for n in net1}
        assert by_name["resolve_issue"]["status"] == "resolved"
        assert by_name["resolve_issue"]["chosen_method"] == "resolve_issue#0"
        for name in ("understand", "fix", "verify", "ship"):
            assert by_name[name]["status"] == "open"
        # total-order chain: each later subtask depends on the previous
        assert by_name["fix"]["preds"] == [by_name["understand"]["node_id"]]
        assert by_name["ship"]["preds"] == [by_name["verify"]["node_id"]]
        # order_index is a dense topological rank
        idxs = sorted(n["order_index"] for n in net1)
        assert idxs == list(range(len(net1)))

    def test_compound_embeds_candidate_methods(self):
        dom = resolve_issue_domain()
        net = build_initial_network(dom, [("resolve_issue", [])])
        # the fix node (after splicing resolve) carries BOTH rival methods
        net = splice_method(dom, net, 0, "resolve_issue#0")
        fix = next(n for n in net if n["name"] == "fix")
        ids = {m["id"] for m in fix["candidate_methods"]}
        assert ids == {"fix#test_driven", "fix#direct"}


# ── end-to-end resolution ────────────────────────────────────────────────────


class TestResolution:
    def test_fanout_produces_two_executable_plans(self, tmp_path):
        """Inv HTN-V2.7 (OR completeness) + Inv HTN-V2.4.1 (executability)."""
        dom = resolve_issue_domain()
        res = _run(dom, ["issue_open"], [("resolve_issue", [])], tmp_path, max_depth=32)
        sols = res["solutions"]
        assert len(sols) == 2, f"expected 2 fan-out solutions, got {len(sols)}"

        # the two solutions use the two rival fix methods
        op_sets = [{s["op"] for s in sol["plan"]} for sol in sols]
        assert any("write_failing_test" in ops and "edit_code" in ops for ops in op_sets)
        assert any("edit_code_direct" in ops for ops in op_sets)

        # every extracted plan REPLAYS from s0 and reaches the goal (soundness)
        for sol in sols:
            final = _replay(dom, ["issue_open"], sol["plan"])
            assert "pr_open" in final
            # seq is strictly monotone (Inv HTN-V2.4.4)
            seqs = [s["seq"] for s in sorted(sol["plan"], key=lambda x: x["seq"])]
            assert seqs == list(range(len(seqs)))

    def test_stop_on_first_returns_one(self, tmp_path):
        dom = resolve_issue_domain()
        res = _run(
            dom, ["issue_open"], [("resolve_issue", [])], tmp_path, max_depth=32, stop_on_first=True
        )
        assert len(res["solutions"]) >= 1

    def test_no_applicable_method_fails_cleanly(self, tmp_path):
        """Inv HTN-V2.3 / guarded gamma: when a method decomposes to a primitive whose
        precondition can never hold, that branch fails precond_unsat and produces NO
        solution — it never fabricates an unexecutable plan."""
        dom = HtnDomain.of(
            operators=[OperatorSpec("impossible", pre_pos=("never_true",), add=("done",))],
            methods=[MethodSpec("goal", "goal#0", subtasks=(("impossible", ()),))],
        )
        res = _run(dom, [], [("goal", [])], tmp_path, max_depth=8)
        assert res["solutions"] == []

    def test_depth_cap_terminates_left_recursion(self, tmp_path):
        """Inv HTN-V2.11: a left-recursive method must NOT hang — the mandatory depth
        cap forces termination with zero solutions rather than diverging."""
        # count -> [tick, count] : unbounded recursion guarded only by the depth cap.
        dom = HtnDomain.of(
            operators=[OperatorSpec("tick", add=("ticked",))],
            methods=[MethodSpec("count", "count#0", subtasks=(("tick", ()), ("count", ())))],
        )
        res = _run(dom, [], [("count", [])], tmp_path, max_depth=5, max_ticks=200)
        assert res["solutions"] == []  # never solves, but terminates
