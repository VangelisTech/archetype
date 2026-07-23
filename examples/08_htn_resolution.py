# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Example 08 — HTN plan resolution as a fan-out AND/OR forest
===========================================================

Resolves a Hierarchical Task Network into executable plans inside an Archetype world.
Every branch of the decomposition is ONE row of ONE ``(Branch,)`` archetype; the
priority-ordered processor chain (Frontier -> Applicability -> Effect -> Termination)
evaluates preconditions and applies STRIPS effects *same-tick, same-row*, while this
driver does the ONE structural step the engine reserves for between ticks: OR fan-out
via ``world.spawn`` (copy-on-write ``plan_id`` multiplication), retiring each expanded
parent to ``superseded``.

Two fan-out axes are visible in the trace:
  * AXIS 1 — every live branch advances one micro-step per tick in a single vectorized
    pass (N branches cost one tick);
  * AXIS 2 — a ready compound with k applicable methods spawns k child branches.

The dogfood domain is an agentic "resolve a software issue" HTN whose ``fix`` task has
two rival methods (test-driven vs. direct), so resolution fans out into two distinct,
independently-executable plans — both extracted from their branch's own append-only
``plan_json``.

This driver lives in ``examples/`` (not ``src/``) because it ``.collect()``s at the
read-decide-spawn boundary — the same boundary as ``examples/pr_triage.py``.

Run:  uv run python examples/08_htn_resolution.py
"""

from __future__ import annotations

import asyncio
import json

from daft import col

from archetype import ArchetypeRuntime
from archetype.core.config import RunConfig, StorageConfig
from archetype.missions.planning import (
    Branch,
    HtnDomain,
    MethodSpec,
    OperatorSpec,
    Solved,
    build_initial_network,
    htn_processors,
    splice_method,
)

# ============================================================================
# The dogfood domain: "resolve a software issue"
# ============================================================================


def resolve_issue_domain() -> HtnDomain:
    """resolve_issue -> understand, fix, verify, ship.

    ``fix`` has two rival methods — test-driven (write a failing test, then edit) and
    direct (edit straight away) — so the planner fans out into two solution branches.
    """
    operators = [
        OperatorSpec("read_code", add=("understood",)),
        OperatorSpec("write_failing_test", pre_pos=("understood",), add=("test_written",)),
        OperatorSpec("edit_code", pre_pos=("test_written",), add=("code_edited",)),
        OperatorSpec("edit_code_direct", pre_pos=("understood",), add=("code_edited",)),
        OperatorSpec("run_tests", pre_pos=("code_edited",), add=("tests_pass",)),
        OperatorSpec("open_pr", pre_pos=("tests_pass",), add=("pr_open",)),
    ]
    methods = [
        MethodSpec(
            "resolve_issue",
            "resolve_issue#0",
            subtasks=(("understand", ()), ("fix", ()), ("verify", ()), ("ship", ())),
        ),
        MethodSpec("understand", "understand#0", subtasks=(("read_code", ()),)),
        # rival methods for `fix` — the OR node:
        MethodSpec(
            "fix",
            "fix#test_driven",
            pre_pos=("understood",),
            subtasks=(("write_failing_test", ()), ("edit_code", ())),
        ),
        MethodSpec(
            "fix",
            "fix#direct",
            pre_pos=("understood",),
            subtasks=(("edit_code_direct", ()),),
        ),
        MethodSpec("verify", "verify#0", subtasks=(("run_tests", ()),)),
        MethodSpec("ship", "ship#0", subtasks=(("open_pr", ()),)),
    ]
    return HtnDomain.of(operators, methods)


# ============================================================================
# The structural-only driver
# ============================================================================


def _branch_from_row(row: dict, **overrides) -> Branch:
    """Reconstruct the FULL ``Branch`` from a queried row (``update_entity`` overlays
    every supplied field, so a partial component would wipe state to defaults)."""
    values = {f: row[f"branch__{f}"] for f in Branch.model_fields}
    values.update(overrides)
    return Branch(**values)


async def resolve_htn(
    world,
    domain: HtnDomain,
    s0: list[str],
    tn0: list[tuple[str, list[str]]],
    *,
    max_ticks: int = 256,
    max_depth: int = 64,
    stop_on_first: bool = False,
    verbose: bool = True,
) -> dict:
    """Drive HTN resolution to completion. Returns the solution plans + a tick log.

    The driver does ZERO precondition / effect work — that all happens in the in-row
    processor chain. Between ticks it only: reads the materialized frontier, OR-spawns
    children for ready compounds, retires expanded parents, and records solved branches.
    """
    # Seed the root branch. The initial network embeds per-node op / method specs so the
    # pure UDFs never need the domain object (Inv HTN-V2.15).
    net0 = build_initial_network(domain, tn0)
    await world.spawn(Branch.make("r", s0, net0, max_depth=max_depth))

    rc = RunConfig(num_steps=1)  # ONE reused run_id for the whole resolution (Inv HTN-V2.14)
    solutions: list[dict] = []
    solved_ids: set[str] = set()
    ticks: list[dict] = []

    for _ in range(max_ticks):
        await world.step(config=rc)
        info = await world.info()
        rows = (await world.query(Branch)).where(col("tick") == info.tick - 1).collect().to_pylist()
        live = [r for r in rows if r["branch__status"] == "live"]
        expansions = 0

        # (1) record solved branches — extract the plan from the branch's own trace.
        for r in rows:
            pid = r["branch__plan_id"]
            if r["branch__status"] == "solved" and pid not in solved_ids:
                solved_ids.add(pid)
                plan = json.loads(r["branch__plan_json"])
                solutions.append({"plan_id": pid, "depth": r["branch__depth"], "plan": plan})

        # (2) OR fan-out: each live, ready COMPOUND with applicable methods spawns one
        #     child per method, then the parent is retired to `superseded`.
        for r in live:
            if r["branch__ready_kind"] != "compound" or not r["branch__needs_expansion"]:
                continue
            methods = json.loads(r["branch__applicable_methods_json"])
            network = json.loads(r["branch__network_json"])
            atoms = json.loads(r["branch__atoms_json"])
            plan = json.loads(r["branch__plan_json"])
            compound_id = r["branch__ready_node_id"]
            for j, m in enumerate(methods):
                child_net = splice_method(domain, network, compound_id, m["id"])
                await world.spawn(
                    Branch.make(
                        f"{r['branch__plan_id']}.{j}",
                        atoms,
                        child_net,
                        parent_branch_id=r["branch__plan_id"],
                        depth=r["branch__depth"] + 1,
                        max_depth=max_depth,
                        plan=plan,
                        seq_next=r["branch__seq_next"],
                    )
                )
            # retire the parent (Inv HTN-V2.17) — children carry the live continuation.
            await world.update(r["entity_id"], _branch_from_row(r, status="superseded"))
            expansions += 1

        if verbose:
            kinds = {}
            for r in live:
                kinds[r["branch__ready_op_name"]] = kinds.get(r["branch__ready_op_name"], 0) + 1
            ticks.append(
                {
                    "tick": info.tick - 1,
                    "live": len(live),
                    "solved": len(solutions),
                    "expansions": expansions,
                    "frontier": kinds,
                }
            )

        if stop_on_first and solutions:
            break
        if not live:  # exhaustion — every branch is solved / failed / superseded
            break

    # Attach the Solved marker to each solution branch (terminal migration, Inv HTN-V2.9)
    # and materialize it once, so solved branches are queryable via query(Branch, Solved).
    info = await world.info()
    latest = (await world.query(Branch)).where(col("tick") == info.tick - 1).collect().to_pylist()
    by_pid = {r["branch__plan_id"]: r for r in latest}
    for s in solutions:
        r = by_pid.get(s["plan_id"])
        if r is not None:
            await world.add_components(
                r["entity_id"], Solved.make(s["plan_id"], plan_length=len(s["plan"]))
            )
    if solutions:
        await world.step(config=rc)

    return {"solutions": solutions, "ticks": ticks}


# ============================================================================
# Demo
# ============================================================================


async def run_demo(storage_uri: str = "./archetype_data") -> dict[str, object]:
    """Resolve the dogfood HTN and return stable, semantic evidence."""
    domain = resolve_issue_domain()
    s0 = ["issue_open"]
    tn0 = [("resolve_issue", [])]

    async with ArchetypeRuntime() as runtime:
        world = runtime.world(
            "htn-resolve-issue",
            storage=StorageConfig(uri=storage_uri, namespace="htn_resolution"),
            processors=htn_processors(),
        )
        result = await resolve_htn(world, domain, s0, tn0, max_depth=32)

        # Solutions are first-class, queryable artifacts — reference them through the world.
        info = await world.info()
        persisted = (
            (await world.query(Branch, Solved))
            .where(col("tick") == info.tick - 1)
            .select("solved__plan_id", "solved__plan_length", "branch__atoms_json")
            .collect()
            .to_pylist()
        )
        solutions = []
        for solution in sorted(result["solutions"], key=lambda item: item["plan_id"]):
            plan = sorted(solution["plan"], key=lambda step: step["seq"])
            solutions.append(
                {
                    "plan_id": solution["plan_id"],
                    "operations": [step["op"] for step in plan],
                    "depth": solution["depth"],
                }
            )
        persisted_solutions = []
        for row in sorted(persisted, key=lambda item: item["solved__plan_id"]):
            persisted_solutions.append(
                {
                    "plan_id": row["solved__plan_id"],
                    "plan_length": row["solved__plan_length"],
                    "final_state": json.loads(row["branch__atoms_json"]),
                }
            )
        return {
            "solutions": solutions,
            "tick_trace": result["ticks"],
            "persisted_solutions": persisted_solutions,
        }


async def main() -> None:
    result = await run_demo()

    print("Resolving HTN: resolve_issue  (fix has 2 rival methods → fan-out)\n")
    print(f"{'tick':>4}  {'live':>4}  {'solved':>6}  {'exp':>3}  frontier")
    for tick in result["tick_trace"]:
        frontier = ", ".join(f"{name}×{count}" for name, count in tick["frontier"].items() if name)
        print(
            f"{tick['tick']:>4}  {tick['live']:>4}  {tick['solved']:>6}  "
            f"{tick['expansions']:>3}  {frontier}"
        )

    solutions = result["solutions"]
    print(f"\n{len(solutions)} solution plan(s) found via fan-out:\n")
    for solution in sorted(solutions, key=lambda item: len(item["operations"])):
        print(
            f"  [{solution['plan_id']}]  ({len(solution['operations'])} ops)  "
            f"{' -> '.join(solution['operations'])}"
        )

    persisted = result["persisted_solutions"]
    print(f"\nSolved branches queryable via query(Branch, Solved): {len(persisted)}")
    for row in persisted:
        print(
            f"  plan_id={row['plan_id']}  len={row['plan_length']}  "
            f"final_state={row['final_state']}"
        )


if __name__ == "__main__":
    asyncio.run(main())
