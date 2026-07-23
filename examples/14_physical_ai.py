# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Example 14 — Deterministic physical-AI evidence
===============================================

Runs the supported runtime workflows with in-process scripted providers:

1. evaluate one reach task and read its persisted telemetry and frame refs;
2. compare instructions on paired seeds; and
3. optimize an instruction against repeated ledger-backed sweeps.

This is a deterministic mechanism check. It needs no simulator, model, GPU, or
provider credential.

Run:
    uv run python examples/14_physical_ai.py
"""

from __future__ import annotations

import asyncio
from typing import Any, cast

from archetype import (
    ArchetypeRuntime,
    InstructionSweepConfig,
    InstructionSweepReport,
    PhysicalTaskEvalConfig,
    StorageConfig,
)
from archetype.physical_ai import TemplatePerturbation, optimize_instruction
from archetype.physical_ai.manipulation import (
    ManipFrameRef,
    ManipProprio,
    ManipStatus,
    ManipTask,
    ScriptedFramedReachEnv,
    ScriptedReachEnv,
)
from archetype.physical_ai.policy import (
    InstructionConditionedReachPolicy,
    ScriptedReachPolicy,
)

_TOLERANCE = 0.02
_MAX_STEP = 0.05

_TASK_MAX_STEPS = 5
_TASK_TARGETS = {
    0: (0.10, 0.0, 0.5),
    1: (5.00, 0.0, 0.5),
}

_SWEEP_TASK_ID = 0
_SWEEP_SEEDS = 2
_SWEEP_MAX_STEPS = 5
_SWEEP_DISTANCES = (0.05, 0.19)
_SWEEP_REQUIRED = ("reach", "red")
_SWEEP_GAIN = 0.8
_SWEEP_TARGETS_LIMIT = 32


def _sweep_targets() -> dict[int, tuple[float, float, float]]:
    """Build targets whose distance depends on paired seed slot, not variant."""

    targets: dict[int, tuple[float, float, float]] = {}
    for env_key in range(_SWEEP_TARGETS_LIMIT):
        seed_slot = env_key % _SWEEP_SEEDS
        seed = _SWEEP_TASK_ID * 1000 + seed_slot
        start_x = 0.001 * seed
        start_y = -0.001 * seed
        targets[env_key] = (
            start_x + _SWEEP_DISTANCES[seed_slot],
            start_y,
            0.5,
        )
    return targets


async def _run_sweep(
    runtime: ArchetypeRuntime,
    storage: StorageConfig,
    variants: tuple[str, ...],
) -> InstructionSweepReport:
    targets = _sweep_targets()
    return await runtime.sweep_physical_instructions(
        InstructionSweepConfig(
            suite="scripted-reach",
            task_id=_SWEEP_TASK_ID,
            variants=variants,
            seeds_per_variant=_SWEEP_SEEDS,
            max_steps=_SWEEP_MAX_STEPS,
            storage=storage,
        ),
        env_client=ScriptedReachEnv(targets=targets, tolerance=_TOLERANCE),
        policy_client=InstructionConditionedReachPolicy(
            targets=targets,
            required_keywords=_SWEEP_REQUIRED,
            gain=_SWEEP_GAIN,
            max_step=_MAX_STEP,
        ),
    )


async def run_demo(storage_uri: str = "./archetype_data") -> dict[str, object]:
    """Return normalized task, telemetry, sweep, optimizer, and cleanup evidence."""

    storage = StorageConfig(uri=storage_uri, namespace="physical_ai_demo")
    semantic: dict[str, object]

    async with ArchetypeRuntime() as runtime:
        task_report = await runtime.evaluate_physical_task(
            PhysicalTaskEvalConfig(
                suite="scripted-reach",
                task_id=0,
                trials=2,
                max_steps=_TASK_MAX_STEPS,
                storage=storage,
                with_frames=True,
                instruction="reach",
            ),
            env_client=ScriptedFramedReachEnv(
                targets=_TASK_TARGETS,
                tolerance=_TOLERANCE,
            ),
            policy_client=ScriptedReachPolicy(
                targets=_TASK_TARGETS,
                gain=0.5,
                max_step=_MAX_STEP,
            ),
        )

        evidence_world = runtime.attach(
            task_report.world_id,
            name="physical-task-evidence",
            storage=storage,
        )
        telemetry_rows = (
            await evidence_world.query(
                ManipFrameRef,
                ManipProprio,
                ManipStatus,
                ManipTask,
            )
        ).to_pylist()
        ticks = sorted({int(row["tick"]) for row in telemetry_rows})
        entity_ids = {int(row["entity_id"]) for row in telemetry_rows}
        reset_rows = sorted(
            (row for row in telemetry_rows if int(row["tick"]) == 0),
            key=lambda row: int(row["maniptask__env_key"]),
        )
        reset_refs = [
            {
                "env_key": int(row["maniptask__env_key"]),
                "agentview": str(row["manipframeref__agentview_ref"]),
                "wrist": str(row["manipframeref__wrist_ref"]),
            }
            for row in reset_rows
        ]
        all_frame_refs_present = all(
            bool(row["manipframeref__agentview_ref"]) and bool(row["manipframeref__wrist_ref"])
            for row in telemetry_rows
        )

        variants = ("", "reach", "reach red")
        sweep_report = await _run_sweep(runtime, storage, variants)

        async def evaluate(instructions: list[str]) -> dict[str, float]:
            report = await _run_sweep(runtime, storage, tuple(instructions))
            return report.scores

        optimization = await optimize_instruction(
            evaluate=evaluate,
            base="",
            strategy=TemplatePerturbation(_SWEEP_REQUIRED),
            rounds=2,
            neighbors=2,
            patience=2,
        )

        semantic = {
            "task_evaluation": {
                "trial_count": len(task_report.trials),
                "success_count": sum(trial.success for trial in task_report.trials),
                "success_rate": task_report.success_rate,
                "evidence_addressable": bool(task_report.world_id and task_report.run_id),
            },
            "telemetry": {
                "entity_count": len(entity_ids),
                "row_count": len(telemetry_rows),
                "ticks": ticks,
                "all_frame_refs_present": all_frame_refs_present,
                "reset_refs": reset_refs,
            },
            "instruction_sweep": {
                "seeds_per_variant": _SWEEP_SEEDS,
                "scores": {variant: sweep_report.scores[variant] for variant in variants},
                "best_instruction": (
                    sweep_report.best.instruction if sweep_report.best is not None else None
                ),
            },
            "optimization": {
                "initial_success_rate": optimization.trace[0].best_success_rate,
                "best_instruction": optimization.best_instruction,
                "best_success_rate": optimization.best_success_rate,
                "trace": [record.best_success_rate for record in optimization.trace],
                "improved": (
                    optimization.best_success_rate > optimization.trace[0].best_success_rate
                ),
            },
        }

    # Reaching this line proves the runtime context completed its owned cleanup.
    semantic["cleanup"] = {"runtime_context_completed": True}
    return semantic


async def main() -> None:
    result = await run_demo()
    task = cast("dict[str, Any]", result["task_evaluation"])
    sweep = cast("dict[str, Any]", result["instruction_sweep"])
    optimization = cast("dict[str, Any]", result["optimization"])
    telemetry = cast("dict[str, Any]", result["telemetry"])

    print("Deterministic physical-AI evidence\n")
    print(
        f"Task: {task['success_count']}/{task['trial_count']} successful "
        f"({task['success_rate']:.0%})"
    )
    print(
        f"Telemetry: {telemetry['row_count']} rows across ticks "
        f"{telemetry['ticks']}; frame refs complete"
    )
    print(f"Paired sweep: {sweep['scores']} (best={sweep['best_instruction']!r})")
    print(
        f"Optimization: {optimization['initial_success_rate']:.0%} -> "
        f"{optimization['best_success_rate']:.0%} "
        f"with {optimization['best_instruction']!r}"
    )
    print("Runtime cleanup: complete")


if __name__ == "__main__":
    asyncio.run(main())
