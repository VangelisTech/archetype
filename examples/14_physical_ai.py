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
from dataclasses import dataclass
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


@dataclass
class _ProviderLifetime:
    """Bounded evidence for one unique provider supplied to the runtime."""

    kind: str
    close_calls: int = 0
    is_open: bool = True


class _ProviderLifetimeTracker:
    """Track example-local providers without retaining them in the receipt."""

    def __init__(self) -> None:
        self._records: list[_ProviderLifetime] = []

    def register(self, kind: str) -> _ProviderLifetime:
        record = _ProviderLifetime(kind=kind)
        self._records.append(record)
        return record

    def receipt(self) -> dict[str, object]:
        closed_once = sum(record.close_calls == 1 for record in self._records)
        open_count = sum(record.is_open for record in self._records)
        if closed_once != len(self._records) or open_count:
            raise RuntimeError("runtime did not close every physical-AI provider exactly once")
        return {
            "runtime_context_completed": True,
            "unique_provider_count": len(self._records),
            "environment_count": sum(record.kind == "environment" for record in self._records),
            "policy_count": sum(record.kind == "policy" for record in self._records),
            "closed_once_count": closed_once,
            "open_provider_count": open_count,
        }


class _TrackedReachEnv(ScriptedReachEnv):
    def __init__(
        self,
        tracker: _ProviderLifetimeTracker,
        *,
        targets: dict[int, tuple[float, float, float]],
        tolerance: float,
    ) -> None:
        super().__init__(targets=targets, tolerance=tolerance)
        self._lifetime = tracker.register("environment")

    async def aclose(self) -> None:
        self._lifetime.close_calls += 1
        self._lifetime.is_open = False
        await super().aclose()


class _TrackedFramedReachEnv(ScriptedFramedReachEnv):
    def __init__(
        self,
        tracker: _ProviderLifetimeTracker,
        *,
        targets: dict[int, tuple[float, float, float]],
        tolerance: float,
    ) -> None:
        super().__init__(targets=targets, tolerance=tolerance)
        self._lifetime = tracker.register("environment")

    async def aclose(self) -> None:
        self._lifetime.close_calls += 1
        self._lifetime.is_open = False
        await super().aclose()


class _TrackedReachPolicy(ScriptedReachPolicy):
    def __init__(
        self,
        tracker: _ProviderLifetimeTracker,
        *,
        targets: dict[int, tuple[float, float, float]],
        gain: float,
        max_step: float,
    ) -> None:
        super().__init__(targets=targets, gain=gain, max_step=max_step)
        self._lifetime = tracker.register("policy")

    async def aclose(self) -> None:
        self._lifetime.close_calls += 1
        self._lifetime.is_open = False
        await super().aclose()


class _TrackedInstructionPolicy(InstructionConditionedReachPolicy):
    def __init__(
        self,
        tracker: _ProviderLifetimeTracker,
        *,
        targets: dict[int, tuple[float, float, float]],
        required_keywords: tuple[str, ...],
        gain: float,
        max_step: float,
    ) -> None:
        super().__init__(
            targets=targets,
            required_keywords=required_keywords,
            gain=gain,
            max_step=max_step,
        )
        self._lifetime = tracker.register("policy")

    async def aclose(self) -> None:
        self._lifetime.close_calls += 1
        self._lifetime.is_open = False
        await super().aclose()


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
    provider_lifetimes: _ProviderLifetimeTracker,
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
        env_client=_TrackedReachEnv(
            provider_lifetimes,
            targets=targets,
            tolerance=_TOLERANCE,
        ),
        policy_client=_TrackedInstructionPolicy(
            provider_lifetimes,
            targets=targets,
            required_keywords=_SWEEP_REQUIRED,
            gain=_SWEEP_GAIN,
            max_step=_MAX_STEP,
        ),
    )


async def run_demo(storage_uri: str = "./archetype_data") -> dict[str, object]:
    """Return normalized task, telemetry, sweep, optimizer, and cleanup evidence."""

    storage = StorageConfig(uri=storage_uri, namespace="physical_ai_demo")
    provider_lifetimes = _ProviderLifetimeTracker()
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
            env_client=_TrackedFramedReachEnv(
                provider_lifetimes,
                targets=_TASK_TARGETS,
                tolerance=_TOLERANCE,
            ),
            policy_client=_TrackedReachPolicy(
                provider_lifetimes,
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
        evidence_info = await evidence_world.info()
        telemetry_rows = (
            await evidence_world.query(
                ManipFrameRef,
                ManipProprio,
                ManipStatus,
                ManipTask,
            )
        ).to_pylist()
        reported_coordinates = (
            str(task_report.world_id),
            str(task_report.run_id),
        )
        info_coordinates = (
            str(evidence_info.world_id),
            str(evidence_info.run_id),
        )
        telemetry_coordinates = {
            (str(row["world_id"]), str(row["run_id"])) for row in telemetry_rows
        }
        evidence_pair_verified = (
            info_coordinates == reported_coordinates
            and telemetry_coordinates == {reported_coordinates}
        )
        if not evidence_pair_verified:
            raise RuntimeError("physical telemetry does not match the reported world/run identity")
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
        sweep_report = await _run_sweep(runtime, storage, variants, provider_lifetimes)

        async def evaluate(instructions: list[str]) -> dict[str, float]:
            report = await _run_sweep(
                runtime,
                storage,
                tuple(instructions),
                provider_lifetimes,
            )
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
                "suite": task_report.suite,
                "task_id": task_report.task_id,
                "instruction": task_report.instruction,
                "trial_count": len(task_report.trials),
                "success_count": sum(trial.success for trial in task_report.trials),
                "success_rate": task_report.success_rate,
                "mean_length": task_report.mean_length,
                "evidence_pair_verified": evidence_pair_verified,
                "trials": [
                    {
                        "trial_idx": trial.trial_idx,
                        "env_key": trial.env_key,
                        "seed": trial.seed,
                        "success": trial.success,
                        "episode_length": trial.episode_length,
                    }
                    for trial in task_report.trials
                ],
            },
            "telemetry": {
                "entity_count": len(entity_ids),
                "row_count": len(telemetry_rows),
                "coordinate_pair_count": len(telemetry_coordinates),
                "ticks": ticks,
                "all_frame_refs_present": all_frame_refs_present,
                "reset_refs": reset_refs,
            },
            "instruction_sweep": {
                "suite": sweep_report.suite,
                "task_id": sweep_report.task_id,
                "seeds_per_variant": _SWEEP_SEEDS,
                "scores": {variant: sweep_report.scores[variant] for variant in variants},
                "best_instruction": (
                    sweep_report.best.instruction if sweep_report.best is not None else None
                ),
                "outcomes": [
                    {
                        "instruction": outcome.instruction,
                        "n_trials": outcome.n_trials,
                        "n_success": outcome.n_success,
                        "success_rate": outcome.success_rate,
                        "mean_length": outcome.mean_length,
                    }
                    for outcome in sweep_report.variants
                ],
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

    semantic["cleanup"] = provider_lifetimes.receipt()
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
