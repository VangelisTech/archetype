from __future__ import annotations

import pytest
from daft import col

from archetype import ArchetypeRuntime
from bench.robosemantic.batched import RsbStatus, RsbTask, run_batched_cell
from bench.robosemantic.protocol import RsbSuite


class FakeRsbEnvBatch:
    def __init__(self) -> None:
        self.reset_calls: list[tuple[list[int], list[int]]] = []
        self.step_calls: list[tuple[list[int], list[list[float]]]] = []
        self.steps: dict[int, int] = {}

    def reset_batch(self, env_keys: list[int], seeds: list[int]) -> list[dict]:
        self.reset_calls.append((list(env_keys), list(seeds)))
        self.steps = {env_key: 0 for env_key in env_keys}
        return [
            {
                "instruction": f"instruction-{seed}",
                "state": [float(seed)] * 14,
                "episode_info": {"seed": seed},
            }
            for seed in seeds
        ]

    def step_batch(self, env_keys: list[int], actions: list[list[float]]) -> list[dict]:
        self.step_calls.append((list(env_keys), [list(action) for action in actions]))
        out = []
        for env_key in env_keys:
            self.steps[env_key] += 1
            step = self.steps[env_key]
            out.append(
                {
                    "state": [float(step)] * 14,
                    "done": step >= env_key + 1,
                    "success": env_key in {0, 2} and step >= env_key + 1,
                    "grasp_success": step >= 1,
                    "episode_info": {"step": step},
                }
            )
        return out


class FakeBatchPolicy:
    def __init__(self) -> None:
        self.calls: list[tuple[list[int], list[str]]] = []

    def infer_batch(self, env_keys: list[int], instructions: list[str], observations: list[dict]):
        self.calls.append((list(env_keys), list(instructions)))
        return [
            [[float(env_key)] * 14, [float(env_key) + 0.5] * 14]
            for env_key in env_keys
        ]


@pytest.mark.asyncio
async def test_run_batched_cell_uses_entities_and_batched_policy_calls(tmp_path):
    suite = RsbSuite("RSB-Math-4", "rsb_math", "rsb_math_train_500", 4)
    env = FakeRsbEnvBatch()
    policy = FakeBatchPolicy()

    async with ArchetypeRuntime() as runtime:
        summary = await run_batched_cell(
            runtime=runtime,
            suite=suite,
            run_name="baseline",
            seeds=[10, 11, 12],
            env=env,
            policy=policy,
            max_steps=4,
            storage=str(tmp_path / "store"),
        )

        world = summary["world"]
        tasks = (await world.query(RsbTask)).where(col("rsbtask__run_name") == "baseline").to_pylist()
        statuses = (await world.query(RsbStatus)).where(col("tick") == summary["ticks"] - 1).to_pylist()

    assert env.reset_calls == [([0, 1, 2], [10, 11, 12])]
    assert policy.calls[0][0] == [0, 1, 2]
    assert policy.calls[0][1] == ["instruction-10", "instruction-11", "instruction-12"]
    assert len({row["entity_id"] for row in tasks}) == 3
    assert {row["rsbtask__seed"] for row in tasks} == {10, 11, 12}
    assert summary["episodes"] == 3
    assert summary["successes"] == 2
    assert summary["grasp_successes"] == 3
    assert summary["task_success_rate"] == pytest.approx(2 / 3)
    assert all(row["rsbstatus__done"] for row in statuses)
