# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Batched-inference contract for ``VlaJepaPolicyClient`` (CI-safe, no Modal).

The throughput primitive ([S3] in ``bench/libero/gepa_runner.py``): when N envs
need a chunk refresh on the same ``act()`` call, the client must issue **one**
``infer_refs_batch`` forward over all N — never a per-env loop of ``infer_refs``.

A fake Modal worker records every ``infer_refs_batch.remote`` call and the batch
size it was handed, so the assertions are exact and require no GPU/Modal:

1. **One forward per refresh boundary**: ticks where K live envs all have empty
   buffers issue exactly one batched call of size K.
2. **Client-side buffering preserved**: between refreshes no forward is issued;
   one action is popped per env per ``act()``.
3. **Row alignment**: the chunk returned for batch row j is the chunk applied to
   the env at the j-th refreshed position (gripper-converted).
"""

from __future__ import annotations

from typing import Any

from archetype.experiments.manipulation import ACTION_DIM
from archetype.experiments.policy import _CHUNK_LEN, VlaJepaPolicyClient


class _FakeBatchMethod:
    """Stands in for ``modal.Method``; records each batched call."""

    def __init__(self, owner: _FakeWorker) -> None:
        self._owner = owner

    def remote(
        self,
        *,
        agentview_refs: list[str],
        wrist_refs: list[str],
        instructions: list[str],
        states: list[list[float]],
    ) -> list[list[list[float]]]:
        n = len(agentview_refs)
        assert len(wrist_refs) == len(instructions) == len(states) == n
        self._owner.batch_sizes.append(n)
        self._owner.calls.append(
            {
                "agentview_refs": list(agentview_refs),
                "instructions": list(instructions),
                "states": [list(s) for s in states],
            }
        )
        # Deterministic chunk per row: encode the row's agentview ref index in
        # action[0] so the test can verify row alignment. gripper dim (6) is set
        # to 1.0 (model "open") so _convert_gripper maps it to -1.0.
        chunks: list[list[list[float]]] = []
        for ref in agentview_refs:
            tag = float(self._owner.ref_tag(ref))
            chunk = []
            for step in range(_CHUNK_LEN):
                action = [0.0] * ACTION_DIM
                action[0] = tag + step * 0.001
                action[6] = 1.0  # model open
                chunk.append(action)
            chunks.append(chunk)
        return chunks


class _FakeWorker:
    """Records batched inference calls; never touches Modal/GPU."""

    def __init__(self) -> None:
        self.batch_sizes: list[int] = []
        self.calls: list[dict[str, Any]] = []
        self.infer_refs_batch = _FakeBatchMethod(self)

    @staticmethod
    def ref_tag(ref: str) -> int:
        # ref looks like "sess/<env>/reset-agentview.png"; tag = the env index.
        return int(ref.split("/")[1])


def _obs(env_key: int) -> dict[str, Any]:
    return {
        "eef_pos": [0.1 * env_key, 0.0, 0.5],
        "eef_quat": [0.0, 0.0, 0.0, 1.0],
        "gripper": [0.0],
        "gripper_qpos": [0.0, 0.0],
        "agentview_ref": f"sess/{env_key}/reset-agentview.png",
        "wrist_ref": f"sess/{env_key}/reset-wrist.png",
    }


def _make_client() -> tuple[VlaJepaPolicyClient, _FakeWorker]:
    client = VlaJepaPolicyClient(suite="libero_spatial", task_id=0)
    fake = _FakeWorker()
    client._worker = fake  # bypass lazy Modal handle construction
    return client, fake


def test_single_batched_forward_over_live_envs() -> None:
    """N=4 envs with empty buffers → exactly ONE batched forward of size 4."""
    client, fake = _make_client()
    env_keys = [0, 1, 2, 3]
    instructions = [f"task {k}" for k in env_keys]
    observations = [_obs(k) for k in env_keys]

    actions = client.act(env_keys, instructions, observations)

    assert len(actions) == 4
    # Exactly one forward, of batch size 4 — not four single-env calls.
    assert fake.batch_sizes == [4], (
        f"expected ONE batched forward of size 4, got calls of sizes {fake.batch_sizes}"
    )
    # Row alignment: action[0] of env k equals its ref tag (k) at chunk step 0.
    for k, action in zip(env_keys, actions, strict=True):
        assert action[0] == float(k), f"env {k}: row misaligned, action[0]={action[0]}"
        # gripper converted model-open(1.0) → robosuite open(-1.0).
        assert action[6] == -1.0, f"env {k}: gripper not converted, got {action[6]}"


def test_buffering_preserved_no_forward_until_exhausted() -> None:
    """Within a chunk, no new forward is issued; one forward every _CHUNK_LEN ticks."""
    client, fake = _make_client()
    env_keys = [0, 1]
    instructions = ["a", "b"]
    observations = [_obs(0), _obs(1)]

    # Tick 1: both buffers empty → one batched forward of size 2.
    client.act(env_keys, instructions, observations)
    assert fake.batch_sizes == [2]

    # Ticks 2.._CHUNK_LEN: buffers non-empty → no further forwards.
    for _ in range(_CHUNK_LEN - 1):
        client.act(env_keys, instructions, observations)
    assert fake.batch_sizes == [2], f"no refresh expected mid-chunk; got {fake.batch_sizes}"

    # Next tick: buffers exhausted → exactly one more batched forward of size 2.
    client.act(env_keys, instructions, observations)
    assert fake.batch_sizes == [2, 2], (
        f"expected a second batched forward at the chunk boundary; got {fake.batch_sizes}"
    )


def test_partial_refresh_only_batches_empty_buffers() -> None:
    """When only some envs are at a chunk boundary, the forward batches just those."""
    client, fake = _make_client()

    # Prime env 0 alone so its buffer starts a chunk early.
    client.act([0], ["a"], [_obs(0)])
    assert fake.batch_sizes == [1]

    # Now act on envs 0 and 1: env 0 still has a buffered chunk, env 1 is empty.
    # Only env 1 should be in the batched forward.
    client.act([0, 1], ["a", "b"], [_obs(0), _obs(1)])
    assert fake.batch_sizes == [1, 1], (
        f"second forward should batch only the one empty-buffer env; got {fake.batch_sizes}"
    )
    # And that second call carried env 1's ref, not env 0's.
    assert fake.calls[1]["agentview_refs"] == ["sess/1/reset-agentview.png"]
