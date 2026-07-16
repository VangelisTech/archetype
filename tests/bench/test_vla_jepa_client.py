# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""No-GPU unit tests for the VLA-JEPA policy translation layer.

``VlaJepaPolicyClient`` is the boundary where ManipProprio becomes the VLA's
state vector and the VLA's action becomes a robosuite action. Every conversion
here is pure arithmetic — fully testable without Modal or a GPU — and a sign or
ordering error silently corrupts the actions the paper measures. ``import modal``
only happens lazily inside ``_get_worker``; injecting a fake worker bypasses it.
"""

from __future__ import annotations

import math
import pickle

import pytest

from bench.libero.clients import VlaJepaPolicyClient

_CHUNK_LEN = 7


def _action(gripper: float) -> list[float]:
    """A 7-dim action with a given model-space gripper value at index 6."""
    return [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, gripper]


def _obs(ref: str = "a.png") -> dict:
    return {
        "eef_pos": [0.1, 0.2, 0.3],
        "eef_quat": [0.0, 0.0, 0.0, 1.0],  # identity (x, y, z, w)
        "gripper_qpos": [0.04, -0.04],
        "agentview_ref": ref,
        "wrist_ref": ref,
    }


# --- gripper convention (model {0,1} -> robosuite {+1 close, -1 open}) --------


@pytest.mark.parametrize(
    "model_value, expected",
    [
        (1.0, -1.0),  # model "open"  -> robosuite open  (-1)
        (0.0, 1.0),  # model "close" -> robosuite close (+1)
        (0.9, -1.0),  # > 0.5 -> open
        (0.1, 1.0),  # < 0.5 -> close
        (0.5, 1.0),  # boundary: 0.5 is NOT > 0.5 -> close
    ],
)
def test_convert_gripper_sign_table(model_value, expected):
    out = VlaJepaPolicyClient._convert_gripper(_action(model_value))
    assert out[6] == expected
    # The 6 delta-pose dims pass through untouched.
    assert out[:6] == [0.0] * 6


# --- 8-dim state vector ordering ---------------------------------------------


def test_build_state_ordering_and_passthrough():
    state = VlaJepaPolicyClient._build_state(_obs())
    assert len(state) == 8, "state = eef_pos(3) + axis_angle(3) + gripper_qpos(2)"
    assert state[0:3] == [0.1, 0.2, 0.3], "eef_pos first, in order"
    assert state[3:6] == [0.0, 0.0, 0.0], "identity quat -> zero axis-angle"
    assert state[6:8] == [0.04, -0.04], "full gripper_qpos passthrough, in order"


# --- quaternion -> axis-angle ------------------------------------------------


def test_quat_to_axis_angle_identity():
    assert VlaJepaPolicyClient._build_state(_obs())[3:6] == [0.0, 0.0, 0.0]


def test_quat_to_axis_angle_ninety_degrees_about_x():
    # 90deg about +x: (x, y, z, w) = (sin45, 0, 0, cos45). Axis-angle magnitude
    # must equal the rotation angle (pi/2), directed along +x.
    s = math.sin(math.pi / 4)
    obs = _obs()
    obs["eef_quat"] = [s, 0.0, 0.0, s]
    aa = VlaJepaPolicyClient._build_state(obs)[3:6]
    assert aa[0] == pytest.approx(math.pi / 2, abs=1e-9)
    assert aa[1] == 0.0 and aa[2] == 0.0


def test_quat_to_axis_angle_clamps_w_overshoot():
    # w > 1 (float overshoot) must clamp, not blow up acos.
    obs = _obs()
    obs["eef_quat"] = [0.0, 0.0, 0.0, 1.5]
    aa = VlaJepaPolicyClient._build_state(obs)[3:6]
    assert aa == [0.0, 0.0, 0.0]


# --- pickle round-trip (Daft worker boundary) --------------------------------


def test_pickle_drops_live_handle_and_buffers():
    client = VlaJepaPolicyClient(suite="libero_spatial", task_id=3)
    client._worker = object()  # pretend a live Modal handle
    client._buffers = {0: [[1.0]]}  # pretend buffered chunks
    restored = pickle.loads(pickle.dumps(client))
    assert restored._worker is None, "live handle must not pickle"
    assert restored._buffers == {}, "buffers must not pickle (replay-from-0 hazard)"
    assert restored._suite == "libero_spatial" and restored._task_id == 3


def test_reset_clears_buffers():
    client = VlaJepaPolicyClient()
    client._buffers = {0: [[1.0]], 1: [[2.0]]}
    client.reset()
    assert client._buffers == {}


# --- act() chunk pop / refresh cadence ---------------------------------------


class _FakeRemote:
    def __init__(self, chunks: list[list[list[float]]]):
        self._chunks = chunks
        self.instructions: list[str] = []

    def remote(self, *, agentview_ref, wrist_ref, instruction, state):
        self.instructions.append(instruction)
        return self._chunks.pop(0)


class _FakeWorker:
    def __init__(self, chunks):
        self.infer_refs = _FakeRemote(chunks)


def test_act_buffers_one_chunk_and_refreshes_when_empty():
    # Two distinct chunks; the model gripper alternates so we can see which chunk
    # each popped action came from after the robosuite sign-flip.
    chunk_a = [_action(1.0) for _ in range(_CHUNK_LEN)]  # gripper -> -1
    chunk_b = [_action(0.0) for _ in range(_CHUNK_LEN)]  # gripper -> +1
    client = VlaJepaPolicyClient(suite="libero_spatial", task_id=0)
    client._worker = _FakeWorker([chunk_a, chunk_b])

    # First _CHUNK_LEN calls drain chunk_a from a SINGLE inference.
    for _ in range(_CHUNK_LEN):
        (action,) = client.act([0], ["pick up the cup"], [_obs()])
        assert action[6] == -1.0
    assert client._worker.infer_refs.instructions == ["pick up the cup"], "one refresh so far"

    # The next call empties the buffer -> a second inference with the new instruction.
    (action,) = client.act([0], ["set down the cup"], [_obs()])
    assert action[6] == 1.0, "now serving chunk_b"
    assert client._worker.infer_refs.instructions == ["pick up the cup", "set down the cup"]


def test_act_keeps_independent_buffers_per_env_key():
    chunk0 = [_action(1.0) for _ in range(_CHUNK_LEN)]
    chunk1 = [_action(0.0) for _ in range(_CHUNK_LEN)]
    client = VlaJepaPolicyClient()
    client._worker = _FakeWorker([chunk0, chunk1])
    out = client.act([0, 1], ["a", "b"], [_obs(), _obs()])
    assert out[0][6] == -1.0 and out[1][6] == 1.0, "env 0 and env 1 get their own chunks"
    assert client._worker.infer_refs.instructions == ["a", "b"]


# --- in-process clients must be picklable scalar stubs (Daft @daft.cls boundary) ---


def test_in_process_clients_pickle_as_scalar_stubs():
    """The colocated env + policy run behind Daft ``@daft.cls`` UDFs, which
    serialize the client. Their live state (MuJoCo envs / direct torch model) lives
    in process-global caches, so the instances must pickle as scalars — this
    is the regression guard for the serialization failure the first eval hit."""
    import pickle  # noqa: PLC0415

    from bench.libero.in_process import InProcessLiberoEnvClient  # noqa: PLC0415
    from bench.libero.in_process_policy import InProcessVlaJepaPolicy  # noqa: PLC0415

    env = InProcessLiberoEnvClient(
        suite="libero_spatial", task_id=2, with_frames=True, frames_dir="/frames"
    )
    env2 = pickle.loads(pickle.dumps(env))
    assert env2._suite_name == "libero_spatial" and env2._task_id == 2
    assert env2._frames_dir == "/frames" and env2._with_frames is True

    policy = InProcessVlaJepaPolicy(ckpt_dir="/ckpts", frames_dir="/frames")
    policy._buffers = {0: [[1.0] * 7]}  # a buffered chunk must NOT cross the boundary
    policy2 = pickle.loads(pickle.dumps(policy))
    assert policy2._ckpt_dir == "/ckpts" and policy2._frames_dir == "/frames"
    assert policy2._buffers == {}


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"use_bf16": False}, id="precision"),
        pytest.param({"use_sdpa": True}, id="attention"),
        pytest.param({"ckpt_dir": "/other-ckpts"}, id="checkpoint"),
    ],
)
def test_in_process_policy_reloads_direct_model_when_load_config_changes(monkeypatch, overrides):
    """Warm-container A/B calls must load the requested PyTorch configuration."""
    import bench.libero.in_process_policy as policy_module  # noqa: PLC0415

    launches = []

    def _fake_load(self):
        model = object()
        launches.append((self._model_config, model))
        return model

    monkeypatch.setattr(policy_module, "_MODEL", None)
    monkeypatch.setattr(policy_module, "_MODEL_CONFIG", None)
    monkeypatch.setattr(policy_module.InProcessVlaJepaPolicy, "_load_model", _fake_load)

    base_kwargs = {"ckpt_dir": "/ckpts", "use_bf16": True, "use_sdpa": False}
    first = policy_module.InProcessVlaJepaPolicy(**base_kwargs)
    first_model = first._ensure_model()

    # An identical client reuses the resident direct model.
    same_model = policy_module.InProcessVlaJepaPolicy(**base_kwargs)._ensure_model()
    assert same_model is first_model
    assert launches == [(first._model_config, first_model)]

    # A load-time option change replaces it instead of silently absorbing the
    # requested discriminator configuration.
    changed = policy_module.InProcessVlaJepaPolicy(**(base_kwargs | overrides))
    changed_model = changed._ensure_model()
    assert changed_model is not first_model
    assert launches == [
        (first._model_config, first_model),
        (changed._model_config, changed_model),
    ]
    assert policy_module._MODEL_CONFIG == changed._model_config
    assert not hasattr(policy_module, "_POLICY_SERVERS")


def test_in_process_policy_calls_upstream_model_directly(monkeypatch):
    import numpy as np  # noqa: PLC0415
    from PIL import Image  # noqa: PLC0415

    from bench.libero.in_process_policy import InProcessVlaJepaPolicy  # noqa: PLC0415

    class _FakeModel:
        def __init__(self) -> None:
            self.payload = None

        def predict_action(self, **payload):
            self.payload = payload
            return {"normalized_actions": np.zeros((1, 7, 7), dtype=np.float32)}

    model = _FakeModel()
    policy = InProcessVlaJepaPolicy()
    monkeypatch.setattr(policy, "_ensure_model", lambda: model)

    rgb = np.zeros((224, 224, 3), dtype=np.uint8)
    normalized = policy._predict_normalized("pick up the bowl", rgb, rgb, [0.0] * 8)

    assert normalized.shape == (7, 7)
    assert model.payload["instructions"] == ["pick up the bowl"]
    assert all(isinstance(image, Image.Image) for image in model.payload["batch_images"][0])
    assert np.asarray(model.payload["state"]).shape == (1, 1, 8)
