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

"""Cartpole-on-the-ledger: MuJoCo dynamics as an Archetype processor.

Stage 1 of the LIBERO/VLA-JEPA ladder. Proves two things end to end:

1. Initial conditions land raw on the ledger at the spawn tick
   (the x_0-is-given contract from the initial-conditions spec).
2. The ledgered trajectory is bit-for-bit identical to a raw MuJoCo
   rollout: row at tick t equals the state after t * substeps physics
   steps, for every entity.

One Archetype tick = ``substeps`` MuJoCo steps (the control-timestep
pattern: later stages run one policy inference per tick while physics
integrates at a finer timestep).

The cartpole model is contact-free and unconstrained (no joint limits,
collisions disabled) with the RK4 integrator, so a MuJoCo step is a pure
function of (qpos, qvel) and bit-exact parity is well-defined. The ledger
row is therefore the *complete* physical state: restoring qpos/qvel from
any tick is a true fork of the world, MuJoCo included.

Requires the ``sim`` extra (``pip install archetype[sim]``). The module
imports without mujoco; only constructing the processor needs it.
"""

from __future__ import annotations

import daft
from daft import DataType, Series, col

from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.component import Component

CARTPOLE_XML = """
<mujoco model="cartpole">
  <option timestep="0.002" integrator="RK4"/>
  <worldbody>
    <body name="cart" pos="0 0 1">
      <joint name="slider" type="slide" axis="1 0 0" damping="0.05"/>
      <geom name="cart_geom" type="box" size="0.2 0.15 0.1" mass="1.0"
            contype="0" conaffinity="0"/>
      <body name="pole" pos="0 0 0">
        <joint name="hinge" type="hinge" axis="0 1 0" damping="0.01"/>
        <geom name="pole_geom" type="capsule" fromto="0 0 0 0 0 0.6"
              size="0.045" mass="0.1" contype="0" conaffinity="0"/>
      </body>
    </body>
  </worldbody>
</mujoco>
"""

DEFAULT_SUBSTEPS = 5


class CartpoleState(Component):
    """Complete cartpole state: qpos = (cart_pos, pole_angle), qvel likewise."""

    cart_pos: float = 0.0
    pole_angle: float = 0.0
    cart_vel: float = 0.0
    pole_vel: float = 0.0


_STATE_STRUCT = DataType.struct(
    {
        "cart_pos": DataType.float64(),
        "pole_angle": DataType.float64(),
        "cart_vel": DataType.float64(),
        "pole_vel": DataType.float64(),
    }
)


@daft.cls()
class _CartpoleStepper:
    """MuJoCo is a stateful C library; its step cannot be a lazy Daft
    expression. This is the sanctioned escape hatch: model loads once per
    worker, each batch crosses Python -> C once."""

    def __init__(self, xml: str, substeps: int):
        import mujoco

        # mujoco binds its members at runtime; alias once for the type checker.
        mj_model = mujoco.MjModel  # ty: ignore[unresolved-attribute]
        mj_data = mujoco.MjData  # ty: ignore[unresolved-attribute]
        self._mujoco = mujoco
        self._model = mj_model.from_xml_string(xml)
        self._data = mj_data(self._model)
        self._substeps = substeps

    @daft.method.batch(return_dtype=_STATE_STRUCT)
    def step(
        self,
        cart_pos: Series,
        pole_angle: Series,
        cart_vel: Series,
        pole_vel: Series,
    ) -> Series:
        mujoco, model, data = self._mujoco, self._model, self._data
        # Runtime-bound members, aliased once for the type checker.
        mj_reset = mujoco.mj_resetData  # ty: ignore[unresolved-attribute]
        mj_step = mujoco.mj_step  # ty: ignore[unresolved-attribute]
        cp = cart_pos.to_pylist()
        pa = pole_angle.to_pylist()
        cv = cart_vel.to_pylist()
        pv = pole_vel.to_pylist()

        out: list[dict[str, float]] = []
        for i in range(len(cp)):
            # The model has no contacts or constraints, so stepping is a pure
            # function of (qpos, qvel): resetting scratch MjData per row is
            # exactly equivalent to a continuous rollout.
            mj_reset(model, data)
            data.qpos[0] = cp[i]
            data.qpos[1] = pa[i]
            data.qvel[0] = cv[i]
            data.qvel[1] = pv[i]
            for _ in range(self._substeps):
                mj_step(model, data)
            out.append(
                {
                    "cart_pos": float(data.qpos[0]),
                    "pole_angle": float(data.qpos[1]),
                    "cart_vel": float(data.qvel[0]),
                    "pole_vel": float(data.qvel[1]),
                }
            )
        return Series.from_pylist(out)


class CartpoleStepProcessor(AsyncProcessor):
    components = (CartpoleState,)
    priority = 10

    def __init__(self, substeps: int = DEFAULT_SUBSTEPS, xml: str = CARTPOLE_XML):
        self._stepper = _CartpoleStepper(xml, substeps)

    async def process(self, df, **kwargs):
        nxt = self._stepper.step(
            col("cartpolestate__cart_pos"),
            col("cartpolestate__pole_angle"),
            col("cartpolestate__cart_vel"),
            col("cartpolestate__pole_vel"),
        )
        return (
            df.with_column("_mj_next", nxt)
            .with_columns(
                {
                    "cartpolestate__cart_pos": col("_mj_next")["cart_pos"],
                    "cartpolestate__pole_angle": col("_mj_next")["pole_angle"],
                    "cartpolestate__cart_vel": col("_mj_next")["cart_vel"],
                    "cartpolestate__pole_vel": col("_mj_next")["pole_vel"],
                }
            )
            .exclude("_mj_next")
        )


def raw_rollout(
    initial_states: list[tuple[float, float, float, float]],
    ticks: int,
    substeps: int = DEFAULT_SUBSTEPS,
    xml: str = CARTPOLE_XML,
) -> list[list[tuple[float, float, float, float]]]:
    """Continuous raw MuJoCo rollouts, one per initial state.

    Returns, per env, the state sequence [x_0, x_1, ..., x_{ticks-1}] where
    x_t is the state after t * substeps physics steps — the exact sequence
    the ledger must contain at ticks 0..ticks-1.
    """
    import mujoco

    # mujoco binds its members at runtime; alias once for the type checker.
    mj_model_cls = mujoco.MjModel  # ty: ignore[unresolved-attribute]
    mj_data_cls = mujoco.MjData  # ty: ignore[unresolved-attribute]
    mj_step = mujoco.mj_step  # ty: ignore[unresolved-attribute]

    model = mj_model_cls.from_xml_string(xml)
    trajectories = []
    for state in initial_states:
        data = mj_data_cls(model)
        data.qpos[0], data.qpos[1], data.qvel[0], data.qvel[1] = state
        trajectory = [state]
        for _ in range(ticks - 1):
            for _ in range(substeps):
                mj_step(model, data)
            trajectory.append(
                (
                    float(data.qpos[0]),
                    float(data.qpos[1]),
                    float(data.qvel[0]),
                    float(data.qvel[1]),
                )
            )
        trajectories.append(trajectory)
    return trajectories
