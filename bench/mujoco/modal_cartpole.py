# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Modal worker for stateless MuJoCo cartpole rollouts.

Deploy:
    uv run --with modal modal deploy bench/mujoco/modal_cartpole.py

Smoke:
    uv run --with modal modal run bench/mujoco/modal_cartpole.py --simulations 8
"""

from __future__ import annotations

from typing import Any

import modal

ROOT = "/repo"

image = (
    modal.Image.debian_slim(python_version="3.12")
    .apt_install("libgl1", "libegl1", "libglfw3", "libosmesa6")
    .pip_install("uv")
    .add_local_dir(
        ".",
        ROOT,
        copy=True,
        ignore=[".git", ".venv", ".context", "**/__pycache__", "archetype_data"],
    )
    .run_commands(f"cd {ROOT} && uv pip install --system -e '.[sim]'")
)

app = modal.App("archetype-mujoco-cartpole", image=image)


@app.function(cpu=1.0, memory=512, timeout=300, max_containers=100)
def run_cartpole_sim(
    sim_id: int,
    initial_state: tuple[float, float, float, float],
    ticks: int,
    substeps: int,
) -> dict[str, Any]:
    from archetype.experiments.mujoco_cartpole import raw_rollout

    trajectory = raw_rollout([initial_state], ticks=ticks, substeps=substeps)[0]
    return {
        "sim_id": sim_id,
        "initial": list(initial_state),
        "ticks": ticks,
        "substeps": substeps,
        "states": [
            {
                "tick": tick,
                "cart_pos": state[0],
                "pole_angle": state[1],
                "cart_vel": state[2],
                "pole_vel": state[3],
            }
            for tick, state in enumerate(trajectory)
        ],
    }


@app.local_entrypoint()
def main(simulations: int = 8, ticks: int = 96, substeps: int = 5):
    import time

    started = time.perf_counter()
    calls = [
        run_cartpole_sim.spawn(
            i,
            (
                (i % 7 - 3) * 0.07,
                0.12 + (i % 11) * 0.035,
                0.2 - (i % 5) * 0.08,
                -0.15 + (i % 3) * 0.15,
            ),
            ticks,
            substeps,
        )
        for i in range(simulations)
    ]
    results = [call.get() for call in calls]
    wall_s = time.perf_counter() - started
    print(
        {
            "simulations": len(results),
            "ticks": ticks,
            "substeps": substeps,
            "wall_s": round(wall_s, 3),
        }
    )
