# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Record a video of VLA-JEPA driving a real LIBERO task.

The first end-to-end run of both deployed workers together: every control
step pulls camera frames from ``archetype-libero-env``, every 7th step
sends them (plus the 8-dim state) to ``archetype-vla-jepa`` for a fresh
action chunk — upstream's own open-loop chunk cadence.

Usage:
    uv run --with modal --with "imageio[ffmpeg]" \\
        python bench/libero/video_rollout.py --steps 120

Writes bench/libero/out/libero_vla_jepa_<suite>_task<id>.mp4
"""

from __future__ import annotations

import argparse
import base64
import io
import math
import sys
import time
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).parent))

import modal  # noqa: E402

CHUNK_SIZE = 7


def quat2axisangle(quat: list[float]) -> np.ndarray:
    """robosuite (x, y, z, w) quaternion -> axis-angle, as upstream's eval."""
    q = np.asarray(quat, dtype=np.float64)
    w = max(-1.0, min(1.0, q[3]))
    den = math.sqrt(max(0.0, 1.0 - w * w))
    if math.isclose(den, 0.0, abs_tol=1e-9):
        return np.zeros(3)
    return q[:3] * 2.0 * math.acos(w) / den


def decode_frame(b64: str) -> np.ndarray:
    from PIL import Image

    rgb = np.asarray(Image.open(io.BytesIO(base64.b64decode(b64))).convert("RGB"))
    # LIBERO renders upside down; flip for human-oriented video.
    return np.ascontiguousarray(rgb[::-1, ::-1])


def state_vector(obs: dict) -> list[float]:
    return [
        *obs["eef_pos"],
        *quat2axisangle(obs["eef_quat"]),
        *obs["gripper_qpos"],
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", default="libero_spatial")
    parser.add_argument("--task-id", type=int, default=0)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--steps", type=int, default=120)
    parser.add_argument("--fps", type=int, default=10)
    args = parser.parse_args()

    env_cls = modal.Cls.from_name("archetype-libero-env", "LiberoEnvBatch")
    env = env_cls(suite=args.suite, task_id=args.task_id)
    policy_cls = modal.Cls.from_name("archetype-vla-jepa", "VlaJepaPolicy")
    policy = policy_cls()

    instruction = env.task_language.remote()
    print(f"task: {instruction}")

    obs = env.reset.remote(0, seed=args.seed, with_frames=True)

    # Upstream waits 10 dummy steps after reset so the scene physics settle
    # before the first inference (LIBERO_DUMMY_ACTION = no-op, gripper open).
    dummy = [0.0] * 6 + [-1.0]
    for _ in range(10):
        (obs,) = env.step.remote([0], [dummy], with_frames=True)

    frames = [decode_frame(obs["agentview_png"])]
    chunk: list[list[float]] = []
    success = False
    started = time.perf_counter()

    for step in range(args.steps):
        if not chunk:
            chunk = policy.infer.remote(
                agentview_png=obs["agentview_png"],
                wrist_png=obs["wrist_png"],
                instruction=instruction,
                state=state_vector(obs),
            )
            print(
                f"step {step:3d}: new chunk ({len(chunk)} actions), "
                f"a0={[f'{v:.3f}' for v in chunk[0]]}"
            )
        action = list(chunk.pop(0))
        # Model emits gripper open in {0, 1}; robosuite wants {-1 open, +1
        # close} — upstream's _binarize_gripper_open: 1 - 2*(open > 0.5).
        action[6] = 1.0 - 2.0 * (action[6] > 0.5)

        (result,) = env.step.remote([0], [action], with_frames=True)
        obs = result
        frames.append(decode_frame(result["agentview_png"]))

        if result["success"]:
            success = True
            print(f"step {step:3d}: SUCCESS — task solved")
            break
        if result["done"]:
            print(f"step {step:3d}: episode done without success")
            break

    elapsed = time.perf_counter() - started
    print(f"{len(frames)} frames in {elapsed:.1f}s  success={success}")

    out_dir = Path(__file__).parent / "out"
    out_dir.mkdir(exist_ok=True)
    out_path = out_dir / f"libero_vla_jepa_{args.suite}_task{args.task_id}.mp4"

    import imageio.v3 as iio

    iio.imwrite(out_path, np.stack(frames), fps=args.fps, codec="libx264")
    print(f"video: {out_path}")


if __name__ == "__main__":
    main()
