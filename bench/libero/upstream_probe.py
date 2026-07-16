# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""LIBERO colocation debugging probes, retained as executable evidence tools.

These probes closed the 2026-07-15/16 0% investigation. The upstream oracle
scored 30/30 in this image, exonerating the py3.12/torch2.6 stack; the
full-episode boundary diff then isolated thread-bound EGL rendering, fixed by
``in_process.py::_EnvThread``. The resulting Archetype loop scored 3/3 on the
same task and init states (see ``image.py``'s RUN LEDGER).

The live probes call the same direct upstream ``baseframework`` model as
``InProcessVlaJepaPolicy``. The localhost transport used during the original
investigation has been retired.

RUN LEDGER (same rule as image.py — update only from watched runs):

    upstream_recon  verified 2026-07-16 (CPU, vangelis-tech): upstream evals
                    through examples/LIBERO/eval_libero.py (tyro CLI, per-step
                    M1Inference client, env.seed(7) constant, 256 render,
                    max_steps=250 for spatial, 10 wait steps).
    upstream_eval   HISTORICAL VERIFIED RUN (entrypoint retired with the model
                    server), 2026-07-16 (L40S, vangelis-tech): libero_spatial,
                    10 tasks x 3 episodes = 30/30 SUCCESS (100%) on the
                    py3.12/torch2.6/flash-attn2.7 stack, same then-current model and
                    checkpoint volume as the colocated eval. First attempt
                    crashed on torch 2.6 weights_only loading LIBERO init-state
                    pickles in their fresh interpreter — fixed via
                    TORCH_FORCE_NO_WEIGHTS_ONLY_LOAD=1 (their code unmodified).
                    VERDICT: the stack is exonerated; the 0% defect is in our
                    control-plane loop, not the image, model, or checkpoint.
    ab_probe        direct-model version NEVER RUN as of 2026-07-16. Its
                    predecessor proved step-0 inputs bit-identical.
    ab_rollout      direct-model version NEVER RUN as of 2026-07-16. Its
                    predecessor isolated EGL thread affinity as the defect.

    modal run bench/libero/upstream_probe.py::upstream_recon
    modal run bench/libero/upstream_probe.py::ab_probe
    modal run bench/libero/upstream_probe.py::ab_rollout
"""

import modal

from bench.libero.image import _VLA_CKPT_DIR, colocated_image, vla_ckpt_volume

app = modal.App("archetype-libero-upstream-probe", image=colocated_image)


@app.function(timeout=600)
def upstream_recon() -> dict:
    """CPU recon: map upstream's eval entrypoints and their CLIs before paying
    for a GPU. Prints the deployment tree, any eval shell scripts, and the
    argparse surface of the eval python scripts."""
    import subprocess  # noqa: PLC0415
    from pathlib import Path  # noqa: PLC0415

    repo = Path("/opt/VLA-JEPA")
    out: dict = {}

    tree = subprocess.run(
        ["find", str(repo), "-maxdepth", "3", "-name", "*eval*"],
        capture_output=True,
        text=True,
        check=False,
    )
    print("=== eval-related paths ===")
    print(tree.stdout)
    out["eval_paths"] = tree.stdout.splitlines()

    for sh in sorted(repo.rglob("*eval*.sh")):
        print(f"=== {sh} ===")
        print(sh.read_text())

    for py in sorted(repo.rglob("*eval*libero*.py")) + sorted(repo.rglob("*libero*eval*.py")):
        text = py.read_text()
        print(f"=== {py} (first 120 lines) ===")
        print("\n".join(text.splitlines()[:120]))

    return out


@app.function(
    gpu="L40S",
    timeout=3600,
    volumes={_VLA_CKPT_DIR: vla_ckpt_volume},
)
def ab_probe(task_id: int = 0) -> dict:
    """Tensor-level A/B between upstream's input pipeline and ours, one model.

    During the closed investigation, upstream scored 30/30 in this image while
    our loop scored 0 despite code-level parity. This probe diffs the actual data by building the step-0
    model input both ways for the same task + init state (their way: fresh env,
    seed 7, 10 dummy steps, rotate+resize in memory; our way:
    ``InProcessLiberoEnvClient`` reset with settle steps -> PNG round-trip ->
    ``_load_and_preprocess_ref``), sends both through the SAME direct model, and
    prints numerical diffs for every tensor plus both thumbnails.
    """
    import base64  # noqa: PLC0415
    import os  # noqa: PLC0415

    import cv2  # noqa: PLC0415
    import numpy as np  # noqa: PLC0415

    from bench.libero.clients import VlaJepaPolicyClient  # noqa: PLC0415
    from bench.libero.in_process import (  # noqa: PLC0415
        InProcessLiberoEnvClient,
        _patch_torch_load_for_libero,
    )
    from bench.libero.in_process_policy import InProcessVlaJepaPolicy  # noqa: PLC0415

    policy = InProcessVlaJepaPolicy(ckpt_dir=_VLA_CKPT_DIR, frames_dir="/tmp/ab-frames")

    def thumb(tag: str, rgb224: "np.ndarray") -> None:
        t = cv2.resize(rgb224, (64, 64), interpolation=cv2.INTER_AREA)
        ok, jpg = cv2.imencode(".jpg", cv2.cvtColor(t, cv2.COLOR_RGB2BGR))
        if ok:
            print(f"THUMB_{tag} {base64.b64encode(jpg.tobytes()).decode()}")

    def infer(agent224: "np.ndarray", wrist224: "np.ndarray", state8: list[float]) -> "np.ndarray":
        return policy._predict_normalized(  # noqa: SLF001
            instruction, agent224, wrist224, state8
        )

    # ---- upstream side: their exact construction --------------------------
    _patch_torch_load_for_libero()
    from libero.libero import benchmark, get_libero_path  # noqa: PLC0415
    from libero.libero.envs import OffScreenRenderEnv  # noqa: PLC0415

    suite = benchmark.get_benchmark_dict()["libero_spatial"]()
    task = suite.get_task(task_id)
    init_states = suite.get_task_init_states(task_id)
    instruction = str(task.language)
    bddl = os.path.join(get_libero_path("bddl_files"), task.problem_folder, task.bddl_file)
    env = OffScreenRenderEnv(bddl_file_name=bddl, camera_heights=256, camera_widths=256)
    env.seed(7)
    env.reset()
    obs_u = env.set_init_state(init_states[0])
    for _ in range(10):
        obs_u, _r, _d, _i = env.step([0.0] * 6 + [-1.0])
    agent_u = np.ascontiguousarray(obs_u["agentview_image"][::-1, ::-1])
    wrist_u = np.ascontiguousarray(obs_u["robot0_eye_in_hand_image"][::-1, ::-1])
    agent_u224 = cv2.resize(agent_u, (224, 224), interpolation=cv2.INTER_AREA)
    wrist_u224 = cv2.resize(wrist_u, (224, 224), interpolation=cv2.INTER_AREA)
    state_u = VlaJepaPolicyClient._build_state(  # noqa: SLF001
        {
            "eef_pos": [float(v) for v in obs_u["robot0_eef_pos"]],
            "eef_quat": [float(v) for v in obs_u["robot0_eef_quat"]],
            "gripper_qpos": [float(v) for v in obs_u["robot0_gripper_qpos"]],
        }
    )
    env.close()

    # ---- our side: the colocated loop's construction ----------------------
    ours_env = InProcessLiberoEnvClient(
        suite="libero_spatial",
        task_id=task_id,
        with_frames=True,
        frames_dir="/tmp/ab-frames",
    )
    obs_o = ours_env.reset(env_id=0, seed=0)
    agent_o224 = policy._load_and_preprocess_ref(obs_o["agentview_ref"])  # noqa: SLF001
    wrist_o224 = policy._load_and_preprocess_ref(obs_o["wrist_ref"])  # noqa: SLF001
    state_o = VlaJepaPolicyClient._build_state(obs_o)  # noqa: SLF001

    # ---- diffs -------------------------------------------------------------
    a_u = agent_u224.astype(np.int16)
    a_o = agent_o224.astype(np.int16)
    a_o_rot = a_o[::-1, ::-1]
    print(f"AB instruction: {instruction!r}")
    print(f"AB agent  |u-o|  mean={np.abs(a_u - a_o).mean():.2f} max={np.abs(a_u - a_o).max()}")
    print(
        f"AB agent  |u-rot180(o)| mean={np.abs(a_u - a_o_rot).mean():.2f} "
        f"max={np.abs(a_u - a_o_rot).max()}"
    )
    w_u = wrist_u224.astype(np.int16)
    w_o = wrist_o224.astype(np.int16)
    print(f"AB wrist  |u-o|  mean={np.abs(w_u - w_o).mean():.2f} max={np.abs(w_u - w_o).max()}")
    print(f"AB state  u={[round(v, 4) for v in state_u]}")
    print(f"AB state  o={[round(v, 4) for v in state_o]}")
    thumb("UPSTREAM", agent_u224)
    thumb("OURS", agent_o224)

    act_u = infer(agent_u224, wrist_u224, state_u)
    act_o = infer(agent_o224, wrist_o224, state_o)
    act_x = infer(agent_u224, wrist_u224, state_o)  # their frames, our state
    print(f"AB act_u[0]={[round(float(v), 4) for v in act_u[0]]}")
    print(f"AB act_o[0]={[round(float(v), 4) for v in act_o[0]]}")
    print(f"AB act_x[0]={[round(float(v), 4) for v in act_x[0]]}")
    print(f"AB |act_u-act_o| max={np.abs(act_u - act_o).max():.4f}")
    return {"done": True}


@app.function(
    gpu="L40S",
    timeout=3600,
    volumes={_VLA_CKPT_DIR: vla_ckpt_volume},
)
def ab_rollout(task_id: int = 0, max_steps: int = 250) -> dict:
    """Full-episode A/B: upstream's inline loop vs our run_task_eval, same
    container, same direct model, same task + init state. Records every action on
    both sides and reports the first step where the trajectories diverge.
    """
    import asyncio  # noqa: PLC0415
    import os  # noqa: PLC0415

    import cv2  # noqa: PLC0415
    import numpy as np  # noqa: PLC0415

    from archetype.app.container import ServiceContainer  # noqa: PLC0415
    from archetype.core.config import StorageConfig  # noqa: PLC0415
    from bench.libero.clients import VlaJepaPolicyClient  # noqa: PLC0415
    from bench.libero.eval_run import run_task_eval  # noqa: PLC0415
    from bench.libero.in_process import (  # noqa: PLC0415
        InProcessLiberoEnvClient,
        _patch_torch_load_for_libero,
    )
    from bench.libero.in_process_policy import InProcessVlaJepaPolicy  # noqa: PLC0415

    policy = InProcessVlaJepaPolicy(ckpt_dir=_VLA_CKPT_DIR, frames_dir="/tmp/abr-frames")

    # ---- upstream inline episode (their loop, verbatim semantics) ----------
    _patch_torch_load_for_libero()
    from libero.libero import benchmark, get_libero_path  # noqa: PLC0415
    from libero.libero.envs import OffScreenRenderEnv  # noqa: PLC0415

    suite = benchmark.get_benchmark_dict()["libero_spatial"]()
    task = suite.get_task(task_id)
    init_states = suite.get_task_init_states(task_id)
    instruction = str(task.language)
    bddl = os.path.join(get_libero_path("bddl_files"), task.problem_folder, task.bddl_file)
    env = OffScreenRenderEnv(bddl_file_name=bddl, camera_heights=256, camera_widths=256)
    env.seed(7)
    env.reset()
    obs = env.set_init_state(init_states[0])
    for _ in range(10):
        obs, _r, _d, _i = env.step([0.0] * 6 + [-1.0])

    actions_u: list[list[float]] = []
    boundary_u: list[dict] = []  # per inference: preprocessed frames + state
    done_u = False
    chunk: list[list[float]] = []
    for step in range(max_steps):
        if step % 7 == 0:
            agent = cv2.resize(
                np.ascontiguousarray(obs["agentview_image"][::-1, ::-1]),
                (224, 224),
                interpolation=cv2.INTER_AREA,
            )
            wrist = cv2.resize(
                np.ascontiguousarray(obs["robot0_eye_in_hand_image"][::-1, ::-1]),
                (224, 224),
                interpolation=cv2.INTER_AREA,
            )
            state = VlaJepaPolicyClient._build_state(  # noqa: SLF001
                {
                    "eef_pos": [float(v) for v in obs["robot0_eef_pos"]],
                    "eef_quat": [float(v) for v in obs["robot0_eef_quat"]],
                    "gripper_qpos": [float(v) for v in obs["robot0_gripper_qpos"]],
                }
            )
            boundary_u.append({"agent": agent, "wrist": wrist, "state": list(state)})
            normalized = policy._predict_normalized(  # noqa: SLF001
                instruction, agent, wrist, state
            )
            rows = policy._unnormalize(normalized)  # noqa: SLF001
            chunk = [VlaJepaPolicyClient._convert_gripper(a) for a in rows]  # noqa: SLF001
        action = chunk[step % 7]
        actions_u.append([float(v) for v in action])
        obs, _r, done, _i = env.step(list(action))
        if done or env.check_success():
            done_u = True
            break
    env.close()
    print(f"ABR upstream: done={done_u} steps={len(actions_u)}")

    # ---- our loop, actions + inference inputs recorded ----------------------
    actions_o: list[list[float]] = []
    boundary_o: list[dict] = []  # refs + state per actual inference
    _orig_act = InProcessVlaJepaPolicy.act
    _orig_infer = InProcessVlaJepaPolicy._infer_chunk  # noqa: SLF001

    def _recording_act(self, env_keys, instructions, observations):  # noqa: ANN001
        out = _orig_act(self, env_keys, instructions, observations)
        actions_o.append([float(v) for v in out[0]])
        return out

    def _recording_infer(self, instruction, obs):  # noqa: ANN001
        boundary_o.append(
            {
                "agentview_ref": obs["agentview_ref"],
                "wrist_ref": obs["wrist_ref"],
                "state": list(VlaJepaPolicyClient._build_state(obs)),  # noqa: SLF001
            }
        )
        return _orig_infer(self, instruction, obs)

    InProcessVlaJepaPolicy.act = _recording_act
    InProcessVlaJepaPolicy._infer_chunk = _recording_infer  # noqa: SLF001
    try:

        async def _run():  # noqa: ANN202
            container = ServiceContainer()
            try:
                ours_env = InProcessLiberoEnvClient(
                    suite="libero_spatial",
                    task_id=task_id,
                    with_frames=True,
                    frames_dir="/tmp/abr-frames",
                )
                return await run_task_eval(
                    world_service=container.world_service,
                    simulation_service=container.simulation_service,
                    eval_service=container.eval_service,
                    env_client=ours_env,
                    policy_client=policy,
                    suite="libero_spatial",
                    task_id=task_id,
                    trials=1,
                    max_steps=max_steps + 1,
                    storage=StorageConfig(uri="/tmp/abr-store", namespace="abr"),
                    with_frames=True,
                )
            finally:
                await container.shutdown()

        report = asyncio.run(_run())
    finally:
        InProcessVlaJepaPolicy.act = _orig_act
        InProcessVlaJepaPolicy._infer_chunk = _orig_infer  # noqa: SLF001
    print(f"ABR ours: success={report.success_rate} steps={len(actions_o)}")

    # ---- boundary-input diffs (the step-0 bit-test, at every boundary) ------
    def thumb(tag: str, rgb224: "np.ndarray") -> None:
        import base64  # noqa: PLC0415

        t = cv2.resize(rgb224, (64, 64), interpolation=cv2.INTER_AREA)
        ok, jpg = cv2.imencode(".jpg", cv2.cvtColor(t, cv2.COLOR_RGB2BGR))
        if ok:
            print(f"ABR_THUMB_{tag} {base64.b64encode(jpg.tobytes()).decode()}")

    for k in range(min(3, len(boundary_u), len(boundary_o))):
        bu, bo = boundary_u[k], boundary_o[k]
        agent_o = policy._load_and_preprocess_ref(bo["agentview_ref"])  # noqa: SLF001
        wrist_o = policy._load_and_preprocess_ref(bo["wrist_ref"])  # noqa: SLF001
        da = np.abs(bu["agent"].astype(np.int16) - agent_o.astype(np.int16))
        dw = np.abs(bu["wrist"].astype(np.int16) - wrist_o.astype(np.int16))
        ds = max(abs(u - o) for u, o in zip(bu["state"], bo["state"], strict=True))
        print(
            f"ABR boundary#{k} ({bo['agentview_ref']}): agent mean={da.mean():.2f} "
            f"max={da.max()} | wrist mean={dw.mean():.2f} max={dw.max()} | state maxdiff={ds:.5f}"
        )
        print(f"ABR boundary#{k} state_u={[round(v, 4) for v in bu['state']]}")
        print(f"ABR boundary#{k} state_o={[round(v, 4) for v in bo['state']]}")
        if k == 1:
            thumb("U1", bu["agent"])
            thumb("O1", agent_o)
            thumb("U1W", bu["wrist"])
            thumb("O1W", wrist_o)

    # ---- first divergence ---------------------------------------------------
    n = min(len(actions_u), len(actions_o))
    first = None
    for t in range(n):
        d = max(abs(u - o) for u, o in zip(actions_u[t], actions_o[t], strict=True))
        if d > 0.05:
            first = (t, d)
            break
    print(f"ABR first divergence >0.05: {first} (compared {n} steps)")
    for t in range(0, min(n, 22)):
        du = [round(v, 3) for v in actions_u[t]]
        do = [round(v, 3) for v in actions_o[t]]
        print(f"ABR t={t:3d} u={du} o={do}")
    if first is not None:
        t0 = max(0, first[0] - 2)
        for t in range(t0, min(n, first[0] + 3)):
            du = [round(v, 3) for v in actions_u[t]]
            do = [round(v, 3) for v in actions_o[t]]
            print(f"ABR DIV t={t:3d} u={du} o={do}")
    return {
        "upstream_done": done_u,
        "upstream_steps": len(actions_u),
        "ours_success": report.success_rate,
        "ours_steps": len(actions_o),
        "first_divergence": first,
    }
