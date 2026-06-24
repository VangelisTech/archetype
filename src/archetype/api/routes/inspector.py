# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Browser UI for running local inspector demos."""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

from fastapi import APIRouter, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel, Field

router = APIRouter(prefix="/inspector", tags=["inspector"])

_REPO_ROOT = Path(__file__).resolve().parents[4]
_OUTPUT_DIR = _REPO_ROOT / ".context" / "live-agent-inspector"
_SCRIPT = _REPO_ROOT / "examples" / "08_live_inspector.py"


class RunInspectorRequest(BaseModel):
    ticks: int = Field(default=6, ge=1, le=50)
    delay: float = Field(default=0.0, ge=0.0, le=5.0)


class RunInspectorResponse(BaseModel):
    status: str
    dashboard_url: str
    latest_url: str
    stdout: str


class MujocoJobRequest(BaseModel):
    backend: Literal["local", "modal"] = "local"
    simulations: int = Field(default=16, ge=1, le=100)
    ticks: int = Field(default=96, ge=2, le=2000)
    substeps: int = Field(default=5, ge=1, le=50)


class MujocoJobResponse(BaseModel):
    job_id: str
    backend: str
    status: str
    stream_url: str


@dataclass
class _StreamJob:
    job_id: str
    backend: str
    created_at: float = field(default_factory=time.time)
    events: list[dict[str, Any]] = field(default_factory=list)
    done: bool = False
    condition: asyncio.Condition = field(default_factory=asyncio.Condition)


_JOBS: dict[str, _StreamJob] = {}


@router.get("", response_class=HTMLResponse)
async def inspector_home() -> str:
    """Render the local inspector launcher."""
    return _render_shell()


@router.post("/live-agent/run", response_model=RunInspectorResponse)
async def run_live_agent_inspector(req: RunInspectorRequest) -> RunInspectorResponse:
    """Run the bundled live-agent inspector and expose its dashboard."""
    if not _SCRIPT.exists():
        raise HTTPException(status_code=500, detail=f"Inspector script not found: {_SCRIPT}")

    _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    env = {**os.environ, "PYTHONPATH": str(_REPO_ROOT / "src")}
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        str(_SCRIPT),
        "--ticks",
        str(req.ticks),
        "--delay",
        str(req.delay),
        "--no-open",
        "--output",
        str(_OUTPUT_DIR),
        cwd=str(_REPO_ROOT),
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    stdout_bytes, _ = await proc.communicate()
    stdout = stdout_bytes.decode(errors="replace")

    if proc.returncode != 0:
        raise HTTPException(
            status_code=500,
            detail={
                "message": "Inspector run failed",
                "returncode": proc.returncode,
                "output": stdout[-4000:],
            },
        )

    return RunInspectorResponse(
        status="complete",
        dashboard_url="/inspector/live-agent/files/index.html",
        latest_url="/inspector/live-agent/files/latest.json",
        stdout=stdout[-4000:],
    )


@router.post("/mujoco/jobs", response_model=MujocoJobResponse)
async def start_mujoco_job(req: MujocoJobRequest) -> MujocoJobResponse:
    job_id = str(uuid4())
    job = _StreamJob(job_id=job_id, backend=req.backend)
    _JOBS[job_id] = job
    asyncio.create_task(_run_mujoco_job(job, req))
    return MujocoJobResponse(
        job_id=job_id,
        backend=req.backend,
        status="running",
        stream_url=f"/inspector/mujoco/jobs/{job_id}/events",
    )


@router.get("/mujoco/jobs/{job_id}/events")
async def stream_mujoco_job(job_id: str) -> StreamingResponse:
    job = _JOBS.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Unknown MuJoCo job")

    async def event_stream():
        index = 0

        def ready() -> bool:
            return index < len(job.events) or job.done

        while True:
            async with job.condition:
                await job.condition.wait_for(ready)
                while index < len(job.events):
                    event = job.events[index]
                    index += 1
                    yield (
                        f"event: {event['event']}\n"
                        f"data: {json.dumps(event['data'], default=str)}\n\n"
                    )
                if job.done:
                    break

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


async def _emit(job: _StreamJob, event: str, data: dict[str, Any]) -> None:
    async with job.condition:
        job.events.append({"event": event, "data": data})
        job.condition.notify_all()


def _initial_state(sim_id: int) -> tuple[float, float, float, float]:
    return (
        (sim_id % 9 - 4) * 0.06,
        0.1 + (sim_id % 13) * 0.035,
        0.25 - (sim_id % 7) * 0.07,
        -0.2 + (sim_id % 5) * 0.1,
    )


async def _run_mujoco_job(job: _StreamJob, req: MujocoJobRequest) -> None:
    started = time.perf_counter()
    await _emit(
        job,
        "job_started",
        {
            "job_id": job.job_id,
            "backend": req.backend,
            "simulations": req.simulations,
            "ticks": req.ticks,
            "substeps": req.substeps,
        },
    )
    try:
        if req.backend == "modal":
            await _run_mujoco_modal(job, req)
        else:
            await _run_mujoco_local(job, req)
        await _emit(
            job,
            "job_complete",
            {
                "job_id": job.job_id,
                "backend": req.backend,
                "simulations": req.simulations,
                "wall_s": round(time.perf_counter() - started, 4),
            },
        )
    except Exception as exc:
        await _emit(
            job,
            "job_error",
            {
                "job_id": job.job_id,
                "backend": req.backend,
                "error": f"{type(exc).__name__}: {exc}",
            },
        )
    finally:
        async with job.condition:
            job.done = True
            job.condition.notify_all()


async def _run_mujoco_local(job: _StreamJob, req: MujocoJobRequest) -> None:
    async def run_one(sim_id: int) -> dict[str, Any]:
        return await asyncio.to_thread(_run_cartpole_local, sim_id, req.ticks, req.substeps)

    tasks = [asyncio.create_task(run_one(sim_id)) for sim_id in range(req.simulations)]
    completed = 0
    for task in asyncio.as_completed(tasks):
        result = await task
        completed += 1
        await _emit(job, "simulation_complete", {**result, "completed": completed})


def _run_cartpole_local(sim_id: int, ticks: int, substeps: int) -> dict[str, Any]:
    from archetype.experiments.mujoco_cartpole import raw_rollout

    initial = _initial_state(sim_id)
    trajectory = raw_rollout([initial], ticks=ticks, substeps=substeps)[0]
    return _serialize_mujoco_result(sim_id, initial, trajectory, ticks, substeps)


async def _run_mujoco_modal(job: _StreamJob, req: MujocoJobRequest) -> None:
    try:
        import modal
    except Exception as exc:
        raise RuntimeError(
            "Modal client is not installed in the server environment. "
            "Start the UI with: uv run --with modal archetype serve --host 127.0.0.1 --port 8000"
        ) from exc

    try:
        fn = modal.Function.from_name("archetype-mujoco-cartpole", "run_cartpole_sim")
    except Exception as exc:
        raise RuntimeError(
            "Modal worker is not deployed. Run: "
            "uv run --with modal modal deploy bench/mujoco/modal_cartpole.py"
        ) from exc

    async def run_one(sim_id: int) -> dict[str, Any]:
        initial = _initial_state(sim_id)
        return await asyncio.to_thread(fn.remote, sim_id, initial, req.ticks, req.substeps)

    tasks = [asyncio.create_task(run_one(sim_id)) for sim_id in range(req.simulations)]
    completed = 0
    for task in asyncio.as_completed(tasks):
        result = await task
        completed += 1
        await _emit(job, "simulation_complete", {**result, "completed": completed})


def _serialize_mujoco_result(
    sim_id: int,
    initial: tuple[float, float, float, float],
    trajectory,
    ticks: int,
    substeps: int,
) -> dict[str, Any]:
    return {
        "sim_id": sim_id,
        "initial": list(initial),
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


def _render_shell() -> str:
    dashboard_exists = (_OUTPUT_DIR / "index.html").exists()
    dashboard_src = "/inspector/live-agent/files/index.html" if dashboard_exists else ""
    iframe = (
        f'<iframe id="dashboard" src="{dashboard_src}" title="Live agent inspector"></iframe>'
        if dashboard_src
        else '<div id="empty">Run the inspector to generate the dashboard.</div>'
    )
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Archetype Inspector</title>
  <style>
    :root {{
      --bg: #070b0f;
      --panel: #101820;
      --panel-2: #0d1319;
      --ink: #e7edf2;
      --muted: #82909b;
      --line: #27333d;
      --line-hot: #00c98e;
      --accent: #00c98e;
      --accent-2: #ffb020;
      --danger: #ff674d;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      min-height: 100vh;
      background: var(--bg);
      color: var(--ink);
      font: 14px/1.45 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }}
    .app {{
      min-height: 100vh;
      display: grid;
      grid-template-rows: auto minmax(0, 1fr);
    }}
    header {{
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 16px;
      align-items: center;
      padding: 14px 16px;
      background: var(--panel);
      border-bottom: 1px solid var(--line);
      box-shadow: inset 0 -2px 0 rgba(0, 201, 142, 0.2);
    }}
    h1 {{
      margin: 0;
      font-size: 18px;
      font-weight: 850;
      letter-spacing: 0;
      text-transform: uppercase;
    }}
    .subtle {{ color: var(--muted); }}
    .forms {{
      display: flex;
      flex-wrap: wrap;
      gap: 14px;
      justify-content: flex-end;
    }}
    form {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      align-items: end;
      justify-content: flex-end;
    }}
    label {{
      display: grid;
      gap: 3px;
      color: var(--muted);
      font-size: 12px;
      font-weight: 700;
      text-transform: uppercase;
    }}
    input {{
      width: 82px;
      height: 34px;
      border: 1px solid #384651;
      border-radius: 6px;
      padding: 4px 8px;
      color: var(--ink);
      background: #060a0e;
    }}
    button {{
      height: 34px;
      border: 1px solid #02e0a0;
      border-radius: 6px;
      padding: 0 14px;
      background: var(--accent);
      color: #06100d;
      font-weight: 850;
      cursor: pointer;
      text-transform: uppercase;
    }}
    button:disabled {{ opacity: 0.55; cursor: wait; }}
    main {{
      display: grid;
      grid-template-rows: auto minmax(0, 1fr);
      gap: 10px;
      min-height: 0;
      padding: 12px;
    }}
    #status {{
      min-height: 34px;
      padding: 9px 11px;
      background: var(--panel);
      border: 1px solid var(--line);
      border-left: 3px solid var(--accent);
      border-radius: 6px;
      color: var(--muted);
      overflow-wrap: anywhere;
    }}
    #status.error {{ color: var(--danger); }}
    iframe, #empty {{
      width: 100%;
      height: 100%;
      min-height: 620px;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: var(--panel-2);
    }}
    #empty {{
      display: grid;
      place-items: center;
      color: var(--muted);
    }}
    pre {{
      margin: 8px 0 0;
      max-height: 120px;
      overflow: auto;
      white-space: pre-wrap;
      color: #a7f3d0;
      font: 12px/1.4 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
    }}
    .stream-console {{
      display: grid;
      gap: 6px;
      margin-top: 10px;
      max-height: 220px;
      overflow: auto;
      font: 12px/1.4 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
    }}
    .stream-row {{
      display: grid;
      grid-template-columns: 142px minmax(0, 1fr);
      gap: 10px;
      padding: 6px 8px;
      border: 1px solid var(--line);
      border-radius: 5px;
      background: #0a1016;
    }}
    .stream-row strong {{ color: var(--accent); }}
    select {{
      height: 34px;
      border: 1px solid #384651;
      border-radius: 6px;
      padding: 4px 8px;
      color: var(--ink);
      background: #060a0e;
    }}
    a {{ color: var(--accent); }}
    @media (max-width: 820px) {{
      header {{ grid-template-columns: 1fr; }}
      form {{ justify-content: flex-start; }}
    }}
  </style>
</head>
<body>
  <div class="app">
    <header>
      <div>
        <h1>Archetype Inspector</h1>
        <div class="subtle">Launch the live-agent run and inspect the generated world state.</div>
      </div>
      <div class="forms">
        <form id="run-form">
          <label>Ticks <input name="ticks" type="number" min="1" max="50" value="6"></label>
          <label>Delay <input name="delay" type="number" min="0" max="5" step="0.1" value="0"></label>
          <button id="run-button" type="submit">Run Inspector</button>
        </form>
        <form id="mujoco-form">
          <label>Backend <select name="backend"><option value="local">Local</option><option value="modal">Modal</option></select></label>
          <label>Sims <input name="simulations" type="number" min="1" max="100" value="16"></label>
          <label>Ticks <input name="ticks" type="number" min="2" max="2000" value="96"></label>
          <button id="mujoco-button" type="submit">Stream MuJoCo</button>
        </form>
      </div>
    </header>
    <main>
      <div id="status">{"Existing dashboard loaded." if dashboard_exists else "Idle."}</div>
      <div id="stream-status">MuJoCo stream idle.</div>
      {iframe}
    </main>
  </div>
  <script>
    const form = document.getElementById("run-form");
    const button = document.getElementById("run-button");
    const status = document.getElementById("status");
    const main = document.querySelector("main");
    const mujocoForm = document.getElementById("mujoco-form");
    const mujocoButton = document.getElementById("mujoco-button");
    const streamStatus = document.getElementById("stream-status");

    function setStatus(text, isError = false, output = "") {{
      status.className = isError ? "error" : "";
      status.innerHTML = text + (output ? `<pre>${{htmlEscape(output)}}</pre>` : "");
    }}

    function ensureFrame(src) {{
      let frame = document.getElementById("dashboard");
      const empty = document.getElementById("empty");
      if (!frame) {{
        frame = document.createElement("iframe");
        frame.id = "dashboard";
        frame.title = "Live agent inspector";
        if (empty) empty.replaceWith(frame);
        else main.appendChild(frame);
      }}
      frame.src = `${{src}}?t=${{Date.now()}}`;
    }}

    form.addEventListener("submit", async (event) => {{
      event.preventDefault();
      button.disabled = true;
      setStatus("Running inspector...");
      const data = Object.fromEntries(new FormData(form).entries());
      data.ticks = Number(data.ticks);
      data.delay = Number(data.delay);
      try {{
        const response = await fetch("/inspector/live-agent/run", {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify(data),
        }});
        const payload = await response.json();
        if (!response.ok) {{
          const detail = typeof payload.detail === "string"
            ? payload.detail
            : JSON.stringify(payload.detail, null, 2);
          throw new Error(detail);
        }}
        ensureFrame(payload.dashboard_url);
        setStatus(
          `Run complete. <a href="${{payload.latest_url}}">latest.json</a>`,
          false,
          payload.stdout
        );
      }} catch (error) {{
        setStatus(htmlEscape(String(error.message || error)), true);
      }} finally {{
        button.disabled = false;
      }}
    }});

    mujocoForm.addEventListener("submit", async (event) => {{
      event.preventDefault();
      mujocoButton.disabled = true;
      streamStatus.innerHTML = "Starting detached MuJoCo job...";
      const data = Object.fromEntries(new FormData(mujocoForm).entries());
      data.simulations = Number(data.simulations);
      data.ticks = Number(data.ticks);
      data.substeps = 5;
      try {{
        const response = await fetch("/inspector/mujoco/jobs", {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify(data),
        }});
        const job = await response.json();
        if (!response.ok) throw new Error(JSON.stringify(job.detail || job));
        streamJob(job);
      }} catch (error) {{
        streamStatus.innerHTML = `<span class="error">${{htmlEscape(String(error.message || error))}}</span>`;
        mujocoButton.disabled = false;
      }}
    }});

    function streamJob(job) {{
      const consoleNode = document.createElement("div");
      consoleNode.className = "stream-console";
      streamStatus.innerHTML = `Streaming job ${{job.job_id}} from ${{job.backend}}`;
      streamStatus.appendChild(consoleNode);
      const source = new EventSource(job.stream_url);
      const addRow = (kind, payload) => {{
        const row = document.createElement("div");
        row.className = "stream-row";
        row.innerHTML = `<strong>${{htmlEscape(kind)}}</strong><span>${{htmlEscape(JSON.stringify(payload))}}</span>`;
        consoleNode.prepend(row);
      }};
      source.addEventListener("job_started", (event) => addRow("started", JSON.parse(event.data)));
      source.addEventListener("simulation_complete", (event) => {{
        const payload = JSON.parse(event.data);
        addRow(`sim ${{payload.sim_id}}`, {{
          completed: payload.completed,
          ticks: payload.ticks,
          final: payload.states[payload.states.length - 1],
        }});
      }});
      source.addEventListener("job_complete", (event) => {{
        addRow("complete", JSON.parse(event.data));
        source.close();
        mujocoButton.disabled = false;
      }});
      source.addEventListener("job_error", (event) => {{
        addRow("error", JSON.parse(event.data));
        source.close();
        mujocoButton.disabled = false;
      }});
      source.onerror = () => {{
        addRow("stream", {{error: "connection closed"}});
        source.close();
        mujocoButton.disabled = false;
      }};
    }}

    function htmlEscape(value) {{
      const node = document.createElement("div");
      node.textContent = value;
      return node.innerHTML;
    }}
  </script>
</body>
</html>
"""


__all__ = ["router"]
