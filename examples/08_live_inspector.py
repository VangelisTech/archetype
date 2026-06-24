# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Live Agent Inspector
====================

Runs a small multi-agent messaging world and records it like a black box:

- live HTML dashboard in .context/live-agent-inspector/index.html
- one JSON snapshot per completed tick
- current entity state, tick diffs, audit trail, processors, resources, hooks
- source snippets for the components/processors currently shaping the world
- optional pdb breakpoint with runtime, world, mailbox, and snapshot in scope

Usage:
    uv run python examples/08_live_inspector.py
    uv run python examples/08_live_inspector.py --ticks 8 --delay 1.0
    uv run python examples/08_live_inspector.py --break-at-tick 2
"""

from __future__ import annotations

import argparse
import asyncio
import html
import inspect
import json
import time
import webbrowser
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import daft
from daft import DataFrame, col

from archetype import ArchetypeRuntime, Component, StorageConfig
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.hooks import PostTick, PreTick
from archetype.core.resources import Resources


class AgentState(Component):
    name: str = "unnamed"
    mood: str = "neutral"
    energy: float = 100.0


class Inbox(Component):
    messages_json: str = "[]"


@dataclass
class SimConfig:
    greeting_boost: float = 15.0


@dataclass
class Mailbox:
    pending: list[dict[str, Any]] = field(default_factory=list)
    delivered: int = 0


class MessageRealizationProcessor(AsyncProcessor):
    """Move pending resource messages into each recipient's Inbox."""

    components = (Inbox,)
    priority = -100

    async def process(self, df: DataFrame, resources: Resources, **kwargs) -> DataFrame:
        mailbox = resources.require(Mailbox)
        if not mailbox.pending:
            return df

        messages = mailbox.pending[:]
        mailbox.pending.clear()
        mailbox.delivered += len(messages)

        messages_by_receiver: dict[int, list[dict[str, Any]]] = {}
        for msg in messages:
            messages_by_receiver.setdefault(int(msg["receiver_id"]), []).append(msg)

        @daft.func.batch(return_dtype=daft.DataType.string())
        def update_inbox(entity_ids: daft.Series, inboxes: daft.Series) -> list[str]:
            results: list[str] = []
            for entity_id, inbox_json in zip(
                entity_ids.to_pylist(), inboxes.to_pylist(), strict=False
            ):
                inbox = json.loads(inbox_json or "[]")
                inbox.extend(messages_by_receiver.get(int(entity_id), []))
                results.append(json.dumps(inbox))
            return results

        return df.with_column(
            "inbox__messages_json",
            update_inbox(col("entity_id"), col("inbox__messages_json")),
        )


class GreetingProcessor(AsyncProcessor):
    """Each agent sends one greeting to every other agent each tick."""

    components = (AgentState,)
    priority = 10

    async def process(self, df: DataFrame, resources: Resources, tick: int, **kwargs) -> DataFrame:
        mailbox = resources.require(Mailbox)
        rows = df.select("entity_id", "agentstate__name").collect().to_pylist()

        for sender in rows:
            for receiver in rows:
                if sender["entity_id"] == receiver["entity_id"]:
                    continue
                mailbox.pending.append(
                    {
                        "sender_id": int(sender["entity_id"]),
                        "sender": sender["agentstate__name"],
                        "receiver_id": int(receiver["entity_id"]),
                        "receiver": receiver["agentstate__name"],
                        "content": f"Hello from {sender['agentstate__name']}",
                        "sent_tick": tick,
                    }
                )

        return df


class MoodProcessor(AsyncProcessor):
    """Derive mood and energy from accumulated inbox messages."""

    components = (AgentState, Inbox)
    priority = 20

    async def process(self, df: DataFrame, resources: Resources, **kwargs) -> DataFrame:
        config = resources.require(SimConfig)

        @daft.func.batch(return_dtype=daft.DataType.float64())
        def energy_boost(inboxes: daft.Series) -> list[float]:
            results: list[float] = []
            for inbox_json in inboxes.to_pylist():
                inbox = json.loads(inbox_json or "[]")
                results.append(len(inbox) * config.greeting_boost)
            return results

        @daft.func.batch(return_dtype=daft.DataType.string())
        def mood(inboxes: daft.Series) -> list[str]:
            results: list[str] = []
            for inbox_json in inboxes.to_pylist():
                count = len(json.loads(inbox_json or "[]"))
                if count >= 4:
                    results.append("delighted")
                elif count >= 2:
                    results.append("happy")
                elif count == 1:
                    results.append("content")
                else:
                    results.append("lonely")
            return results

        return (
            df.with_column("_boost", energy_boost(col("inbox__messages_json")))
            .with_column("agentstate__energy", col("agentstate__energy") + col("_boost"))
            .with_column("agentstate__mood", mood(col("inbox__messages_json")))
            .exclude("_boost")
        )


@dataclass
class TickMetric:
    phase: str
    tick: int
    message: str
    at_ms: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a live Archetype inspector demo.")
    parser.add_argument("--ticks", type=int, default=5, help="Completed ticks to record.")
    parser.add_argument(
        "--delay",
        type=float,
        default=0.75,
        help="Seconds to pause between ticks so the dashboard can be watched live.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(".context/live-agent-inspector"),
        help="Directory for the HTML dashboard and JSON snapshots.",
    )
    parser.add_argument(
        "--break-at-tick",
        type=int,
        default=None,
        help="Drop into pdb after writing this completed tick snapshot.",
    )
    parser.add_argument("--no-open", action="store_true", help="Do not open the dashboard.")
    return parser.parse_args()


def code_scope(*objects: object) -> list[dict[str, Any]]:
    scopes: list[dict[str, Any]] = []
    for obj in objects:
        source, start_line = inspect.getsourcelines(obj)
        file_path = inspect.getsourcefile(obj) or ""
        scopes.append(
            {
                "name": getattr(obj, "__name__", repr(obj)),
                "file": file_path,
                "start_line": start_line,
                "source": "".join(source),
            }
        )
    return scopes


def safe_model_dump(item: Any) -> dict[str, Any]:
    if hasattr(item, "model_dump"):
        return item.model_dump(mode="json")
    if hasattr(item, "dict"):
        return item.dict()
    return {"value": str(item)}


def decode_messages(messages_json: str | None) -> list[dict[str, Any]]:
    try:
        raw = json.loads(messages_json or "[]")
    except json.JSONDecodeError:
        return [{"content": messages_json or ""}]
    return raw if isinstance(raw, list) else []


def diff_rows(
    previous: list[dict[str, Any]],
    current: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    by_entity = {row["entity_id"]: row for row in previous}
    diffs: list[dict[str, Any]] = []
    for row in current:
        old = by_entity.get(row["entity_id"])
        messages = decode_messages(row.get("inbox__messages_json"))
        old_messages = decode_messages(old.get("inbox__messages_json")) if old else []
        diffs.append(
            {
                "entity_id": row["entity_id"],
                "name": row.get("agentstate__name", "?"),
                "mood": row.get("agentstate__mood", "?"),
                "mood_was": old.get("agentstate__mood") if old else None,
                "energy_delta": (
                    round(row.get("agentstate__energy", 0) - old.get("agentstate__energy", 0), 2)
                    if old
                    else None
                ),
                "new_messages": len(messages) - len(old_messages),
                "total_messages": len(messages),
            }
        )
    return diffs


def collect_mujoco_demo() -> dict[str, Any]:
    initial_states = [
        (0.0, 0.25, 0.4, 0.0),
        (-0.25, -0.18, 0.15, 0.35),
        (0.22, 0.38, -0.25, -0.15),
    ]
    ticks = 96
    substeps = 5
    try:
        from archetype.experiments.mujoco_cartpole import raw_rollout

        trajectories = raw_rollout(initial_states, ticks=ticks, substeps=substeps)
    except Exception as exc:
        return {
            "available": False,
            "error": f"{type(exc).__name__}: {exc}",
            "model": "cartpole",
            "ticks": ticks,
            "substeps": substeps,
            "trajectories": [],
        }

    envs = []
    for env_index, trajectory in enumerate(trajectories):
        envs.append(
            {
                "id": env_index,
                "name": f"cartpole-{env_index + 1}",
                "initial": list(initial_states[env_index]),
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
        )

    return {
        "available": True,
        "error": "",
        "model": "cartpole",
        "ticks": ticks,
        "substeps": substeps,
        "trajectories": envs,
    }


async def collect_snapshot(
    *,
    world,
    mailbox: Mailbox,
    completed_tick: int,
    previous_rows: list[dict[str, Any]],
    metrics: list[TickMetric],
    scopes: list[dict[str, Any]],
) -> dict[str, Any]:
    info = await world.info()
    state_df = await world.query(AgentState, Inbox)
    rows = (
        state_df.where(col("tick") == completed_tick)
        .select(
            "tick",
            "entity_id",
            "agentstate__name",
            "agentstate__mood",
            "agentstate__energy",
            "inbox__messages_json",
        )
        .collect()
        .to_pylist()
    )
    rows = sorted(rows, key=lambda row: row.get("agentstate__name", ""))

    audit_df = await world.history(limit=40)
    audit_rows = audit_df.collect().to_pylist()

    return {
        "captured_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "world": safe_model_dump(info),
        "completed_tick": completed_tick,
        "mailbox": {
            "pending": len(mailbox.pending),
            "pending_preview": mailbox.pending[:6],
            "delivered": mailbox.delivered,
        },
        "entities": rows,
        "diff": diff_rows(previous_rows, rows),
        "audit": audit_rows[-12:],
        "processors": [safe_model_dump(item) for item in await world.list_processors()],
        "resources": [safe_model_dump(item) for item in await world.list_resources()],
        "hooks": [safe_model_dump(item) for item in await world.list_hooks()],
        "metrics": [metric.__dict__ for metric in metrics[-12:]],
        "code_scope": scopes,
        "mujoco": collect_mujoco_demo(),
    }


def write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str) + "\n")


def td(value: Any) -> str:
    return f"<td>{html.escape(str(value))}</td>"


def render_rows(snapshot: dict[str, Any]) -> str:
    body = []
    for row in snapshot["entities"]:
        messages = decode_messages(row.get("inbox__messages_json"))
        body.append(
            "<tr>"
            + td(row["entity_id"])
            + td(row.get("agentstate__name", "?"))
            + td(row.get("agentstate__mood", "?"))
            + td(round(row.get("agentstate__energy", 0.0), 1))
            + td(len(messages))
            + td(messages[-1].get("content", "") if messages else "")
            + "</tr>"
        )
    return "\n".join(body)


def render_diff(snapshot: dict[str, Any]) -> str:
    body = []
    for row in snapshot["diff"]:
        mood_change = f"{row['mood_was']} -> {row['mood']}" if row.get("mood_was") else row["mood"]
        energy_delta = "new" if row["energy_delta"] is None else f"{row['energy_delta']:+.1f}"
        body.append(
            "<tr>"
            + td(row["name"])
            + td(mood_change)
            + td(energy_delta)
            + td(row["new_messages"])
            + td(row["total_messages"])
            + "</tr>"
        )
    return "\n".join(body)


def render_key_values(items: list[dict[str, Any]], fields: list[str]) -> str:
    rows = []
    for item in items:
        rows.append("<tr>" + "".join(td(item.get(field, "")) for field in fields) + "</tr>")
    return "\n".join(rows)


def json_for_script(data: Any) -> str:
    return json.dumps(data, default=str).replace("</", "<\\/")


def render_snapshot_links(output_dir: Path, completed_tick: int) -> str:
    snapshots_dir = output_dir / "snapshots"
    if not snapshots_dir.exists():
        return '<li><span class="muted">No snapshots yet</span></li>'
    links = []
    paths = []
    for path in snapshots_dir.glob("tick-*.json"):
        try:
            tick = int(path.stem.removeprefix("tick-"))
        except ValueError:
            continue
        if tick <= completed_tick:
            paths.append((tick, path))
    for _, path in sorted(paths, reverse=True)[:12]:
        links.append(
            f'<li><a href="snapshots/{html.escape(path.name)}">{html.escape(path.stem)}</a></li>'
        )
    return "\n".join(links) or '<li><span class="muted">No snapshots yet</span></li>'


def render_sidebar(snapshot: dict[str, Any], output_dir: Path) -> str:
    entity_count = len(snapshot["entities"])
    processor_count = len(snapshot["processors"])
    hook_count = len(snapshot["hooks"])
    return f"""
    <aside class="rail">
      <div class="brand">
        <div class="brand-mark">AI</div>
        <div>
          <strong>Inspector</strong>
          <span>{"live run" if snapshot["mailbox"]["pending"] else "world view"}</span>
        </div>
      </div>
      <nav class="nav">
        <a href="#state"><span>State</span><b>{entity_count}</b></a>
        <a href="#timeline"><span>Timeline</span><b>{len(snapshot["metrics"])}</b></a>
        <a href="#runtime"><span>Runtime</span><b>{processor_count + hook_count}</b></a>
        <a href="#audit"><span>Audit</span><b>{len(snapshot["audit"])}</b></a>
        <a href="#scope"><span>Code</span><b>{len(snapshot["code_scope"])}</b></a>
      </nav>
      <section class="rail-section">
        <h3>Snapshots</h3>
        <ol class="snapshot-list">
          {render_snapshot_links(output_dir, snapshot["completed_tick"])}
        </ol>
      </section>
      <section class="rail-section">
        <h3>World</h3>
        <dl class="kv">
          <div><dt>completed</dt><dd>{snapshot["completed_tick"]}</dd></div>
          <div><dt>current</dt><dd>{snapshot["world"].get("tick", "")}</dd></div>
          <div><dt>run</dt><dd>{html.escape(str(snapshot["world"].get("run_id", "")))[:12]}</dd></div>
        </dl>
      </section>
    </aside>
    """


def render_code(scopes: list[dict[str, Any]]) -> str:
    blocks = []
    for index, scope in enumerate(scopes):
        escaped = html.escape(scope["source"])
        open_attr = " open" if index == 0 else ""
        blocks.append(
            f"<details{open_attr}>"
            f"<summary>{html.escape(scope['name'])} "
            f"<span>{html.escape(scope['file'])}:{scope['start_line']}</span></summary>"
            f"<pre><code>{escaped}</code></pre>"
            "</details>"
        )
    return "\n".join(blocks)


def render_html(snapshot: dict[str, Any], output_dir: Path, *, running: bool) -> str:
    refresh = '<meta http-equiv="refresh" content="1">' if running else ""
    latest_json = "latest.json"
    title = "Archetype Live Agent Inspector"
    status = "running" if running else "complete"
    pending_preview = html.escape(
        json.dumps(snapshot["mailbox"]["pending_preview"], indent=2, default=str)
    )
    mujoco = snapshot.get("mujoco", {"available": False, "error": "not collected"})
    mujoco_payload = json_for_script(mujoco)
    mujoco_state = "ready" if mujoco.get("available") else "missing"
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  {refresh}
  <title>{title}</title>
  <style>
    :root {{
      --bg: #070b0f;
      --panel: #111922;
      --panel-2: #0d1319;
      --ink: #e6edf2;
      --muted: #82909b;
      --line: #26323a;
      --line-strong: #384650;
      --accent: #00c98e;
      --accent-2: #ffb020;
      --warn: #ff674d;
      --code: #05080b;
      --rail: #0b1016;
      --rail-ink: #c7d1d9;
      --selected: #12352d;
    }}
    * {{ box-sizing: border-box; }}
    html, body {{ min-height: 100%; }}
    body {{
      margin: 0;
      background: var(--bg);
      color: var(--ink);
      font: 14px/1.45 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }}
    a {{ color: var(--accent); text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}
    header {{
      grid-area: topbar;
      display: grid;
      grid-template-columns: 1fr auto;
      align-items: center;
      gap: 16px;
      min-width: 0;
      min-height: 56px;
      padding: 10px 18px;
      border-bottom: 1px solid var(--line);
      background: #101820;
      box-shadow: inset 0 -2px 0 rgba(0, 201, 142, 0.18);
    }}
    h1 {{
      margin: 0;
      font-size: 16px;
      font-weight: 850;
      letter-spacing: 0;
      text-transform: uppercase;
    }}
    h2 {{ margin: 0; font-size: 14px; letter-spacing: 0; }}
    h3 {{
      margin: 0 0 8px;
      color: var(--muted);
      font-size: 11px;
      font-weight: 850;
      letter-spacing: 0;
      text-transform: uppercase;
    }}
    .app {{
      min-height: 100vh;
      display: grid;
      grid-template-columns: 244px minmax(0, 1fr);
      grid-template-rows: auto minmax(0, 1fr);
      grid-template-areas:
        "rail topbar"
        "rail workbench";
    }}
    .rail {{
      grid-area: rail;
      min-height: 100vh;
      padding: 14px;
      background: var(--rail);
      border-right: 1px solid var(--line-strong);
      overflow-y: auto;
    }}
    .brand {{
      display: grid;
      grid-template-columns: 34px minmax(0, 1fr);
      align-items: center;
      gap: 10px;
      padding: 4px 4px 16px;
    }}
    .brand-mark {{
      display: grid;
      place-items: center;
      width: 34px;
      height: 34px;
      border-radius: 8px;
      border: 1px solid var(--accent);
      background: #07110e;
      color: var(--accent);
      font-weight: 800;
      font-size: 12px;
    }}
    .brand strong {{ display: block; }}
    .brand span, .muted, .subtle, summary span {{ color: var(--muted); }}
    .nav {{
      display: grid;
      gap: 4px;
      margin-bottom: 18px;
    }}
    .nav a {{
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      align-items: center;
      min-height: 32px;
      padding: 6px 8px;
      border-radius: 6px;
      color: var(--rail-ink);
    }}
    .nav a:first-child {{
      background: var(--selected);
      box-shadow: inset 3px 0 0 var(--accent);
    }}
    .nav b {{
      color: var(--muted);
      font-size: 12px;
      font-weight: 600;
    }}
    .rail-section {{
      padding: 12px 4px;
      border-top: 1px solid var(--line);
    }}
    .snapshot-list {{
      margin: 0;
      padding-left: 20px;
      display: grid;
      gap: 4px;
      font-size: 13px;
    }}
    .kv {{
      display: grid;
      gap: 7px;
      margin: 0;
    }}
    .kv div {{
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 10px;
    }}
    .kv dt {{ color: var(--muted); }}
    .kv dd {{ margin: 0; overflow-wrap: anywhere; }}
    .top-meta {{
      display: flex;
      flex-wrap: wrap;
      justify-content: flex-end;
      gap: 8px;
      min-width: 0;
    }}
    .chip {{
      display: inline-flex;
      align-items: center;
      min-height: 26px;
      padding: 3px 8px;
      border: 1px solid var(--line);
      border-radius: 999px;
      background: #0a0f14;
      color: var(--muted);
      font-size: 12px;
      white-space: nowrap;
    }}
    .chip.strong {{
      color: #06100d;
      background: var(--accent);
      border-color: var(--accent);
      font-weight: 850;
      text-transform: uppercase;
    }}
    main {{
      grid-area: workbench;
      display: grid;
      grid-template-columns: minmax(520px, 1.35fr) minmax(360px, 0.75fr);
      grid-template-rows: auto minmax(280px, 1fr) minmax(260px, 0.82fr);
      grid-template-areas:
        "metrics metrics"
        "state inspector"
        "timeline code";
      gap: 12px;
      min-height: 0;
      padding: 12px;
      overflow: hidden;
    }}
    .metrics {{ grid-area: metrics; display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 8px; }}
    .state {{ grid-area: state; }}
    .inspector {{ grid-area: inspector; }}
    .timeline {{ grid-area: timeline; }}
    .code-scope {{ grid-area: code; }}
    .panel {{
      display: flex;
      flex-direction: column;
      min-width: 0;
      min-height: 0;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      overflow: hidden;
    }}
    .panel.state {{
      border-color: #38524b;
      box-shadow: inset 0 2px 0 rgba(0, 201, 142, 0.35);
    }}
    .panel-header {{
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 10px;
      align-items: center;
      min-height: 42px;
      padding: 10px 12px;
      border-bottom: 1px solid var(--line);
      background: #0d141b;
    }}
    .panel-title {{
      display: flex;
      align-items: center;
      gap: 10px;
      min-width: 0;
    }}
    .tabs {{
      display: inline-flex;
      gap: 4px;
      padding: 3px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #070b0f;
    }}
    .tab-button {{
      min-height: 26px;
      border: 0;
      border-radius: 6px;
      padding: 3px 9px;
      background: transparent;
      color: var(--muted);
      font: inherit;
      font-size: 12px;
      font-weight: 800;
      cursor: pointer;
      text-transform: uppercase;
    }}
    .tab-button.active {{
      background: var(--accent);
      color: #06100d;
    }}
    .panel-body {{
      min-height: 0;
      padding: 12px;
      overflow: auto;
    }}
    .metric {{
      min-height: 72px;
      padding: 12px;
      background: #0f171f;
      border-left: 3px solid var(--accent);
    }}
    .metric strong {{
      display: block;
      font-size: 24px;
      margin-top: 4px;
      color: #ffffff;
    }}
    .metric span {{ color: var(--muted); }}
    table {{ width: 100%; border-collapse: collapse; table-layout: fixed; }}
    th, td {{
      text-align: left;
      border-bottom: 1px solid #202b33;
      padding: 7px 6px;
      vertical-align: top;
      overflow-wrap: anywhere;
    }}
    tbody tr:hover {{ background: #122019; }}
    th {{ color: var(--muted); font-size: 12px; font-weight: 600; }}
    code, pre {{ font: 12px/1.5 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }}
    pre {{
      margin: 0;
      padding: 12px;
      background: var(--code);
      color: #c9f7e5;
      border-radius: 6px;
      overflow-x: auto;
    }}
    details {{ border-top: 1px solid #202b33; padding-top: 9px; margin-top: 9px; }}
    details:first-child {{ border-top: 0; padding-top: 0; margin-top: 0; }}
    summary {{ cursor: pointer; font-weight: 700; }}
    .stack {{ display: grid; gap: 12px; }}
    .split {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 12px;
      min-height: 0;
    }}
    .mini {{
      border: 1px solid var(--line);
      border-radius: 7px;
      overflow: hidden;
      min-height: 0;
      background: var(--panel-2);
    }}
    .mini h3 {{
      margin: 0;
      padding: 8px 10px;
      border-bottom: 1px solid var(--line);
      background: #0a1016;
    }}
    .mini-content {{ padding: 10px; overflow: auto; }}
    .world-id {{
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      max-width: 44vw;
    }}
    .tab-panel {{ display: none; }}
    .tab-panel.active {{ display: block; }}
    .mujoco-layout {{
      display: grid;
      grid-template-columns: minmax(0, 1.25fr) minmax(280px, 0.75fr);
      gap: 12px;
      min-height: 0;
    }}
    .mujoco-stage {{
      min-height: 360px;
      border: 1px solid var(--line);
      border-radius: 8px;
      overflow: hidden;
      background:
        linear-gradient(rgba(0, 201, 142, 0.06) 1px, transparent 1px),
        linear-gradient(90deg, rgba(0, 201, 142, 0.06) 1px, transparent 1px),
        #05080b;
      background-size: 28px 28px;
    }}
    #mujoco-canvas {{
      display: block;
      width: 100%;
      height: 360px;
    }}
    .telemetry {{
      display: grid;
      gap: 8px;
      margin: 0;
    }}
    .telemetry div {{
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 10px;
      padding: 8px 0;
      border-bottom: 1px solid var(--line);
    }}
    .telemetry dt {{ color: var(--muted); }}
    .telemetry dd {{ margin: 0; color: #ffffff; font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }}
    .mujoco-controls {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-top: 12px;
    }}
    .mujoco-controls button {{
      min-height: 30px;
      border: 1px solid var(--line-strong);
      border-radius: 6px;
      padding: 4px 9px;
      background: #101820;
      color: var(--ink);
      font-weight: 800;
      cursor: pointer;
      text-transform: uppercase;
    }}
    .mujoco-controls button.active {{
      border-color: var(--accent);
      color: var(--accent);
    }}
    .sim-error {{
      display: grid;
      place-items: center;
      min-height: 260px;
      border: 1px solid var(--line);
      border-radius: 8px;
      color: var(--warn);
      text-align: center;
      padding: 20px;
    }}
    @media (max-width: 1080px) {{
      .app {{
        grid-template-columns: 1fr;
        grid-template-areas:
          "topbar"
          "workbench";
      }}
      .rail {{ display: none; }}
      main {{
        grid-template-columns: 1fr;
        grid-template-rows: auto auto auto auto;
        grid-template-areas:
          "metrics"
          "state"
          "inspector"
          "timeline"
          "code";
        overflow: visible;
      }}
      .metrics {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
      .split {{ grid-template-columns: 1fr; }}
      .mujoco-layout {{ grid-template-columns: 1fr; }}
      .world-id {{ max-width: 100%; white-space: normal; }}
    }}
  </style>
</head>
<body>
<div class="app">
  {render_sidebar(snapshot, output_dir)}
  <header>
    <div>
      <h1>{title}</h1>
      <div class="subtle">Captured {html.escape(snapshot["captured_at"])}.
      Raw snapshot: <a href="{latest_json}">{latest_json}</a></div>
    </div>
    <div class="top-meta">
      <span class="chip strong">{status}</span>
      <span class="chip world-id">world {html.escape(str(snapshot["world"].get("world_id", "")))}</span>
    </div>
  </header>
  <main>
    <section class="metrics">
      <div class="panel metric"><span>completed tick</span><strong>{snapshot["completed_tick"]}</strong></div>
      <div class="panel metric"><span>world tick</span><strong>{snapshot["world"].get("tick", "")}</strong></div>
      <div class="panel metric"><span>delivered</span><strong>{snapshot["mailbox"]["delivered"]}</strong></div>
      <div class="panel metric"><span>pending</span><strong>{snapshot["mailbox"]["pending"]}</strong></div>
    </section>

    <section class="panel state" id="state">
      <div class="panel-header">
        <div class="panel-title">
          <h2>Entity State</h2>
          <div class="tabs" role="tablist" aria-label="State views">
            <button class="tab-button active" type="button" data-tab="entities">Entities</button>
            <button class="tab-button" type="button" data-tab="mujoco">MuJoCo</button>
          </div>
        </div>
        <span class="chip">{len(snapshot["entities"])} rows / mujoco {mujoco_state}</span>
      </div>
      <div class="panel-body tab-panel active" data-panel="entities">
        <table>
          <thead><tr><th>entity</th><th>name</th><th>mood</th><th>energy</th><th>messages</th><th>latest message</th></tr></thead>
          <tbody>{render_rows(snapshot)}</tbody>
        </table>
      </div>
      <div class="panel-body tab-panel" data-panel="mujoco">
        <div id="mujoco-root"></div>
      </div>
    </section>

    <section class="panel inspector">
      <div class="panel-header">
        <h2>Inspector</h2>
        <span class="chip">tick {snapshot["completed_tick"]}</span>
      </div>
      <div class="panel-body stack">
        <div class="mini">
          <h3>Tick Diff</h3>
          <div class="mini-content">
            <table>
              <thead><tr><th>agent</th><th>mood</th><th>energy</th><th>new</th><th>total</th></tr></thead>
              <tbody>{render_diff(snapshot)}</tbody>
            </table>
          </div>
        </div>
        <div class="mini">
          <h3>Mailbox Pending Preview</h3>
          <div class="mini-content"><pre><code>{pending_preview}</code></pre></div>
        </div>
        <div class="split">
          <div class="mini" id="runtime">
            <h3>Processors</h3>
            <div class="mini-content">
              <table>
                <thead><tr><th>qualname</th><th>priority</th><th>components</th></tr></thead>
                <tbody>{render_key_values(snapshot["processors"], ["qualname", "priority", "components"])}</tbody>
              </table>
            </div>
          </div>
          <div class="mini">
            <h3>Resources</h3>
            <div class="mini-content">
              <table>
                <thead><tr><th>qualname</th></tr></thead>
                <tbody>{render_key_values(snapshot["resources"], ["qualname"])}</tbody>
              </table>
            </div>
          </div>
        </div>
      </div>
    </section>

    <section class="panel timeline" id="timeline">
      <div class="panel-header">
        <h2>Timeline And Audit</h2>
        <span class="chip">{len(snapshot["metrics"])} events</span>
      </div>
      <div class="panel-body split">
        <div class="mini">
          <h3>Runtime Events</h3>
          <div class="mini-content">
            <table>
              <thead><tr><th>phase</th><th>tick</th><th>message</th></tr></thead>
              <tbody>{render_key_values(snapshot["metrics"], ["phase", "tick", "message"])}</tbody>
            </table>
          </div>
        </div>
        <div class="mini" id="audit">
          <h3>Audit Tail</h3>
          <div class="mini-content">
            <table>
              <thead><tr><th>command</th><th>status</th><th>payload</th></tr></thead>
              <tbody>{render_key_values(snapshot["audit"], ["command_type", "status", "payload_json"])}</tbody>
            </table>
          </div>
        </div>
      </div>
    </section>

    <section class="panel code-scope" id="scope">
      <div class="panel-header">
        <h2>Code Scope</h2>
        <span class="chip">{len(snapshot["code_scope"])} symbols</span>
      </div>
      <div class="panel-body">
        <div class="mini">
          <h3>Hooks</h3>
          <div class="mini-content">
            <table>
              <thead><tr><th>event</th><th>handler</th><th>mode</th></tr></thead>
              <tbody>{render_key_values(snapshot["hooks"], ["event_type", "handler_qualname", "mode"])}</tbody>
            </table>
          </div>
        </div>
        {render_code(snapshot["code_scope"])}
      </div>
    </section>
  </main>
</div>
<script>
  const mujocoDemo = {mujoco_payload};

  document.querySelectorAll(".tab-button").forEach((button) => {{
    button.addEventListener("click", () => {{
      const target = button.dataset.tab;
      document.querySelectorAll(".tab-button").forEach((item) => {{
        item.classList.toggle("active", item === button);
      }});
      document.querySelectorAll(".tab-panel").forEach((panel) => {{
        panel.classList.toggle("active", panel.dataset.panel === target);
      }});
      if (target === "mujoco") renderMujoco();
    }});
  }});

  let mujocoRendered = false;
  let mujocoEnv = 0;
  let mujocoTick = 0;
  let mujocoPlaying = true;
  let mujocoLastFrame = 0;

  function renderMujoco() {{
    if (mujocoRendered) return;
    mujocoRendered = true;
    const root = document.getElementById("mujoco-root");
    if (!mujocoDemo.available) {{
      root.innerHTML = `<div class="sim-error">MuJoCo unavailable<br><code>${{escapeHtml(mujocoDemo.error || "missing dependency")}}</code></div>`;
      return;
    }}

    root.innerHTML = `
      <div class="mujoco-layout">
        <div class="mujoco-stage"><canvas id="mujoco-canvas"></canvas></div>
        <div class="mini">
          <h3>Raw MuJoCo Cartpole</h3>
          <div class="mini-content">
            <dl class="telemetry">
              <div><dt>model</dt><dd>${{escapeHtml(mujocoDemo.model)}}</dd></div>
              <div><dt>tick</dt><dd id="mj-tick">0</dd></div>
              <div><dt>cart_pos</dt><dd id="mj-cart">0.0000</dd></div>
              <div><dt>pole_angle</dt><dd id="mj-angle">0.0000</dd></div>
              <div><dt>cart_vel</dt><dd id="mj-cart-vel">0.0000</dd></div>
              <div><dt>pole_vel</dt><dd id="mj-pole-vel">0.0000</dd></div>
              <div><dt>substeps</dt><dd>${{mujocoDemo.substeps}}</dd></div>
            </dl>
            <div class="mujoco-controls" id="mujoco-envs"></div>
            <div class="mujoco-controls">
              <button type="button" id="mj-toggle">Pause</button>
              <button type="button" id="mj-reset">Reset</button>
            </div>
          </div>
        </div>
      </div>`;

    const envs = document.getElementById("mujoco-envs");
    mujocoDemo.trajectories.forEach((trajectory, index) => {{
      const button = document.createElement("button");
      button.type = "button";
      button.textContent = trajectory.name;
      button.className = index === mujocoEnv ? "active" : "";
      button.addEventListener("click", () => {{
        mujocoEnv = index;
        mujocoTick = 0;
        document.querySelectorAll("#mujoco-envs button").forEach((item, itemIndex) => {{
          item.classList.toggle("active", itemIndex === index);
        }});
        drawMujoco();
      }});
      envs.appendChild(button);
    }});

    document.getElementById("mj-toggle").addEventListener("click", (event) => {{
      mujocoPlaying = !mujocoPlaying;
      event.currentTarget.textContent = mujocoPlaying ? "Pause" : "Play";
    }});
    document.getElementById("mj-reset").addEventListener("click", () => {{
      mujocoTick = 0;
      drawMujoco();
    }});
    requestAnimationFrame(animateMujoco);
  }}

  function animateMujoco(timestamp) {{
    if (!mujocoRendered || !mujocoDemo.available) return;
    if (mujocoPlaying && timestamp - mujocoLastFrame > 70) {{
      mujocoLastFrame = timestamp;
      const states = mujocoDemo.trajectories[mujocoEnv].states;
      mujocoTick = (mujocoTick + 1) % states.length;
      drawMujoco();
    }}
    requestAnimationFrame(animateMujoco);
  }}

  function drawMujoco() {{
    const canvas = document.getElementById("mujoco-canvas");
    if (!canvas) return;
    const rect = canvas.getBoundingClientRect();
    const scale = window.devicePixelRatio || 1;
    canvas.width = Math.max(1, Math.floor(rect.width * scale));
    canvas.height = Math.max(1, Math.floor(rect.height * scale));
    const ctx = canvas.getContext("2d");
    ctx.setTransform(scale, 0, 0, scale, 0, 0);
    const width = rect.width;
    const height = rect.height;
    ctx.clearRect(0, 0, width, height);

    const state = mujocoDemo.trajectories[mujocoEnv].states[mujocoTick];
    const trackY = height * 0.68;
    const originX = width / 2;
    const cartX = originX + state.cart_pos * 280;
    const cartW = 78;
    const cartH = 34;
    const poleLength = 170;
    const poleX = cartX + Math.sin(state.pole_angle) * poleLength;
    const poleY = trackY - cartH / 2 - Math.cos(state.pole_angle) * poleLength;

    ctx.strokeStyle = "rgba(130, 144, 155, 0.45)";
    ctx.lineWidth = 2;
    ctx.beginPath();
    ctx.moveTo(30, trackY + 24);
    ctx.lineTo(width - 30, trackY + 24);
    ctx.stroke();

    ctx.fillStyle = "rgba(0, 201, 142, 0.14)";
    ctx.fillRect(30, trackY + 28, width - 60, 3);

    ctx.fillStyle = "#18242d";
    ctx.strokeStyle = "#00c98e";
    ctx.lineWidth = 2;
    roundRect(ctx, cartX - cartW / 2, trackY - cartH, cartW, cartH, 7);
    ctx.fill();
    ctx.stroke();

    ctx.strokeStyle = "#ffb020";
    ctx.lineWidth = 8;
    ctx.lineCap = "round";
    ctx.beginPath();
    ctx.moveTo(cartX, trackY - cartH);
    ctx.lineTo(poleX, poleY);
    ctx.stroke();

    ctx.fillStyle = "#ff674d";
    ctx.beginPath();
    ctx.arc(poleX, poleY, 8, 0, Math.PI * 2);
    ctx.fill();

    ctx.fillStyle = "#c7d1d9";
    ctx.font = "12px ui-monospace, SFMono-Regular, Menlo, Consolas, monospace";
    ctx.fillText(`env=${{mujocoDemo.trajectories[mujocoEnv].name}}`, 18, 24);
    ctx.fillText(`tick=${{state.tick}}`, 18, 42);

    updateMujocoTelemetry(state);
  }}

  function updateMujocoTelemetry(state) {{
    document.getElementById("mj-tick").textContent = state.tick;
    document.getElementById("mj-cart").textContent = state.cart_pos.toFixed(5);
    document.getElementById("mj-angle").textContent = state.pole_angle.toFixed(5);
    document.getElementById("mj-cart-vel").textContent = state.cart_vel.toFixed(5);
    document.getElementById("mj-pole-vel").textContent = state.pole_vel.toFixed(5);
  }}

  function roundRect(ctx, x, y, width, height, radius) {{
    ctx.beginPath();
    ctx.moveTo(x + radius, y);
    ctx.lineTo(x + width - radius, y);
    ctx.quadraticCurveTo(x + width, y, x + width, y + radius);
    ctx.lineTo(x + width, y + height - radius);
    ctx.quadraticCurveTo(x + width, y + height, x + width - radius, y + height);
    ctx.lineTo(x + radius, y + height);
    ctx.quadraticCurveTo(x, y + height, x, y + height - radius);
    ctx.lineTo(x, y + radius);
    ctx.quadraticCurveTo(x, y, x + radius, y);
    ctx.closePath();
  }}

  function escapeHtml(value) {{
    const node = document.createElement("div");
    node.textContent = String(value);
    return node.innerHTML;
  }}
</script>
</body>
</html>
"""


def publish_dashboard(snapshot: dict[str, Any], output_dir: Path, *, running: bool) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    snapshots_dir = output_dir / "snapshots"
    write_json(snapshots_dir / f"tick-{snapshot['completed_tick']:03d}.json", snapshot)
    write_json(output_dir / "latest.json", snapshot)
    (output_dir / "index.html").write_text(render_html(snapshot, output_dir, running=running))


async def main() -> None:
    args = parse_args()
    output_dir = args.output
    metrics: list[TickMetric] = []
    started_at = time.perf_counter()
    mailbox = Mailbox()
    scopes = code_scope(
        AgentState,
        Inbox,
        MessageRealizationProcessor,
        GreetingProcessor,
        MoodProcessor,
    )

    def note(phase: str, tick: int, message: str) -> None:
        metrics.append(
            TickMetric(
                phase=phase,
                tick=tick,
                message=message,
                at_ms=round((time.perf_counter() - started_at) * 1000, 2),
            )
        )

    async def on_pre_tick(event: PreTick) -> None:
        note("pre", event.tick, "tick execution started")

    async def on_post_tick(event: PostTick) -> None:
        note("post", event.tick - 1, f"{len(event.results)} archetype frame(s) persisted")

    storage = StorageConfig(uri="./archetype_data", namespace="live_agent_inspector")

    async with ArchetypeRuntime() as runtime:
        world = runtime.world(
            "live-agent-inspector",
            storage=storage,
            processors=[
                MessageRealizationProcessor(),
                GreetingProcessor(),
                MoodProcessor(),
            ],
            resources=[SimConfig(), mailbox],
            hooks=[(PreTick, on_pre_tick), (PostTick, on_post_tick)],
        )

        for name in ("Ada", "Rex", "Iris"):
            await world.spawn(AgentState(name=name), Inbox())

        dashboard = output_dir / "index.html"
        if not args.no_open:
            webbrowser.open(dashboard.resolve().as_uri())

        previous_rows: list[dict[str, Any]] = []
        last_snapshot: dict[str, Any] | None = None
        for _ in range(args.ticks):
            await world.step()
            info = await world.info()
            completed_tick = int(info.tick) - 1
            snapshot = await collect_snapshot(
                world=world,
                mailbox=mailbox,
                completed_tick=completed_tick,
                previous_rows=previous_rows,
                metrics=metrics,
                scopes=scopes,
            )
            publish_dashboard(snapshot, output_dir, running=True)
            previous_rows = snapshot["entities"]
            last_snapshot = snapshot
            print(
                f"tick {completed_tick}: "
                f"{len(previous_rows)} agents, "
                f"{mailbox.delivered} delivered, "
                f"{len(mailbox.pending)} pending -> {dashboard}"
            )

            if args.break_at_tick == completed_tick:
                print("Debugger scope: runtime, world, mailbox, snapshot")
                breakpoint()

            if args.delay > 0:
                await asyncio.sleep(args.delay)

        if last_snapshot is not None:
            publish_dashboard(last_snapshot, output_dir, running=False)

    print(f"\nDashboard: {dashboard.resolve().as_uri()}")
    print(f"Snapshots:  {output_dir.resolve() / 'snapshots'}")


if __name__ == "__main__":
    asyncio.run(main())
