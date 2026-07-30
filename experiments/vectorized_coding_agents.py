# /// script
# requires-python = ">=3.12"
# dependencies = ["daft[openai]==0.7.19", "pydantic-monty==0.0.18"]
# ///
# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Minimal Daft-vectorized coding agents with Monty code mode."""
# fmt: off

import argparse
import ast
import json
import os
import time
from pathlib import Path

import daft
import pydantic_monty as monty
from daft import Series, col
from daft.ai.openai.provider import OpenAIProvider
from daft.functions import file, prompt
from pydantic import BaseModel

SYSTEM = """You are one coding agent. app.py is attached. Return its complete
replacement content with the requested bug fixed. Make the smallest valid edit.
Set done=true only when the replacement completes the task."""
class Edit(BaseModel):
    content: str
    done: bool

BUGS = (
    ("def normalize(text):\n    return text.lower()\n", "Fix normalize() to strip surrounding whitespace before lowercasing.", "assert normalize('  HeLLo \\n') == 'hello'", "def normalize(text):\n    return text.strip().lower()\n"),
    ("def safe_ratio(total, count):\n    return total / count\n", "Fix safe_ratio() to return 0.0 for a zero count without changing normal division.", "assert safe_ratio(3, 0) == 0.0\nassert safe_ratio(6, 2) == 3.0", "def safe_ratio(total, count):\n    return 0.0 if count == 0 else total / count\n"),
    ("def unique(values):\n    return list(set(values))\n", "Fix unique() to remove duplicates while preserving first-seen order.", "assert unique([3, 1, 3, 2]) == [3, 1, 2]", "def unique(values):\n    return list(dict.fromkeys(values))\n"),
)
def parse_action(raw: object) -> tuple[str, bool]:
    action = json.loads(raw) if isinstance(raw, str) else raw
    return str(action["content"]), bool(action["done"])


@daft.func(return_dtype=daft.DataType.string())
def mock_action(state_json: str) -> str:
    state = json.loads(state_json)
    return json.dumps({"content": state["solution"], "done": True})


@daft.func.batch(
    return_dtype=daft.DataType.string(),
    use_process=True,
    batch_size=1024,
    max_retries=0,
)
def run_tool_batch(states: Series, actions: Series) -> Series:
    state_rows, raw_actions = states.to_pylist(), actions.to_pylist()
    advanced = []
    for state_json, raw in zip(state_rows, raw_actions, strict=True):
        state = json.loads(state_json)
        try:
            content, requested_done = parse_action(raw)
            code = f"from pathlib import Path\nPath('/workspace/app.py').write_text({content!r})"
            stdout = monty.CollectString()
            repl = monty.MontyRepl(
                limits={"max_duration_secs": 2, "max_memory": 64_000_000})
            result = repl.feed_run(
                code,
                mount=monty.MountDir(
                    "/workspace", state["workspace"],
                    mode="read-write", write_bytes_limit=1_000_000,
                ),
                print_callback=stdout,
            )
            result = repl.feed_run(content + "\n" + state["mre"])
            observation = {"action": str(raw)[-4000:], "stdout": stdout.output[-4000:], "result": repr(result)}
            state["status"] = "done" if requested_done else "active"
        except Exception as exc:
            observation = {"action": str(raw)[-4000:], "error": f"{type(exc).__name__}: {exc}"}
            state["status"] = "active"
        state["history"] = [*state["history"][-2:], observation]
        state["tool_batch"] = len(state_rows)
        state["turn"] += 1
        advanced.append(json.dumps(state))
    return Series.from_pylist(advanced)


def make_states(count: int, root: Path) -> list[dict]:
    states = []
    for index in range(count):
        source_text, task, mre, solution = BUGS[index % len(BUGS)]
        workspace = root / f"agent-{index:04d}"
        workspace.mkdir(parents=True)
        source = workspace / "app.py"
        source.write_text(source_text)
        states.append({
            "id": f"agent-{index:04d}",
            "task": task,
            "mre": mre,
            "solution": solution,
            "workspace": str(workspace),
            "path": str(source),
            "turn": 0,
            "status": "active",
            "history": [],
        })
    return states


def agent_prompt(state: dict) -> str:
    return (
        f"Agent: {state['id']}\nTask: {state['task']}\nMRE: {state['mre']}\nTurn: {state['turn']}\n"
        f"Prior observations: {json.dumps(state['history'])}\nExecute the edit now. A read-only action fails this mission."
    )


def verified(state: dict) -> bool:
    source = (Path(state["workspace"]) / "app.py").read_text()
    try:
        ast.parse(source)
        monty.MontyRepl(limits={"max_duration_secs": 2}).feed_run(source + "\n" + state["mre"])
        return True
    except Exception:
        return False


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--agents", type=int, default=32)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--mock", action="store_true")
    parser.add_argument("--base-url", default=os.getenv("LLM_BASE_URL"))
    parser.add_argument("--model", default=os.getenv("LLM_MODEL"))
    parser.add_argument("--root", type=Path, default=Path(".context/vectorized-proof"))
    args = parser.parse_args()
    if len(Path(__file__).read_text().splitlines()) >= 200:
        raise SystemExit("proof must remain under 200 lines")
    if not args.mock and (not args.base_url or not args.model):
        raise SystemExit("set LLM_BASE_URL, LLM_MODEL, and optionally LLM_API_KEY")

    run_root = args.root / f"run-{time.time_ns()}"
    run_root.mkdir(parents=True)
    states = make_states(args.agents, run_root)
    provider = None if args.mock else OpenAIProvider(
        name="proof", base_url=args.base_url,
        api_key=os.getenv("LLM_API_KEY", "local"), timeout=120, max_retries=0,
    )
    totals = {"inference_s": 0.0, "tools_s": 0.0}
    for frontier in range(args.rounds):
        active = [state for state in states if state["status"] == "active"]
        if not active:
            break
        frame = daft.from_pylist([{
            "state": json.dumps(state),
            "message": agent_prompt(state),
            "path": state["path"],
        } for state in active])
        action = (
            mock_action(col("state"))
            if args.mock
            else prompt(
                [col("message"), file(col("path"))],
                return_format=Edit,
                system_message=SYSTEM, provider=provider, model=args.model,
                use_chat_completions=True,
                reasoning_effort="none", temperature=0, max_tokens=220,
            )
        )
        started = time.perf_counter()
        inferred = frame.with_column("action", action).collect()
        inference_s = time.perf_counter() - started
        started = time.perf_counter()
        stepped = inferred.with_column("next_state", run_tool_batch(
            col("state"), col("action")
        )).select("next_state").collect()
        tools_s = time.perf_counter() - started
        rows = (json.loads(item["next_state"]) for item in stepped.to_pylist())
        replacements = {row["id"]: row for row in rows}
        states = [replacements.get(state["id"], state) for state in states]
        (run_root / "states.json").write_text(json.dumps(states))
        totals["inference_s"] += inference_s
        totals["tools_s"] += tools_s
        print(json.dumps({
            "frontier": frontier, "agents": len(active),
            "inference_s": round(inference_s, 3),
            "inference_agents_s": round(len(active) / inference_s, 1),
            "tools_s": round(tools_s, 3),
            "tool_agents_s": round(len(active) / tools_s, 1),
        }))
    passed = sum(verified(state) for state in states)
    print(json.dumps({
        "agents": len(states), "verified": passed,
        "wall_s": round(sum(totals.values()), 3),
        "tool_batch": max(state.get("tool_batch", 0) for state in states),
        "workspaces": str(run_root),
        "source_lines": len(Path(__file__).read_text().splitlines()),
    }))
    raise SystemExit(0 if passed == len(states) else 1)


if __name__ == "__main__":
    main()
