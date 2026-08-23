# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "daft[openai]==0.7.19",
#   "pydantic>=2.11",
#   "pydantic-monty==0.0.18",
# ]
# ///
# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""One Monty coding-agent turn per Daft row.

This is deliberately *not* ``pydantic_ai.Agent.run()``. ``Agent.run()`` owns
its model loop, while this experiment needs Daft to own the inference frontier
so every active agent reaches one OpenAI-compatible OpenRouter endpoint
together.
Pydantic still owns the row/action contracts, and Monty owns the stateful
Python execution session for each row.

Each frontier has this shape:

    serialized agent rows
        -> daft.functions.prompt (one model turn per active row)
        -> restore each row's Monty snapshot
        -> execute one Python action in its worktree mount
        -> serialize the next frontier
        -> collect the frontier

Git is intentionally outside Monty. The host provisions detached worktrees
before mounting them, records read-only Git evidence after each action, and
leaves real-repository worktrees in place by default for inspection.

Offline demonstration:

    uv run --script experiments/daft_monty_agent_frontier.py --demo --dry-run

OpenRouter endpoint:

    export OPENROUTER_API_KEY=sk-or-v1-...
    export OPENROUTER_MODEL=anthropic/claude-sonnet-5

    uv run --script experiments/daft_monty_agent_frontier.py \
      --repo . \
      --task "Add a short architecture note to docs/agent-note.md" \
      --task "Inspect README.md and add a concise quickstart clarification" \
      --rounds 8

Use ``--cleanup`` only when the printed patches are disposable.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import subprocess
import tempfile
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import daft
import pydantic_monty
from daft import col
from daft.ai.openai.provider import OpenAIProvider
from daft.functions import prompt
from pydantic import BaseModel, ConfigDict, Field

VIRTUAL_WORKSPACE = "/workspace"
SYSTEM_PROMPT = """\
You are a Python-only coding agent operating in a restricted Monty interpreter.
The repository is mounted read-write at /workspace.

You may use supported pure Python and pathlib to inspect and edit files. You
cannot use subprocess, git, pytest, uv, shell commands, networking, environment
variables, or import modules from the mounted repository. Print observations
that should be available to your next turn.

Return exactly one JSON object and no markdown:
{"summary": "short rationale", "code": "Python source for this turn", "done": false}

Make one bounded step per turn. Inspect before editing. Set done=true only when
the requested file changes are complete. An empty code string is allowed when
done=true.
"""


class AgentAction(BaseModel):
    """The sole model-visible action protocol."""

    model_config = ConfigDict(extra="forbid")

    summary: str
    code: str = ""
    done: bool = False


class AgentState(BaseModel):
    """All state needed to resume one logical agent on another frontier."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    mission_id: str
    task: str
    base_sha: str
    workspace_path: str
    turn: int = 0
    status: Literal["active", "done", "failed", "max_turns"] = "active"
    transcript: list[dict[str, Any]] = Field(default_factory=list)
    monty_snapshot_b64: str | None = None

    def prompt_text(self) -> str:
        recent = json.dumps(self.transcript[-6:], ensure_ascii=False)
        if len(recent) > 16_000:
            recent = recent[-16_000:]
        return (
            f"Mission: {self.mission_id}\n"
            f"Task: {self.task}\n"
            f"Turn: {self.turn}\n"
            f"Recent action/observation transcript:\n{recent}\n\n"
            "Choose the next bounded Python action."
        )


@dataclass(frozen=True)
class WorktreeLease:
    mission_id: str
    path: Path
    base_sha: str


@dataclass(frozen=True)
class EndpointConfig:
    base_url: str
    model: str
    api_key: str

    @classmethod
    def from_env(cls) -> EndpointConfig:
        api_key = os.getenv("OPENROUTER_API_KEY")
        model = os.getenv("OPENROUTER_MODEL")
        if not api_key or not model:
            raise SystemExit(
                "OPENROUTER_API_KEY and OPENROUTER_MODEL are required unless --dry-run is used"
            )
        return cls(
            base_url=os.getenv(
                "OPENROUTER_BASE_URL",
                "https://openrouter.ai/api/v1",
            ).rstrip("/"),
            model=model,
            api_key=api_key,
        )

    def provider(self) -> OpenAIProvider:
        return OpenAIProvider(
            name="openrouter",
            base_url=self.base_url,
            api_key=self.api_key,
            default_headers={
                "HTTP-Referer": "https://github.com/Vangelis-Technologies/archetype",
                "X-OpenRouter-Title": "Archetype Daft Monty Frontier",
            },
            timeout=300.0,
            max_retries=2,
        )


class WorktreeBroker:
    """Minimal host-owned Git wrapper for this experiment."""

    def __init__(self, repository: Path, root: Path) -> None:
        self.repository = Path(
            self._git(repository, "rev-parse", "--show-toplevel").strip()
        ).resolve()
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self._leases: list[WorktreeLease] = []

    def provision(self, mission_id: str, base: str) -> WorktreeLease:
        base_sha = self._git(self.repository, "rev-parse", f"{base}^{{commit}}").strip()
        path = self.root / mission_id
        if path.exists():
            raise RuntimeError(f"refusing to reuse existing worktree path: {path}")
        self._git(
            self.repository,
            "worktree",
            "add",
            "--detach",
            str(path),
            base_sha,
        )
        lease = WorktreeLease(
            mission_id=mission_id,
            path=path.resolve(),
            base_sha=base_sha,
        )
        self._leases.append(lease)
        return lease

    def evidence(self, lease: WorktreeLease) -> dict[str, str]:
        return {
            "status": self._git(lease.path, "status", "--short"),
            "diff_stat": self._git(lease.path, "diff", "--stat"),
        }

    def patch(self, lease: WorktreeLease) -> str:
        chunks = [self._git(lease.path, "diff", "--no-ext-diff", "--binary")]
        untracked = self._git(
            lease.path,
            "ls-files",
            "--others",
            "--exclude-standard",
        ).splitlines()
        for relative_path in untracked:
            completed = subprocess.run(
                [
                    "git",
                    "-C",
                    str(lease.path),
                    "diff",
                    "--no-index",
                    "--binary",
                    "--",
                    "/dev/null",
                    relative_path,
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            if completed.returncode not in {0, 1}:
                raise RuntimeError(completed.stderr)
            chunks.append(completed.stdout)
        return "".join(chunks)

    @property
    def has_leases(self) -> bool:
        return bool(self._leases)

    def cleanup(self) -> None:
        for lease in reversed(self._leases):
            if lease.path.exists():
                self._git(
                    self.repository,
                    "worktree",
                    "remove",
                    "--force",
                    str(lease.path),
                )
        self._git(self.repository, "worktree", "prune")

    @staticmethod
    def _git(cwd: Path, *args: str) -> str:
        completed = subprocess.run(
            ["git", "-C", str(cwd), *args],
            check=True,
            capture_output=True,
            text=True,
        )
        return completed.stdout


@daft.func(return_dtype=daft.DataType.string())
def _deterministic_action(mission_id: str, task: str, turn: int) -> str:
    """Offline policy that exercises the same Daft frontier without an LLM."""

    if turn == 0:
        code = """\
from pathlib import Path
root = Path("/workspace")
files = sorted(path.name for path in root.iterdir())
readme = root / "README.md"
readme_preview = readme.read_text()[:500] if readme.exists() else "<no README>"
print({"files": files, "readme_preview": readme_preview})
"""
        action = AgentAction(
            summary="Inspect the mounted worktree before editing.",
            code=code,
        )
    elif turn == 1:
        code = (
            "from pathlib import Path\n"
            f"task = {task!r}\n"
            f"mission_id = {mission_id!r}\n"
            "target = Path('/workspace/monty-agent-result.txt')\n"
            "target.write_text("
            "'mission: ' + mission_id + '\\n'"
            " + 'task: ' + task + '\\n'"
            " + 'top-level entries observed: ' + str(len(files)) + '\\n'"
            ")\n"
            "print({'wrote': str(target), 'bytes': len(target.read_bytes())})\n"
        )
        action = AgentAction(
            summary="Write a deterministic result using state from the prior Monty turn.",
            code=code,
        )
    else:
        action = AgentAction(
            summary="The requested demonstration edit is complete.",
            done=True,
        )
    return action.model_dump_json()


def infer_frontier(
    states: list[AgentState],
    *,
    endpoint: EndpointConfig | None,
) -> list[AgentState]:
    """Materialize one complete Daft agent-turn frontier."""

    rows = [
        {
            "mission_id": state.mission_id,
            "task": state.task,
            "turn": state.turn,
            "prompt_text": state.prompt_text(),
            "state_json": state.model_dump_json(),
        }
        for state in states
        if state.status == "active"
    ]
    if not rows:
        return []

    frontier = daft.from_pylist(rows)
    if endpoint is None:
        action_expression = _deterministic_action(
            col("mission_id"),
            col("task"),
            col("turn"),
        )
    else:
        action_expression = prompt(
            col("prompt_text"),
            system_message=SYSTEM_PROMPT,
            provider=endpoint.provider(),
            model=endpoint.model,
            # OpenRouter exposes the OpenAI Chat Completions API.
            use_chat_completions=True,
            temperature=0.2,
            max_tokens=1_200,
        )

    advanced = (
        frontier.with_column("raw_action", action_expression)
        .with_column(
            "next_state_json",
            _apply_monty_row(col("state_json"), col("raw_action")),
        )
        .select("next_state_json")
        .collect()
        .to_pylist()
    )
    return [AgentState.model_validate_json(row["next_state_json"]) for row in advanced]


def apply_monty_turn(
    state: AgentState,
    raw_action: str,
) -> AgentState:
    """Restore one row's interpreter, apply one action, and return its next state."""

    try:
        action = _parse_action(raw_action)
    except Exception as exc:
        return state.model_copy(
            update={
                "status": "failed",
                "transcript": [
                    *state.transcript,
                    {
                        "turn": state.turn,
                        "raw_action": raw_action,
                        "protocol_error": f"{type(exc).__name__}: {exc}",
                    },
                ],
            }
        )

    if state.monty_snapshot_b64 is None:
        session = pydantic_monty.MontyRepl(
            limits={
                "max_duration_secs": 2.0,
                "max_memory": 64_000_000,
                "max_recursion_depth": 200,
            }
        )
    else:
        snapshot = base64.b64decode(state.monty_snapshot_b64)
        session = pydantic_monty.MontyRepl.load(snapshot)

    mount = pydantic_monty.MountDir(
        virtual_path=VIRTUAL_WORKSPACE,
        host_path=state.workspace_path,
        mode="read-write",
        write_bytes_limit=2_000_000,
    )
    stdout = pydantic_monty.CollectString()
    observation: dict[str, Any]
    try:
        result = (
            session.feed_run(
                action.code,
                mount=mount,
                print_callback=stdout,
            )
            if action.code.strip()
            else None
        )
        observation = {
            "stdout": stdout.output[-8_000:],
            "result": repr(result)[:2_000],
            "git": _workspace_git_evidence(Path(state.workspace_path)),
        }
        status: Literal["active", "done", "failed", "max_turns"] = (
            "done" if action.done else "active"
        )
    except Exception as exc:
        observation = {
            "stdout": stdout.output[-8_000:],
            "error": f"{type(exc).__name__}: {exc}",
            "git": _workspace_git_evidence(Path(state.workspace_path)),
        }
        # A failed code action is an observation for the next model turn, not
        # automatically a terminal mission failure.
        status = "active"

    snapshot_b64 = base64.b64encode(session.dump()).decode("ascii")
    transcript = [
        *state.transcript,
        {
            "turn": state.turn,
            "action": action.model_dump(),
            "observation": observation,
        },
    ]
    return state.model_copy(
        update={
            "turn": state.turn + 1,
            "status": status,
            "transcript": transcript,
            "monty_snapshot_b64": snapshot_b64,
        }
    )


@daft.func(
    return_dtype=daft.DataType.string(),
    use_process=True,
    max_retries=0,
)
def _apply_monty_row(state_json: str, raw_action: str) -> str:
    """Execute the stateful, filesystem-mutating half of one Daft row."""

    state = AgentState.model_validate_json(state_json)
    return apply_monty_turn(state, raw_action).model_dump_json()


def _workspace_git_evidence(workspace: Path) -> dict[str, str]:
    return {
        "status": WorktreeBroker._git(workspace, "status", "--short"),
        "diff_stat": WorktreeBroker._git(workspace, "diff", "--stat"),
    }


def run_frontiers(
    states: list[AgentState],
    *,
    endpoint: EndpointConfig | None,
    rounds: int,
) -> list[AgentState]:
    for frontier_number in range(rounds):
        active = [state for state in states if state.status == "active"]
        if not active:
            break
        print(
            json.dumps(
                {
                    "frontier": frontier_number,
                    "active_agents": len(active),
                    "mission_ids": [state.mission_id for state in active],
                }
            )
        )
        advanced = infer_frontier(active, endpoint=endpoint)
        replacements = {state.mission_id: state for state in advanced}
        states = [replacements.get(state.mission_id, state) for state in states]

    return [
        state.model_copy(update={"status": "max_turns"}) if state.status == "active" else state
        for state in states
    ]


def _parse_action(raw: str) -> AgentAction:
    text = raw.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        text = "\n".join(lines[1:-1]).strip()
        if text.startswith("json"):
            text = text[4:].lstrip()
    try:
        return AgentAction.model_validate_json(text)
    except Exception:
        start = text.find("{")
        end = text.rfind("}")
        if start < 0 or end <= start:
            raise ValueError("model response does not contain a JSON object") from None
        return AgentAction.model_validate_json(text[start : end + 1])


def _create_demo_repository(root: Path) -> Path:
    repository = root / "seed-repository"
    repository.mkdir()
    (repository / "README.md").write_text(
        "# Monty frontier demo\n\nThis repository is edited by isolated agents.\n"
    )
    (repository / "notes.txt").write_text("base\n")
    subprocess.run(["git", "init", str(repository)], check=True, capture_output=True)
    subprocess.run(
        ["git", "-C", str(repository), "add", "."],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        [
            "git",
            "-C",
            str(repository),
            "-c",
            "user.name=Monty Spike",
            "-c",
            "user.email=monty-spike@example.invalid",
            "commit",
            "-m",
            "seed",
        ],
        check=True,
        capture_output=True,
    )
    return repository


def _states_for(
    tasks: Iterable[str],
    broker: WorktreeBroker,
    *,
    base: str,
) -> tuple[list[AgentState], dict[str, WorktreeLease]]:
    states: list[AgentState] = []
    leases: dict[str, WorktreeLease] = {}
    for index, task in enumerate(tasks):
        mission_id = f"agent-{index:04d}-{uuid.uuid4().hex[:8]}"
        lease = broker.provision(mission_id, base)
        leases[mission_id] = lease
        states.append(
            AgentState(
                mission_id=mission_id,
                task=task,
                base_sha=lease.base_sha,
                workspace_path=str(lease.path),
            )
        )
    return states, leases


def _report(
    states: list[AgentState],
    broker: WorktreeBroker,
    leases: dict[str, WorktreeLease],
) -> dict[str, Any]:
    return {
        "schema": "experiments.daft-monty-agent-frontier/v1",
        "daft_version": daft.__version__,
        "pydantic_monty_version": pydantic_monty.__version__,
        "missions": [
            {
                "mission_id": state.mission_id,
                "task": state.task,
                "status": state.status,
                "turns": state.turn,
                "base_sha": state.base_sha,
                "workspace_path": state.workspace_path,
                "git": broker.evidence(leases[state.mission_id]),
                "patch": broker.patch(leases[state.mission_id])[:30_000],
                "last_observation": (
                    state.transcript[-1]["observation"] if state.transcript else None
                ),
                "transcript": state.transcript,
            }
            for state in states
        ],
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, help="Git repository to create worktrees from")
    parser.add_argument("--base", default="HEAD", help="Commit-ish for detached worktrees")
    parser.add_argument("--task", action="append", default=[], help="One mission; repeatable")
    parser.add_argument("--rounds", type=int, default=6)
    parser.add_argument("--worktree-root", type=Path)
    parser.add_argument("--dry-run", action="store_true", help="Use deterministic actions")
    parser.add_argument("--demo", action="store_true", help="Create a temporary seed repository")
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Force-remove provisioned worktrees after printing their patches",
    )
    return parser


def main() -> None:
    args = _parser().parse_args()
    if args.rounds < 1:
        raise SystemExit("--rounds must be positive")

    demo_directory: tempfile.TemporaryDirectory[str] | None = None
    if args.demo:
        demo_directory = tempfile.TemporaryDirectory(prefix="daft-monty-frontier-")
        demo_root = Path(demo_directory.name)
        repository = _create_demo_repository(demo_root)
        worktree_root = demo_root / "worktrees"
        tasks = args.task or [
            "Inspect the repository and produce the first isolated result.",
            "Inspect the repository and produce the second isolated result.",
        ]
        cleanup = True
    else:
        if args.repo is None or not args.task:
            raise SystemExit("provide --repo and at least one --task, or use --demo")
        repository = args.repo
        worktree_root = args.worktree_root or (
            repository.resolve() / ".context" / "daft-monty-worktrees" / uuid.uuid4().hex
        )
        tasks = args.task
        cleanup = args.cleanup

    endpoint = None if args.dry_run else EndpointConfig.from_env()
    broker = WorktreeBroker(repository, worktree_root)
    try:
        states, leases = _states_for(tasks, broker, base=args.base)
        states = run_frontiers(
            states,
            endpoint=endpoint,
            rounds=args.rounds,
        )
        print(json.dumps(_report(states, broker, leases), indent=2))
    finally:
        if cleanup:
            broker.cleanup()
        elif broker.has_leases:
            print(f"worktrees retained under {broker.root}")
        if demo_directory is not None:
            demo_directory.cleanup()


if __name__ == "__main__":
    main()
