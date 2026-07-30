# /// script
# requires-python = ">=3.12"
# dependencies = ["daft[openai]==0.7.19", "pydantic-monty==0.0.18"]
# ///
# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Daft-vectorized coding missions through isolated Git worktrees and draft PRs."""

# fmt: off
from __future__ import annotations

import argparse
import ast
import atexit
import hashlib
import json
import os
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

import daft
import pydantic_monty as monty
from daft import Series, col
from daft.ai.openai.provider import OpenAIProvider
from daft.functions import file, prompt
from pydantic import BaseModel

SYSTEM = """You are one coding agent. The current target file is attached. Return
its complete replacement with the requested bug fixed. Make the smallest valid
edit. The supplied MRE is the oracle. Set done=true only when it should pass."""
class Edit(BaseModel):
    content: str
    done: bool
@dataclass(frozen=True)
class Bug:
    slug: str
    source: str
    task: str
    mre: str
    solution: str
BUGS = (
    Bug("normalize", "def normalize(text):\n    return text.lower()\n",
        "Fix normalize() to strip surrounding whitespace before lowercasing.",
        "assert normalize('  HeLLo \\n') == 'hello'",
        "def normalize(text):\n    return text.strip().lower()\n"),
    Bug("safe-ratio", "def safe_ratio(total, count):\n    return total / count\n",
        "Fix safe_ratio() to return 0.0 for zero count without changing normal division.",
        "assert safe_ratio(3, 0) == 0.0\nassert safe_ratio(6, 2) == 3.0",
        "def safe_ratio(total, count):\n    return 0.0 if count == 0 else total / count\n"),
    Bug("stable-unique", "def unique(values):\n    return sorted(set(values))\n",
        "Fix unique() to remove duplicates while preserving first-seen order.",
        "assert unique([3, 1, 3, 2]) == [3, 1, 2]",
        "def unique(values):\n    return list(dict.fromkeys(values))\n"),
)
def command(args: list[str], *, cwd: Path | None = None,
            check: bool = True) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(args, cwd=cwd, check=False, capture_output=True,
                               text=True)
    if check and completed.returncode:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise RuntimeError(f"{' '.join(args)} failed: {detail}")
    return completed
def atomic_json(path: Path, value: object) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2))
    temporary.replace(path)
def digest(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()
def validate_source(source: str, mre: str) -> None:
    ast.parse(source)
    repl = monty.MontyRepl(
        limits={"max_duration_secs": 2, "max_memory": 64_000_000})
    repl.feed_run(source + "\n" + mre)
class WorktreeBroker:
    """Host-owned exact-base worktree, commit, push, and PR capability."""

    def __init__(self, repository: Path, root: Path, base: str, run_id: str,
                 branch_prefix: str) -> None:
        top = command(["git", "-C", str(repository), "rev-parse", "--show-toplevel"]).stdout.strip()
        self.repository = Path(top).resolve()
        self.base_sha = command(
            ["git", "-C", str(self.repository), "rev-parse", f"{base}^{{commit}}"]
        ).stdout.strip()
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=False)
        self.receipt_root = self.root.parent / "receipts"
        self.receipt_root.mkdir()
        self.run_id = run_id
        self.branch_prefix = branch_prefix.strip("/")
        self.leases: dict[str, dict[str, str]] = {}
    def provision(self, mission_id: str, bug: Bug, index: int) -> dict[str, str]:
        branch = f"{self.branch_prefix}/{self.run_id}-{index:04d}-{bug.slug}"
        path = self.root / mission_id
        command(["git", "-C", str(self.repository), "worktree", "add", "-b",
                 branch, str(path), self.base_sha])
        lease = {"workspace": str(path.resolve()), "branch": branch,
                 "base_sha": self.base_sha}
        self.leases[mission_id] = lease
        relative_root = Path("experiments") / "agent_pr_proof" / self.run_id / mission_id
        target = relative_root / "app.py"
        mre_path = relative_root / "mre.py"
        (path / target).parent.mkdir(parents=True)
        (path / target).write_text(bug.source)
        (path / mre_path).write_text(bug.mre + "\n")
        lease.update({"target": target.as_posix(), "mre_path": mre_path.as_posix(),
                      "path": str((path / target).resolve())})
        return lease
    def assert_publish_base(self, pr_base: str) -> None:
        local = command(["git", "-C", str(self.repository), "rev-parse",
                         f"{pr_base}^{{commit}}"]).stdout.strip()
        remote = command(["git", "-C", str(self.repository), "ls-remote",
                          "--heads", "origin", pr_base]).stdout.split()
        remote_sha = remote[0] if remote else ""
        if local != self.base_sha or remote_sha != self.base_sha:
            raise RuntimeError("exact --base must equal local and remote --pr-base")
    def _record(self, receipt: dict[str, Any]) -> None:
        atomic_json(self.receipt_root / f"{receipt['mission_id']}.json", receipt)
    def finalize(self, state: dict[str, Any], *, publish: bool,
                 pr_base: str | None, model: str) -> dict[str, Any]:
        workspace = Path(state["workspace"])
        paths = [state["target"], state["mre_path"]]
        receipt: dict[str, Any] = {
            "mission_id": state["id"], "branch": state["branch"],
            "base_sha": state["base_sha"], "paths": paths, "stage": "verified",
            "head_sha": None, "pr_url": None, "error": None,
        }
        self._record(receipt)
        try:
            source = (workspace / state["target"]).read_text()
            mre = (workspace / state["mre_path"]).read_text()
            validate_source(source, mre)
            receipt.update({"source_digest": digest(source),
                            "mre_digest": digest(mre)})
            command(["git", "add", "--", *paths], cwd=workspace)
            command(["git", "diff", "--cached", "--check"], cwd=workspace)
            changed = command(["git", "diff", "--cached", "--quiet"],
                              cwd=workspace, check=False)
            head = command(["git", "rev-parse", "HEAD"], cwd=workspace).stdout.strip()
            title = f"fix(proof): complete {state['bug']} agent MRE"
            if changed.returncode == 1:
                command(["git", "commit", "--signoff", "-m", title], cwd=workspace)
                head = command(["git", "rev-parse", "HEAD"], cwd=workspace).stdout.strip()
            if head == state["base_sha"]:
                raise RuntimeError(f"{state['id']} has no mission commit")
            committed_source = command(["git", "show", f"HEAD:{paths[0]}"],
                                       cwd=workspace).stdout
            committed_mre = command(["git", "show", f"HEAD:{paths[1]}"],
                                    cwd=workspace).stdout
            validate_source(committed_source, committed_mre)
            if digest(committed_source) != receipt["source_digest"] or \
                    digest(committed_mre) != receipt["mre_digest"]:
                raise RuntimeError("committed bytes differ from verified bytes")
            receipt.update({"head_sha": head, "stage": "committed"})
            self._record(receipt)
            if not publish:
                return receipt
            command(["git", "push", "origin",
                     f"HEAD:refs/heads/{state['branch']}"], cwd=workspace)
            receipt["stage"] = "pushed"
            self._record(receipt)
            body = (
                "Proof-only draft produced by the Daft vectorized-agent harness.\n\n"
                f"- Mission: `{state['id']}`\n"
                f"- Model: `{model}`\n"
                f"- Exact base: `{state['base_sha']}`\n"
                f"- Task: {state['task']}\n"
                f"- MRE: `{mre.strip()}`\n"
                f"- Daft turns: {state['turn']}\n"
                "- Acceptance: AST parse plus isolated Monty execution passed.\n\n"
                "This draft exists to prove the Git publication boundary. "
                "Do not merge it."
            )
            existing = command(["gh", "pr", "list", "--state", "all", "--head",
                                state["branch"], "--json", "url", "--jq",
                                ".[0].url"], cwd=workspace).stdout.strip()
            if not existing:
                existing = command(
                    ["gh", "pr", "create", "--draft", "--base", pr_base or "",
                     "--head", state["branch"], "--title", f"[agent proof] {title}",
                     "--body", body], cwd=workspace).stdout.strip().splitlines()[-1]
            receipt.update({"pr_url": existing, "stage": "pr_opened"})
        except Exception as exc:
            receipt.update({"stage": "error",
                            "error": f"{type(exc).__name__}: {exc}"})
        self._record(receipt)
        return receipt
    def cleanup(self) -> None:
        for lease in reversed(list(self.leases.values())):
            path = Path(lease["workspace"])
            if path.exists():
                command(["git", "-C", str(self.repository), "worktree",
                         "remove", "--force", str(path)])
            command(["git", "-C", str(self.repository), "branch", "-D",
                     lease["branch"]], check=False)
def parse_edit(raw: object) -> tuple[str, bool]:
    action = json.loads(raw) if isinstance(raw, str) else raw
    return str(action["content"]), bool(action["done"])
@daft.func(return_dtype=daft.DataType.string())
def mock_edit(state_json: str) -> str:
    state = json.loads(state_json)
    solution = next(bug.solution for bug in BUGS if bug.slug == state["bug"])
    return json.dumps({"content": solution, "done": True})
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
            content, claimed_done = parse_edit(raw)
            mre = (Path(state["workspace"]) / state["mre_path"]).read_text()
            validate_source(content, mre)
            observation = {
                "action": str(raw)[-4_000:],
                "claimed_done": claimed_done,
                "mre": "passed",
            }
            state.update({"candidate": content, "materialized": False})
            state["status"] = "done"
        except Exception as exc:
            observation = {
                "action": str(raw)[-4_000:],
                "error": f"{type(exc).__name__}: {exc}",
            }
            state["status"] = "active"
        state["history"] = [*state["history"][-2:], observation]
        state["tool_batch"] = len(state_rows)
        state["turn"] += 1
        advanced.append(json.dumps(state))
    return Series.from_pylist(advanced)
def setup_plain_workspace(run_root: Path, mission_id: str,
                          bug: Bug) -> dict[str, str]:
    workspace = run_root / "workspaces" / mission_id
    workspace.mkdir(parents=True)
    target = Path("app.py")
    mre_path = Path("mre.py")
    (workspace / target).write_text(bug.source)
    (workspace / mre_path).write_text(bug.mre + "\n")
    return {
        "workspace": str(workspace.resolve()),
        "target": target.as_posix(),
        "mre_path": mre_path.as_posix(),
        "path": str((workspace / target).resolve()),
        "branch": "",
        "base_sha": "",
    }
def make_states(count: int, run_root: Path,
                broker: WorktreeBroker | None) -> list[dict[str, Any]]:
    states = []
    for index in range(count):
        bug = BUGS[index % len(BUGS)]
        mission_id = f"agent-{index:04d}-{bug.slug}"
        lease = (
            broker.provision(mission_id, bug, index)
            if broker
            else setup_plain_workspace(run_root, mission_id, bug)
        )
        states.append(
            {
                "id": mission_id,
                "bug": bug.slug,
                "task": bug.task,
                **lease,
                "turn": 0,
                "status": "active",
                "history": [],
            }
        )
    return states
def agent_prompt(state: dict[str, Any]) -> str:
    current = Path(state["path"]).read_text()[:8_000]
    mre = (Path(state["workspace"]) / state["mre_path"]).read_text()
    return (
        f"Agent: {state['id']}\nTask: {state['task']}\n"
        f"MRE: {mre}\nTurn: {state['turn']}\n"
        f"Current source:\n```python\n{current}\n```\n"
        f"Prior observations: {json.dumps(state['history'])}\n"
        "The attached implementation fails the MRE. Do not echo it unchanged. "
        "Return a changed, complete corrected target now."
    )
def verify(state: dict[str, Any]) -> bool:
    try:
        mre = (Path(state["workspace"]) / state["mre_path"]).read_text()
        validate_source(Path(state["path"]).read_text(), mre)
        return True
    except Exception:
        return False
def preflight(states: list[dict[str, Any]]) -> None:
    for state in states:
        mre = (Path(state["workspace"]) / state["mre_path"]).read_text()
        try:
            validate_source(Path(state["path"]).read_text(), mre)
        except Exception as exc:
            state["baseline_failure"] = f"{type(exc).__name__}: {exc}"
        else:
            raise RuntimeError(f"{state['id']} MRE does not reproduce the bug")
def materialize(states: list[dict[str, Any]]) -> None:
    for state in states:
        if "candidate" not in state or state.get("materialized"):
            continue
        target = PurePosixPath(state["target"])
        if target.is_absolute() or ".." in target.parts:
            raise ValueError(f"unsafe mission target: {target}")
        path = Path(state["workspace"]) / target
        temporary = path.with_suffix(path.suffix + ".candidate")
        temporary.write_text(state["candidate"])
        temporary.replace(path)
        state.update({"materialized": True,
                      "source_digest": digest(state["candidate"])})
def create_demo_repository(run_root: Path) -> Path:
    repository = run_root / "demo-repository"
    repository.mkdir()
    command(["git", "init", str(repository)])
    command(["git", "config", "user.name", "Daft Agent Proof"], cwd=repository)
    command(["git", "config", "user.email", "daft-agent@example.invalid"],
            cwd=repository)
    (repository / "README.md").write_text("# Vectorized agent Git proof\n")
    command(["git", "add", "README.md"], cwd=repository)
    command(["git", "commit", "-m", "chore: seed agent proof repository"],
            cwd=repository)
    return repository
def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser()
    result.add_argument("--agents", type=int, default=3)
    result.add_argument("--rounds", type=int, default=3)
    result.add_argument("--mock", action="store_true")
    result.add_argument("--base-url", default=os.getenv("LLM_BASE_URL"))
    result.add_argument("--model", default=os.getenv("LLM_MODEL"))
    result.add_argument("--reasoning-effort",
                        default=os.getenv("LLM_REASONING_EFFORT"))
    result.add_argument("--root", type=Path, default=Path(".context/vectorized-proof"))
    result.add_argument("--git-repo", type=Path)
    result.add_argument("--demo-git", action="store_true")
    result.add_argument("--base", default="HEAD")
    result.add_argument("--branch-prefix", default="proof/daft-agent")
    result.add_argument("--pr-base")
    result.add_argument("--publish", action="store_true")
    result.add_argument("--cleanup", action="store_true")
    return result
def main() -> None:
    lifecycle_started = time.perf_counter()
    args = parser().parse_args()
    lines = len(Path(__file__).read_text().splitlines())
    if lines > 500:
        raise SystemExit("proof exceeded 500 lines")
    if args.agents < 1 or args.rounds < 1:
        raise SystemExit("--agents and --rounds must be positive")
    if args.git_repo and args.demo_git:
        raise SystemExit("choose --git-repo or --demo-git, not both")
    if args.publish and (not args.git_repo or not args.pr_base):
        raise SystemExit("--publish requires --git-repo and --pr-base")
    if not args.mock and (not args.base_url or not args.model):
        raise SystemExit("set LLM_BASE_URL, LLM_MODEL, and optionally LLM_API_KEY")

    run_id = f"run-{time.time_ns()}"
    run_root = args.root.resolve() / run_id
    run_root.mkdir(parents=True)
    repository = create_demo_repository(run_root) if args.demo_git else args.git_repo
    broker = (
        WorktreeBroker(
            repository=repository,
            root=run_root / "worktrees",
            base=args.base,
            run_id=run_id,
            branch_prefix=args.branch_prefix,
        )
        if repository
        else None
    )
    if broker and args.publish:
        broker.assert_publish_base(args.pr_base)
    if broker and args.cleanup:
        atexit.register(broker.cleanup)
    states = make_states(args.agents, run_root, broker)
    state_path = run_root / "states.json"
    preflight(states)
    atomic_json(state_path, states)
    provider = (
        None
        if args.mock
        else OpenAIProvider(
            name="proof",
            base_url=args.base_url,
            api_key=os.getenv("LLM_API_KEY", "local"),
            timeout=120,
            max_retries=0,
        )
    )
    totals = {"inference_s": 0.0, "tools_s": 0.0}
    for frontier in range(args.rounds):
        active = [state for state in states if state["status"] == "active"]
        if not active:
            break
        frame = daft.from_pylist(
            [
                {
                    "state": json.dumps(state),
                    "message": agent_prompt(state),
                    "path": state["path"],
                }
                for state in active
            ]
        )
        if args.mock:
            action = mock_edit(col("state"))
        else:
            options: dict[str, Any] = {
                "use_chat_completions": True,
                "temperature": 0,
                "max_tokens": 300,
            }
            if args.reasoning_effort:
                options["reasoning_effort"] = args.reasoning_effort
            action = prompt(
                [col("message"), file(col("path"))],
                return_format=Edit,
                system_message=SYSTEM,
                provider=provider,
                model=args.model,
                **options,
            )
        started = time.perf_counter()
        inferred = frame.with_column("action", action).collect()
        inference_s = time.perf_counter() - started
        started = time.perf_counter()
        stepped = (
            inferred.with_column(
                "next_state",
                run_tool_batch(col("state"), col("action")),
            )
            .select("next_state")
            .collect()
        )
        tools_s = time.perf_counter() - started
        replacements = {row["id"]: row for row in
                        (json.loads(item["next_state"]) for item in stepped.to_pylist())}
        states = [replacements.get(state["id"], state) for state in states]
        atomic_json(state_path, states)
        materialize(states)
        atomic_json(state_path, states)
        totals["inference_s"] += inference_s
        totals["tools_s"] += tools_s
        print(
            json.dumps(
                {
                    "frontier": frontier,
                    "agents": len(active),
                    "inference_s": round(inference_s, 3),
                    "tools_s": round(tools_s, 3),
                    "tool_agents_s": round(len(active) / tools_s, 1),
                }
            )
        )

    accepted = [state for state in states if verify(state)]
    receipts: list[dict[str, Any]] = []
    if broker and accepted:
        with ThreadPoolExecutor(max_workers=min(8, len(accepted))) as pool:
            receipts = list(
                pool.map(
                    lambda state: broker.finalize(
                        state,
                        publish=args.publish,
                        pr_base=args.pr_base,
                        model=args.model or "mock",
                    ),
                    accepted,
                )
        )
        atomic_json(run_root / "publications.json", receipts)
    publication_ok = all(not receipt["error"] for receipt in receipts)
    if broker and args.cleanup:
        broker.cleanup()
        atexit.unregister(broker.cleanup)
    summary = {
        "agents": len(states),
        "verified": len(accepted),
        "failed": len(states) - len(accepted),
        "frontier_compute_s": round(sum(totals.values()), 3),
        "lifecycle_s": round(time.perf_counter() - lifecycle_started, 3),
        "max_tool_batch": max(state.get("tool_batch", 0) for state in states),
        "commits": sum(bool(receipt["head_sha"]) for receipt in receipts),
        "prs": [receipt["pr_url"] for receipt in receipts if receipt["pr_url"]],
        "workspaces": str(run_root),
        "source_lines": lines,
    }
    print(json.dumps(summary))
    success = len(accepted) == len(states) and publication_ok
    raise SystemExit(0 if success else 1)
if __name__ == "__main__":
    main()
