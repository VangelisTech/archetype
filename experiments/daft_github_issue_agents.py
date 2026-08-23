# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import argparse
import atexit
import hashlib
import json
import os
import subprocess
import time
from pathlib import Path, PurePosixPath
from typing import Any

import daft
from apple_validator import execute as execute_validator
from apple_validator import safe_path
from apple_validator import start as start_validator
from apple_validator import stop as stop_validator
from daft import Series, col
from daft.ai.openai.provider import OpenAIProvider
from daft.functions import file, prompt
from daft_modal_saturation import endpoint_json, gpu_summary

SYSTEM = """You are a coding agent fixing a real GitHub issue from an exact
repository base. Return one minimal unified Git patch, including deterministic
regression coverage. Preserve the stated invariants. Use only allowed paths.
The patch must start with diff --git. Emit only changed diff hunks: never copy
complete files, never add Markdown fences, and keep the patch below 16,000
characters and 120 changed lines. Every modified file must have literal
--- a/path and +++ b/path headers followed by an @@ hunk."""


def command(
    args: list[str],
    *,
    cwd: Path,
    env: dict[str, str] | None = None,
    input_text: str | None = None,
    timeout: int = 300,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        args,
        cwd=cwd,
        env=env,
        input=input_text,
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )
    if check and completed.returncode:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise RuntimeError(f"{' '.join(args)} failed: {detail[-4_000:]}")
    return completed


def atomic_json(path: Path, value: object) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2))
    temporary.replace(path)


def parse_action(raw: object) -> str:
    if not isinstance(raw, str):
        raise TypeError("patch response must be text")
    patch = raw.strip()
    start = patch.find("diff --git ")
    if start < 0:
        raise ValueError("candidate is not a unified Git patch")
    patch = patch[start:]
    if "\n```" in patch:
        patch = patch.split("\n```", 1)[0]
    return patch.rstrip() + "\n"


def patch_paths(patch: str) -> set[str]:
    paths: set[str] = set()
    if "diff --git " not in patch:
        raise ValueError("candidate is not a unified Git patch")
    for line in patch.splitlines():
        if not line.startswith(("--- ", "+++ ")):
            continue
        raw = line[4:].split("\t", 1)[0]
        if raw == "/dev/null":
            continue
        if raw.startswith(("a/", "b/")):
            raw = raw[2:]
        path = PurePosixPath(raw)
        if path.is_absolute() or ".." in path.parts or not path.parts:
            raise ValueError(f"unsafe patch path: {raw}")
        paths.add(path.as_posix())
    if not paths:
        raise ValueError("candidate patch contains no file paths")
    return paths


@daft.func.batch(
    return_dtype=daft.DataType.string(),
    use_process=False,
    batch_size=64,
    max_retries=0,
)
def preflight_batch(
    specs: Series,
    validators: Series,
    mre_paths: Series,
) -> Series:
    receipts = []
    spec_rows = specs.to_pylist()
    rows = zip(
        spec_rows,
        validators.to_pylist(),
        mre_paths.to_pylist(),
        strict=True,
    )
    for spec_json, validator, mre_raw in rows:
        spec = json.loads(spec_json)
        completed = execute_validator(
            validator,
            ["-m", "pytest", "-q", str(mre_raw)],
            check=False,
        )
        detail = completed.stdout + completed.stderr
        reproduced = completed.returncode != 0 and spec["baseline_contains"] in detail
        receipts.append(
            json.dumps(
                {
                    "reproduced": reproduced,
                    "exit": completed.returncode,
                    "evidence": detail[-2_000:],
                    "tool_batch": len(spec_rows),
                }
            )
        )
    return Series.from_pylist(receipts)


@daft.func.batch(
    return_dtype=daft.DataType.string(),
    use_process=False,
    batch_size=64,
    max_retries=0,
)
def validate_patch_batch(
    specs: Series,
    actions: Series,
    workspaces: Series,
    validators: Series,
    mre_paths: Series,
) -> Series:
    receipts = []
    spec_rows = specs.to_pylist()
    rows = zip(
        spec_rows,
        actions.to_pylist(),
        workspaces.to_pylist(),
        validators.to_pylist(),
        mre_paths.to_pylist(),
        strict=True,
    )
    for spec_json, raw, workspace_raw, validator, mre_raw in rows:
        spec = json.loads(spec_json)
        workspace = Path(workspace_raw)
        receipt: dict[str, Any] = {"verified": False, "tool_batch": len(spec_rows)}
        try:
            patch = parse_action(raw)
            receipt.update(
                {
                    "candidate_chars": len(patch),
                    "candidate_preview": patch[:2_000],
                    "candidate_sha256": hashlib.sha256(patch.encode()).hexdigest(),
                }
            )
            paths = patch_paths(patch)
            allowed = set(spec["allowed_paths"])
            required = set(spec["required_paths"])
            if not paths <= allowed:
                raise ValueError(f"patch escaped allowed paths: {sorted(paths - allowed)}")
            if not required <= paths:
                raise ValueError(f"patch omitted required paths: {sorted(required - paths)}")
            command(
                ["git", "apply", "--check", "--whitespace=error-all", "-"],
                cwd=workspace,
                input_text=patch,
            )
            command(
                ["git", "apply", "--whitespace=error-all", "-"],
                cwd=workspace,
                input_text=patch,
            )
            changed = set(
                command(
                    ["git", "diff", "--name-only"],
                    cwd=workspace,
                ).stdout.splitlines()
            )
            if changed != paths:
                raise RuntimeError("applied paths differ from candidate patch paths")
            diff = command(["git", "diff", "--binary"], cwd=workspace).stdout
            mre_copy = workspace / ".context" / "mission_mre.py"
            mre_digest = hashlib.sha256(mre_copy.read_bytes()).digest()
            execute_validator(
                validator,
                ["-m", "pytest", "-q", str(mre_raw)],
            )
            for validation in spec["validation"]:
                execute_validator(validator, validation)
            execute_validator(validator, ["-m", "pytest", "-q", str(mre_raw)])
            final_diff = command(["git", "diff", "--binary"], cwd=workspace).stdout
            if final_diff != diff or hashlib.sha256(mre_copy.read_bytes()).digest() != mre_digest:
                raise RuntimeError("validation changed candidate or oracle bytes")
            command(["git", "diff", "--check"], cwd=workspace)
            receipt.update(
                {
                    "verified": True,
                    "changed_paths": sorted(changed),
                    "diff_sha256": hashlib.sha256(diff.encode()).hexdigest(),
                }
            )
        except Exception as exc:
            receipt["error"] = f"{type(exc).__name__}: {exc}"
            command(
                [
                    "git",
                    "restore",
                    "--source=HEAD",
                    "--staged",
                    "--worktree",
                    "--",
                    *spec["allowed_paths"],
                ],
                cwd=workspace,
                check=False,
            )
        receipts.append(json.dumps(receipt))
    return Series.from_pylist(receipts)


def context_bundle(workspace: Path, spec: dict[str, Any], target: Path) -> None:
    sections = [f"Issue: {spec['issue_url']}\nTitle: {spec['title']}\nTask: {spec['task']}\n"]
    for item in spec["context"]:
        path = safe_path(workspace, item["path"])
        lines = path.read_text().splitlines()
        for start, end in item["ranges"]:
            excerpt = "\n".join(
                f"{number:04d}: {lines[number - 1]}"
                for number in range(start, min(end, len(lines)) + 1)
            )
            sections.append(f"\n## {item['path']} lines {start}-{end}\n{excerpt}\n")
    target.write_text("\n".join(sections))


def exact_remote_base(repository: Path, base: str, pr_base: str) -> str:
    base_sha = command(["git", "rev-parse", f"{base}^{{commit}}"], cwd=repository)
    base_sha = base_sha.stdout.strip()
    remote = command(["git", "ls-remote", "--heads", "origin", pr_base], cwd=repository)
    remote = remote.stdout.split()
    if not remote or remote[0] != base_sha:
        raise RuntimeError("local mission base does not equal remote PR base")
    return base_sha


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mission",
        type=Path,
        default=Path("experiments/github_missions/issue_655.json"),
    )
    parser.add_argument("--endpoint", default="")
    parser.add_argument("--model", default="gemma-4-e4b")
    parser.add_argument("--max-tokens", type=int, default=6144)
    parser.add_argument("--temperature", type=float, default=0.0)
    parser.add_argument("--top-p", type=float, default=1.0)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--preflight-only", action="store_true")
    parser.add_argument("--root", type=Path, default=Path(".context/github-issue-missions"))
    args = parser.parse_args()

    root = command(["git", "rev-parse", "--show-toplevel"], cwd=Path.cwd()).stdout
    repository = Path(root.strip())
    spec = json.loads(args.mission.resolve().read_text())
    mission_paths = [
        spec["mre"],
        *spec["allowed_paths"],
        *spec["required_paths"],
        *(item["path"] for item in spec["context"]),
    ]
    for raw_path in mission_paths:
        safe_path(repository, raw_path)
    mre_source = safe_path(repository, spec["mre"])
    base_sha = exact_remote_base(repository, spec["base"], spec["pr_base"])
    run_id = f"run-{time.time_ns()}"
    run_root = (repository / args.root / run_id).resolve()
    workspace = run_root / "worktree"
    run_root.mkdir(parents=True)
    branch = f"{spec['branch_prefix']}-{run_id.removeprefix('run-')}"
    command(
        ["git", "worktree", "add", "-b", branch, str(workspace), base_sha],
        cwd=repository,
    )
    validator: str | None = None

    def cleanup_failed_mission() -> None:
        if validator is not None:
            stop_validator(validator, check=False)
        command(
            ["git", "worktree", "remove", "--force", str(workspace)],
            cwd=repository,
            check=False,
        )
        command(["git", "branch", "-D", branch], cwd=repository, check=False)

    atexit.register(cleanup_failed_mission)

    bundle = run_root / "context.txt"
    context_bundle(workspace, spec, bundle)
    mre_copy = workspace / ".context" / "mission_mre.py"
    mre_copy.parent.mkdir()
    mre_copy.write_text(mre_source.read_text())
    validator = start_validator(workspace, f"archetype-validator-{time.time_ns()}")
    row: dict[str, str] = {
        "spec": json.dumps(spec),
        "workspace": str(workspace),
        "validator": validator,
        "mre_path": "/workspace/.context/mission_mre.py",
        "context_path": str(bundle),
    }
    frame = daft.from_pylist([row])
    preflight = (
        frame.with_column(
            "preflight",
            preflight_batch(col("spec"), col("validator"), col("mre_path")),
        )
        .select("preflight")
        .collect()
        .to_pylist()[0]["preflight"]
    )
    preflight_receipt = json.loads(preflight)
    atomic_json(run_root / "preflight.json", preflight_receipt)
    if not preflight_receipt["reproduced"]:
        raise RuntimeError("mission MRE did not reproduce its expected failure")
    if args.preflight_only:
        print(preflight)
        return
    if not args.endpoint:
        parser.error("--endpoint is required unless --preflight-only is set")

    key = os.environ["LLM_API_KEY"]
    endpoint_info = endpoint_json(args.endpoint, key, "/gpu/info")
    provider = OpenAIProvider(
        name="modal-real-issues",
        base_url=f"{args.endpoint.rstrip('/')}/v1",
        api_key=key,
        timeout=360,
        max_retries=0,
    )
    base_message = (
        f"Fix {spec['issue_url']} from exact base {base_sha}.\n"
        f"{spec['task']}\nAllowed paths: {spec['allowed_paths']}.\n"
        "The attached line-numbered context is authoritative."
    )
    history: list[dict[str, str]] = []
    round_results: list[dict[str, Any]] = []
    all_gpu_samples: list[dict[str, float]] = []
    total_inference_s = 0.0
    total_tool_s = 0.0
    receipt: dict[str, Any] = {"verified": False, "error": "no frontier ran"}
    for turn in range(args.rounds):
        message = (
            f"{base_message}\nPrior validator evidence: {json.dumps(history)}\n"
            "Return a complete applicable patch now."
        )
        frontier = daft.from_pylist([{**row, "message": message}])
        action = prompt(
            [col("message"), file(col("context_path"))],
            system_message=SYSTEM,
            provider=provider,
            model=args.model,
            use_chat_completions=True,
            temperature=args.temperature,
            top_p=args.top_p,
            max_tokens=args.max_tokens,
        )
        gpu_start = int(endpoint_json(args.endpoint, key, "/gpu/mark")["time_ns"])
        started = time.perf_counter()
        try:
            inferred = frontier.with_column("action", action).collect()
        except Exception as exc:
            inference_s = time.perf_counter() - started
            gpu_end = int(endpoint_json(args.endpoint, key, "/gpu/mark")["time_ns"])
            gpu_samples = endpoint_json(
                args.endpoint,
                key,
                f"/gpu/samples?since_ns={gpu_start}&until_ns={gpu_end}",
            )
            all_gpu_samples.extend(gpu_samples)
            total_inference_s += inference_s
            error = f"{type(exc).__name__}: {exc}"[-2_000:]
            receipt = {"verified": False, "error": error}
            round_result = {
                "frontier": turn,
                "inference_s": round(inference_s, 3),
                "tool_s": 0.0,
                "verified": 0,
                "error": error,
                "gpu": gpu_summary(gpu_samples),
            }
            round_results.append(round_result)
            history.append({"error": error})
            atomic_json(run_root / f"frontier-{turn}.json", round_result)
            print(json.dumps(round_result), flush=True)
            continue
        inference_s = time.perf_counter() - started
        total_inference_s += inference_s
        raw_action = inferred.to_pylist()[0]["action"]
        atomic_json(
            run_root / f"candidate-{turn}.json",
            {"response": raw_action, "response_type": type(raw_action).__name__},
        )
        gpu_end = int(endpoint_json(args.endpoint, key, "/gpu/mark")["time_ns"])
        gpu_samples = endpoint_json(
            args.endpoint,
            key,
            f"/gpu/samples?since_ns={gpu_start}&until_ns={gpu_end}",
        )
        all_gpu_samples.extend(gpu_samples)
        started = time.perf_counter()
        validated = (
            inferred.with_column(
                "receipt",
                validate_patch_batch(
                    col("spec"),
                    col("action"),
                    col("workspace"),
                    col("validator"),
                    col("mre_path"),
                ),
            )
            .select("receipt")
            .collect()
            .to_pylist()[0]["receipt"]
        )
        tool_s = time.perf_counter() - started
        total_tool_s += tool_s
        receipt = json.loads(validated)
        atomic_json(run_root / f"validation-{turn}.json", receipt)
        round_result = {
            "frontier": turn,
            "inference_s": round(inference_s, 3),
            "tool_s": round(tool_s, 3),
            "verified": int(receipt["verified"]),
            "error": receipt.get("error"),
            "gpu": gpu_summary(gpu_samples),
        }
        round_results.append(round_result)
        atomic_json(run_root / f"frontier-{turn}.json", round_result)
        print(json.dumps(round_result), flush=True)
        if receipt["verified"]:
            break
        history.append(
            {
                "candidate": receipt.get("candidate_preview", "")[-2_000:],
                "error": receipt.get("error", "validator rejected candidate"),
            }
        )

    result = {
        "mission": spec["id"],
        "issue": spec["issue_url"],
        "base_sha": base_sha,
        "model": args.model,
        "endpoint": endpoint_info,
        "inference_s": round(total_inference_s, 3),
        "tool_s": round(total_tool_s, 3),
        "verified": int(receipt["verified"]),
        "changed_paths": receipt.get("changed_paths", []),
        "error": receipt.get("error"),
        "gpu": gpu_summary(all_gpu_samples),
        "frontiers": round_results,
        "evidence": str(run_root),
    }
    atomic_json(run_root / "result.json", result)
    print(json.dumps(result))
    success = bool(receipt["verified"])
    if success:
        stop_validator(validator)
        validator = None
        atexit.unregister(cleanup_failed_mission)
    raise SystemExit(0 if success else 1)


if __name__ == "__main__":
    main()
