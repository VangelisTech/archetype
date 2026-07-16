# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Independent-process idempotency scenarios over real LanceDB and SQLite files."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from evals.graders import state_check
from evals.types import GraderResult

ROOT = Path(__file__).resolve().parents[2]
WORKER_MODULE = "evals.infra.idempotency_worker"
_READY_TIMEOUT_SECONDS = 90.0


def _env() -> dict[str, str]:
    env = os.environ.copy()
    roots = [str(ROOT / "src"), str(ROOT)]
    if env.get("PYTHONPATH"):
        roots.append(env["PYTHONPATH"])
    env["PYTHONPATH"] = os.pathsep.join(roots)
    return env


def _command(action: str, uri: str, namespace: str, *args: str) -> list[str]:
    return [
        sys.executable,
        "-m",
        WORKER_MODULE,
        action,
        "--uri",
        uri,
        "--namespace",
        namespace,
        *args,
    ]


def _parse_output(stdout: str, stderr: str) -> dict:
    for line in reversed(stdout.splitlines()):
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            continue
    raise AssertionError(f"worker produced no JSON\nstdout:\n{stdout}\nstderr:\n{stderr}")


def _run(
    action: str,
    uri: str,
    namespace: str,
    *args: str,
    expected_returncode: int = 0,
    timeout: float = 120.0,
) -> dict:
    proc = subprocess.run(
        _command(action, uri, namespace, *args),
        cwd=ROOT,
        env=_env(),
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    if proc.returncode != expected_returncode:
        raise AssertionError(
            f"worker {action!r} exited {proc.returncode}, expected {expected_returncode}\n"
            f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
        )
    if expected_returncode != 0:
        return {"returncode": proc.returncode}
    return _parse_output(proc.stdout, proc.stderr)


def _spawn(action: str, uri: str, namespace: str, *args: str) -> subprocess.Popen:
    return subprocess.Popen(
        _command(action, uri, namespace, *args),
        cwd=ROOT,
        env=_env(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _failed_process_report(processes: list[subprocess.Popen]) -> str:
    for proc in processes:
        if proc.poll() is None:
            proc.terminate()

    reports: list[str] = []
    for proc in processes:
        try:
            stdout, stderr = proc.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            stdout, stderr = proc.communicate(timeout=5)
        reports.append(
            f"pid={proc.pid} returncode={proc.returncode}\nstdout:\n{stdout}\nstderr:\n{stderr}"
        )
    return "\n\n".join(reports)


def _wait_for_markers(
    markers: list[Path],
    processes: list[subprocess.Popen],
    timeout: float = _READY_TIMEOUT_SECONDS,
) -> None:
    deadline = time.monotonic() + timeout
    while not all(marker.exists() for marker in markers):
        if any(proc.poll() is not None for proc in processes):
            report = _failed_process_report(processes)
            raise AssertionError(f"worker exited before readiness\n{report}")
        if time.monotonic() >= deadline:
            missing = [str(marker) for marker in markers if not marker.exists()]
            report = _failed_process_report(processes)
            raise TimeoutError(f"workers did not become ready: {missing}\n{report}")
        time.sleep(0.01)


def _collect(processes: list[subprocess.Popen], timeout: float = 120.0) -> list[dict]:
    results: list[dict] = []
    try:
        for proc in processes:
            stdout, stderr = proc.communicate(timeout=timeout)
            if proc.returncode != 0:
                raise AssertionError(
                    f"parallel worker exited {proc.returncode}\n"
                    f"stdout:\n{stdout}\nstderr:\n{stderr}"
                )
            results.append(_parse_output(stdout, stderr))
    finally:
        for proc in processes:
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=10)
    return results


def task_process_crash_cold_resume() -> list[GraderResult]:
    """A hard-killed writer leaves rows invisible; fresh process resumes correctly."""
    with tempfile.TemporaryDirectory() as tmp:
        uri = str(Path(tmp) / "store")
        namespace = "hard_crash"
        seed = _run("seed", uri, namespace, "--name", "hard-crash")
        crash = _run(
            "crash-publish",
            uri,
            namespace,
            "--world-id",
            seed["world_id"],
            "--exit-code",
            "91",
            expected_returncode=91,
        )
        resumed = _run("resume-verify", uri, namespace, "--world-id", seed["world_id"])

        return [
            state_check(
                {
                    "worker_died_without_cleanup": crash["returncode"] == 91,
                    "cold_resume_ignored_unpublished_rows": resumed["resume_tick"] == 1,
                    "resumed_writer_advanced_once": resumed["final_tick"] == 2,
                    "one_retry_attempt_is_visible": resumed["visible_rows"] == 1,
                    "manifests_are_contiguous": resumed["manifest_ticks"] == [0, 1],
                    "crash_and_resume_advanced_fences": resumed["epoch"] == 3,
                },
                name="process_crash_cold_resume",
            )
        ]


def task_process_writer_fence_race() -> list[GraderResult]:
    """Two fresh processes race; exactly one publishes under the winning fence."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        uri = str(root / "store")
        namespace = "writer_race"
        seed = _run("seed", uri, namespace, "--name", "writer-race")
        go = root / "go"
        ready = [root / "ready-0", root / "ready-1"]
        processes = [
            _spawn(
                "resume-race",
                uri,
                namespace,
                "--world-id",
                seed["world_id"],
                "--ready",
                str(marker),
                "--go",
                str(go),
            )
            for marker in ready
        ]
        try:
            _wait_for_markers(ready, processes)
            epochs = sorted(json.loads(marker.read_text())["epoch"] for marker in ready)
            go.write_text("go")
            results = _collect(processes)
        finally:
            for proc in processes:
                if proc.poll() is None:
                    proc.kill()
                    proc.wait(timeout=10)

        visible = _run("query-world", uri, namespace, "--world-id", seed["world_id"])
        statuses = sorted(result["status"] for result in results)
        return [
            state_check(
                {
                    "writers_acquired_distinct_epochs": epochs == [2, 3],
                    "one_writer_published_one_was_stale": statuses == ["published", "stale"],
                    "only_two_ticks_are_visible": visible["row_ticks"] == [0, 1],
                    "one_manifest_per_tick": visible["manifest_ticks"] == [0, 1],
                    "one_entity_version_per_tick": visible["rows"] == 2,
                },
                name="process_writer_fence_race",
            )
        ]


def task_process_fact_replay() -> list[GraderResult]:
    """Eight processes submit one external fact; one visible identity wins."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        uri = str(root / "store")
        namespace = "fact_race"
        seed = _run("seed", uri, namespace, "--name", "fact-race")
        go = root / "go"
        ready = [root / f"ready-{index}" for index in range(8)]
        processes = [
            _spawn(
                "ingest-fact",
                uri,
                namespace,
                "--world-id",
                seed["world_id"],
                "--ready",
                str(marker),
                "--go",
                str(go),
                "--external-id",
                "shared-process-event",
                "--producer",
                "process-sensor",
                "--value",
                "21.5",
            )
            for marker in ready
        ]
        try:
            _wait_for_markers(ready, processes)
            go.write_text("go")
            results = _collect(processes)
        finally:
            for proc in processes:
                if proc.poll() is None:
                    proc.kill()
                    proc.wait(timeout=10)

        facts = _run("query-facts", uri, namespace, "--world-id", seed["world_id"])
        return [
            state_check(
                {
                    "all_processes_converged_on_token": len(
                        {result["commit_token"] for result in results}
                    )
                    == 1,
                    "one_process_owned_the_append": sum(
                        not result["duplicate"] for result in results
                    )
                    == 1,
                    "one_fact_is_visible": facts["rows"] == 1,
                    "external_identity_is_durable": facts["external_ids"]
                    == ["shared-process-event"],
                    "one_visible_commit_identity": len(facts["commit_ids"]) == 1,
                },
                name="process_fact_replay",
            )
        ]


def task_process_evaluation_replay() -> list[GraderResult]:
    """Concurrent processes grade once; changed subject conflicts before grading."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        uri = str(root / "store")
        namespace = "evaluation_race"
        seed = _run("seed", uri, namespace, "--name", "evaluation-race")
        go = root / "go"
        grader_log = root / "grader-calls.log"
        ready = [root / f"ready-{index}" for index in range(8)]
        processes = [
            _spawn(
                "evaluate",
                uri,
                namespace,
                "--world-id",
                seed["world_id"],
                "--ready",
                str(marker),
                "--go",
                str(go),
                "--evaluation-id",
                "shared-process-evaluation",
                "--grader-log",
                str(grader_log),
            )
            for marker in ready
        ]
        try:
            _wait_for_markers(ready, processes)
            go.write_text("go")
            results = _collect(processes)
        finally:
            for proc in processes:
                if proc.poll() is None:
                    proc.kill()
                    proc.wait(timeout=10)

        receipts = _run("query-receipts", uri, namespace, "--world-id", seed["world_id"])
        grader_calls_before_conflict = grader_log.read_text().splitlines()
        _run("advance", uri, namespace, "--world-id", seed["world_id"])
        conflict = _run(
            "evaluate-conflict",
            uri,
            namespace,
            "--world-id",
            seed["world_id"],
            "--evaluation-id",
            "shared-process-evaluation",
            "--grader-log",
            str(grader_log),
        )
        grader_calls_after_conflict = grader_log.read_text().splitlines()

        return [
            state_check(
                {
                    "all_processes_converged_on_token": len(
                        {result["commit_token"] for result in results}
                    )
                    == 1,
                    "one_process_owned_evaluation": sum(
                        not result["duplicate"] for result in results
                    )
                    == 1,
                    "external_grader_side_effect_happened_once": len(grader_calls_before_conflict)
                    == 1,
                    "one_receipt_is_visible": receipts["rows"] == 1,
                    "receipt_identity_is_durable": receipts["evaluation_ids"]
                    == ["shared-process-evaluation"],
                    "changed_subject_conflicts": conflict["conflict"] is True,
                    "subject_conflict_did_not_run_grader": (
                        grader_calls_after_conflict == grader_calls_before_conflict
                    ),
                },
                name="process_evaluation_replay",
            )
        ]
