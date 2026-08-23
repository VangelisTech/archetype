# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "pydantic-monty==0.0.18",
# ]
# ///
# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Executable feasibility probe for Monty as a Python mission sandbox.

This deliberately exercises the latest stable PyPI release rather than adding
an experimental runtime to Archetype's shipped dependencies.

Run:
    uv run --script experiments/monty_sandbox_spike.py
"""

from __future__ import annotations

import json
import tempfile
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pydantic_monty


def _probe(name: str, call: Callable[[], Any]) -> dict[str, Any]:
    started = time.perf_counter()
    try:
        value = call()
    except Exception as exc:
        return {
            "name": name,
            "succeeded": False,
            "exception": type(exc).__name__,
            "detail": str(exc),
            "elapsed_ms": round((time.perf_counter() - started) * 1000, 3),
        }
    return {
        "name": name,
        "succeeded": True,
        "value": value,
        "elapsed_ms": round((time.perf_counter() - started) * 1000, 3),
    }


def _assert_denied(result: dict[str, Any], expected: str) -> None:
    assert not result["succeeded"], result
    assert expected in result["detail"], result


def _captured_stdout(session: Any) -> str:
    output = pydantic_monty.CollectString()
    session.feed_run("print('captured')", print_callback=output)
    return output.output


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="archetype-monty-") as directory:
        repository = Path(directory)
        (repository / "input.txt").write_text("mounted input")
        (repository / "module.py").write_text("answer = 42\n")
        workspace = pydantic_monty.MountDir(
            virtual_path="/workspace",
            host_path=repository,
            mode="read-write",
            write_bytes_limit=1024,
        )
        session = pydantic_monty.MontyRepl(
            limits={
                "max_duration_secs": 0.1,
                "max_memory": 1_000_000,
                "max_recursion_depth": 100,
            }
        )

        results = [
            _probe(
                "mounted_read",
                lambda: session.feed_run(
                    "from pathlib import Path\nPath('/workspace/input.txt').read_text()",
                    mount=workspace,
                ),
            ),
            _probe(
                "mounted_write",
                lambda: session.feed_run(
                    "from pathlib import Path\n"
                    "Path('/workspace/generated.py').write_text('value = 7\\n')",
                    mount=workspace,
                ),
            ),
            _probe(
                "path_traversal",
                lambda: session.feed_run(
                    "from pathlib import Path\nPath('/workspace/../../etc/passwd').read_text()",
                    mount=workspace,
                ),
            ),
            _probe(
                "unmounted_read",
                lambda: session.feed_run(
                    "from pathlib import Path\nPath('/etc/passwd').read_text()",
                    mount=workspace,
                ),
            ),
            _probe(
                "host_environment",
                lambda: session.feed_run("import os\nos.getenv('PATH')", mount=workspace),
            ),
            _probe(
                "subprocess_command",
                lambda: session.feed_run(
                    "import subprocess\nsubprocess.run(['git', 'status'])",
                    mount=workspace,
                ),
            ),
            _probe(
                "mounted_module_import",
                lambda: session.feed_run(
                    "import sys\nsys.path.append('/workspace')\nimport module\nmodule.answer",
                    mount=workspace,
                ),
            ),
            _probe(
                "mount_write_quota",
                lambda: session.feed_run(
                    "from pathlib import Path\n"
                    "Path('/workspace/too-large.txt').write_text('x' * 2048)",
                    mount=workspace,
                ),
            ),
            _probe("stdout_capture", lambda: _captured_stdout(session)),
        ]

        assert results[0]["value"] == "mounted input", results[0]
        assert results[1]["succeeded"], results[1]
        assert (repository / "generated.py").read_text() == "value = 7\n"
        _assert_denied(results[2], "Permission denied")
        _assert_denied(results[3], "Permission denied")
        _assert_denied(results[4], "not supported")
        _assert_denied(results[5], "No module named 'subprocess'")
        assert not results[6]["succeeded"], results[6]
        _assert_denied(results[7], "write limit")
        assert results[8]["value"] == "captured\n", results[8]

        session.feed_run("counter = 41")
        snapshot = session.dump()
        restored = pydantic_monty.MontyRepl.load(snapshot)
        snapshot_result = _probe("interpreter_snapshot", lambda: restored.feed_run("counter + 1"))
        assert snapshot_result["value"] == 42, snapshot_result
        results.append(snapshot_result)

        timeout_result = _probe(
            "duration_limit",
            lambda: session.feed_run("while True:\n    pass"),
        )
        _assert_denied(timeout_result, "time limit exceeded")
        results.append(timeout_result)

        report = {
            "pydantic_monty_version": pydantic_monty.__version__,
            "decision": {
                "current_mission_sandbox_backend": "no-go",
                "narrow_python_code_executor": "promising",
            },
            "results": results,
        }
        print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
