# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Credentialed semantic receipt for the LLM-agent example."""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path
from types import ModuleType

import pytest
from daft import lit

from scripts.run_example_receipt import (
    CAPTURED_RECEIPT_ENV,
    captured_receipt_or_run,
)

_EXAMPLE = Path(__file__).resolve().parents[2] / "examples" / "05_llm_agents.py"
_EXPECTED_RECEIPT = {
    "schema": "examples.llm-agent-thought-coverage/v1",
    "ticks_completed": 5,
    "agents": [
        {"name": "Ada", "thought_count": 5, "journal_entries": 5},
        {"name": "Iris", "thought_count": 5, "journal_entries": 5},
        {"name": "Rex", "thought_count": 5, "journal_entries": 5},
    ],
}


def _load_example() -> ModuleType:
    module_name = "llm_agent_example_receipt"
    spec = importlib.util.spec_from_file_location(module_name, _EXAMPLE)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


llm_agents = _load_example()


@pytest.mark.asyncio
async def test_llm_agent_receipt_tick_accounting_without_credentials(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        llm_agents,
        "prompt",
        lambda *_args, **_kwargs: lit('deterministic "offline" \\ thought\nnext'),
    )

    result = await llm_agents.run_demo(str(tmp_path / "llm-agents-offline"))

    assert result == _EXPECTED_RECEIPT


@pytest.mark.skipif(
    not (os.environ.get("OPENAI_API_KEY") or os.environ.get(CAPTURED_RECEIPT_ENV)),
    reason="credentialed example requires OPENAI_API_KEY or a captured receipt",
)
@pytest.mark.asyncio
async def test_llm_agent_receipt_proves_per_tick_thought_coverage(tmp_path: Path) -> None:
    result = await captured_receipt_or_run(
        llm_agents.run_demo,
        str(tmp_path / "llm-agents"),
    )

    assert result == _EXPECTED_RECEIPT
