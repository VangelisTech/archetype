# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for the cooperative-emergence experiment schema."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest


def _load_experiment():
    path = Path("experiments/cooperative_emergence.py")
    spec = importlib.util.spec_from_file_location("cooperative_emergence_contract", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_strategy_payload_preserves_domain_kind() -> None:
    experiment = _load_experiment()

    payload = experiment.Strategy(kind="greedy").to_payload()

    assert payload["type"] == "Strategy"
    assert payload["kind"] == "greedy"


@pytest.mark.asyncio
async def test_experiment_uses_supported_world_autoresearch_handle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    experiment = _load_experiment()
    autoresearch_calls: list[tuple[Any, object]] = []

    class FakeFork:
        async def update(self, _entity_id: int, _component: object) -> None:
            return None

        async def step(self) -> None:
            return None

        async def autoresearch(self, config: Any, evaluator: object) -> Any:
            autoresearch_calls.append((config, evaluator))
            return SimpleNamespace(final_score=0.75, iterations=())

    class FakeWorld:
        def __init__(self) -> None:
            self.next_entity_id = 1

        async def spawn(self, _component: object) -> int:
            entity_id = self.next_entity_id
            self.next_entity_id += 1
            return entity_id

        async def add_hook(self, _event_type: object, _handler: object) -> None:
            return None

        async def step(self) -> None:
            return None

        async def info(self) -> object:
            return SimpleNamespace(world_id="base-world")

        async def fork(self, _name: str) -> FakeFork:
            return FakeFork()

    class FakeRuntime:
        async def __aenter__(self) -> FakeRuntime:
            return self

        async def __aexit__(self, *_exc: object) -> None:
            return None

        def world(self, _name: str, **_kwargs: object) -> FakeWorld:
            return FakeWorld()

    monkeypatch.setattr(experiment, "ArchetypeRuntime", FakeRuntime)

    await experiment.main()

    assert len(autoresearch_calls) == 5
    assert [config.experiment_id for config, _evaluator in autoresearch_calls] == [
        "commons-regen-0.01",
        "commons-regen-0.03",
        "commons-regen-0.05",
        "commons-regen-0.1",
        "commons-regen-0.2",
    ]
    assert all(callable(evaluator) for _config, evaluator in autoresearch_calls)
