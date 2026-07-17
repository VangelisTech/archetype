# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for the cooperative-emergence experiment schema."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


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
