# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Closed transition oracles for catalog-owned fleet-recovery state."""

import re
from collections.abc import Mapping
from enum import StrEnum
from pathlib import Path

import pytest

from archetype.app.storage.recovery_transitions import (
    RECOVERY_EXCEPTION_TRANSITION_GRAPH,
    RECOVERY_SWEEP_TRANSITION_GRAPH,
    RecoveryExceptionEvent,
    RecoveryExceptionStatus,
    RecoveryExceptionTransitionGraph,
    RecoverySweepEvent,
    RecoverySweepStatus,
    RecoverySweepTransitionGraph,
)

pytestmark = pytest.mark.contract("recovery.control.fenced")

_WORKER_SOURCE = (
    Path(__file__).resolve().parents[2] / "infra" / "control-catalog" / "src" / "index.ts"
)


def test_sweep_graph_is_exhaustive_and_fail_closed() -> None:
    for status in (None, *RecoverySweepStatus):
        for event in RecoverySweepEvent:
            edge = (status, event)
            if edge in RECOVERY_SWEEP_TRANSITION_GRAPH:
                assert (
                    RecoverySweepTransitionGraph.transition(*edge)
                    is RECOVERY_SWEEP_TRANSITION_GRAPH[edge]
                )
            else:
                with pytest.raises(ValueError, match="illegal recovery sweep transition"):
                    RecoverySweepTransitionGraph.transition(*edge)

    with pytest.raises(ValueError, match="unknown recovery sweep state"):
        RecoverySweepTransitionGraph.state("invented")
    with pytest.raises(ValueError, match="unknown recovery sweep event"):
        RecoverySweepTransitionGraph.transition(RecoverySweepStatus.IDLE, "invented")


def test_exception_graph_is_exhaustive_and_fail_closed() -> None:
    for status in (None, *RecoveryExceptionStatus):
        for event in RecoveryExceptionEvent:
            edge = (status, event)
            if edge in RECOVERY_EXCEPTION_TRANSITION_GRAPH:
                assert (
                    RecoveryExceptionTransitionGraph.transition(*edge)
                    is RECOVERY_EXCEPTION_TRANSITION_GRAPH[edge]
                )
            else:
                with pytest.raises(ValueError, match="illegal recovery exception transition"):
                    RecoveryExceptionTransitionGraph.transition(*edge)

    with pytest.raises(ValueError, match="unknown recovery exception state"):
        RecoveryExceptionTransitionGraph.state("invented")


def _worker_transition_graph(source: str, name: str) -> dict[str, str]:
    start = source.index(f"const {name}:")
    start = source.index("new Map([", start)
    end = source.index("]);", start)
    return dict(re.findall(r'\[\s*"([^"]+)"\s*,\s*"([^"]+)"\s*\]', source[start:end]))


def _python_transition_graph(
    graph: Mapping[tuple[StrEnum | None, StrEnum], StrEnum],
) -> dict[str, str]:
    return {
        f"{'<absent>' if source is None else source.value}|{event.value}": target.value
        for (source, event), target in graph.items()
    }


def test_worker_transition_graph_cannot_drift_from_storage_authority() -> None:
    source = _WORKER_SOURCE.read_text()
    assert _worker_transition_graph(source, "RECOVERY_SWEEP_TRANSITIONS") == (
        _python_transition_graph(RECOVERY_SWEEP_TRANSITION_GRAPH)
    )
    assert _worker_transition_graph(source, "RECOVERY_EXCEPTION_TRANSITIONS") == (
        _python_transition_graph(RECOVERY_EXCEPTION_TRANSITION_GRAPH)
    )

    # Every durable mutation path invokes the mirrored oracle, including
    # creation and status-preserving renew/checkpoint writes.
    assert source.count("recoverySweepTransition(") == 9
    assert source.count("recoveryExceptionTransition(") == 5
