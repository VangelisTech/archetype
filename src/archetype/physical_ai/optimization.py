# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Pure instruction-search behavior for physical-AI policies.

The evaluator is injected, so this module knows nothing about worlds, storage,
application services, or a particular simulator. A caller may score candidates
with persisted rollouts or with a cheaper model-based objective.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol


def _deduplicate(items: Sequence[str]) -> list[str]:
    return list(dict.fromkeys(items))


class PerturbationStrategy(Protocol):
    """Propose instruction variants near an incumbent instruction."""

    def propose(self, base: str, n: int) -> list[str]: ...


@dataclass(frozen=True)
class TemplatePerturbation:
    """Deterministic one-token toggle over a fixed vocabulary.

    This dependency-free strategy is a mechanism-check fixture. Production
    search can provide a semantic or model-backed implementation of
    :class:`PerturbationStrategy` without changing the optimizer.
    """

    vocabulary: tuple[str, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "vocabulary", tuple(self.vocabulary))
        if not self.vocabulary:
            raise ValueError("vocabulary must contain at least one token")

    def propose(self, base: str, n: int) -> list[str]:
        if n < 0:
            raise ValueError("n must not be negative")
        base_tokens = base.split()
        proposals: list[str] = []
        for index in range(n):
            word = self.vocabulary[index % len(self.vocabulary)]
            tokens = list(base_tokens)
            if word in tokens:
                tokens.remove(word)
            else:
                tokens.append(word)
            proposals.append(" ".join(tokens))
        return proposals


@dataclass(frozen=True)
class RoundRecord:
    """Best-so-far instruction after one optimization round."""

    round: int
    best_instruction: str
    best_success_rate: float
    evaluated: int


@dataclass(frozen=True)
class OptimizationResult:
    """Final instruction and the monotonic best-so-far trace."""

    best_instruction: str
    best_success_rate: float
    trace: tuple[RoundRecord, ...] = ()


Evaluator = Callable[[list[str]], Awaitable[Mapping[str, float]]]


async def optimize_instruction(
    *,
    evaluate: Evaluator,
    base: str,
    strategy: PerturbationStrategy,
    rounds: int,
    neighbors: int,
    patience: int = 2,
) -> OptimizationResult:
    """Hill-climb an instruction against an injected success-rate evaluator."""

    if rounds < 0:
        raise ValueError("rounds must not be negative")
    if neighbors < 0:
        raise ValueError("neighbors must not be negative")
    if patience < 1:
        raise ValueError("patience must be at least 1")

    scores = await evaluate([base])
    best_instruction = base
    best_success_rate = scores.get(base, 0.0)
    trace = [RoundRecord(0, best_instruction, best_success_rate, 1)]
    stale = 0

    for round_index in range(1, rounds + 1):
        if best_success_rate >= 1.0:
            break
        candidates = _deduplicate(
            [best_instruction, *strategy.propose(best_instruction, neighbors)]
        )
        scores = await evaluate(candidates)
        top = min(
            candidates,
            key=lambda candidate: (
                -scores.get(candidate, 0.0),
                len(candidate),
                candidate,
            ),
        )
        top_success_rate = scores.get(top, 0.0)
        improved = top_success_rate > best_success_rate or (
            top_success_rate == best_success_rate and len(top) < len(best_instruction)
        )
        if improved:
            best_instruction = top
            best_success_rate = top_success_rate
            stale = 0
        else:
            stale += 1
        trace.append(
            RoundRecord(
                round_index,
                best_instruction,
                best_success_rate,
                len(candidates),
            )
        )
        if stale >= patience:
            break

    return OptimizationResult(
        best_instruction=best_instruction,
        best_success_rate=best_success_rate,
        trace=tuple(trace),
    )
