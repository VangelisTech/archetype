"""
Memory Bank (rules, learnings, prompts).

This module is intentionally stdlib-only.

It is a tiny, versionable substrate for:
- capturing "learnings" as they happen (so we don't relearn the same lessons),
- maintaining explicit rules/constraints (style, security, API constraints),
- storing reusable prompts,
- rendering a canonical "system prompt" / operating context snapshot.
"""

from __future__ import annotations

from .store import MemoryBank, MemoryBankPaths

__all__ = ["MemoryBank", "MemoryBankPaths"]

