# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
PR Triage with Archetype
=========================

Dogfoods Archetype on its own PR management: fetches live PRs via ``gh``,
spawns each as an entity, runs 4 processors in 1 tick, and prints a
grouped triage report.

Usage:
    uv run python examples/pr_triage.py
"""

import asyncio
import json
import subprocess
import sys
from datetime import UTC, datetime

import daft
from daft import DataFrame, col

from archetype import ArchetypeRuntime
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.component import Component
from archetype.core.config import StorageConfig

# ── Components ───────────────────────────────────────────────────────────────


class PullRequest(Component):
    number: int = 0
    title: str = ""
    state: str = ""
    author: str = ""
    created_at: str = ""
    url: str = ""


class Triage(Component):
    category: str = "other"
    age_days: float = 0.0
    staleness: float = 0.0
    risk: str = "low"
    action: str = ""
    priority_rank: int = 99


# ── Processors ───────────────────────────────────────────────────────────────


class CategoryProcessor(AsyncProcessor):
    """Parse title prefix into a category."""

    components = (PullRequest, Triage)
    priority = 10

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        @daft.func
        def categorize(title: str) -> str:
            t = title.lower().strip()
            if t.startswith("fix:") or t.startswith("fix("):
                return "bug"
            if t.startswith("feat:") or t.startswith("feat("):
                return "feat"
            if t.startswith("test:") or t.startswith("test("):
                return "test"
            if t.startswith("docs:") or t.startswith("docs("):
                return "docs"
            if t.startswith(("ci:", "ci(", "chore:", "chore(")):
                return "infra"
            if t.startswith("refactor:") or t.startswith("refactor("):
                return "refactor"
            return "other"

        return df.with_column("triage__category", categorize(col("pullrequest__title")))


class StalenessProcessor(AsyncProcessor):
    """Compute age in days and a 0-1 staleness score (capped at 90d)."""

    components = (PullRequest, Triage)
    priority = 20

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        now = datetime.now(UTC)

        @daft.func
        def age_days(created_at: str) -> float:
            if not created_at:
                return 0.0
            try:
                created = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                delta = now - created
                return max(0.0, delta.total_seconds() / 86400.0)
            except (ValueError, TypeError):
                return 0.0

        @daft.func
        def staleness_score(age: float) -> float:
            return min(1.0, age / 90.0)

        df = df.with_column("triage__age_days", age_days(col("pullrequest__created_at")))
        return df.with_column("triage__staleness", staleness_score(col("triage__age_days")))


class RiskProcessor(AsyncProcessor):
    """Assign risk level based on category."""

    components = (PullRequest, Triage)
    priority = 30

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        @daft.func
        def assess_risk(category: str) -> str:
            if category == "bug":
                return "high"
            if category in ("feat", "refactor"):
                return "med"
            return "low"

        return df.with_column("triage__risk", assess_risk(col("triage__category")))


class TriageProcessor(AsyncProcessor):
    """Combine state + staleness + risk into action and priority rank."""

    components = (PullRequest, Triage)
    priority = 40

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        @daft.func
        def decide(state: str, staleness: float, risk: str) -> str:
            if state in ("CLOSED", "MERGED"):
                return "done"
            if staleness > 0.7:
                return "close"
            if risk == "high":
                return "review"
            if staleness < 0.3 and risk == "low":
                return "merge"
            return "park"

        @daft.func
        def rank(action: str, staleness: float, risk: str) -> int:
            action_order = {"review": 0, "merge": 1, "park": 2, "close": 3, "done": 4}
            risk_order = {"high": 0, "med": 1, "low": 2}
            base = action_order.get(action, 5) * 100
            base += risk_order.get(risk, 3) * 10
            base += int(staleness * 9)
            return base

        df = df.with_column(
            "triage__action",
            decide(
                col("pullrequest__state"),
                col("triage__staleness"),
                col("triage__risk"),
            ),
        )
        return df.with_column(
            "triage__priority_rank",
            rank(
                col("triage__action"),
                col("triage__staleness"),
                col("triage__risk"),
            ),
        )


# ── Fetch + Run ──────────────────────────────────────────────────────────────


def fetch_prs() -> list[dict]:
    """Fetch all PRs (open + merged) via gh CLI."""
    try:
        result = subprocess.run(
            [
                "gh",
                "pr",
                "list",
                "--repo",
                "VangelisTech/archetype",
                "--state",
                "all",
                "--limit",
                "100",
                "--json",
                "number,title,state,author,createdAt,url",
            ],
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        print(
            "Error: GitHub CLI ('gh') not found. "
            "Install from https://cli.github.com/ and run 'gh auth login'.",
            file=sys.stderr,
        )
        sys.exit(1)
    if result.returncode != 0:
        print(f"Error fetching PRs: {result.stderr.strip()}", file=sys.stderr)
        sys.exit(1)
    return json.loads(result.stdout)


async def main():
    prs = fetch_prs()
    if not prs:
        print("No PRs found.")
        return

    async with ArchetypeRuntime() as runtime:
        world = runtime.world(
            "pr-triage",
            storage=StorageConfig(uri="./archetype_data", namespace="pr_triage"),
            processors=[
                CategoryProcessor(),
                StalenessProcessor(),
                RiskProcessor(),
                TriageProcessor(),
            ],
        )

        for pr in prs:
            await world.spawn(
                PullRequest(
                    number=pr["number"],
                    title=pr["title"],
                    state=pr.get("state", ""),
                    author=pr.get("author", {}).get("login", ""),
                    created_at=pr.get("createdAt", ""),
                    url=pr.get("url", ""),
                ),
                Triage(),
            )

        await world.run(steps=1)
        rows = (await world.query(PullRequest, Triage)).collect().to_pylist()

    rows.sort(key=lambda r: r.get("triage__priority_rank", 99))

    # Group by action
    groups: dict[str, list] = {}
    for row in rows:
        action = row.get("triage__action", "other")
        groups.setdefault(action, []).append(row)

    # Print report
    print(f"\n=== Archetype PR Triage ({len(rows)} PRs, 1 tick) ===\n")
    for action in ("review", "merge", "park", "close", "done"):
        group = groups.get(action, [])
        if not group:
            continue
        print(f"{action.upper()} ({len(group)})")
        for row in group:
            num = row.get("pullrequest__number", 0)
            cat = row.get("triage__category", "?")
            risk = row.get("triage__risk", "?")
            title = row.get("pullrequest__title", "")
            age = row.get("triage__age_days", 0.0)
            if len(title) > 50:
                title = title[:47] + "..."
            print(f"  #{num:<4} [{cat}/{risk}]  {title:<55} {age:.0f}d")
        print()


if __name__ == "__main__":
    asyncio.run(main())
