#!/usr/bin/env python3
"""
Memory Bank Demo
================

Demonstrates the MemoryBank as structured long-term context:
- Append-only JSONL storage (rules, learnings, prompts)
- Renders to markdown for LLM consumption
- Arbitrarily segmentable and labelable

This is "memory" or "context"—persistent structured knowledge
that accumulates over time and renders to markdown.
"""

from pathlib import Path
import tempfile

from archetype.memory_bank import MemoryBank, MemoryBankPaths
from archetype.memory_bank.render import render_system_prompt


def main():
    # Create a temporary memory bank (in practice, use .archetype_meta/memory_bank)
    with tempfile.TemporaryDirectory() as tmp:
        paths = MemoryBankPaths(root_dir=Path(tmp))
        bank = MemoryBank(paths)

        print("=" * 70)
        print("MEMORY BANK DEMO")
        print("=" * 70)
        print(f"\nStorage location: {paths.root_dir}")
        print()

        # =====================================================================
        # 1. ADD RULES (constraints, invariants)
        # =====================================================================
        print("[1] Adding rules...")

        bank.add_rule(
            "Always validate user input before processing",
            tags=["security", "validation"],
            scope="global",
            source="security_review_2025",
        )

        bank.add_rule(
            "Use UTC timestamps for all datetime fields",
            tags=["data", "consistency"],
            scope="storage",
        )

        bank.add_rule(
            "Processors must be idempotent—same input yields same output",
            tags=["architecture", "reliability"],
            scope="processors",
        )

        print(f"   Added {len(bank.read_rules())} rules")

        # =====================================================================
        # 2. ADD LEARNINGS (hard-won heuristics)
        # =====================================================================
        print("\n[2] Adding learnings...")

        bank.add_learning(
            "Messages enqueued at tick N are realized at tick N+1",
            tags=["messaging", "tick-semantics"],
            evidence="Debugged in messaging_example.py—processor priority order matters",
            source="session_jan_2025",
        )

        bank.add_learning(
            "LanceDB doesn't support list[dict]; use list[str] with JSON serialization",
            tags=["lancedb", "serialization"],
            evidence="TypeError: Converting Pydantic type to Arrow Type: unsupported type",
        )

        bank.add_learning(
            "@daft.udf is deprecated; use @daft.func.batch for Series→list transforms",
            tags=["daft", "migration"],
            evidence="DeprecationWarning in Daft 0.7.x",
        )

        print(f"   Added {len(bank.read_learnings())} learnings")

        # =====================================================================
        # 3. ADD PROMPTS (reusable operators)
        # =====================================================================
        print("\n[3] Adding prompts...")

        bank.add_prompt(
            name="plan_trajectory",
            purpose="Generate action sequences for agent simulation",
            tags=["planning", "trajectory"],
            content="""Given the environment state:
{state}

Generate a sequence of {num_steps} actions to achieve the goal:
{goal}

For each step, provide:
- action: the action to take
- reasoning: why this action advances the goal
- expected_state: predicted state after action
""",
        )

        bank.add_prompt(
            name="evaluate_rollout",
            purpose="Score a completed trajectory",
            tags=["evaluation", "reward"],
            content="""Evaluate this trajectory:
{trajectory}

Metrics to consider:
- Goal achievement (0-1)
- Efficiency (steps taken vs optimal)
- Safety (any constraint violations?)

Return a JSON object with scores and brief justification.
""",
        )

        print(f"   Added {len(bank.read_prompts())} prompts")

        # =====================================================================
        # 4. RENDER TO MARKDOWN
        # =====================================================================
        print("\n[4] Rendering to markdown...")

        markdown = render_system_prompt(
            rules=bank.read_rules(),
            learnings=bank.read_learnings(),
            prompts=bank.read_prompts(),
            title="Archetype Operating Context",
        )

        print("\n" + "=" * 70)
        print("RENDERED CONTEXT (for LLM consumption)")
        print("=" * 70 + "\n")
        print(markdown)

        # =====================================================================
        # 5. SHOW RAW JSONL (human-greppable, merge-friendly)
        # =====================================================================
        print("=" * 70)
        print("RAW STORAGE (JSONL)")
        print("=" * 70)

        for name, path in [
            ("rules", paths.rules_path),
            ("learnings", paths.learnings_path),
            ("prompts", paths.prompts_path),
        ]:
            if path.exists():
                print(f"\n--- {name}.jsonl ---")
                print(path.read_text()[:500] + "..." if len(path.read_text()) > 500 else path.read_text())

        # =====================================================================
        # 6. KEY INSIGHT
        # =====================================================================
        print("\n" + "=" * 70)
        print("KEY INSIGHT")
        print("=" * 70)
        print("""
The MemoryBank is just structured context:
- Arbitrarily segmentable (rules, learnings, prompts, or any category you add)
- Labelable (tags, scopes, sources)
- Append-only (history-preserving, merge-friendly)
- Renders to markdown (LLM-consumable)

In a simulation context, each world could have its own MemoryBank path:
  .archetype_meta/worlds/{world_id}/context/

When worlds fork, they inherit the parent's context and diverge from there.
This is "git for agent memory"—diffable, branchable, mergeable.
""")


if __name__ == "__main__":
    main()
