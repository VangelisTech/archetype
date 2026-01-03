from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from typing import Any


def _fmt_dt(iso: str | None) -> str:
    if not iso:
        return ""
    try:
        # Accept typical ISO-8601, keep date+time short.
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%MZ")
    except Exception:
        return iso


def render_system_prompt(
    *,
    rules: Iterable[dict[str, Any]],
    learnings: Iterable[dict[str, Any]],
    prompts: Iterable[dict[str, Any]],
    title: str = "Operating Context (generated)",
) -> str:
    """
    Render a single markdown document suitable as a system prompt seed.

    This is intentionally opinionated:
    - rules first (constraints),
    - learnings second (hard-won heuristics),
    - prompts last (reusable operators).
    """

    lines: list[str] = []
    lines.append(f"# {title}")
    lines.append("")

    rules_list = list(rules)
    learnings_list = list(learnings)
    prompts_list = list(prompts)

    lines.append("## Rules")
    if not rules_list:
        lines.append("_None yet._")
    else:
        for r in rules_list:
            scope = r.get("scope")
            tags = r.get("tags") or []
            meta = " · ".join([x for x in [scope, ", ".join(tags) if tags else None] if x])
            meta_str = f" ({meta})" if meta else ""
            created = _fmt_dt(r.get("created_at"))
            created_str = f" — {created}" if created else ""
            content = str(r.get("content", "")).strip()
            if not content:
                continue
            lines.append(f"- {content}{meta_str}{created_str}")
    lines.append("")

    lines.append("## Learnings")
    if not learnings_list:
        lines.append("_None yet._")
    else:
        for l in learnings_list:
            tags = l.get("tags") or []
            tags_str = f" [{', '.join(tags)}]" if tags else ""
            created = _fmt_dt(l.get("created_at"))
            created_str = f" — {created}" if created else ""
            content = str(l.get("content", "")).strip()
            if not content:
                continue
            lines.append(f"- {content}{tags_str}{created_str}")
    lines.append("")

    lines.append("## Prompt Library")
    if not prompts_list:
        lines.append("_None yet._")
    else:
        for p in prompts_list:
            name = str(p.get("name", "")).strip() or "(unnamed)"
            purpose = str(p.get("purpose", "")).strip()
            tags = p.get("tags") or []
            header_bits = [name]
            if purpose:
                header_bits.append(f"— {purpose}")
            if tags:
                header_bits.append(f"[{', '.join(tags)}]")
            lines.append(f"### { ' '.join(header_bits) }")
            lines.append("")
            lines.append("```")
            lines.append(str(p.get("content", "")).rstrip())
            lines.append("```")
            lines.append("")

    return "\n".join(lines).rstrip() + "\n"

