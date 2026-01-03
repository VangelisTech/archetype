from __future__ import annotations

import argparse
import sys
from pathlib import Path

from archetype.memory_bank.render import render_system_prompt
from archetype.memory_bank.store import MemoryBank, MemoryBankPaths


def _parse_tags(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [t.strip() for t in raw.split(",") if t.strip()]


def _read_stdin_if_empty(value: str | None) -> str:
    if value is not None and value.strip():
        return value
    data = sys.stdin.read()
    if not data.strip():
        raise SystemExit("Error: missing content (provide arg or pipe via stdin).")
    return data


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m archetype.memory_bank",
        description="Local memory bank for rules/learnings/prompts.",
    )
    p.add_argument(
        "--dir",
        dest="dir_",
        default=None,
        help="Storage directory (default: .archetype_meta/memory_bank in current dir).",
    )

    sub = p.add_subparsers(dest="cmd", required=True)

    add_rule = sub.add_parser("add-rule", help="Add a rule (constraint).")
    add_rule.add_argument("content", nargs="?", help="Rule text (or pipe via stdin).")
    add_rule.add_argument("--tags", default=None, help="Comma-separated tags.")
    add_rule.add_argument("--scope", default=None, help="Optional scope (e.g. python, security).")
    add_rule.add_argument("--source", default=None, help="Optional source reference.")

    add_learning = sub.add_parser("add-learning", help="Add a learning (heuristic).")
    add_learning.add_argument("content", nargs="?", help="Learning text (or pipe via stdin).")
    add_learning.add_argument("--tags", default=None, help="Comma-separated tags.")
    add_learning.add_argument("--evidence", default=None, help="Optional evidence (file, PR, link).")
    add_learning.add_argument("--source", default=None, help="Optional source reference.")

    add_prompt = sub.add_parser("add-prompt", help="Add a reusable prompt.")
    add_prompt.add_argument("--name", required=True, help="Prompt name.")
    add_prompt.add_argument(
        "--content",
        default=None,
        help="Prompt content (if omitted, read from stdin).",
    )
    add_prompt.add_argument("--purpose", default=None, help="Optional purpose line.")
    add_prompt.add_argument("--tags", default=None, help="Comma-separated tags.")
    add_prompt.add_argument("--source", default=None, help="Optional source reference.")

    render = sub.add_parser("render", help="Render combined operating context.")
    render.add_argument("--title", default="Operating Context (generated)")
    render.add_argument("--out", default=None, help="Write to file instead of stdout.")

    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    paths = (
        MemoryBankPaths(root_dir=Path(args.dir_).expanduser())
        if args.dir_
        else MemoryBankPaths.default()
    )
    bank = MemoryBank(paths)

    if args.cmd == "add-rule":
        rec = bank.add_rule(
            _read_stdin_if_empty(args.content),
            tags=_parse_tags(args.tags),
            scope=args.scope,
            source=args.source,
        )
        sys.stdout.write(rec["id"] + "\n")
        return 0

    if args.cmd == "add-learning":
        rec = bank.add_learning(
            _read_stdin_if_empty(args.content),
            tags=_parse_tags(args.tags),
            evidence=args.evidence,
            source=args.source,
        )
        sys.stdout.write(rec["id"] + "\n")
        return 0

    if args.cmd == "add-prompt":
        rec = bank.add_prompt(
            args.name,
            _read_stdin_if_empty(args.content),
            tags=_parse_tags(args.tags),
            purpose=args.purpose,
            source=args.source,
        )
        sys.stdout.write(rec["id"] + "\n")
        return 0

    if args.cmd == "render":
        text = render_system_prompt(
            rules=bank.read_rules(),
            learnings=bank.read_learnings(),
            prompts=bank.read_prompts(),
            title=args.title,
        )
        if args.out:
            out = Path(args.out).expanduser()
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(text, encoding="utf-8")
        else:
            sys.stdout.write(text)
        return 0

    raise SystemExit(f"Unknown command: {args.cmd}")


if __name__ == "__main__":
    raise SystemExit(main())

