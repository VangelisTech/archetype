from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from uuid import uuid4


def _utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat(timespec="seconds")


def _ensure_jsonable_tags(tags: Iterable[str] | None) -> list[str]:
    if not tags:
        return []
    cleaned: list[str] = []
    for tag in tags:
        t = str(tag).strip()
        if not t:
            continue
        cleaned.append(t)
    # Stable order, de-dupe.
    return sorted(set(cleaned))


@dataclass(frozen=True)
class MemoryBankPaths:
    """
    File locations for the memory bank.

    Defaults to a repo-local scratch directory so teams can opt-in to commit
    a curated memory bank later, without polluting normal diffs.
    """

    root_dir: Path

    @property
    def rules_path(self) -> Path:
        return self.root_dir / "rules.jsonl"

    @property
    def learnings_path(self) -> Path:
        return self.root_dir / "learnings.jsonl"

    @property
    def prompts_path(self) -> Path:
        return self.root_dir / "prompts.jsonl"

    @classmethod
    def default(cls, *, cwd: Path | None = None) -> "MemoryBankPaths":
        base = (cwd or Path.cwd()) / ".archetype_meta" / "memory_bank"
        return cls(root_dir=base)


class MemoryBank:
    """
    Append-only JSONL store for rules, learnings, and prompts.

    Why JSONL:
    - human-greppable
    - merge-friendly-ish
    - append-only makes it hard to accidentally destroy history
    """

    def __init__(self, paths: MemoryBankPaths):
        self._paths = paths
        self._paths.root_dir.mkdir(parents=True, exist_ok=True)

    @property
    def paths(self) -> MemoryBankPaths:
        return self._paths

    def add_rule(
        self,
        content: str,
        *,
        tags: Iterable[str] | None = None,
        scope: str | None = None,
        source: str | None = None,
    ) -> dict[str, Any]:
        return self._append(
            self._paths.rules_path,
            {
                "id": str(uuid4()),
                "created_at": _utc_now_iso(),
                "kind": "rule",
                "scope": scope,
                "tags": _ensure_jsonable_tags(tags),
                "source": source,
                "content": content.strip(),
            },
        )

    def add_learning(
        self,
        content: str,
        *,
        tags: Iterable[str] | None = None,
        evidence: str | None = None,
        source: str | None = None,
    ) -> dict[str, Any]:
        return self._append(
            self._paths.learnings_path,
            {
                "id": str(uuid4()),
                "created_at": _utc_now_iso(),
                "kind": "learning",
                "tags": _ensure_jsonable_tags(tags),
                "evidence": evidence,
                "source": source,
                "content": content.strip(),
            },
        )

    def add_prompt(
        self,
        name: str,
        content: str,
        *,
        tags: Iterable[str] | None = None,
        purpose: str | None = None,
        source: str | None = None,
    ) -> dict[str, Any]:
        name_clean = name.strip()
        if not name_clean:
            raise ValueError("Prompt name must be non-empty.")
        return self._append(
            self._paths.prompts_path,
            {
                "id": str(uuid4()),
                "created_at": _utc_now_iso(),
                "kind": "prompt",
                "name": name_clean,
                "purpose": purpose,
                "tags": _ensure_jsonable_tags(tags),
                "source": source,
                "content": content.rstrip() + "\n",
            },
        )

    def read_rules(self) -> list[dict[str, Any]]:
        return self._read_all(self._paths.rules_path)

    def read_learnings(self) -> list[dict[str, Any]]:
        return self._read_all(self._paths.learnings_path)

    def read_prompts(self) -> list[dict[str, Any]]:
        return self._read_all(self._paths.prompts_path)

    def _append(self, path: Path, record: dict[str, Any]) -> dict[str, Any]:
        if not record.get("content"):
            raise ValueError("Record content must be non-empty.")
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
        return record

    def _read_all(self, path: Path) -> list[dict[str, Any]]:
        if not path.exists():
            return []
        out: list[dict[str, Any]] = []
        with path.open("r", encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line:
                    continue
                out.append(json.loads(line))
        return out

