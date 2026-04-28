# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Audit Section H.3 — stale references.

Cheap regex-style scans for pre-rewrite identifiers (``remove_world``,
``MutationService`` outside its home, ``_mutations`` in runtime/) that
the spec explicitly retired.

C.3.1/C.3.2 (deprecated role strings) are already enforced by
``tests/app/test_old_role_cleanup.py``; the manifest points there.

The audit suite itself necessarily *mentions* the strings it forbids,
so this file's own directory is excluded from the scan. Migration
docs (CHANGELOG, migration notes) are excluded where the audit allows.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.audit

ROOT = Path(__file__).resolve().parents[2]
AUDIT_DIR = Path(__file__).resolve().parent
CATEGORIZATION_DOC = ROOT / "docs" / "audits" / "categorization.md"

# Files whose purpose is to describe past or pending migrations; the
# audit explicitly tolerates retired identifiers in these.
MIGRATION_DOCS = {
    ROOT / "CHANGELOG.md",
}


def _iter_files(
    base: Path,
    *,
    suffixes: tuple[str, ...] = (".py",),
    extra_excludes: frozenset[Path] = frozenset(),
) -> list[Path]:
    """All files under `base` with a target suffix, excluding the audit
    suite, migration docs, and any caller-supplied paths.
    """
    excludes = {AUDIT_DIR, CATEGORIZATION_DOC, *MIGRATION_DOCS, *extra_excludes}
    out: list[Path] = []
    for path in base.rglob("*"):
        if not path.is_file():
            continue
        if path.suffix not in suffixes:
            continue
        if any(part == "__pycache__" for part in path.parts):
            continue
        if any(path == ex or ex in path.parents for ex in excludes):
            continue
        out.append(path)
    return sorted(out)


def _grep(paths: list[Path], needle: str) -> list[str]:
    """Return ``"<rel>:<lineno>: <line>"`` for every line containing `needle`."""
    hits: list[str] = []
    for path in paths:
        try:
            text = path.read_text()
        except (UnicodeDecodeError, OSError):
            continue
        for lineno, line in enumerate(text.splitlines(), 1):
            if needle in line:
                rel = path.relative_to(ROOT)
                hits.append(f"{rel}:{lineno}: {line.strip()}")
    return hits


# ── H.3.1 — no remove_world in active code ──────────────────────────────


def test_h_3_1_no_remove_world_in_active_code() -> None:
    """The post-rewrite API uses `destroy_world`. `remove_world` survives only
    in CHANGELOG / migration notes."""
    targets = _iter_files(ROOT / "src")
    hits = _grep(targets, "remove_world")
    assert not hits, (
        "Stale `remove_world` reference in src/:\n  - " + "\n  - ".join(hits)
    )


# ── H.3.2 — no MutationService outside app/ ─────────────────────────────


def test_h_3_2_no_mutation_service_outside_app() -> None:
    """`MutationService` is internal to app/; surface layers must not name it."""
    targets = (
        _iter_files(ROOT / "src" / "archetype" / "runtime")
        + _iter_files(ROOT / "src" / "archetype" / "api")
        + _iter_files(ROOT / "src" / "archetype" / "cli")
    )
    hits = _grep(targets, "MutationService")
    assert not hits, (
        "MutationService leaked outside app/:\n  - " + "\n  - ".join(hits)
    )


# ── H.3.3 — no _mutations in runtime/ ──────────────────────────────────


def test_h_3_3_no_underscore_mutations_in_runtime() -> None:
    """The pre-rewrite `_mutations` attribute was renamed to `_commands`."""
    targets = _iter_files(ROOT / "src" / "archetype" / "runtime")
    hits = _grep(targets, "_mutations")
    assert not hits, (
        "Stale `_mutations` reference in runtime/:\n  - " + "\n  - ".join(hits)
    )
