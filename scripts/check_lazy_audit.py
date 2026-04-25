# Copyright 2025 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Lazy-evaluation audit.

Daft is lazily evaluated. DataFrames represent computations, not results.
Calling ``.collect()`` or ``.to_pylist()`` forces the frame through Python
memory, defeats query planning, and stops scaling at in-memory size.

Every such call inside ``src/`` is a contract exception against Archetype's
lazy execution model. This script enumerates production call sites and
gates them against ``lazy_audit.toml``. New, undocumented sites cause a
non-zero exit; stale entries (allowlisted lines that no longer hold a
matching call) are also surfaced so the audit stays honest under refactors.

Tests are intentionally out of scope: terminal materialization at the
assertion boundary is expected. The contract being audited is the
production execution model, not test ergonomics.

Run via ``make lazy-audit`` or as a pre-commit hook.
"""

from __future__ import annotations

import argparse
import re
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path

ROOTS: tuple[str, ...] = ("src",)
ALLOWLIST_FILENAME = "lazy_audit.toml"
SELF_RELATIVE = "scripts/check_lazy_audit.py"

# Match attribute-style calls only. Whitespace before the dot avoids hits on
# e.g. ``module.collect(`` where ``collect`` is a free function in another
# package — the audit is about chained DataFrame methods, not arbitrary names.
PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("collect", re.compile(r"\.collect\s*\(")),
    ("to_pylist", re.compile(r"\.to_pylist\s*\(")),
)


@dataclass(frozen=True)
class Site:
    path: str
    line: int
    method: str
    snippet: str


@dataclass(frozen=True)
class Entry:
    path: str
    line: int
    method: str
    reason: str


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _scan_file(path: Path, rel: str) -> list[Site]:
    sites: list[Site] = []
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return sites
    for n, raw in enumerate(text.splitlines(), start=1):
        stripped = raw.lstrip()
        if stripped.startswith("#"):
            continue
        for method, pat in PATTERNS:
            if pat.search(raw):
                sites.append(Site(path=rel, line=n, method=method, snippet=stripped))
    return sites


def scan(root: Path) -> list[Site]:
    sites: list[Site] = []
    for top in ROOTS:
        base = root / top
        if not base.exists():
            continue
        for path in sorted(base.rglob("*.py")):
            rel = path.relative_to(root).as_posix()
            if rel == SELF_RELATIVE:
                continue
            sites.extend(_scan_file(path, rel))
    return sites


def load_allowlist(root: Path) -> tuple[list[Entry], str | None]:
    path = root / ALLOWLIST_FILENAME
    if not path.exists():
        return [], f"missing {ALLOWLIST_FILENAME}"
    try:
        data = tomllib.loads(path.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as exc:
        return [], f"could not parse {ALLOWLIST_FILENAME}: {exc}"
    raw_entries = data.get("entries", [])
    out: list[Entry] = []
    for raw in raw_entries:
        try:
            out.append(
                Entry(
                    path=str(raw["path"]),
                    line=int(raw["line"]),
                    method=str(raw["method"]),
                    reason=str(raw.get("reason", "")).strip(),
                )
            )
        except (KeyError, TypeError, ValueError) as exc:
            return [], f"malformed entry in {ALLOWLIST_FILENAME}: {raw!r} ({exc})"
    return out, None


_BANNED_REASON_TOKENS = {
    "",
    "todo",
    "fixme",
    "needed",
    "necessary",
    "required",
    "for the test",
    "for tests",
    "easier",
    "convenience",
    "tbd",
}


def _reason_is_substantive(reason: str) -> bool:
    norm = reason.strip().lower().rstrip(".")
    if norm in _BANNED_REASON_TOKENS:
        return False
    return len(norm) >= 20


STERN_HEADER = (
    "─────────────────────────────────────────────────────────────────────\n"
    "LAZY EXECUTION AUDIT FAILED\n"
    "─────────────────────────────────────────────────────────────────────"
)

STERN_BODY = """\
Daft is lazily evaluated. DataFrames represent computations, not results.
.collect() and .to_pylist() force the frame through Python memory, defeat
query planning, and stop scaling at in-memory dataset sizes. Every such
call is a contract exception against Archetype's lazy execution model.

If you are reading this, the most likely answer is to rewrite the
expression in Daft. Reach for where, select, with_column, agg, join,
sort, distinct, count_rows before pulling rows into Python. See
LEARNINGS.md and docs/guide/specification.md for the lazy contract.

If the materialization is genuinely unavoidable (storage write boundary,
single-row migration extract, debug logging, terminal test assertion),
the exception must be documented in writing and visible in code review:

  * The reason field in lazy_audit.toml must state the technical reason
    the boundary cannot be expressed lazily. Generic phrases ("needed",
    "for the test", "convenience") are rejected automatically.
  * The new entry must be called out in the PR description or as a PR
    comment so a human reviewer signs off on the exception explicitly.

Quietly adding entries to silence this check is itself a signal that
the change is gaming the lazy-evaluation contract. PRs that do this
will be reverted at review.
"""


def _format_section(title: str, lines: list[str]) -> str:
    if not lines:
        return ""
    return f"\n{title}\n" + "\n".join(f"  {ln}" for ln in lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--list",
        action="store_true",
        help="Print every detected materialization site and exit 0 (no gating).",
    )
    parser.add_argument(
        "files",
        nargs="*",
        help="Optional file list (used by pre-commit). The full repo is always scanned.",
    )
    args = parser.parse_args()
    del args  # we always scan the full repo so a moved call elsewhere can't slip through

    root = _project_root()
    sites = scan(root)

    if "--list" in sys.argv:
        for s in sites:
            print(f"{s.path}:{s.line}  .{s.method}()  {s.snippet}")
        return 0

    allow, allow_err = load_allowlist(root)
    if allow_err is not None:
        print(STERN_HEADER, file=sys.stderr)
        print(f"\nallowlist error: {allow_err}\n", file=sys.stderr)
        print(STERN_BODY, file=sys.stderr)
        return 2

    site_keys = {(s.path, s.line, s.method): s for s in sites}
    allow_keys = {(e.path, e.line, e.method): e for e in allow}

    new_sites = [site_keys[k] for k in site_keys.keys() - allow_keys.keys()]
    stale_entries = [allow_keys[k] for k in allow_keys.keys() - site_keys.keys()]
    weak_reasons = [e for e in allow if not _reason_is_substantive(e.reason)]

    new_sites.sort(key=lambda s: (s.path, s.line))
    stale_entries.sort(key=lambda e: (e.path, e.line))
    weak_reasons.sort(key=lambda e: (e.path, e.line))

    if not (new_sites or stale_entries or weak_reasons):
        print(f"lazy audit: {len(sites)} site(s), all accounted for.")
        return 0

    print(STERN_HEADER, file=sys.stderr)

    if new_sites:
        rendered = [f"{s.path}:{s.line}  .{s.method}()  {s.snippet}" for s in new_sites]
        sys.stderr.write(_format_section("New, undocumented materialization points:", rendered))

    if stale_entries:
        rendered = [f"{e.path}:{e.line}  .{e.method}()  reason={e.reason!r}" for e in stale_entries]
        sys.stderr.write(
            _format_section(
                "Stale allowlist entries (line no longer holds a matching call):",
                rendered,
            )
        )

    if weak_reasons:
        rendered = [f"{e.path}:{e.line}  .{e.method}()  reason={e.reason!r}" for e in weak_reasons]
        sys.stderr.write(
            _format_section(
                "Allowlist entries with unjustified reasons (rejected at review):",
                rendered,
            )
        )

    print(file=sys.stderr)
    print(STERN_BODY, file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
