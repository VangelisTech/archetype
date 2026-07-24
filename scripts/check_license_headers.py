#!/usr/bin/env python3
"""
Pre-commit hook to ensure Apache 2.0 license headers are present in Python files.

Two header conventions are valid, and both must stay valid: the original
long-form Apache header (2025-era files) and the compact SPDX form newer
files use:

    # Copyright 2026 Vangelis Technologies Inc.
    # SPDX-License-Identifier: Apache-2.0

The check is year-agnostic. Recognizing only one literal year made every
compact-header file read as "missing" — and `--fix` then prepended a second,
stale-dated copyright block onto 130+ correctly headered files while exiting 0.
`--fix` now refuses to touch any file that already carries a copyright line it
cannot classify, and a missing or refused header always exits 1.

The gate also fails loudly rather than silently: an empty audit set and a path
that does not exist are errors, not passes, and skip rules match whole path
components so `.github/` is audited instead of being swallowed by a `.git`
substring match.
"""

import argparse
import datetime
import re
import sys
from collections.abc import Sequence
from pathlib import Path

COPYRIGHT_RE = re.compile(r"Copyright \d{4}(?:-\d{4})? Vangelis Technologies Inc\.")
LICENSE_MARKERS = ("SPDX-License-Identifier: Apache-2.0", "Apache License")

# Headers live at the top of the file. Only inspect the head so a docstring or
# string literal that merely mentions the license cannot satisfy the check.
HEADER_SEARCH_LIMIT = 2048


def _license_header_template() -> str:
    """Compact SPDX header for newly stamped files, dated to the current year."""
    year = datetime.date.today().year
    return (
        f"# Copyright {year} Vangelis Technologies Inc.\n# SPDX-License-Identifier: Apache-2.0\n\n"
    )


def _file_head(file_path: Path) -> str | None:
    try:
        with open(file_path, encoding="utf-8") as f:
            return f.read(HEADER_SEARCH_LIMIT)
    except (OSError, UnicodeDecodeError):
        return None


def has_license_header(file_path: Path) -> bool:
    """Check if a file already has a valid Apache license header."""
    head = _file_head(file_path)
    if head is None:
        return False
    return bool(COPYRIGHT_RE.search(head)) and any(marker in head for marker in LICENSE_MARKERS)


def add_license_header(file_path: Path) -> bool:
    """Add the compact SPDX license header to a file.

    Returns True only when the file was rewritten. A file that already carries
    a Vangelis copyright line is never rewritten: it failed the full check, so
    its header is malformed in a way this script must not guess at — stacking
    a second copyright block on top of it would hide the problem behind a
    green exit.
    """
    try:
        with open(file_path, encoding="utf-8") as f:
            content = f.read()

        # Skip if it's an empty file
        if not content.strip():
            return False

        if COPYRIGHT_RE.search(content[:HEADER_SEARCH_LIMIT]):
            print(
                f"Refusing to stack a second header on {file_path}: it already "
                "contains a Vangelis copyright line but no recognized license "
                "marker. Repair the existing header by hand."
            )
            return False

        # Handle files that start with shebang
        header = _license_header_template()
        lines = content.splitlines(keepends=True)
        if lines and lines[0].startswith("#!"):
            # Insert license after shebang
            new_content = lines[0] + header + "".join(lines[1:])
        else:
            # Insert license at the beginning
            new_content = header + content

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(new_content)

        return True
    except (OSError, UnicodeDecodeError) as e:
        print(f"Error processing {file_path}: {e}")
        return False


def should_skip_file(file_path: Path) -> bool:
    """Check if a file should be skipped for license header checking."""
    # Match whole path components. A substring test for ".git" also matched
    # ".github/...", silently exempting every workflow helper from the gate —
    # a hole that is invisible precisely because it reports success.
    if {".git", "__pycache__"}.intersection(file_path.parts) or file_path.suffix == ".pyc":
        return True

    # Skip setup.py files that might have special requirements
    if file_path.name == "setup.py":
        return True

    return False


def resolve_files(names: Sequence[str]) -> tuple[list[Path], list[str]]:
    """Resolve the audit set, naming every reason it might be unusable.

    Returns ``(files, errors)``. An empty audit set is an error rather than a
    pass: a gate that inspects nothing and prints success is how this checker
    stayed green while most of the tree drifted out from under it.
    """
    errors: list[str] = []

    if names:
        candidates = [Path(name) for name in names if name.endswith(".py")]
    else:
        # No files specified: audit every Python file under src/.
        src_dir = Path(__file__).parent.parent / "src"
        if not src_dir.is_dir():
            return [], [f"source root does not exist: {src_dir}"]
        candidates = sorted(src_dir.glob("**/*.py"))

    files: list[Path] = []
    for path in candidates:
        if should_skip_file(path):
            continue
        # A path that does not exist is a bad argument, not an unlicensed
        # file. Reporting it as "missing a license header" sends the reader
        # off to add a header to a file that is not there.
        if not path.exists():
            errors.append(f"path does not exist: {path}")
            continue
        files.append(path)

    if not files and not errors:
        errors.append("no Python files to audit — an empty file set is an error, not a pass")

    return files, errors


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check and add Apache 2.0 license headers to Python files"
    )
    parser.add_argument(
        "--fix", action="store_true", help="Automatically add missing license headers"
    )
    parser.add_argument("files", nargs="*", help="Files to check")

    args = parser.parse_args()

    python_files, errors = resolve_files(args.files)

    missing_headers = []

    for file_path in python_files:
        if not has_license_header(file_path):
            missing_headers.append(file_path)

            if args.fix:
                if add_license_header(file_path):
                    print(f"Added license header to {file_path}")
                else:
                    print(f"Failed to add license header to {file_path}")

    if args.fix:
        # A refused or failed fix must keep the run red; exiting 0 here is how
        # 130+ files were once rewritten wrongly under a green exit.
        missing_headers = [f for f in missing_headers if not has_license_header(f)]

    for message in errors:
        print(f"error: {message}")

    if missing_headers:
        print("The following files are missing Apache 2.0 license headers:")
        for file_path in missing_headers:
            print(f"  {file_path}")
        if not args.fix:
            print("\nRun with --fix to automatically add license headers.")

    if missing_headers or errors:
        return 1

    print(f"All {len(python_files)} Python files have proper license headers.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
