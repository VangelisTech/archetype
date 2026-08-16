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
"""

import argparse
import datetime
import re
import sys
from pathlib import Path

COPYRIGHT_RE = re.compile(r"Copyright \d{4}(?:-\d{4})? Vangelis Technologies Inc\.")
LICENSE_MARKERS = ("SPDX-License-Identifier: Apache-2.0", "Apache License")
WORKSPACE_SOURCE_ROOTS = (
    Path("packages/archetype-ecs/src"),
    Path("packages/archetype-missions/src"),
    Path("packages/archetype-physical-ai/src"),
    Path("packages/archetype-research/src"),
)

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
    # Skip __pycache__ directories and .pyc files
    if "__pycache__" in str(file_path) or file_path.suffix == ".pyc":
        return True

    # Skip files in .git directory
    if ".git" in str(file_path):
        return True

    # Skip setup.py files that might have special requirements
    if file_path.name == "setup.py":
        return True

    return False


def default_python_files(project_root: Path) -> list[Path]:
    """Return Python sources from every required workspace distribution."""

    missing = [
        relative for relative in WORKSPACE_SOURCE_ROOTS if not (project_root / relative).is_dir()
    ]
    if missing:
        rendered = ", ".join(str(relative) for relative in missing)
        raise FileNotFoundError(f"workspace source roots are incomplete; missing: {rendered}")
    return sorted(
        path
        for relative in WORKSPACE_SOURCE_ROOTS
        for path in (project_root / relative).rglob("*.py")
        if path.is_file()
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check and add Apache 2.0 license headers to Python files"
    )
    parser.add_argument(
        "--fix", action="store_true", help="Automatically add missing license headers"
    )
    parser.add_argument("files", nargs="*", help="Files to check")

    args = parser.parse_args()

    if not args.files:
        # If no files are specified, audit all four published source trees.
        project_root = Path(__file__).parent.parent
        try:
            python_files = default_python_files(project_root)
        except FileNotFoundError as error:
            print(error)
            return 1
    else:
        python_files = [Path(f) for f in args.files if f.endswith(".py")]

    missing_headers = []

    for file_path in python_files:
        if should_skip_file(file_path):
            continue

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

    if missing_headers:
        print("The following files are missing Apache 2.0 license headers:")
        for file_path in missing_headers:
            print(f"  {file_path}")
        if not args.fix:
            print("\nRun with --fix to automatically add license headers.")
        return 1

    print("All Python files have proper license headers.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
