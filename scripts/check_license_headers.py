#!/usr/bin/env python3
"""
Pre-commit hook to ensure Apache 2.0 license headers are present in Python files.
"""

import argparse
import sys
from pathlib import Path

APACHE_LICENSE_HEADER = """# Copyright 2025 Vangelis Technologies Inc.
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

"""


def has_license_header(file_path: Path) -> bool:
    """Check if a file already has the Apache license header."""
    try:
        with open(file_path, encoding="utf-8") as f:
            content = f.read()
            return (
                "Copyright 2025 Vangelis Technologies Inc." in content
                and "Apache License" in content
            )
    except (OSError, UnicodeDecodeError):
        return False


def add_license_header(file_path: Path) -> bool:
    """Add the Apache license header to a file."""
    try:
        with open(file_path, encoding="utf-8") as f:
            content = f.read()

        # Skip if it's an empty file
        if not content.strip():
            return False

        # Handle files that start with shebang
        lines = content.splitlines(keepends=True)
        if lines and lines[0].startswith("#!"):
            # Insert license after shebang
            new_content = lines[0] + APACHE_LICENSE_HEADER + "".join(lines[1:])
        else:
            # Insert license at the beginning
            new_content = APACHE_LICENSE_HEADER + content

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
        # If no files specified, check all Python files in the src directory
        project_root = Path(__file__).parent.parent
        src_dir = project_root / "src"
        if src_dir.exists():
            python_files = list(src_dir.glob("**/*.py"))
        else:
            python_files = []
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

    if missing_headers and not args.fix:
        print("The following files are missing Apache 2.0 license headers:")
        for file_path in missing_headers:
            print(f"  {file_path}")
        print("\nRun with --fix to automatically add license headers.")
        return 1

    if not missing_headers:
        print("All Python files have proper license headers.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
