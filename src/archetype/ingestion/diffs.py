# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Structural indexes for unified and Git patch artifacts."""

from __future__ import annotations

from typing import BinaryIO, cast

import daft
from daft import DataFrame, DataType, col

ARTIFACT_DIFF = "artifact_diff"
_DIFF_METADATA = DataType.struct(
    {
        "format": DataType.string(),
        "file_count": DataType.int64(),
        "hunk_count": DataType.int64(),
        "additions": DataType.int64(),
        "deletions": DataType.int64(),
        "binary_file_count": DataType.int64(),
    }
)


@daft.func(return_dtype=DataType.bool())
def _is_diff(logical_path: str) -> bool:
    return logical_path.lower().endswith((".diff", ".patch"))


@daft.func(return_dtype=_DIFF_METADATA)
def _diff_metadata(file: daft.File) -> dict[str, str | int]:
    with file.open() as opened:
        return scan_diff_metadata(cast(BinaryIO, opened))


def scan_diff_metadata(stream: BinaryIO) -> dict[str, str | int]:
    """Scan unified or Git patch structure from one binary stream."""

    git_files = 0
    unified_files = 0
    hunk_count = 0
    additions = 0
    deletions = 0
    binary_files = 0

    def observe(raw_line: bytes) -> None:
        nonlocal git_files, unified_files, hunk_count, additions, deletions, binary_files
        line = raw_line.decode("utf-8", errors="replace")
        if line.startswith("diff --git "):
            git_files += 1
        elif line.startswith("+++ "):
            unified_files += 1
        elif line.startswith("@@"):
            hunk_count += 1
        elif line.startswith("+") and not line.startswith("+++"):
            additions += 1
        elif line.startswith("-") and not line.startswith("---"):
            deletions += 1
        elif line.startswith("GIT binary patch") or (
            line.startswith("Binary files ") and line.rstrip().endswith(" differ")
        ):
            binary_files += 1

    buffered = b""
    while chunk := stream.read(1 << 20):
        lines = (buffered + chunk).split(b"\n")
        buffered = lines.pop()
        for line in lines:
            observe(line)
    if buffered:
        observe(buffered)
    return {
        "format": "git" if git_files else "unified",
        "file_count": git_files or unified_files,
        "hunk_count": hunk_count,
        "additions": additions,
        "deletions": deletions,
        "binary_file_count": binary_files,
    }


def diff_index(files: DataFrame) -> DataFrame:
    """Project bounded patch structure without storing duplicate diff content."""

    diffs = files.where(_is_diff(col("logical_path")))
    diffs = diffs.with_column("_metadata", _diff_metadata(col("file")))
    return diffs.select("artifact_id", col("_metadata").unnest())
