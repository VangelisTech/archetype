# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Bounded text metadata indexes; content remains in the artifact object."""

from __future__ import annotations

import codecs
from pathlib import PurePosixPath
from typing import BinaryIO, cast

import daft
from daft import DataFrame, DataType, col

from archetype.ingestion.contracts import IngestionTable

ARTIFACT_TEXT = IngestionTable("artifact_text", key_columns=("artifact_id",))
_TEXT_METADATA = DataType.struct(
    {
        "text_kind": DataType.string(),
        "language": DataType.string(),
        "line_count": DataType.int64(),
        "utf8": DataType.bool(),
    }
)
_LANGUAGES = {
    ".c": "c",
    ".cc": "cpp",
    ".cpp": "cpp",
    ".css": "css",
    ".diff": "diff",
    ".go": "go",
    ".h": "c",
    ".hpp": "cpp",
    ".html": "html",
    ".java": "java",
    ".js": "javascript",
    ".json": "json",
    ".jsonl": "jsonl",
    ".jsx": "jsx",
    ".md": "markdown",
    ".mdx": "mdx",
    ".ndjson": "jsonl",
    ".patch": "diff",
    ".py": "python",
    ".rs": "rust",
    ".sh": "shell",
    ".sql": "sql",
    ".toml": "toml",
    ".ts": "typescript",
    ".tsx": "tsx",
    ".yaml": "yaml",
    ".yml": "yaml",
}
_SOURCE_LANGUAGES = {
    "c",
    "cpp",
    "css",
    "go",
    "html",
    "java",
    "javascript",
    "jsx",
    "python",
    "rust",
    "shell",
    "sql",
    "tsx",
    "typescript",
}


def _kind(language: str) -> str:
    if language in {"markdown", "mdx"}:
        return "markdown"
    if language == "diff":
        return "diff"
    if language in {"json", "jsonl", "toml", "yaml"}:
        return "structured_text"
    if language in _SOURCE_LANGUAGES:
        return "source_code"
    return "plain_text"


@daft.func(return_dtype=_TEXT_METADATA)
def _text_metadata(file: daft.File, logical_path: str) -> dict[str, str | int | bool]:
    """Scan line count and UTF-8 validity without retaining submitted content."""

    decoder = codecs.getincrementaldecoder("utf-8")("strict")
    utf8 = True
    line_count = 0
    size = 0
    final_byte = b""
    with file.open() as opened:
        stream = cast(BinaryIO, opened)
        while chunk := stream.read(1 << 20):
            size += len(chunk)
            final_byte = chunk[-1:]
            line_count += chunk.count(b"\n")
            if utf8:
                try:
                    decoder.decode(chunk, final=False)
                except UnicodeDecodeError:
                    utf8 = False
        if utf8:
            try:
                decoder.decode(b"", final=True)
            except UnicodeDecodeError:
                utf8 = False
    if size and final_byte != b"\n":
        line_count += 1
    language = _LANGUAGES.get(PurePosixPath(logical_path).suffix.lower(), "text")
    return {
        "text_kind": _kind(language),
        "language": language,
        "line_count": line_count,
        "utf8": utf8,
    }


def text_index(files: DataFrame) -> DataFrame:
    """Attach safe text shape metadata without persisting submitted content."""

    text = files.where(
        files["media_family"] == "text"  # ty: ignore[invalid-argument-type]
    )
    text = text.with_column("_metadata", _text_metadata(col("file"), col("logical_path")))
    return text.select("artifact_id", col("_metadata").unnest())


def text_files(files: DataFrame) -> DataFrame:
    """Select text-like files for derivative workflows."""

    return files.where(files["media_family"] == "text")  # ty: ignore[invalid-argument-type]
