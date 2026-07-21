# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Pure bounded stream parsers used by the file-ingestion Daft graph."""

from __future__ import annotations

import codecs
import hashlib
from io import BytesIO
from pathlib import PurePosixPath
from typing import Any, BinaryIO

import xxhash
from pypdf import PdfReader

_COPY_BUFFER = 1 << 20
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


def hash_file(stream: BinaryIO) -> dict[str, str | int]:
    """Compute SHA-256, XXH3-64, and size in one streaming pass."""

    sha256 = hashlib.sha256()
    fast = xxhash.xxh3_64()
    size = 0
    while chunk := stream.read(_COPY_BUFFER):
        sha256.update(chunk)
        fast.update(chunk)
        size += len(chunk)
    return {
        "sha256": sha256.hexdigest(),
        "xxhash3_64": fast.hexdigest(),
        "size_bytes": size,
    }


def scan_pdf_metadata(payload: bytes) -> dict[str, Any]:
    """Read bounded PDF catalog metadata from an in-memory artifact."""

    reader = PdfReader(BytesIO(payload))
    metadata = reader.metadata
    return {
        "page_count": len(reader.pages),
        "encrypted": reader.is_encrypted,
        "title": str(metadata.title or "") if metadata is not None else "",
        "author": str(metadata.author or "") if metadata is not None else "",
    }


def scan_text_metadata(
    stream: BinaryIO,
    logical_path: str,
) -> dict[str, str | int | bool]:
    """Scan text shape, language, and UTF-8 validity without retaining content."""

    decoder = codecs.getincrementaldecoder("utf-8")("strict")
    utf8 = True
    line_count = 0
    size = 0
    final_byte = b""
    while chunk := stream.read(_COPY_BUFFER):
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
    if language in {"markdown", "mdx"}:
        text_kind = "markdown"
    elif language == "diff":
        text_kind = "diff"
    elif language in {"json", "jsonl", "toml", "yaml"}:
        text_kind = "structured_text"
    elif language in _SOURCE_LANGUAGES:
        text_kind = "source_code"
    else:
        text_kind = "plain_text"
    return {
        "text_kind": text_kind,
        "language": language,
        "line_count": line_count,
        "utf8": utf8,
    }


def scan_diff_metadata(stream: BinaryIO) -> dict[str, str | int]:
    """Scan unified or Git patch structure without retaining diff content."""

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
    while chunk := stream.read(_COPY_BUFFER):
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
