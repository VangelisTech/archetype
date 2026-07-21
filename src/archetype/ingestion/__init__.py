# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Reusable lazy file-ingestion pipeline and pure scanners."""

from archetype.ingestion.pipeline import (
    ARTIFACT_AUDIO,
    ARTIFACT_DIFF,
    ARTIFACT_FILES,
    ARTIFACT_IMAGES,
    ARTIFACT_PDF,
    ARTIFACT_TEXT,
    ARTIFACT_VIDEO,
    FileIngestionPipeline,
    ingestion_time_for,
    media_family_for,
)
from archetype.ingestion.scanners import (
    hash_file,
    scan_diff_metadata,
    scan_pdf_metadata,
    scan_text_metadata,
)

__all__ = [
    "ARTIFACT_AUDIO",
    "ARTIFACT_DIFF",
    "ARTIFACT_FILES",
    "ARTIFACT_IMAGES",
    "ARTIFACT_PDF",
    "ARTIFACT_TEXT",
    "ARTIFACT_VIDEO",
    "FileIngestionPipeline",
    "hash_file",
    "ingestion_time_for",
    "media_family_for",
    "scan_diff_metadata",
    "scan_pdf_metadata",
    "scan_text_metadata",
]
