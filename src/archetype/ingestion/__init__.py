# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Reusable contracts and lazy transforms for typed ingestion pipelines."""

from archetype.ingestion.audio import ARTIFACT_AUDIO, audio_index
from archetype.ingestion.contracts import IngestionTable, TableVersion
from archetype.ingestion.documents import ARTIFACT_PDF, pdf_index
from archetype.ingestion.files import ARTIFACT_FILES, common_index, logical_path_for, scan_files
from archetype.ingestion.images import ARTIFACT_IMAGES, image_index
from archetype.ingestion.video import ARTIFACT_VIDEO, video_index

__all__ = [
    "ARTIFACT_AUDIO",
    "ARTIFACT_FILES",
    "ARTIFACT_IMAGES",
    "ARTIFACT_PDF",
    "ARTIFACT_VIDEO",
    "IngestionTable",
    "TableVersion",
    "audio_index",
    "common_index",
    "image_index",
    "logical_path_for",
    "pdf_index",
    "scan_files",
    "video_index",
]
