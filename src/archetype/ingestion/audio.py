# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Audio metadata index transforms."""

from daft import DataFrame, col
from daft.functions import audio_file, audio_metadata

from archetype.ingestion.contracts import IngestionTable

ARTIFACT_AUDIO = IngestionTable("artifact_audio", key_columns=("artifact_id",))


def audio_index(files: DataFrame) -> DataFrame:
    """Attach audio container metadata and derived duration."""

    audio = files.where(
        files["media_family"] == "audio"  # ty: ignore[invalid-argument-type]
    )
    audio = audio.with_column("_audio", audio_file(col("file")))
    audio = audio.with_column("_metadata", audio_metadata(col("_audio")))
    audio = audio.select("artifact_id", col("_metadata").unnest())
    return audio.with_column("duration_seconds", col("frames") / col("sample_rate"))
