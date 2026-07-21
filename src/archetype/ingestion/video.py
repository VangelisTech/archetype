# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Video metadata index transforms."""

from daft import DataFrame, col
from daft.functions import video_file, video_metadata

ARTIFACT_VIDEO = "artifact_video"


def video_index(files: DataFrame) -> DataFrame:
    """Attach video container metadata and derived duration."""

    video = files.where(
        files["media_family"] == "video"  # ty: ignore[invalid-argument-type]
    )
    video = video.with_column("_video", video_file(col("file")))
    video = video.with_column("_metadata", video_metadata(col("_video")))
    video = video.select("artifact_id", col("_metadata").unnest())
    return video.with_column("duration_seconds", col("frame_count") / col("fps"))
