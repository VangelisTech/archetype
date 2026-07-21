# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Image metadata index transforms."""

from daft import DataFrame, col
from daft.functions import image_file, image_file_metadata

from archetype.ingestion.contracts import IngestionTable

ARTIFACT_IMAGES = IngestionTable("artifact_images", key_columns=("artifact_id",))


def image_index(files: DataFrame) -> DataFrame:
    """Attach header-only image metadata and unnest it into index columns."""

    images = files.where(
        files["media_family"] == "image"  # ty: ignore[invalid-argument-type]
    )
    images = images.with_column("_image", image_file(col("file")))
    images = images.with_column("_metadata", image_file_metadata(col("_image")))
    return images.select("artifact_id", col("_metadata").unnest())
