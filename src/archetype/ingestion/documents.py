# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Document metadata index transforms."""

from __future__ import annotations

from typing import IO, Any, cast

import daft
from daft import DataFrame, DataType, col
from pypdf import PdfReader

from archetype.ingestion.contracts import IngestionTable

ARTIFACT_PDF = IngestionTable("artifact_pdf", key_columns=("artifact_id",))
_PDF_METADATA = DataType.struct(
    {
        "page_count": DataType.int64(),
        "encrypted": DataType.bool(),
        "title": DataType.string(),
        "author": DataType.string(),
    }
)


@daft.func(return_dtype=_PDF_METADATA)
def _pdf_metadata(file: daft.File) -> dict[str, Any]:
    with file.open() as stream:
        reader = PdfReader(cast(IO[Any], stream))
        metadata = reader.metadata
        return {
            "page_count": len(reader.pages),
            "encrypted": reader.is_encrypted,
            "title": str(metadata.title or "") if metadata is not None else "",
            "author": str(metadata.author or "") if metadata is not None else "",
        }


def pdf_index(files: DataFrame) -> DataFrame:
    """Attach bounded PDF catalog metadata without extracting document content."""

    pdfs = files.where(
        files["media_family"] == "pdf"  # ty: ignore[invalid-argument-type]
    )
    pdfs = pdfs.with_column("_metadata", _pdf_metadata(col("file")))
    return pdfs.select("artifact_id", col("_metadata").unnest())
