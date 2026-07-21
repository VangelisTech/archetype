# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Document metadata index transforms."""

from __future__ import annotations

from io import BytesIO
from typing import Any

import daft
from daft import DataFrame, DataType, col
from pypdf import PdfReader

ARTIFACT_PDF = "artifact_pdf"
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
    # PdfReader performs many small seeks. Replaying those seeks against an
    # object-store stream turns a metadata scan into hundreds of range reads,
    # so make the single bounded artifact read explicit at this boundary.
    with file.open() as stream:
        payload = stream.read()
    reader = PdfReader(BytesIO(payload))
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
