# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Text-family predicates; content extraction belongs to derivative workflows."""

from daft import DataFrame


def text_files(files: DataFrame) -> DataFrame:
    """Select text-like files without loading their contents into the index."""

    return files.where(
        files["media_family"] == "text"  # ty: ignore[invalid-argument-type]
    )
