# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Provider-neutral ports for pre-durability redaction."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Protocol, runtime_checkable

from pydantic import JsonValue

from archetype.app.redaction.models import (
    RedactedFile,
    RedactedRecord,
    RedactedText,
    RedactionReceipt,
)


@runtime_checkable
class iRedactionService(Protocol):
    """Scan and sanitize values before a durable writer can observe them."""

    @property
    def policy_id(self) -> str: ...

    def redact_text(self, value: str, *, scope: str) -> RedactedText: ...

    def redact_record(
        self,
        value: Mapping[str, JsonValue],
        *,
        scope: str,
    ) -> RedactedRecord: ...

    def assert_safe_metadata(self, value: str, *, field: str) -> RedactionReceipt: ...

    def sanitize_file(
        self,
        source: Path,
        destination: Path,
        *,
        logical_path: str,
    ) -> RedactedFile: ...
