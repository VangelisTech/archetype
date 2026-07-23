# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Typed values for the pre-durability secret-redaction boundary."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, JsonValue, field_validator, model_validator

from archetype.errors import PayloadRejectedError

RedactionStatus = Literal["clean", "redacted"]


class RedactionPolicyConfig(BaseModel):
    """Bounded archive and stream policy for one scanner implementation.

    Policy configuration is process-local. Only the derived ``policy_id`` is
    permitted to cross a durable boundary.
    """

    model_config = dict(frozen=True)

    max_archive_members: int = Field(default=10_000, ge=1)
    max_archive_member_bytes: int = Field(default=1 << 30, ge=1)
    max_archive_expanded_bytes: int = Field(default=4 << 30, ge=1)
    scan_chunk_bytes: int = Field(default=1 << 20, ge=4096)
    max_text_line_bytes: int = Field(default=8 << 20, ge=4096)

    @model_validator(mode="after")
    def _consistent_archive_limits(self) -> RedactionPolicyConfig:
        if self.max_archive_expanded_bytes < self.max_archive_member_bytes:
            raise ValueError("max_archive_expanded_bytes must be >= max_archive_member_bytes")
        return self


class RedactionReceipt(BaseModel):
    """Safe evidence that one scope was scanned before durability.

    Receipts contain rule identifiers and counts, never matched text.
    """

    model_config = dict(frozen=True)

    policy_id: str
    scope: str
    status: RedactionStatus
    scanned_bytes: int = Field(ge=0)
    redaction_count: int = Field(ge=0)
    rule_ids: tuple[str, ...] = ()

    @field_validator("policy_id", "scope")
    @classmethod
    def _nonempty(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("must not be empty")
        return value

    @field_validator("rule_ids")
    @classmethod
    def _canonical_rules(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not rule.strip() for rule in value):
            raise ValueError("redaction rule identifiers must not be empty")
        return tuple(sorted(set(value)))

    @model_validator(mode="after")
    def _status_matches_findings(self) -> RedactionReceipt:
        if self.status == "clean" and (self.redaction_count or self.rule_ids):
            raise ValueError("a clean redaction receipt cannot contain findings")
        if self.status == "redacted" and (self.redaction_count < 1 or not self.rule_ids):
            raise ValueError("a redacted receipt requires findings")
        return self


class RedactedText(BaseModel):
    """Sanitized text and its safe receipt."""

    model_config = dict(frozen=True)

    text: str
    receipt: RedactionReceipt


class RedactedRecord(BaseModel):
    """Sanitized structured event/span attributes and their receipt."""

    model_config = dict(frozen=True)

    value: dict[str, JsonValue]
    receipt: RedactionReceipt


class RedactedFile(BaseModel):
    """Controlled file snapshot containing exactly the bytes approved for upload."""

    model_config = dict(frozen=True)

    path: Path
    source_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    receipt: RedactionReceipt


class SecretQuarantineError(PayloadRejectedError):
    """A safe failure raised when a payload cannot be rewritten safely.

    The matched credential is deliberately not retained on the exception.
    """

    def __init__(self, scope: str, rule_ids: tuple[str, ...]) -> None:
        # ``scope`` can originate in a path or provider diagnostic. It is an
        # input to the decision, never part of the exception: retry catalogs,
        # logs, and transport adapters may retain the exception string.
        del scope
        self.rule_ids = tuple(sorted(set(rule_ids)))
        rules = ", ".join(self.rule_ids) or "unspecified-secret-rule"
        super().__init__(f"secret-bearing payload quarantined; rules: {rules}")
