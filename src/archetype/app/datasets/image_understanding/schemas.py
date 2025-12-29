"""
Schemas for Image Understanding Evaluation + Curation.

These are used with `daft.functions.prompt(..., return_format=...)`.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class MultipleChoiceAnswer(BaseModel):
    """Structured output for multiple-choice answers."""

    choice: str = Field(
        ...,
        description="The letter of the correct choice (e.g., A, B, C, D). Return only a single letter.",
    )


class TextBiasLabel(BaseModel):
    """Text-only bias label for a multiple-choice question."""

    solvable_without_image: bool = Field(
        description=(
            "True if a competent test-taker could likely pick the correct answer without seeing the image, "
            "using only the question text and general world knowledge."
        )
    )
    confidence: float = Field(
        ge=0.0,
        le=1.0,
        description="Confidence in solvable_without_image decision (0-1).",
    )
    bias_pattern: str = Field(
        description="One of: option_leakage, world_knowledge, ambiguity, requires_image, other",
    )
    rationale: str = Field(
        description="High-signal explanation of why it is (or isn't) solvable from text alone."
    )


class ImageNecessityLabel(BaseModel):
    """Image-necessity + ambiguity label using both image and text."""

    requires_image: bool = Field(
        description="True if the image is necessary to reliably pick the correct answer."
    )
    unambiguous_with_image: bool = Field(
        description="True if, given the image, the question has a clear correct answer (not ambiguous/mislabeled)."
    )
    confidence: float = Field(
        ge=0.0,
        le=1.0,
        description="Overall confidence in requires_image and unambiguous_with_image (0-1).",
    )
    rationale: str = Field(description="Short, high-signal reasoning.")
