"""
Image Understanding Dataset Builder (Archetype-native)
=====================================================

Goal: create a *less biased* VLM image understanding dataset by:
- Running the ablation study (with-image vs text-only)
- Filtering to cases where the image matters
- Adding text-bias labels and image-necessity labels
- Writing curated rows to your configured Archetype storage backend

This is designed to be run at Daft scale while using Archetype's `StorageConfig`
to configure storage/I/O in one place.

Inspired by the quadrant methodology described in:
`https://www.daft.ai/blog/multimodal-structured-outputs-evaluating-vlm-image-understanding-at-scale`
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import daft  # type: ignore[import-not-found]
from daft import col  # type: ignore[import-not-found]
from daft.functions import format, prompt, when  # type: ignore[import-not-found]

from archetype.core.config import StorageConfig
from archetype.core.runtime.storage import StorageContextFactory

from .schemas import ImageNecessityLabel, MultipleChoiceAnswer, TextBiasLabel

TEXT_BIAS_SYSTEM = """
You are auditing a multiple-choice vision benchmark for TEXT BIAS.

You will be given ONLY the question text (which may include answer choices).
You will NOT see the image.

Determine if the correct option is reasonably inferable without the image.

Bias patterns:
- option_leakage: the correct option is guessable from phrasing/choices alone
- world_knowledge: generic causal/definitional reasoning gives away answer
- ambiguity: multiple options plausible without visual disambiguation

Be strict: if the image is necessary to disambiguate, solvable_without_image=false.
"""


IMAGE_NECESSITY_SYSTEM = """
You are auditing a multiple-choice vision benchmark for IMAGE NECESSITY and AMBIGUITY.

You WILL see the image and the question.

Decide:
1) requires_image: is the image necessary to reliably pick the correct answer?
2) unambiguous_with_image: given the image, is the question clearly answerable (not ambiguous/mislabeled)?

Be conservative. If unsure, set confidence lower.
"""


@dataclass(frozen=True)
class BuildConfig:
    """
    Configuration for building a curated dataset.

    Notes:
    - `answer_model_id` is the model we ablate (with-image vs text-only).
    - `bias_judge_model_id` is text-only; it labels bias.
    - `image_judge_model_id` sees image+text; it filters ambiguity/mislabeled items.
    """

    category: str
    subset: str
    source_uri: str

    answer_model_id: str
    answer_params: dict[str, Any]

    bias_judge_model_id: str
    image_judge_model_id: str

    limit: int | None = None

    # Filtering thresholds
    bias_confidence_min: float = 0.75
    image_judge_confidence_min: float = 0.70

    # Output
    dest_uri: str = "./archetype_data/datasets/image_understanding/curated.parquet"


def _with_storage_io(storage_config: StorageConfig) -> None:
    """
    Configure Daft I/O using Archetype storage config.
    """

    ctx = StorageContextFactory.build(storage_config)
    if ctx.io_config is not None:
        daft.set_planning_config(default_io_config=ctx.io_config)


def _preprocess(df: daft.DataFrame, *, cfg: BuildConfig) -> daft.DataFrame:
    """
    Preprocess Cauldron-form rows.

    Assumptions (matching your existing pipeline):
    - The dataset has a `texts` column with nested {user, assistant, ...}.

    We handle two common upstream formats:
    - **Pre-decoded**: `image` column already exists
    - **Cauldron raw**: `images` is a list, with `images["bytes"]` holding bytes to decode
    """
    # Ensure we have an `image` column.
    if "image" not in df.column_names and "images" in df.column_names:
        df = (
            df.explode("images")
            .with_column("image_bytes", col("images")["bytes"])
            .with_column("image", col("images")["bytes"].decode_image())
        )

    df = df.with_columns(
        {
            "category": daft.lit(cfg.category),
            "subset": daft.lit(cfg.subset),
            "answer_model_id": daft.lit(cfg.answer_model_id),
            "answer_params": daft.lit(cfg.answer_params),
        }
    )

    df = df.explode("texts").with_column(
        "answer",
        col("texts")["assistant"].regexp_replace("Answer: ", "").lstrip().rstrip(),
    )
    return df


def _infer_choice(
    df: daft.DataFrame,
    *,
    model_id: str,
    params: dict[str, Any],
    with_image: bool,
) -> daft.DataFrame:
    if with_image:
        messages = [col("image"), col("texts")["user"]]
        result_col = "pred_with_image"
        correct_col = "is_correct_with_image"
    else:
        messages = col("texts")["user"]
        result_col = "pred_no_image"
        correct_col = "is_correct_no_image"

    df = df.with_column(
        result_col,
        prompt(
            messages=messages,
            model=model_id,
            use_chat_completions=True,
            return_format=MultipleChoiceAnswer,
            **params,
        ),
    )
    df = df.with_column(
        correct_col,
        col(result_col)["choice"].lstrip().rstrip() == col("answer"),
    )
    return df


def _classify_quadrant(df: daft.DataFrame) -> daft.DataFrame:
    return df.with_column(
        "quadrant",
        when(
            col("is_correct_with_image") & col("is_correct_no_image"),
            "Both Correct",
        )
        .when(
            col("is_correct_with_image") & (~col("is_correct_no_image")),
            "Image Helped",
        )
        .when(
            (~col("is_correct_with_image")) & col("is_correct_no_image"),
            "Image Hurt",
        )
        .otherwise("Both Incorrect"),
    )


def _label_text_bias(df: daft.DataFrame, *, model_id: str) -> daft.DataFrame:
    template = format(
        """Decide if this multiple-choice question is solvable without the image.

<question>
{}
</question>
""",
        col("texts")["user"],
    )
    return df.with_column(
        "text_bias",
        prompt(
            messages=template,
            system_message=TEXT_BIAS_SYSTEM,
            model=model_id,
            use_chat_completions=True,
            return_format=TextBiasLabel,
        ),
    )


def _label_image_necessity(df: daft.DataFrame, *, model_id: str) -> daft.DataFrame:
    template = format(
        """Analyze whether the image is necessary and whether the item is unambiguous.

<question>
{}
</question>

<correct_answer_letter>
{}
</correct_answer_letter>
""",
        col("texts")["user"],
        col("answer"),
    )
    return df.with_column(
        "image_label",
        prompt(
            messages=[col("image"), template],
            system_message=IMAGE_NECESSITY_SYSTEM,
            model=model_id,
            use_chat_completions=True,
            return_format=ImageNecessityLabel,
        ),
    )


def build_curated_dataset(
    *,
    storage_config: StorageConfig,
    cfg: BuildConfig,
    provider: str = "daft",
) -> daft.DataFrame:
    """
    Build and write a curated dataset.

    Returns the *lazy* output DataFrame (the write forces execution).
    """
    daft.set_provider(provider)
    _with_storage_io(storage_config)

    df = daft.read_parquet(cfg.source_uri)
    if cfg.limit is not None:
        df = df.limit(int(cfg.limit))

    df = _preprocess(df, cfg=cfg)

    # Ablation: same model, with image vs without image
    df = _infer_choice(df, model_id=cfg.answer_model_id, params=cfg.answer_params, with_image=True)
    df = _infer_choice(df, model_id=cfg.answer_model_id, params=cfg.answer_params, with_image=False)
    df = _classify_quadrant(df)

    # Candidate set: image helped on the answerer model
    df = df.where(col("quadrant") == "Image Helped")

    # Filters: remove text-solvable items; remove ambiguous items
    df = _label_text_bias(df, model_id=cfg.bias_judge_model_id)
    df = df.where(
        (~col("text_bias")["solvable_without_image"])
        & (col("text_bias")["confidence"] >= daft.lit(float(cfg.bias_confidence_min)))
    )

    df = _label_image_necessity(df, model_id=cfg.image_judge_model_id)
    df = df.where(
        col("image_label")["requires_image"]
        & col("image_label")["unambiguous_with_image"]
        & (col("image_label")["confidence"] >= daft.lit(float(cfg.image_judge_confidence_min)))
    )

    # Emit a stable, training-friendly schema
    df_out = df.select(
        "category",
        "subset",
        col("texts")["user"].alias("question"),
        col("answer").alias("answer_choice"),
        "image",
        # Optional raw bytes retained for vLLM server / custom providers.
        # When the source dataset didn't have `images`, this column will not exist.
        *(["image_bytes"] if "image_bytes" in df.column_names else []),
        "quadrant",
        col("pred_with_image")["choice"].alias("pred_choice_with_image"),
        col("pred_no_image")["choice"].alias("pred_choice_no_image"),
        col("text_bias")["solvable_without_image"].alias("solvable_without_image"),
        col("text_bias")["confidence"].alias("text_bias_confidence"),
        col("text_bias")["bias_pattern"].alias("text_bias_pattern"),
        col("image_label")["requires_image"].alias("requires_image"),
        col("image_label")["unambiguous_with_image"].alias("unambiguous_with_image"),
        col("image_label")["confidence"].alias("image_judge_confidence"),
    )

    df_out.write_parquet(cfg.dest_uri, write_mode="overwrite")
    return df_out
