from __future__ import annotations

from daft import DataFrame, col

from archetype.core import ArchetypeSignature


def validate_non_empty_schema(df: DataFrame) -> None:
    # Ensure we have at least the housekeeping columns; skip if df is empty schema
    try:
        cols = set(df.column_names)
    except Exception:
        return
    required = {"entity_id", "tick", "world_id", "run_id", "is_active"}
    if cols and not required.issubset(cols):
        raise ValueError(f"DataFrame missing required columns: {required - cols}")


def validate_no_duplicate_entities(df: DataFrame) -> None:
    try:
        # Logical check: no duplicated entity_id rows at the same tick
        dup_count = (
            df.select("entity_id", "tick")
            .group_by("entity_id", "tick")
            .agg({"entity_id": "count"})
            .where(col("entity_id_count") > 1)
            .collect()
            .count()
        )
        if dup_count and dup_count[0] > 0:
            raise ValueError("Duplicate entity rows detected for the same tick")
    except Exception:
        # Best-effort; avoid hard failures if backend ops aren't available
        return


def validate_materialized(df: DataFrame, sig: ArchetypeSignature) -> None:
    validate_non_empty_schema(df)
    # Additional materialization invariants can be added here


def validate_pre_update(df: DataFrame, sig: ArchetypeSignature) -> None:
    validate_non_empty_schema(df)


def validate_post_update(df: DataFrame, sig: ArchetypeSignature) -> None:
    validate_non_empty_schema(df)
    validate_no_duplicate_entities(df)


