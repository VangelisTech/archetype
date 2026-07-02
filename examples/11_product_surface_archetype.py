# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Example 11 — Product surface archetype from structured output
============================================================

Turns a structured product-surface prompt output into a mechanical checklist.
The fixture uses the cloud-storage example as the source surface.

Run: uv run python examples/11_product_surface_archetype.py
"""

from __future__ import annotations

from archetype.contrib.product_surface import (
    PRODUCT_SURFACE_JSON_SCHEMA,
    PRODUCT_SURFACE_OUTPUT_GRAMMAR,
    PRODUCT_SURFACE_PROMPT,
    product_surface_plan_from_structured_output,
    render_product_surface_checklist,
)

STRUCTURED_OUTPUT = {
    "surface_name": "Cloud storage provider setup",
    "user_promise": "Users can configure local or cloud-backed Archetype storage through one runtime API.",
    "real_api_path": {
        "claim": "StorageConfig flows into ArchetypeRuntime.world(..., storage=...).",
        "source_refs": [
            "src/archetype/core/config.py:38",
            "src/archetype/runtime/runtime.py:99",
            "src/archetype/app/storage_service.py:40",
        ],
    },
    "runnable_example": {
        "path": "examples/09_cloud_storage.py",
        "command": "uv run python examples/09_cloud_storage.py",
        "smoke_command": "uv run python examples/09_cloud_storage.py --smoke-local",
        "source_refs": ["examples/09_cloud_storage.py:50", "examples/09_cloud_storage.py:230"],
    },
    "banners": [
        {
            "title": "AWS S3",
            "use_case": "Iceberg warehouse on S3.",
            "code_refs": ["examples/09_cloud_storage.py:55"],
            "required_env": ["ARCHETYPE_S3_URI", "AWS_REGION", "AWS_PROFILE"],
        },
        {
            "title": "Google Cloud Storage",
            "use_case": "Iceberg warehouse on GCS.",
            "code_refs": ["examples/09_cloud_storage.py:72"],
            "required_env": [
                "ARCHETYPE_GCS_URI",
                "GOOGLE_CLOUD_PROJECT",
                "GOOGLE_OAUTH_ACCESS_TOKEN",
            ],
        },
        {
            "title": "Azure Blob / ADLS",
            "use_case": "Iceberg warehouse on Azure object storage.",
            "code_refs": ["examples/09_cloud_storage.py:89"],
            "required_env": ["ARCHETYPE_AZURE_URI", "AZURE_STORAGE_ACCOUNT"],
        },
    ],
    "docs": [
        {
            "path": "docs/guide/stores.md",
            "section": "Cloud Provider Banners",
            "source_refs": ["docs/guide/stores.md:53"],
        }
    ],
    "smoke_tests": [
        {
            "path": "tests/experiments/test_cloud_storage_example.py",
            "assertion": "Provider configs build and local runtime smoke spawns one entity.",
            "command": "uv run pytest tests/experiments/test_cloud_storage_example.py -q",
        }
    ],
    "done_when": [
        "The example uses the real runtime storage API.",
        "Every banner maps to copyable StorageConfig code.",
        "The smoke path runs without cloud credentials.",
    ],
}


def main() -> None:
    plan = product_surface_plan_from_structured_output(STRUCTURED_OUTPUT)
    checklist = render_product_surface_checklist(plan)

    print("Product-surface prompt contract")
    print(f"  prompt_chars={len(PRODUCT_SURFACE_PROMPT)}")
    print(f"  grammar_root={'product_surface_output' in PRODUCT_SURFACE_OUTPUT_GRAMMAR}")
    print(f"  schema_fields={len(PRODUCT_SURFACE_JSON_SCHEMA['required'])}")
    print()
    print(checklist)


if __name__ == "__main__":
    main()
