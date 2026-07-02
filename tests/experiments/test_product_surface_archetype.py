# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for the product-surface structured prompt archetype."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

from archetype.contrib.product_surface import (
    PRODUCT_SURFACE_JSON_SCHEMA,
    PRODUCT_SURFACE_OUTPUT_GRAMMAR,
    PRODUCT_SURFACE_PROMPT,
    product_surface_plan_from_structured_output,
    render_product_surface_checklist,
)

_EXAMPLE = Path(__file__).resolve().parents[2] / "examples" / "11_product_surface_archetype.py"


def _load_example():
    spec = importlib.util.spec_from_file_location("product_surface_example", _EXAMPLE)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_product_surface_prompt_contract_is_structured() -> None:
    assert "product_surface_output" in PRODUCT_SURFACE_OUTPUT_GRAMMAR
    assert "Every claim must include source_refs" in PRODUCT_SURFACE_PROMPT
    assert PRODUCT_SURFACE_JSON_SCHEMA["required"] == [
        "surface_name",
        "user_promise",
        "real_api_path",
        "runnable_example",
        "banners",
        "docs",
        "smoke_tests",
        "done_when",
    ]


def test_product_surface_fixture_renders_mechanical_checklist() -> None:
    module = _load_example()

    plan = product_surface_plan_from_structured_output(module.STRUCTURED_OUTPUT)
    checklist = render_product_surface_checklist(plan)

    assert plan.surface_name == "Cloud storage provider setup"
    assert plan.real_api_path.source_refs == (
        "src/archetype/core/config.py:38",
        "src/archetype/runtime/runtime.py:99",
        "src/archetype/app/storage_service.py:40",
    )
    assert [banner.title for banner in plan.banners] == [
        "AWS S3",
        "Google Cloud Storage",
        "Azure Blob / ADLS",
    ]
    assert "uv run python examples/09_cloud_storage.py --smoke-local" in checklist
    assert "Every banner maps to copyable StorageConfig code." in checklist


def test_product_surface_parser_rejects_unattributed_api_path() -> None:
    module = _load_example()
    output = dict(module.STRUCTURED_OUTPUT)
    output["real_api_path"] = {"claim": "Use the API.", "source_refs": []}

    with pytest.raises(ValueError, match="source_refs"):
        product_surface_plan_from_structured_output(output)


def test_product_surface_example_smoke(capsys) -> None:
    module = _load_example()

    module.main()
    out = capsys.readouterr().out

    assert "grammar_root=True" in out
    assert "# Cloud storage provider setup" in out
    assert "Banners:" in out
    assert "AWS S3" in out
