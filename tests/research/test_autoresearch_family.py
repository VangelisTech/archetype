# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Research-family ownership and value-identity contracts."""

from __future__ import annotations

import ast
import hashlib
import json
import subprocess
import sys
from dataclasses import fields
from pathlib import Path
from typing import get_type_hints

from pydantic import TypeAdapter

from archetype.missions import Candidate
from archetype.research import contracts, models
from archetype.research.handlers import _config_identity
from archetype.research.models import (
    AutoResearch,
    AutoResearchConfig,
    CandidateContext,
    CandidatePreparer,
    Evaluator,
    ResearchCandidateContext,
)
from archetype.world.models import EpisodeConfig, RolloutResult

_RESEARCH_ROOT = Path("src/archetype/research")
_LEDGER_SOURCE_MANIFEST = Path("tests/research/fixtures") / "autoresearch_ledger_core_v0.json"
_FORBIDDEN_PRODUCTION_PREFIXES = (
    "archetype.app",
    "archetype.api",
    "archetype.cli",
    "archetype.commands",
    "archetype.runtime",
    "archetype.runtime_resources",
    "archetype.wiring",
)


def _imports(path: Path) -> set[str]:
    modules: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            modules.add(node.module)
    return modules


def test_models_are_canonical_and_compatibility_exports_are_object_identical() -> None:
    assert CandidateContext is ResearchCandidateContext
    assert CandidateContext is not Candidate

    names = (
        "AutoResearchConfig",
        "AutoResearchResult",
        "CandidateContext",
        "CandidatePreparer",
        "Evaluation",
        "EvaluationResult",
        "Evaluator",
        "IterationResult",
    )
    for name in names:
        canonical = getattr(models, name)
        assert getattr(contracts, name) is canonical


def test_callback_contracts_are_structural_protocols() -> None:
    assert getattr(Evaluator, "_is_protocol", False)
    assert getattr(CandidatePreparer, "_is_protocol", False)


def test_public_research_signature_annotations_resolve() -> None:
    assert get_type_hints(AutoResearchConfig)["episode_config"] is EpisodeConfig
    assert get_type_hints(models.IterationResult)["rollout"] is RolloutResult
    assert get_type_hints(Evaluator.__call__)["rollout"] is RolloutResult
    assert "episode_config" in TypeAdapter(AutoResearchConfig).json_schema()["properties"]
    assert "rollout" in TypeAdapter(models.IterationResult).json_schema()["properties"]


def test_models_import_without_core_world_or_dataframe_stacks() -> None:
    probe = (
        "import sys\n"
        "from archetype.research.models import AutoResearchConfig, ResearchCandidateContext\n"
        "heavy = [module for module in (\n"
        "    'archetype.core', 'archetype.core.component', 'archetype.world',\n"
        "    'archetype.world.models', 'daft', 'pyarrow', 'lancedb'\n"
        ") if module in sys.modules]\n"
        "assert not heavy, f'research models import loaded {heavy}'\n"
        "assert AutoResearchConfig.__module__ == 'archetype.research.models'\n"
        "assert ResearchCandidateContext.__module__ == 'archetype.research.models'\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", probe],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr

    annotations = {field.name: field.type for field in fields(AutoResearchConfig)}
    assert annotations["episode_config"] == "EpisodeConfig"


def test_autoresearch_is_the_sole_research_operation_model() -> None:
    assert AutoResearch.model_fields["operation"].default == "autoresearch"
    assert not hasattr(__import__("archetype.research.models", fromlist=["*"]), "RunAutoResearch")
    assert not hasattr(__import__("archetype.research.models", fromlist=["*"]), "SweepAutoResearch")


def test_research_family_has_no_outward_or_lifetime_authority_imports() -> None:
    imported: set[str] = set()
    for path in _RESEARCH_ROOT.glob("*.py"):
        imported.update(_imports(path))

    assert not {module for module in imported if module.startswith(_FORBIDDEN_PRODUCTION_PREFIXES)}

    source = "\n".join(path.read_text() for path in _RESEARCH_ROOT.glob("*.py"))
    assert "ContextVar" not in source
    assert "create_task(" not in source


def test_config_identity_preserves_the_frozen_semantic_digest() -> None:
    characterization = json.loads(_LEDGER_SOURCE_MANIFEST.read_text())[
        "config_identity_characterization"
    ]
    config = AutoResearchConfig(
        experiment_name=characterization["input"]["experiment_name"],
        experiment_id=characterization["input"]["experiment_id"],
        evaluator_id=characterization["input"]["evaluator_id"],
        rollout_contract_id=characterization["input"]["rollout_contract_id"],
        episode_config=EpisodeConfig(max_steps=characterization["input"]["episode_max_steps"]),
        num_episodes=characterization["input"]["num_episodes"],
        max_iterations=characterization["input"]["max_iterations"],
    )

    digest, payload = _config_identity(config)
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode()

    assert payload == characterization["canonical_payload"]
    assert len(encoded) == characterization["byte_length"]
    assert digest == hashlib.sha256(encoded).hexdigest()
    assert digest == characterization["sha256"]
    assert "experiment_id" not in payload
    assert "max_iterations" not in payload


def test_historical_ledger_source_digest_has_reproducible_provenance() -> None:
    manifest = json.loads(_LEDGER_SOURCE_MANIFEST.read_text())

    assert manifest["base_revision"] == "984c91775f60f328ba572ca94c142b2e5521e7fa"
    assert manifest["capture"] == {
        "method_source": "inspect.getsource",
        "join": "\n",
        "preserve_final_method_newline": True,
    }
    assert manifest["methods"] == [
        "_attach_ledger",
        "_read_experiment",
        "_read_head",
        "_next_iteration",
        "_record_running",
        "_record_terminal",
    ]
    assert manifest["byte_length"] == 12_830
    assert manifest["sha256"] == (
        "feb615ecf76ed8b7cdeaca8694a54928b18ad7f49fc6271c8df3db4ff72e9288"
    )
    assert all(Path(path).is_file() for path in manifest["semantic_oracles"])
