# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Real hosted Physical-AI execution with Modal compute and R2 world evidence."""

from __future__ import annotations

import os
from dataclasses import replace
from pathlib import Path
from urllib.parse import urlparse

import pytest
from pyarrow.fs import FileSelector, S3FileSystem
from uuid_utils import uuid7

from archetype import (
    ArchetypeRuntime,
    HostedEpisodeObservation,
    HostedEpisodeRequest,
    ModalHostedEpisodeConfig,
)
from archetype.core.config import StorageBackend, StorageConfig
from archetype.physical_ai.hosted_modal import (
    ModalHostedEpisodeProvider,
    ModalNamedHostedEpisodeRuntime,
    build_seeded_modal_hosted_episode_app,
)
from archetype.runtime import runtime as runtime_module

modal = pytest.importorskip(
    "modal", reason="the live provider test requires the coding-agent extra"
)

ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID")
SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY")
API_ENDPOINT = os.environ.get("R2_API_ENDPOINT")
BUCKET = os.environ.get("R2_BUCKET")
MODAL_WORKSPACE = os.environ.get("CODING_AGENT_MODAL_WORKSPACE")
MODAL_ENVIRONMENT = os.environ.get("CODING_AGENT_MODAL_ENVIRONMENT")
_LIVE = os.environ.get("ARCHETYPE_MODAL_PHYSICAL_R2_LIVE") == "1"
_REQUIRED = (
    ACCESS_KEY_ID,
    SECRET_ACCESS_KEY,
    API_ENDPOINT,
    BUCKET,
    MODAL_WORKSPACE,
    MODAL_ENVIRONMENT,
)

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.integration,
    pytest.mark.external,
    pytest.mark.slow,
    pytest.mark.skipif(
        not _LIVE or not all(_REQUIRED),
        reason="set the paid Modal/R2 release evidence configuration",
    ),
]


def _configure_lancedb_r2(monkeypatch: pytest.MonkeyPatch) -> None:
    assert ACCESS_KEY_ID is not None
    assert SECRET_ACCESS_KEY is not None
    assert API_ENDPOINT is not None
    for key, value in {
        "AWS_ACCESS_KEY_ID": ACCESS_KEY_ID,
        "AWS_SECRET_ACCESS_KEY": SECRET_ACCESS_KEY,
        "AWS_ENDPOINT": API_ENDPOINT,
        "AWS_ENDPOINT_URL": API_ENDPOINT,
        "AWS_DEFAULT_REGION": "auto",
        "AWS_REGION": "auto",
    }.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("AWS_SESSION_TOKEN", raising=False)


def _r2_filesystem() -> S3FileSystem:
    assert ACCESS_KEY_ID is not None
    assert SECRET_ACCESS_KEY is not None
    assert API_ENDPOINT is not None
    endpoint = urlparse(API_ENDPOINT)
    return S3FileSystem(
        access_key=ACCESS_KEY_ID,
        secret_key=SECRET_ACCESS_KEY,
        region="auto",
        scheme=endpoint.scheme,
        endpoint_override=endpoint.netloc,
        force_virtual_addressing=False,
    )


def _request() -> HostedEpisodeRequest:
    return HostedEpisodeRequest(
        trial_id=0,
        suite="seeded-reach",
        task_id=7,
        seed=100,
        instruction="reach the target",
        max_transitions=2,
        environment_id="seeded-reach@v1",
        policy_id="scripted-reach@v1",
        config_json='{"reward_per_transition":0.25,"success_after_transitions":1}',
    )


async def test_modal_episode_round_trips_durable_evidence_through_r2(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Run one GPU episode, cold-read R2 state, and reconcile without replay."""

    assert BUCKET is not None
    assert MODAL_WORKSPACE is not None
    assert MODAL_ENVIRONMENT is not None
    identity = uuid7().hex
    suffix = identity[:12]
    prefix = f"archetype-ci/physical-modal-r2/{identity}"
    storage = StorageConfig(
        uri=f"s3://{BUCKET}/{prefix}/worlds",
        namespace=f"physical_modal_r2_{suffix}",
        backend=StorageBackend.LANCEDB,
    )
    provider_config = ModalHostedEpisodeConfig(
        workspace_name=MODAL_WORKSPACE,
        environment_name=MODAL_ENVIRONMENT,
        app_name=f"archetype-physical-v050-{suffix}",
        function_name="seeded-hosted-episode",
        result_dict_name=f"archetype-physical-results-{suffix}",
        result_volume_name=f"archetype-physical-values-{suffix}",
        create_if_missing=True,
        call_timeout_seconds=600,
    )
    image = (
        modal.Image.debian_slim(python_version="3.12")
        .pip_install_from_pyproject("pyproject.toml")
        .add_local_python_source("archetype", copy=True)
    )
    app, function = build_seeded_modal_hosted_episode_app(
        provider_config,
        gpu="T4",
        image=image,
    )
    provider_runtimes: list[ModalNamedHostedEpisodeRuntime] = []

    def provider_factory(config: ModalHostedEpisodeConfig) -> ModalHostedEpisodeProvider:
        provider_runtime = ModalNamedHostedEpisodeRuntime(config, function=function)
        provider_runtimes.append(provider_runtime)
        return ModalHostedEpisodeProvider(config, runtime=provider_runtime)

    original_bootstrap = runtime_module._bootstrap_config
    monkeypatch.setattr(
        runtime_module,
        "_bootstrap_config",
        lambda **kwargs: replace(
            original_bootstrap(**kwargs),
            hosted_episode_provider_factory=provider_factory,
        ),
    )
    monkeypatch.setenv("ARCHETYPE_CATALOG_DIR", str(tmp_path / "control"))
    _configure_lancedb_r2(monkeypatch)
    filesystem = _r2_filesystem()
    world_id = ""
    first: HostedEpisodeObservation | None = None

    try:
        async with app.run(
            name=provider_config.app_name,
            environment_name=provider_config.environment_name,
        ):
            async with ArchetypeRuntime() as runtime:
                world = runtime.world("physical-modal-r2-live", storage=storage)
                first = await world.run_hosted_episode(
                    [_request()],
                    provider=provider_config,
                    activity_id="v050-live-episode",
                )
                world_id = str(world.world_id)

            assert first is not None
            assert first.episode_count == first.success_count == 1
            assert len(first.result_digest) == 64
            completion = provider_runtimes[0].last_completion
            assert isinstance(completion, dict)
            assert completion["gpu_count"] >= 1

            # A fresh process owner must rediscover the world through its
            # durable catalog and reconstruct the committed observation from R2.
            async with ArchetypeRuntime() as cold_runtime:
                resumed = await cold_runtime.resume(world_id, storage=storage)
                rows = (await resumed.query(HostedEpisodeObservation)).to_pylist()
                recovered = await resumed.run_hosted_episode(
                    [_request()],
                    provider=provider_config,
                    activity_id="v050-live-episode",
                )
            assert any(
                row["hostedepisodeobservation__result_digest"] == first.result_digest
                for row in rows
            )
            assert recovered.result_digest == first.result_digest
            assert recovered.operation_id == first.operation_id
            assert (
                len([item for item in provider_runtimes if item.last_completion is not None]) == 1
            )

        physical = filesystem.get_file_info(FileSelector(f"{BUCKET}/{prefix}", recursive=True))
        assert physical, "the hosted intent and observation never reached R2"
        assert any(item.is_file and item.size > 0 for item in physical)
    finally:
        await modal.Dict.objects.delete.aio(
            provider_config.result_dict_name,
            allow_missing=True,
            environment_name=provider_config.environment_name,
        )
        await modal.Volume.objects.delete.aio(
            provider_config.result_volume_name,
            allow_missing=True,
            environment_name=provider_config.environment_name,
        )
        try:
            filesystem.delete_dir(f"{BUCKET}/{prefix}")
        except FileNotFoundError:
            pass
