import pytest

from archetype.core.runtime.runner import initialize_runner, ExecutionContextFactory


def test_initialize_runner_sets_native_and_io_config(monkeypatch):
    calls = {"native": 0, "planning": 0}

    def fake_set_runner_native():
        calls["native"] += 1

    def fake_set_planning_config(**kwargs):
        calls["planning"] += 1
        return None

    # If already initialized in this process, the initializer is idempotent.
    # Reset internal flag so we can assert calls deterministically.
    monkeypatch.setattr("archetype.core.runtime.runner._INITIALIZED", False, raising=False)
    monkeypatch.setattr("archetype.core.runtime.runner.set_runner_native", fake_set_runner_native)
    monkeypatch.setattr("archetype.core.runtime.runner.set_planning_config", fake_set_planning_config)

    from daft.io import IOConfig
    initialize_runner(IOConfig())
    assert calls["native"] == 1
    assert calls["planning"] == 1


def test_execution_context_factory_per_op_overrides():
    from daft.io import IOConfig, S3Config
    base = IOConfig()
    s3 = S3Config(key_id="k", access_key="a")
    new = ExecutionContextFactory.per_op_io_config(base, s3=s3)
    # IOConfig is immutable; per_op_io_config returns a new IOConfig
    assert new.s3.key_id == s3.key_id and new.s3.access_key == s3.access_key


