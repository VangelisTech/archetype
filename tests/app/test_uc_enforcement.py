import types
import pytest

from archetype.app.security.uc_wrappers import UcEnforcedQueryManager, UcEnforcedUpdateManager
from archetype.core.component import Component
from archetype.core import Archetype


class DemoA(Component):
    a: int


class DemoB(Component):
    b: int


class FakeUC:
    def __init__(self, *args, **kwargs):
        pass

    def get_table(self, full_name: str):
        return {"table_id": "t1", "storage_location": "s3://bucket/path"}

    def generate_temporary_table_credentials(self, table_id: str, operation: str):
        return {"creds": True}

    def generate_temporary_path_credentials(self, url: str, operation: str):
        return {"creds": True}


class FakeStore:
    def __init__(self):
        self.io_config = None


@pytest.mark.asyncio
async def test_temp_creds_attach_to_cached_store_inner(monkeypatch):
    # Patch UC client and perms to allow
    monkeypatch.setattr("archetype.app.security.uc_wrappers.UnityCatalogREST", FakeUC)
    monkeypatch.setattr("archetype.app.security.uc_wrappers.UCPermissions", RecordingPerms)

    # Build a wrapper with a fake inner that has a cached store-like attribute
    class Inner:
        def __init__(self):
            class Cached:
                def __init__(self):
                    self._inner = FakeStore()
            self._store = Cached()

        async def query_archetype(self, *args, **kwargs):
            from daft import from_arrow
            import pyarrow as pa
            schema = Archetype.get_archetype_schema(tuple(sorted((DemoA,), key=lambda t: t.__name__)))
            return from_arrow(pa.Table.from_batches([], schema=schema))

    wrapper = UcEnforcedQueryManager(Inner(), _cfg_uc(), actor_ctx_provider=lambda: None)
    sig = tuple(sorted((DemoA,), key=lambda t: t.__name__))
    await wrapper.query_archetype(sig, world_id="w", run_id="r")

    # Ensure io_config was attached to inner store
    assert wrapper._inner._store._inner.io_config is not None


class RecordingPerms:
    calls: list[tuple[str, str, tuple[str, ...], str | None]] = []

    def __init__(self, uc):
        self.uc = uc

    def ensure_allowed(self, securable_type, full_name, required, principal=None):
        RecordingPerms.calls.append((securable_type, full_name, tuple(required), principal))


class DenyingPerms(RecordingPerms):
    def ensure_allowed(self, securable_type, full_name, required, principal=None):
        from archetype.integrations.unity.uc_permissions import PermissionDenied
        raise PermissionDenied(securable_type=securable_type, full_name=full_name, principal=principal, missing=set(required))


def _cfg_uc():
    # Storage-like object with nested uc_config to match latest implementation
    uc_cfg = types.SimpleNamespace(
        enabled=True,
        enforce_permissions=True,
        endpoint="http://uc",
        token="tok",
        catalog="cat",
        schema="sch",
        default_region="us-east-1",
    )
    return types.SimpleNamespace(uc_config=uc_cfg)


@pytest.mark.asyncio
async def test_uc_query_wrapper_calls_permission_checks(monkeypatch):
    # Patch UC client and perms
    monkeypatch.setattr("archetype.app.security.uc_wrappers.UnityCatalogREST", FakeUC)
    monkeypatch.setattr("archetype.app.security.uc_wrappers.UCPermissions", RecordingPerms)

    # Minimal inner querier
    class Inner:
        async def get_archetype(self, *args, **kwargs):
            from daft import from_arrow
            import pyarrow as pa
            schema = Archetype.get_archetype_schema(tuple(sorted((DemoA,), key=lambda t: t.__name__)))
            return from_arrow(pa.Table.from_batches([], schema=schema))

        async def query_archetype(self, *args, **kwargs):
            return await self.get_archetype(*args, **kwargs)

    wrapper = UcEnforcedQueryManager(Inner(), _cfg_uc(), actor_ctx_provider=lambda: None)
    sig = tuple(sorted((DemoA,), key=lambda t: t.__name__))
    await wrapper.query_archetype(sig, world_id="w", run_id="r")

    assert RecordingPerms.calls, "expected permission checks to be recorded"
    securable_type, full_name, required, _ = RecordingPerms.calls[-1]
    assert securable_type == "table"
    assert full_name.startswith("cat.sch.")
    assert "SELECT" in required


@pytest.mark.asyncio
async def test_uc_query_wrapper_denies(monkeypatch):
    monkeypatch.setattr("archetype.app.security.uc_wrappers.UnityCatalogREST", FakeUC)
    monkeypatch.setattr("archetype.app.security.uc_wrappers.UCPermissions", DenyingPerms)

    class Inner:
        async def query_archetype(self, *args, **kwargs):
            raise AssertionError("should not reach inner on denied perms")

    wrapper = UcEnforcedQueryManager(Inner(), _cfg_uc(), actor_ctx_provider=lambda: None)
    sig = tuple(sorted((DemoA,), key=lambda t: t.__name__))
    from archetype.integrations.unity.uc_permissions import PermissionDenied
    with pytest.raises(PermissionDenied):
        await wrapper.query_archetype(sig, world_id="w", run_id="r")


@pytest.mark.asyncio
async def test_uc_update_wrapper_denies(monkeypatch):
    monkeypatch.setattr("archetype.app.security.uc_wrappers.UnityCatalogREST", FakeUC)
    monkeypatch.setattr("archetype.app.security.uc_wrappers.UCPermissions", DenyingPerms)

    class Inner:
        async def update(self, *args, **kwargs):
            raise AssertionError("should not reach inner on denied perms")

    wrapper = UcEnforcedUpdateManager(Inner(), _cfg_uc(), actor_ctx_provider=lambda: None)
    sig = tuple(sorted((DemoA, DemoB), key=lambda t: t.__name__))
    from daft import from_arrow
    import pyarrow as pa
    schema = Archetype.get_archetype_schema(sig)
    df = from_arrow(pa.Table.from_batches([], schema=schema))

    from archetype.integrations.unity.uc_permissions import PermissionDenied
    with pytest.raises(PermissionDenied):
        await wrapper.update(df, sig, tick=0, world_id="w", run_id="r")


