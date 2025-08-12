from __future__ import annotations

from typing import Optional, Iterable

from archetype.core.interfaces import iAsyncQueryManager, iAsyncUpdateManager
from archetype.core.interfaces import ArchetypeSignature
from archetype.core import Archetype
from archetype.core.config import StorageConfig
from archetype.app.auth.models import ActorCtx

from archetype.integrations.unity.uc_client import UnityCatalogREST
from archetype.integrations.unity.uc_permissions import UCPermissions, OP_TO_PRIVILEGES
from archetype.integrations.unity.uc_credentials import io_config_from_uc_credentials


class ActorCtxProvider:
    """Callable provider to access per-request ActorCtx without coupling to FastAPI.

    Implementations can capture a ContextVar, thread-local, or inject a fixed context.
    """

    def __call__(self) -> Optional[ActorCtx]:  # pragma: no cover - simple interface
        return None


class UcEnforcedQueryManager(iAsyncQueryManager):
    def __init__(self, inner: iAsyncQueryManager, storage_config: StorageConfig, actor_ctx_provider: ActorCtxProvider):
        self._inner = inner
        self._cfg = storage_config
        self._ctx = actor_ctx_provider

    async def get_archetype(self, sig: ArchetypeSignature, world_id: str, run_id: str):
        await self._ensure_select(sig)
        return await self._inner.get_archetype(sig, world_id, run_id)

    async def query_archetype(self, sig: ArchetypeSignature, world_id: str, ticks=None, entity_ids=None, components=None, run_id: str = None):
        await self._ensure_select(sig)
        return await self._inner.query_archetype(sig, world_id, ticks, entity_ids, components, run_id)

    async def _ensure_select(self, sig: ArchetypeSignature) -> None:
        if not (self._cfg.use_unity_catalog and self._cfg.uc_catalog and self._cfg.uc_schema):
            return
        actor = self._ctx()
        principal = actor.principal if actor else None
        token = (actor.uc_token if actor and actor.uc_token else (self._cfg.uc_token or ""))
        uc = UnityCatalogREST(endpoint=self._cfg.uc_endpoint, token=token)
        perms = UCPermissions(uc)
        full_name = f"{self._cfg.uc_catalog}.{self._cfg.uc_schema}.{Archetype.get_name(sig)}"
        perms.ensure_allowed("table", full_name, OP_TO_PRIVILEGES["read_table"], principal=principal)
        # Optional: temp creds (table, then fallback to path)
        self._apply_temp_creds(uc, full_name, operation="READ", attach_target=getattr(self._inner, "_store", None))


class UcEnforcedUpdateManager(iAsyncUpdateManager):
    def __init__(self, inner: iAsyncUpdateManager, storage_config: StorageConfig, actor_ctx_provider: ActorCtxProvider):
        self._inner = inner
        self._cfg = storage_config
        self._ctx = actor_ctx_provider

    async def update(self, df, sig: ArchetypeSignature, tick: int, world_id: str, run_id: str):
        await self._ensure_modify(sig)
        return await self._inner.update(df, sig, tick, world_id, run_id)

    async def _ensure_modify(self, sig: ArchetypeSignature) -> None:
        if not (self._cfg.use_unity_catalog and self._cfg.uc_catalog and self._cfg.uc_schema):
            return
        actor = self._ctx()
        principal = actor.principal if actor else None
        token = (actor.uc_token if actor and actor.uc_token else (self._cfg.uc_token or ""))
        uc = UnityCatalogREST(endpoint=self._cfg.uc_endpoint, token=token)
        perms = UCPermissions(uc)
        full_name = f"{self._cfg.uc_catalog}.{self._cfg.uc_schema}.{Archetype.get_name(sig)}"
        perms.ensure_allowed("table", full_name, OP_TO_PRIVILEGES["append_table"], principal=principal)
        # Optional: temp creds (table, then fallback to path)
        self._apply_temp_creds(uc, full_name, operation="READ_WRITE", attach_target=getattr(self._inner, "store", None))

    # -----------------------------
    # Shared helpers
    # -----------------------------
    def _apply_temp_creds(self, uc: UnityCatalogREST, full_name: str, *, operation: str, attach_target) -> None:
        if attach_target is None or not hasattr(attach_target, "io_config"):
            return
        try:
            info = uc.get_table(full_name=full_name)
            table_id = info.get("table_id")
            if table_id:
                creds = uc.generate_temporary_table_credentials(table_id=table_id, operation=operation)
                io_cfg = io_config_from_uc_credentials(creds, default_region=self._cfg.uc_default_region)
                setattr(attach_target, "io_config", io_cfg)
                return
            # Fallback to path creds using storage_location if table_id is missing
            storage_location = info.get("storage_location")
            if storage_location:
                path_op = "PATH_READ_WRITE" if operation == "READ_WRITE" else "PATH_READ"
                creds = uc.generate_temporary_path_credentials(url=storage_location, operation=path_op)
                io_cfg = io_config_from_uc_credentials(creds, default_region=self._cfg.uc_default_region)
                setattr(attach_target, "io_config", io_cfg)
        except Exception:
            # Best-effort only
            return


