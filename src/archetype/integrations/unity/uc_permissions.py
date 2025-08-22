"""
Unity Catalog permission utilities.

Maps Archetype operations to Unity Catalog privileges and provides a small
cache around permission checks. Intended to be called just-in-time before
catalog/schema/table operations.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Optional, Set
import time

from .uc_client import UnityCatalogREST, UCError


DEFAULT_PERM_CACHE_TTL_SEC = 5.0


@dataclass(frozen=True)
class PermissionDenied(Exception):
    securable_type: str
    full_name: str
    principal: Optional[str]
    missing: Set[str]

    def __str__(self) -> str:
        who = self.principal or "<unknown>"
        return (
            f"Permission denied for principal={who} on {self.securable_type}:{self.full_name}; "
            f"missing={sorted(self.missing)}"
        )


class _PermCache:
    def __init__(self, ttl_sec: float = DEFAULT_PERM_CACHE_TTL_SEC) -> None:
        self._ttl = ttl_sec
        self._store: dict[tuple[str, str, Optional[str]], tuple[float, set[str]]] = {}

    def get(self, securable_type: str, full_name: str, principal: Optional[str]) -> Optional[Set[str]]:
        key = (securable_type, full_name, principal)
        item = self._store.get(key)
        if not item:
            return None
        exp, privs = item
        if time.time() > exp:
            self._store.pop(key, None)
            return None
        return set(privs)

    def set(self, securable_type: str, full_name: str, principal: Optional[str], privileges: Iterable[str]) -> None:
        key = (securable_type, full_name, principal)
        self._store[key] = (time.time() + self._ttl, set(privileges))


class UCPermissions:
    """Encapsulates UC authorization checks with small caching.

    Usage:
        perms = UCPermissions(uc)
        perms.ensure_allowed("table", "cat.schema.table", {"SELECT"}, principal="user@example.com")
    """

    def __init__(self, uc_rest: UnityCatalogREST, cache_ttl_sec: float = DEFAULT_PERM_CACHE_TTL_SEC) -> None:
        self._uc = uc_rest
        self._cache = _PermCache(ttl_sec=cache_ttl_sec)

    def _fetch_privileges(self, securable_type: str, full_name: str, principal: Optional[str]) -> Set[str]:
        cached = self._cache.get(securable_type, full_name, principal)
        if cached is not None:
            return cached

        resp = self._uc.get_permissions(securable_type=securable_type, full_name=full_name, principal=principal)
        privs: set[str] = set()
        for item in resp.get("privilege_assignments", []) or []:
            if principal is not None and item.get("principal") != principal:
                continue
            for p in item.get("privileges", []) or []:
                privs.add(p)

        self._cache.set(securable_type, full_name, principal, privs)
        return privs

    def is_allowed(self, securable_type: str, full_name: str, required: Iterable[str], *, principal: Optional[str] = None) -> bool:
        have = self._fetch_privileges(securable_type, full_name, principal)
        required_set = set(required)
        return required_set.issubset(have)

    def ensure_allowed(self, securable_type: str, full_name: str, required: Iterable[str], *, principal: Optional[str] = None) -> None:
        have = self._fetch_privileges(securable_type, full_name, principal)
        required_set = set(required)
        missing = required_set.difference(have)
        if missing:
            raise PermissionDenied(securable_type=securable_type, full_name=full_name, principal=principal, missing=missing)


# Opinionated mappings from Archetype operations to UC privileges

OP_TO_PRIVILEGES: dict[str, list[str]] = {
    # Reads
    "read_table": ["SELECT"],
    "query_table": ["SELECT"],
    # Writes
    "append_table": ["MODIFY"],
    "create_table": ["CREATE TABLE"],
    # App-level command ops → table privileges
    "create_entity": ["MODIFY"],
    "add_component": ["MODIFY"],
    "remove_component": ["MODIFY"],
    "remove_entity": ["MODIFY"],
    # Discovery
    "list_schemas": ["USE CATALOG"],
    "list_tables": ["USE SCHEMA"],
}


__all__ = [
    "UCPermissions",
    "PermissionDenied",
    "OP_TO_PRIVILEGES",
]


