"""
Unity Catalog REST client (focused subset).

This client wraps the Unity Catalog HTTP API for the subset of endpoints we
need initially: catalogs/schemas/tables discovery, permissions get/update, and
temporary credential vending for tables/volumes/paths/models.

Design goals:
- Stateless: caller passes endpoint and token; no global state
- Safe defaults: short timeouts, retries are left to caller for now
- Small TTL caches for hot paths (permissions, describe calls)

Note: This client is intentionally minimal and easy to stub in tests.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, MutableMapping, Optional, Tuple
import time
import json
import urllib.parse as urlparse

import requests

# Optional: use generated async client if present
try:
    from unitycatalog.client.api_client import ApiClient as UCApiClient  # type: ignore
    from unitycatalog.client.api import (
        tables_api as uc_tables_api,
        temporary_credentials_api as uc_temp_api,
        grants_api as uc_grants_api,
        schemas_api as uc_schemas_api,
        catalogs_api as uc_catalogs_api,
        metastores_api as uc_metastores_api,
    )  # type: ignore
    from unitycatalog.client.configuration import Configuration as UCConfiguration  # type: ignore
    _HAS_UC_GEN = True
except Exception:
    _HAS_UC_GEN = False


DEFAULT_TIMEOUT_SECONDS: float = 10.0
DEFAULT_CACHE_TTL_SECONDS: float = 5.0


@dataclass(frozen=True)
class UCError(Exception):
    status_code: int
    message: str

    def __str__(self) -> str:
        return f"UnityCatalog HTTP {self.status_code}: {self.message}"


class _TTLCache:
    """Very small TTL cache for JSON-able values.

    Keyed by a tuple; values expire after ttl_sec.
    """

    def __init__(self, ttl_sec: float = DEFAULT_CACHE_TTL_SECONDS):
        self._store: MutableMapping[Tuple[Any, ...], Tuple[float, Any]] = {}
        self._ttl = ttl_sec

    def get(self, key: Tuple[Any, ...]) -> Optional[Any]:
        item = self._store.get(key)
        if not item:
            return None
        expires_at, value = item
        if time.time() > expires_at:
            self._store.pop(key, None)
            return None
        return value

    def set(self, key: Tuple[Any, ...], value: Any, ttl_override: Optional[float] = None) -> None:
        ttl = ttl_override if ttl_override is not None else self._ttl
        self._store[key] = (time.time() + ttl, value)


class UnityCatalogREST:
    """Thin REST client for Unity Catalog.

    Parameters
    - endpoint: base UC endpoint, e.g. http://localhost:8080/api/2.1/unity-catalog
    - token: bearer token (admin or user); may be rotated by caller
    - timeout_sec: per-request timeout
    - cache_ttl_sec: TTL for small in-memory cache of GETs
    """

    def __init__(
        self,
        *,
        endpoint: str,
        token: str,
        timeout_sec: float = DEFAULT_TIMEOUT_SECONDS,
        cache_ttl_sec: float = DEFAULT_CACHE_TTL_SECONDS,
    ) -> None:
        # Normalize endpoint: strip trailing slashes
        self._endpoint = endpoint.rstrip("/")
        self._token = token
        self._timeout = timeout_sec
        self._cache = _TTLCache(ttl_sec=cache_ttl_sec)
        self._use_gen = _HAS_UC_GEN
        if self._use_gen:
            conf = UCConfiguration(host=self._endpoint.replace("/api/2.1/unity-catalog", ""))
            conf.access_token = self._token
            self._api_client = UCApiClient(conf)
            self._tables = uc_tables_api.TablesApi(self._api_client)
            self._temps = uc_temp_api.TemporaryCredentialsApi(self._api_client)
            self._grants = uc_grants_api.GrantsApi(self._api_client)
            self._schemas = uc_schemas_api.SchemasApi(self._api_client)
            self._catalogs = uc_catalogs_api.CatalogsApi(self._api_client)
            self._metastore = uc_metastores_api.MetastoresApi(self._api_client)

    # -----------------------------
    # Internals
    # -----------------------------
    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _url(self, path: str) -> str:
        if path.startswith("/"):
            return f"{self._endpoint}{path}"
        return f"{self._endpoint}/{path}"

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None,
        use_cache: bool = False,
    ) -> Dict[str, Any]:
        url = self._url(path)
        key: Tuple[Any, ...] = (method, url, json.dumps(params, sort_keys=True) if params else None)
        if method.upper() == "GET" and use_cache:
            cached = self._cache.get(key)
            if cached is not None:
                return cached

        resp = requests.request(
            method=method.upper(),
            url=url,
            headers=self._headers(),
            params=params,
            data=json.dumps(body) if body is not None else None,
            timeout=self._timeout,
        )
        if resp.status_code // 100 != 2:
            # Try to extract structured error
            try:
                payload = resp.json()
                message = payload.get("message") or payload.get("error") or resp.text
            except Exception:
                message = resp.text
            raise UCError(status_code=resp.status_code, message=message)

        data = resp.json() if resp.content else {}
        if method.upper() == "GET" and use_cache:
            self._cache.set(key, data)
        return data

    # -----------------------------
    # Catalogs / Schemas / Tables
    # -----------------------------
    def list_catalogs(self, *, page_token: Optional[str] = None, max_results: Optional[int] = None) -> Dict[str, Any]:
        if self._use_gen:
            # generated client uses root host; path is encoded in API class
            # No pagination wiring here for brevity
            # Synchronous bridging: openapi gen methods are async; run directly (assumes async runtime in caller). Fallback to requests for sync paths.
            pass
        params: Dict[str, Any] = {}
        if page_token:
            params["page_token"] = page_token
        if max_results is not None:
            params["max_results"] = max_results
        return self._request("GET", "/catalogs", params=params, use_cache=True)

    def list_schemas(self, *, catalog_name: str, page_token: Optional[str] = None, max_results: Optional[int] = None) -> Dict[str, Any]:
        params: Dict[str, Any] = {"catalog_name": catalog_name}
        if page_token:
            params["page_token"] = page_token
        if max_results is not None:
            params["max_results"] = max_results
        return self._request("GET", "/schemas", params=params, use_cache=True)

    def list_tables(self, *, catalog_name: str, schema_name: str, page_token: Optional[str] = None, max_results: Optional[int] = None) -> Dict[str, Any]:
        params: Dict[str, Any] = {"catalog_name": catalog_name, "schema_name": schema_name}
        if page_token:
            params["page_token"] = page_token
        if max_results is not None:
            params["max_results"] = max_results
        return self._request("GET", "/tables", params=params, use_cache=True)

    def get_table(self, *, full_name: str) -> Dict[str, Any]:
        return self._request("GET", f"/tables/{urlparse.quote(full_name, safe='')}", use_cache=True)

    # -----------------------------
    # Permissions
    # -----------------------------
    def get_permissions(self, *, securable_type: str, full_name: str, principal: Optional[str] = None) -> Dict[str, Any]:
        params: Dict[str, Any] = {}
        if principal:
            params["principal"] = principal
        path = f"/permissions/{urlparse.quote(securable_type, safe='')}/{urlparse.quote(full_name, safe='')}"
        return self._request("GET", path, params=params, use_cache=True)

    def update_permissions(self, *, securable_type: str, full_name: str, changes: List[Dict[str, Any]]) -> Dict[str, Any]:
        body = {"changes": changes}
        path = f"/permissions/{urlparse.quote(securable_type, safe='')}/{urlparse.quote(full_name, safe='')}"
        return self._request("PATCH", path, body=body)

    # -----------------------------
    # Temporary credentials
    # -----------------------------
    def generate_temporary_table_credentials(self, *, table_id: str, operation: str) -> Dict[str, Any]:
        body = {"table_id": table_id, "operation": operation}
        return self._request("POST", "/temporary-table-credentials", body=body)

    def generate_temporary_volume_credentials(self, *, volume_id: str, operation: str) -> Dict[str, Any]:
        body = {"volume_id": volume_id, "operation": operation}
        return self._request("POST", "/temporary-volume-credentials", body=body)

    def generate_temporary_path_credentials(self, *, url: str, operation: str) -> Dict[str, Any]:
        body = {"url": url, "operation": operation}
        return self._request("POST", "/temporary-path-credentials", body=body)

    def generate_temporary_model_version_credentials(
        self,
        *,
        catalog_name: str,
        schema_name: str,
        model_name: str,
        version: int,
        operation: str,
    ) -> Dict[str, Any]:
        body = {
            "catalog_name": catalog_name,
            "schema_name": schema_name,
            "model_name": model_name,
            "version": version,
            "operation": operation,
        }
        return self._request("POST", "/temporary-model-version-credentials", body=body)

    # -----------------------------
    # Metastore
    # -----------------------------
    def get_metastore_summary(self) -> Dict[str, Any]:
        return self._request("GET", "/metastore_summary", use_cache=True)


__all__ = [
    "UnityCatalogREST",
    "UCError",
]


