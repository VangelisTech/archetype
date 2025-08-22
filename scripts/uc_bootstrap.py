#!/usr/bin/env python3
"""
Unity Catalog bootstrap helper.

Ensures a catalog and schema exist and (optionally) grants basic privileges to a user.

Env vars (or CLI args) used:
  UC_ENDPOINT, UC_ADMIN_TOKEN, UC_CATALOG, UC_SCHEMA, USER_EMAIL

Examples:
  UC_ENDPOINT=http://localhost:8080/api/2.1/unity-catalog \
  UC_ADMIN_TOKEN=$(make uc-admin-token) \
  UC_CATALOG=unity UC_SCHEMA=default \
  USER_EMAIL=user@example.com \
  uv run python scripts/uc_bootstrap.py
"""

from __future__ import annotations

import os
import sys
import argparse
from typing import Dict, Any

from archetype.integrations.unity.uc_client import UnityCatalogREST, UCError


def ensure_catalog(uc: UnityCatalogREST, catalog: str) -> None:
    cats = uc.list_catalogs()
    names = {c.get("name") for c in cats.get("catalogs", []) or []}
    if catalog in names:
        print(f"catalog exists: {catalog}")
        return
    try:
        # Minimal payload
        uc._request("POST", "/catalogs", body={"name": catalog, "comment": "bootstrap"})
        print(f"catalog created: {catalog}")
    except UCError as e:
        print(f"WARN: failed to create catalog {catalog}: {e}")


def ensure_schema(uc: UnityCatalogREST, catalog: str, schema: str) -> None:
    sch = uc.list_schemas(catalog_name=catalog)
    names = {s.get("name") for s in sch.get("schemas", []) or []}
    if schema in names:
        print(f"schema exists: {catalog}.{schema}")
        return
    try:
        uc._request("POST", "/schemas", body={"name": schema, "catalog_name": catalog, "comment": "bootstrap"})
        print(f"schema created: {catalog}.{schema}")
    except UCError as e:
        print(f"WARN: failed to create schema {catalog}.{schema}: {e}")


def grant_basic(uc: UnityCatalogREST, catalog: str, schema: str, user_email: str) -> None:
    """Best-effort: grant USE CATALOG and USE SCHEMA to the user."""
    try:
        uc.update_permissions(
            securable_type="catalog",
            full_name=catalog,
            changes=[{"principal": user_email, "add": ["USE CATALOG"]}],
        )
        print(f"granted USE CATALOG on {catalog} to {user_email}")
    except Exception as e:
        print(f"WARN: granting catalog perms failed: {e}")
    try:
        uc.update_permissions(
            securable_type="schema",
            full_name=f"{catalog}.{schema}",
            changes=[{"principal": user_email, "add": ["USE SCHEMA"]}],
        )
        print(f"granted USE SCHEMA on {catalog}.{schema} to {user_email}")
    except Exception as e:
        print(f"WARN: granting schema perms failed: {e}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Bootstrap UC catalog/schema and grants")
    p.add_argument("--endpoint", default=os.getenv("UC_ENDPOINT"), help="UC endpoint, e.g. http://localhost:8080/api/2.1/unity-catalog")
    p.add_argument("--token", default=os.getenv("UC_ADMIN_TOKEN"), help="Admin bearer token")
    p.add_argument("--catalog", default=os.getenv("UC_CATALOG", "unity"))
    p.add_argument("--schema", default=os.getenv("UC_SCHEMA", "default"))
    p.add_argument("--user-email", default=os.getenv("USER_EMAIL"))
    return p.parse_args()


def main() -> int:
    args = parse_args()
    if not args.endpoint or not args.token:
        print("ERROR: Provide UC endpoint and admin token via --endpoint/--token or env vars.")
        return 2
    uc = UnityCatalogREST(endpoint=args.endpoint, token=args.token)
    ensure_catalog(uc, args.catalog)
    ensure_schema(uc, args.catalog, args.schema)
    if args.user_email:
        grant_basic(uc, args.catalog, args.schema, args.user_email)
    else:
        print("INFO: USER_EMAIL not provided; skipping grants.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


