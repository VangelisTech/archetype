# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Narrow Flecs Remote API adapter for Sander Mertens' live Biome world."""

from __future__ import annotations

import ipaddress
import re
from dataclasses import dataclass
from typing import Any

import httpx

from .contracts import (
    BiomeObservation,
    DepositObservation,
    DrillObservation,
    PlaceExtractorAction,
    TerrainCell,
)

DEPOSIT = "biome.miner.Deposit"
BUILDING = "biome.buildings.Building"
TERRAIN_POSITION = "flecs.engine.terrain.TerrainPosition"
POWER_CONSUMER = "biome.power.PowerConsumer"
MINER = "biome.miner.Miner"
STORAGE = "biome.resources.Storage"
NATIVE_PLACE_BUILDING = "/call/archetype/biome/placeBuilding"

_QUALIFIED_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)+$")


class FlecsRemoteError(RuntimeError):
    """A live Flecs endpoint rejected or could not satisfy an operation."""


@dataclass(frozen=True)
class NativePlacement:
    """Stable identity returned by Biome's native placement bridge."""

    entity_id: int
    entity_path: str


def _entity_path(payload: dict[str, Any]) -> str:
    parent = str(payload.get("parent") or "")
    name = str(payload.get("name") or "")
    return f"{parent}.{name}" if parent else name


def _rest_path(entity_path: str) -> str:
    return entity_path.replace(".", "/")


def _is_loopback(host: str) -> bool:
    if host.casefold() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


class BiomeClient:
    """Read observations and apply high-level actions to one Biome process."""

    def __init__(
        self,
        base_url: str = "http://127.0.0.1:27750",
        *,
        timeout: float = 5.0,
        client: httpx.Client | None = None,
        allow_remote: bool = False,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        parsed_url = httpx.URL(self.base_url)
        if parsed_url.scheme not in {"http", "https"} or not parsed_url.host:
            raise ValueError("base_url must be an HTTP(S) URL with a host")
        if not allow_remote and not _is_loopback(parsed_url.host):
            raise ValueError(
                "BiomeClient refuses a non-loopback Flecs endpoint by default; "
                "the upstream REST server has no authentication"
            )
        self._owns_client = client is None
        self._client = client or httpx.Client(base_url=self.base_url, timeout=timeout)

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> BiomeClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def is_ready(self) -> bool:
        try:
            response = self._client.get("/entity/flecs/core/World")
            return response.status_code == 200 and "WorldSummary" in response.text
        except httpx.HTTPError:
            return False

    def _request(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        try:
            response = self._client.request(method, path, **kwargs)
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise FlecsRemoteError(f"{method} {path} failed: {exc}") from exc

        if not response.content:
            return {}
        try:
            payload = response.json()
        except ValueError as exc:
            raise FlecsRemoteError(f"{method} {path} returned non-JSON data") from exc
        if not isinstance(payload, dict):
            raise FlecsRemoteError(f"{method} {path} returned a non-object JSON value")
        if payload.get("error"):
            raise FlecsRemoteError(str(payload["error"]))
        return payload

    def _query(self, expression: str) -> list[dict[str, Any]]:
        payload = self._request(
            "GET",
            "/query",
            params={"expr": expression, "values": "true", "entity_ids": "true"},
        )
        results = payload.get("results", [])
        if not isinstance(results, list):
            raise FlecsRemoteError("query response has no results list")
        return results

    def observe(self) -> BiomeObservation:
        deposits = tuple(self.list_deposits())
        occupied = frozenset(self.list_occupied_cells())
        return BiomeObservation(deposits=deposits, occupied_cells=occupied)

    def list_deposits(self) -> list[DepositObservation]:
        rows = self._query(f"{DEPOSIT},{TERRAIN_POSITION}")
        return [self._deposit_from_query(row) for row in rows]

    def list_occupied_cells(self) -> list[TerrainCell]:
        rows = self._query(f"{BUILDING},{TERRAIN_POSITION}")
        cells: list[TerrainCell] = []
        for row in rows:
            values = row.get("fields", {}).get("values", [])
            if len(values) < 2:
                raise FlecsRemoteError("building query omitted TerrainPosition")
            position = values[1]
            cells.append(TerrainCell(x=int(position["x"]), y=int(position["y"])))
        return cells

    def get_deposit(self, entity_path: str) -> DepositObservation:
        payload = self._request("GET", f"/entity/{_rest_path(entity_path)}")
        components = payload.get("components", {})
        deposit = components.get(DEPOSIT)
        position = components.get(TERRAIN_POSITION)
        if not isinstance(deposit, dict) or not isinstance(position, dict):
            raise FlecsRemoteError(f"{entity_path} is not an observable terrain deposit")
        return DepositObservation(
            entity_id=int(payload.get("id", 0)),
            entity_path=_entity_path(payload),
            resource=str(deposit["resource"]),
            amount=int(deposit["amount"]),
            terrain=str(position["terrain"]),
            cell=TerrainCell(x=int(position["x"]), y=int(position["y"])),
        )

    def get_drill(self, entity_path: str, resource: str) -> DrillObservation:
        payload = self._request("GET", f"/entity/{_rest_path(entity_path)}")
        components = payload.get("components", {})
        power = components.get(POWER_CONSUMER, {})
        miner = components.get(MINER, {})
        storage = components.get(STORAGE, {})
        resources = storage.get("resources", {}) if isinstance(storage, dict) else {}
        return DrillObservation(
            entity_id=int(payload.get("id", 0)),
            entity_path=_entity_path(payload),
            powered=bool(power.get("powered", False)),
            deposit_path=str(miner.get("deposit", "#0")),
            stored_resource=resource,
            stored_amount=int(resources.get(resource, 0)),
        )

    def deploy(self, action: PlaceExtractorAction) -> None:
        """Install a managed action script that composes real Biome prefabs."""

        self._request("PUT", f"/entity/{action.script_name}")
        script = self._render_action_script(action)
        self._request(
            "PUT",
            f"/script/{action.script_name}",
            content=script,
            headers={"Content-Type": "text/plain"},
        )

    def place_building(
        self,
        prefab: str,
        terrain: str,
        cell: TerrainCell,
        *,
        name: str,
    ) -> NativePlacement:
        """Purchase and place one building through Biome's native factory path.

        The reflected C function derives the footprint from the selected prefab,
        charges Biome's real resource storages, invokes ``biomePlaceBuilding``,
        and refunds the purchase if native placement rejects the action.
        """

        for value, field in ((prefab, "prefab"), (terrain, "terrain")):
            if not _QUALIFIED_IDENTIFIER.fullmatch(value):
                raise ValueError(f"{field} must be a qualified Flecs identifier, got {value!r}")
        if not name.isascii() or not name.isidentifier():
            raise ValueError(f"name must be a Flecs identifier, got {name!r}")

        try:
            response = self._client.get(
                NATIVE_PLACE_BUILDING,
                params={
                    "prefab": prefab,
                    "terrain": terrain,
                    "x": cell.x,
                    "y": cell.y,
                    "name": name,
                },
            )
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise FlecsRemoteError(f"GET {NATIVE_PLACE_BUILDING} failed: {exc}") from exc

        try:
            payload = response.json()
        except ValueError as exc:
            raise FlecsRemoteError(f"GET {NATIVE_PLACE_BUILDING} returned non-JSON data") from exc
        if isinstance(payload, dict) and payload.get("error"):
            raise FlecsRemoteError(str(payload["error"]))
        if isinstance(payload, str):
            if not payload.isascii() or not payload.isdecimal():
                raise FlecsRemoteError("native placement returned a non-integer entity id")
            payload = int(payload)
        if isinstance(payload, bool) or not isinstance(payload, int):
            raise FlecsRemoteError("native placement returned a non-integer entity id")
        if payload <= 0:
            raise FlecsRemoteError(f"Biome rejected placement of {prefab} at ({cell.x}, {cell.y})")
        return NativePlacement(
            entity_id=payload,
            entity_path=f"scene.buildings.{name}",
        )

    @staticmethod
    def _deposit_from_query(row: dict[str, Any]) -> DepositObservation:
        values = row.get("fields", {}).get("values", [])
        if len(values) < 2:
            raise FlecsRemoteError("deposit query omitted reflected values")
        deposit, position = values[:2]
        return DepositObservation(
            entity_id=int(row["id"]),
            entity_path=_entity_path(row),
            resource=str(deposit["resource"]),
            amount=int(deposit["amount"]),
            terrain=str(position["terrain"]),
            cell=TerrainCell(x=int(position["x"]), y=int(position["y"])),
        )

    @staticmethod
    def _render_action_script(action: PlaceExtractorAction) -> str:
        return f"""using flecs.engine.*
using biome.*

{action.namespace} {{
    {action.drill_name} : buildings.Drill {{
        TerrainPosition: {{
            terrain: {action.terrain}
            x: {action.drill_cell.x}
            y: {action.drill_cell.y}
            span_x: 1
            span_y: 1
        }}
    }}

    {action.power_name} : buildings.Solar {{
        TerrainPosition: {{
            terrain: {action.terrain}
            x: {action.power_cell.x}
            y: {action.power_cell.y}
            span_x: 1
            span_y: 1
        }}
    }}
}}
"""
