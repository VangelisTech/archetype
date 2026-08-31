# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Concrete named-Modal runtime for durable Mission controller jobs."""

from __future__ import annotations

import asyncio
import re
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any

from archetype.missions.modal_jobs import (
    ModalMissionFamily,
    ModalMissionJobRef,
    ModalMissionJobStillRunning,
)

_MODAL_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$")

ModalMissionResultReader = Callable[[ModalMissionJobRef], Awaitable[bytes | None]]


def _require_modal_name(value: str, field: str) -> None:
    if not isinstance(value, str) or _MODAL_NAME.fullmatch(value) is None:
        raise ValueError(f"{field} must be a valid Modal name")


def _load_modal() -> Any:
    try:
        import modal
    except ImportError as exc:  # pragma: no cover - depends on an optional extra
        raise RuntimeError(
            "Modal Mission jobs require the archetype-missions[modal] extra"
        ) from exc
    return modal


@dataclass(frozen=True, slots=True)
class ModalMissionJobRuntimeConfig:
    """Exact named Modal objects used by the durable Mission job runtime."""

    workspace_name: str
    environment_name: str
    app_name: str
    job_dict_name: str
    author_function_name: str
    critic_function_name: str
    create_if_missing: bool = False

    def __post_init__(self) -> None:
        for field in (
            "workspace_name",
            "environment_name",
            "app_name",
            "job_dict_name",
            "author_function_name",
            "critic_function_name",
        ):
            _require_modal_name(getattr(self, field), f"Modal Mission {field}")
        if not isinstance(self.create_if_missing, bool):
            raise ValueError("Modal Mission create_if_missing must be a boolean")

    def function_name(self, family: ModalMissionFamily) -> str:
        if family == "author":
            return self.author_function_name
        if family == "critic":
            return self.critic_function_name
        raise ValueError("Modal Mission job family is invalid")


class ModalNamedMissionJobRuntime:
    """Use named Modal durability and deployed Functions for Mission jobs.

    The result-ready predicate is injected because author and critic result
    catalogs own different durable keys and codecs. Keeping that read-only seam
    here avoids coupling this provider runtime to either family implementation.
    """

    def __init__(
        self,
        config: ModalMissionJobRuntimeConfig,
        *,
        result_reader: ModalMissionResultReader,
    ) -> None:
        self._config = config
        self._result_reader = result_reader
        self._lock = asyncio.Lock()
        self._modal: Any | None = None
        self._client: Any | None = None
        self._dictionary: Any | None = None
        self._functions: dict[ModalMissionFamily, Any] = {}

    async def get(self, key: str) -> object:
        dictionary = await self._get_dictionary()
        return await dictionary.get.aio(key, None)

    async def put_if_absent(self, key: str, value: Mapping[str, Any]) -> bool:
        dictionary = await self._get_dictionary()
        return bool(await dictionary.put.aio(key, dict(value), skip_if_exists=True))

    async def spawn(
        self,
        *,
        family: ModalMissionFamily,
        operation_id: str,
        request_bytes: bytes,
        namespace_digest: str,
    ) -> object:
        function = await self._get_function(family)
        return await function.spawn.aio(family, operation_id, request_bytes, namespace_digest)

    def call_id(self, call: object) -> str:
        value = getattr(call, "object_id", None)
        if not isinstance(value, str) or not value:
            raise TypeError("Modal Mission FunctionCall has no durable object identity")
        return value

    async def reattach(self, call_id: str) -> object:
        modal, client = await self._get_context()
        return modal.FunctionCall.from_id(call_id, client=client)

    async def cancel(self, call: object) -> None:
        cancel = getattr(call, "cancel", None)
        if cancel is None or not hasattr(cancel, "aio"):
            raise TypeError("Modal Mission FunctionCall has no async cancellation boundary")
        await cancel.aio()

    async def call_result(self, call: object, *, timeout_seconds: float) -> object:
        if isinstance(timeout_seconds, bool) or timeout_seconds != 0:
            raise ValueError("Modal Mission job polling must use a zero-second timeout")
        get = getattr(call, "get", None)
        if get is None or not hasattr(get, "aio"):
            raise TypeError("Modal Mission FunctionCall has no async result boundary")
        try:
            return await get.aio(timeout=0)
        except TimeoutError:
            raise ModalMissionJobStillRunning from None

    async def result_ready(self, ref: ModalMissionJobRef) -> bool:
        return await self.result_payload(ref) is not None

    async def result_payload(self, ref: ModalMissionJobRef) -> bytes | None:
        payload = await self._result_reader(ref)
        if payload is not None and type(payload) is not bytes:
            raise TypeError("Modal Mission result reader must return bytes or None")
        return payload

    async def _get_context(self) -> tuple[Any, Any]:
        if self._modal is not None and self._client is not None:
            return self._modal, self._client
        async with self._lock:
            if self._modal is None or self._client is None:
                modal = _load_modal()
                workspace = modal.Workspace.from_context()
                await workspace.hydrate.aio()
                observed = str(workspace.name or "")
                if observed != self._config.workspace_name:
                    raise RuntimeError("Modal workspace does not match the Mission job namespace")
                self._modal = modal
                self._client = workspace.client
            return self._modal, self._client

    async def _get_dictionary(self) -> Any:
        if self._dictionary is not None:
            return self._dictionary
        modal, client = await self._get_context()
        async with self._lock:
            if self._dictionary is None:
                dictionary = modal.Dict.from_name(
                    self._config.job_dict_name,
                    environment_name=self._config.environment_name,
                    create_if_missing=self._config.create_if_missing,
                    client=client,
                )
                await dictionary.hydrate.aio()
                self._dictionary = dictionary
            return self._dictionary

    async def _get_function(self, family: ModalMissionFamily) -> Any:
        function_name = self._config.function_name(family)
        if family in self._functions:
            return self._functions[family]
        modal, client = await self._get_context()
        async with self._lock:
            if family not in self._functions:
                function = modal.Function.from_name(
                    self._config.app_name,
                    function_name,
                    environment_name=self._config.environment_name,
                    client=client,
                )
                await function.hydrate.aio()
                self._functions[family] = function
            return self._functions[family]


__all__ = [
    "ModalMissionJobRuntimeConfig",
    "ModalMissionResultReader",
    "ModalNamedMissionJobRuntime",
]
