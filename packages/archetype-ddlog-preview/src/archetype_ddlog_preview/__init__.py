"""Explicit local preview, independent of the retained ArchetypeRuntime API.

Native calls release the GIL. Use asyncio.to_thread for blocking calls in async
applications; cancelling that waiter does not cancel the native operation.
Always close explicitly. No finalizer attempts process control during GC/fork.
"""

from __future__ import annotations

import ctypes
import json
import math
import os
from pathlib import Path
from typing import Any

from .programs import Composition, LeafProgram, ProgramReference
from .values import identifier, string_cell

__all__ = ["Host", "NativeError", "ProtocolError", "ConstructionCleanupError"]


class NativeError(RuntimeError):
    """Native failure, with no implied rollback or permission to replay inputs."""

    def __init__(self, kind: str, message: str, operation: str, *, code: str | None = None):
        super().__init__(f"{operation}: {kind}: {message}")
        self.kind, self.operation = kind, operation
        self.code = code


class ProtocolError(RuntimeError):
    """The explicitly supplied library violated the versioned buffer protocol."""


class ConstructionCleanupError(RuntimeError):
    """Interrupted construction whose cleanup failed; `host` retains ownership.

    Repair the reported failure and call error.host.close() explicitly. Keep
    the exception/host alive until close succeeds or the process exits.
    """

    def __init__(self, host: Host, original: BaseException, cleanup: BaseException):
        super().__init__(f"Host construction interrupted; close failed: {cleanup}")
        self.host = host
        self.original = original
        self.cleanup = cleanup


class _Buffer(ctypes.Structure):
    _fields_ = [("data", ctypes.c_void_p), ("length", ctypes.c_size_t)]


def _validate(value: Any, depth: int = 0) -> None:
    if depth > 64:
        raise ValueError("JSON nesting exceeds 64")
    if value is None or type(value) in (str, bool):
        return
    if type(value) is int:
        if not -(2**63) <= value < 2**64:
            raise ValueError("Integer outside native JSON range")
        return
    if type(value) is float and math.isfinite(value):
        return
    if type(value) is list:
        for item in value:
            _validate(item, depth + 1)
        return
    if type(value) is dict:
        for key, item in value.items():
            if type(key) is not str:
                raise TypeError("JSON dictionary keys must be strings")
            _validate(item, depth + 1)
        return
    raise TypeError("Only JSON dictionaries, lists, strings, exact integers, bool and null")


def _absolute(value: os.PathLike[str] | str) -> str:
    result = os.fspath(value)
    if not isinstance(result, str) or not Path(result).is_absolute():
        raise ValueError("Native library and operator paths must be absolute strings")
    return result


def _object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ProtocolError("Duplicate response key")
        result[key] = value
    return result


class Host:
    """One existing Rust WorldManager, hosted CutStore and executor.

    No implicit start, tick, polling, admission retry or publication. This
    trusted local surface is not an authenticated host or ArchetypeRuntime.
    The library is a separately built native prerequisite, never auto-loaded
    on import or downloaded. A failed close retains its handle for retry.
    """

    def __init__(
        self,
        *,
        library: os.PathLike[str] | str,
        registry_root: os.PathLike[str] | str | None,
        build_root: os.PathLike[str] | str | None,
        driver: os.PathLike[str] | str | None,
        store_root: os.PathLike[str] | str,
    ):
        self._pid = os.getpid()
        self._handle = 0
        config = {
            "registry_root": None if registry_root is None else _absolute(registry_root),
            "build_root": None if build_root is None else _absolute(build_root),
            "driver": None if driver is None else _absolute(driver),
            "store_root": _absolute(store_root),
        }
        self._lib = ctypes.CDLL(_absolute(library))
        self._lib.arct_ddlog_abi_version.argtypes = []
        self._lib.arct_ddlog_abi_version.restype = ctypes.c_uint32
        if self._lib.arct_ddlog_abi_version() != 1:
            raise ProtocolError("Expected DDlog preview ABI 1")
        try:
            contract = self._lib.arct_ddlog_contract_version
        except AttributeError as error:
            raise ProtocolError("Missing DDlog operation/schema/cell contract version") from error
        contract.argtypes = []
        contract.restype = ctypes.c_uint32
        if contract() != 2:
            raise ProtocolError("Expected DDlog operation/schema/cell contract 2")
        out = ctypes.POINTER(_Buffer)
        self._lib.arct_ddlog_open.argtypes = [ctypes.c_void_p, ctypes.c_size_t, out]
        self._lib.arct_ddlog_open.restype = ctypes.c_int
        self._lib.arct_ddlog_call.argtypes = [
            ctypes.c_uint64,
            ctypes.c_void_p,
            ctypes.c_size_t,
            out,
        ]
        self._lib.arct_ddlog_call.restype = ctypes.c_int
        self._lib.arct_ddlog_close.argtypes = [ctypes.c_uint64, out]
        self._lib.arct_ddlog_close.restype = ctypes.c_int
        self._lib.arct_ddlog_buffer_free.argtypes = [out]
        self._lib.arct_ddlog_buffer_free.restype = None
        try:
            result = self._exchange("open", config, transport="open")
            self.ddlog_revision = result["ddlog_revision"]
        except BaseException as original:
            # Also covers signal delivery as ctypes returns from successful open.
            # _exchange adopts the returned handle before freeing its buffer.
            try:
                self.close()
            except BaseException as cleanup:
                raise ConstructionCleanupError(self, original, cleanup) from original
            raise

    def _owner(self) -> None:
        if self._pid != os.getpid():
            raise NativeError("forked", "Inherited host requires exec in a new process", "handle")

    def _exchange(
        self, operation: str, request: dict[str, Any] | None = None, *, transport: str = "call"
    ) -> Any:
        self._owner()
        data = b""
        if request is not None:
            _validate(request)
            data = json.dumps(
                request, ensure_ascii=False, allow_nan=False, separators=(",", ":")
            ).encode("utf-8")
            if not 0 < len(data) <= 1024 * 1024:
                raise ValueError("Request exceeds 1 MiB")
        output = _Buffer()
        # Keep self/CDLL, input bytes and native call alive together. ctypes
        # releases the GIL; no Python lock spans native work or close.
        try:
            if transport == "open":
                status = self._lib.arct_ddlog_open(data, len(data), ctypes.byref(output))
            elif transport == "close":
                status = self._lib.arct_ddlog_close(self._handle, ctypes.byref(output))
            else:
                status = self._lib.arct_ddlog_call(
                    self._handle, data, len(data), ctypes.byref(output)
                )
            response = self._response(output)
            if transport == "open" and response["ok"]:
                self._handle = response["value"]["handle"]
            if not response["ok"]:
                error = response["error"]
                raise NativeError(
                    error["kind"], error["message"], operation, code=error.get("code")
                )
            if status != 0:
                raise ProtocolError("Native status/envelope mismatch")
            return response["value"]
        except BaseException:
            if transport == "open" and not self._handle and output.data:
                # A signal can interrupt the CDLL return before `status` or
                # Python ownership is assigned. Recover only from its own
                # successful response; never probe or guess a handle.
                response = self._response(output)
                if response["ok"]:
                    self._handle = response["value"]["handle"]
            raise
        finally:
            self._lib.arct_ddlog_buffer_free(ctypes.byref(output))

    @staticmethod
    def _response(output: _Buffer) -> dict[str, Any]:
        if not output.data or not 0 < output.length <= 16 * 1024 * 1024:
            raise ProtocolError("Invalid native response buffer")

        def invalid_constant(value: str) -> None:
            raise ProtocolError(f"Nonfinite response number: {value}")

        try:
            response = json.loads(
                ctypes.string_at(output.data, output.length),
                object_pairs_hook=_object,
                parse_constant=invalid_constant,
            )
        except (ValueError, UnicodeError) as error:
            raise ProtocolError("Malformed native response JSON") from error
        if not isinstance(response, dict) or type(response.get("ok")) is not bool:
            raise ProtocolError("Invalid response envelope")
        if response["ok"]:
            if "value" not in response:
                raise ProtocolError("Missing native result")
        else:
            error = response.get("error")
            if not isinstance(error, dict) or any(
                type(error.get(k)) is not str for k in ("kind", "message")
            ):
                raise ProtocolError("Malformed native error")
            if "code" in error and type(error["code"]) is not str:
                raise ProtocolError("Malformed native error code")
        return response

    def request(self, op: str, **arguments: Any) -> Any:
        """Send a typed operation; unknown operations/fields fail in native code."""
        self._owner()
        if not self._handle:
            raise NativeError("closed", "Host is closed", op)
        return self._exchange(op, {"op": op, **arguments})

    def register(self, name: str, definition: dict[str, Any]) -> dict[str, Any]:
        return self.request("register", request={"name": name, "definition": definition})

    def publish_program(
        self,
        resource: str,
        *,
        request_key: str,
        description: str,
        definition: LeafProgram | Composition,
    ) -> dict[str, Any]:
        """Reconcile one registry-owned logical publication and its exact pin."""
        identifier(resource)
        identifier(request_key)
        string_cell(description)
        if type(definition) not in (LeafProgram, Composition):
            raise ValueError("Expected immutable typed program declaration")
        if isinstance(definition, Composition):
            for ref in definition.references():
                retained = self.resolve_program(ref.resource)
                if (
                    retained["resource"] != ref.resource
                    or retained["processor"] != ref.pin()
                    or retained["phase"] != "published"
                ):
                    raise ValueError("Reference differs from logical program")
        return self.request(
            "logical",
            request={
                "op": "program_publish",
                "request": {
                    "resource": resource,
                    "request_key": request_key,
                    "description": description,
                    "definition": definition.native(),
                    "git_provenance": None,
                    "lowering_version": 2,
                },
            },
        )

    def resolve_program(self, resource: str) -> dict[str, Any]:
        return self.request(
            "logical", request={"op": "program_resolve", "resource": identifier(resource)}
        )

    def describe_program(self, resource: str) -> dict[str, Any]:
        return self.request(
            "logical", request={"op": "program_describe", "resource": identifier(resource)}
        )

    def list_programs(
        self, *, limit: int = 32, after: str | None = None, include_archived: bool = False
    ) -> dict[str, Any]:
        """Trusted bounded registry inventory, including non-logical definitions."""
        if type(limit) is not int or not 1 <= limit <= 100 or type(include_archived) is not bool:
            raise ValueError("Invalid program listing bounds")
        if after is not None:
            identifier(after)
        return self.request(
            "logical",
            request={
                "op": "program_list",
                "limit": limit,
                "after": after,
                "include_archived": include_archived,
            },
        )

    def create_logical(
        self,
        resource: str,
        *,
        world: str,
        run: str,
        request_key: str,
        label: str,
        program: ProgramReference,
        components: list[dict[str, Any]],
        inputs: dict[str, list[str]],
    ) -> dict[str, Any]:
        """Reserve, publish context, and acknowledge one native-owned birth.

        Input types use native names (int/string/bool/double).
        Creation does not start a compiler or submit any live inputs.
        """
        if type(program) is not ProgramReference:
            raise ValueError("Expected exact logical program reference")
        return self.request(
            "logical",
            request={
                "op": "world_create",
                "destination": {
                    "resource": identifier(resource),
                    "world": identifier(world),
                    "run": identifier(run),
                },
                "request_key": identifier(request_key),
                "label": string_cell(label),
                "program": program.native(),
                "declarations": {"components": components, "inputs": inputs},
            },
        )

    def resolve_logical(self, resource: str, *, world: str, run: str) -> dict[str, Any]:
        return self.request(
            "logical",
            request={
                "op": "world_resolve",
                "destination": {
                    "resource": identifier(resource),
                    "world": identifier(world),
                    "run": identifier(run),
                },
            },
        )

    def create(self, label: str, processor: dict[str, str], outputs: list[str]) -> str:
        """Create a native policy world; bind analytical scope separately."""
        return self.request("create", label=label, processor=processor, outputs=outputs)["id"]

    def bind(self, scope: dict[str, str], components: list[dict[str, Any]]) -> dict[str, Any]:
        binding = {"scope": scope, "components": components}
        self.request("bind", binding=binding)
        # Owned snapshot of configuration only, never native execution state.
        return json.loads(json.dumps(binding))

    def start(self, id: str) -> dict[str, Any]:
        return self.request("start", id=id)

    def status(self, id: str) -> dict[str, Any]:
        return self.request("status", id=id)

    def stop(self, id: str) -> dict[str, Any]:
        return self.request("stop", id=id)

    def admit(
        self,
        binding: dict[str, Any],
        *,
        expected_head: str | None,
        generation: int,
        revision: int,
        key: str,
        changes: list[dict[str, Any]],
    ) -> dict[str, Any]:
        for value in (generation, revision):
            if type(value) is not int or not 0 <= value < 2**64:
                raise ValueError("Generation/revision must be exact unsigned 64-bit integers")
        for change in changes:
            for value in change["values"]:
                if not (
                    type(value) in (str, bool)
                    or type(value) is int
                    and -(2**63) <= value < 2**63
                    or type(value) is float
                    and math.isfinite(value)
                ):
                    raise ValueError("Cells must be signed Int64, string, Bool or finite Float64")
        return self.request(
            "admit",
            binding=binding,
            expected_head=expected_head,
            admission={
                "id": binding["scope"]["native_world"],
                "expected_generation": generation,
                "expected_revision": revision,
                "admission_key": key,
                "changes": changes,
            },
        )

    def admission_status(self, id: str, generation: int, key: str) -> dict[str, Any]:
        return self.request(
            "admission_status", query={"id": id, "generation": generation, "admission_key": key}
        )

    def publish(self, binding: dict[str, Any], key: dict[str, Any]) -> dict[str, Any]:
        return self.request("publish", binding=binding, key=key)

    def reconcile(
        self,
        binding: dict[str, Any],
        key: dict[str, Any],
        *,
        tick: int,
        expected_parent: str | None,
    ) -> dict[str, Any]:
        return self.request(
            "reconcile", binding=binding, key=key, tick=tick, expected_parent=expected_parent
        )

    def confirm(
        self,
        binding: dict[str, Any],
        key: dict[str, Any],
        *,
        tick: int,
        expected_parent: str | None,
    ) -> dict[str, Any]:
        return self.request(
            "confirm", binding=binding, key=key, tick=tick, expected_parent=expected_parent
        )

    def history(self, world: str, run: str, *, offset: int = 0, limit: int = 100) -> dict[str, Any]:
        return self.request("history", world=world, run=run, offset=offset, limit=limit)

    def read(
        self, receipt: dict[str, Any], component: str, *, offset: int = 0, limit: int = 1000
    ) -> dict[str, Any]:
        return self.request(
            "read",
            receipt={k: receipt[k] for k in ("world", "run", "tick", "cut_id")},
            component=component,
            offset=offset,
            limit=limit,
        )

    def restore(
        self, binding: dict[str, Any], receipt: dict[str, Any], *, expected_generation: int
    ) -> dict[str, Any]:
        return self.request(
            "restore",
            binding=binding,
            receipt={k: receipt[k] for k in ("world", "run", "tick", "cut_id")},
            expected_generation=expected_generation,
        )

    def fork(
        self,
        binding: dict[str, Any],
        receipt: dict[str, Any],
        *,
        world: str,
        run: str,
        label: str,
        request_key: str,
        expected_generation: int = 0,
    ) -> dict[str, Any]:
        """Reserve and progress one exact historical fork; never replay inputs.

        A starting reply keeps lineage unready. Repeat the exact call to observe
        completion and confirm its durable origin. Interrupted restore requires
        the caller to supply the newly observed generation explicitly.
        """
        return self.request(
            "fork",
            binding=binding,
            receipt={k: receipt[k] for k in ("world", "run", "tick", "cut_id")},
            destination={"world": world, "run": run},
            label=label,
            request_key=request_key,
            expected_generation=expected_generation,
        )

    def fork_binding(
        self,
        world: str,
        run: str,
        components: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Resolve immutable child identity from its durable analytical origin."""
        return self.request(
            "fork_binding", destination={"world": world, "run": run}, components=components
        )

    def close(self) -> None:
        self._owner()
        if self._handle:
            self._exchange("close", transport="close")
            self._handle = 0

    def __enter__(self) -> Host:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def publish_collection(self, world: str, run: str) -> dict[str, Any]:
        """Publish a nonexecuting artifact collection; exact scope retries adopt."""
        return self.request("publish_collection", world=world, run=run)

    def publish_hosted_context(self, binding: dict[str, Any]) -> dict[str, Any]:
        """Persist verified hosted declarations without starting or ticking."""
        return self.request("publish_hosted_context", binding=binding)

    def context(self, world: str, run: str) -> dict[str, Any]:
        """Verify a published descriptor using only retained storage evidence."""
        return self.request("context_at", world=world, run=run)


class Store(Host):
    """Own only CutStore and its executor, with the same lease/close protocol.

    No manager, registry or native build driver is constructed. Simulation
    operations require a Host; context and immutable read operations work here.
    """

    def __init__(self, *, library: os.PathLike[str] | str, store_root: os.PathLike[str] | str):
        super().__init__(
            library=library, store_root=store_root, registry_root=None, build_root=None, driver=None
        )
