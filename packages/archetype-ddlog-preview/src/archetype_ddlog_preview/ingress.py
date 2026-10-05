"""Transport-neutral, capability-gated local preview over one trusted Host.

There is no listener, credential provisioner, native owner or retry scheduler.
The operator supplies a real verifier and immutable resource grants.
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Protocol

from archetype_ddlog_preview import Host, NativeError
from archetype_ddlog_preview import wire as w


class Principal(Protocol):
    """Structural contract of archetype.api.principals.Principal."""

    @property
    def principal_id(self) -> str: ...

    @property
    def capabilities(self) -> frozenset[str]: ...


class PrincipalVerifier(Protocol):
    """Supply the existing real PrincipalDirectory; no verifier is invented here."""

    @property
    def configured(self) -> bool: ...

    def authenticate(self, credential: str) -> Principal: ...


@dataclass(frozen=True, slots=True)
class Component:
    name: str
    output: str
    fields: tuple[str, ...]
    entity_field: int

    def native(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "output": self.output,
            "fields": list(self.fields),
            "entity_field": self.entity_field,
        }


@dataclass(frozen=True, slots=True)
class Resource:
    """Immutable operator configuration; no live state, head or admission inventory."""

    name: str
    native_world: str
    world: str
    run: str
    components: tuple[Component, ...]
    inputs: tuple[tuple[str, tuple[str, ...]], ...]

    @classmethod
    def from_binding(
        cls, name: str, binding: dict[str, Any], *, inputs: dict[str, tuple[str, ...]]
    ) -> Resource:
        w.fields(binding, "scope components")
        scope = w.fields(binding["scope"], "native_world world run")
        components = []
        for raw in binding["components"]:
            row = w.fields(raw, "name output fields entity_field")
            components.append(
                Component(row["name"], row["output"], tuple(row["fields"]), row["entity_field"])
            )
        return cls(
            name,
            scope["native_world"],
            scope["world"],
            scope["run"],
            tuple(components),
            tuple((name, tuple(types)) for name, types in inputs.items()),
        )

    def __post_init__(self) -> None:
        for value in (self.name, self.native_world, self.world, self.run):
            w.identifier(value)
        if type(self.components) is not tuple or not 1 <= len(self.components) <= 64:
            raise ValueError("Invalid component configuration")
        for c in self.components:
            if type(c) is not Component:
                raise ValueError("Expected immutable component configuration")
            w.identifier(c.name)
            w.identifier(c.output)
            if type(c.fields) is not tuple or not 1 <= len(c.fields) <= w.MAX_CELLS:
                raise ValueError("Invalid component fields")
            for field in c.fields:
                w.identifier(field)
            if type(c.entity_field) is not int or not 0 <= c.entity_field < len(c.fields):
                raise ValueError("Invalid entity field")
        if len({c.name for c in self.components}) != len(self.components) or len(
            {c.output for c in self.components}
        ) != len(self.components):
            raise ValueError("Duplicate component configuration")
        if type(self.inputs) is not tuple or len(self.inputs) > 64:
            raise ValueError("Invalid input configuration")
        for entry in self.inputs:
            if type(entry) is not tuple or len(entry) != 2:
                raise ValueError("Expected immutable input configuration")
            predicate, types = entry
            w.identifier(predicate)
            if (
                type(types) is not tuple
                or not 1 <= len(types) <= w.MAX_CELLS
                or any(t not in ("int64", "string") for t in types)
            ):
                raise ValueError("Invalid input schema")
        if len(dict(self.inputs)) != len(self.inputs):
            raise ValueError("Duplicate input configuration")

    def binding(self) -> dict[str, Any]:
        return {
            "scope": {"native_world": self.native_world, "world": self.world, "run": self.run},
            "components": [c.native() for c in self.components],
        }


@dataclass(frozen=True, slots=True)
class Grant:
    principal_id: str
    resource: str
    capabilities: frozenset[str]

    def __post_init__(self) -> None:
        w.identifier(self.principal_id)
        w.identifier(self.resource)
        if type(self.capabilities) is not frozenset or not self.capabilities <= frozenset(
            w.CAPABILITIES.values()
        ):
            raise ValueError("Unknown capability grant")


def _error(code: str, *, dispatched: bool = False) -> bytes:
    return w.response(
        {
            "version": 1,
            "ok": False,
            "error": {"code": code, "outcome": "unknown" if dispatched else "not_dispatched"},
        }
    )


class Ingress:
    """One event-loop-bound ingress borrowing an already-owned Host.

    authenticate -> decode exact model -> capability/resource grant -> binding
    resolution -> bounded concurrent call -> safe projection. No retry or poll.
    drain() does not close the Host; the operator retains all process ownership.
    """

    def __init__(
        self,
        host: Host,
        *,
        verifier: PrincipalVerifier,
        resources: tuple[Resource, ...],
        grants: tuple[Grant, ...],
        max_inflight: int = 4,
    ):
        if verifier.configured is not True:
            raise ValueError("A configured real principal verifier is required")
        if type(max_inflight) is not int or not 1 <= max_inflight <= 16:
            raise ValueError("max_inflight must be 1..16")
        # Snapshots contain composition and access configuration, never world state.
        for keys in (
            [r.name for r in resources],
            [r.native_world for r in resources],
            [(r.world, r.run) for r in resources],
        ):
            if len(set(keys)) != len(resources):
                raise ValueError("Conflicting resource binding")
        configured = {r.name: r for r in resources}
        allowed = {}
        for grant in grants:
            key = (grant.principal_id, grant.resource)
            if grant.resource not in configured or key in allowed:
                raise ValueError("Unknown or duplicate resource grant")
            allowed[key] = grant.capabilities
        self._resources = MappingProxyType(configured)
        self._grants = MappingProxyType(allowed)
        self._host, self._verifier = host, verifier
        self._max_inflight = max_inflight
        self._pending: set[asyncio.Task[bytes]] = set()
        self._accepting = True
        self._pid = os.getpid()
        self._loop: asyncio.AbstractEventLoop | None = None

    def _owner(self) -> None:
        if os.getpid() != self._pid:
            raise RuntimeError("Inherited ingress requires a fresh process")
        loop = asyncio.get_running_loop()
        if self._loop is None:
            self._loop = loop
        elif self._loop is not loop:
            raise RuntimeError("Ingress belongs to another event loop")

    def authenticate(self, credential: str) -> Principal:
        """Verify through the configured authority for transport auth/context.

        This checks no operation or resource. invoke() always re-verifies and
        performs the exact grants itself; transport context cannot bypass it.
        """
        if (
            type(credential) is not str
            or not 24 <= len(credential) <= 4096
            or any(c.isspace() for c in credential)
        ):
            raise ValueError("Invalid credential")
        principal = self._verifier.authenticate(credential)
        w.identifier(principal.principal_id)
        capabilities = principal.capabilities
        if type(capabilities) is not frozenset or any(type(c) is not str for c in capabilities):
            raise ValueError("Invalid principal")
        return principal

    async def invoke(self, credential: str, request: bytes) -> bytes:
        """Credential is out-of-band, never an actor/role in caller JSON."""
        self._owner()
        if not self._accepting:
            return _error("unavailable")
        try:
            principal = self.authenticate(credential)
            principal_id, capabilities = principal.principal_id, principal.capabilities
        except Exception:
            return _error("unauthenticated")
        try:
            decoded = w.Request.decode(request)
        except (ValueError, TypeError, KeyError, UnicodeError, RecursionError):
            return _error("invalid_request")
        capability = w.CAPABILITIES[type(decoded.operation)]
        if capability not in capabilities or capability not in self._grants.get(
            (principal_id, decoded.resource), frozenset()
        ):
            return _error("forbidden")
        # No native lookup, even status, happens before BOTH exact grants.
        resource = self._resources[decoded.resource]
        try:
            self._validate_scope(resource, decoded.operation)
        except (ValueError, TypeError):
            return _error("invalid_request")
        if len(self._pending) >= self._max_inflight:
            return _error("busy")
        task = asyncio.create_task(self._call(resource, decoded.operation))
        self._pending.add(task)
        task.add_done_callback(self._pending.discard)
        # The caller can cancel only its waiter. The real call occupies capacity
        # and keeps Host alive until completion; no credential is passed to it.
        return await asyncio.shield(task)

    @staticmethod
    def _validate_scope(resource: Resource, op: w.Operation) -> None:
        if isinstance(op, w.Admit):
            schemas = dict(resource.inputs)
            for change in op.changes:
                if schemas.get(change.predicate) != tuple(c.kind for c in change.cells):
                    raise ValueError("Input outside configured schema")
        if isinstance(op, w.Restore) and (op.receipt.world, op.receipt.run) != (
            resource.world,
            resource.run,
        ):
            raise ValueError("Receipt outside configured scope")

    async def _call(self, resource: Resource, op: w.Operation) -> bytes:
        try:
            value = await asyncio.to_thread(self._native, resource, op)
            projected = _project(resource, op, value)
            return w.response(
                {
                    "version": 1,
                    "ok": True,
                    "resource": resource.name,
                    "operation": op.name,
                    "value": projected,
                }
            )
        except NativeError as error:
            code = (
                error.code
                if error.code
                in {"resource_limit", "corrupt_data", "invalid_request", "unsupported_format"}
                else "operation_failed"
            )
            return _error(code, dispatched=True)
        except Exception:
            # Neither opaque native text nor post-dispatch encoding failure
            # proves rollback, absence, conflict, or permission to retry.
            return _error("operation_failed", dispatched=True)

    def _native(self, resource: Resource, op: w.Operation) -> Any:
        host, native_id = self._host, resource.native_world
        if isinstance(op, w.Status):
            return host.status(native_id)
        if isinstance(op, w.Start):
            return host.start(native_id)
        if isinstance(op, w.Stop):
            return host.stop(native_id)
        if isinstance(op, w.AdmissionStatus):
            return host.admission_status(native_id, op.generation, op.admission_key)
        binding = resource.binding()
        if isinstance(op, w.Admit):
            return host.admit(
                binding,
                expected_head=op.expected_head,
                generation=op.generation,
                revision=op.revision,
                key=op.admission_key,
                changes=[change.native() for change in op.changes],
            )
        if isinstance(op, w.Publish):
            return host.publish(binding, op.boundary.native(native_id))
        if isinstance(op, w.Reconcile):
            return host.reconcile(
                binding,
                op.boundary.native(native_id),
                tick=op.tick,
                expected_parent=op.expected_parent,
            )
        if isinstance(op, w.Confirm):
            return host.confirm(
                binding,
                op.boundary.native(native_id),
                tick=op.tick,
                expected_parent=op.expected_parent,
            )
        if isinstance(op, w.Restore):
            return host.restore(
                binding, op.receipt.native(), expected_generation=op.expected_generation
            )
        raise TypeError("Unregistered operation")

    def stop_accepting(self) -> None:
        self._owner()
        self._accepting = False

    async def drain(self) -> None:
        """Stop ingress and await real completions; cancellation retains ownership."""
        self.stop_accepting()
        while self._pending:
            # Wait never propagates waiter cancellation into the owned tasks.
            await asyncio.wait(tuple(self._pending))


def _choice(value: Any, choices: str) -> str:
    if type(value) is not str or value not in choices.split():
        raise ValueError("Unexpected native state")
    return value


def _boundary(resource: Resource, raw: Any) -> dict[str, Any]:
    if raw["world_id"] != resource.native_world:
        raise ValueError("Native boundary owner mismatch")
    return {
        "generation": w.unsigned(raw["generation"]),
        "admission_key": w.identifier(raw["admission_key"]),
        "request_sha256": w.digest(raw["request_sha256"]),
    }


def _project(resource: Resource, op: w.Operation, raw: Any) -> dict[str, Any]:
    if isinstance(op, (w.Publish, w.Reconcile)):
        if (raw["world"], raw["run"]) != (resource.world, resource.run):
            raise ValueError("Native receipt owner mismatch")
        if isinstance(op, w.Reconcile) and (raw["tick"], raw["parent"]) != (
            op.tick,
            op.expected_parent,
        ):
            raise ValueError("Native receipt selector mismatch")
        return {
            "receipt": {
                "world": resource.world,
                "run": resource.run,
                "tick": w.unsigned(raw["tick"]),
                "cut_id": w.digest(raw["cut_id"]),
            },
            "parent": w.optional_digest(raw["parent"]),
        }
    if raw["id"] != resource.native_world:
        raise ValueError("Native world mismatch")
    generation = w.unsigned(raw["generation"])
    if isinstance(op, (w.Status, w.Start, w.Stop, w.Restore)):
        return {
            "state": _choice(
                raw["state"], "created starting running stopping stopped failed interrupted"
            ),
            "generation": generation,
            "revision": None if raw["revision"] is None else w.unsigned(raw["revision"]),
            "has_error": raw.get("error") is not None,
        }
    result: dict[str, Any] = {
        "generation": generation,
        "admission_key": w.identifier(raw["admission_key"]),
        "state": _choice(
            raw["state"],
            "not_applied pending durable frozen published uncertain applied_but_unpublished",
        ),
        "publication": _choice(raw["publication"], "not_attempted pending published uncertain"),
        "applied_revision": None
        if raw["applied_revision"] is None
        else w.unsigned(raw["applied_revision"]),
        "has_error": raw.get("error") is not None,
    }
    expected = op.boundary if isinstance(op, w.Confirm) else op
    if (raw["generation"], raw["admission_key"]) != (expected.generation, expected.admission_key):
        raise ValueError("Native admission selector mismatch")
    if raw.get("boundary") is not None:
        result["boundary"] = _boundary(resource, raw["boundary"]["key"])
        if (result["boundary"]["generation"], result["boundary"]["admission_key"]) != (
            generation,
            result["admission_key"],
        ):
            raise ValueError("Native boundary selector mismatch")
        if (
            isinstance(op, w.Confirm)
            and result["boundary"]["request_sha256"] != op.boundary.request_sha256
        ):
            raise ValueError("Native request digest mismatch")
    elif isinstance(op, w.Confirm):
        raise ValueError("Missing native boundary")
    return result
