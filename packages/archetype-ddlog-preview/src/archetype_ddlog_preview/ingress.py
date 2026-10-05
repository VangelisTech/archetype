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
from archetype_ddlog_preview.programs import ProgramReference, native_types


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
    native_world: str | None
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
        for value in (self.name, self.world, self.run):
            w.identifier(value)
        if self.native_world is not None:
            w.identifier(self.native_world)
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
                or any(t not in ("int64", "string", "bool", "float64") for t in types)
            ):
                raise ValueError("Invalid input schema")
        if len(dict(self.inputs)) != len(self.inputs):
            raise ValueError("Duplicate input configuration")

    def binding(self) -> dict[str, Any]:
        if self.native_world is None:
            raise ValueError("Fork resource requires durable origin resolution")
        return {
            "scope": {"native_world": self.native_world, "world": self.world, "run": self.run},
            "components": [c.native() for c in self.components],
        }


@dataclass(frozen=True, slots=True)
class ContextResource:
    """Configured data scope and immutable optional hosted publication source."""

    name: str
    world: str
    run: str
    source_resource: str | None = None

    def __post_init__(self) -> None:
        for value in (self.name, self.world, self.run):
            w.identifier(value)
        if self.source_resource is not None:
            w.identifier(self.source_resource)


@dataclass(frozen=True, slots=True)
class LogicalResource:
    """Configured destination/declarations; native catalog owns its identity."""

    name: str
    world: str
    run: str
    components: tuple[Component, ...]
    inputs: tuple[tuple[str, tuple[str, ...]], ...]

    def __post_init__(self) -> None:
        # Reuse only declaration validation, never legacy fork resolution.
        Resource(self.name, None, self.world, self.run, self.components, self.inputs)

    def destination(self) -> dict[str, str]:
        return {"resource": self.name, "world": self.world, "run": self.run}

    def declarations(self) -> dict[str, Any]:
        return {
            "components": [c.native() for c in self.components],
            "inputs": {name: native_types(types) for name, types in self.inputs},
        }


@dataclass(frozen=True, slots=True)
class ProgramResource:
    """Logical registry resource; no source, current version or live inventory."""

    name: str

    def __post_init__(self) -> None:
        w.identifier(self.name)


type ConfiguredResource = Resource | LogicalResource | ContextResource | ProgramResource


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
        resources: tuple[ConfiguredResource, ...],
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
            [
                r.native_world
                for r in resources
                if isinstance(r, Resource) and r.native_world is not None
            ],
            [
                ("context" if isinstance(r, ContextResource) else "execution", r.world, r.run)
                for r in resources
                if not isinstance(r, ProgramResource)
            ],
        ):
            if len(set(keys)) != len(keys):
                raise ValueError("Conflicting resource binding")
        configured = {r.name: r for r in resources}
        for resource in resources:
            if not isinstance(resource, ContextResource):
                continue
            source = configured.get(resource.source_resource)
            if resource.source_resource is not None and (
                not isinstance(source, (Resource, LogicalResource))
                or (source.world, source.run) != (resource.world, resource.run)
            ):
                raise ValueError("Context requires its configured execution source")
            if resource.source_resource is None and any(
                isinstance(other, (Resource, LogicalResource))
                and (other.world, other.run) == (resource.world, resource.run)
                for other in resources
            ):
                raise ValueError("Shared execution scope requires a hosted context source")
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
        for name, capability in w.requirements(decoded):
            if capability not in capabilities or capability not in self._grants.get(
                (principal_id, name), frozenset()
            ):
                return _error("forbidden")
        # Every grant, including every protected program reference, precedes
        # configuration, registry, native or storage resolution.
        try:
            resource = self._resources[decoded.resource]
            refs = ()
            if isinstance(decoded.operation, w.Create):
                refs = (decoded.operation.program,)
            elif isinstance(decoded.operation, w.ProgramCompose):
                refs = decoded.operation.composition.references()
            if any(not isinstance(self._resources[ref.resource], ProgramResource) for ref in refs):
                raise ValueError("Protected program resource required")
            if isinstance(decoded.operation, w.Fork):
                source = self._resources[decoded.operation.source_resource]
                if not isinstance(resource, (Resource, LogicalResource)) or not isinstance(
                    source, (Resource, LogicalResource)
                ):
                    raise ValueError("Fork requires execution resources")
                if source.name == resource.name or (
                    isinstance(resource, Resource) and resource.native_world is not None
                ):
                    raise ValueError("Fork requires a distinct configured destination")
                if source.components != resource.components or source.inputs != resource.inputs:
                    raise ValueError("Fork destination configuration must match source")
            if (
                isinstance(decoded.operation, w.PublishContext)
                and decoded.operation.source_resource is not None
            ):
                source = self._resources[decoded.operation.source_resource]
                if (
                    not isinstance(source, (Resource, LogicalResource))
                    or not isinstance(resource, ContextResource)
                    or (source.world, source.run)
                    != (
                        resource.world,
                        resource.run,
                    )
                ):
                    raise ValueError("Hosted context must match the granted source scope")
            self._validate_scope(resource, decoded.operation)
        except (ValueError, TypeError, KeyError):
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
    def _validate_scope(resource: ConfiguredResource, op: w.Operation) -> None:
        if isinstance(op, (w.ProgramCreate, w.ProgramCompose, w.ProgramResolve, w.ProgramDescribe)):
            if not isinstance(resource, ProgramResource):
                raise ValueError("Program resource required")
            return
        if isinstance(op, (w.Create, w.Resolve)) and not isinstance(resource, LogicalResource):
            raise ValueError("Logical execution resource required")
        if isinstance(op, (w.PublishContext, w.ReadContext, w.ContextArtifacts)):
            if not isinstance(resource, ContextResource):
                raise ValueError("Context resource required")
            if isinstance(op, w.PublishContext) and op.source_resource != resource.source_resource:
                raise ValueError("Context publication must use its configured origin")
            return
        if not isinstance(resource, (Resource, LogicalResource)):
            raise ValueError("Execution resource required")
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

    async def _call(self, resource: ConfiguredResource, op: w.Operation) -> bytes:
        try:
            resource, value = await asyncio.to_thread(self._dispatch, resource, op)
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

    def _logical(self, op: str, **arguments: Any) -> Any:
        return self._host.request("logical", request={"op": op, **arguments})

    def _resolve(self, resource: Resource | LogicalResource) -> Resource:
        if isinstance(resource, LogicalResource):
            value = self._logical("world_resolve", destination=resource.destination())
            return _logical_binding(resource, value)
        if resource.native_world is not None:
            return resource
        binding = self._host.fork_binding(
            resource.world, resource.run, [c.native() for c in resource.components]
        )
        resolved = Resource.from_binding(resource.name, binding, inputs=dict(resource.inputs))
        if (resolved.world, resolved.run, resolved.components) != (
            resource.world,
            resource.run,
            resource.components,
        ):
            raise ValueError("Resolved fork resource mismatch")
        return resolved

    def _dispatch(
        self, resource: ConfiguredResource, op: w.Operation
    ) -> tuple[ConfiguredResource, Any]:
        if isinstance(resource, ProgramResource):
            if isinstance(op, (w.ProgramCreate, w.ProgramCompose)):
                if isinstance(op, w.ProgramCompose):
                    for ref in op.composition.references():
                        retained = self._logical("program_resolve", resource=ref.resource)
                        if (
                            retained["resource"] != ref.resource
                            or retained["processor"] != ref.pin()
                            or retained["phase"] != "published"
                        ):
                            raise ValueError("Reference differs from protected logical program")
                    definition = op.composition.native()
                else:
                    definition = op.definition.native()
                return resource, self._logical(
                    "program_publish",
                    request={
                        "resource": resource.name,
                        "request_key": op.request_key,
                        "description": op.description,
                        "definition": definition,
                        "git_provenance": None,
                        "lowering_version": 2,
                    },
                )
            if isinstance(op, (w.ProgramResolve, w.ProgramDescribe)):
                return resource, self._logical(op.name, resource=resource.name)
            raise ValueError("Unknown program operation")
        if isinstance(resource, ContextResource):
            if isinstance(op, w.PublishContext):
                if op.source_resource is None:
                    return resource, self._host.publish_collection(resource.world, resource.run)
                source = self._resources[op.source_resource]
                if not isinstance(source, (Resource, LogicalResource)):
                    raise ValueError("Execution source required")
                return resource, self._host.publish_hosted_context(self._resolve(source).binding())
            if isinstance(op, w.ReadContext):
                return resource, self._host.context(resource.world, resource.run)
            if isinstance(op, w.ContextArtifacts):
                return resource, self._host.request(
                    "read_context_artifacts",
                    context={
                        "world": resource.world,
                        "run": resource.run,
                        "context_id": op.context_id,
                    },
                    selection=op.selection(),
                    offset=op.offset,
                    limit=op.limit,
                )
            raise ValueError("Unknown context operation")
        if isinstance(op, (w.Create, w.Resolve)):
            if not isinstance(resource, LogicalResource):
                raise ValueError("Logical resource required")
            if isinstance(op, w.Create):
                result = self._logical(
                    "world_create",
                    destination=resource.destination(),
                    request_key=op.request_key,
                    label=op.label,
                    program=op.program.native(),
                    declarations=resource.declarations(),
                )
            else:
                result = self._logical("world_resolve", destination=resource.destination())
            _logical_binding(resource, result)
            return resource, result
        if isinstance(op, w.Fork):
            configured_source = self._resources[op.source_resource]
            if not isinstance(configured_source, (Resource, LogicalResource)):
                raise ValueError("Execution source required")
            source = self._resolve(configured_source)
            if isinstance(resource, LogicalResource):
                result = self._logical(
                    "world_fork",
                    source_binding=source.binding(),
                    receipt=op.receipt.native(),
                    destination=resource.destination(),
                    label=resource.name,
                    request_key=op.request_key,
                    inputs=resource.declarations()["inputs"],
                    expected_generation=op.expected_generation,
                )
                _logical_binding(resource, result)
                return resource, result
            return resource, self._host.fork(
                source.binding(),
                op.receipt.native(),
                world=resource.world,
                run=resource.run,
                label=resource.name,
                request_key=op.request_key,
                expected_generation=op.expected_generation,
            )
        resolved = self._resolve(resource)
        return resolved, self._native(resolved, op)

    def _native(self, resource: Resource, op: w.Operation) -> Any:
        host, native_id = self._host, resource.native_world
        if native_id is None:
            raise ValueError("Unresolved fork resource")
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


def _logical_binding(resource: LogicalResource, raw: Any) -> Resource:
    creation = raw["creation"]
    reservation = creation["reservation"]
    resolved = Resource.from_binding(resource.name, raw["binding"], inputs=dict(resource.inputs))
    if (
        reservation["schema_version"] != 2
        or reservation["destination"] != resource.destination()
        or reservation["world_id"] != resolved.native_world
        or raw["status"]["id"] != resolved.native_world
        or (resolved.world, resolved.run, resolved.components)
        != (resource.world, resource.run, resource.components)
        or raw["inputs"] != resource.declarations()["inputs"]
        or creation["binding"]["declarations"] != resource.declarations()
        or creation["binding"]["program_resource"] != raw["program_resource"]
    ):
        raise ValueError("Logical binding differs from configured declarations")
    if type(creation["context_confirmed"]) is not bool:
        raise ValueError("Invalid context readiness")
    context = raw["context"]
    if context is not None and (context["world"], context["run"]) != (resource.world, resource.run):
        raise ValueError("Logical context scope mismatch")
    if creation["context_id"] is not None and (
        context is None or context["context_id"] != creation["context_id"]
    ):
        raise ValueError("Logical context acknowledgment mismatch")
    if creation["context_confirmed"] and creation["context_id"] is None:
        raise ValueError("Confirmed creation requires context")
    fork = creation["fork"]
    if reservation["kind"] == "fork":
        native_fork = raw["status"]["external_publication"]["fork"]
        if (
            fork is None
            or native_fork["reservation"] != fork
            or type(native_fork["ready"]) is not bool
            or (
                fork["child_world_id"] != resolved.native_world
                or fork["destination"] != {"world": resource.world, "run": resource.run}
                or fork["request_key"] != reservation["request_key"]
            )
        ):
            raise ValueError("Logical fork reservation mismatch")
        origin = raw["origin"]
        if origin is not None and origin["reservation"] != fork:
            raise ValueError("Logical fork origin mismatch")
        if native_fork["ready"] and origin is None:
            raise ValueError("Ready fork requires retained origin")
    elif fork is not None or raw["origin"] is not None:
        raise ValueError("Fresh creation cannot contain fork lineage")
    return resolved


def _program_projection(resource: ProgramResource, raw: Any) -> dict[str, Any]:
    if raw["resource"] != resource.name:
        raise ValueError("Native program resource mismatch")
    ref = ProgramReference(resource.name, **raw["processor"])
    return {
        "program": {"resource": ref.resource, **ref.pin()},
        "request_key": w.identifier(raw["request_key"]),
        "request_sha256": w.digest(raw["request_sha256"]),
        "phase": _choice(raw["phase"], "prepared published"),
    }


def _project(resource: ConfiguredResource, op: w.Operation, raw: Any) -> dict[str, Any]:
    if isinstance(resource, ProgramResource):
        selected = raw["program"] if isinstance(op, w.ProgramDescribe) else raw
        result = _program_projection(resource, selected)
        if (
            isinstance(op, (w.ProgramCreate, w.ProgramCompose))
            and selected["request_key"] != op.request_key
        ):
            raise ValueError("Program request key mismatch")
        if isinstance(op, w.ProgramDescribe):
            relations = raw["relations"]
            if type(relations) is not list or len(relations) > 128:
                raise ValueError("Unbounded program description")
            result["kind"] = _choice(raw["kind"], "program composition")
            result["relations"] = []
            for relation in relations:
                types = relation["fields"]
                if (
                    type(relation["input"]) is not bool
                    or type(types) is not list
                    or not 1 <= len(types) <= 64
                    or any(t not in ("int", "string", "bool", "double") for t in types)
                ):
                    raise ValueError("Invalid public relation")
                result["relations"].append(
                    {
                        "name": w.identifier(relation["name"]),
                        "input": relation["input"],
                        "fields": [{"int": "int64", "double": "float64"}.get(t, t) for t in types],
                    }
                )
        return result
    if isinstance(resource, LogicalResource):
        resolved = _logical_binding(resource, raw)
        creation, status = raw["creation"], raw["status"]
        reservation = creation["reservation"]
        kind = _choice(reservation["kind"], "fresh fork")
        if isinstance(op, (w.Create, w.Fork)) and reservation["request_key"] != op.request_key:
            raise ValueError("Creation request key mismatch")
        processor = creation["definition"]["processor"]
        ProgramReference("inherited", **processor)
        if isinstance(op, w.Create) and (
            kind != "fresh"
            or raw["program_resource"] != op.program.resource
            or processor != op.program.pin()
        ):
            raise ValueError("Fresh creation program mismatch")
        result = {
            "destination": resource.destination(),
            "kind": kind,
            "request_key": w.identifier(reservation["request_key"]),
            "request_sha256": w.digest(reservation["request_sha256"]),
            "program": {
                "resource": None
                if raw["program_resource"] is None
                else w.identifier(raw["program_resource"]),
                **processor,
            },
            "context_id": w.optional_digest(creation["context_id"]),
            "context_ready": creation["context_confirmed"],
            **_project(resolved, w.Status(), status),
        }
        if isinstance(op, w.Fork):
            if kind != "fork" or creation["fork"] is None:
                raise ValueError("Fork creation lineage missing")
            if raw["origin"] is None or raw["origin"]["source"] != op.receipt.native():
                raise ValueError("Fork creation source mismatch")
        if raw["origin"] is not None:
            origin = raw["origin"]
            receipt = origin["source"]
            result["source"] = {
                "world": w.identifier(receipt["world"]),
                "run": w.identifier(receipt["run"]),
                "tick": w.unsigned(receipt["tick"]),
                "cut_id": w.digest(receipt["cut_id"]),
            }
            result["lineage_sha256"] = w.digest(origin["lineage_sha256"])
        return result
    if isinstance(resource, ContextResource):
        if isinstance(op, (w.PublishContext, w.ReadContext)):
            if raw["version"] != 1 or (raw["world"], raw["run"]) != (resource.world, resource.run):
                raise ValueError("Published context scope mismatch")
            origin = _choice(raw["origin"]["kind"], "hosted artifact_collection")
            if isinstance(op, w.PublishContext) and origin != (
                "artifact_collection" if op.source_resource is None else "hosted"
            ):
                raise ValueError("Published context origin mismatch")
            return {
                "context_id": w.digest(raw["context_id"]),
                "world": resource.world,
                "run": resource.run,
                "origin": origin,
            }
        if isinstance(op, w.ContextArtifacts):
            if type(raw["items"]) is not list or len(raw["items"]) > op.limit:
                raise ValueError("Invalid artifact result count")
            items = []
            for item in raw["items"]:
                receipt = item["receipt"]
                target = receipt["target"]
                if receipt["version"] != 1 or target["context"] != {
                    "world": resource.world,
                    "run": resource.run,
                    "context_id": op.context_id,
                }:
                    raise ValueError("Artifact context mismatch")
                exact = target["exact_cut"]
                if not op.all and exact != op.selection()["exact_cut"]:
                    raise ValueError("Artifact cut attribution mismatch")
                public_cut = (
                    None
                    if exact is None
                    else {"tick": w.unsigned(exact["tick"]), "cut_id": w.digest(exact["cut_id"])}
                )
                items.append(
                    {
                        "artifact_id": w.identifier(receipt["artifact_id"]),
                        "context_id": op.context_id,
                        "exact_cut": public_cut,
                        "sha256": w.digest(item["sha256"]),
                        "media_type": w.string_cell(item["media_type"]),
                        "size_bytes": w.unsigned(item["size_bytes"]),
                    }
                )
            return {
                "items": items,
                "total": w.unsigned(raw["total"]),
                "next_offset": None
                if raw["next_offset"] is None
                else w.unsigned(raw["next_offset"]),
            }
        raise ValueError("Unknown context projection")
    if isinstance(op, w.Fork):
        destination = {"world": resource.world, "run": resource.run}
        origin, status = raw["origin"], raw["status"]
        reservation = origin["reservation"]
        if (
            raw["destination"] != destination
            or raw["source"] != op.receipt.native()
            or raw["request_key"] != op.request_key
            or reservation["request_key"] != op.request_key
            or reservation["destination"] != destination
            or status["id"] != reservation["child_world_id"]
            or status["external_publication"]["fork"]["reservation"] != reservation
        ):
            raise ValueError("Native fork result identity mismatch")
        ready = status["external_publication"]["fork"]["ready"]
        if type(ready) is not bool:
            raise ValueError("Invalid fork readiness")
        return {
            "request_key": op.request_key,
            "destination": destination,
            "source": {**op.receipt.native(), "tick": w.unsigned(op.receipt.tick)},
            "lineage_sha256": w.digest(origin["lineage_sha256"]),
            "lineage_ready": ready,
            "state": _choice(
                status["state"], "created starting running stopping stopped failed interrupted"
            ),
            "generation": w.unsigned(status["generation"]),
        }
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
        result = {
            "state": _choice(
                raw["state"], "created starting running stopping stopped failed interrupted"
            ),
            "generation": generation,
            "revision": None if raw["revision"] is None else w.unsigned(raw["revision"]),
            "has_error": raw.get("error") is not None,
        }
        fork = raw.get("external_publication", {}).get("fork")
        if fork is not None:
            if type(fork["ready"]) is not bool:
                raise ValueError("Invalid fork readiness")
            result["lineage_ready"] = fork["ready"]
        return result
    if not isinstance(op, (w.AdmissionStatus, w.Admit, w.Confirm)):
        raise ValueError("Expected admission operation")
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
