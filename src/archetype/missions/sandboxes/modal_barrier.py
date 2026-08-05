# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Persistent Modal provider barriers for one logical Mission author operation."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

from archetype.missions.sandboxes.contracts import SandboxSpec
from archetype.missions.sandboxes.modal import (
    MODAL_ACTIVITY_PROTOCOL_EPOCH,
    ModalSandboxOperationCapability,
    ModalSandboxOperationIdentity,
    ModalSandboxSession,
)

_MAX_NAMESPACE_NAME_LENGTH = 63
_MAX_MARKER_NAME_LENGTH = 63
_MAX_OBJECT_ID_LENGTH = 256
_MAX_REASON_LENGTH = 512
_MARKER_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,62}")


def _require_namespace_name(value: str, *, label: str) -> str:
    if not _MARKER_NAME.fullmatch(value) or len(value) > _MAX_NAMESPACE_NAME_LENGTH:
        raise ValueError(f"Modal barrier {label} name is invalid")
    return value


def _canonical_digest(value: dict[str, str | int]) -> str:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode()
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


def _operation_marker_name(identity: ModalSandboxOperationIdentity) -> str:
    digest = bytes.fromhex(identity.digest.removeprefix("sha256:"))
    encoded = base64.urlsafe_b64encode(digest).decode().rstrip("=")
    return f"op-v1-{encoded}"


def _run_marker_name(guard_digest: str) -> str:
    digest = hashlib.sha256(guard_digest.encode()).digest()
    encoded = base64.urlsafe_b64encode(digest).decode().rstrip("=")
    return f"run-v1-{encoded}"


@dataclass(frozen=True, slots=True)
class ModalPersistentDictMarker:
    """Bounded identity of one persistent named Modal Dict object."""

    workspace_name: str
    environment_name: str
    app_name: str
    protocol_epoch: int
    name: str
    object_id: str

    def __post_init__(self) -> None:
        _require_namespace_name(self.workspace_name, label="workspace")
        _require_namespace_name(self.environment_name, label="environment")
        _require_namespace_name(self.app_name, label="app")
        if self.protocol_epoch != MODAL_ACTIVITY_PROTOCOL_EPOCH:
            raise ValueError("Modal persistent marker protocol epoch is not barrier-aware")
        if not _MARKER_NAME.fullmatch(self.name) or len(self.name) > _MAX_MARKER_NAME_LENGTH:
            raise ValueError("Modal persistent marker name is invalid")
        if (
            not self.object_id.strip()
            or self.object_id != self.object_id.strip()
            or "\x00" in self.object_id
            or len(self.object_id) > _MAX_OBJECT_ID_LENGTH
        ):
            raise ValueError("Modal persistent marker object identity is invalid")

    @property
    def reference(self) -> str:
        """Return a bounded provider locator suitable for an Activity catalog."""

        return (
            "modal-dict://"
            + quote(self.workspace_name, safe="")
            + "/"
            + quote(self.environment_name, safe="")
            + "/"
            + self.name
            + "#"
            + quote(self.object_id, safe="")
        )

    @property
    def digest(self) -> str:
        """Return the immutable digest of this exact provider object."""

        return _canonical_digest(
            {
                "schema_version": 2,
                "provider": "modal",
                "workspace_name": self.workspace_name,
                "environment_name": self.environment_name,
                "app_name": self.app_name,
                "protocol_epoch": self.protocol_epoch,
                "name": self.name,
                "object_id": self.object_id,
            }
        )


@dataclass(frozen=True, slots=True)
class ModalProviderOperationGuard:
    """Persistent one-winner barrier bound to one logical provider operation."""

    identity: ModalSandboxOperationIdentity
    marker: ModalPersistentDictMarker

    def __post_init__(self) -> None:
        if self.marker.name != _operation_marker_name(self.identity):
            raise ValueError("Modal operation guard marker does not match its operation")
        if self.marker.workspace_name != self.identity.workspace_name:
            raise ValueError("Modal operation guard belongs to another workspace")
        if self.marker.environment_name != self.identity.environment_name:
            raise ValueError("Modal operation guard belongs to another environment")
        if self.marker.app_name != self.identity.app_name:
            raise ValueError("Modal operation guard belongs to another app")
        if self.marker.protocol_epoch != self.identity.protocol_epoch:
            raise ValueError("Modal operation guard belongs to another protocol epoch")

    @property
    def reference(self) -> str:
        """Return the bounded provider retry-guard reference."""

        return self.marker.reference

    @property
    def digest(self) -> str:
        """Bind logical operation identity to the exact persistent marker."""

        return _canonical_digest(
            {
                "schema_version": 2,
                "kind": "modal_operation_guard",
                "operation_digest": self.identity.digest,
                "marker_digest": self.marker.digest,
            }
        )


@dataclass(frozen=True, slots=True)
class ModalProviderRunPermit:
    """Evidence created inside the acknowledged run-marker/start transaction.

    The marker is permanent and must never be deleted. Losing this value after
    marker creation leaves the operation Unknown forever; another claimant may
    not reconstruct execution authority from provider lookup alone.

    This frozen value is evidence, not transferable authority. Public provider
    execution accepts no run-permit argument. Only
    ``ModalProviderStartBarrier`` may interpret the instance it receives
    directly from its own acknowledged marker create.
    """

    guard: ModalProviderOperationGuard
    marker: ModalPersistentDictMarker

    def __post_init__(self) -> None:
        if self.marker.workspace_name != self.guard.marker.workspace_name:
            raise ValueError("Modal run marker belongs to another workspace")
        if self.marker.environment_name != self.guard.marker.environment_name:
            raise ValueError("Modal run marker belongs to another environment")
        if self.marker.app_name != self.guard.marker.app_name:
            raise ValueError("Modal run marker belongs to another app")
        if self.marker.protocol_epoch != self.guard.marker.protocol_epoch:
            raise ValueError("Modal run marker belongs to another protocol epoch")
        if self.marker.name != _run_marker_name(self.guard.digest):
            raise ValueError("Modal run marker does not match its operation guard")

    @property
    def reference(self) -> str:
        return self.marker.reference

    @property
    def digest(self) -> str:
        return _canonical_digest(
            {
                "schema_version": 2,
                "kind": "modal_run_permit",
                "guard_digest": self.guard.digest,
                "marker_digest": self.marker.digest,
            }
        )


@dataclass(frozen=True, slots=True)
class ModalProviderMarkerExists:
    """Another claimant already owns this permanent provider marker."""

    identity: ModalSandboxOperationIdentity
    phase: str
    marker_name: str

    def __post_init__(self) -> None:
        if self.phase not in {"operation", "run"}:
            raise ValueError("Modal provider marker phase is invalid")
        if not _MARKER_NAME.fullmatch(self.marker_name):
            raise ValueError("Modal provider marker name is invalid")


@dataclass(frozen=True, slots=True)
class ModalProviderOperationMissing:
    """Read-only evidence that the permanent operation marker was not found.

    This value is deliberately not a guard or permit. It cannot be supplied to
    the operation capability and does not authorize execution. A later retry
    must still win ``start_retry``'s atomic marker-create/start transaction.
    """

    identity: ModalSandboxOperationIdentity
    marker_name: str

    def __post_init__(self) -> None:
        if self.marker_name != _operation_marker_name(self.identity):
            raise ValueError("missing Modal marker does not match its operation")


@dataclass(frozen=True, slots=True)
class ModalProviderBarrierUnknown:
    """Provider evidence cannot grant guard or run authority."""

    identity: ModalSandboxOperationIdentity
    phase: str
    reason: str

    def __post_init__(self) -> None:
        if self.phase not in {"operation", "run"}:
            raise ValueError("Modal provider barrier phase is invalid")
        if not self.reason.strip() or len(self.reason) > _MAX_REASON_LENGTH:
            raise ValueError("Modal provider barrier unknown reason is invalid")


type ModalOperationGuardAcquisition = (
    ModalProviderOperationGuard | ModalProviderMarkerExists | ModalProviderBarrierUnknown
)
type ModalRunPermitAcquisition = (
    ModalProviderRunPermit | ModalProviderMarkerExists | ModalProviderBarrierUnknown
)
type ModalOperationMarkerObservation = (
    ModalProviderMarkerExists | ModalProviderOperationMissing | ModalProviderBarrierUnknown
)


@dataclass(frozen=True, slots=True)
class ModalProviderStarted:
    """Exact acknowledged run-marker evidence paired with its live session."""

    permit: ModalProviderRunPermit
    session: ModalSandboxSession

    def __post_init__(self) -> None:
        if self.session.operation_identity != self.permit.guard.identity:
            raise ValueError("Modal provider session does not match its run marker")

    @property
    def identity(self) -> ModalSandboxOperationIdentity:
        return self.permit.guard.identity

    @property
    def run_marker_reference(self) -> str:
        return self.permit.reference

    @property
    def run_marker_digest(self) -> str:
        return self.permit.digest


type ModalProviderStartOutcome = (
    ModalProviderStarted | ModalProviderMarkerExists | ModalProviderBarrierUnknown
)


@dataclass(frozen=True, slots=True)
class _MarkerLookupFailure:
    reason: str


_MARKER_MISSING = object()


class ModalProviderStartBarrier:
    """Atomically select one permanent operation owner and one run owner.

    Both tiers use named Modal Dict object creation with
    ``allow_existing=False``. Dict object names persist until explicitly
    deleted; this capability deliberately exposes no deletion operation.
    Its configured Modal workspace credentials, Environment, App, protocol
    epoch, and both marker objects are durable provider identity and must never
    be deleted or silently replaced. Only operations admitted into
    ``MODAL_ACTIVITY_PROTOCOL_EPOCH`` from birth may use this barrier.
    Pre-barrier and in-flight legacy operations remain Unknown even when no
    marker exists.

    Safety is fail-closed. An ambiguous create, a winner lost before effect,
    or a winner lost between tiers remains Unknown forever. The public start
    path couples acknowledged operation-marker creation, run-marker creation,
    and exactly one provider start in this same coroutine; it never releases
    transferable execution authority. This capability provides no lease,
    handoff, takeover, replay, or liveness promise.
    """

    def __init__(
        self,
        *,
        workspace_name: str,
        environment_name: str,
        app_name: str,
        protocol_epoch: int,
    ) -> None:
        self._workspace_name = _require_namespace_name(
            workspace_name,
            label="workspace",
        )
        self._environment_name = _require_namespace_name(
            environment_name,
            label="environment",
        )
        self._app_name = _require_namespace_name(app_name, label="app")
        if protocol_epoch != MODAL_ACTIVITY_PROTOCOL_EPOCH:
            raise ValueError(
                "Modal provider barrier requires the barrier-aware protocol epoch "
                f"{MODAL_ACTIVITY_PROTOCOL_EPOCH}"
            )
        self._protocol_epoch = protocol_epoch

    @property
    def workspace_name(self) -> str:
        return self._workspace_name

    @property
    def environment_name(self) -> str:
        return self._environment_name

    @property
    def app_name(self) -> str:
        return self._app_name

    @property
    def protocol_epoch(self) -> int:
        return self._protocol_epoch

    def operation_marker_name(self, identity: ModalSandboxOperationIdentity) -> str:
        """Derive the permanent first-tier marker name without provider I/O."""

        failure = self._identity_failure(identity)
        if failure is not None:
            raise ValueError(failure)
        return _operation_marker_name(identity)

    async def observe_operation_marker(
        self,
        *,
        identity: ModalSandboxOperationIdentity,
    ) -> ModalOperationMarkerObservation:
        """Observe marker presence without constructing execution authority.

        Absence can route an Activity through ``start_retry`` after it obtains
        a fresh workflow fence. The returned value itself never bypasses that
        method's atomic provider lookup, marker creation, and immediate start.
        """

        failure = self._identity_failure(identity)
        if failure is not None:
            return ModalProviderBarrierUnknown(identity, "operation", failure)
        marker_name = _operation_marker_name(identity)
        observed = await self._lookup_marker(marker_name, phase="operation")
        if isinstance(observed, ModalPersistentDictMarker):
            return ModalProviderMarkerExists(identity, "operation", marker_name)
        if isinstance(observed, _MarkerLookupFailure):
            return ModalProviderBarrierUnknown(identity, "operation", observed.reason)
        if observed is _MARKER_MISSING:
            return ModalProviderOperationMissing(identity, marker_name)
        return ModalProviderBarrierUnknown(
            identity,
            "operation",
            "operation marker lookup returned invalid evidence",
        )

    @staticmethod
    def run_marker_name(guard: ModalProviderOperationGuard) -> str:
        """Derive the permanent second-tier marker name without provider I/O."""

        return _run_marker_name(guard.digest)

    async def _acquire_initial(
        self,
        *,
        identity: ModalSandboxOperationIdentity,
    ) -> ModalOperationGuardAcquisition:
        """Atomically acquire the permanent marker before an initial effect."""

        failure = self._identity_failure(identity)
        if failure is not None:
            return ModalProviderBarrierUnknown(identity, "operation", failure)
        return await self._create_operation_guard(identity)

    async def _acquire_retry_guard(
        self,
        *,
        identity: ModalSandboxOperationIdentity,
    ) -> ModalOperationGuardAcquisition:
        """Acquire only for a barrier-born operation with no provider owner."""

        failure = self._identity_failure(identity)
        if failure is not None:
            return ModalProviderBarrierUnknown(identity, "operation", failure)
        marker_name = _operation_marker_name(identity)
        observed = await self._lookup_marker(marker_name, phase="operation")
        if isinstance(observed, ModalPersistentDictMarker):
            return ModalProviderMarkerExists(identity, "operation", marker_name)
        if isinstance(observed, _MarkerLookupFailure):
            return ModalProviderBarrierUnknown(identity, "operation", observed.reason)
        return await self._create_operation_guard(identity)

    async def start_initial(
        self,
        *,
        identity: ModalSandboxOperationIdentity,
        capability: ModalSandboxOperationCapability,
        spec: SandboxSpec,
    ) -> ModalProviderStartOutcome:
        """Acquire both permanent markers and immediately start one pair.

        No operation guard or run permit crosses this public boundary.
        Cancellation, local failure, or an ambiguous provider response after
        either acknowledged marker permanently sacrifices replay.
        """

        invalid = self._validate_start_request(
            identity=identity,
            capability=capability,
            spec=spec,
        )
        if invalid is not None:
            return invalid
        guard = await self._acquire_initial(identity=identity)
        if not isinstance(guard, ModalProviderOperationGuard):
            return guard
        return await self._start_after_guard(
            guard=guard,
            capability=capability,
            spec=spec,
        )

    async def start_retry(
        self,
        *,
        identity: ModalSandboxOperationIdentity,
        capability: ModalSandboxOperationCapability,
        spec: SandboxSpec,
    ) -> ModalProviderStartOutcome:
        """Start only when retry atomically creates the still-missing guard.

        Exact persistent operation-marker existence never reconstructs
        authority. A retry claimant must be the acknowledged creator returned
        in this coroutine, after a provider lookup established safe absence.
        """

        invalid = self._validate_start_request(
            identity=identity,
            capability=capability,
            spec=spec,
        )
        if invalid is not None:
            return invalid
        guard = await self._acquire_retry_guard(identity=identity)
        if not isinstance(guard, ModalProviderOperationGuard):
            return guard
        return await self._start_after_guard(
            guard=guard,
            capability=capability,
            spec=spec,
        )

    async def _start_after_guard(
        self,
        *,
        guard: ModalProviderOperationGuard,
        capability: ModalSandboxOperationCapability,
        spec: SandboxSpec,
    ) -> ModalProviderStartOutcome:
        permit = await self._acquire_run(guard=guard)
        if not isinstance(permit, ModalProviderRunPermit):
            return permit
        session = await capability._start_after_provider_barrier(
            identity=guard.identity,
            spec=spec,
        )
        return ModalProviderStarted(permit, session)

    def _validate_start_request(
        self,
        *,
        identity: ModalSandboxOperationIdentity,
        capability: ModalSandboxOperationCapability,
        spec: SandboxSpec,
    ) -> ModalProviderBarrierUnknown | None:
        failure = self._identity_failure(identity)
        if failure is not None:
            return ModalProviderBarrierUnknown(identity, "operation", failure)
        capability_identity = capability.identity(identity.operation_id)
        if capability_identity != identity:
            return ModalProviderBarrierUnknown(
                identity,
                "operation",
                "operation capability belongs to another Modal provider namespace",
            )
        capability._validate_spec(spec)
        return None

    async def _acquire_run(
        self,
        *,
        guard: ModalProviderOperationGuard,
    ) -> ModalRunPermitAcquisition:
        """Verify the exact guard object, then atomically acquire run authority."""

        identity = guard.identity
        failure = self._identity_failure(identity)
        if failure is not None:
            return ModalProviderBarrierUnknown(identity, "run", failure)
        if guard.marker.workspace_name != self._workspace_name:
            return ModalProviderBarrierUnknown(
                identity,
                "run",
                "operation guard belongs to another Modal workspace",
            )
        if guard.marker.environment_name != self._environment_name:
            return ModalProviderBarrierUnknown(
                identity,
                "run",
                "operation guard belongs to another Modal environment",
            )
        if guard.marker.app_name != self._app_name:
            return ModalProviderBarrierUnknown(
                identity,
                "run",
                "operation guard belongs to another Modal app",
            )
        if guard.marker.protocol_epoch != self._protocol_epoch:
            return ModalProviderBarrierUnknown(
                identity,
                "run",
                "operation guard belongs to another Modal protocol epoch",
            )
        expected_guard_name = _operation_marker_name(identity)
        if guard.marker.name != expected_guard_name:
            return ModalProviderBarrierUnknown(
                identity,
                "run",
                "operation guard marker name does not match",
            )
        observed = await self._lookup_marker(expected_guard_name, phase="run")
        if observed is _MARKER_MISSING:
            return ModalProviderBarrierUnknown(
                identity,
                "run",
                "persistent operation guard is missing",
            )
        if isinstance(observed, _MarkerLookupFailure):
            return ModalProviderBarrierUnknown(identity, "run", observed.reason)
        if not isinstance(observed, ModalPersistentDictMarker):
            return ModalProviderBarrierUnknown(
                identity,
                "run",
                "persistent operation guard lookup returned invalid evidence",
            )
        if observed.object_id != guard.marker.object_id:
            return ModalProviderBarrierUnknown(
                identity,
                "run",
                "persistent operation guard object identity changed",
            )

        marker_name = _run_marker_name(guard.digest)
        created = await self._create_marker(
            identity=identity,
            phase="run",
            marker_name=marker_name,
        )
        if isinstance(created, ModalPersistentDictMarker):
            return ModalProviderRunPermit(guard, created)
        return created

    async def _create_operation_guard(
        self,
        identity: ModalSandboxOperationIdentity,
    ) -> ModalOperationGuardAcquisition:
        marker_name = _operation_marker_name(identity)
        created = await self._create_marker(
            identity=identity,
            phase="operation",
            marker_name=marker_name,
        )
        if isinstance(created, ModalPersistentDictMarker):
            return ModalProviderOperationGuard(identity, created)
        return created

    async def _create_marker(
        self,
        *,
        identity: ModalSandboxOperationIdentity,
        phase: str,
        marker_name: str,
    ) -> ModalPersistentDictMarker | ModalProviderMarkerExists | ModalProviderBarrierUnknown:
        modal = self._load_modal()
        client = await self._verified_client(modal, phase=phase)
        if isinstance(client, _MarkerLookupFailure):
            return ModalProviderBarrierUnknown(identity, phase, client.reason)
        try:
            await modal.Dict.objects.create.aio(
                marker_name,
                allow_existing=False,
                environment_name=self._environment_name,
                client=client,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            already_exists = getattr(
                getattr(modal, "exception", None),
                "AlreadyExistsError",
                None,
            )
            if isinstance(already_exists, type) and isinstance(exc, already_exists):
                return ModalProviderMarkerExists(identity, phase, marker_name)
            return ModalProviderBarrierUnknown(
                identity,
                phase,
                f"persistent marker create failed ({type(exc).__name__[:128]})",
            )

        observed = await self._lookup_marker(
            marker_name,
            phase=phase,
            verify_workspace=False,
            client=client,
        )
        if isinstance(observed, ModalPersistentDictMarker):
            return observed
        if isinstance(observed, _MarkerLookupFailure):
            reason = observed.reason
        else:
            reason = "acknowledged persistent marker is not visible"
        return ModalProviderBarrierUnknown(identity, phase, reason)

    async def _lookup_marker(
        self,
        marker_name: str,
        *,
        phase: str,
        verify_workspace: bool = True,
        client: Any | None = None,
    ) -> ModalPersistentDictMarker | _MarkerLookupFailure | object:
        modal = self._load_modal()
        if verify_workspace:
            client = await self._verified_client(modal, phase=phase)
            if isinstance(client, _MarkerLookupFailure):
                return client
        if client is None:
            return _MarkerLookupFailure(f"{phase} marker lookup has no verified provider client")
        try:
            marker = modal.Dict.from_name(
                marker_name,
                create_if_missing=False,
                environment_name=self._environment_name,
                client=client,
            )
            await marker.hydrate.aio()
            return ModalPersistentDictMarker(
                workspace_name=self._workspace_name,
                environment_name=self._environment_name,
                app_name=self._app_name,
                protocol_epoch=self._protocol_epoch,
                name=marker_name,
                object_id=str(marker.object_id),
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            not_found = getattr(getattr(modal, "exception", None), "NotFoundError", None)
            if isinstance(not_found, type) and isinstance(exc, not_found):
                return _MARKER_MISSING
            return _MarkerLookupFailure(
                f"{phase} marker lookup failed ({type(exc).__name__[:128]})"
            )

    async def _verified_client(
        self,
        modal: Any,
        *,
        phase: str,
    ) -> Any | _MarkerLookupFailure:
        try:
            workspace = modal.Workspace.from_context()
            await workspace.hydrate.aio()
            observed = str(workspace.name or "")
            client = workspace.client
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            return _MarkerLookupFailure(
                f"{phase} workspace lookup failed ({type(exc).__name__[:128]})"
            )
        if observed != self._workspace_name:
            return _MarkerLookupFailure(
                f"{phase} workspace identity does not match configured provider namespace"
            )
        return client

    def _identity_failure(
        self,
        identity: ModalSandboxOperationIdentity,
    ) -> str | None:
        if identity.protocol_epoch != MODAL_ACTIVITY_PROTOCOL_EPOCH:
            return (
                "operation predates the barrier-aware protocol epoch; "
                "legacy or in-flight absence remains Unknown"
            )
        if identity.protocol_epoch != self._protocol_epoch:
            return "operation protocol epoch does not match provider barrier"
        if identity.workspace_name != self._workspace_name:
            return "operation belongs to another Modal workspace"
        if identity.environment_name != self._environment_name:
            return "operation belongs to another Modal environment"
        if identity.app_name != self._app_name:
            return "operation belongs to another Modal app"
        return None

    @staticmethod
    def _load_modal() -> Any:
        try:
            import modal
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(
                "Modal support is optional; install it with `uv sync --extra coding-agent`"
            ) from exc
        return modal


__all__ = [
    "ModalOperationMarkerObservation",
    "ModalOperationGuardAcquisition",
    "ModalPersistentDictMarker",
    "ModalProviderBarrierUnknown",
    "ModalProviderMarkerExists",
    "ModalProviderOperationMissing",
    "ModalProviderOperationGuard",
    "ModalProviderRunPermit",
    "ModalProviderStartOutcome",
    "ModalProviderStarted",
    "ModalProviderStartBarrier",
    "ModalRunPermitAcquisition",
]
