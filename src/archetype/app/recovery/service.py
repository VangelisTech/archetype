# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Bounded storage-scoped fleet recovery orchestration."""

from __future__ import annotations

import asyncio
import hashlib
import re
import time
from dataclasses import dataclass
from typing import cast

from pydantic import BaseModel
from uuid_utils import uuid7

from archetype.app.recovery.interfaces import (
    iMaintenanceRecoveryHandler,
    iRecoverySource,
)
from archetype.app.recovery.models import (
    MAINTENANCE_RECOVERY_KINDS,
    FleetRecoveryCursor,
    RecoveryErrorCode,
    RecoveryException,
    RecoveryExceptionStatus,
    RecoveryItemDisposition,
    RecoveryItemResult,
    RecoveryKind,
    RecoveryLimits,
    RecoveryPage,
    RecoveryPassResult,
    RecoveryPolicy,
    RecoverySubject,
    RecoverySweep,
    RecoverySweepStatus,
    recovery_backoff_ms,
    recovery_subject_key,
)
from archetype.app.storage.catalog import (
    ControlCatalog,
    RecoveryExceptionRecord,
    RecoverySweepPendingError,
    RecoverySweepRecord,
    RecoverySweepStaleError,
    WorldRecord,
    recovery_exception_key,
    recovery_sweep_key,
)

_SHA256_RE = re.compile(r"[0-9a-f]{64}")


def _strict_model_input(value: object) -> object:
    """Lower Pydantic instances to primitives before trust-boundary validation."""

    if isinstance(value, BaseModel):
        return dict(value.__dict__)
    return value


@dataclass
class _PassCounters:
    worlds_examined: int = 0
    sweeps_examined: int = 0
    sweeps_acquired: int = 0
    lease_contentions: int = 0
    items_examined: int = 0
    completed: int = 0
    obsolete: int = 0
    failed: int = 0
    dead_lettered: int = 0
    paused: int = 0


class _RecoverySourceContractError(ValueError):
    """A source violated bounded deterministic discovery."""


class _RecoveryCatalogContractError(ValueError):
    """The control authority returned a structurally impossible result."""


class FleetRecoveryService:
    """Schedule maintenance recovery without possessing model capability.

    One instance is bound to one explicit storage identity. Source records
    remain authoritative; sweep and exception rows only coordinate bounded
    discovery, retries, and operator inspection.
    """

    def __init__(
        self,
        catalog: ControlCatalog,
        *,
        storage_fingerprint: str,
        sources: tuple[iRecoverySource, ...],
        handlers: tuple[iMaintenanceRecoveryHandler, ...],
        policy: RecoveryPolicy | None = None,
    ) -> None:
        if not _SHA256_RE.fullmatch(storage_fingerprint):
            raise ValueError("fleet recovery storage_fingerprint must be a SHA-256 digest")
        self._catalog = catalog
        self._storage_fingerprint = storage_fingerprint
        self._policy = RecoveryPolicy.model_validate(
            _strict_model_input(policy or RecoveryPolicy())
        )
        self._sources = self._index_sources(sources)
        self._handlers = self._index_handlers(handlers)
        if self._sources.keys() != self._handlers.keys():
            raise ValueError("every maintenance recovery kind requires one source and one handler")

    @staticmethod
    def _index_sources(
        values: tuple[iRecoverySource, ...],
    ) -> dict[RecoveryKind, iRecoverySource]:
        result: dict[RecoveryKind, iRecoverySource] = {}
        for source in values:
            if not isinstance(source, iRecoverySource) or not all(
                callable(getattr(source, method, None)) for method in ("discover", "resolve")
            ):
                raise ValueError("fleet recovery source capability is unavailable")
            if callable(getattr(source, "recover_model", None)):
                raise ValueError(
                    "fleet maintenance sources cannot possess model recovery capability"
                )
            kind = RecoveryKind(source.kind)
            if kind not in MAINTENANCE_RECOVERY_KINDS:
                raise ValueError("fleet maintenance sources cannot register model recovery")
            if kind in result:
                raise ValueError(f"duplicate fleet recovery source for {kind.value}")
            result[kind] = source
        return result

    @staticmethod
    def _index_handlers(
        values: tuple[iMaintenanceRecoveryHandler, ...],
    ) -> dict[RecoveryKind, iMaintenanceRecoveryHandler]:
        result: dict[RecoveryKind, iMaintenanceRecoveryHandler] = {}
        for handler in values:
            if not isinstance(handler, iMaintenanceRecoveryHandler) or not callable(
                getattr(handler, "recover", None)
            ):
                raise ValueError("fleet recovery handler capability is unavailable")
            if callable(getattr(handler, "recover_model", None)):
                raise ValueError(
                    "fleet maintenance handlers cannot possess model recovery capability"
                )
            kind = RecoveryKind(handler.kind)
            if kind not in MAINTENANCE_RECOVERY_KINDS:
                raise ValueError("fleet maintenance handlers cannot register model recovery")
            if kind in result:
                raise ValueError(f"duplicate fleet recovery handler for {kind.value}")
            result[kind] = handler
        return result

    async def run_once(
        self,
        *,
        claimant: str,
        cursor: FleetRecoveryCursor | None = None,
        limits: RecoveryLimits | None = None,
        kinds: tuple[RecoveryKind, ...] | None = None,
    ) -> RecoveryPassResult:
        """Run one bounded pass; a process host owns recurrence."""

        if not isinstance(claimant, str):
            raise TypeError("fleet recovery claimant must be a string")
        if not claimant.strip() or len(claimant) > 512:
            raise ValueError(
                "fleet recovery claimant must be a non-empty identifier of at most 512 characters"
            )
        limits = RecoveryLimits.model_validate(_strict_model_input(limits or RecoveryLimits()))
        cursor = FleetRecoveryCursor.model_validate(
            _strict_model_input(cursor or FleetRecoveryCursor())
        )
        selected = self._selected_kinds(kinds)
        if not selected:
            raise ValueError("fleet recovery requires at least one registered maintenance kind")
        if limits.max_sweeps < len(selected):
            raise ValueError(
                "fleet recovery max_sweeps must cover every selected kind for one world"
            )
        durable_claimant = hashlib.sha256(
            ("archetype.fleet-recovery-claimant.v1\0" + claimant + "\0" + str(uuid7())).encode()
        ).hexdigest()
        started_ns = time.monotonic_ns()
        deadline_ns = started_ns + limits.max_elapsed_ms * 1_000_000
        counters = _PassCounters()
        worlds: list[WorldRecord] = []
        last_complete_world = cursor.after_world_id
        stopped = False

        pass_timeout = asyncio.timeout(limits.max_elapsed_ms / 1_000)
        try:
            async with pass_timeout:
                worlds = self._world_page(
                    await self._catalog.list_worlds_page(
                        after_world_id=cursor.after_world_id,
                        limit=limits.world_page_size,
                    ),
                    after_world_id=cursor.after_world_id,
                    limit=limits.world_page_size,
                )
                for world in worlds:
                    remaining_sweeps = limits.max_sweeps - counters.sweeps_examined
                    if remaining_sweeps < len(selected) or self._deadline_reached(deadline_ns):
                        stopped = True
                        break
                    counters.worlds_examined += 1
                    world_complete = True
                    for kind in selected:
                        if self._deadline_reached(deadline_ns):
                            world_complete = False
                            stopped = True
                            break
                        counters.sweeps_examined += 1
                        await self._catalog.ensure_recovery_sweep(
                            self._storage_fingerprint,
                            world.world_id,
                            kind.value,
                            max_consecutive_failures=self._policy.maximum_sweep_failures,
                        )
                        try:
                            outcome, sweep = self._lease_result(
                                await self._catalog.lease_recovery_sweep(
                                    world.world_id,
                                    kind.value,
                                    durable_claimant,
                                    lease_ms=self._policy.lease_ms,
                                ),
                                world_id=world.world_id,
                                kind=kind,
                                claimant=durable_claimant,
                            )
                        except RecoverySweepPendingError:
                            counters.lease_contentions += 1
                            continue
                        if outcome == "paused":
                            counters.paused += 1
                            continue
                        if outcome == "not_due":
                            continue
                        counters.sweeps_acquired += 1
                        try:
                            await self._run_sweep(
                                sweep,
                                claimant=durable_claimant,
                                source=self._sources[kind],
                                handler=self._handlers[kind],
                                item_limit=limits.items_per_sweep,
                                deadline_ns=deadline_ns,
                                counters=counters,
                            )
                        except RecoverySweepStaleError:
                            counters.lease_contentions += 1
                    if world_complete:
                        last_complete_world = world.world_id
                    if stopped:
                        break
        except TimeoutError:
            if not pass_timeout.expired():
                # A catalog, source, or transport timeout is an operational
                # failure, not evidence that this bounded pass consumed its
                # own budget. Do not turn an outage into an empty-success
                # result that can advance a host-level cursor.
                raise
            # Cancellation may leave a leased active subject. That durable
            # state is intentional: a later fenced takeover reconciles it.
            stopped = True

        if not stopped and len(worlds) < limits.world_page_size:
            next_cursor = FleetRecoveryCursor()
        else:
            next_cursor = FleetRecoveryCursor(after_world_id=last_complete_world)
        elapsed_ms = max(0, (time.monotonic_ns() - started_ns) // 1_000_000)
        return RecoveryPassResult(
            cursor=next_cursor,
            worlds_examined=counters.worlds_examined,
            sweeps_examined=counters.sweeps_examined,
            sweeps_acquired=counters.sweeps_acquired,
            lease_contentions=counters.lease_contentions,
            items_examined=counters.items_examined,
            completed=counters.completed,
            obsolete=counters.obsolete,
            failed=counters.failed,
            dead_lettered=counters.dead_lettered,
            paused=counters.paused,
            elapsed_ms=elapsed_ms,
        )

    def _selected_kinds(
        self,
        kinds: tuple[RecoveryKind, ...] | None,
    ) -> tuple[RecoveryKind, ...]:
        requested = (
            tuple(self._handlers) if kinds is None else tuple(RecoveryKind(v) for v in kinds)
        )
        if len(requested) != len(set(requested)):
            raise ValueError("fleet recovery kinds cannot contain duplicates")
        if RecoveryKind.MISSION_MODEL_RECOVERY in requested:
            raise ValueError("model recovery requires the separate #504 supervisor")
        missing = set(requested) - self._handlers.keys()
        if missing:
            names = ", ".join(sorted(kind.value for kind in missing))
            raise ValueError(f"fleet recovery has no maintenance handler for: {names}")
        return tuple(sorted(requested, key=lambda kind: kind.value))

    @staticmethod
    def _world_page(
        value: object,
        *,
        after_world_id: str,
        limit: int,
    ) -> list[WorldRecord]:
        if not isinstance(value, (list, tuple)):
            raise ValueError("fleet recovery catalog returned an invalid world page")
        if len(value) > limit:
            raise ValueError("fleet recovery catalog over-delivered its world page")
        if not all(isinstance(row, WorldRecord) for row in value):
            raise ValueError("fleet recovery catalog returned an invalid world record")
        rows = cast(list[WorldRecord], list(value))
        world_ids = [row.world_id for row in rows]
        if any(
            not isinstance(world_id, str) or not world_id or len(world_id) > 512
            for world_id in world_ids
        ):
            raise ValueError("fleet recovery catalog returned an unsafe world identity")
        if world_ids != sorted(world_ids) or len(world_ids) != len(set(world_ids)):
            raise ValueError("fleet recovery catalog world page must increase strictly")
        if world_ids and world_ids[0] <= after_world_id:
            raise ValueError("fleet recovery catalog world page did not advance its cursor")
        return rows

    def _lease_result(
        self,
        value: object,
        *,
        world_id: str,
        kind: RecoveryKind,
        claimant: str,
    ) -> tuple[str, RecoverySweepRecord]:
        if not isinstance(value, tuple) or len(value) != 2:
            raise _RecoveryCatalogContractError(
                "fleet recovery catalog returned an invalid lease result"
            )
        outcome, row = value
        if not isinstance(outcome, str) or not isinstance(row, RecoverySweepRecord):
            raise _RecoveryCatalogContractError(
                "fleet recovery catalog returned an invalid lease result"
            )
        if outcome not in {"acquired", "recovered", "owned", "paused", "not_due"}:
            raise _RecoveryCatalogContractError(
                "fleet recovery catalog returned an unknown lease outcome"
            )
        try:
            self._sweep_projection(row)
        except (TypeError, ValueError) as exc:
            raise _RecoveryCatalogContractError(
                "fleet recovery catalog returned an invalid sweep record"
            ) from exc
        if (
            row.sweep_key != recovery_sweep_key(self._storage_fingerprint, world_id, kind.value)
            or row.storage_fingerprint != self._storage_fingerprint
            or row.world_id != world_id
            or row.kind != kind.value
        ):
            raise _RecoveryCatalogContractError(
                "fleet recovery catalog substituted sweep authority"
            )
        if outcome in {"acquired", "recovered", "owned"}:
            if (
                row.status != RecoverySweepStatus.LEASED.value
                or row.claimant != claimant
                or row.fence_epoch < 1
                or row.lease_expires_at_ms < 1
            ):
                raise _RecoveryCatalogContractError(
                    "fleet recovery catalog returned an unauthenticated lease"
                )
        elif outcome == "paused" and row.status != RecoverySweepStatus.PAUSED.value:
            raise _RecoveryCatalogContractError(
                "fleet recovery catalog returned an inconsistent paused lease"
            )
        elif outcome == "not_due" and row.status not in {
            RecoverySweepStatus.IDLE.value,
            RecoverySweepStatus.RETRY_WAIT.value,
        }:
            raise _RecoveryCatalogContractError(
                "fleet recovery catalog returned an inconsistent not-due lease"
            )
        return outcome, row

    def _due_exception_page(
        self,
        value: object,
        *,
        sweep: RecoverySweepRecord,
        limit: int,
    ) -> list[RecoveryExceptionRecord]:
        if not isinstance(value, (list, tuple)):
            raise _RecoveryCatalogContractError(
                "fleet recovery catalog returned an invalid exception page"
            )
        if len(value) > limit:
            raise _RecoveryCatalogContractError(
                "fleet recovery catalog over-delivered its exception page"
            )
        if not all(isinstance(row, RecoveryExceptionRecord) for row in value):
            raise _RecoveryCatalogContractError(
                "fleet recovery catalog returned an invalid exception record"
            )
        rows = cast(list[RecoveryExceptionRecord], list(value))
        ordering = [(row.retry_at_ms, row.exception_key) for row in rows]
        if ordering != sorted(ordering) or len(ordering) != len(set(ordering)):
            raise _RecoveryCatalogContractError(
                "fleet recovery catalog exception page must increase strictly"
            )
        subject_keys = [row.subject_key for row in rows]
        if len(subject_keys) != len(set(subject_keys)):
            raise _RecoveryCatalogContractError(
                "fleet recovery catalog repeated an exception subject"
            )
        for row in rows:
            try:
                self._exception_projection(row)
            except (TypeError, ValueError) as exc:
                raise _RecoveryCatalogContractError(
                    "fleet recovery catalog returned an invalid exception record"
                ) from exc
            if (
                row.sweep_key != sweep.sweep_key
                or row.storage_fingerprint != self._storage_fingerprint
                or row.world_id != sweep.world_id
                or row.kind != sweep.kind
                or row.status != RecoveryExceptionStatus.RETRY_WAIT.value
                or row.attempt_count < 1
                or row.subject_key
                != recovery_subject_key(
                    RecoveryKind(row.kind),
                    row.world_id,
                    row.authority_key,
                )
                or row.exception_key != recovery_exception_key(sweep.sweep_key, row.subject_key)
            ):
                raise _RecoveryCatalogContractError(
                    "fleet recovery catalog substituted exception authority"
                )
        return rows

    async def _run_sweep(
        self,
        sweep: RecoverySweepRecord,
        *,
        claimant: str,
        source: iRecoverySource,
        handler: iMaintenanceRecoveryHandler,
        item_limit: int,
        deadline_ns: int,
        counters: _PassCounters,
    ) -> None:
        cursor = sweep.cursor
        processed_keys: set[str] = set()
        try:
            if sweep.active_subject_key and not self._deadline_reached(deadline_ns):
                subject = self._source_subject(
                    await source.resolve(sweep.world_id, sweep.active_subject_key)
                )
                if subject is None:
                    await self._catalog.checkpoint_recovery_sweep(
                        sweep.world_id,
                        sweep.kind,
                        claimant,
                        sweep.fence_epoch,
                        cursor=cursor,
                    )
                else:
                    self._require_subject(
                        subject,
                        sweep,
                        expected_authority_key=sweep.active_subject_key,
                    )
                    active_exception = await self._catalog.get_recovery_exception(
                        sweep.world_id,
                        sweep.kind,
                        recovery_exception_key(sweep.sweep_key, subject.subject_key),
                    )
                    if active_exception is not None and active_exception.status in {
                        RecoveryExceptionStatus.RETRY_WAIT.value,
                        RecoveryExceptionStatus.DEAD_LETTER.value,
                    }:
                        # A crash may land after retry/DLQ persistence but
                        # before clearing the active subject. The sparse row
                        # now owns scheduling; never bypass its server-clock
                        # delay or operator redrive requirement.
                        await self._catalog.checkpoint_recovery_sweep(
                            sweep.world_id,
                            sweep.kind,
                            claimant,
                            sweep.fence_epoch,
                            cursor=cursor,
                        )
                    else:
                        await self._process_subject(
                            sweep,
                            claimant=claimant,
                            subject=subject,
                            handler=handler,
                            cursor=cursor,
                            deadline_ns=deadline_ns,
                            counters=counters,
                            exception=active_exception,
                        )
                        processed_keys.add(subject.subject_key)

            remaining = item_limit - len(processed_keys)
            exception_budget = (
                min(remaining, max(1, item_limit // 4))
                if item_limit > 1
                else (1 if sweep.cycle % 2 == 0 else 0)
            )
            if exception_budget > 0 and not self._deadline_reached(deadline_ns):
                due = self._due_exception_page(
                    await self._catalog.list_recovery_exceptions(
                        sweep.world_id,
                        kind=sweep.kind,
                        status=RecoveryExceptionStatus.RETRY_WAIT.value,
                        due_only=True,
                        limit=exception_budget,
                    ),
                    sweep=sweep,
                    limit=exception_budget,
                )
                for exception in due:
                    if exception.subject_key in processed_keys:
                        continue
                    if self._deadline_reached(deadline_ns):
                        break
                    subject = self._source_subject(
                        await source.resolve(sweep.world_id, exception.authority_key)
                    )
                    if subject is None:
                        await self._catalog.resolve_recovery_exception(
                            sweep.world_id,
                            sweep.kind,
                            claimant,
                            sweep.fence_epoch,
                            exception.exception_key,
                        )
                        counters.items_examined += 1
                        counters.obsolete += 1
                    else:
                        self._require_subject(
                            subject,
                            sweep,
                            expected_subject_key=exception.subject_key,
                            expected_authority_key=exception.authority_key,
                        )
                        await self._process_subject(
                            sweep,
                            claimant=claimant,
                            subject=subject,
                            handler=handler,
                            cursor=cursor,
                            deadline_ns=deadline_ns,
                            counters=counters,
                            exception=exception,
                        )
                    processed_keys.add(exception.subject_key)

            remaining = item_limit - len(processed_keys)
            if remaining > 0 and not self._deadline_reached(deadline_ns):
                try:
                    page = RecoveryPage.model_validate(
                        _strict_model_input(
                            await source.discover(sweep.world_id, cursor, limit=remaining)
                        ),
                        from_attributes=True,
                    )
                except (TypeError, ValueError) as exc:
                    raise _RecoverySourceContractError(
                        "recovery source returned an invalid page"
                    ) from exc
                if len(page.subjects) > remaining:
                    raise _RecoverySourceContractError(
                        "recovery source returned more subjects than requested"
                    )
                if page.subjects and page.subjects[0].cursor_after <= cursor:
                    raise _RecoverySourceContractError(
                        "recovery source page did not advance its cursor"
                    )
                page_complete = True
                for subject in page.subjects:
                    self._require_subject(subject, sweep)
                    if self._deadline_reached(deadline_ns):
                        page_complete = False
                        break
                    if subject.subject_key in processed_keys:
                        cursor = subject.cursor_after or cursor
                        continue
                    exception_key = recovery_exception_key(sweep.sweep_key, subject.subject_key)
                    existing = await self._catalog.get_recovery_exception(
                        sweep.world_id,
                        sweep.kind,
                        exception_key,
                    )
                    if existing is not None and existing.status in {
                        RecoveryExceptionStatus.RETRY_WAIT.value,
                        RecoveryExceptionStatus.DEAD_LETTER.value,
                    }:
                        processed_keys.add(subject.subject_key)
                        cursor = subject.cursor_after or cursor
                        continue
                    if existing is not None:
                        raise RuntimeError(
                            "a resolved recovery exception still has authoritative due work"
                        )
                    await self._process_subject(
                        sweep,
                        claimant=claimant,
                        subject=subject,
                        handler=handler,
                        cursor=cursor,
                        deadline_ns=deadline_ns,
                        counters=counters,
                    )
                    processed_keys.add(subject.subject_key)
                    cursor = subject.cursor_after or cursor
                if page_complete:
                    if page.subjects and not page.exhausted:
                        cursor = page.next_cursor
                    elif page.exhausted:
                        cursor = ""

            await self._catalog.checkpoint_recovery_sweep(
                sweep.world_id,
                sweep.kind,
                claimant,
                sweep.fence_epoch,
                cursor=cursor,
            )
            await self._catalog.yield_recovery_sweep(
                sweep.world_id,
                sweep.kind,
                claimant,
                sweep.fence_epoch,
                next_delay_ms=self._policy.recurring_delay_ms,
            )
        except RecoverySweepStaleError:
            raise
        except Exception as exc:
            attempt = sweep.consecutive_failures + 1
            delay = recovery_backoff_ms(
                sweep.sweep_key,
                attempt,
                initial_delay_ms=self._policy.initial_retry_delay_ms,
                maximum_delay_ms=self._policy.maximum_retry_delay_ms,
                jitter_basis_points=self._policy.jitter_basis_points,
            )
            failed = await self._catalog.fail_recovery_sweep(
                sweep.world_id,
                sweep.kind,
                claimant,
                sweep.fence_epoch,
                error_code=(
                    RecoveryErrorCode.SOURCE_CORRUPT.value
                    if isinstance(exc, _RecoverySourceContractError)
                    else RecoveryErrorCode.DISCOVERY_FAILED.value
                ),
                # Error details are deliberately empty. Arbitrary exception
                # types and messages are attacker-controlled text and cannot
                # cross this durability boundary; the closed error_code is the
                # complete operator-visible classification.
                error_detail="",
                retry_delay_ms=delay,
            )
            if failed.status == RecoverySweepStatus.PAUSED.value:
                counters.paused += 1

    async def _process_subject(
        self,
        sweep: RecoverySweepRecord,
        *,
        claimant: str,
        subject: RecoverySubject,
        handler: iMaintenanceRecoveryHandler,
        cursor: str,
        deadline_ns: int,
        counters: _PassCounters,
        exception: RecoveryExceptionRecord | None = None,
    ) -> None:
        await self._catalog.renew_recovery_sweep(
            sweep.world_id,
            sweep.kind,
            claimant,
            sweep.fence_epoch,
            lease_ms=self._policy.lease_ms,
        )
        await self._catalog.checkpoint_recovery_sweep(
            sweep.world_id,
            sweep.kind,
            claimant,
            sweep.fence_epoch,
            cursor=cursor,
            active_subject_key=subject.authority_key,
        )
        counters.items_examined += 1
        timeout_seconds = min(
            self._remaining_seconds(deadline_ns),
            max(0.001, self._policy.lease_ms / 2_000),
        )
        try:
            async with asyncio.timeout(timeout_seconds):
                result = RecoveryItemResult.model_validate(
                    _strict_model_input(await handler.recover(subject)),
                    from_attributes=True,
                )
            if result.subject_key != subject.subject_key:
                raise RuntimeError("recovery handler returned a different subject identity")
        except Exception:
            counters.failed += 1
            expected_attempt = exception.attempt_count if exception is not None else 0
            exception_key = recovery_exception_key(sweep.sweep_key, subject.subject_key)
            delay = recovery_backoff_ms(
                exception_key,
                expected_attempt + 1,
                initial_delay_ms=self._policy.initial_retry_delay_ms,
                maximum_delay_ms=self._policy.maximum_retry_delay_ms,
                jitter_basis_points=self._policy.jitter_basis_points,
            )
            recorded = await self._catalog.retry_recovery_exception(
                sweep.world_id,
                sweep.kind,
                claimant,
                sweep.fence_epoch,
                subject_key=subject.subject_key,
                authority_key=subject.authority_key,
                expected_attempt_count=expected_attempt,
                error_code=RecoveryErrorCode.HANDLER_FAILED.value,
                # See the sweep failure path above. Narrative diagnostics may
                # exist only in a separately redacted artifact.
                error_detail="",
                retry_delay_ms=delay,
                # Sparse exception identity includes its original attempt
                # ceiling. A restarted worker may have newer policy, but it
                # must finish this durable retry lineage under the persisted
                # ceiling rather than invoke the handler and then conflict.
                max_attempts=(
                    exception.max_attempts
                    if exception is not None
                    else self._policy.maximum_exception_attempts
                ),
            )
            if recorded.status == RecoveryExceptionStatus.DEAD_LETTER.value:
                counters.dead_lettered += 1
        else:
            if exception is not None:
                await self._catalog.resolve_recovery_exception(
                    sweep.world_id,
                    sweep.kind,
                    claimant,
                    sweep.fence_epoch,
                    exception.exception_key,
                )
            if result.disposition is RecoveryItemDisposition.OBSOLETE:
                counters.obsolete += 1
            else:
                counters.completed += 1
        await self._catalog.checkpoint_recovery_sweep(
            sweep.world_id,
            sweep.kind,
            claimant,
            sweep.fence_epoch,
            # Clearing the active subject is durable progress; cursor
            # advancement remains the discovery loop's responsibility. A
            # resolver reconstructs authority after a crash but does not own
            # the page cursor, so persisting its cursor here could skip unseen
            # tail work if source code drifted or was compromised. Re-scanning
            # from the predecessor is safe because the source outcome or sparse
            # exception above is already idempotent and durable.
            cursor=cursor,
        )

    @staticmethod
    def _require_subject(
        subject: RecoverySubject,
        sweep: RecoverySweepRecord,
        *,
        expected_subject_key: str | None = None,
        expected_authority_key: str | None = None,
    ) -> None:
        if subject.world_id != sweep.world_id or subject.kind.value != sweep.kind:
            raise _RecoverySourceContractError(
                "recovery source returned a subject outside its sweep authority"
            )
        if expected_subject_key is not None and subject.subject_key != expected_subject_key:
            raise _RecoverySourceContractError(
                "recovery source resolved a different scheduled subject"
            )
        if expected_authority_key is not None and subject.authority_key != expected_authority_key:
            raise _RecoverySourceContractError("recovery source resolved a different authority")

    @staticmethod
    def _source_subject(value: object | None) -> RecoverySubject | None:
        if value is None:
            return None
        try:
            return RecoverySubject.model_validate(
                _strict_model_input(value),
                from_attributes=True,
            )
        except (TypeError, ValueError) as exc:
            raise _RecoverySourceContractError(
                "recovery source resolved an invalid subject"
            ) from exc

    @staticmethod
    def _deadline_reached(deadline_ns: int) -> bool:
        return time.monotonic_ns() >= deadline_ns

    @staticmethod
    def _remaining_seconds(deadline_ns: int) -> float:
        return max(0.001, (deadline_ns - time.monotonic_ns()) / 1_000_000_000)

    async def list_sweeps(
        self,
        *,
        world_id: str | None = None,
        kind: RecoveryKind | None = None,
        status: RecoverySweepStatus | None = None,
        limit: int = 100,
    ) -> tuple[RecoverySweep, ...]:
        """Return a bounded safe projection; raw claimants and details stay private."""

        worlds = await self._inspection_worlds(world_id, limit)
        results: list[RecoverySweep] = []
        for current_world_id in worlds:
            rows = await self._catalog.list_recovery_sweeps(
                current_world_id,
                status=status.value if status is not None else None,
            )
            for row in rows:
                if kind is not None and row.kind != kind.value:
                    continue
                results.append(self._sweep_projection(row))
                if len(results) >= limit:
                    return tuple(results)
        return tuple(results)

    async def list_exceptions(
        self,
        *,
        world_id: str | None = None,
        kind: RecoveryKind | None = None,
        status: RecoveryExceptionStatus | None = None,
        limit: int = 100,
    ) -> tuple[RecoveryException, ...]:
        """Return bounded safe retry/DLQ evidence without raw diagnostics."""

        worlds = await self._inspection_worlds(world_id, limit)
        results: list[RecoveryException] = []
        for current_world_id in worlds:
            rows = await self._catalog.list_recovery_exceptions(
                current_world_id,
                kind=kind.value if kind is not None else None,
                status=status.value if status is not None else None,
                limit=max(1, limit - len(results)),
            )
            results.extend(self._exception_projection(row) for row in rows)
            if len(results) >= limit:
                return tuple(results[:limit])
        return tuple(results)

    async def _inspection_worlds(self, world_id: str | None, limit: int) -> tuple[str, ...]:
        if type(limit) is not int or limit < 1 or limit > 10_000:
            raise ValueError("recovery inspection limit must be between 1 and 10000")
        if world_id is not None:
            if not isinstance(world_id, str) or not world_id or len(world_id) > 512:
                raise ValueError(
                    "recovery inspection world_id must be a non-empty string of at most "
                    "512 characters"
                )
            return (world_id,)
        rows = self._world_page(
            await self._catalog.list_worlds_page(limit=limit),
            after_world_id="",
            limit=limit,
        )
        return tuple(row.world_id for row in rows)

    @staticmethod
    def _claimant_digest(value: str) -> str:
        if not value:
            return ""
        return hashlib.sha256(
            ("archetype.fleet-recovery-inspector-claimant.v1\0" + value).encode()
        ).hexdigest()

    def _sweep_projection(self, row: RecoverySweepRecord) -> RecoverySweep:
        return RecoverySweep(
            sweep_key=row.sweep_key,
            storage_fingerprint=row.storage_fingerprint,
            world_id=row.world_id,
            kind=RecoveryKind(row.kind),
            status=RecoverySweepStatus(row.status),
            cursor=row.cursor,
            cycle=row.cycle,
            claimant_digest=(
                self._claimant_digest(row.claimant)
                if row.status == RecoverySweepStatus.LEASED.value
                else ""
            ),
            lease_expires_at_ms=row.lease_expires_at_ms,
            fence_epoch=row.fence_epoch,
            active_subject_key=row.active_subject_key,
            consecutive_failures=row.consecutive_failures,
            maximum_consecutive_failures=row.max_consecutive_failures,
            next_due_at_ms=row.next_due_at_ms,
            last_error_code=(RecoveryErrorCode(row.last_error_code) if row.last_error_code else ""),
            created_at_ms=row.created_at_ms,
            updated_at_ms=row.updated_at_ms,
            paused_at_ms=row.paused_at_ms,
        )

    @staticmethod
    def _exception_projection(row: RecoveryExceptionRecord) -> RecoveryException:
        return RecoveryException(
            exception_key=row.exception_key,
            sweep_key=row.sweep_key,
            storage_fingerprint=row.storage_fingerprint,
            world_id=row.world_id,
            kind=RecoveryKind(row.kind),
            subject_key=row.subject_key,
            authority_key=row.authority_key,
            status=RecoveryExceptionStatus(row.status),
            attempt_count=row.attempt_count,
            maximum_attempts=row.max_attempts,
            retry_at_ms=row.retry_at_ms,
            last_error_code=(RecoveryErrorCode(row.last_error_code) if row.last_error_code else ""),
            created_at_ms=row.created_at_ms,
            updated_at_ms=row.updated_at_ms,
            resolved_at_ms=row.resolved_at_ms,
            dead_lettered_at_ms=row.dead_lettered_at_ms,
        )
