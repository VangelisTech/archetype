# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Cross-process fixture for provider-native durable Mission jobs.

The provider double deliberately keeps its job records and first result in a
SQLite database that is independent from the production Activity catalog.  A
hard failpoint uses ``os._exit`` so replacement actions cannot inherit Python
objects, pending tasks, or cleanup from the process that started the job.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from archetype.activities import (
    ActivityAdmission,
    ActivityCoordinator,
    ActivityExecutionIdentity,
    ActivityResultRef,
    ActivitySettlement,
)
from archetype.core.interfaces import CommittedTickReceipt
from archetype.missions.modal_jobs import (
    ModalMissionJobClient,
    ModalMissionJobNamespace,
    ModalMissionJobReady,
    ModalMissionJobRef,
    ModalMissionJobRunning,
    ModalMissionJobStillRunning,
    ModalMissionJobUnknown,
)
from archetype.storage.activity_catalog import (
    SqliteActivityCatalog,
    inspect_sqlite_activity_catalog,
)

_ACTIVITY_ID = "dispatch-process-1"
_ACTIVITY_KIND = "missions.author"
_FAMILY = "author"
_OPERATION_ID = "mission:author:process-crash-1"
_PROVIDER = "modal-process-double"
_REQUEST_BYTES = b'{"mission":"process-crash-proof","schema_version":1}'
_REQUEST_DIGEST = hashlib.sha256(_REQUEST_BYTES).hexdigest()
_RESULT_MEDIA_TYPE = "application/vnd.archetype.mission-author+json"

_CALL_IDENTITY_CRASH = 86
_RESULT_RECORD_CRASH = 87

_Failpoint = Literal["none", "after-call-identity", "after-result-record"]

_DDL = """
CREATE TABLE IF NOT EXISTS job_records (
    key TEXT PRIMARY KEY,
    value_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS provider_sequence (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    next_value INTEGER NOT NULL
);
INSERT OR IGNORE INTO provider_sequence (singleton, next_value) VALUES (1, 1);
CREATE TABLE IF NOT EXISTS provider_calls (
    call_id TEXT PRIMARY KEY,
    family TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    request_bytes BLOB NOT NULL,
    request_digest TEXT NOT NULL,
    namespace_digest TEXT NOT NULL,
    status TEXT NOT NULL,
    result_bytes BLOB,
    result_digest TEXT
);
CREATE TABLE IF NOT EXISTS provider_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    worker TEXT NOT NULL,
    event TEXT NOT NULL,
    call_id TEXT,
    detail TEXT NOT NULL
);
"""


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=True,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _namespace() -> ModalMissionJobNamespace:
    return ModalMissionJobNamespace(
        deployment_digest="a" * 64,
        image_id="process-test-image",
        result_dict_name="process-test-results",
        redaction_policy_id="process-test-redaction-v1",
    )


def _source_receipt() -> CommittedTickReceipt:
    return CommittedTickReceipt(
        world_id="mission-process-world",
        run_id="mission-process-run",
        committed_tick=1,
        visibility_token="mission-process-source-1",
        commands_applied=0,
    )


def _observation_receipt() -> CommittedTickReceipt:
    return CommittedTickReceipt(
        world_id="mission-process-world",
        run_id="mission-process-run",
        committed_tick=2,
        visibility_token="mission-process-observation-2",
        commands_applied=0,
    )


@dataclass(frozen=True, slots=True)
class _Call:
    call_id: str


class _SqliteMissionJobRuntime:
    """Persistent double for the public ``ModalMissionJobRuntime`` protocol."""

    def __init__(
        self,
        path: Path,
        *,
        worker: str,
        failpoint: _Failpoint = "none",
    ) -> None:
        self._path = path
        self._worker = worker
        self._failpoint = failpoint
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self._path, timeout=5)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=5000")
        return connection

    def _initialize(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        connection = self._connect()
        try:
            connection.executescript(_DDL)
            connection.commit()
        finally:
            connection.close()

    @staticmethod
    def _event(
        connection: sqlite3.Connection,
        *,
        worker: str,
        event: str,
        call_id: str | None = None,
        detail: str = "",
    ) -> None:
        connection.execute(
            """
            INSERT INTO provider_events (worker, event, call_id, detail)
            VALUES (?, ?, ?, ?)
            """,
            (worker, event, call_id, detail),
        )

    async def get(self, key: str) -> object:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT value_json FROM job_records WHERE key = ?",
                (key,),
            ).fetchone()
        finally:
            connection.close()
        return None if row is None else json.loads(str(row["value_json"]))

    async def put_if_absent(self, key: str, value: Mapping[str, Any]) -> bool:
        encoded = _canonical_json(dict(value))
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(
                "INSERT OR IGNORE INTO job_records (key, value_json) VALUES (?, ?)",
                (key, encoded),
            )
            inserted = cursor.rowcount == 1
            if inserted:
                event = "call_identity_inserted" if ":call:" in key else "job_record_inserted"
                self._event(
                    connection,
                    worker=self._worker,
                    event=event,
                    detail=key,
                )
            connection.commit()
        finally:
            connection.close()
        if inserted and ":call:" in key and self._failpoint == "after-call-identity":
            # The transaction is closed before this hard exit. The caller gets
            # no response and no Python cleanup can make the test pass.
            os._exit(_CALL_IDENTITY_CRASH)
        return inserted

    async def spawn(
        self,
        *,
        family: Literal["author", "critic"],
        operation_id: str,
        request_bytes: bytes,
        namespace_digest: str,
    ) -> object:
        request_digest = hashlib.sha256(request_bytes).hexdigest()
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT next_value FROM provider_sequence WHERE singleton = 1"
            ).fetchone()
            assert row is not None
            sequence = int(row["next_value"])
            connection.execute(
                "UPDATE provider_sequence SET next_value = ? WHERE singleton = 1",
                (sequence + 1,),
            )
            call_id = f"fc-process-{sequence}"
            connection.execute(
                """
                INSERT INTO provider_calls (
                    call_id,
                    family,
                    operation_id,
                    request_bytes,
                    request_digest,
                    namespace_digest,
                    status
                ) VALUES (?, ?, ?, ?, ?, ?, 'running')
                """,
                (
                    call_id,
                    family,
                    operation_id,
                    sqlite3.Binary(request_bytes),
                    request_digest,
                    namespace_digest,
                ),
            )
            self._event(
                connection,
                worker=self._worker,
                event="spawn",
                call_id=call_id,
                detail=operation_id,
            )
            connection.commit()
        finally:
            connection.close()

        # Model the remote controller fencing itself before it crosses the
        # effect boundary. This public call writes the durable call identity
        # before ``spawn`` returns to the original host.
        registered = await ModalMissionJobClient(_namespace(), self).register_remote_call(
            family=family,
            operation_id=operation_id,
            request_digest=request_digest,
            call_id=call_id,
        )
        if not isinstance(registered, ModalMissionJobRef):
            raise RuntimeError(f"remote registration failed: {registered!r}")
        return _Call(call_id)

    def call_id(self, call: object) -> str:
        if not isinstance(call, _Call):
            raise TypeError("process provider call has an invalid handle")
        return call.call_id

    async def reattach(self, call_id: str) -> object:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT call_id FROM provider_calls WHERE call_id = ?",
                (call_id,),
            ).fetchone()
            if row is None:
                raise LookupError(f"provider call {call_id!r} is absent")
            self._event(
                connection,
                worker=self._worker,
                event="reattach",
                call_id=call_id,
            )
            connection.commit()
        finally:
            connection.close()
        return _Call(call_id)

    async def cancel(self, call: object) -> None:
        call_id = self.call_id(call)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT call_id FROM provider_calls WHERE call_id = ?",
                (call_id,),
            ).fetchone()
            if row is None:
                raise LookupError(f"provider call {call_id!r} is absent")
            self._event(
                connection,
                worker=self._worker,
                event="cancel",
                call_id=call_id,
            )
            connection.commit()
        finally:
            connection.close()

    async def call_result(self, call: object, *, timeout_seconds: float) -> object:
        if timeout_seconds != 0:
            raise ValueError("process provider polling must be nonblocking")
        call_id = self.call_id(call)
        connection = self._connect()
        try:
            row = connection.execute(
                """
                SELECT status, result_bytes
                FROM provider_calls
                WHERE call_id = ?
                """,
                (call_id,),
            ).fetchone()
        finally:
            connection.close()
        if row is None:
            raise LookupError(f"provider call {call_id!r} is absent")
        status = str(row["status"])
        if status == "running":
            raise ModalMissionJobStillRunning
        if status == "terminal-failed":
            raise RuntimeError("provider output expired after first-result publication")
        raw = row["result_bytes"]
        if raw is None:
            raise RuntimeError("provider call completed without a first result")
        return json.loads(bytes(raw))

    async def result_ready(self, ref: ModalMissionJobRef) -> bool:
        connection = self._connect()
        try:
            row = connection.execute(
                """
                SELECT result_bytes
                FROM provider_calls
                WHERE call_id = ?
                  AND family = ?
                  AND operation_id = ?
                  AND request_digest = ?
                  AND namespace_digest = ?
                """,
                (
                    ref.call_id,
                    ref.family,
                    ref.operation_id,
                    ref.request_digest,
                    ref.namespace_digest,
                ),
            ).fetchone()
        finally:
            connection.close()
        return row is not None and row["result_bytes"] is not None

    def publish_first_result(self) -> tuple[str, str, int]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute(
                """
                SELECT call_id, operation_id, result_bytes, result_digest
                FROM provider_calls
                ORDER BY call_id
                """
            ).fetchall()
            if len(rows) != 1:
                raise RuntimeError(f"expected one provider call, found {len(rows)}")
            row = rows[0]
            call_id = str(row["call_id"])
            result_bytes = _canonical_json(
                {
                    "call_id": call_id,
                    "operation_id": str(row["operation_id"]),
                    "schema_version": 1,
                    "status": "complete",
                }
            ).encode()
            result_digest = hashlib.sha256(result_bytes).hexdigest()
            existing = row["result_bytes"]
            if existing is not None and (
                bytes(existing) != result_bytes or str(row["result_digest"]) != result_digest
            ):
                raise RuntimeError("provider first result conflicts")
            if existing is None:
                connection.execute(
                    """
                    UPDATE provider_calls
                    SET result_bytes = ?, result_digest = ?, status = 'terminal-failed'
                    WHERE call_id = ?
                    """,
                    (sqlite3.Binary(result_bytes), result_digest, call_id),
                )
                self._event(
                    connection,
                    worker=self._worker,
                    event="result_published",
                    call_id=call_id,
                    detail=result_digest,
                )
            connection.commit()
        finally:
            connection.close()
        return call_id, result_digest, len(result_bytes)

    def read_first_result(self, ref: ModalMissionJobRef) -> tuple[bytes, str]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT result_bytes, result_digest
                FROM provider_calls
                WHERE call_id = ?
                  AND family = ?
                  AND operation_id = ?
                  AND request_digest = ?
                  AND namespace_digest = ?
                """,
                (
                    ref.call_id,
                    ref.family,
                    ref.operation_id,
                    ref.request_digest,
                    ref.namespace_digest,
                ),
            ).fetchone()
            if row is None or row["result_bytes"] is None or row["result_digest"] is None:
                raise RuntimeError("exact provider first result is absent")
            result_bytes = bytes(row["result_bytes"])
            result_digest = str(row["result_digest"])
            if hashlib.sha256(result_bytes).hexdigest() != result_digest:
                raise RuntimeError("provider first result digest is corrupt")
            self._event(
                connection,
                worker=self._worker,
                event="result_read",
                call_id=ref.call_id,
                detail=result_digest,
            )
            connection.commit()
        finally:
            connection.close()
        return result_bytes, result_digest

    def snapshot(self) -> dict[str, Any]:
        connection = self._connect()
        try:
            calls = connection.execute(
                """
                SELECT call_id, family, operation_id, request_digest,
                       namespace_digest, status, result_digest
                FROM provider_calls
                ORDER BY call_id
                """
            ).fetchall()
            events = connection.execute(
                """
                SELECT worker, event, call_id, detail
                FROM provider_events
                ORDER BY sequence
                """
            ).fetchall()
            record_count = int(connection.execute("SELECT COUNT(*) FROM job_records").fetchone()[0])
        finally:
            connection.close()
        event_values = [dict(row) for row in events]
        return {
            "call_identity_insertions": sum(
                event["event"] == "call_identity_inserted" for event in event_values
            ),
            "calls": [dict(row) for row in calls],
            "event_count": len(event_values),
            "events": event_values,
            "job_record_count": record_count,
            "reattach_call_ids": [
                str(event["call_id"]) for event in event_values if event["event"] == "reattach"
            ],
            "result_publication_count": sum(
                event["event"] == "result_published" for event in event_values
            ),
            "spawn_count": sum(event["event"] == "spawn" for event in event_values),
        }


async def _admit_activity(path: Path) -> None:
    catalog = SqliteActivityCatalog(path)
    coordinator = ActivityCoordinator(catalog)
    try:
        await coordinator.admit(
            ActivityAdmission(
                activity_id=_ACTIVITY_ID,
                kind=_ACTIVITY_KIND,
                source=_source_receipt(),
                input_ref=f"inline://mission-request/{_REQUEST_DIGEST}",
                input_digest=_REQUEST_DIGEST,
            ),
            ActivityExecutionIdentity(
                provider=_PROVIDER,
                operation_id=_OPERATION_ID,
            ),
        )
    finally:
        await catalog.close()


async def _recover_ref(runtime: _SqliteMissionJobRuntime) -> ModalMissionJobRef:
    outcome = await ModalMissionJobClient(_namespace(), runtime).start(
        family=_FAMILY,
        operation_id=_OPERATION_ID,
        request_bytes=_REQUEST_BYTES,
        request_digest=_REQUEST_DIGEST,
    )
    if isinstance(outcome, ModalMissionJobUnknown):
        raise RuntimeError(f"durable Mission job is unknown: {outcome.reason}")
    return outcome


async def _record_activity_result(
    activity_path: Path,
    runtime: _SqliteMissionJobRuntime,
    ref: ModalMissionJobRef,
) -> str:
    result_bytes, result_digest = runtime.read_first_result(ref)
    catalog = SqliteActivityCatalog(activity_path)
    coordinator = ActivityCoordinator(catalog)
    try:
        await coordinator.record_orchestrated_result(
            _source_receipt().world_id,
            _ACTIVITY_KIND,
            _ACTIVITY_ID,
            ActivityExecutionIdentity(
                provider=_PROVIDER,
                operation_id=_OPERATION_ID,
            ),
            ActivityResultRef(
                ref=f"sqlite-result://mission-jobs/{ref.call_id}",
                digest=result_digest,
                media_type=_RESULT_MEDIA_TYPE,
                size_bytes=len(result_bytes),
            ),
        )
    finally:
        await catalog.close()
    return result_digest


async def _activity_snapshot(path: Path) -> dict[str, Any] | None:
    catalog = SqliteActivityCatalog(path)
    coordinator = ActivityCoordinator(catalog)
    try:
        snapshot = await coordinator.get(
            _source_receipt().world_id,
            _ACTIVITY_KIND,
            _ACTIVITY_ID,
        )
    finally:
        await catalog.close()
    if snapshot is None:
        return None
    result = snapshot.result
    settlement = snapshot.settlement
    return {
        "activity_id": snapshot.admission.activity_id,
        "execution": (
            None
            if snapshot.execution is None
            else {
                "operation_id": snapshot.execution.operation_id,
                "provider": snapshot.execution.provider,
            }
        ),
        "result": (
            None
            if result is None
            else {
                "digest": result.digest,
                "media_type": result.media_type,
                "ref": result.ref,
                "size_bytes": result.size_bytes,
            }
        ),
        "result_fence": snapshot.result_fence,
        "result_pending_observation": snapshot.result_pending_observation,
        "result_attempt": snapshot.result_attempt,
        "settlement": (
            None
            if settlement is None
            else {
                "committed_tick": settlement.receipt.committed_tick,
                "result_digest": settlement.result_digest,
                "run_id": settlement.receipt.run_id,
                "visibility_token": settlement.receipt.visibility_token,
                "world_id": settlement.receipt.world_id,
            }
        ),
        "world_id": snapshot.admission.source.world_id,
    }


async def _state(root: Path, runtime: _SqliteMissionJobRuntime) -> dict[str, Any]:
    activity_path = root / "activities.sqlite3"
    activity = await _activity_snapshot(activity_path)
    inventory = inspect_sqlite_activity_catalog(activity_path)
    return {
        "activity": activity,
        "activity_inventory": {
            "activity_count": inventory.activity_count,
            "attempt_count": inventory.attempt_count,
            "provider_operation_count": inventory.provider_operation_count,
        },
        "provider": runtime.snapshot(),
    }


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    root = Path(args.state_dir)
    root.mkdir(parents=True, exist_ok=True)
    activity_path = root / "activities.sqlite3"
    runtime = _SqliteMissionJobRuntime(
        root / "provider.sqlite3",
        worker=str(args.action),
        failpoint=args.failpoint,
    )
    action = str(args.action)

    if action == "start":
        await _admit_activity(activity_path)
        ref = await _recover_ref(runtime)
        return {**await _state(root, runtime), "call_id": ref.call_id}

    if action == "poll":
        ref = await _recover_ref(runtime)
        outcome = await ModalMissionJobClient(_namespace(), runtime).poll(ref)
        if isinstance(outcome, ModalMissionJobRunning):
            status = "running"
        elif isinstance(outcome, ModalMissionJobReady):
            status = "ready"
        else:
            status = f"unknown:{outcome.reason}"
        return {
            **await _state(root, runtime),
            "call_id": ref.call_id,
            "poll": status,
        }

    if action == "publish":
        call_id, result_digest, result_size = runtime.publish_first_result()
        return {
            **await _state(root, runtime),
            "call_id": call_id,
            "published_result_digest": result_digest,
            "published_result_size": result_size,
        }

    if action == "collect":
        ref = await _recover_ref(runtime)
        outcome = await ModalMissionJobClient(_namespace(), runtime).poll(ref)
        if not isinstance(outcome, ModalMissionJobReady):
            raise RuntimeError(f"durable Mission job is not ready: {outcome!r}")
        result_digest = await _record_activity_result(activity_path, runtime, ref)
        if args.failpoint == "after-result-record":
            # ``record_orchestrated_result`` committed before this hard exit;
            # no settlement receipt has been supplied yet.
            os._exit(_RESULT_RECORD_CRASH)
        return {
            **await _state(root, runtime),
            "call_id": ref.call_id,
            "collected_result_digest": result_digest,
            "poll": "ready",
        }

    if action == "settle":
        ref = await _recover_ref(runtime)
        outcome = await ModalMissionJobClient(_namespace(), runtime).poll(ref)
        if not isinstance(outcome, ModalMissionJobReady):
            raise RuntimeError(f"durable Mission job is not ready: {outcome!r}")
        result_digest = await _record_activity_result(activity_path, runtime, ref)
        catalog = SqliteActivityCatalog(activity_path)
        coordinator = ActivityCoordinator(catalog)
        try:
            await coordinator.settle_observation(
                _source_receipt().world_id,
                _ACTIVITY_KIND,
                _ACTIVITY_ID,
                ActivitySettlement(
                    receipt=_observation_receipt(),
                    result_digest=result_digest,
                ),
            )
        finally:
            await catalog.close()
        return {
            **await _state(root, runtime),
            "call_id": ref.call_id,
            "poll": "ready",
            "settled_result_digest": result_digest,
        }

    if action == "inspect":
        return await _state(root, runtime)

    raise AssertionError(f"unsupported action {action!r}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("state_dir")
    parser.add_argument(
        "action",
        choices=("start", "poll", "publish", "collect", "settle", "inspect"),
    )
    parser.add_argument(
        "--failpoint",
        choices=("none", "after-call-identity", "after-result-record"),
        default="none",
    )
    args = parser.parse_args()
    payload = asyncio.run(_run(args))
    print(_canonical_json(payload))


if __name__ == "__main__":
    main()
