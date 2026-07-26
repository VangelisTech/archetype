# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Canonical, secret-safe values crossing Mission author durability boundaries."""

from __future__ import annotations

import json
from dataclasses import replace
from typing import Any, Protocol, runtime_checkable

from pydantic import TypeAdapter

from archetype.missions.activities import (
    AuthorExecutionObservation,
    DurableAuthorExecutionObservation,
)
from archetype.missions.coding_agents.contracts import (
    CommitObservation,
    FrictionObservation,
    TaskDispatchRequest,
    ValidationObservation,
)

AUTHOR_RESULT_MAX_BYTES = 512 * 1024
_RESULT_KIND = "result"
_SCHEMA_VERSION = 1
_REQUEST_ADAPTER = TypeAdapter(TaskDispatchRequest)
_RESULT_ADAPTER = TypeAdapter(DurableAuthorExecutionObservation)


@runtime_checkable
class AuthorValueRedactor(Protocol):
    """Small structural redaction port required by the family-owned codec."""

    @property
    def policy_id(self) -> str: ...

    def redact_text(self, value: str, *, scope: str) -> Any: ...

    def assert_safe_metadata(self, value: str, *, field: str) -> Any: ...


class MissionAuthorValueCodec:
    """Sanitize and canonically encode author requests and observations.

    The same codec is used before local value-store writes and before Modal
    provider-result writes. Free text is redacted before bounds are applied;
    structural identities are quarantined rather than rewritten.
    """

    def __init__(
        self,
        *,
        redactor: AuthorValueRedactor,
        max_result_bytes: int = AUTHOR_RESULT_MAX_BYTES,
    ) -> None:
        if not redactor.policy_id.strip():
            raise ValueError("author value redactor requires a policy identity")
        if max_result_bytes < 1:
            raise ValueError("author result byte limit must be positive")
        self._redactor = redactor
        self._max_result_bytes = max_result_bytes

    @property
    def redaction_policy_id(self) -> str:
        return self._redactor.policy_id

    @property
    def max_result_bytes(self) -> int:
        return self._max_result_bytes

    def sanitize_request(self, request: TaskDispatchRequest) -> TaskDispatchRequest:
        """Remove free-text secrets and reject unsafe structural identities."""

        for field, value in (
            ("author.request.dispatch_id", request.dispatch_id),
            ("author.request.repository", request.repository),
            ("author.request.branch", request.branch),
            ("author.request.base_ref", request.base_ref),
            ("author.request.task_base_revision", request.task_base_revision),
            (
                "author.request.previous_agent_session_id",
                request.previous_agent_session_id,
            ),
        ):
            self._safe(value, field)

        value = _REQUEST_ADAPTER.dump_python(request, mode="json")

        def visit(item: Any, path: tuple[str, ...]) -> Any:
            if isinstance(item, str):
                scope = "mission-author-request:" + ".".join(path)
                return self._redactor.redact_text(item, scope=scope).text
            if isinstance(item, list):
                return [visit(child, (*path, str(position))) for position, child in enumerate(item)]
            if isinstance(item, dict):
                return {key: visit(child, (*path, str(key))) for key, child in item.items()}
            return item

        return _REQUEST_ADAPTER.validate_python(visit(value, ("request",)))

    def sanitize_observation(
        self,
        observation: AuthorExecutionObservation,
    ) -> DurableAuthorExecutionObservation:
        """Return the bounded durable form of one raw provider observation."""

        result = observation.result
        dispatch_id = self._safe(result.dispatch_id, "author.dispatch_id")
        scope = f"mission:{result.mission_id}:author:{dispatch_id}"
        sandbox = result.sandbox
        safe_sandbox = replace(
            sandbox,
            provider=self._safe(sandbox.provider, "author.sandbox.provider"),
            sandbox_id=self._safe(sandbox.sandbox_id, "author.sandbox.id"),
            environment=self._safe(
                sandbox.environment,
                "author.sandbox.environment",
            ),
        )

        validation = tuple(
            ValidationObservation(
                validator_id=item.validator_id,
                name=self._redact(item.name, 1_000, f"{scope}:validator-name"),
                command=tuple(
                    self._redact(
                        argument,
                        2_000,
                        f"{scope}:validator-command:{position}",
                    )
                    for position, argument in enumerate(item.command)
                ),
                expected_returncode=item.expected_returncode,
                actual_returncode=item.actual_returncode,
                revision=self._safe(item.revision, "author.validation.revision"),
                stdout=self._redact(
                    item.stdout,
                    4_000,
                    f"{scope}:validator-stdout",
                ),
                stderr=self._redact(
                    item.stderr,
                    4_000,
                    f"{scope}:validator-stderr",
                ),
            )
            for item in result.validation
        )
        commits = tuple(
            CommitObservation(
                sha=self._safe(item.sha, "author.commit.sha"),
                message=self._redact(
                    item.message,
                    4_000,
                    f"{scope}:commit-message",
                ),
                branch=self._safe(item.branch, "author.commit.branch"),
                pushed=item.pushed,
                final_revision=item.final_revision,
            )
            for item in result.commits
        )
        friction = tuple(
            FrictionObservation(
                kind=self._safe(item.kind, "author.friction.kind"),
                message=self._redact(
                    item.message,
                    4_000,
                    f"{scope}:friction",
                ),
            )
            for item in result.friction
        )
        sanitized = replace(
            result,
            dispatch_id=dispatch_id,
            sandbox=safe_sandbox,
            worktree=self._safe(result.worktree, "author.worktree"),
            agent_session_id=self._safe(
                result.agent_session_id,
                "author.agent_session_id",
            ),
            starting_revision=self._safe(
                result.starting_revision,
                "author.starting_revision",
            ),
            final_revision=self._safe(
                result.final_revision,
                "author.final_revision",
            ),
            diff_digest=self._safe(result.diff_digest, "author.diff_digest"),
            validator_bundle_digest=self._safe(
                result.validator_bundle_digest,
                "author.validator_bundle_digest",
            ),
            agent_stdout=self._redact(
                result.agent_stdout,
                16_000,
                f"{scope}:agent-stdout",
            ),
            agent_stderr=self._redact(
                result.agent_stderr,
                16_000,
                f"{scope}:agent-stderr",
            ),
            trace_uri=self._safe(result.trace_uri, "author.trace_uri"),
            validation=validation,
            commits=commits,
            friction=friction,
            error=self._redact(result.error, 4_000, f"{scope}:error"),
        )
        durable = DurableAuthorExecutionObservation(
            result=sanitized,
            sandbox_status=observation.sandbox_status,
            redaction_policy_id=self._redactor.policy_id,
            bind_mission=observation.bind_mission,
        )
        self.encode_observation(durable)
        return durable

    def encode_observation(
        self,
        observation: DurableAuthorExecutionObservation,
    ) -> bytes:
        """Return the bounded canonical result envelope used by every store."""

        encoded = json.dumps(
            {
                "kind": _RESULT_KIND,
                "schema_version": _SCHEMA_VERSION,
                "value": _RESULT_ADAPTER.dump_python(observation, mode="json"),
            },
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        if len(encoded) > self._max_result_bytes:
            raise ValueError(
                f"author activity result exceeds the {self._max_result_bytes}-byte durability limit"
            )
        return encoded

    def decode_observation(
        self,
        encoded: bytes,
    ) -> DurableAuthorExecutionObservation:
        """Decode only the exact canonical result envelope."""

        if len(encoded) > self._max_result_bytes:
            raise ValueError(
                f"author activity result exceeds the {self._max_result_bytes}-byte durability limit"
            )
        try:
            envelope = json.loads(encoded)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("author activity result is not canonical JSON") from exc
        if (
            not isinstance(envelope, dict)
            or set(envelope) != {"kind", "schema_version", "value"}
            or envelope.get("schema_version") != _SCHEMA_VERSION
            or envelope.get("kind") != _RESULT_KIND
        ):
            raise ValueError("author activity result has an incompatible envelope")
        observation = _RESULT_ADAPTER.validate_python(envelope["value"])
        if self.encode_observation(observation) != encoded:
            raise ValueError("author activity result is not canonically encoded")
        return observation

    @staticmethod
    def execution_observation(
        durable: DurableAuthorExecutionObservation,
    ) -> AuthorExecutionObservation:
        """Return the already-redacted provider value expected by the worker."""

        return AuthorExecutionObservation(
            result=durable.result,
            sandbox_status=durable.sandbox_status,
            bind_mission=durable.bind_mission,
        )

    def _redact(self, value: str, limit: int, scope: str) -> str:
        if not value:
            return ""
        return self._redactor.redact_text(value, scope=scope).text[-limit:]

    def _safe(self, value: str, field: str, *, limit: int = 4_096) -> str:
        if len(value) > limit:
            raise ValueError(f"{field} must be at most {limit} characters")
        if value:
            self._redactor.assert_safe_metadata(value, field=field)
        return value


__all__ = [
    "AUTHOR_RESULT_MAX_BYTES",
    "AuthorValueRedactor",
    "MissionAuthorValueCodec",
]
