# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Optional Agent Missions read-projection and MissionRun control routers."""

from typing import Annotated, Any, Literal, cast

from daft import DataFrame, Expression, col
from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, status
from pydantic import BaseModel, ConfigDict, Field, field_validator

from archetype.api.deps import get_actor_ctx, get_dispatcher, get_mission_principal
from archetype.api.errors import raise_api_error
from archetype.api.models import dataframe_to_rows
from archetype.api.principals import MissionPrincipal
from archetype.commands.dispatch import CommandDispatcher
from archetype.commands.models import ActorCtx
from archetype.core.component import Component
from archetype.errors import PayloadRejectedError
from archetype.missions.components import (
    AgentExecution,
    Mission,
    MissionState,
    Task,
    TaskDispatch,
    TaskPolicy,
    TaskState,
    TaskValidator,
    ValidationResult,
)
from archetype.missions.authorization import (
    MISSION_CAPABILITY,
    MissionAuthorizer,
    require_capability,
    require_run_access,
)
from archetype.missions.config import installed_execution_profiles
from archetype.missions.contracts import (
    AgentTask,
    CommandValidator,
    MissionSubmission,
)
from archetype.missions.execution_profiles import (
    ExecutionProfile,
    ExecutionProfileCatalog,
    MissionProfileRequest,
)
from archetype.missions.models import (
    AcceptMissionRun,
    CancelMissionRun,
    GetMissionRun,
    GetMissionRunEvents,
    ListMissionRuns,
)
from archetype.missions.relations import DependsOn, Guards, PartOfMission
from archetype.missions.run_contracts import (
    MISSION_RUN_EVENT_MAX_PAGE,
    MissionRun,
    MissionRunEvent,
    MissionRunRequest,
)
from archetype.redaction import RedactionService
from archetype.world.models import ComponentTypeRef, GetWorldInfo, QueryComponents

_TASK_TYPES: list[type[Component]] = [Task, TaskState, TaskDispatch, TaskPolicy]

# One stable process owner for every REST-dispatched MissionRun operation, so
# an API restart resumes the same durable catalog under the same reservation.
_MISSION_RUN_OWNER = "mission-control:runs"
_MISSION_RUN_OPERATION_NAME = "mission-run-control"

# Request bounds owned by this surface. Profile-owned bounds (validator count
# and timeout ceilings) are enforced against the resolved execution profile.
_MAX_RUN_TASKS = 32
_MAX_PROMPT_BYTES = 65_536
_MAX_VALIDATOR_ARGV = 64
_MAX_ARGV_CHARS = 4_096
_MAX_TEXT_CHARS = 512
_MAX_NAME_CHARS = 128
_MAX_CANCEL_REASON_CHARS = 512
_MAX_RESULT_REASON_CHARS = 4_096
_MAX_IDEMPOTENCY_KEY_CHARS = 255
_MAX_COMMIT_SHAS = 64


class MissionRunLimitError(PayloadRejectedError):
    """A mission-run request exceeds a profile-owned execution bound."""

    public_detail = "Mission request exceeds its execution-profile bounds"


class _FrozenRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


# Projection-time redaction is defense in depth: durable reason text is
# already redacted at its write sites, and this deterministic default-policy
# scanner keeps client-echoed and legacy text credential-free on the wire.
_PROJECTION_REDACTION = RedactionService()
_PROJECTION_REDACTION_SCOPE = "mission-run-projection"


def _redacted_text(value: str, limit: int) -> str:
    if not value:
        return value
    return _PROJECTION_REDACTION.redact_text(
        value,
        scope=_PROJECTION_REDACTION_SCOPE,
    ).text[:limit]


async def get_execution_profiles(request: Request) -> ExecutionProfileCatalog:
    """Resolve the server-owned execution-profile catalog from lifespan state.

    Route handlers depend on this to turn a client ``profile_id`` into the
    catalog's ``ExecutionProfileBinding`` and then a live ``build_config()``;
    the catalog itself was bound through ``world_library_configs`` at wiring.
    """

    installed = request.app.state.resources.world_library("missions")
    return installed_execution_profiles(installed)


async def _components_frame(
    dispatcher: CommandDispatcher,
    ctx: ActorCtx,
    world_id: str,
    component_types: list[type[Component]],
    *,
    entity_ids: list[int] | None = None,
) -> DataFrame | None:
    try:
        info = await dispatcher.apply_as(ctx, GetWorldInfo(world_id=world_id))
        query_world_id, run_id = str(info.world_id), str(info.run_id or "")
    except KeyError:
        query_world_id, run_id = str(world_id), ""
    try:
        return await dispatcher.apply_as(
            ctx,
            QueryComponents(
                components=tuple(
                    ComponentTypeRef.from_type(component_type) for component_type in component_types
                ),
                world_id=query_world_id,
                run_id=run_id,
                entity_ids=tuple(entity_ids) if entity_ids is not None else None,
            ),
        )
    except KeyError:
        # A world that has never spawned this component table is a normal
        # state (no DependsOn edges yet, no executions yet): read as empty.
        return None


def _edge_rows(
    frame: DataFrame | None,
    relation: type[Component],
    *,
    where: Expression | None = None,
) -> list[dict[str, Any]]:
    """Serialize relation rows to unprefixed ``source``/``target`` pairs."""

    if frame is None:
        return []
    if where is not None:
        frame = frame.where(where)
    prefix = relation.get_prefix()
    rows = dataframe_to_rows(frame)
    return [{"source": row[f"{prefix}source"], "target": row[f"{prefix}target"]} for row in rows]


async def list_missions(
    world_id: str,
    dispatcher: Annotated[CommandDispatcher, Depends(get_dispatcher)],
    ctx: Annotated[ActorCtx, Depends(get_actor_ctx)],
):
    """List mission entities with their current rollup state."""

    try:
        frame = await _components_frame(
            dispatcher,
            ctx,
            world_id,
            [Mission, MissionState],
        )
        return [] if frame is None else dataframe_to_rows(frame)
    except Exception as exc:
        raise_api_error(exc)


async def get_mission_tasks(
    world_id: str,
    mission_id: int,
    dispatcher: Annotated[CommandDispatcher, Depends(get_dispatcher)],
    ctx: Annotated[ActorCtx, Depends(get_actor_ctx)],
):
    """Project one mission's task DAG and dependency edges."""

    try:
        membership = await _components_frame(
            dispatcher,
            ctx,
            world_id,
            [PartOfMission],
        )
        edges = _edge_rows(
            membership,
            PartOfMission,
            where=cast(Expression, col(f"{PartOfMission.get_prefix()}target") == mission_id),
        )
        task_ids = [edge["source"] for edge in edges]
        if not task_ids:
            return {"mission_id": mission_id, "tasks": [], "depends_on": []}

        task_frame = await _components_frame(
            dispatcher,
            ctx,
            world_id,
            _TASK_TYPES,
            entity_ids=task_ids,
        )
        dependency_frame = await _components_frame(
            dispatcher,
            ctx,
            world_id,
            [DependsOn],
        )
        depends_on = _edge_rows(
            dependency_frame,
            DependsOn,
            where=col(f"{DependsOn.get_prefix()}source").is_in(task_ids),
        )
        return {
            "mission_id": mission_id,
            "tasks": [] if task_frame is None else dataframe_to_rows(task_frame),
            "depends_on": depends_on,
        }
    except Exception as exc:
        raise_api_error(exc)


async def get_task_card(
    world_id: str,
    task_id: int,
    dispatcher: Annotated[CommandDispatcher, Depends(get_dispatcher)],
    ctx: Annotated[ActorCtx, Depends(get_actor_ctx)],
):
    """Project one task's state, validators, executions, and results."""

    try:
        task_frame = await _components_frame(
            dispatcher,
            ctx,
            world_id,
            _TASK_TYPES,
            entity_ids=[task_id],
        )
        task_rows = [] if task_frame is None else dataframe_to_rows(task_frame)
        if not task_rows:
            raise KeyError(f"task {task_id} not found")

        guard_frame = await _components_frame(
            dispatcher,
            ctx,
            world_id,
            [Guards],
        )
        guard_edges = _edge_rows(
            guard_frame,
            Guards,
            where=cast(Expression, col(f"{Guards.get_prefix()}target") == task_id),
        )
        validator_ids = [edge["source"] for edge in guard_edges]
        validators: list[dict[str, Any]] = []
        if validator_ids:
            validator_frame = await _components_frame(
                dispatcher,
                ctx,
                world_id,
                [TaskValidator],
                entity_ids=validator_ids,
            )
            if validator_frame is not None:
                validators = dataframe_to_rows(validator_frame)

        executions: list[dict[str, Any]] = []
        execution_frame = await _components_frame(
            dispatcher,
            ctx,
            world_id,
            [AgentExecution],
        )
        if execution_frame is not None:
            executions = dataframe_to_rows(
                execution_frame.where(
                    cast(
                        Expression,
                        col(f"{AgentExecution.get_prefix()}task_id") == task_id,
                    )
                )
            )

        validations: list[dict[str, Any]] = []
        validation_frame = await _components_frame(
            dispatcher,
            ctx,
            world_id,
            [ValidationResult],
        )
        if validation_frame is not None:
            validations = dataframe_to_rows(
                validation_frame.where(
                    cast(
                        Expression,
                        col(f"{ValidationResult.get_prefix()}task_id") == task_id,
                    )
                )
            )

        return {
            "task": task_rows,
            "validators": validators,
            "executions": executions,
            "validations": validations,
        }
    except Exception as exc:
        raise_api_error(exc)


class MissionRunValidatorRequest(_FrozenRequest):
    """One bounded command validator; execution authority stays host-owned."""

    name: str = Field(min_length=1, max_length=_MAX_NAME_CHARS)
    command: list[str] = Field(min_length=1, max_length=_MAX_VALIDATOR_ARGV)
    expected_returncode: int = 0
    timeout_seconds: int = Field(default=300, ge=1)

    @field_validator("command")
    @classmethod
    def _bounded_arguments(cls, value: list[str]) -> list[str]:
        for argument in value:
            if len(argument) > _MAX_ARGV_CHARS:
                raise ValueError(
                    f"validator arguments must be at most {_MAX_ARGV_CHARS} characters"
                )
        return value


class MissionRunTaskRequest(_FrozenRequest):
    """One explicitly authored task with bounded prompt and validators."""

    name: str = Field(min_length=1, max_length=_MAX_NAME_CHARS)
    prompt: str = Field(min_length=1)
    validators: list[MissionRunValidatorRequest] = Field(min_length=1)
    depends_on: list[str] = Field(default_factory=list, max_length=_MAX_RUN_TASKS)
    max_dispatches: int = Field(default=3, ge=1, le=10)

    @field_validator("prompt")
    @classmethod
    def _bounded_prompt(cls, value: str) -> str:
        if len(value.encode("utf-8")) > _MAX_PROMPT_BYTES:
            raise ValueError(f"task prompts must be at most {_MAX_PROMPT_BYTES} bytes")
        return value


class MissionRunSubmitRequest(_FrozenRequest):
    """Client-owned mission-run coordinates and an explicit bounded task DAG.

    Sandbox, secret, driver, model, critic, publication-credential, and any
    other host execution choice cannot ride this body; ``extra="forbid"``
    rejects unknown fields and the selected profile owns execution authority.
    """

    profile_id: str = Field(min_length=1, max_length=_MAX_TEXT_CHARS)
    repository: str = Field(min_length=1, max_length=_MAX_TEXT_CHARS)
    branch: str = Field(min_length=1, max_length=_MAX_TEXT_CHARS)
    base_ref: str = Field(default="main", min_length=1, max_length=_MAX_TEXT_CHARS)
    name: str = Field(default="agent-mission", min_length=1, max_length=_MAX_NAME_CHARS)
    tasks: list[MissionRunTaskRequest] = Field(min_length=1, max_length=_MAX_RUN_TASKS)


class MissionRunCancelRequest(_FrozenRequest):
    """Optional bounded cancellation reason."""

    reason: str = Field(default="", max_length=_MAX_CANCEL_REASON_CHARS)


class MissionRunProfileResponse(_FrozenRequest):
    """Pinned execution-profile identity recorded on the durable run."""

    profile_id: str
    version: str
    digest: str


MissionRunState = Literal[
    "accepted",
    "running",
    "succeeded",
    "failed",
    "cancelling",
    "cancelled",
    "interrupted",
]


class MissionRunAcceptedResponse(_FrozenRequest):
    """202 admission projection: identity and digests, never host internals."""

    run_id: str
    state: MissionRunState
    request_digest: str
    profile: MissionRunProfileResponse
    status_url: str


class MissionRunStatusResponse(_FrozenRequest):
    """Bounded run projection: no component tables, no provider internals."""

    run_id: str
    state: MissionRunState
    profile: MissionRunProfileResponse
    request_digest: str
    world_id: str
    mission_id: int | None
    episode_id: str
    cancellation_requested: bool
    cancellation_reason: str
    cleanup_state: str
    interrupted_reason: str
    accepted_at_ms: int
    running_at_ms: int | None
    terminal_at_ms: int | None
    updated_at_ms: int


class MissionRunListResponse(_FrozenRequest):
    """One bounded newest-first page of the caller's own durable runs."""

    runs: list[MissionRunStatusResponse]


class MissionRunEventResponse(_FrozenRequest):
    """One ordered durable progress event with a deterministic identity."""

    event_id: str
    cursor: int
    schema_version: int
    event_type: str
    phase: str
    created_at_ms: int
    payload: dict[str, Any]


class MissionRunEventPageResponse(_FrozenRequest):
    """One bounded cursor page; replay from ``next_after`` has no gaps."""

    run_id: str
    after: int
    next_after: int
    events: list[MissionRunEventResponse]


class MissionRunTaskResultResponse(_FrozenRequest):
    """One bounded terminal task fact."""

    task_id: int
    name: str
    status: str
    dispatches: int
    commit_shas: list[str] = Field(max_length=_MAX_COMMIT_SHAS)
    reason: str


class MissionRunResultDetailResponse(_FrozenRequest):
    """The bounded governed MissionResult evidence."""

    mission_id: int
    episode_id: str
    status: str
    repository: str
    branch: str
    ticks_completed: int
    reason: str
    tasks: list[MissionRunTaskResultResponse]


class MissionRunResultResponse(_FrozenRequest):
    """One immutable terminal outcome; absent evidence stays absent."""

    run_id: str
    state: MissionRunState
    result: MissionRunResultDetailResponse | None
    interrupted_reason: str


def _run_profile_response(run: MissionRun) -> MissionRunProfileResponse:
    return MissionRunProfileResponse(
        profile_id=run.profile.profile_id,
        version=run.profile.version,
        digest=run.profile.digest,
    )


def _run_status_response(run: MissionRun) -> MissionRunStatusResponse:
    return MissionRunStatusResponse(
        run_id=run.run_id,
        state=run.status.value,
        profile=_run_profile_response(run),
        request_digest=run.request_digest,
        world_id=run.world_id,
        mission_id=run.mission_id,
        episode_id=run.episode_id,
        cancellation_requested=run.cancellation_intent,
        cancellation_reason=_redacted_text(run.cancellation_reason, _MAX_CANCEL_REASON_CHARS),
        cleanup_state=run.cleanup_state.value,
        interrupted_reason=_redacted_text(run.interrupted_reason, _MAX_RESULT_REASON_CHARS),
        accepted_at_ms=run.accepted_at_ms,
        running_at_ms=run.running_at_ms,
        terminal_at_ms=run.terminal_at_ms,
        updated_at_ms=run.updated_at_ms,
    )


def _run_result_response(run: MissionRun) -> MissionRunResultResponse:
    detail: MissionRunResultDetailResponse | None = None
    if run.result is not None:
        detail = MissionRunResultDetailResponse(
            mission_id=run.result.mission_id,
            episode_id=run.result.episode_id,
            status=run.result.status,
            repository=run.result.repository,
            branch=run.result.branch,
            ticks_completed=run.result.ticks_completed,
            reason=_redacted_text(run.result.reason, _MAX_RESULT_REASON_CHARS),
            tasks=[
                MissionRunTaskResultResponse(
                    task_id=task.task_id,
                    name=task.name,
                    status=task.status,
                    dispatches=task.dispatches,
                    commit_shas=list(task.commit_shas[:_MAX_COMMIT_SHAS]),
                    reason=_redacted_text(task.reason, _MAX_RESULT_REASON_CHARS),
                )
                for task in run.result.tasks
            ],
        )
    return MissionRunResultResponse(
        run_id=run.run_id,
        state=run.status.value,
        result=detail,
        interrupted_reason=_redacted_text(run.interrupted_reason, _MAX_RESULT_REASON_CHARS),
    )


def _submission_from_request(
    req: MissionRunSubmitRequest,
    profile: ExecutionProfile,
) -> MissionSubmission:
    """Translate the bounded wire body into the missions submission value.

    Structural bounds are pydantic-enforced; profile-owned validator ceilings
    fail closed here. Publication policy comes from the profile, never the
    request body.
    """

    tasks: list[AgentTask] = []
    for task in req.tasks:
        if len(task.validators) > profile.max_validators_per_task:
            raise MissionRunLimitError(
                f"task {task.name!r} exceeds the profile validator count bound"
            )
        validators: list[CommandValidator] = []
        for validator in task.validators:
            if validator.timeout_seconds > profile.max_validator_timeout_seconds:
                raise MissionRunLimitError(
                    f"validator {validator.name!r} exceeds the profile timeout bound"
                )
            validators.append(
                CommandValidator(
                    name=validator.name,
                    command=tuple(validator.command),
                    expected_returncode=validator.expected_returncode,
                    timeout_seconds=validator.timeout_seconds,
                )
            )
        tasks.append(
            AgentTask(
                name=task.name,
                prompt=task.prompt,
                validators=tuple(validators),
                depends_on=tuple(task.depends_on),
                max_dispatches=task.max_dispatches,
                publication_policy=profile.publication_policy,
            )
        )
    return MissionSubmission(
        repository=req.repository,
        branch=req.branch,
        tasks=tuple(tasks),
        name=req.name,
        base_ref=req.base_ref,
    )


class _DurableRunAccess:
    """Project one durable MissionRun into the Missions authorization facts.

    The durable record is the sole ownership authority. It carries no grant
    store yet, so the granted-principal set is honestly empty; a future grant
    fact extends this projection without changing the policy call.
    """

    __slots__ = ("granted_principal_ids", "owner_principal_id")

    def __init__(self, run: MissionRun) -> None:
        self.owner_principal_id = run.principal
        self.granted_principal_ids: frozenset[str] = frozenset()


async def _authorized_run(
    dispatcher: CommandDispatcher,
    principal: MissionPrincipal,
    run_id: str,
    *,
    capability: str,
) -> MissionRun:
    """Check the capability, load the durable run, and check ownership."""

    require_capability(principal, capability)
    run = await dispatcher.apply(
        GetMissionRun(
            owner_id=_MISSION_RUN_OWNER,
            name=_MISSION_RUN_OPERATION_NAME,
            run_id=run_id,
        )
    )
    require_run_access(principal, cast(Any, _DurableRunAccess(run)))
    return run


def _require_profile_allows_cancel(catalog: ExecutionProfileCatalog, run: MissionRun) -> None:
    """Fail closed unless the exact pinned profile version permits cancel."""

    try:
        binding = catalog.resolve(
            run.profile.profile_id,
            version=run.profile.version,
            digest=run.profile.digest,
        )
    except (KeyError, ValueError):
        raise PermissionError("Permission denied") from None
    if not binding.profile.allow_cancel:
        raise PermissionError("Permission denied")


async def submit_mission_run(
    req: MissionRunSubmitRequest,
    principal: Annotated[MissionPrincipal, Depends(get_mission_principal)],
    profiles: Annotated[ExecutionProfileCatalog, Depends(get_execution_profiles)],
    dispatcher: Annotated[CommandDispatcher, Depends(get_dispatcher)],
    idempotency_key: Annotated[
        str,
        Header(
            alias="Idempotency-Key",
            min_length=1,
            max_length=_MAX_IDEMPOTENCY_KEY_CHARS,
            description="Caller-owned idempotency identity for this submission.",
        ),
    ],
) -> MissionRunAcceptedResponse:
    """Accept one durable MissionRun under a pinned server-owned profile.

    The same principal, key, and canonical digest return the original run;
    a changed digest under the same key returns 409. The handler dispatches
    the registered ``accept_mission_run`` operation and never constructs
    Mission ECS state, opens a runtime handle, or supervises execution.
    """

    try:
        binding = MissionAuthorizer(profiles).submit(
            principal,
            MissionProfileRequest(
                profile_id=req.profile_id,
                repository=req.repository,
                branch=req.branch,
                base_ref=req.base_ref,
            ),
        )
        pinned = binding.identity
        submission = _submission_from_request(req, binding.profile)
        run = await dispatcher.apply(
            AcceptMissionRun(
                owner_id=_MISSION_RUN_OWNER,
                name=req.name,
                request=MissionRunRequest(
                    principal=principal.principal_id,
                    idempotency_key=idempotency_key,
                    submission=submission,
                ),
                profile_id=pinned.profile_id,
                profile_version=pinned.version,
                profile_digest=pinned.digest,
            )
        )
        return MissionRunAcceptedResponse(
            run_id=run.run_id,
            state=run.status.value,
            request_digest=run.request_digest,
            profile=_run_profile_response(run),
            status_url=f"/v1/mission-runs/{run.run_id}",
        )
    except Exception as exc:
        raise_api_error(exc)


async def list_mission_runs(
    principal: Annotated[MissionPrincipal, Depends(get_mission_principal)],
    dispatcher: Annotated[CommandDispatcher, Depends(get_dispatcher)],
    limit: Annotated[
        int,
        Query(ge=1, le=MISSION_RUN_EVENT_MAX_PAGE, description="Bounded page size."),
    ] = 100,
) -> MissionRunListResponse:
    """Project one bounded page of the caller's own durable runs.

    Issue #809 documents no list route; the merged MCP adapter contract
    (#820, ``mission_list``) requires a principal-scoped GET collection, so
    the page is filtered to runs the caller owns — never another
    principal's — and stays a pure read: no supervision resumes here.
    """

    try:
        require_capability(principal, MISSION_CAPABILITY["read"])
        runs: tuple[MissionRun, ...] = await dispatcher.apply(
            ListMissionRuns(
                owner_id=_MISSION_RUN_OWNER,
                name=_MISSION_RUN_OPERATION_NAME,
                principal=principal.principal_id,
                limit=limit,
            )
        )
        return MissionRunListResponse(runs=[_run_status_response(run) for run in runs])
    except Exception as exc:
        raise_api_error(exc)


async def get_mission_run(
    run_id: str,
    principal: Annotated[MissionPrincipal, Depends(get_mission_principal)],
    dispatcher: Annotated[CommandDispatcher, Depends(get_dispatcher)],
) -> MissionRunStatusResponse:
    """Project one durable MissionRun the principal owns or was granted."""

    try:
        run = await _authorized_run(
            dispatcher,
            principal,
            run_id,
            capability=MISSION_CAPABILITY["read"],
        )
        return _run_status_response(run)
    except Exception as exc:
        raise_api_error(exc)


async def get_mission_run_events(
    run_id: str,
    principal: Annotated[MissionPrincipal, Depends(get_mission_principal)],
    dispatcher: Annotated[CommandDispatcher, Depends(get_dispatcher)],
    after: Annotated[
        int,
        Query(ge=0, description="Replay strictly after this run-local cursor."),
    ] = 0,
    limit: Annotated[
        int,
        Query(ge=1, le=MISSION_RUN_EVENT_MAX_PAGE, description="Bounded page size."),
    ] = 100,
) -> MissionRunEventPageResponse:
    """Return one ordered durable event page with no gaps or duplicates."""

    try:
        await _authorized_run(
            dispatcher,
            principal,
            run_id,
            capability=MISSION_CAPABILITY["read"],
        )
        events: tuple[MissionRunEvent, ...] = await dispatcher.apply(
            GetMissionRunEvents(
                owner_id=_MISSION_RUN_OWNER,
                name=_MISSION_RUN_OPERATION_NAME,
                run_id=run_id,
                after=after,
                limit=limit,
            )
        )
        return MissionRunEventPageResponse(
            run_id=run_id,
            after=after,
            next_after=events[-1].cursor if events else after,
            events=[
                MissionRunEventResponse(
                    event_id=event.event_id,
                    cursor=event.cursor,
                    schema_version=event.schema_version,
                    event_type=event.event_type,
                    phase=event.phase,
                    created_at_ms=event.created_at_ms,
                    payload=dict(event.payload),
                )
                for event in events
            ],
        )
    except Exception as exc:
        raise_api_error(exc)


async def get_mission_run_result(
    run_id: str,
    principal: Annotated[MissionPrincipal, Depends(get_mission_principal)],
    dispatcher: Annotated[CommandDispatcher, Depends(get_dispatcher)],
) -> MissionRunResultResponse:
    """Return the one immutable bounded terminal result, or 425 while open."""

    try:
        run = await _authorized_run(
            dispatcher,
            principal,
            run_id,
            capability=MISSION_CAPABILITY["read"],
        )
    except Exception as exc:
        raise_api_error(exc)
    if not run.terminal:
        raise HTTPException(
            status_code=status.HTTP_425_TOO_EARLY,
            detail="Mission run has not reached a terminal result",
        )
    return _run_result_response(run)


async def cancel_mission_run(
    run_id: str,
    principal: Annotated[MissionPrincipal, Depends(get_mission_principal)],
    profiles: Annotated[ExecutionProfileCatalog, Depends(get_execution_profiles)],
    dispatcher: Annotated[CommandDispatcher, Depends(get_dispatcher)],
    body: MissionRunCancelRequest | None = None,
) -> MissionRunStatusResponse:
    """Durably record cancellation intent before reporting acceptance.

    Repeated requests are idempotent; ``cancelling`` stays distinct from
    ``cancelled``, and a completion race resolves to the committed execution
    fact through the registered ``cancel_mission_run`` operation.
    """

    try:
        run = await _authorized_run(
            dispatcher,
            principal,
            run_id,
            capability=MISSION_CAPABILITY["cancel"],
        )
        _require_profile_allows_cancel(profiles, run)
        cancelled = await dispatcher.apply(
            CancelMissionRun(
                owner_id=_MISSION_RUN_OWNER,
                name=_MISSION_RUN_OPERATION_NAME,
                run_id=run.run_id,
                reason=(
                    _redacted_text(body.reason, _MAX_CANCEL_REASON_CHARS)
                    if body is not None
                    else ""
                ),
            )
        )
        return _run_status_response(cancelled)
    except Exception as exc:
        raise_api_error(exc)


_MISSION_RUN_AUTH_RESPONSES: dict[int | str, dict[str, Any]] = {
    401: {"description": "Authentication required"},
    403: {"description": "Permission denied"},
}
_MISSION_RUN_LOOKUP_RESPONSES: dict[int | str, dict[str, Any]] = {
    **_MISSION_RUN_AUTH_RESPONSES,
    404: {"description": "Mission run not found"},
}
_MISSION_RUN_SUBMIT_RESPONSES: dict[int | str, dict[str, Any]] = {
    **_MISSION_RUN_AUTH_RESPONSES,
    400: {"description": "Malformed mission request"},
    404: {"description": "Execution profile not configured"},
    409: {"description": "Idempotency key reused with a different request digest"},
    422: {"description": "Request exceeds a structural or profile-owned bound"},
}
_MISSION_RUN_RESULT_RESPONSES: dict[int | str, dict[str, Any]] = {
    **_MISSION_RUN_LOOKUP_RESPONSES,
    425: {"description": "Mission run has not reached a terminal result"},
}
_MISSION_RUN_CANCEL_RESPONSES: dict[int | str, dict[str, Any]] = {
    **_MISSION_RUN_LOOKUP_RESPONSES,
    409: {"description": "Mission run changed concurrently"},
}


def create_router() -> APIRouter:
    """Create one fresh Missions router for an API host installation."""

    router = APIRouter(tags=["missions"])
    router.add_api_route(
        "/worlds/{world_id}/missions",
        list_missions,
        methods=["GET"],
        response_model=list[dict[str, Any]],
    )
    router.add_api_route(
        "/worlds/{world_id}/missions/{mission_id}/tasks",
        get_mission_tasks,
        methods=["GET"],
        response_model=dict[str, Any],
    )
    router.add_api_route(
        "/worlds/{world_id}/tasks/{task_id}",
        get_task_card,
        methods=["GET"],
        response_model=dict[str, Any],
    )
    router.add_api_route(
        "/v1/mission-runs",
        submit_mission_run,
        methods=["POST"],
        response_model=MissionRunAcceptedResponse,
        status_code=status.HTTP_202_ACCEPTED,
        responses=_MISSION_RUN_SUBMIT_RESPONSES,
    )
    router.add_api_route(
        "/v1/mission-runs",
        list_mission_runs,
        methods=["GET"],
        response_model=MissionRunListResponse,
        responses=_MISSION_RUN_AUTH_RESPONSES,
    )
    router.add_api_route(
        "/v1/mission-runs/{run_id}",
        get_mission_run,
        methods=["GET"],
        response_model=MissionRunStatusResponse,
        responses=_MISSION_RUN_LOOKUP_RESPONSES,
    )
    router.add_api_route(
        "/v1/mission-runs/{run_id}/events",
        get_mission_run_events,
        methods=["GET"],
        response_model=MissionRunEventPageResponse,
        responses=_MISSION_RUN_LOOKUP_RESPONSES,
    )
    router.add_api_route(
        "/v1/mission-runs/{run_id}/result",
        get_mission_run_result,
        methods=["GET"],
        response_model=MissionRunResultResponse,
        responses=_MISSION_RUN_RESULT_RESPONSES,
    )
    router.add_api_route(
        "/v1/mission-runs/{run_id}/cancel",
        cancel_mission_run,
        methods=["POST"],
        response_model=MissionRunStatusResponse,
        status_code=status.HTTP_202_ACCEPTED,
        responses=_MISSION_RUN_CANCEL_RESPONSES,
    )
    return router


# Compatibility for direct imports during the 0.6 migration. API hosting uses
# the manifest factory so each host receives its own router instance.
router = create_router()


__all__ = ["create_router", "get_execution_profiles", "router"]
