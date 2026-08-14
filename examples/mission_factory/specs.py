# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Trusted authoring values for the first mission-factory asset kit."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class BehaviorSpec:
    authority: str
    observes: tuple[str, ...]
    effect: str


@dataclass(frozen=True, slots=True)
class StateSpec:
    signal: str
    visual_state: str
    animation_clip: str
    priority: int


@dataclass(frozen=True, slots=True)
class InteractionSpec:
    name: str
    permission: str
    action: str
    confirmation_required: bool = False


@dataclass(frozen=True, slots=True)
class VisualAssetSpec:
    key: str
    display_name: str
    collection: str
    footprint: tuple[int, int]
    dimensions_m: tuple[float, float, float]
    max_triangles: int
    prompt: str
    sockets: tuple[tuple[str, str], ...]
    behaviors: tuple[BehaviorSpec, ...]
    states: tuple[StateSpec, ...]
    interactions: tuple[InteractionSpec, ...] = ()


@dataclass(frozen=True, slots=True)
class BlueprintNodeSpec:
    key: str
    role: str
    visual_asset: str
    order: int


@dataclass(frozen=True, slots=True)
class TaskSpec:
    key: str
    prompt_template: str
    max_dispatches: int = 3
    critic_max_reviews: int = 2
    critic_timeout_seconds: int = 2700


@dataclass(frozen=True, slots=True)
class ValidatorSpec:
    key: str
    task_key: str
    name: str
    command: tuple[str, ...]
    expected_returncode: int = 0
    timeout_seconds: int = 900


@dataclass(frozen=True, slots=True)
class RuleSpec:
    relation: str
    source_slot: str
    target_slot: str


_STYLE = (
    "Cohesive low-poly industrial science-fiction machine for an isometric "
    "factory-management game, clean space-colony design language, strong readable "
    "silhouette at distant camera scale, matte graphite and warm off-white PBR "
    "materials, separate controllable emissive accents, modular hard-surface panels"
)

NEGATIVE_PROMPT = (
    "No letters, words, logos, baked UI, terrain, characters, weapons, exposed cables "
    "without sockets, photoreal grime, transparent background cards, or merged emissive "
    "materials. Do not offset the ground contact from the model origin."
)


VISUAL_ASSETS = (
    VisualAssetSpec(
        key="mission_core",
        display_name="Mission Core",
        collection="structures",
        footprint=(3, 3),
        dimensions_m=(15.0, 15.0, 10.0),
        max_triangles=24_000,
        prompt=(
            f"{_STYLE}. Large three-by-three command structure with a grounded hexagonal "
            "base, protected central holographic projector, three radial operator bays, "
            "a ring-shaped repository display, roof antenna cluster, four symmetric data "
            "ports, and a prominent state beacon. The form should read as the durable "
            "origin and rollup authority for an entire production line."
        ),
        sockets=(
            ("socket_line_out", "dependency_output"),
            ("socket_agent", "agent_dock"),
            ("status_beacon", "state_emissive"),
            ("repository_hologram", "dynamic_display"),
        ),
        behaviors=(
            BehaviorSpec(
                authority="archetype.missions.processors.MissionRollupProcessor",
                observes=("Mission", "MissionState", "PartOfMission", "TaskState"),
                effect="Roll every member task into the committed mission outcome.",
            ),
        ),
        states=(
            StateSpec("mission.running", "active", "active", 10),
            StateSpec("mission.succeeded", "complete", "complete", 100),
            StateSpec("mission.failed", "faulted", "fault", 100),
        ),
        interactions=(
            InteractionSpec("inspect", "viewer", "mission.inspect"),
            InteractionSpec("submit", "operator", "mission.submit", True),
        ),
    ),
    VisualAssetSpec(
        key="agent_workcell",
        display_name="Agent Workcell",
        collection="structures",
        footprint=(2, 2),
        dimensions_m=(10.0, 10.0, 7.0),
        max_triangles=20_000,
        prompt=(
            f"{_STYLE}. Modular two-by-two workcell with a heavy square chassis, central "
            "recessed docking bay for a small service robot, twin vertical compute racks, "
            "two articulated maintenance arms, overhead status ring, left input port, "
            "right output port, rear service connectors, and a replaceable task module."
        ),
        sockets=(
            ("socket_dependency_left", "dependency_input"),
            ("socket_artifact_left", "artifact_input"),
            ("socket_artifact_right", "artifact_output"),
            ("socket_agent", "agent_dock"),
            ("status_ring", "state_emissive"),
            ("bay_door", "animated_part"),
            ("terminal_screen", "dynamic_display"),
            ("work_arm_left", "animated_part"),
            ("work_arm_right", "animated_part"),
        ),
        behaviors=(
            BehaviorSpec(
                authority="archetype.missions.processors.TaskReadinessProcessor",
                observes=("Task", "TaskState", "DependsOn"),
                effect="Make a pending task ready only after every prerequisite is accepted.",
            ),
            BehaviorSpec(
                authority="archetype.missions.processors.TaskDecisionProcessor",
                observes=(
                    "TaskState",
                    "TaskDispatch",
                    "AgentExecution",
                    "ValidationResult",
                    "Candidate",
                    "CriticReceipt",
                ),
                effect="Promote, repair, accept, or exhaust from committed evidence.",
            ),
        ),
        states=(
            StateSpec("task.pending", "blocked", "idle", 10),
            StateSpec("task.ready", "ready", "ready_pulse", 20),
            StateSpec("task.dispatched", "starting", "agent_arrive", 30),
            StateSpec("execution.starting", "starting", "agent_arrive", 40),
            StateSpec("execution.running", "working", "working", 50),
            StateSpec("validation.running", "scanning", "scanning", 60),
            StateSpec("task.candidate", "awaiting_review", "candidate_ready", 70),
            StateSpec("critic.running", "reviewing", "reviewing", 80),
            StateSpec("task.accepted", "complete", "complete", 100),
            StateSpec("task.failed", "faulted", "fault", 100),
        ),
        interactions=(
            InteractionSpec("inspect", "viewer", "task.inspect"),
            InteractionSpec("spectate", "viewer", "terminal.spectate"),
            InteractionSpec("takeover", "operator", "terminal.takeover", True),
        ),
    ),
    VisualAssetSpec(
        key="validator_gate",
        display_name="Validator Gate",
        collection="structures",
        footprint=(2, 1),
        dimensions_m=(10.0, 5.0, 6.0),
        max_triangles=12_000,
        prompt=(
            f"{_STYLE}. Wide scanner arch spanning a data conveyor, with three independent "
            "vertical indicator columns, a moving horizontal scan bar, shielded side "
            "cabinets, and input and output apertures sized for evidence capsules. It must "
            "read as executable verification rather than a decorative checkpoint."
        ),
        sockets=(
            ("socket_artifact_in", "artifact_input"),
            ("socket_artifact_out", "artifact_output"),
            ("scanner_bar", "animated_part"),
            ("status_column_1", "state_emissive"),
            ("status_column_2", "state_emissive"),
            ("status_column_3", "state_emissive"),
        ),
        behaviors=(
            BehaviorSpec(
                authority="archetype.missions.service.MissionService",
                observes=("TaskValidator", "AgentExecution", "ValidationResult"),
                effect="Run repository validators and stage revision-bound factual results.",
            ),
        ),
        states=(
            StateSpec("validation.waiting", "idle", "idle", 10),
            StateSpec("validation.running", "scanning", "scanning", 50),
            StateSpec("validation.passed", "passed", "pass", 100),
            StateSpec("validation.failed", "failed", "fault", 100),
        ),
        interactions=(
            InteractionSpec("inspect", "viewer", "validator.inspect"),
            InteractionSpec("view_output", "viewer", "validator.output"),
        ),
    ),
    VisualAssetSpec(
        key="critic_gate",
        display_name="Independent Critic Gate",
        collection="structures",
        footprint=(2, 2),
        dimensions_m=(10.0, 10.0, 9.0),
        max_triangles=18_000,
        prompt=(
            f"{_STYLE}. Isolated review observatory with two physically separated opposing "
            "scanner towers, a sealed candidate chamber between them, independent antennae, "
            "a rotating upper lens assembly, and no shared operator bay with the workcell. "
            "Its silhouette must communicate independent exact-candidate review."
        ),
        sockets=(
            ("socket_candidate_in", "candidate_input"),
            ("socket_receipt_out", "receipt_output"),
            ("scanner_left", "animated_part"),
            ("scanner_right", "animated_part"),
            ("review_lens", "animated_part"),
            ("status_beacon", "state_emissive"),
        ),
        behaviors=(
            BehaviorSpec(
                authority="archetype.missions.critics.harness.CriticHarness",
                observes=("Candidate", "CriticExecution", "CriticFinding", "CriticReceipt"),
                effect="Review the exact immutable candidate in a distinct sandbox.",
            ),
        ),
        states=(
            StateSpec("critic.waiting", "idle", "idle", 10),
            StateSpec("critic.running", "reviewing", "reviewing", 50),
            StateSpec("critic.approved", "approved", "complete", 100),
            StateSpec("critic.blocking", "blocked", "return_candidate", 100),
            StateSpec("critic.errored", "faulted", "fault", 100),
        ),
        interactions=(
            InteractionSpec("inspect", "viewer", "critic.inspect"),
            InteractionSpec("view_findings", "viewer", "critic.findings"),
        ),
    ),
    VisualAssetSpec(
        key="artifact_depot",
        display_name="Artifact Depot",
        collection="structures",
        footprint=(2, 2),
        dimensions_m=(10.0, 10.0, 6.0),
        max_triangles=14_000,
        prompt=(
            f"{_STYLE}. Secure two-by-two storage structure with stacked content-addressed "
            "vault drawers, a checkpoint cylinder, visibly fillable rack slots, a compact "
            "retrieval crane, and separate artifact input and output hatches. The stored "
            "objects should be replaceable engine-side inserts, not baked geometry."
        ),
        sockets=(
            ("socket_artifact_in", "artifact_input"),
            ("socket_artifact_out", "artifact_output"),
            ("socket_checkpoint", "checkpoint_insert"),
            ("rack_slot_1", "artifact_insert"),
            ("rack_slot_2", "artifact_insert"),
            ("rack_slot_3", "artifact_insert"),
            ("retrieval_crane", "animated_part"),
        ),
        behaviors=(
            BehaviorSpec(
                authority="archetype.missions.service.MissionService",
                observes=("Commit", "Checkpoint", "FilesystemManifest", "AgentArtifact"),
                effect="Project durable outputs and recovery references without owning them.",
            ),
        ),
        states=(
            StateSpec("depot.empty", "empty", "idle", 10),
            StateSpec("depot.receiving", "receiving", "receive", 50),
            StateSpec("depot.available", "available", "available", 70),
            StateSpec("depot.error", "faulted", "fault", 100),
        ),
        interactions=(
            InteractionSpec("inspect", "viewer", "artifact.inspect"),
            InteractionSpec("restore", "operator", "checkpoint.restore", True),
        ),
    ),
    VisualAssetSpec(
        key="publication_uplink",
        display_name="Publication Uplink",
        collection="structures",
        footprint=(2, 2),
        dimensions_m=(10.0, 10.0, 10.0),
        max_triangles=16_000,
        prompt=(
            f"{_STYLE}. Delivery structure with a tilting communications dish, protected "
            "candidate loading cradle, mechanical release gate, branch-status beacon, and "
            "a narrow launch rail for accepted evidence capsules. It should communicate a "
            "pushed repository revision, not a missile launcher."
        ),
        sockets=(
            ("socket_candidate_in", "candidate_input"),
            ("socket_delivery_out", "delivery_output"),
            ("uplink_dish", "animated_part"),
            ("release_gate", "animated_part"),
            ("status_beacon", "state_emissive"),
        ),
        behaviors=(
            BehaviorSpec(
                authority="archetype.missions.coding_agents.harness.CodingAgentHarness",
                observes=("Commit", "Candidate", "TaskState", "MissionState"),
                effect="Display exact pushed revision and accepted delivery state.",
            ),
        ),
        states=(
            StateSpec("publication.locked", "locked", "idle", 10),
            StateSpec("publication.pushed", "candidate_available", "uplink", 50),
            StateSpec("publication.accepted", "delivered", "release", 100),
            StateSpec("publication.failed", "faulted", "fault", 100),
        ),
        interactions=(
            InteractionSpec("inspect", "viewer", "publication.inspect"),
            InteractionSpec("open_branch", "viewer", "publication.open_branch"),
        ),
    ),
    VisualAssetSpec(
        key="agent_unit",
        display_name="Agent Unit",
        collection="units",
        footprint=(1, 1),
        dimensions_m=(2.5, 2.5, 2.5),
        max_triangles=8_000,
        prompt=(
            f"{_STYLE}. Small non-humanoid service robot with a low stable body, two compact "
            "manipulator arms, protected terminal core, four-wheel or short tracked base, "
            "front sensor bar, top status light, and a rear docking plug compatible with the "
            "Agent Workcell. Friendly industrial tool, not a combat robot."
        ),
        sockets=(
            ("socket_dock", "workcell_dock"),
            ("terminal_core", "dynamic_display"),
            ("status_light", "state_emissive"),
            ("tool_left", "animated_part"),
            ("tool_right", "animated_part"),
        ),
        behaviors=(
            BehaviorSpec(
                authority="archetype.missions.service.MissionService",
                observes=("Sandbox", "AgentExecution", "RunsIn", "Executes"),
                effect="Represent one factual agent process and its sandbox placement.",
            ),
        ),
        states=(
            StateSpec("execution.starting", "provisioning", "boot", 20),
            StateSpec("execution.running", "working", "working", 50),
            StateSpec("execution.exited", "returning", "depart", 80),
            StateSpec("execution.interrupted", "interrupted", "halt", 100),
            StateSpec("execution.errored", "faulted", "fault", 100),
        ),
        interactions=(
            InteractionSpec("spectate", "viewer", "terminal.spectate"),
            InteractionSpec("takeover", "operator", "terminal.takeover", True),
        ),
    ),
    VisualAssetSpec(
        key="dependency_conduit",
        display_name="Dependency Conduit",
        collection="logistics",
        footprint=(1, 1),
        dimensions_m=(5.0, 5.0, 0.5),
        max_triangles=3_000,
        prompt=(
            f"{_STYLE}. Tileable ground-level enclosed data conduit segment with a recessed "
            "luminous channel, clean ninety-degree-compatible endpoints, visible pulse lane, "
            "small service covers, and no loose wires. Designed to repeat between factory "
            "structures and show dependency readiness moving in one direction."
        ),
        sockets=(
            ("socket_in", "dependency_input"),
            ("socket_out", "dependency_output"),
            ("pulse_path", "state_emissive"),
        ),
        behaviors=(
            BehaviorSpec(
                authority="archetype.missions.processors.TaskReadinessProcessor",
                observes=("DependsOn", "TaskState"),
                effect="Display the committed readiness dependency without becoming it.",
            ),
        ),
        states=(
            StateSpec("dependency.blocked", "blocked", "idle", 10),
            StateSpec("dependency.satisfied", "satisfied", "pulse", 100),
        ),
    ),
    VisualAssetSpec(
        key="evidence_capsule",
        display_name="Evidence Capsule",
        collection="logistics",
        footprint=(1, 1),
        dimensions_m=(1.4, 1.4, 1.2),
        max_triangles=4_000,
        prompt=(
            f"{_STYLE}. Compact sealed data container with a durable hexagonal shell, inset "
            "identity band, four swappable icon plates, underside conveyor guide, small status "
            "light, and a protected content core. It must support engine-side variants for "
            "commit, validation, candidate, critic receipt, and checkpoint evidence."
        ),
        sockets=(
            ("identity_band", "variant_material"),
            ("icon_plate_front", "variant_insert"),
            ("icon_plate_top", "variant_insert"),
            ("status_light", "state_emissive"),
            ("socket_path", "conveyor_anchor"),
        ),
        behaviors=(
            BehaviorSpec(
                authority="archetype.missions.service.MissionService",
                observes=(
                    "ValidationResult",
                    "Commit",
                    "Candidate",
                    "CriticReceipt",
                    "Checkpoint",
                ),
                effect="Appear only after the represented evidence is durably committed.",
            ),
        ),
        states=(
            StateSpec("evidence.committed", "available", "appear", 20),
            StateSpec("evidence.in_transit", "moving", "travel", 50),
            StateSpec("evidence.consumed", "delivered", "arrive", 100),
        ),
        interactions=(InteractionSpec("inspect", "viewer", "evidence.inspect"),),
    ),
)


BUGFIX_NODES = (
    BlueprintNodeSpec("intake", "mission", "mission_core", 10),
    BlueprintNodeSpec("reproduction", "task", "agent_workcell", 20),
    BlueprintNodeSpec("evidence_depot", "system", "artifact_depot", 30),
    BlueprintNodeSpec("implementation", "task", "agent_workcell", 40),
    BlueprintNodeSpec("critic", "system", "critic_gate", 50),
    BlueprintNodeSpec("delivery", "system", "publication_uplink", 60),
)


BUGFIX_TASKS = (
    TaskSpec(
        key="reproduction",
        max_dispatches=2,
        prompt_template=(
            "Reproduce {issue}. Add one deterministic failing regression at {test_path}. "
            "Do not change production code in this task. Commit and push the evidence."
        ),
    ),
    TaskSpec(
        key="implementation",
        max_dispatches=3,
        prompt_template=(
            "Implement the smallest layer-correct fix for {issue}. Preserve the predecessor's "
            "failing regression at {test_path}, make it pass, and avoid unrelated changes."
        ),
    ),
)


BUGFIX_VALIDATORS = (
    ValidatorSpec(
        key="regression_is_red",
        task_key="reproduction",
        name="regression_is_red",
        command=("uv", "run", "pytest", "-q", "{test_path}"),
        expected_returncode=1,
    ),
    ValidatorSpec(
        key="regression_diff_check",
        task_key="reproduction",
        name="regression_diff_check",
        command=("git", "diff", "--check", "{base_ref}...HEAD"),
        timeout_seconds=60,
    ),
    ValidatorSpec(
        key="focused_contract",
        task_key="implementation",
        name="focused_contract",
        command=("uv", "run", "pytest", "-q", "{test_path}"),
    ),
    ValidatorSpec(
        key="architecture",
        task_key="implementation",
        name="architecture",
        command=("uv", "run", "python", "scripts/check_architecture.py"),
        timeout_seconds=300,
    ),
    ValidatorSpec(
        key="implementation_diff_check",
        task_key="implementation",
        name="implementation_diff_check",
        command=("git", "diff", "--check", "{base_ref}...HEAD"),
        timeout_seconds=60,
    ),
)


BUGFIX_RULES = (
    RuleSpec("DependsOn", "implementation", "reproduction"),
    *(RuleSpec("Guards", validator.key, validator.task_key) for validator in BUGFIX_VALIDATORS),
)
