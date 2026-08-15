# Mission Factory Asset Bible

**Document type:** Example-local asset and integration contract.

The first Agent Missions factory is a real prefab library, not an RTS mockup.
Its ECS world contains reusable workflow recipes and presentation contracts.
Instantiating a line produces data that compiles into the supported Agent
Missions authoring surface; committed mission state can then drive a Biome or
other 3D projection.

```text
mission-factory library world
  ├── nine visual prefab graphs ───────────────┐
  └── BugFixLine recipe + durable rules        │
                    │ instantiate              │ export
                    v                          v
             copied ChildOf tree         AI-generated GLBs
                    │ compile                   │
                    v                           │
        Agent Missions submission               │
                    │ committed facts           │
                    └────────── projection ──────┘
```

The implementation stays under `examples/mission_factory/`. It does not add a
production family, processors, or architecture policy. Agent Missions owns
readiness, dispatch, execution, validation, review, repair, publication, and
rollup. A renderer owns only presentation and interaction forwarding.

## Relationship to the framework prefab contract

This example applies the framework's generic prefab pattern to a real
software-production line. Its library stores nine AI-ready visual prefab
graphs plus a reusable `BugFixLine`. Instantiation copies the line's `ChildOf`
subtree; a trusted compiler reads stable slot keys and explicitly interprets
allowlisted `DependsOn` and `Guards` rule entities into the public Agent
Missions authoring contract.

The factory models are projections of committed mission facts, not transition
owners, and the line recipe is not a second mission engine. The generic
relation and copy-on-instantiate behavior remains owned by `archetype-ecs`;
mission workflow meaning remains owned by `archetype-missions`.

Run the example credential-free or export its full model briefs with:

```bash
uv run python examples/15_mission_factory_assets.py --briefs-json
```

## The first production line

`BugFixLine` is the smallest workflow worth manufacturing. It is a copied
prefab subtree with six stable slots, five validators, and six explicit rule
entities:

```text
BugFixLine
├── intake                         Mission Core
├── reproduction                   Agent Workcell
│   ├── regression_is_red          Validator Gate recipe
│   └── regression_diff_check      Validator Gate recipe
├── evidence_depot                 Artifact Depot
├── implementation                 Agent Workcell
│   ├── focused_contract           Validator Gate recipe
│   ├── architecture               Validator Gate recipe
│   └── implementation_diff_check  Validator Gate recipe
├── critic                         Independent Critic Gate
├── delivery                       Publication Uplink
├── DependsOn(implementation, reproduction)
└── Guards(validator, task) × 5
```

The reproduction validator expects the focused regression to fail. The
implementation task cannot become ready until reproduction is accepted, and
its validators expect the regression and architecture checks to pass. Both
tasks use exact Agent Missions publication and critic policies; the library
does not invent a parallel task state machine.

Generic `instantiate()` still copies only component values and a bounded
`ChildOf` subtree and records `IsA` provenance. It neither clones arbitrary
relations nor returns an old-to-new entity map. The trusted example compiler
resolves copied nodes by `BlueprintSlot.key`, accepts only `DependsOn` and
`Guards`, rejects unknown rules, and constructs public `MissionSubmission`
values. A future generalized compiler needs the same explicit relation
allowlist and authority boundary.

## The nine-object kit

One tile is five metres. Every object has a machine-readable footprint,
dimensions, triangle ceiling, model path, named sockets, behavior bindings,
presentation states, and interactions stored as ECS components.

| Prefab | Footprint | Maximum size | Job in the factory |
|---|---:|---:|---|
| Mission Core | 3×3 | 15×15×10 m | Mission origin, repository identity, and terminal rollup |
| Agent Workcell | 2×2 | 10×10×7 m | One task, its agent dock, terminal, and evidence flow |
| Validator Gate | 2×1 | 10×5×6 m | Revision-bound command validation |
| Independent Critic Gate | 2×2 | 10×10×9 m | Exact-candidate review in a distinct sandbox |
| Artifact Depot | 2×2 | 10×10×6 m | Visible committed artifacts and checkpoint references |
| Publication Uplink | 2×2 | 10×10×10 m | Pushed candidate and accepted delivery projection |
| Agent Unit | 1×1 | 2.5×2.5×2.5 m | One factual agent process and sandbox placement |
| Dependency Conduit | 1×1 | 5×5×0.5 m | Directional display of `DependsOn` readiness |
| Evidence Capsule | 1×1 | 1.4×1.4×1.2 m | Commit, validation, candidate, review, or checkpoint evidence |

The full descriptions are intentionally data, not prose-only documentation.
Generate a deterministic JSON handoff from committed ECS rows:

```bash
uv run python examples/15_mission_factory_assets.py --briefs-json
```

Each exported brief contains:

- a positive object prompt and shared negative prompt;
- GLB path, Y-up coordinates, ground-centred origin, dimensions, footprint,
  and triangle budget;
- required named transform sockets and their semantic roles;
- the existing Archetype authority and Components each behavior observes;
- precedence-ordered semantic signals, visual states, and animation clips; and
- permitted interactions, permission level, application action, and whether
  operator confirmation is mandatory.

`model.status` is `brief`. The example does not claim that a GLB exists until
one is generated, reviewed, and moved to the declared URI.

## Visual grammar

All nine objects share one low-poly industrial science-fiction language:
matte graphite and warm off-white hard surfaces, a readable isometric
silhouette, modular panels, and separate controllable emissive materials.
Models must not bake letters, logos, UI, terrain, or state-specific colors
into their base textures. Dynamic labels and repository data belong to engine
displays. Emissive state surfaces, inserts, doors, arms, dishes, scanner bars,
and docking transforms must remain separately addressable nodes.

The state signal is factual and has one direction:

```text
committed Agent Missions components
             │ read-only adapter
             v
visual state + animation + spawned capsule
```

Animation completion never advances a task. A green gate does not make a
validator pass; a passing committed `ValidationResult` makes the gate green.
If several signals apply, the highest exported `priority` wins. Terminal and
fault states therefore override transient work animations.

## Behaviors are eligibility, not methods

The assets follow the same Biome rule that made `Drill` useful: component
composition makes an entity eligible for existing systems. A workcell does
not implement `run_agent()`. Its `BehaviorBinding` rows identify the real
processors and evidence that give it meaning. The renderer joins those rows
to committed mission state and projects the result.

Important examples include:

| Visual | Existing authority | Facts projected |
|---|---|---|
| Mission Core | `MissionRollupProcessor` | `MissionState`, member `TaskState` |
| Agent Workcell | readiness and decision processors | dependencies, execution, validation, candidate, critic receipt |
| Validator Gate | mission application service | task validators and revision-bound results |
| Critic Gate | `CriticHarness` | candidate, critic execution, findings, receipt |
| Agent Unit | mission application service | sandbox, `RunsIn`, `Executes`, agent execution |
| Dependency Conduit | `TaskReadinessProcessor` | `DependsOn` and prerequisite state |
| Evidence Capsule | mission application service | only evidence that is already durable |

This separation also answers what the environment is. Repository state,
sandbox/provider availability, validator exit status, review findings, and
publication outcomes are environment facts the agent does not control. The
factory exposes them as constraints and flow; it never substitutes fabricated
ore, power, or currency.

## Interaction and terminal boundary

Inspect and spectate actions are read-only. Mission submission, checkpoint
restore, and terminal takeover require operator permission and explicit
confirmation. The model only names an application action; it never stores a
credential, opens a socket, or bypasses gateway authorization.

A future terminal modal can attach to the Agent Workcell's `terminal_screen`
or the Agent Unit's `terminal_core`. The safe path is:

```text
click model → request authorized application action → server validates access
            → mint short-lived terminal session → render terminal in modal
```

The ECS interaction recipe describes the affordance. Authentication, tmux,
TTY transport, session lifetime, and audit remain application and transport
authority.

## Acceptance checklist for generated models

A generated asset is ready for integration only when:

1. the file is GLB, Y-up, ground-centred, and within its triangle budget;
2. its footprint and bounding dimensions match the exported contract;
3. every required socket exists once with the exact exported name;
4. animated and emissive parts are separate addressable nodes or materials;
5. the neutral model contains no baked status, text, logo, or repository data;
6. all declared clips can play without moving the ground root; and
7. visual review confirms the silhouette remains legible at an isometric game
   camera distance.

After that review, set `VisualGeometry.model_status` to an honest integrated
state in the library and add a renderer contract. Until then, the JSON export
is the authoritative generation brief and the credential-free receipt proves
the executable mission composition.
