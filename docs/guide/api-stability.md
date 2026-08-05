# API stability and docstrings

Archetype separates compatibility from prominence. A symbol can be supported
without being the first interface shown to a new user.

## API tiers

| Tier | Contract | Documentation |
| --- | --- | --- |
| Recommended | Default application interface | Complete reference and workflow examples |
| Extension | Supported customization interface | Complete semantics and focused examples |
| Integration | Supported host and service interface | Advanced reference without tutorial repetition |
| Compatibility | Stable, frozen, or deprecated interface | Terse reference with migration direction |
| Internal | No compatibility promise | Maintainer context or explicit migration inventory only |

The recommended interface is `ArchetypeRuntime` and its world handles.
Components, processors, resources, and the configuration and result types
required by runtime signatures form the extension/signature interface. REST and
CLI are supported adapters over the same governed operations. Concrete
application services, app protocols, process wiring, and `RuntimeResources`
are internal. The synchronous
educational engine remains a compatibility interface.

## What counts as public

A supported name is one classified by the generated Python API manifest or a
focused specification. Types that appear in the arguments or return values of
supported names are public dependencies even when they live in a submodule.
Exporting a name from a lower-level package does not promote it to a supported
or recommended interface.

`archetype.__all__` does not include concrete application services,
`RuntimeResources`, or process-wiring helpers. Those objects carry no
compatibility promise. Repository composition code imports them from their
owning modules; applications use `ArchetypeRuntime`, REST, or CLI.

Names beginning with an underscore are internal. Modules explicitly labeled
experimental may change without the compatibility guarantees of the main API.

### Reviewed capability packages

The provisional production `archetype.experiments` package has been removed.
Standalone scripts under the repository-root `experiments/` directory are
consumers of the shipped library; they do not define an importable domain
family or application authority.

`ClaudeTranscriptSource` and `TranscriptIngestionResult` are supported types
in the `RuntimeWorld.ingest_claude_transcript()` signature. They remain
namespaced under `archetype.missions.trajectories`; support does not require promotion to the
root import surface. The parser's in-memory `LoadedSession` and the concrete
transcript composition plus artifact-family handlers remain implementation
details.

`archetype.physical_ai` is the reviewed owner for reusable physical state,
hosted episode contracts, provider reconciliation, and pure instruction
optimization. `HostedEpisodeRequest`, `HostedEpisodeObservation`, and
`ModalHostedEpisodeConfig` are canonical in
`archetype.physical_ai.models` and supported at the top level because they
appear in `RuntimeWorld.run_hosted_episode()`. The family workflow, Activity
binding, worker, and exact operation model remain internal. Raw-client
environment and policy processors are internal in-process implementation
details and are not a distributed runtime surface.

`FrameGrader`, `Outcome`, `GraderContract`, and `EvalReceipt` are supported
top-level evaluation contracts. `TrajectoryGrader` remains an object-identical
namespaced alias of `FrameGrader` for the existing trajectory consumer; the
evaluation workflow itself is implemented by family-owned free handlers.

The ownership trajectory is recorded in
[Agent Missions V1, section 9](agent-missions.md#9-family-direction-after-v1),
and the named family moves are complete. Dataset evidence identity now lives in
`archetype.evaluation.contracts`, and the former `archetype.datasets` package is
gone. The former `archetype.htn` resolver now lives under
`archetype.missions.planning`; its future adapter to mission task entities is
not yet a supported authoring surface. Trajectory schemas, Claude source
parsing, and pure transforms live under `archetype.missions.trajectories`;
physical state and the hosted episode workflow live in `physical_ai`; that
workflow is registered behind one exact trusted-only direct operation; and
research values, ledger state, views, decoder, and free workflow handler live
in `archetype.research`. `ResearchCandidateContext` is the canonical supported
preparer-callback value. `CandidateContext` remains an object-identical
one-release alias; it is not the persisted `archetype.missions.Candidate`
review subject.

Do not build a compatibility promise around the planning adapter or concrete
application module paths. New applications use `ArchetypeRuntime` and the
supported extension/signature types inventoried by the generated reference. A
future graduation must name an owning domain family and enter that inventory.

Supported exports are additive within a release line. Removing or changing
their meaning requires a versioned migration. Every classification or export
change must update the Python reference manifest; the docs build rejects
missing or stale entries.

The file-artifact consolidation is the recorded `0.4.1` to `0.5` migration.
Its removed bundle, claim, receipt, and reconciliation contracts must not ship
in another `0.4.x` release. The replacement surface and direct call mapping are
documented in [Artifacts and ingestion](artifacts.md#12-migration-from-the-04-artifact-surface).

The authoritative boundary and dependency rules are in
[Application Architecture](application-architecture.md).

## Docstring standard

Public docstrings use Google style and begin with one direct summary sentence.
Additional prose should explain only behavior that the signature cannot:

- lifecycle and ownership;
- persistence or mutation semantics;
- concurrency guarantees;
- intentional exceptions;
- surprising defaults or side effects.

Use `Args`, `Returns`, and `Raises` when their semantics are not obvious from names
and annotations. Do not repeat types or defaults already present in the signature.
Examples belong on recommended entry points and non-obvious workflows, not on every
method. Prefer a guide when an example spans several calls.

Public docstrings must not contain issue numbers, implementation shorthand,
development TODOs, or references to private services. Put that context in code
comments, specifications, or development guides.

Internal docstrings describe invariants and rationale for maintainers. They do not
need user-facing examples or exhaustive argument sections.
