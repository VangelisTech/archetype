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
CLI are supported adapters over the same gateway behavior. Concrete application
services, app protocols, and `ServiceContainer` are internal. The synchronous
educational engine remains a compatibility interface.

## What counts as public

A supported name is one classified by the generated Python API manifest or a
focused specification. Types that appear in the arguments or return values of
supported names are public dependencies even when they live in a submodule.
Exporting a name from a lower-level package does not promote it to a supported
or recommended interface.

`archetype.__all__` does not include concrete application services,
`CommandGateway`, or `ServiceContainer`. Those objects are internal wiring and
carry no compatibility promise. Repository composition code imports them from
their owning family modules; applications use `ArchetypeRuntime`, REST, or CLI.

Names beginning with an underscore are internal. Modules explicitly labeled
experimental may change without the compatibility guarantees of the main API.

### Provisional capability packages

`archetype.experiments` is not a supported application surface merely because
its modules are importable. It contains working prototypes, but its package
boundary will disappear as physical-AI and mission capabilities settle. The
import-boundary checker constrains what the package may depend on; it does not
promote it to public API.

`archetype.physical_ai` is the reviewed owner for reusable physical state,
policy contracts, and external-step processors. Its current prototype symbols
remain provisional until a focused specification graduates an explicit surface.

The target ownership is recorded in
[Agent Missions V1, section 9](agent-missions.md#9-family-direction-after-v1),
and is landing incrementally. Dataset evidence identity now lives in
`archetype.evaluation.contracts`, and the former `archetype.datasets` package is
gone. The former `archetype.htn` resolver now lives under
`archetype.missions.planning`; its future adapter to mission task entities is
not yet a supported authoring surface. The mixed `experiments` package is
being staged out: trajectory schemas and transforms now live under
`archetype.missions.trajectories`; physical state and processors have moved to
`physical_ai`; and research ledger state and its pure runner decoder live in
`archetype.research`. The surviving family paths remain provisional until an
implementation issue explicitly graduates a surface.

Do not build a compatibility promise around those module paths yet. New
applications use `ArchetypeRuntime` and supported extension types. A future
graduation must name an owning domain family, add it to the API manifest, and
provide a migration from the provisional path.

Supported exports are additive within a release line. Removing or changing
their meaning requires a versioned migration. Every classification or export
change must update the Python reference manifest; the docs build rejects
missing or stale entries.

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
