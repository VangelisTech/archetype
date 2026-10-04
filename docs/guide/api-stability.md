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
are internal. `ArchetypeRuntime.sync()` and its blocking handles are a
recommended facade over the same asynchronous production engine; they are not
a second ECS kernel.

Separately installed world libraries expose supported family-qualified imports
and typed adapters. The base framework root does not own their API. See
[World Libraries](world-libraries.md).

The independently installed [`archetype-smol`](../smol/index.md) package is a
small synchronous engine for education and experimentation. It is not a
compatibility API, world library, backend, or alias layer for `archetype-ecs`,
and no migration or behavioral-parity promise connects the two engines.

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

Research is the retained separately distributed world library. Its values and
adapter are imported from `archetype.research`; installation contributes no
dynamic root exports or world methods. The framework owns execution episodes,
generic evaluation, artifacts, and storage. `ResearchCandidateContext` is the
supported preparation callback value. Concrete workflow implementations remain
internal.

Missions and Physical AI products are removed in the DDlog migration branch.
This is an intentional breaking change for a future versioned release, not a
compatibility promise for previously published 0.6 wheels. Old consumers must
keep their pinned environment until migrated. See
[World libraries](world-libraries.md) for stale entry-point rejection and the
[DDlog migration](ddlog-runtime.md) for the separate preview contract.
Historical release notes describe what those releases shipped.

The provisional production `archetype.experiments` package remains removed;
repository-root experiments are consumers of the shipped library.

Supported exports are additive within a release line. Removing or changing
their meaning requires a versioned migration. Every classification or export
change must update the Python reference manifest; the docs build rejects
missing or stale entries.

The file-artifact consolidation is the recorded `0.4.1` to `0.5` migration.
Its removed bundle, claim, receipt, and reconciliation contracts must not ship
in another `0.4.x` release. The replacement surface and direct call mapping are
documented in [Artifacts and ingestion](artifacts.md#11-migration-from-the-04-artifact-surface).

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
