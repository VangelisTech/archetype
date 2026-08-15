# World libraries

**Document type:** Normative.

**Scope:** Distribution boundaries, trusted extension discovery, framework
version ranges, installation order, adapters, and release policy for Archetype
world libraries.

## 1. Framework and library distributions

Archetype ships one generic framework distribution and three first-party world
libraries:

| Distribution | Import namespace | Responsibility |
|---|---|---|
| `archetype-ecs` | `archetype` | ECS execution, worlds, storage, migration, commands, Activities, artifacts, evaluation, runtime, API, and CLI hosting |
| `archetype-missions` | `archetype.missions` | Coding-agent missions, sandboxes, sessions, transcripts, and trajectory evidence |
| `archetype-physical-ai` | `archetype.physical_ai` | Physical state, local policies, and hosted physical episodes |
| `archetype-research` | `archetype.research` | Minimal world-library optimization and AutoResearch ledger workflow |

The framework MUST install and start without any world library. A world library
depends on `archetype-ecs`; `archetype-ecs` MUST NOT depend on or import a world
library by package name. First-party world libraries MUST NOT depend on one
another. An application may compose several libraries above their public
surfaces.

The `archetype` import package is intentionally extensible. The framework owns
`archetype/__init__.py`; separately built wheels contribute only their named
subpackages. Family-qualified imports are canonical even though their code is
released from another distribution.

`archetype-smol` is intentionally outside this framework/library graph. It is
a separate synchronous in-memory teaching engine, has no dependency on
`archetype-ecs`, publishes no extension manifest, and is never selected by an
`archetype-ecs` extra. See [Smol](../smol/index.md).

## 2. Trusted Python extension boundary

### Installation

Choose the smallest installation that owns the behavior your application uses:

<!-- markdownlint-disable MD046 -->

=== "uv"

    ```bash
    # Generic framework only
    uv add archetype-ecs

    # One world library; each pulls in a compatible framework
    uv add archetype-missions
    uv add archetype-physical-ai
    uv add archetype-research

    # Every first-party world library
    uv add "archetype-ecs[all]"

    # A selective combination
    uv add "archetype-ecs[missions,research]"
    ```

=== "pip"

    ```bash
    # Generic framework only
    pip install archetype-ecs

    # One world library; each pulls in a compatible framework
    pip install archetype-missions
    pip install archetype-physical-ai
    pip install archetype-research

    # Every first-party world library
    pip install "archetype-ecs[all]"

    # A selective combination
    pip install "archetype-ecs[missions,research]"
    ```

<!-- markdownlint-enable MD046 -->

The framework also exposes selective `missions`, `physical-ai`, and `research`
extras, which may be combined, for example
`archetype-ecs[missions,research]`. Provider-specific dependencies remain
library extras: Missions exposes `modal`; Physical AI exposes `modal`, `sim`,
and `all`.

Installing a library is sufficient for ordinary process composition. Its wheel
publishes an entry point, and `ArchetypeRuntime` and the FastAPI host discover
the installed, compatible manifest set. Framework-only installation is a
supported empty set, not a configuration error.

### Manifest contract

An installed world library advertises one entry point in the
`archetype.world_libraries` group. The entry point loads a side-effect-free
`WorldLibraryManifest`. A manifest declares:

- one canonical library and distribution identity;
- its library version and compatible `archetype-ecs` version range;
- every exact operation model/discriminator it will install;
- one private synchronous installation function;
- optional API router factories.

Discovery sorts manifests by canonical library name. Before any library
installer runs, composition MUST reject:

- duplicate library names;
- duplicate operation discriminators or exact model types;
- conflicts with framework-owned operations;
- a malformed manifest; or
- a framework version outside the declared compatibility range.

The installer receives a `WorldLibraryContext` containing the already composed
framework capabilities. It may register only the operations declared by its
manifest. The synchronous installation transaction is declarative: it MUST NOT
acquire external resources, reserve process owners, or start work. Handlers may
acquire their resources lazily, under the process owner, after installation has
committed. The installer returns the installed typed-adapter surface; it does
not replace the framework registry, dispatcher, process owner, storage
authority, world registry, or shutdown protocol. Installation is complete
before the runtime or API host becomes visible to callers.

World libraries are trusted process extensions. Installing one authorizes its
Python code to register declared behavior and to construct family internals
lazily when that behavior runs. The manifest is a deterministic composition
contract, not a sandbox or a security boundary.

## 3. Runtime and transport adapters

Domain behavior is exposed by explicit typed adapters rather than by adding
permanent domain methods to the framework's `RuntimeWorld`:

```python
from archetype import ArchetypeRuntime
from archetype.research import AutoResearchConfig, Research

async with ArchetypeRuntime() as runtime:
    world = runtime.world("experiment", storage="./data")
    research = Research(world)
    result = await research.autoresearch(
        AutoResearchConfig(...),
        evaluator,
    )
```

Missions uses a runtime-scoped `Missions` adapter because it owns workflow
resources and may create its structural world. Physical AI and Research use
world-scoped adapters. The generic `runtime.library(name, ...)` and
`world.library(name)` lookup exists for hosts that cannot statically import an
optional library; ordinary application code SHOULD import the typed adapter.
Both lookup forms and all current world-library adapters are async-only. The
synchronous runtime deliberately exposes only the domain-free framework
surface instead of returning adapters whose methods would leak coroutines.

API routers are contributed by manifest factories and are mounted by the base
FastAPI host in the same deterministic order. A base-only server exposes only
framework routes. CLI remains an HTTP client and does not discover or compose
domain behavior independently.

## 4. Episodes and trajectory ownership

There is no `archetype-episodes` distribution or `archetype.episodes` package.
The word *episode* names several distinct identities whose owners remain
explicit:

- execution `EpisodeConfig`, `EpisodeResult`, and `RunEpisode` belong to
  `archetype.world`;
- Claude session ingestion, transcript rows, `TrajectorySelection`, trajectory
  query, and trajectory grading belong to `archetype.missions.trajectories`;
- hosted physical episodes belong to `archetype.physical_ai`; and
- dataset episode identity belongs to `archetype.evaluation`.

A trajectory is a Missions-derived learning view over episode evidence, not a
generic framework entity. Shared `episode_id` values permit joins without
creating a package dependency or a new universal episode authority.

## 5. Native extensions

The world-library manifest is not the native processor ABI. In particular,
`archetype-ffi` v1 is a C ABI for moving Arrow `RecordBatch` values across the
Arrow C Data Interface and invoking native processor/kernel work. It cannot
represent Python operation handlers, routers, process owners, projectors,
configuration, or teardown.

The trusted Python manifest is the world-library lifecycle and composition
boundary. A future manifest version may advertise an optional native processor
artifact and `archetype-ffi` ABI range, but that native artifact remains a
nested implementation capability of a library or the ECS framework. It is not
the discovery or lifecycle protocol for the library itself.

## 6. Packaging and release policy

The repository is one uv workspace with one lock and five independently built
projects: the framework, three world libraries, and the separate Smol teaching
engine. Published dependencies use normal version ranges; uv workspace source
overrides are development-only. Every wheel MUST build with workspace sources
disabled and pass an isolated install/import smoke test.

The framework extras `missions`, `physical-ai`, `research`, and `all` are
installation conveniences. They add the corresponding distributions; they do
not copy domain code into the framework wheel. The resulting dependency cycle
in package metadata is intentional and benign: each library requires a
compatible already selected framework version, while the extra selects the
library at the same release line.

The split is an intentional pre-1.0 packaging break for the 0.6 line. It has no
compatibility layer:

- `archetype.<family>` imports remain the supported family paths;
- domain types are no longer owned by the framework root facade;
- installed libraries do not add dynamic methods to runtime/world handles;
- installed libraries do not add domain values to the framework root; and
- `archetype.episodes` is removed without a replacement shim.

Each distribution is built and tested independently. The 0.6 distributions are
published as one coordinated release set, which MUST also pass a full-stack
matrix proving deterministic composition, operation dispatch, router
installation, process teardown, and duplicate/incompatible manifest failure.
Smol receives its own isolated install and behavior proof; it is not inserted
into the framework extension matrix.
See [Archetype 0.6](release-0.6.md) for the exact clean-break upgrade contract.

## 7. Executable evidence

Required evidence includes:

- base-only editable and wheel installs;
- each library with the base, both editable and from wheels;
- a full-stack install of all three first-party world libraries;
- exact operation inventories for each installation set;
- base runtime and API startup with zero extensions;
- deterministic manifest ordering independent of entry-point order;
- fail-closed duplicate names, models, discriminators, and incompatible
  framework ranges;
- extension-owned runtime behavior and API routes; and
- clean framework plus extension-owner teardown.

The repository architecture and observability audits MUST scan every package
source root. A split that makes a checker ignore library source is a harness
failure, not successful isolation.
