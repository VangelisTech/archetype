# Why Archetype

Archetype is an opinion about what data engineering should be: composable,
declarative, and data-centric. In practice it is a state machine that uses
big-data technology to run itself — a simulation runtime where the entire
stack collapses into two declarations, and where everything that happens
becomes data you can query.

This page explains the reasoning. For how to use the runtime, start with the
[quickstart](quickstart.md).

## Two primitives

You declare two things:

- A **component** is typed state. Its class definition is also its schema —
  fields become Arrow columns. See [Components](components.md).
- A **processor** is a declared transform: one DataFrame in, one DataFrame
  out, applied to every entity that has the components it names. See
  [Processors](processors.md).

Everything else follows mechanically. Entities that share a component set
share an archetype; an archetype is a table schema; a tick appends rows to
that table; a query reads them back. There is no separate pipeline definition,
no serialization layer to maintain, and no catalog that has to be told what
the data means. The declaration that produced the data is the same code you
use to read it.

That collapse is the point. Modern data infrastructure decoupled storage from
compute physically — which is worth keeping, and Archetype keeps it: Daft
executes, Lance stores. But the same movement severed them semantically: the
code that produces data and the artifacts you read knowledge from became
separate things, held together by catalogs and conventions. In Archetype,
provenance is derivation. A component is its schema; a processor is its
lineage.

## Append-only is a policy

Every tick persists as immutable rows. Nothing is overwritten — an update
stages new given state, a despawn flips a flag, and the past stays put. The
one-off run you thought you would throw away is still there, because the
belief underneath the design is that the data is the thing that matters, and
"I won't need that run" is usually an inductive bias, not a fact.

Holding that policy makes several expensive things cheap:

- **The past is a filter.** Any earlier state is `where(tick == t)` on an
  ordinary query. There is no replay system because none is needed. See
  [History and forks](history-and-forks.md).
- **A counterfactual is a fork plus a join.** A fork inherits its source
  history through lineage and writes its own future. Comparing "what
  happened" against "what would have happened" is a join over two branches,
  not a second experiment. The README's butterfly example and
  [Choose an example](examples.md) show this end to end.
- **Evaluation is a query.** Grading reads the same rows the simulation
  wrote. There is no exporter between the run and the judgment of the run.
  See [Evals](evals.md).

The record is not a trace bolted onto the system. It is the system's state,
kept.

## Built for agents, twice over

Agents appear on both sides of the runtime.

**Agents run inside worlds.** An entity with a prompt-bearing component and a
processor that calls a model is an agent; the LLM call is one more columnar
transform, executed across the population, its outputs persisted like any
other state. Nothing about the framework owns your agent loop — bring
`daft.functions.prompt`, an SDK client, or a policy network.

**Agents operate the runtime.** The documentation you are reading is written
with a machine reader in mind: contracts, invariants, and primitives rather
than persuasion. The normative pages under
[the specification](specification.md) exist so that an agent extending the
system has guarantees it can rely on, and the
[command gate](command-gate.md) authorizes and audits mutations so that
autonomous operation leaves a clean, reviewable record on the same substrate
as the simulation itself. The wager is simple: scaffolding depreciates as
models improve; primitives appreciate.

## What Archetype is not

- **Not an agent-loop framework.** It does not manage conversations, tools,
  or planning. It gives loops a world to act in and a history to be judged
  by.
- **Not observability added after the fact.** There is nothing to instrument
  to get the record — writing the record is what a tick is. (Telemetry for
  the runtime's own internals exists separately and is host-configured; see
  [Observability](observability.md).)
- **Not a game engine or an RL gym.** There is no renderer and no reward API.
  Environments and reward logic can be hosted as components and processors,
  but they are yours to define.

## When to reach for it

Archetype earns its keep when behavior has to be explained, compared, or
improved after the fact: populations of entities, evaluation and optimization
loops, regression across runs, and questions of the form "which change caused
which behavior." If your workload is a single stateless agent that never asks
what happened, you do not need a world for it.

## Where next

- [Quickstart](quickstart.md) — first world in a few minutes.
- [Building simulations](building-simulations.md) — the working patterns.
- [Choose an example](examples.md) — runnable, credential-free starting
  points.
- [Specification](specification.md) — the contracts the engine holds.
