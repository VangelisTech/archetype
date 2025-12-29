# Glossary

## Archetype

In Archetype (the engine), an **archetype** is the set of component types attached to an entity. It maps to a physical table with a unified schema.

## Component

A **component** is a typed record (Pydantic + LanceModel) that defines a schema fragment. Stored columns are prefixed by component name (e.g. `position__x`).

## Entity

An **entity** is an ID (`entity_id`) whose state is represented by the components attached to it.

## Processor

A **processor** is a pure transformation: `DataFrame -> DataFrame`. It declares which components it requires and runs inside a system.

## System

A **system** is a priority-ordered collection of processors. Each tick, the system runs processors that match an archetype signature.

## World

A **world** is the unit of simulation execution. It owns:

- entity IDs + entity → archetype mapping
- the tick counter
- the system (processors)
- the querier/updater (storage I/O)

## Tick

A **tick** is one simulation step. Each tick produces new state rows and persists them (append-only).

## Run ID

A **run_id** groups ticks into a run (a contiguous sequence of ticks) and enables time-travel queries across runs.

## Live snapshot

The **live snapshot** is an in-memory cache of the most recent DataFrame per archetype signature. It enables fast reads without hitting storage (when enabled).

## Time travel

“Time travel” means you can query historic state by `(world_id, run_id, tick)` — because state is stored append-only per tick.

## GRPO

**Group Relative Policy Optimization**: an RL method where you sample K completions per prompt, score them, and compute group-relative advantages.

## Rollout artifact contract

For RL correctness, rollouts must provide:

- exact generated token IDs
- per-token logprobs under the behavior policy

This avoids retokenization drift between inference and training.

## Weights as data

“Weights as data” is the pattern where training steps **write** new checkpoints and pass the path forward (rather than trying to “return weights” from a distributed UDF).

