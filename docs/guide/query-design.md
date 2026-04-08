# Query Design: From Service to DataFrame Tools

## Current State (v0.2.0)

`QueryService` is a service-layer facade that wires API routes to real data. It reads from `AsyncWorld._live` for current-tick queries and from the store via `AsyncWorld.query_archetype()` for historical ticks. It materializes DataFrames to Python dicts for JSON serialization at the API boundary.

This works for REST consumers. It does not work for agents.

## Why QueryService Is the Wrong Abstraction for Agents

An LLM-powered agent operating inside a simulation needs to query world state, compose that with other queries, and feed results into downstream operations — all without leaving Daft's lazy execution model. QueryService materializes to Python dicts immediately, which means:

1. **The DAG breaks.** Every query collects to Python, discarding Daft's lazy evaluation, plan optimization, and parallelism. If the agent wants to filter the result, join it with another world's state, or feed it into a processor, it has to re-create a DataFrame from dicts — the exact anti-pattern documented in LEARNINGS.md.

2. **The abstraction fights the engine.** Archetype is built on a single primitive: processors are `DataFrame -> DataFrame` transforms. Components declare the schema. The system finds matching archetype tables, joins them, and feeds the union to processors. A query is just a read-only processor chain — it uses the same join, the same projection, the same filtering. QueryService reimplements this join by hand rather than using the machinery that already exists.

3. **The component tuple is the query predicate.** When you write `world.query(Agent, Score)`, you're asking for the union of all archetype tables whose signature is a superset of `{Agent, Score}`, projected to those components' schema. That's exactly what `AsyncWorld.get_components([Agent, Score])` does. The "query" is the component declaration. Everything downstream — tick filtering, entity filtering, aggregation — is just DataFrame expressions on that result.

## Where This Leads: DataFrame as Tool Interface

The natural query interface for an LLM agent isn't a Python DSL or a query language. It's a set of tools that compose lazily over DataFrames:

| Tool | Input | Output | Materializes? |
|------|-------|--------|---------------|
| `query` | Component types | DataFrame handle | No |
| `filter` | DataFrame handle + predicate | DataFrame handle | No |
| `project` | DataFrame handle + columns | DataFrame handle | No |
| `at` | DataFrame handle + tick | DataFrame handle | No |
| `show` | DataFrame handle + N rows | Rendered rows | Yes (terminal) |
| `count` | DataFrame handle | Integer | Yes (terminal) |
| `join` | Two DataFrame handles | DataFrame handle | No |

Each tool takes a handle in, returns a handle out. The LLM chains them as tool calls. The DAG stays lazy across the entire chain. Only terminal operations (`show`, `count`) materialize — and `show` materializes just N rows, giving the LLM feedback without collecting the full dataset.

This is the same pattern as `@behavior` in the agent DSL: ergonomic sugar at the interface level, processors and DataFrames at the engine level.

## What Ships Now vs. What Comes Next

**This PR** wires QueryService to real data. The REST API returns actual entity/component state with time-travel support. This is necessary plumbing — external consumers (dashboards, CLI, scripts) need JSON endpoints that work.

**Next** is the DataFrame tool interface: a set of MCP tools (or equivalent) that let an LLM agent compose lazy queries over world state without materializing. QueryService stays for REST consumers. The tools are the agent-native query path.

The key insight: the query primitive already exists. It's `world.get_components(*types)`. The work is exposing that — and the DataFrame operations downstream of it — as composable tools an LLM can reason about.
