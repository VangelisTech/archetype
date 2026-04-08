# Query Design: From Service to CLI to Self-Observation

## Current State (v0.2.0)

`QueryService` is a service-layer facade that wires API routes to real data. It reads from `AsyncWorld._live` for current-tick queries and from the store via `AsyncWorld.query_archetype()` for historical ticks. It materializes DataFrames to Python dicts for JSON serialization at the API boundary.

This works for REST consumers. It does not work for agents.

## Why QueryService Is the Wrong Abstraction for Agents

An LLM-powered agent operating inside a simulation needs to query world state, compose that with other queries, and feed results into downstream operations — all without leaving Daft's lazy execution model. QueryService materializes to Python dicts immediately, which means:

1. **The DAG breaks.** Every query collects to Python, discarding Daft's lazy evaluation, plan optimization, and parallelism. If the agent wants to filter the result, join it with another world's state, or feed it into a processor, it has to re-create a DataFrame from dicts — the exact anti-pattern documented in LEARNINGS.md.

2. **The abstraction fights the engine.** Archetype is built on a single primitive: processors are `DataFrame -> DataFrame` transforms. Components declare the schema. The system finds matching archetype tables, joins them, and feeds the union to processors. A query is just a read-only processor chain — it uses the same join, the same projection, the same filtering. QueryService reimplements this join by hand rather than using the machinery that already exists.

3. **The component tuple is the query predicate.** When you write `world.query(Agent, Score)`, you're asking for the union of all archetype tables whose signature is a superset of `{Agent, Score}`, projected to those components' schema. That's exactly what `AsyncWorld.get_components([Agent, Score])` does. The "query" is the component declaration. Everything downstream — tick filtering, entity filtering, aggregation — is just DataFrame expressions on that result.

## The CLI Is the Tool Interface

The agent's tool interface already exists: the `archetype` CLI. Every subcommand is a tool call the LLM knows how to make. Queries are self-contained expressions — component types, optional filter, optional tick, terminal operation:

```bash
archetype query <world-id> Agent,Score --tick 3 --show 5
archetype query <world-id> Agent,Score --where "score__val > 0.5" --show 10
archetype query <world-id> Trajectory,Label --count
```

The server builds the lazy DataFrame chain internally, materializes only at the terminal operation (`--show`, `--count`), and returns the result. The CLI is the thin HTTP client it already is. No MCP tools, no DataFrame handles, no new abstraction layer.

No state needed across tool calls. Each query is a single self-contained command. The server composes the lazy chain, the CLI renders the output.

## Session Entities: stdio as Components

Each terminal session — each agent's interaction with the CLI — is itself an entity in the system. The session's stdio (commands issued, query results received, errors encountered) is stored as components on that entity.

This means:

- An agent runs `archetype query ...` and gets results in stdout
- That stdout is captured as component data on the session entity
- Other agents (or the same agent later) can query those session logs through the same CLI
- The system observes itself through its own query interface

The session entity turns the CLI into a feedback loop. Queries produce output. Output becomes data. Data is queryable. An agent can ask "what did agent X query at tick 5?" using the same primitive it uses to ask "what is agent X's score at tick 5?"

This is the meta-goal made concrete: Archetype observing Archetype, using the same Component/Processor/DataFrame primitives all the way down.

## What Ships Now vs. What Comes Next

**This PR** wires QueryService to real data. The REST API returns actual entity/component state with time-travel support. This is necessary plumbing — external consumers (dashboards, CLI, scripts) need JSON endpoints that work.

**Next** (#103) is the DataFrame-native query path:

1. Expose the DataFrame API through CLI subcommands
2. Each session's stdio stored as components on a session entity
3. QueryService stays for REST consumers — the CLI and session entities are the agent-native path

The query primitive already exists. It's `world.get_components(*types)`. The component tuple is the query. The CLI is the tool. The session entity closes the loop.
