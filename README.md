# Archetype

[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)

> **Retired.** This repository is no longer under active development. The 0.6
> line remains installable for anyone still running against it; no further
> releases, fixes, or roadmap items are planned.

Archetype was an over-engineered control plane for a Data Efficient Autonomous
Research Substrate. It compiled ECS semantics from Pydantic Components and
[Daft](https://www.daft.ai/) DataFrame processors into Iceberg-backed worlds,
and exposed agent missions, physical-AI evals, and research as queryable,
forkable histories.

## Why it's retired

Effects broke the data plane. The runtime treated an LLM step — with its
file, tool, and sandbox side effects — as something to orchestrate *outside*
the dataframe pipeline and then reconcile back in. That kept concurrent agent
count bounded by cross-process fan-out and made unbounded recursive loops
expensive by construction. It is a framework-shape mismatch, not a feature
gap, so the project is retired rather than iterated.

## Historical layout

| Package | What it was |
|---|---|
| `archetype-ecs` | Worlds, ticks, storage, commands, the runtime, REST, and CLI |
| `archetype-missions` | Coding-agent missions and the agent-facing MCP server |
| `archetype-physical-ai` | Physical state, policies, and hosted episodes |
| `archetype-research` | AutoResearch candidates, evaluators, and the experiment ledger |

Framework internals, forks, and history examples still live in
[`examples/`](examples/README.md).

## Install the last release

```bash
uv add archetype-ecs
```

World libraries pin a compatible `archetype-ecs`:

```bash
uv add archetype-missions
uv add archetype-physical-ai
uv add archetype-research
```

```bash
uv add "archetype-ecs[all]"
uv add "archetype-ecs[missions,research]"
```

The same specifiers work with `pip install`. `archetype-smol` — a small
synchronous, in-memory teaching engine — is separate and is not selected by
`archetype-ecs[all]`.

Version 0.6 is the final pre-1.0 split. See the
[0.6 release note](docs/guide/release-0.6.md) for the last set of source and
storage changes.

## Agent interface (MCP), as shipped

The last supported agent interface was Archetype's native Mission MCP server.
ACP-capable clients (Claude Code, Cursor, and other MCP hosts) talked to
Archetype through this server. Archetype never shipped an ACP implementation:
ACP owns the client session, MCP is replaceable transport, and mission
authority stays on the host.

```bash
uv add archetype-missions
archetype serve
archetype-missions-mcp
```

`python -m archetype.missions.mcp` is the same entry point. Trusted
environment supplies the host URL and credential:

| Variable | Role |
|---|---|
| `ARCHETYPE_MISSIONS_MCP_URL` | MissionRun REST base URL (default `http://localhost:8000`) |
| `ARCHETYPE_MISSIONS_MCP_CREDENTIAL` | Mission principal bearer (or `..._CREDENTIAL_FILE`) |

A model cannot supply a URL, token, REST path, backend, or secret.

Example MCP host config:

```json
{
  "mcpServers": {
    "archetype-missions": {
      "command": "archetype-missions-mcp",
      "env": {
        "ARCHETYPE_MISSIONS_MCP_URL": "http://127.0.0.1:8000",
        "ARCHETYPE_MISSIONS_MCP_CREDENTIAL": "<mission-principal-token>"
      }
    }
  }
}
```

Six asynchronous tools, and only these six:

| Tool | What it does |
|---|---|
| `mission_submit` | Start a durable run; returns `run_id` immediately. Caller-owned `idempotency_key` recovers the same run after a crash. |
| `mission_get` | Bounded status for one run |
| `mission_events` | Ordered events after an opaque cursor |
| `mission_result` | Immutable terminal result (`not_ready` while the run is open) |
| `mission_cancel` | Durable cancel intent; idempotent by `run_id` |
| `mission_list` | Runs owned by the authenticated principal |

Contract and usage: [Agent Missions — Mission MCP server](https://archetype.vangelis.tech/docs/guide/agent-missions/#11-mission-mcp-server).
REST under that adapter: [Missions REST API](https://archetype.vangelis.tech/docs/reference/rest-api-missions/).

## Documentation

Docs remain online as a reference for the 0.6 line:

- [World libraries](https://archetype.vangelis.tech/docs/guide/world-libraries/)
- [Agent Missions](https://archetype.vangelis.tech/docs/guide/agent-missions/)
- [Physical AI](https://archetype.vangelis.tech/docs/guide/physical-ai/)
- [AutoResearch](https://archetype.vangelis.tech/docs/guide/autoresearch/)
- [Framework quickstart](https://archetype.vangelis.tech/docs/guide/quickstart/)
- [Python API](https://archetype.vangelis.tech/docs/reference/python-api/),
  [CLI](https://archetype.vangelis.tech/docs/reference/cli/),
  [REST](https://archetype.vangelis.tech/docs/reference/rest-api/)

## Development (frozen)

The Makefile targets still run against a checkout, but no PRs are being
accepted:

```bash
make sync-dev  # install development dependencies
make test      # run the fast test suite
make check     # format and lint
make docs      # generate references and build the docs site
```

## License

Apache-2.0. See [LICENSE](LICENSE).
