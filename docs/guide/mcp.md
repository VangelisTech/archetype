# MCP server

Archetype exposes a small control plane over MCP (Model Context Protocol) so an agent can:

- create worlds
- run worlds (single or parallel)
- submit commands
- inspect command queues/history

## Run it

```bash
cd archetype
python -m archetype.mcp
```

## Tool surface

See `src/archetype/mcp/server.py` for the canonical tool list and argument schemas. The current tools include:

- `create_world`
- `list_worlds`
- `get_world_status`
- `run_world`
- `run_parallel_worlds`
- `run_monte_carlo`
- `submit_command`
- `get_pending_commands`
- `get_command_history`
