# Examples

Runnable examples demonstrating Archetype's core features.

```bash
uv run python examples/<filename>.py
```

| Example | Description | Requires |
|---------|-------------|----------|
| [`llm_agents.py`](llm_agents.py) | LLM-powered agents — each entity gets a parallel LLM call every tick via `daft.functions.prompt` | `OPENAI_API_KEY` |
| [`messaging_example.py`](messaging_example.py) | Agent-to-agent messaging via the broker — resources, `MESSAGE` commands, and lifecycle hooks | None |
| [`trajectories/run.py`](trajectories/run.py) | Trajectory analysis pipeline — ingest, label, and compare agent trajectories using world forking | Optional: `OPENAI_API_KEY` |
