# archetype-physical-ai

The separately installable Physical AI world library for Archetype.

```bash
uv add archetype-physical-ai
```

Use `archetype.physical_ai.PhysicalAI` with an Archetype runtime world. Provider
and simulator dependencies are available through the `modal` and `sim` extras.
Requests, provider configuration, observations, and the adapter are imported
from `archetype.physical_ai`; the generic world does not expose a hosted-episode
method.
