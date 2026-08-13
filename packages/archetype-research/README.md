# archetype-research

The separately installable AutoResearch world library for Archetype.

```bash
uv add archetype-research
```

Use `archetype.research.Research` with a base runtime world to optimize world
libraries without coupling Research to Missions or another domain package.
Research values and the adapter are imported from `archetype.research`; the
generic world does not expose an AutoResearch method. Pre-0.6 Research ledgers
are unsupported and are not migrated in place.
