# Episodes

An **episode** is a single simulation run from sampled initial conditions to a horizon (or termination).

In Archetype, `Episode` is a thin wrapper around an `AsyncWorld` that:

- spawns an entity with the provided initial components
- steps the world
- collects a trajectory as a Daft DataFrame

## When to use episodes

Use episodes when you want:

- parallel rollout collection (many independent worlds/episodes)
- a clean “unit of experience” to store, analyze, and feed into training

## Example

See `src/archetype/app/episodes/episode.py` and `examples/` for patterns.
