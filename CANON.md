# Canon

This file is the canonical record of what Archetype is, in the words of its
architect, Everett Kleven. It exists because agents given large context dumps
interpret instead of citing, and interpretation compounds into drift.

**The rule:** when describing Archetype's purpose, concepts, or vocabulary —
in documentation, specifications, plans, prompts, commit messages, or PR
bodies — cite this file verbatim or mark the sentence as an **unratified
proposal**. Never silently coin a term. Never paraphrase a quote and present
the paraphrase as the project's position. Everett ratifies additions; nothing
enters this file by summarization.

Everything in quotation blocks below is verbatim Everett, dated. Connective
prose outside quote blocks is scaffolding and carries no authority.

---

## What Archetype is

> Archetype is the result of what happens when a person like me, who takes on
> the design challenge of "what can Daft not do," goes as far as he can take
> it.
>
> — Everett, 2026-07-26

> You don't need to know how archetype is storing your data. You just need
> the code that you used to run the simulation to query it. That's the whole
> point of us using components: we have a single source of truth for how data
> is stored in the same way that it's composed and declared.
>
> — Everett, 2026-07-26

> You get an ECS game engine that scales horizontally. You have a data
> catalog that has seamless interop in the cloud.
>
> — Everett, 2026-07-26

> My bet with Archetype was, at the world library level, the prefab level, if
> you have strong enough semantics, the agents should be able to effectively
> compose systems, go run experiments, and hill climb with a strong ability,
> a truly trivial ability to run counterfactuals and trade studies with
> reproducible experiments. In parallel, depending on how much you want to
> spend.
>
> — Everett, 2026-07-26

> All to make our missions and our physical AI evals more native and
> integrated with this opinionated framework to be able to query, fork, and
> analyze runs. We go do something. We try to make it as Daft native as
> possible so that we can highly parallelize.
>
> — Everett, 2026-07-26

## The tick

> We're only ever running Daft for a single tick. A single tick could take a
> long time. It could run an entire simulation episode. It could run an
> entire agent mission on a modal sandbox, and then we're collecting data
> after that and writing it to storage.
>
> — Everett, 2026-07-26

> Our ticks aren't simulation steps at the 100 hertz level, they're the state
> transition authority from a task level for agents.
>
> — Everett, 2026-07-26

## Resources and Activities

> A Resource is a world-declared capability that Archetype makes available
> during every tick while the world is active — either by reconnecting inside
> the tick or routing through a persistent host. If using it changes external
> state, the resource must also say how that effect can be recovered relative
> to tick commit.
>
> — Everett, 2026-07-25 (ratified: "Canonical.")

> Activities and resources effectively help us define the contract between
> Archetype and third-party systems so that it can wrap and interact with
> those systems in a durable way.
>
> — Everett, 2026-07-26

> There are some things that don't belong in Daft and outlive the processor
> tick lifecycle, but are more tied to the world lifecycle, and activities is
> our answer to deal with that.
>
> — Everett, 2026-07-26

## Vocabulary

The nouns of Archetype, as enumerated by Everett (2026-07-26):

> We have artifacts, we have worlds, we have entities, components,
> processors, prefabs, worlds, simulations, episodes, rollouts. We have agent
> missions. Tasks, validators. We have harnesses. We've got unit tests,
> integration tests, linter rules, dependency, flow, requirements.
>
> We use Daft and we use Iceberg. We have support for LanceDB. We'll see if
> we keep LanceDB. We're able to connect to R2 and the R2 data catalog by
> Iceberg.
>
> We run things on Modal, and that works really, really well. Apple
> containers and Modal sandboxes work well.
>
> We have a biome, real-time strategy, world library. We have scripts. We
> have an ingestion pipeline that ports artifacts from a world context into
> our lakehouse indexing with different media types.

Terms that are **not** Archetype vocabulary and must not appear as if they
were:

- **"fact"** — > "There's no such thing as a fact in Archetype."
  (Everett, 2026-07-26). Results, observations, and artifacts have their own
  names; use them.
- **"embodiment"** — > "The term 'embodiment' is just incorrect here."
  (Everett, 2026-07-25). Domain-specific participant, policy, agent,
  environment, and task context remain domain-owned.

## Design posture

> We identified that the durability we were building distracted from the
> core mental model and concepts that actually drive archetype. Edge cases
> are not primary behavior, and by over-indexing on completeness maximalism,
> we lose coherence because we cannot tell what we need to attend to anymore.
>
> — Everett, 2026-07-26

> Autonomous software development isn't just about moving fast. It's about
> the ability to move fast and also understand the system underneath it.
>
> — Everett, 2026-07-26

> The only thing that is consistent across this project is me, the architect
> and its creator.
>
> — Everett, 2026-07-26

---

## Maintenance

- Additions are verbatim, dated quotes ratified by Everett. No summaries.
- When existing documentation conflicts with this file, this file wins and
  the documentation is the bug.
- Agents proposing new concepts or names must label them proposals and leave
  them out of docs, specs, and schemas until ratified here.
