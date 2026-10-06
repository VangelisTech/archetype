# Prefab Inheritance and Instantiation Policies — Design

**Status:** Proposed, awaiting Everett's ruling. Synthesizes the 2026-07-20
asset-graph design discussion with what shipped through #617. Nothing here
is implemented; `docs/design/prefab-registry.md` R2/R7 are the shipped
foundation this builds on.

---

## 1. The tension this resolves

Flecs-style prefabs share definition state: instances store only overrides,
and inherited values live on the asset. That is the right storage and
authoring model for mission topologies — a thousand mission instances should
not each carry a fully materialized copy of their agent topology and
policies. But live prototype inheritance mutates: editing the asset changes
every instance retroactively, which violates the shipped D5 ruling
(copy-on-instantiate; re-instantiation is the upgrade path) and the
reproducibility that grading requires.

The ledger dissolves the tension. `IsA` lineage now carries the full version
coordinate `(world, run, at_tick)` (#615, #617), so **inherited state can be
a projection pinned to the instantiated version**: join the instance's
override rows over the template's values *at that coordinate*. Shared state
stays in the library world; instances store only deltas; and because the
join is pinned, editing the template never retroactively changes an
instance. Flecs amortizes inherited lookup in query caches; Archetype
amortizes it as a lazy join against a historical slice.

**Proposed ruling: INHERIT means version-pinned projection, never live
mutation.**

---

## 2. Instantiation policies per component

Copy-everything is an accidental `deepcopy()` framework. Each component
class declares what instantiation does with it:

```python
class InstantiationPolicy(StrEnum):
    COPY = "copy"        # materialize the template's values (today's behavior)
    INHERIT = "inherit"  # store no row; effective value is the pinned projection
    RESET = "reset"      # attach the component with its class defaults
    OMIT = "omit"        # never attaches to instances
```

The shipped code already has two of these de facto: the `Prefab` marker is
OMIT, everything else is COPY. Mission examples of the other two:
`MissionPolicy` INHERIT (shared, versioned definition), `RetryCounter` and
`MissionStatus` RESET (operational state must start fresh). Declaration site:
a class attribute on `Component` subclasses, defaulting to COPY so nothing
shipped changes behavior.

---

## 3. Variant derivation needs stable node identities

`CodingMission IsA SoftwareMission` between *templates* currently means
nothing to `instantiate`. Before prefab-to-prefab inheritance can compose
subtrees, same-named children across base and derived templates are
ambiguous graph overlays. Proposed: each authored child carries a stable
prefab-local identity (`PrefabNodeKey("validator")`), and derivation states
its intent explicitly — override, extend, remove (tombstone), or add. Names
alone are insufficient; the verbs must be explicit or variants become
unreviewable.

---

## 4. Relation-copy policies (R7's sanctioned broadening)

R7 holds: `instantiate` rebuilds only `ChildOf` and records `IsA`. The
deliberate broadening, when needed, is `InstantiationResult(root_id,
id_map)` plus per-relation copy policies distinguishing:

- **internal** relations (both endpoints inside the copied subtree) — remap
  to the new ids (`completion_gate Observes test_runner`);
- **shared** references (endpoint outside the subtree) — keep pointing at
  the shared asset (`reviewer Uses CodeReviewPolicy`);
- **runtime** references — bound at instantiation by the driver, never
  copied.

---

## 5. Core matching asks: inherited capabilities and template exclusion

An INHERIT instance stores no physical row for the inherited component. That
means today's archetype-signature matching cannot see the capability:
`AsyncQueryManager.query()` will omit the instance, and `AsyncSystem` will not
run a processor that requires the inherited type. A family-side value overlay
after selection is too late to repair either decision.

Before INHERIT can be ruled implementable, the engine design must therefore
choose one of two explicit strategies:

- make query and processor eligibility operate on an effective signature that
  includes version-pinned inherited component types; or
- materialize a durable instance-side capability footprint that participates
  in matching while the component value itself remains inherited.

The selected strategy must preserve lazy, snapshot-pinned reads and must not
turn a template edit into a retroactive signature change.

Library entities must not execute. Today a template carrying `Depth` will be
processed by `DepthProcessor` — inertness is a convention, not an
enforcement. Proposed: `excludes: tuple[type[Component], ...]` on processor
declarations, honored by archetype matching, with `Prefab` excluded by
default for domain processors. Negative matching and inherited positive
matching are both engine-level requirements; everything else can remain
family-level.

---

## 6. Open questions for the ruling

1. INHERIT-as-pinned-projection: confirm the semantic, then decide where the
   effective-state join and effective-signature matching live. A family-only
   projection helper is insufficient for processor/query eligibility.
2. `InstantiationPolicy` declaration site and default (proposed: class
   attribute, default COPY).
3. Whether variant derivation (§3) lands before or after `PrefabLibrary` —
   it reshapes the authoring surface.
4. Matching (§5): choose effective signatures versus a materialized capability
   footprint, and decide whether negative matching lands in the same change.
