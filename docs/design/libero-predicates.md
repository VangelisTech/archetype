# LIBERO Predicate Vocabulary

**Document type:** Reference. This closes core-loop §10 (the open build-time
item): the enumerated, *verified* predicate vocabulary so the GEPA scorer's
constructed-subgoal helper (`eval_subgoals`) is concrete, not ad hoc.

**How this was produced:** introspected live inside the LIBERO container on
Modal by `bench/libero/predicate_vocab.py` (app `libero-predicate-vocab`,
verified 2026-06-13 against `libero_spatial` task 0). Nothing here is guessed:
the registry, the dispatch path, and the predicate semantics are read from the
installed LIBERO source (SHA-pinned `8f1084e…`, same image as `modal_worker.py`)
and the live-eval probes were run against a reset MuJoCo state. Re-run with:

```
modal run bench/libero/predicate_vocab.py
modal run bench/libero/predicate_vocab.py --suite libero_object --task-id 3
```

---

## 1. The dispatch path (how `_eval_predicate` works)

A goal/subgoal is a parsed list `[name, arg1, (arg2)]` of *string* names. The
chain is fixed and short — there is no separate parser, the strings index a
dict directly:

```python
# libero/libero/envs/bddl_base_domain.py  (the live env, env.env)
def _eval_predicate(self, state):
    if len(state) == 3:                                   # binary
        return eval_predicate_fn(state[0],
                                 self.object_states_dict[state[1]],
                                 self.object_states_dict[state[2]])
    elif len(state) == 2:                                 # unary
        return eval_predicate_fn(state[0],
                                 self.object_states_dict[state[1]])

# libero/libero/envs/predicates/__init__.py
def eval_predicate_fn(predicate_fn_name, *args):
    assert predicate_fn_name in VALIDATE_PREDICATE_FN_DICT
    return VALIDATE_PREDICATE_FN_DICT[predicate_fn_name](*args)   # NOTE: not .lower()
```

`_check_success` is just the **conjunction** of `_eval_predicate` over
`parsed_problem["goal_state"]`. So evaluating an arbitrary constructed subgoal
is literally `env.env._eval_predicate(["on", "akita_black_bowl_1", "plate_1"])`
— same code path the success check uses, which is why scorer subgoals are
ground truth, not an approximation.

Three hard facts the scorer must respect (all verified):

1. **Args are object/region *names* (strings)**, resolved through
   `object_states_dict`. A name that isn't a key raises `KeyError`. Valid keys
   = every object **and every named region/site** (see §4).
2. **`eval_predicate_fn` does NOT lowercase** the name (only the unused
   `get_predicate_fn` helper does). BDDL files happen to be lowercase, so emit
   subgoal names lowercase: `"on"`, not `"On"`.
3. **`_eval_predicate` only handles len 2 or 3** (unary / binary). A bare
   0-arity predicate name (`true`/`false`) returns `None` through this method;
   call `eval_predicate_fn` directly if you ever need those. The scorer never
   does — every useful subgoal is unary or binary.

---

## 2. The complete registered vocabulary

`VALIDATE_PREDICATE_FN_DICT` (the *entire* dispatch table, verbatim) has **10
entries**. This is the whole language — there is no plugin/extension mechanism
loaded at runtime; `Stack`, `InContact`, `Temporal` exist as classes but are
**commented out of the registry** and therefore *not callable* via
`_eval_predicate` (see §3).

| name              | arity | class               | semantics (one line)                                                                 |
|-------------------|:-----:|---------------------|--------------------------------------------------------------------------------------|
| `on`              | 2     | `On`                | `arg2.check_ontop(arg1)` — arg1 rests on top of arg2 (surface/region). **2nd arg = the surface.** |
| `in`              | 2     | `In`                | `arg2.check_contact(arg1) AND arg2.check_contain(arg1)` — arg1 inside arg2's bbox + touching. **2nd arg = the container.** |
| `up`              | 1*    | `Up`                | `arg1.world_z >= 1.0` — object raised above a fixed height. The native **lift / off-table** proxy. |
| `open`            | 1     | `Open`              | `arg1.is_open()` — any articulated joint of arg1 past its open threshold (drawer/door). |
| `close`           | 1     | `Close`             | `arg1.is_close()` — all articulated joints of arg1 within closed threshold.            |
| `turnon`          | 1     | `TurnOn`            | `arg1.turn_on()` — articulated affordance (e.g. stove knob) in the on range.           |
| `turnoff`         | 1     | `TurnOff`           | `arg1.turn_off()` — articulated affordance in the off range.                            |
| `printjointstate` | 1     | `PrintJointState`   | debug only — prints joint qpos, always returns `True`. **Not a real subgoal.**         |
| `true`            | 0     | `TruePredicateFn`   | constant `True`. (Not reachable through `_eval_predicate`; len must be 2 or 3.)         |
| `false`           | 0     | `FalsePredicateFn`  | constant `False`. (Same caveat.)                                                        |

\* `up` is *declared* `BinaryAtomic` upstream but its `__call__(self, arg1)`
takes a single object, so dispatch it as a **unary** predicate `["up", obj]`.
This is an upstream quirk, confirmed live (`["up", "akita_black_bowl_1"]`
returns `False` at reset; `["up", obj, x]` would error).

### Underlying geometry (so the scorer can reason about *why* a subgoal flips)

Read from `object_states/base_object_states.py`:

- **`check_ontop(other)`** (drives `on`): `self.z <= other.z` AND in-contact AND
  `‖self.xy − other.xy‖ < 0.03` m. (For **site/region** targets it instead uses
  the region's `in_box`, and `check_contact` is hard-True for sites — i.e.
  `on(obj, region)` is a pure xy/z containment test, no physical contact needed.)
- **`check_contain(other)`** (drives `in`): `self.in_box(self_pos, other_pos)` —
  other's body center is inside self's bounding box.
- **`check_contact(other)`**: MuJoCo contact between the two bodies (always
  `True` when `self` is a site/region).
- **`is_open / is_close / turn_on / turn_off`**: iterate the object's joints and
  threshold their qpos. **Only defined for articulated objects** (cabinets,
  drawers, stoves). On a rigid object (bowl/plate) these **raise**
  (`too many indices for array` / no `joints`) — verified live.

---

## 3. Predicates that exist as classes but are NOT in the registry

Do **not** emit these as subgoals — `eval_predicate_fn` asserts the name is in
the dict and will `AssertionError`. They are dead code upstream:

| class                  | would-be semantics                                                            | status |
|------------------------|-------------------------------------------------------------------------------|--------|
| `Stack` (`stack`)      | `contact AND contain AND arg1.z > arg2.z` — a true vertical stack             | commented out of registry |
| `InContactPredicateFn` | `arg1.check_contact(arg2)` — raw MuJoCo contact, no containment               | commented out of registry |
| `TemporalPredicate`    | (referenced in a comment only; class not even present)                        | absent |

If a task needs stacking semantics, the scorer should **construct it from the
registered primitives** (`in` + a z-comparison via `object_states_dict[...]
.get_geom_state()["pos"][2]`) rather than relying on `stack`.

Also note: the *BDDL base package* registry could not be enumerated in this
image (`import bddl.activity` fails on a missing `IPython` dependency — LIBERO
only needs `bddl==1.0.1`'s predicate objects, not its activity tooling). That is
irrelevant to the scorer: **LIBERO ships its own `VALIDATE_PREDICATE_FN_DICT`
in `libero.libero.envs.predicates` and `_eval_predicate` dispatches against
that dict, not BDDL's.** The table in §2 is the complete operative vocabulary.

---

## 4. Constructed intermediate subgoals — what `eval_subgoals` should build

The goal_state of most tasks is one coarse predicate (e.g. `libero_spatial`
task 0 = `[["on", "akita_black_bowl_1", "plate_1"]]`). The scorer's value is in
**decomposing** that into per-tick intermediate subgoals it constructs and
evaluates itself. Live-verified building blocks, in failure-phase order:

| phase      | constructed subgoal                                  | how to build it (registered primitives only)                                                        |
|------------|------------------------------------------------------|------------------------------------------------------------------------------------------------------|
| **approach** | eef near object                                    | not a predicate — read `obs["{obj}_to_robot0_eef_pos"]` norm (core-loop §5). Predicate dict can't express this. |
| **grasp**    | object in contact with gripper / moving with eef   | no `grasp` predicate exists. Proxy: `up(obj)` becoming true, or eef-relative pose tracking the obj across ticks. |
| **lift**     | `["up", obj]`                                       | **directly registered.** `obj.world_z >= 1.0`. The cleanest "is it off the table" subgoal. ✔ dispatches |
| **transport**| `["on", obj, intermediate_region]` / region membership | use a **named region** as arg2: `["on", "akita_black_bowl_1", "main_table_between_plate_ramekin_region"]`. Region targets are sites → pure xy/z containment. ✔ |
| **place**    | the goal predicate itself, e.g. `["on", obj, "plate_1"]` | the task's own `goal_state` entry. Ground truth. ✔ |

**Region membership is first-class** and is the most useful constructed subgoal
beyond `up`/the goal. Every task scene exposes named pickup/dropoff regions in
`object_states_dict` — for `libero_spatial` task 0 they include
(verified live):

```
main_table_plate_region              main_table_ramekin_region
main_table_between_plate_ramekin_region   main_table_box_region
main_table_next_to_plate_region      main_table_stove_region
wooden_cabinet_1_top_region          wooden_cabinet_1_middle_region
wooden_cabinet_1_bottom_region       flat_stove_1_cook_region
main_table_table_center              main_table_table_front
```

Because regions are `SiteObjectState`s, `on(obj, region)` and `in(obj, region)`
reduce to a bounding-box test (`in_box`) with `check_contact` short-circuited to
`True`. So the scorer can ask "is the bowl over the *between* region right now?"
at any replayed tick with `["on", obj, "main_table_between_plate_ramekin_region"]`
— exactly the grounding the paraphrase strategy is being optimized to achieve.

### Recommended `eval_subgoals(obj_of_interest, goal_state)` recipe

For each task, construct and evaluate per tick:

1. `["up", o]` for each `o in obj_of_interest` (manipulands) — **lift** signal.
2. The raw `goal_state` predicate(s) — **place** signal / ground truth.
3. `["on"/"in", manipuland, region]` for each task-relevant region named in
   `object_states_dict` — **transport / region-membership** signal.
4. Articulated subgoals (`["open", fixture]`, `["turnon", fixture]`) **only**
   when the task scene has an articulated fixture (cabinet/drawer/stove); skip
   on rigid-only scenes (they raise on bowls/plates).

`grasp` and `approach` are **not** in the predicate language — derive them from
`obs` eef-relative poses, not from `_eval_predicate` (core-loop §5 already
lists `eef_to(obj, t)` and `find_grasp_attempts()` as REPL helpers for exactly
this gap).

---

## 5. Quick reference for the REPL scorer

```python
e = env.env
# Ground-truth evaluator — same path as _check_success:
e._eval_predicate(["on", "akita_black_bowl_1", "plate_1"])     # -> bool (place / goal)
e._eval_predicate(["up", "akita_black_bowl_1"])                # -> bool (lifted off table)
e._eval_predicate(["on", "akita_black_bowl_1",
                   "main_table_between_plate_ramekin_region"]) # -> bool (region membership)

# All valid arg names (objects + regions):
sorted(e.object_states_dict.keys())

# Task-relevant objects to build subgoals around:
e.parsed_problem["obj_of_interest"]   # e.g. ['akita_black_bowl_1', 'plate_1']
e.parsed_problem["goal_state"]        # e.g. [['on', 'akita_black_bowl_1', 'plate_1']]
```

**Registered subgoal names (the only ones that dispatch):**
`on` · `in` · `up` · `open` · `close` · `turnon` · `turnoff`
(plus debug `printjointstate`; constants `true`/`false` not reachable via
`_eval_predicate`).
