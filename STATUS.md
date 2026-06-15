# STATUS — read this first when you come back

_Single source of truth for "what are we doing + is it green or red." Claude keeps the top
sections current; run `./status.sh` for live git/Modal/run signals._

## 🎯 Goal
Prove the **GEPA-on-frozen-VLA** pipeline works (LIBERO-Plus Language, zero weight changes),
then run the overnight A/B + GEPA experiment (#19). Pre-flight gate before that big run.

## 🚦 Health: YELLOW (→ GREEN when the smoke completes)
- **Code: GREEN.** Pipeline contract-correct; `when().otherwise(prompt)` footgun fixed
  (`956bb42`); GEPA lib parity-tested (5/5).
- **Launch: FIXED.** Switched from `modal run --detach` (local CLI drives the job → session
  reaped → Modal cancels) to **`modal deploy` + `spawn()`**: the job runs server-side, fully
  decoupled; submit returns in ~1s. Smoke re-submitted this way → running in the cloud.
- **Pending:** one clean completed smoke result, then #23 → #19.

## ▶️ Next action
Fetch the in-flight smoke (`make status`, or `submit_gepa.py --fetch <id>`). On green →
#23 headroom scan → #19 overnight — all via `submit_gepa.py` (spawn), **never** `modal run`.

## 🧱 Blockers
- None open. Long runs now use `bench/libero/submit_gepa.py` → `spawn()` on the deployed
  `archetype-gepa-daft` app. **Never launch the overnight via `modal run`** — it ties the job
  to this session and gets cancelled.

## ✅ Done recently
- HTML run-book: `docs/design/gepa-run-book.html` (open it for the full end-to-end).
- GEPA promoted to lib `src/archetype/optimize/gepa.py` (+ demo, parity test).
- daft-examples PR #42 (standalone GEPA-as-Daft gist, OpenRouter).
- Pushed 4 commits (#20); redeployed both env workers (#21).

## 🔑 Key files
- Pipeline: `bench/libero/gepa_daft.py`  ·  Algorithm: `src/archetype/optimize/gepa.py`
- Submit/fetch (spawn): `bench/libero/submit_gepa.py`  ·  Run-book: `docs/design/gepa-run-book.html`
- Headroom probe: `bench/libero/libero_plus_sweep.py`

## 📌 Standing decisions
- Zero weight changes (test-time only). Sim `pass` = ground truth (never an LLM judge).
- Tier-1 legitimacy ordering: `A-default ≤ D-paraphrase < B-seed < C-oracle`.
- Reflect prompt is answer-blind (never names the correct target).
