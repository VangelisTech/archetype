# STATUS — read this first when you come back

_Single source of truth for "what are we doing + is it green or red." Claude keeps the top
sections current; run `./status.sh` for live git/Modal/run signals._

## 🎯 Goal
Prove the **GEPA-on-frozen-VLA** pipeline works (LIBERO-Plus Language, zero weight changes),
then run the overnight A/B + GEPA experiment (#19). Pre-flight gate before that big run.

## 🚦 Health: YELLOW
- **Code: GREEN.** Pipeline is contract-correct; the `when().otherwise(prompt)` footgun is
  fixed + committed (`956bb42`); GEPA lib is parity-tested (5/5).
- **Runs: BLOCKED.** Long Modal jobs launched from this session keep getting **cancelled
  ~3–10 min in** (environment reaps the client → Modal cancels). Smoke #2 and #3 both got
  past the bug into real rollouts with **no code error**, then were killed. So we do **not
  yet have one cleanly-completed smoke**. This is a *launch-method* problem, not a code problem.

## ▶️ Next action
Re-launch the smoke via **deploy + `spawn()`** (server-side, decoupled from this session — no
local streamer to kill). Once one smoke completes green → #23 headroom scan → #19 overnight.

## 🧱 Blockers
- Detached `modal run` from this session does not survive. Fix = `modal deploy` the app, then
  `run.spawn(...)` and read the result from the ledger / function-call id.

## ✅ Done recently
- HTML run-book: `docs/design/gepa-run-book.html` (open it for the full end-to-end).
- GEPA promoted to lib `src/archetype/optimize/gepa.py` (+ demo, parity test).
- daft-examples PR #42 (standalone GEPA-as-Daft gist, OpenRouter).
- Pushed 4 commits (#20); redeployed both env workers (#21).

## 🔑 Key files
- Pipeline: `bench/libero/gepa_daft.py`  ·  Algorithm: `src/archetype/optimize/gepa.py`
- Run-book: `docs/design/gepa-run-book.html`  ·  Headroom probe: `bench/libero/libero_plus_sweep.py`

## 📌 Standing decisions
- Zero weight changes (test-time only). Sim `pass` = ground truth (never an LLM judge).
- Tier-1 legitimacy ordering: `A-default ≤ D-paraphrase < B-seed < C-oracle`.
- Reflect prompt is answer-blind (never names the correct target).
