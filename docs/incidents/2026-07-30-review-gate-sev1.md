# SEV-1: Review gate and CI stalled Archetype delivery

- **Incident date:** 2026-07-23 through 2026-07-30
- **Declared:** 2026-07-30
- **Severity:** SEV-1 (internal delivery-control-plane outage)
- **Status:** Mitigation in progress
- **Incident commander:** [Owner: release owner]
- **CI owner:** [Owner: repository maintainer]
- **Postmortem owner:** [Owner: engineering effectiveness]

## Executive summary

Archetype's repository harness became the critical-path failure it was meant to
prevent. A probabilistic AI review system was made a mandatory merge authority,
ran a six-lens by two-reviewer audit on every PR head, and was coupled to custom
thread-resolution, auto-merge, merge-queue, and scheduled reconciliation
machinery. Small corrective pushes invalidated the prior review and restarted
the full audit. Provider quota failures, model-schema failures, invalid evidence
paths, prompt-size limits, and findings in the gate's own repair PRs all became
merge blockers.

The broader CI profile compounded the problem. Every ordinary PR ran full tests
on Python 3.12 and 3.13, real R2 integration, conformance/reliability/capability
evals, installed-wheel scenarios, source examples, and four documentation jobs.
The system optimized for exhaustive evidence at every commit rather than fast
feedback during development and complete evidence at an appropriate release
boundary.

The v0.5.0 release target was the morning of July 27. On July 30, the release PR
and its prerequisite work remained open. The release owner stopped all work
after reaching a cognitive-load limit and made the executive decision to remove
the AI review gate from merge authority, disable the merge queue and review
ruleset, simplify CI, and use Codex and Cursor Bugbot reviews as non-blocking
human inputs in the interim.

No production service was unavailable, no customer data was lost, and the
session record states that no external user was yet depending on Archetype.
This is nevertheless SEV-1 because the project's delivery control plane caused
a total stop-work event on the sole release path during a release-critical
window.

## Severity rationale

This incident meets the internal SEV-1 threshold:

- The only supported path to ship the release was effectively unavailable.
- The failure was systemic across the open PR set, not isolated to one product
  branch.
- The control plane recursively blocked changes intended to repair the control
  plane.
- The sole human release owner reached an explicit cognitive-load ceiling and
  ordered all work stopped to recover.
- The release missed its declared target by more than three days.

The severity is about delivery-system availability and human sustainability,
not production-user impact.

## Impact

### Developer impact

- The release owner repeatedly shifted from product work to CI authentication,
  reviewer-provider routing, rulesets, review threads, queue state, and workflow
  failures.
- Session evidence shows concern about review churn by July 23, a request to
  stop for harness repairs on July 24, emergency provider and ruleset work on
  July 26, manual-merge instructions on July 27, a reported 200 minutes spent
  on PR #724 on July 28, and a full project stop on July 30.
- The 27-session workspace record contains 313 human prompts. Sixty mention
  review, twelve mention the footgun system, twelve mention the merge queue,
  eleven mention churn, and seventeen mention usage, tokens, or credits.
- The same record contains 3,370 distinct agent tool calls whose command invoked
  GitHub PR/run/API or repository review-state helpers. This is an operational
  toil signal, not a precise measure of human time.
- Product work and release judgment were repeatedly interrupted by review
  infrastructure work. The project's human bottleneck became supervision of
  the verifier rather than comprehension of Archetype.

### Release impact

- [PR #724](https://github.com/VangelisTech/archetype/pull/724), the A8 hosted
  composition prerequisite, remained open after 37 commits, 91 changed files,
  and a net diff of more than 9,500 lines.
- Its branch triggered 21 deterministic-review runs. Nine were cancelled and
  twelve failed; none succeeded. Their accumulated workflow elapsed time was
  322.5 minutes. Because runs overlapped and some were cancelled by later
  pushes, this number is neither billed job-minutes nor additive human delay.
- On July 28, one #724 review run completed all twelve primary lens jobs and
  reached two adjudications after 33.7 minutes, then failed because the
  adjudicators returned evidence paths that the gate rejected.
- On July 30, another #724 review ran for 37.4 minutes, completed all twelve
  primary lenses, launched four adjudications, and again failed on an invalid
  evidence path.
- At the July 30 evidence snapshot, all seven open PRs had a failed or cancelled
  `review-complete` result. Several also had ordinary CI failures, were behind
  main, or had merge conflicts, so the review gate was not the sole technical
  blocker on every PR. It was the common failed control-plane dependency.

### Customer impact

- No production outage or data loss occurred.
- No existing external customer was blocked from an already supported service.
- Prospective users did not receive the planned v0.5.0 release.
- Confidence in the project's ability to ship and in the intelligibility of its
  own development process was materially reduced.

## Detection

The incident was detected by the release owner through accumulated human toil,
not by an automated delivery SLO.

Warning signs were present from the first day:

- duplicate review of an unchanged draft-to-ready head;
- a green auto-merge guard that left a PR armed while unresolved threads still
  blocked it;
- cancelled reviews publishing stale blocker-shaped evidence;
- hosted review completing only after every deterministic check was green;
- repeated provider quota and schema failures;
- manual thread-resolution and queue-rearming rituals;
- release work paused to repair the harness that was reviewing the release.

No alert fired on PR lead time, review-gate failure rate, repeated review of the
same head, model-call budget, or percentage of engineering effort spent on the
harness. The terminal signal was human: "the entire Archetype project is
stalled on its own review gate" followed by an executive stop-work decision.

## Evidence and metrics

### Conductor workspace

The investigation reviewed **27 of 27 sessions** associated with Conductor
workspace ID `86d3c6ad-1416-47b9-860f-caa1f8faf2bc`:

| Agent surface | Sessions | Non-empty |
|---|---:|---:|
| Claude | 11 | 11 |
| Codex | 11 | 10 |
| Cursor | 4 | 3 |
| ACP/OpenCode | 1 | 1 |
| **Total** | **27** | **25** |

The read-only database snapshot contained 330,071 `session_messages` rows:
329,758 structured assistant/system/tool/result events and 313 direct human
prompts. Two sessions were empty launches. Every row was included in the
inventory and classification queries; direct human prompts were then reviewed
chronologically, and assistant/tool events were analyzed by event type, command
class, errors, and incident keywords.

### GitHub Actions, July 22–30

For the deterministic-review workflow:

| Measure | Value |
|---|---:|
| Workflow runs | 379 |
| PR-target review runs | 317 |
| Merge-group skips | 62 |
| PR-target successes | 107 |
| PR-target failures | 156 |
| PR-target cancellations | 54 |
| PR-target success rate | 33.8% |
| PR-target runs lasting at least 10 minutes | 124 |
| PR-target runs lasting at least 20 minutes | 28 |
| Accumulated PR-target workflow elapsed time | 3,053.5 minutes |

Workflow elapsed time measures wall-clock duration per workflow, not aggregate
runner time, model tokens, cost, or human wait. Runs overlapped. The repository
did not retain a trustworthy end-to-end token and cost ledger, which is itself
a control gap.

Over the same period, the Quality workflow ran 404 times and the Docs workflow
ran 400 times. There were 275 PR runs of each. The Auto-merge workflow ran 913
times, the merge-group recheck ran 135 times, and the scheduled queue
re-evaluator ran 52 times.

At the incident-state commit, direct gate, queue, helper, and test machinery
comprised approximately 8,069 lines, excluding prompts, skills, documentation,
and historical artifacts:

- 1,233 lines of deterministic-review workflow;
- 559 lines of auto-merge, merge-group, and queue-re-evaluation workflows;
- 3,868 lines of review gate, contracts, and aggregation Python;
- 380 lines of PR-state/thread helper scripts;
- 2,029 lines of direct review-gate tests.

Twenty-three commits touching the review/queue implementation or its direct
tests landed between July 24 and July 27.

## Timeline

Times are shown in UTC where available.

| Time | Event |
|---|---|
| Jul 23, 18:59 | The release owner warned that the program could not spend hours sitting in review gates and asked for parallel execution. |
| Jul 23 | PR #639 reviewed the same exact head as a draft and again when marked ready. The friction log recorded unnecessary critical-path latency and model spend. |
| Jul 24, 02:32 | The release owner reported several hours of review churn on the next PR. |
| Jul 23–24 | PR #645 exposed that `review-complete` could be green while unresolved threads still blocked merge. The resulting queue-readiness fix became PR #654. |
| Jul 24, 20:36 | PR #661 merged the six-lens by two-reviewer multi-backend matrix. |
| Jul 24–25 | PRs #662, #664, and #668 added severity tiers, oversized-evidence degradation, and evidence/publication separation. |
| Jul 24 | The project paused product execution to harden the review and merge process. |
| Jul 26, 18:24 | The release owner stated that the harness had to be fixed before continuing because merge-queue races were blocking A1–A7. |
| Jul 26, 19:44 | Kimi quota exhaustion forced an emergency backend change in PR #697. |
| Jul 26, 21:23 | Claude's spend-cap outage forced another backend change in PR #699. |
| Jul 26, 21:42–Jul 27, 02:06 | Queue recheck, scheduled re-evaluation, outage-proof topology, schema, scope, and design-brief repairs landed in PRs #689, #690, #698, #700, #701, and #707. |
| Jul 26, 21:49 | The release owner rejected a 30-minute review cycle and asked what risk remained in merging. |
| Jul 27, 02:05 | The release owner ordered the merge queue turned off and manual merging used for the release path. |
| Jul 27, 04:20 | The release owner required explicit agreement on which checks would be honored. |
| Jul 27, 17:53 | PR #724 opened. |
| Jul 27–28 | PRs #725, #726, #730, #731, and #732 repaired prompt budgets, blocked verdict handling, reviewer tools, Codex retries, and review completion. |
| Jul 28, 00:28 | A #724 review failed because both seats for one lens produced infrastructure failures; no product verdict existed. |
| Jul 28, 03:13 | A #724 review reached finalization after 25.9 minutes, then failed because the human-design prompt exceeded its 900,000-character repository budget. |
| Jul 28, 05:18 | A #724 review reached two adjudications after 33.7 minutes; both returned invalid evidence paths and the entire gate failed. |
| Jul 28, 06:23 | The release owner reported that 200 minutes had been spent on #724 without a merge. |
| Jul 28 | A real review finding—failed validation losing local commit evidence—was fixed and promoted into a regression test. The corrective push restarted review of the full PR. |
| Jul 30, 06:46 | PR #740 opened to accept line-suffixed adjudication paths. The same gate reported three blocking findings on its own repair PR, leaving the repair unmerged. |
| Jul 30, 15:31 | The latest #724 review completed twelve primary lenses and launched four adjudications. One invalid line-suffixed evidence path failed the gate after 37.4 minutes. |
| Jul 30, 22:06 | The release owner declared the project stalled, reached a cognitive-load limit, and asked whether to remove the gate. |
| Jul 30, 22:11 | Executive decision: stop all work, remove the review gate from the repository's merge path, simplify CI, disable merge queue/review rules, and rely temporarily on Codex and Cursor Bugbot review. |

## Root causes

### Primary technical root cause: probabilistic review became synchronous merge authority

The system treated model availability, schema compliance, evidence formatting,
adjudication output, publication, thread state, and merge readiness as one
blocking transaction. A failure anywhere denied delivery even when ordinary
tests were green and no validated product defect existed.

This conflated three different concerns:

1. discovering novel risks;
2. enforcing known deterministic invariants;
3. authorizing a merge.

AI review is suitable for discovery and human decision support. It was not
reliable enough to be the repository's availability-critical authorization
service.

### Full-matrix review ran at the wrong cadence

The gate ran six lenses with two independent reviewers against the full diff on
every new head. It could add per-lens correction attempts, selective
adjudications, and a separate human-design brief. A small repair push therefore
repeated work unrelated to the repair.

The workflow had no path/risk-based lens selection, no reuse of unaffected
receipts after a delta, and no distinction among task-time checks, PR readiness,
repair verification, and release audit. Episode-level audit ran at commit
cadence.

### Failure domains were amplified instead of isolated

The gate failed closed for:

- exhausted provider quotas and subscription spend caps;
- runner/tool mismatch;
- model return-schema failures;
- prompt-size overflow;
- invalid or line-suffixed evidence paths;
- missing adjudication receipts;
- blocking findings in the gate's own repair PRs.

Fail-closed validation correctly prevented false evidence from being accepted,
but the architecture offered no bounded fallback to a human decision. A safe
validator failure therefore became a repository outage.

### Merge automation added a second distributed state machine

GitHub Actions cannot trigger directly on review-thread resolution. The
repository compensated with custom arm/disarm logic, review-submission signals,
scheduled reconciliation, queue rechecks, and helper scripts. Races among head
changes, review publication, thread arrival, queue entry, and merge groups
created more states than one maintainer could reliably supervise.

The result was a control plane beside the product control plane, with its own
leases, freshness rules, exact-head identity, recovery paths, and operational
runbooks.

### CI used release evidence as ordinary PR feedback

The PR workflow ran:

- full repository tests on two Python versions;
- coverage upload;
- static, lock, contract, and benchmark audits;
- real Cloudflare R2 integration with a retry;
- conformance, reliability, and capability evals;
- package build and installed-wheel operational scenarios;
- source examples and receipts;
- spelling, Markdown, links, and full docs build on every PR.

These checks are individually defensible. Their universal cadence was not.
Fast change-local feedback, main-branch evidence, and release-candidate proof
were not separated.

## Organizational root causes

### Completeness maximalism displaced the release objective

The program explicitly intended a fast, simplifying v0.5 refactor. When the
harness exposed edge cases, the response was usually another contract,
workflow, provider seat, receipt, guard, or reconciliation path. Local safety
improvements accumulated into global operational complexity.

The team optimized each discovered failure mode without repeatedly asking
whether the review gate itself should continue to exist as a merge authority.

### No stop-loss or service-level objective existed

There was no agreed maximum for:

- PR feedback latency;
- model invocations per head;
- total review spend;
- retries or corrective cycles;
- harness work as a share of release work;
- number of infrastructure failures before human override.

Warnings were recorded in friction logs, but no threshold automatically reduced
scope or removed the gate from authority.

### The verifier was allowed to self-host before it was operationally mature

Repairs to the review system had to pass the review system. This circular
dependency made the control plane harder to recover precisely when it was
unhealthy. PR #740 is the clearest terminal example.

### One human remained the coherence and escalation authority

Many agents operated in parallel, but one person retained product vocabulary,
release priority, ruleset authority, credential decisions, and final merge
judgment. Context was repeatedly reconstructed across sessions and handoffs.
Parallel agent capacity increased the number of outputs requiring supervision;
it did not remove the human cognitive bottleneck.

## Contributing factors

- Release PRs were unusually large. #645 changed 210 files; #646 changed 101;
  #724 changed 91. Large diffs increased review time and finding ambiguity.
- Frequent corrective pushes cancelled in-flight work and started new
  exact-head reviews.
- Provider choice was influenced by expiring subscription quotas and unused
  credits rather than a stable availability contract.
- Token and cost telemetry was discussed but not implemented, so the system
  could not enforce an economic budget.
- Review findings, infrastructure failures, and malformed reviewer output were
  all rendered as red merge checks, forcing humans to inspect logs to learn
  which category had occurred.
- Auto-merge and merge-queue rules made emergency admin bypass non-obvious or
  unavailable.
- The repository contained stale instructions that advised rerunning the gate
  after resolving threads, which republished findings and recreated unresolved
  work.
- Full deterministic CI was already heavy, so review latency arrived after
  substantial prior waiting rather than providing early risk selection.

## What worked

- The review system found real defects, including cleanup retry holes, lifecycle
  boundary failures, and the #724 commit-evidence loss.
- Confirmed behavioral findings were sometimes converted into deterministic
  regression tests. This is the durable value to retain.
- Exact-head binding, scope validation, evidence digests, and invalid-path
  rejection prevented malformed model output from masquerading as verified
  evidence.
- Ordinary tests, static architecture checks, installed-wheel scenarios, and
  provider dogfood produced substantial release evidence.
- Append-only execution findings and retained workflow receipts made this
  incident reconstructable.
- The release owner recognized the human sustainability failure and stopped
  work rather than continuing to compound it.

## What failed

- Review availability was below any acceptable merge-gate threshold: only 33.8%
  of PR-target review workflows succeeded during the measured period.
- The gate could not distinguish "no verdict because infrastructure failed"
  from "the code is unsafe to merge" at the repository-authority boundary.
- A tiny repair could not reuse unaffected successful lens evidence.
- The same gate reviewed and blocked its own recovery changes.
- Review findings were not systematically retired from model review after a
  lint, contract test, or scenario became the authoritative oracle.
- CI did not select checks by changed path, affected contract, or lifecycle
  boundary.
- Queue automation required too much human state reconstruction.
- Metrics described evidence correctness but not developer latency, cognitive
  load, model calls, or cost.
- Agents continued to babysit workflows after the release owner had signaled
  that the process was too onerous. Status and override options were not
  surfaced early enough.

## Corrective actions

### P0 — restore delivery

| Action | Owner | Status |
|---|---|---|
| Remove the Deterministic Review Gate workflow and its required contexts from the merge path. Do not leave a dormant automatic trigger that can recreate comments or checks. | [Owner: CI owner] | In progress |
| Disable the merge-queue and review rulesets; retain only simple PR integrity and deterministic required checks. | [Owner: repository admin] | Completed and verified 2026-07-30 |
| Remove review-complete, footgun, queue-readiness, arm/disarm, and scheduled re-evaluation dependencies from merge authorization. | [Owner: repository admin] | In progress |
| Simplify normal PR CI to a fast deterministic profile and move full/release evidence off the per-push critical path. | [Owner: CI owner] | In progress |
| Cancel obsolete review and queue runs, then reassess every open PR using deterministic checks and direct human judgment. | [Owner: release owner] | Pending |
| Use Codex and Cursor Bugbot as advisory reviewers until a replacement policy is explicitly approved. Their unavailability or disagreement must not create a required status context. | [Owner: release owner] | Approved |
| Preserve confirmed regression tests, lints, operational scenarios, historical receipts, and the lens taxonomy; quarantine retired gate machinery and do not continue repairing it on the release critical path. | [Owner: repository maintainer] | Approved |

### P1 — establish a sustainable deterministic CI contract

| Action | Owner | Due |
|---|---|---|
| Define a normal-PR SLO: first actionable feedback within 10 minutes and no external-service dependency unless the changed paths require it. | [Owner: engineering effectiveness] | [Date] |
| Add explicit change routing: every file maps to relevant deterministic checks, an explicit no-special-check classification, or `unknown`. Unknown fails configuration review, not the product PR by silently running everything. | [Owner: CI owner] | [Date] |
| Run one Python version, static checks, and affected tests on normal PRs. Run compatibility Python, full suites, process/reliability, R2, exact-wheel, and broad docs evidence on main, nightly, or release candidates according to risk. | [Owner: CI owner] | [Date] |
| Add test-impact selection with a conservative fallback and record why each check ran. | [Owner: test owner] | [Date] |
| Create a finding-disposition register: reproducible behavior to a contract test, static invariant to a lint/audit, lifecycle/provider failure to an operational scenario, performance to a benchmark, and design taste to advisory human guidance. | [Owner: quality owner] | [Date] |
| Once deterministic coverage is accepted, retire that exact finding class from routine AI review. Support active, superseded, and retired states. | [Owner: quality owner] | [Date] |
| Add CI telemetry for queue time, run time, cancellation, retry, external-service use, and estimated cost. Establish a hard per-PR compute budget. | [Owner: engineering effectiveness] | [Date] |
| Define a documented emergency merge procedure that does not require changing global policy under pressure. | [Owner: repository admin] | [Date] |

### P2 — research adaptive review outside merge authority

| Action | Owner | Due |
|---|---|---|
| Replay proposed review-selection policies against historical receipts and confirmed/refuted findings. Measure blocker recall, false-positive rate, calls, elapsed time, and cost before any live deployment. | [Owner: research owner] | [Date] |
| If AI review returns, begin with one risk-selected primary reviewer. Escalate to a targeted challenger only for a concrete claim, missing deterministic evidence, or disagreement. | [Owner: quality owner] | [Date] |
| Reserve full multi-lens audits for an explicit release/audit boundary against a frozen candidate. Never run them automatically on every corrective head. | [Owner: release owner] | [Date] |
| Separate task/tick verification, PR/episode judgment, release audit, and post-episode reflection. Harness-learning work must run asynchronously from the merge-critical path. | [Owner: architecture owner] | [Date] |
| Evaluate friction logs periodically as biased observations, then promote only human-ratified improvements through ordinary reviewed PRs. Do not permit silent self-modification of policy. | [Owner: engineering effectiveness] | [Date] |
| Set a reinstatement bar for any AI gate proposal: deterministic bypass for infrastructure failure, bounded human fallback, receipt reuse after deltas, measured availability above 99%, and demonstrated net reduction in merge lead time. | [Owner: repository admin] | [Date] |

## Methodology

This postmortem used four evidence sources:

1. The Conductor SQLite database at
   `/Users/everettkleven/Library/Application Support/com.conductor.app/conductor.db`
   was opened read-only with SQLite `mode=ro&immutable=1`. All **27/27**
   sessions for workspace ID
   `86d3c6ad-1416-47b9-860f-caa1f8faf2bc` and all associated
   `session_messages` rows were inventoried. Human prompts were reviewed in
   chronological session order. Structured events were classified by type,
   tool, command, error, and incident term.
2. Relevant local `.context` evidence was reviewed, including the v0.5
   execution findings, queue-readiness diagnosis, harness handoffs, review
   artifacts, and the four July 27 conversations about adaptive linting,
   test selection, code-quality judgment, memory/forgetting, friction logs,
   and mission/tick/episode semantics.
3. GitHub was inspected read-only with `gh`: PR metadata, workflow
   configuration at `origin/main`, Actions run/job history, failed-run logs,
   and repository rulesets. No GitHub state was changed by this investigation.
4. Repository history and line counts were inspected locally to establish the
   growth and cadence of direct review/queue machinery.

Raw private transcripts are not reproduced here. User statements are
summarized, secrets are excluded, and workflow metrics are labeled according
to what they actually measure. Counts are a point-in-time snapshot and may
change as GitHub retention or the Conductor session continues.

## Final lesson

The failure was not that Archetype cared too much about correctness. The
failure was assigning correctness discovery, evidence production, merge
authorization, queue coordination, and process learning to one synchronous
system with no budget or escape hatch.

Known invariants belong in deterministic tests and lints. Novel review belongs
in bounded advisory judgment. Release proof belongs at the release boundary.
The delivery control plane must remain smaller, faster, and more reliable than
the product it governs.
