# Coding-agent mission production-readiness inventory

**Document type:** Dated readiness inventory, historical and non-normative.

**Captured:** 2026-07-18, after draft PR
[#487](https://github.com/VangelisTech/archetype/pull/487) was rebased onto the
merged application-family architecture.

This is the consolidated successor to two earlier lists: the eight seams found
during formalization and the original “missing before production-ready”
checklist. Duplicates have been merged, completed prerequisites have been
removed from the numbered queue, and new observability/evaluation work has
been placed behind its durability and security dependencies.

The numbers are priority and dependency order at this date. GitHub issues own
the live work; this page is the historical baseline against which reprioritizing
can be explained.

## Completed prerequisites

- The repository-harness and application-family refactor landed in
  [#475](https://github.com/VangelisTech/archetype/pull/475).
- The coding-agent vertical slice was rebased onto that merged tree and the new
  main verification profile passed.
- The prior prototype remains preserved in
  [#474](https://github.com/VangelisTech/archetype/pull/474); draft PR #487 is
  its architectural successor.
- Same-world operation serialization from
  [#457](https://github.com/VangelisTech/archetype/issues/457) has an
  implementation in the refactor lineage, but the issue should be reconciled
  and closed only after its published regression evidence is confirmed.

## Remaining work, in priority order

1. **Make the landing reviewable without losing the vertical proof.** Decide
   whether #487 remains one capability PR or is mechanically split into
   sandbox/auth transport, mission/artifact durability, and harness/benchmark
   slices. Do not merge both #474 and #487, and do not separate code from the
   contract tests that prove it. **Done when:** each review unit has a coherent
   authority boundary and the full stacked tree still passes the same gate.

2. **Extract provider-neutral execution primitives.** Move the shared harness,
   validator, commit, evidence, checkpoint, and process records out of the
   Modal module into a provider-neutral module. Apple must not import another
   provider's implementation module. **Tracking:**
   [#477](https://github.com/VangelisTech/archetype/issues/477). **Done when:**
   import/architecture tests reject provider-to-provider implementation edges.

3. **Decompose `run_attempt` into explicit phases.** Separate harness
   execution, validation, repository finalization, evidence capture,
   checkpointing, and artifact handoff behind typed records. **Done when:** a
   crash/fault test can stop and resume at every phase without rerunning an
   unrelated phase.

4. **Persist one typed mission transition graph.** Replace state strings and
   scattered validation with enums/typed records and a single fail-closed graph
   for attempt, task, and mission transitions. **Done when:** every invalid
   edge and invalid evidence combination has one executable rejection oracle.

5. **Add a durable pre-execution claim.** Record ownership/lease and the
   intended model submission before invoking a harness; document whether a
   crash at each boundary may repeat execution. Do not claim exactly once
   without provider support that proves it. **Done when:** recovery can
   distinguish never-started, possibly-submitted, running, and completed work
   without checkpoint reconciliation creating a new submission.

6. **Move artifact projection and `indexed` gating into mission finalization.**
   The application/mission path should construct `ArtifactBundleRequest` and
   enforce the configured `checkpointed` or `indexed` threshold; the example
   should not own the authoritative teardown loop. **Done when:** publication
   retry never creates another model attempt and an `indexed` policy advances
   atomically according to the documented transition contract.

7. **Retire or further isolate the six lazy-materialization exceptions.** The
   current sites represent external sandbox and object-store side effects, but
   phase decomposition may narrow their row/column inputs or move them to
   explicit adapter boundaries. **Done when:** every remaining `.collect()` or
   `.to_pylist()` has the smallest justified execution boundary and a current
   `lazy_audit.toml` owner.

8. **Add one pre-durability secret scanning and redaction policy.** Apply it to
   spans, live events, manifests, session logs, patches, worktree archives, and
   declared artifacts before upload/export. Keep credential files out by
   construction and use scanning as defense in depth. **Done when:** a shared
   negative corpus covers Codex, Claude, GitHub, Modal, OpenRouter, cloud, and
   generic bearer/key formats, with explicit quarantine/failure behavior.

9. **Pin every executable environment.** Pin Codex, Claude Code, OpenCode,
   provider SDK, base image, collector/proxy, and relevant package versions or
   digests in attempt evidence. **Done when:** a run can be reproduced or
   declared unavailable from recorded version identities, and upgrades have a
   dedicated live compatibility proof.

10. **Add a portable full-worktree archive.** Preserve ignored, untracked,
    rejected, and uncommitted changes in addition to the sanitized Git bundle,
    patches, `.context`, and declared outputs. **Done when:** a credential-free
    restore test reconstructs the terminal worktree byte-for-byte while the
    archive remains bounded, hashed, scanned, and retention-classified.

11. **Dogfood R2 and the remote Iceberg index end to end.** Exercise real Daft
    upload, content reuse, manifest publication, query, replay after injected
    failure, retention metadata, and authorized download against R2 rather than
    only the local object/Iceberg store. **Done when:** the remote proof covers
    both an accepted and rejected attempt and survives a process restart.

12. **Deploy the fleet reconciler and retention garbage collector.** Discover
    due worlds, acquire/renew per-publication leases, complete pending/uploads,
    expire provider-dependent claims, delete eligible object bytes and
    provider snapshots, and retain/tombstone audit rows. **Done when:** multiple
    reconcilers safely shard billions of indexed records without cross-world
    locking and all crash points are idempotent.

13. **Add an automatic sandbox supervisor.** Detect provider/process loss,
    consult the durable attempt and latest checkpoint, choose restore versus
    authenticated resume, and enforce retry/budget policy. **Done when:** fault
    injection kills the controller and sandbox in every phase and the
    supervisor converges without silently duplicating accepted work.

14. **Define and emit correlated sandbox-side OTel spans.** Standardize the
    attempt span tree, W3C context propagation, bounded heartbeat/output
    events, semantic version, and content-off defaults across all harnesses.
    **Tracking:** [#488](https://github.com/VangelisTech/archetype/issues/488).

15. **Export through a redacting OTel collector to Logfire.** Keep the Logfire
    credential outside the mission sandbox, use collector-side enrichment and
    redaction, persistent sending queues/WAL, and backend-outage markers.
    **Tracking:** [#492](https://github.com/VangelisTech/archetype/issues/492).
    **Depends on:** items 8, 9, and 14.

16. **Prove policy-controlled model/API traffic capture.** Prefer an explicit
    L7 relay and provider network enforcement over transparent TLS
    interception; publish a support matrix for each harness and credential
    mode. Metadata is default; content is explicit and governed. **Tracking:**
    [#490](https://github.com/VangelisTech/archetype/issues/490). **Depends
    on:** items 8, 9, and 14.

17. **Add a durable external event bus for live streams.** Persist a per-world
    event/outbox record before at-least-once queue publication, provide
    idempotent consumers, cursor replay, Hibernation WebSockets, archival
    projection, and an active DLQ path. **Tracking:**
    [#491](https://github.com/VangelisTech/archetype/issues/491). **Depends
    on:** item 8; complements but does not replace items 10–15.

18. **Integrate Pydantic Evals and span evaluators.** Map typed mission cases
    to Archetype's dataset/evaluation ontology, grade outcome/process/efficiency,
    and project experiments to Logfire without creating a second transition
    authority. **Tracking:**
    [#489](https://github.com/VangelisTech/archetype/issues/489). **Depends
    on:** item 14 for stable process assertions.

19. **Run and retain the paid Claude Code edit/resume proof.** Use subscription
    authentication, make a material repository edit, checkpoint, restore in a
    distinct sandbox, continue the same agent session, validate independently,
    and publish its artifact/evaluation evidence. **Done when:** evidence is
    queryable by world/run/attempt and no credential is present in snapshots or
    artifacts.

20. **Run the direct endpoint benchmark from a credentialed Modal load
    generator.** Remove the local client/network as the load source and measure
    endpoint queue, token throughput, first-token latency, completion latency,
    error/retry rate, and validated repository outcomes. **Done when:** raw
    sample evidence is durable and the report binds endpoint configuration,
    model, replica limit, workload, and Git revision.

21. **Repeat concurrency 16/24/32 to establish variance.** Run enough
    independent repetitions to estimate confidence intervals and distinguish
    agent-quality variance from endpoint saturation. **Done when:** the
    recommended cap is based on accepted-throughput and tail-latency
    distributions, not one sweep.

22. **Add endpoint-level admission, budgets, and conservative capacity
    defaults.** Schedule by provider/endpoint/model and enforce concurrent
    sandbox, token, cost, wall-clock, retry, and tenant budgets. Start at 16 or
    the repeated-evidence cap rather than the theoretical 32. **Done when:**
    overload queues predictably, fairness and cancellation are tested, and the
    operator can override a recorded default.

23. **Implement actual PR creation and CI/frontier evaluation.** A successful
    mission should push the sanitized branch, open a PR, observe CI/review
    evidence, and represent `pr_ready`, `ci_passed`, `reviewed`, and `merged`
    as separate delivery states. **Done when:** a live mission opens a material
    Archetype PR and the mission cannot claim merge success from a local
    validator alone.

24. **Only then graduate to multi-task missions or HTN.** Define multi-task
    transition and checkpoint semantics before same-sandbox multi-agent
    collaboration. Benchmark one-sandbox/many-agent behavior separately from
    the proven one-mission/one-sandbox/one-session fanout. **Done when:** task
    dependencies, partial success, resume, budget allocation, and artifact
    lineage have explicit contracts and failure tests.

## Interpretation

Items 1–7 stabilize authority and phase boundaries. Items 8–13 establish the
security, reproducibility, and recovery floor. Items 14–18 add observability
and evaluation without changing the system of record. Items 19–22 supply paid
evidence and fleet capacity controls. Item 23 completes the software-delivery
loop. Item 24 deliberately remains last: orchestration breadth is useful only
after one mission is durably recoverable, observable, and governable.
