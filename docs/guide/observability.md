# Observability

**Document type:** Normative.

**Scope:** Safe trace, metric, outcome, and correlation semantics; process-host
provider ownership; and the boundary between advisory telemetry and durable
application authority.

## 1. Safe signal contract

Telemetry is advisory. Durable records, typed results, exceptions, receipts,
and state transitions remain authoritative. No Archetype signal operation or
Archetype-owned adapter failure may change an application result, retry
decision, commit, authorization decision, or exception identity. A process
host remains responsible for failure behavior in handlers it installs itself.

Family and core code emit telemetry through the private `archetype._obs`
boundary and diagnostics through stdlib logging. They do not import an OTel
SDK, exporter, collector, or vendor integration. Process hosts own provider,
exporter, and handler installation through the private `archetype._logging`
adapter. Both modules remain internal and do not expand the supported Python
or REST surface.

The signal boundary cannot import `app.redaction`: core imports `_obs`, so that
edge would invert the application dependency direction. Instead, `_obs` is
safe by construction through a closed schema. An outer adapter handling a
content-bearing event or export record still consumes `iRedactionService`
before its own durable or external write. These protections are complementary.

## 2. Vocabulary and validation

`SIGNAL_SCHEMA_VERSION`, `SPAN_NAMES`, `LEGACY_SPAN_NAMES`,
`TRACE_ATTRIBUTE_KEYS`, `METRIC_NAMES`, `METRIC_LABEL_KEYS`, `EVENT_NAMES`,
`ERROR_TYPES`, `FAILURE_DISPOSITIONS`, `OUTCOMES`, `SPAN_NAME_ALIASES`, and
`TRACE_ATTRIBUTE_ALIASES` in `archetype._obs` are the single machine-readable
vocabulary. The repository audit consumes these literals; it must not maintain
a second allowlist.

Signal names are fixed. Unknown or dynamic names are no-ops. Custom attributes
use the `archetype.*` namespace; `error.type` is the approved standard semantic
key. Unknown keys, secret/content keys, unsupported mappings, and invalid
values are omitted without calling arbitrary `str()`, `repr()`, iteration, or
object properties.

| Attribute class | Accepted representation |
|---|---|
| World, run, actor, or command ID | Canonical lowercase, hyphenated UUID string |
| Bundle, attempt, idempotency, or correlation digest | Precomputed lowercase 64-character hexadecimal digest |
| Entity ID, tick, or count | Exact non-boolean integer from zero through signed 64-bit maximum |
| Component signature | Canonical `a_<count>c_s<16 lowercase hex>` storage signature |
| Failure disposition, outcome, operation, or error type | Exact member of its immutable literal vocabulary |
| Redaction rule IDs | Non-empty exact tuple of at most 16 bounded rule identifiers |

Raw attempt IDs, idempotency keys, and arbitrary correlation strings are not
aliases and are not hashed at the signal boundary, even when their text happens
to look like a digest. A producer may supply an explicitly named semantic
digest only after its owning layer has validated the source value. This avoids
turning a credential or PII value into a stable, offline-guessable telemetry
identifier. Canonical attributes take precedence when a temporary legacy alias
is also present.

Approved metric labels are only bounded operation, outcome, failure
disposition, and error categories. World, run, actor, command, artifact,
attempt, idempotency, mission, task, evaluation, and entity identifiers are
never metric labels.

The current canonical span vocabulary is:

- `gateway.create_entity`, `gateway.create_world`, and
  `gateway.get_world_info`;
- `artifact.publish`, `artifact.upload`, and `artifact.index`; and
- the existing bounded `world.query` and `world.update` scopes, without an
  execution-attribution claim; and
- the legacy `world.materialize` and `world.execute` phases listed in section 7
  pending measured attribution.

Gateway source uses only the canonical `gateway.*` names. The former
`gate.create_world` and `gate.get_world_info` spellings are neither accepted
legacy names nor aliases. `SPAN_NAME_ALIASES` remains part of the versioned
vocabulary and is currently empty.

## 3. Failure and outcome semantics

A span helper yields `None`; callers never receive a raw OTel span that could
bypass validation. The OTel current context contains a non-recording view with
the same span context, retaining child-span parentage without exposing mutable
attributes, events, or status. OpenTelemetry automatic exception recording and
automatic status derivation are disabled.

When an application `Exception` propagates through a span, `_obs`:

1. preserves and re-raises the identical exception object;
2. sets `ERROR` status without a description;
3. records only a bounded `error.type`; and
4. records no exception message, stack, object representation, or exception
   event.

Application control flow represented by `BaseException` outside `Exception`,
including cancellation, `KeyboardInterrupt`, and `SystemExit`, propagates
unchanged and leaves span status unset.

`record_failure(..., disposition="handled" | "retrying")` emits a fixed safe
event and bounded counter. It does not mark a successful enclosing operation
as failed. `record_outcome()` emits only an approved advisory outcome. Neither
helper replaces the family-owned result, receipt, retry row, or durable
settlement that proves what happened.

Failures while starting, entering, mutating, ending, exporting, or incrementing
a signal are isolated at that exact signal operation. This rule does not permit
broad suppression of application work under an “observability” label.

## 4. Context and metric semantics

`bind_context()` stores only validated canonical correlation coordinates in a
`ContextVar`; nested bindings restore their predecessor on normal return or
failure. `capture_context()` returns a detached copy that the private host
logging adapter revalidates before enriching a record. Context never contains
payloads, prompts, paths, URLs, headers, exception text, or arbitrary object
strings.

Proxy tracers and counters may be created before a provider. Once a host
registers a provider, future calls use it. Signals emitted before registration
are dropped rather than buffered or replayed. Durable evidence is therefore
the only source for recovery or retrospective truth.

## 5. Process-host ownership

Importing `_obs` installs no provider, exporter, logging handler, or vendor
integration. `configure_tracing()` is an explicit process-host adapter used by
runtime/server startup:

- an existing host provider is respected and never replaced or shut down;
- a no-backend decision remains unlatched so a later host can configure;
- an Archetype-created provider candidate is installed once under a lock;
- a candidate that loses every global signal-provider registration is shut down
  without touching the host; a Logfire meter or logger that won its independent
  global slot is never shut down merely because an external tracer won;
- adapter failures produce only fixed diagnostics and remain retryable; and
- Archetype-owned processors snapshot and revalidate approved Archetype scope,
  names, attributes, events, status, context, and resource before enqueue or
  export. Foreign fields, links, trace state, and status descriptions are
  discarded.

The SDK bundled for the explicit debug-console host path is not permission for
family imports. Optional OTLP and Logfire integrations remain host concerns.
`RunConfig.debug` controls execution diagnostics and is not a telemetry-export
switch.

Importing Archetype, its runtime, or its API installs no handler or provider;
`create_app()` is also inert. The explicit configuration points are runtime
construction for trusted scripts, CLI `serve` startup for the server process,
and each FastAPI lifespan for the serving worker. Repeated worker setup is
safe. `create_app()` does not automatically invoke Logfire FastAPI
instrumentation; explicit backend selection continues through the
vendor-neutral host adapter.

When `ARCHETYPE_LOG=debug|info|warning|error` or the runtime's `log=` argument
enables logging, the host adapter owns at most one `archetype` package stderr
handler. Otherwise the explicit host boundary installs a package-owned null
handler so Python's `lastResort` handler cannot emit warnings or errors; any
handler the host explicitly installed remains authoritative through normal
propagation. Later quiet/default setup does not downgrade an already enabled
owned stderr handler. The adapter does not alter root handlers, root filters,
root level, the global `LogRecordFactory`, or foreign handlers and filters. Its
fail-open filter first
removes forged reserved fields and then restores only valid lowercase
`trace_id` and `span_id` coordinates plus revalidated
`TRACE_ATTRIBUTE_KEYS`. `LOG_RECORD_FIELDS` is exactly that derived union, not
a second attribute vocabulary. With no active context the fields are absent.
Correlation never reads or renders payloads, prompts, arbitrary objects,
exception objects, traceback text, or exception messages. The default
formatter suppresses traceback rendering and writes human diagnostics to
stderr, then restores the producer's message, arguments, and exception/stack
fields for later host-owned handlers. It cannot sanitize sensitive text already
embedded in a primitive string; producer-side policy and foreign-handler export
safety remain host responsibilities. Stdout remains owned by application and
machine-result output. When enabled, the stderr handler runs first, so a host
handler attached to the `archetype` package logger receives the same enriched
correlation fields without moving the filter onto that foreign handler or the
root logger.

The old `contrib.logfire_observer.logfire_hooks()` entry point is a deprecated,
warning-only shim that returns no hooks. It cannot create a second tick root,
retain an open-span table, or bypass the safe signal boundary.

## 6. Family dispositions

| Family or layer | Current signal disposition | Authoritative outcome |
|---|---|---|
| Runtime host | Explicit construction-time provider and owned-handler setup; no family workflow span | Runtime lifecycle and returned/raised result |
| CLI and API | `serve` and worker lifespan configure the host; imports and `create_app()` remain inert | HTTP result and gateway/domain result |
| Gateway | Child spans for the three currently decorated operations; #515 owns a coherent ingress-root design | RBAC decision, typed application result, and access-audit evidence |
| RuntimeApplication | No direct signal yet; lower owning family remains visible | Typed family result/exception |
| Commands | No direct signal yet | Durable command ledger and settlement |
| World lifecycle, mutation, simulation | Existing query/update scopes without execution-attribution claims; materialize/execute names are legacy pending #518/#519 | Tick manifest, world record, and typed result/exception |
| Storage and query | No direct signal yet | Store/catalog state and returned frame |
| Redaction | No direct signal; safe rule IDs may be carried by approved callers | Redaction receipt or quarantine exception |
| Artifacts | Child spans for publish, upload, and index | Publication row, object/index state, and publish receipt |
| Evaluation and research | No direct signal yet | Snapshot-pinned evaluation/research receipts |
| Audit | Logging only; no direct signal yet | Journal/outbox and projection watermark |
| Missions and sandboxes | No direct signal yet | Typed transition rows, attempt state, checkpoints, and artifacts |

The machine authority is one independently owned manifest per family under
`quality/observability/<family>.toml`. The required universe is every callable
member — method, async method, or property — of every `Protocol` declared
anywhere under `src/archetype/app/<family>/`, not only protocols co-located in
`interfaces.py`. Every such member has exactly one disposition row in its
owning family manifest. Rows use exact qualified names; wildcards, method
ranges, and inherited blanket dispositions are forbidden. A family may add an
exact workflow row for an instrumented internal operation that is not a
protocol member.

Each row declares plural signals and outcomes, its authoritative durable or
typed evidence when one exists, and only the fixed names, fields, and bounded
metric labels it uses. `root` and `child` are mutually exclusive. `none` is
exclusive, requires a rationale, and means no new signal has been approved —
not that the operation lacks an outcome. A temporary legacy exception names
one exact rule, path, qualified scope, and target together with its owner,
issue, reason, and objective expiry condition. A missing, duplicate, phantom,
wildcard, or stale row fails the audit.

An owner cannot absorb another package's workflow or legacy debt. The sole
current cross-package ownership is the `world` family's two explicit
`AsyncWorld` compute/commit workflows. Host capabilities live only in
`hosts.toml`: provider setup and console export remain in `_obs`, logging
configuration remains in `_logging`, and runtime/API/CLI hosts may only invoke
those private adapters.

`root` describes Archetype's logical ingress ownership; it does not discard an
upstream distributed parent. Runtime and gateway ingress workflows may own
roots, while `RuntimeApplication` and lower families own children. The
repository audit enforces the vocabulary, declared ownership, and
root/child/none exclusivity, but it does not prove runtime topology. This
change leaves the three existing gateway decorators as child dispositions;
#515 owns the coherent root model and any corresponding instrumentation.

`scripts/check_observability.py` provides deterministic syntax and disposition
enforcement from source and these manifests. It does not parse exported
telemetry or depend on a collector. The existing footgun reviewer separately
checks semantic boundary/authority and safety/cardinality mistakes that syntax
cannot prove, including values smuggled under an approved key and telemetry
used as application authority. Focused behavior contracts still prove typed
failure identity, retry behavior, and durable evidence.

## 7. Lazy execution honesty

`world.materialize` and `world.execute` are retained legacy names, not claims
that Daft work was materialized or executed inside those spans. Processor
methods commonly build lazy expressions whose work runs at a later terminal
boundary. Telemetry must not add `.collect()`, `.to_pylist()`, or any other
materialization to manufacture a duration.

Issue #518 must characterize Daft execution attribution and worker context with the
locked version and supported runner. #519 then replaces or redefines these
world phases from that evidence. Until then, no processor planning duration may
be described as processor execution duration.
