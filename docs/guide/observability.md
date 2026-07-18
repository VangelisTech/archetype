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

Family and core code emit through the private `archetype._obs` boundary using
OpenTelemetry APIs and stdlib logging only. They do not import an OTel SDK,
exporter, collector, or vendor integration. Process hosts own provider and
exporter installation. `archetype._obs` remains internal and does not expand
the supported Python or REST surface.

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

`gate.create_world` and `gate.get_world_info` temporarily normalize to the
canonical `gateway.*` names. The deterministic audit introduced by #514 owns
their source migration and expiry.

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
failure. `capture_context()` returns a detached copy suitable for #326's
structured logging adapter. Context never contains payloads, prompts, paths,
URLs, headers, exception text, or arbitrary object strings.

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
switch. #326 owns removing API import-time setup and correlating stdlib logs.

## 6. Family dispositions

| Family or layer | Current signal disposition | Authoritative outcome |
|---|---|---|
| Runtime host | Explicit provider/log-handler configuration; no family workflow span | Runtime lifecycle and returned/raised result |
| CLI and API | No family-owned signals in this contract; #326 owns host/log correlation | HTTP result and gateway/domain result |
| Gateway | Child spans for the three currently decorated operations | RBAC decision, typed application result, and access-audit evidence |
| RuntimeApplication | No direct signal yet; lower owning family remains visible | Typed family result/exception |
| Commands | No direct signal yet | Durable command ledger and settlement |
| World lifecycle, mutation, simulation | Existing query/update scopes without execution-attribution claims; materialize/execute names are legacy pending #518/#519 | Tick manifest, world record, and typed result/exception |
| Storage and query | No direct signal yet | Store/catalog state and returned frame |
| Redaction | No direct signal; safe rule IDs may be carried by approved callers | Redaction receipt or quarantine exception |
| Artifacts | Child spans for publish, upload, and index | Publication row, object/index state, and publish receipt |
| Evaluation and research | No direct signal yet | Snapshot-pinned evaluation/research receipts |
| Audit | Logging only; no direct signal yet | Journal/outbox and projection watermark |
| Missions and sandboxes | No direct signal yet | Typed transition rows, attempt state, checkpoints, and artifacts |

`none` means no new signal has been approved, not that an operation lacks an
outcome. #514 turns these dispositions into per-family machine manifests and
requires rationale for every `none` row.

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
