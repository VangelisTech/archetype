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
`ERROR_TYPES`, `FAILURE_DISPOSITIONS`, `OUTCOMES`, `SPAN_NAME_ALIASES`,
`TRACE_ATTRIBUTE_ALIASES`, `RECORDER_METRIC_NAMES`, and
`RECORDER_METRIC_LABEL_KEYS` in `archetype._obs` are the single
machine-readable vocabulary. The repository audit consumes these literals; it
must not maintain a second allowlist.

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
  pending replacement by the logical/physical ownership model in #519.

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

Archetype-owned HTTP exporters suppress dependency exception tracebacks and
collector response bodies, replacing a failed export with one fixed diagnostic.
OTLP endpoint URLs are routing configuration, not a credential transport: only
absolute `http` or `https` URLs without user information, a query, or a fragment
are accepted. Authentication belongs in the operator-owned OTLP header settings.
Rejected endpoint values are removed and are never copied into a diagnostic.
Endpoint hosts and paths remain non-secret routing data; a host that enables
root-level HTTP dependency debug logs may observe them. Valid standard sampling
configuration remains host-owned; malformed SDK configuration is prevented from
echoing its raw value while the dependency falls back to its documented default.

Daft 0.7.19 owns separate Rust providers initialized when its compiled module
is imported. Standard generic OTLP environment variables are broadcast
configuration, not evidence that those dependency providers passed
Archetype's safe-signal boundary. Before any Archetype submodule can import
Daft, the package host therefore applies this signal-routing contract:

| Host setting | Owner and behavior |
|---|---|
| `ARCHETYPE_OTLP_TRACES_ENDPOINT` | Full HTTP/protobuf traces endpoint consumed only by Archetype's filtered exporter. This is the preferred explicit setting. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Compatibility input consumed once, converted to its `/v1/traces` endpoint, and removed from the process and inherited worker environment. |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | Compatibility input consumed as Archetype's full traces endpoint and then removed. |
| `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` | The only supported Daft-native opt-in. It exports physical engine metrics directly through Daft and is never re-emitted as Archetype metrics. |
| `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT` or `DAFT_DEV_OTEL_EXPORTER_OTLP_ENDPOINT` | Unsupported content-bearing dependency routes; removed before Daft initialization. |

The consumed generic/log/trace variables are not restored: a later local,
spawned, or distributed Daft worker that inherits this host environment must
not recreate the unsafe providers. Externally managed workers require the same
endpoint policy in their own launch environment. When native metrics are
requested, arbitrary `OTEL_RESOURCE_ATTRIBUTES` and `OTEL_SERVICE_NAME` values
are removed before Daft import; Daft uses its fixed default service identity.
Daft metrics are enabled only for versions named in the fresh-process
compatibility matrix (currently exactly 0.7.19), and accept only `grpc` or
`http/protobuf`, spelled exactly as Daft parses them; an unvalidated version,
empty/malformed endpoint, or another protocol is removed so telemetry
configuration cannot make application import fail. The next explicit host
configuration emits a fixed diagnostic without the rejected value. Transport
headers remain operator-owned exporter configuration and are never copied into
Archetype signal attributes. Native Daft metrics are uncompressed because the
0.7.19 wheel does not compile the Rust compression features: generic and
metrics-specific compression settings are removed whenever that endpoint is
enabled. A configured export interval must be a positive unsigned 64-bit
millisecond value; zero and malformed values are removed before they can create
an invalid or busy-spinning native reader.

This bootstrap installs no provider and preserves lazy package imports. It can
protect only an Archetype-owned host: importing Daft first under a generic,
logs, or traces endpoint initializes native providers before Archetype code can
run, and Daft 0.7.19 exposes no shutdown or reconfiguration API for them. Such
hosts must install an external provider before importing Archetype or use the
Archetype-specific traces endpoint, and must expose only the metrics-specific
endpoint to Daft. A fixed diagnostic reports a detectable late ordering, but
only when the explicit host adapter next runs; telemetry never aborts or changes
application work. Endpoint settings are process-start configuration: mutating
standard OTLP endpoint variables after the first Archetype import and then
importing Daft directly is unsupported.

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

The former `contrib.logfire_observer` hook surface has been removed. The private
`archetype._obs` boundary owns the retained vendor-neutral vocabulary; process
hosts select Logfire or OTLP exporters. There is no alternate hook factory that
can create a second tick root, retain an open-span table, or bypass the safe
signal boundary.

## 6. Family dispositions

| Family or layer | Current signal disposition | Authoritative outcome |
|---|---|---|
| Runtime host | Explicit construction-time provider and owned-handler setup; no family workflow span | Runtime lifecycle and returned/raised result |
| CLI and API | `serve` and worker lifespan configure the host; imports and `create_app()` remain inert | HTTP result and gateway/domain result |
| Gateway | Child spans for the three currently decorated operations; #515 owns a coherent ingress-root design | RBAC decision, typed application result, and access-audit evidence |
| RuntimeApplication | No direct signal yet; lower owning family remains visible | Typed family result/exception |
| Commands | No direct signal yet | Durable command ledger and settlement |
| World lifecycle, mutation, simulation | Existing query/update scopes without execution-attribution claims; materialize/execute names are legacy planning scopes pending #519 | Tick manifest, world record, and typed result/exception |
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
protocol member; there is no reverse requirement that every safe internal
emitter have a workflow row.

Each row declares plural signals and outcomes, its authoritative durable or
typed evidence when one exists, and only the fixed names, fields, and bounded
metric labels it uses. Every workflow claim must exactly match literal
emissions in its declared callable. Context-manager factories count only when
entered directly and decorator factories only when applied directly; merely
constructing either object is not an emission. Called helpers and nested
callables are not attributed transitively. The fixed metric contract of
`record_failure()` and `record_outcome()` comes from `_obs`'s machine-readable
recorder vocabulary. A positive protocol disposition lists same-owner
`emission_workflows` and its fixed signal claims must equal the union of those
source-backed workflows. This binds positive intent to source without
inventing protocol-to-implementation mappings or requiring a workflow for
every internal emitter.

`root` and `child` are mutually exclusive. `none` is exclusive, requires a
rationale, and records approval intent: no new signal is approved for that
contract. It does not claim the operation lacks an outcome, nor does it pretend
to prove source absence without a protocol-to-implementation registry. A
temporary legacy exception names one exact rule, path, qualified scope, and
target together with its owner, issue, reason, and objective expiry condition.
A missing, duplicate, phantom, wildcard, stale, cross-owner, or
source-divergent row fails the audit.

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
root/child/none exclusivity. It also binds fixed workflow fields to exact
lexical source emissions, but it does not prove runtime topology. This change
leaves the three existing gateway decorators as child dispositions; Issue #515
owns the coherent root model and any corresponding instrumentation.

`scripts/check_observability.py` provides deterministic syntax and disposition
enforcement from source and these manifests. It does not parse exported
telemetry or depend on a collector. The existing footgun reviewer separately
checks semantic boundary/authority and safety/cardinality mistakes that syntax
cannot prove, including values smuggled under an approved key and telemetry
used as application authority. Focused behavior contracts still prove typed
failure identity, retry behavior, and durable evidence.

## 7. Lazy planning and execution ownership

`world.materialize` and `world.execute` are retained legacy names, not claims
that Daft work was materialized or executed inside those spans. Processor
methods build a lazy DataFrame plan. Building that plan is normally cheap and
does not perform the physical work it describes; the same plan may execute
later at a terminal boundary or in a distributed execution environment.
Telemetry must not add any materialization or execution boundary to manufacture
a duration.

Archetype and Daft therefore own different parts of the trace:

- Archetype owns the logical and durable workflow envelope: tick attempt,
  mutation composition, terminal invocation, persistence, visibility
  publication, command settlement, and typed outcome.
- Daft owns physical execution telemetry for plans, stages, operators, tasks,
  UDF execution, workers, and engine resource use through its native OTel
  surface.
- The process host owns the explicit bridge between those surfaces, including
  provider and exporter configuration, signal selection, safe routing, and
  whatever correlation the locked Daft version and runner can truthfully
  preserve.

An Archetype span may measure the wall-clock workflow phase that invokes an
already-owned terminal boundary. It must not infer physical processor, stage,
or worker execution from Python planning calls. Engine-level attribution comes
from Daft or remains unavailable.

The #518 characterization established that processor calls and physical plan
nodes are not one-to-one and that application OTel context does not reliably
propagate into Daft workers. A unified parent-child trace is therefore not a
contract unless a focused integration test proves it for the locked Daft
version and supported runner. Otherwise Archetype and Daft telemetry remain
explicitly separate or use only a safe, evidence-backed correlation mechanism;
the system never fabricates ancestry.

Generic process-wide OTLP configuration is not sufficient authorization to
export dependency-owned telemetry. The host routing in section 5 prevents
native logs, exception text, tracebacks, UDF arguments, payloads, or secrets
from bypassing Archetype's signal policy while preserving explicit Daft
metrics. #444 preserves typed processor failures at the logical boundary. With
those prerequisites, #519 replaces the legacy world phases with the truthful
tick and commit envelope described here while consuming, rather than
duplicating, Daft's physical execution telemetry.
