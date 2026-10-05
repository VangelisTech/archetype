# DDlog local HTTP and MCP preview

Status: locally tested adapters over the shared ingress and its existing Python
Host/Rust owner. This is an optional distribution, not production hosting or a
migration of the retained runtime, Gateway, Holocron, or their clients. The
pinned upstream DDlog revision is still unpublished, so clean external native
builds remain blocked. These adapters need no new native build.

## Scope and composition

`archetype-ddlog-transports==0.1.0` provides
`archetype_ddlog_transports.create_app(ingress)`. It borrows one configured
`archetype_ddlog_preview.ingress.Ingress`; it creates no Host, world, binding,
driver, grant, credential, durable ledger, scheduler, or tick loop. Its exact
dependencies are preview 0.1.2, the official `mcp==2.3.0` SDK, and
`starlette==1.3.1`. The SDK brings its own transitive dependencies, including
`mcp-types==2.3.0`. This is separate from the stdlib-only preview distribution
and stays outside the default UV workspace and retained `archetype` family DAG.
`quality/ddlog-transports.toml` and `scripts/check_ddlog_transports.py` reserve
and audit this bounded infrastructure exception.

Construction binds no socket. An enclosing local ASGI server must bind loopback,
enter the returned app's lifespan, and retain sole ownership of its ingress and
Host. Host/Origin allowlists are DNS-rebinding checks, not listener binding or
network isolation. The app permits localhost, 127.0.0.1 and [::1], with local
HTTP origins. No remote deployment settings, TLS termination, proxy trust,
credential provisioning, OAuth issuance/discovery, or server CLI are supplied.

The operator passes the existing real `PrincipalDirectory` through the shared
verifier port. This remains a source-level framework dependency of the enclosing
composition, as described in the [ingress contract](ddlog-ingress-preview.md).
The transport wheel does not extract or package an authentication authority.

## HTTP and SDK contract

`POST /invoke` accepts the original shared version-1 JSON bytes, with
`Content-Type: application/json` and `Authorization: Bearer …`. It returns the
same safe ingress response. Success is HTTP 200. Shared invalid request,
authentication, authorization, busy, unavailable and native-failure results map
to 400, 401, 403, 429, 503 and 500 respectively. No retry is performed.

`/mcp` is the official SDK's stateless Streamable HTTP endpoint with JSON
responses. SDK initialization, JSON-RPC methods, negotiation, transport tasks
and protocol errors stay SDK-owned. Its only listed tool is `simulation`, with
exact arguments:

```json
{"request_json":"{\"version\":1,\"operation\":\"status\",\"resource\":\"experiment\",\"arguments\":{}}"}
```

The tool's string argument preserves the original strict JSON contract,
duplicate-key checks, and exact decimal integer strings. It returns the shared
response both as JSON text content and as `structuredContent`, setting
`isError` for an ingress failure. It does not accept an actor, role, bearer
token, native identifier or operator configuration in tool arguments. Unknown
tool names and unexpected tool arguments produce a shared invalid-request
result without native dispatch.

Both paths use the SDK's `BearerAuthBackend`, `AuthContextMiddleware` and
`RequireAuthMiddleware`. The verifier calls `Ingress.authenticate`, which
delegates to the configured real authority. It maps only the actual principal
identifier and capabilities into the SDK access-token context. It fabricates no
issuer, audience, expiry or OAuth metadata. `Ingress.invoke` always verifies
again and checks the exact capability/resource grants before resolving the
native binding. Transport context cannot bypass those checks. This is a local
static service credential profile, not complete production MCP OAuth hosting.

The app rejects duplicate security headers and compressed bodies. The SDK body
limiter caps HTTP input at 64 KiB and outer MCP JSON-RPC input at 512 KiB,
including chunked input without Content-Length. The shared inner request still
has its independent 64-KiB cap. Outer MCP duplicate keys and non-JSON numeric
constants (NaN/Infinity) are rejected before SDK parsing loses them, including
when mounted beneath a URL prefix. Shared response
projections remain at most 16 KiB; MCP wraps those projections in its protocol
envelope. Protocol and authentication failures may use SDK HTTP/JSON-RPC error
shapes rather than the ingress result shape.

## Ownership, retry and shutdown

Native work is the same ingress-owned shielded task used by direct callers.
Cancelling/disconnecting a transport waiter does not cancel its native call,
release its concurrency slot, or authorize replay. Both adapters share the same
capacity. Retain the exact admission key and inspect admission status to resolve
ambiguous responses. An explicit identical admission retry can resolve through
the existing native ledger while its hosted preflight remains valid. Once
publication advances the analytical head, the original expected head is stale;
query the original admission status instead. Neither transport mints keys or
retries itself.

The app lifespan runs the SDK session manager and owns only SDK tasks. Exiting
it never closes the borrowed Host or drains ingress automatically. The enclosing
owner must stop ingress, call Host.close off the event loop so independent
shutdown can unblock native work, and then await ingress.drain before ending
the event loop. A mounted app needs its lifespan entered explicitly by its
parent. Each created SDK manager is single-use; create a new app for a new
lifespan.

Request/response caps and retained-call limits do not bound native execution
cost, status diagnostics, internal catalog scans, latency, connection counts,
or total pre-dispatch parsing concurrency. History and component reads remain
absent; arbitrary registration/configuration and raw native calls remain local
trusted operations. Production connection/rate limits, native typed errors,
bounded storage reads, distributed ownership and broader migration are separate
work.

## Focused executable evidence

`packages/archetype-ddlog-transports/tests/test_transports.py` uses the actual
SDK HTTP client/session over an in-memory ASGI transport and enters the SDK
lifespan. It does not use the direct `Client(server)` testing shortcut that
bypasses HTTP authentication. The fixture directory is the real source
`PrincipalDirectory`, with existing synthetic credentials and no configuration
files. Tests cover both entry paths, authentication/context, denied resource
lookup, exact values, malformed/unknown operations, limits, mounting, safe
errors, incomplete-body disconnects, cancellation and shared capacity.

One integration uses the existing C ABI library, real local Iceberg and an
explicitly simulated native driver. Before publication, HTTP admission retried
through MCP retains one boundary, advances one revision and publishes one exact
Int64/string row. After publication, the stale-head retry fails and the original
admission remains inspectable.
This is not new actual-DDlog compiler acceptance.

```sh
make ddlog-transports-check \
  DDLOG_PYTHON=/absolute/isolated/environment/bin/python \
  DDLOG_PYTHON_LIBRARY=/absolute/existing/libarchetype_ddlog_python.dylib
```

This focused target does not invoke Cargo. Install the two Python wheels and
their registry dependencies only into a separate transport environment. Do not
replace the prior preview wheel/environment to test this slice. The validation
receipt records the exact installed artifacts and transitive dependency set.

Official contracts: [SDK authorization](https://py.sdk.modelcontextprotocol.io/run/authorization/),
[ASGI integration](https://py.sdk.modelcontextprotocol.io/run/asgi/), and
[SDK testing](https://py.sdk.modelcontextprotocol.io/get-started/testing/).
