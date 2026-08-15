# Redis Cache — `dev.mcpg.cache.redis`

> class `cache` · `native` · package `mcpg-plugin-cache-redis` · artifact `libmcpg_plugin_cache_redis.so` · Apache-2.0

A `cache` plugin for the MCPG gateway that puts cached state in Redis instead of
process memory. Every namespace the gateway asks for is served from one Redis
instance under the flat `{key_prefix}:{namespace}:{key}` layout, over a
`deadpool-redis` connection pool with per-operation deadlines. Reach for it when
a cache namespace — a response cache, a JWKS document, a rate-limit counter —
has to be shared across gateway replicas rather than rebuilt independently in
each one.

## What it does
- Serves **any** namespace (`serves_any_namespace()` is `true`), so a single
  configured instance can back every namespace bound to it.
- Maps the cache surface onto plain Redis commands, with `incr` executed as a
  server-side Lua script so the increment and its TTL are applied atomically.
- Isolates deployments sharing one Redis through `key_prefix`; namespace-wide
  invalidation walks `SCAN MATCH` and batch-`DEL`s, never `FLUSHDB`.
- Bounds every operation by `connection.operation_timeout_ms`; a timeout is
  reported as a backend I/O error rather than hanging the caller.
- Refuses to register on unparseable or invalid config — a misconfigured cache
  fails the gateway's boot instead of silently degrading.
- Declares the `network_outbound` capability for its TCP egress to Redis; the
  gateway refuses to load the plugin unless the `plugins[]` entry grants it.

## Configuration
Loaded from the flat top-level `plugins:` list with `class: cache`. The block
under the entry's `config:` key is handed to the plugin verbatim and parsed by
the schema below.

```yaml
plugins:
  - id: dev.mcpg.cache.redis
    class: cache
    kind: native
    source:
      path: ./plugins/libmcpg_plugin_cache_redis.so
      # or, platform-agnostic:
      # oci: ghcr.io/mcpg-dev/source-code/plugins/cache-redis:protocol-1
    granted_capabilities:
      - network_outbound
    config:
      url: ${env.REDIS_URL}     # redis://<user>:<password>@redis-primary.internal:6379/0
      key_prefix: mcpg
      connection:
        pool_size: 16
        connect_timeout_ms: 1000
        operation_timeout_ms: 5000
```

| Field | Type | Default | Description |
|---|---|---|---|
| `url` | string | — (required) | Connection URL, and the only channel the client reads. Standard redis-rs syntax, so credentials and the database index belong here: `redis://<user>:<password>@<host>:<port>/<db>`. |
| `key_prefix` | string | `mcpg` | Prefix for every key: `{key_prefix}:{namespace}:{key}`. Must not be empty. |
| `connection.pool_size` | integer | `16` | Connection-pool size. Must be greater than zero. |
| `connection.connect_timeout_ms` | integer | `1000` | Pool create and wait deadline in milliseconds. Must be greater than zero. |
| `connection.operation_timeout_ms` | integer | `5000` | Per-operation deadline in milliseconds. Must be greater than zero. |

The schema also accepts an `auth` block (`username`, `password`) and a `tls`
block (`enabled`, `ca_cert`, `verify_peer`). Both are validated but not applied:
the pool is built from `url` alone. Put credentials in the URL, and see
**Security** below for the TLS story.

Unknown fields are rejected.

The validator only accepts a `redis://` or `rediss://` scheme, and rejects
`redis+sentinel://` explicitly rather than silently downgrading it to a
standalone connection — point the plugin at the currently elected primary
instead. Cluster topologies are out of scope for this plugin.

## Operations

| Cache operation | Redis command |
|---|---|
| `get` | `GET` |
| `put` | `SET … PX <ttl>` |
| `delete` | `DEL` |
| `clear(namespace)` | `SCAN MATCH {key_prefix}:{namespace}:* COUNT 1000` + batched `DEL` |
| `incr` | Lua script: `INCRBY` followed by `PEXPIRE`, evaluated server-side |

A `put` or `incr` with a zero TTL is written with `PX 1` — Redis rejects `PX 0`,
and the surface contract for a zero TTL is "expire on the next tick".

`get` follows the cache-trait contract of collapsing a backend failure into a
miss, so a Redis outage degrades to cache-misses rather than failing requests;
the failure is still logged, metered, and — for connection or authentication
failures — audited. `put`, `clear`, and `incr` surface a typed error: a
`BUSY`/`TRYAGAIN` reply maps to a throttled result, everything else to a backend
error carrying the operation name.

## Connection pooling
Connections come from a `deadpool-redis` pool sized by `connection.pool_size`,
with the create and wait timeouts both set from `connection.connect_timeout_ms`.
The pool is built at registration time and opens no socket until the first
operation, so a Redis that is briefly unreachable at boot does not prevent the
gateway from starting. Because the cache surface is synchronous while the Redis
client is asynchronous, the plugin owns a small two-worker Tokio runtime and
blocks each operation on it. Shutdown is implicit: the pool drops with the
plugin handle.

## Observability
Each operation opens a host span (`cache_redis.get`, `cache_redis.put`,
`cache_redis.invalidate`) carrying the namespace as a bounded `scope`
attribute, and records a latency histogram plus an outcome counter:

| Metric | Outcome labels |
|---|---|
| `mcpg_cache_redis_get_seconds` / `mcpg_cache_redis_get_total` | `hit`, `miss`, `error` |
| `mcpg_cache_redis_put_seconds` / `mcpg_cache_redis_put_total` | `ok`, `error` |
| `mcpg_cache_redis_invalidate_seconds` / `mcpg_cache_redis_invalidate_total` | `ok`, `error` |

`incr` shares the `put` series; `delete` and `clear` share the `invalidate`
series. Cache keys never appear in span attributes or metric labels — the key
space is unbounded and would blow up cardinality.

Audit emission is deliberately sparse: only
`dev.mcpg.cache.redis.connection_failed` (the pool could not hand out a
connection) and `dev.mcpg.cache.redis.auth_failed` (Redis rejected the
credentials) are emitted. Hits, misses, successful writes, and transient
retryable errors produce no audit traffic.

## Security
- **The bundled Redis client is compiled without TLS support.** A `rediss://`
  URL therefore fails at pool construction with "can't connect with TLS, the
  feature is not enabled", which refuses the plugin's registration rather than
  quietly falling back to plaintext. Terminate TLS in front of Redis — a
  sidecar proxy, a service mesh, or a stunnel-style listener — and point `url`
  at that endpoint, or keep the link inside a trusted network segment.
- Supply credentials through the URL, sourced from the environment
  (`url: ${env.REDIS_URL}`) or a secret provider, so no secret is committed to
  the config artifact. The gateway resolves `${env.…}` references before the
  config reaches the plugin.
- Give each deployment sharing a Redis instance a distinct `key_prefix`, and
  pair it with a Redis ACL key pattern so one deployment cannot read another's
  namespaces.
- The plugin never issues `FLUSHDB`, so it cannot destroy unrelated keyspaces
  living in the same Redis.
- A config that fails to parse or validate panics the plugin's factory, which
  the host reports as a failed registration — a misconfigured cache never
  silently degrades into an unprotected one.

## Build
`cdylib-export` is enabled by default, so the plain build already produces the
loadable artifact. Disable the default features when linking this crate as an
rlib path dependency alongside other plugins, so the workspace build does not
link two `mcpg_plugin_register` exports.

```bash
cargo build -p mcpg-plugin-cache-redis --features cdylib-export --release   # → target/release/libmcpg_plugin_cache_redis.so
```

## Testing
The unit suite is offline and points at an unresolvable host, so it needs no
Redis:

```bash
cargo test -p mcpg-plugin-cache-redis --lib
```

The integration suite starts a Redis 7 container per test and requires a Docker
daemon:

```bash
cargo test -p mcpg-plugin-cache-redis --features integration-tests
```

## Sign & load (production)
Sign the artifact, pin/verify via the entry's `signature:` block, and honour
revocations. See <https://mcpg.dev/docs/security/plugin-security>.

## See also
- <https://mcpg.dev/docs/plugins/plugins-and-protocol> — plugin classes, the ABI, and how the gateway loads them.
- <https://mcpg.dev/docs/reference/configuration> — the full gateway config schema, including `plugins[]`.
- `libs/plugins/cache/memory` — the in-process sibling (`dev.mcpg.cache.memory`), for single-instance or dev deployments.
