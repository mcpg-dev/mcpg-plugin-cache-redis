//! `dev.mcpg.cache.redis` — Redis-backed `cache` plugin.
//!
//! This crate is the implementation; the operator-facing
//! summary lives in `README.md`.
//!
//! # Layout
//!
//! - [`config`] — operator-supplied YAML/JSON shape + parsing.
//! - [`error`] — Redis-error → [`CacheError`] mapping.
//! - [`pool`] — deadpool connection setup + atomic-incr Lua script.
//! - The crate root wires the [`SyncCachePlugin`] impl + the
//!   [`declare_plugin!`](mcpg_plugin_sdk::declare_plugin)
//!   invocation.
//!
//! # Why a bundled tokio runtime
//!
//! The `cache` FFI is sync per [`SyncCachePlugin`]. The `redis` crate
//! is async. The plugin owns a `tokio::runtime::Runtime` and
//! `block_on`s every op on it, satisfying the host's
//! `spawn_blocking` invocation contract without leaking async into
//! the FFI. Same pattern documented for native plugins that need
//! their own runtime in `plugin-design-rfc.md §6`.

mod config;
mod error;
mod pool;

use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use mcpg_plugin_protocol::audit::{AuditEvent, AuditOutcome};
use mcpg_plugin_protocol::cache::CacheError;
use mcpg_plugin_protocol::types::PluginIdentity;
use mcpg_plugin_protocol::{PluginClass, PluginManifest};
use mcpg_plugin_sdk::HostHandle;
use mcpg_plugin_sdk::declare_plugin;
use mcpg_plugin_sdk::ffi::SyncCachePlugin;
use redis::AsyncCommands;
use tokio::runtime::Runtime;

pub use config::RedisCacheConfig;
pub use error::ConfigError;

const PLUGIN_ID: &str = "dev.mcpg.cache.redis";

/// Default-and-only entry-point. Cheap to clone.
pub struct RedisCache {
    inner: Arc<RedisCacheInner>,
}

struct RedisCacheInner {
    manifest: PluginManifest,
    config: RedisCacheConfig,
    pool: pool::RedisPool,
    /// Bundled runtime for the async Redis client. Sized for the
    /// plugin's own concurrency — every trait method block_on's
    /// here. The host's `spawn_blocking` wrapper provides the OS
    /// thread.
    runtime: Runtime,
    /// Unified host-observability handle.
    /// `OnceLock` because the factory closure installs it exactly
    /// once at boot via `set_host_handle`, after the plugin is
    /// constructed but before any traffic reaches `get`/`put`/...
    /// Test paths that construct the plugin without the macro
    /// factory leave the slot empty; `host_handle()` returns
    /// `None` and the per-op triad short-circuits to a no-op. The
    /// internal `tracing::*` calls remain wired in both modes
    /// (coexistence with the host triad is intentional).
    host_handle: OnceLock<HostHandle>,
}

impl RedisCache {
    /// Factory used by `declare_plugin!`. Panics on bad config —
    /// a misconfigured cache backend must not silently register;
    /// the macro's `catch_panic_to_null_handle` translates the
    /// panic into a host-visible "plugin failed to register" error
    /// referencing `plugin_id`.
    pub fn from_config_json(config_json: &str) -> Self {
        let config = RedisCacheConfig::parse(config_json).unwrap_or_else(|err| {
            tracing::error!(
                plugin_id = PLUGIN_ID,
                error = %err,
                "redis cache: config parse failed; refusing to register"
            );
            panic!("redis cache config parse failed: {err}")
        });

        let pool = pool::RedisPool::from_config(&config).unwrap_or_else(|err| {
            tracing::error!(
                plugin_id = PLUGIN_ID,
                error = %err,
                url = %config.url,
                "redis cache: pool init failed; refusing to register"
            );
            panic!("redis cache pool init failed: {err}")
        });

        // Multi-threaded runtime so the per-op block_on's can
        // overlap. Two worker threads is plenty for typical MCP QPS;
        // operators with heavier load can bump pool_size + see the
        // runtime spawn more I/O coroutines naturally.
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("mcpg-cache-redis")
            .enable_all()
            .build()
            .unwrap_or_else(|err| {
                tracing::error!(
                    plugin_id = PLUGIN_ID,
                    error = %err,
                    "redis cache: tokio runtime init failed; refusing to register"
                );
                panic!("redis cache tokio runtime init failed: {err}")
            });

        Self {
            inner: Arc::new(RedisCacheInner {
                manifest: PluginManifest {
                    id: PLUGIN_ID.into(),
                    version: env!("CARGO_PKG_VERSION").into(),
                    name: "Redis Cache".into(),
                    plugin_class: PluginClass::Cache,
                    protocol_version: "1.0".into(),
                    license: None,
                    required_capabilities: Vec::new(),
                    tags: Vec::new(),
                    provides: Vec::new(),
                    provides_schemes: Vec::new(),
                    module_path_prefix: ::std::module_path!()
                        .split("::")
                        .next()
                        .unwrap_or("")
                        .to_owned(),
                    backend_profile: None,
                },
                config,
                pool,
                runtime,
                host_handle: OnceLock::new(),
            }),
        }
    }

    fn op_timeout(&self) -> Duration {
        Duration::from_millis(self.inner.pool.connection.operation_timeout_ms)
    }

    /// Install the unified [`HostHandle`] surface
    /// for per-op observability. The SDK factory closure installs
    /// this exactly once at boot, after constructing the plugin but
    /// before any `get`/`put`/`delete`/`clear`/`incr` traffic is
    /// dispatched, threading a handle built from the late-bound
    /// `HostServices`.
    ///
    /// Idempotent — a second call returns `false` so reload paths
    /// that re-enter the install site don't panic. The returned
    /// `bool` indicates whether the handle was installed (`true`)
    /// or the slot was already occupied (`false`).
    pub fn set_host_handle(&self, host: HostHandle) -> bool {
        self.inner.host_handle.set(host).is_ok()
    }

    /// Borrow the installed unified host surface,
    /// if any. Returns `None` in test harnesses that constructed
    /// the plugin without calling [`RedisCache::set_host_handle`].
    /// Callers MUST treat `None` as "skip the host triad" — the
    /// plugin's internal `tracing::*` calls remain wired and carry
    /// the load on their own.
    fn host_handle(&self) -> Option<&HostHandle> {
        self.inner.host_handle.get()
    }
}

impl SyncCachePlugin for RedisCache {
    fn manifest(&self) -> &PluginManifest {
        &self.inner.manifest
    }

    fn supported_namespaces(&self) -> Vec<String> {
        Vec::new()
    }

    fn serves_any_namespace(&self) -> bool {
        true
    }

    /// Per the trait contract `get` returns `None` on miss AND on
    /// backend failure — see the error mapping in `error.rs`.
    /// The host emits its own `mcpg_cache_errors_total` via the
    /// `MeteredCache` decorator; we additionally bump a plugin-
    /// local error counter as `tracing::warn!` so operators can
    /// pivot on plugin-attributed failures without parsing the
    /// host's free-form metric labels.
    ///
    /// Opens a host-attributed span at
    /// `cache_redis.get`, records `mcpg_cache_redis_get_seconds` +
    /// `mcpg_cache_redis_get_total` with outcome ∈ {hit, miss,
    /// error}, and emits a sparse audit only on persistent
    /// connection / auth failure (never per-key on hit/miss).
    fn get(&self, ns: &str, key: &str) -> Option<Vec<u8>> {
        // Cardinality note: span attrs carry the (bounded) ns
        // string + request-flavored fields. The cache *key* is
        // NOT in the span attrs (high cardinality — one bucket
        // per key would be lethal). Operators can drill into a
        // specific key only via the rare audit emissions; metric
        // labels stay outcome-only.
        let host_span = self
            .host_handle()
            .map(|h| h.span("cache_redis.get", serde_json::json!({ "scope": ns })));

        let full_key = self.inner.config.render_key(ns, key);
        let pool = self.inner.pool.pool.clone();
        let timeout = self.op_timeout();

        let started = Instant::now();
        let result: Result<Option<Vec<u8>>, redis::RedisError> =
            self.inner.runtime.block_on(async move {
                let fut = async move {
                    let mut conn = pool.get().await.map_err(error::deadpool_to_redis)?;
                    conn.get(&full_key).await
                };
                tokio::time::timeout(timeout, fut)
                    .await
                    .unwrap_or_else(|_| {
                        Err(redis::RedisError::from((
                            redis::ErrorKind::IoError,
                            "redis cache get: operation timeout",
                        )))
                    })
            });
        let elapsed = started.elapsed();

        let (returned, outcome): (Option<Vec<u8>>, &'static str) = match result {
            Ok(Some(v)) => (Some(v), "hit"),
            Ok(None) => (None, "miss"),
            Err(err) => {
                tracing::warn!(
                    plugin_id = PLUGIN_ID,
                    namespace = ns,
                    error = %err,
                    "redis cache: get failed — surfacing as miss per trait contract"
                );
                self.maybe_emit_backend_audit("get", ns, &err);
                (None, "error")
            }
        };

        self.emit_op_observability("get", ns, outcome, elapsed);
        drop(host_span);
        returned
    }

    fn put(&self, ns: &str, key: &str, value: Vec<u8>, ttl_ms: u64) -> Result<(), CacheError> {
        let host_span = self
            .host_handle()
            .map(|h| h.span("cache_redis.put", serde_json::json!({ "scope": ns })));

        let full_key = self.inner.config.render_key(ns, key);
        // PX 0 is rejected by Redis ≥ 7. The trait
        // documents `ttl = ZERO` → "expire on next tick"; map that
        // to PX 1 (expires within 1 ms).
        let px = if ttl_ms == 0 { 1 } else { ttl_ms };
        let pool = self.inner.pool.pool.clone();
        let timeout = self.op_timeout();

        let started = Instant::now();
        let result: Result<(), redis::RedisError> = self.inner.runtime.block_on(async move {
            let fut = async move {
                let mut conn = pool.get().await.map_err(error::deadpool_to_redis)?;
                redis::cmd("SET")
                    .arg(&full_key)
                    .arg(value)
                    .arg("PX")
                    .arg(px)
                    .query_async::<()>(&mut conn)
                    .await
            };
            tokio::time::timeout(timeout, fut)
                .await
                .unwrap_or_else(|_| {
                    Err(redis::RedisError::from((
                        redis::ErrorKind::IoError,
                        "redis cache put: operation timeout",
                    )))
                })
        });
        let elapsed = started.elapsed();

        let outcome: &'static str = match &result {
            Ok(()) => "ok",
            Err(err) => {
                self.maybe_emit_backend_audit("put", ns, err);
                "error"
            }
        };
        self.emit_op_observability("put", ns, outcome, elapsed);
        drop(host_span);

        result.map_err(|err| error::map_runtime_error("put", &err))
    }

    fn delete(&self, ns: &str, key: &str) {
        // `delete` is the single-key invalidate path. The host
        // metric set names this op `invalidate`, separating it
        // from the namespace-wide `clear()` (which uses the same
        // metric names but with a richer scope label).
        let host_span = self.host_handle().map(|h| {
            h.span(
                "cache_redis.invalidate",
                serde_json::json!({ "scope": ns, "kind": "key" }),
            )
        });

        let full_key = self.inner.config.render_key(ns, key);
        let pool = self.inner.pool.pool.clone();
        let timeout = self.op_timeout();

        let started = Instant::now();
        let result: Result<(), redis::RedisError> = self.inner.runtime.block_on(async move {
            let fut = async move {
                let mut conn = pool.get().await.map_err(error::deadpool_to_redis)?;
                let _: i64 = conn.del(&full_key).await?;
                Ok(())
            };
            tokio::time::timeout(timeout, fut)
                .await
                .unwrap_or_else(|_| {
                    Err(redis::RedisError::from((
                        redis::ErrorKind::IoError,
                        "redis cache delete: operation timeout",
                    )))
                })
        });
        let elapsed = started.elapsed();

        let outcome: &'static str = match &result {
            Ok(()) => "ok",
            Err(err) => {
                tracing::warn!(
                    plugin_id = PLUGIN_ID,
                    namespace = ns,
                    error = %err,
                    "redis cache: delete failed (trait swallows error)"
                );
                self.maybe_emit_backend_audit("invalidate", ns, err);
                "error"
            }
        };
        self.emit_op_observability("invalidate", ns, outcome, elapsed);
        drop(host_span);
    }

    /// `clear(ns)` walks `SCAN MATCH {prefix}:{ns}:*` to avoid
    /// `FLUSHDB` (which would clobber other workloads sharing
    /// this Redis). Batch-deletes 1000 keys per round-trip — RFC
    /// §4.5 picks the batch size to bound stall time vs. RTT cost.
    fn clear(&self, ns: &str) -> Result<(), CacheError> {
        // Namespace-wide invalidate: same metric names as the
        // single-key `delete` path so operator dashboards roll up
        // both axes naturally; span attrs distinguish kind for
        // forensic drill-down.
        let host_span = self.host_handle().map(|h| {
            h.span(
                "cache_redis.invalidate",
                serde_json::json!({ "scope": ns, "kind": "namespace" }),
            )
        });

        let pattern = self.inner.config.render_namespace_pattern(ns);
        let pool = self.inner.pool.pool.clone();
        let timeout = self.op_timeout();

        let started = Instant::now();
        let result: Result<(), redis::RedisError> = self.inner.runtime.block_on(async move {
            let fut = async move {
                let mut conn = pool.get().await.map_err(error::deadpool_to_redis)?;
                let mut cursor: u64 = 0;
                loop {
                    let (next, batch): (u64, Vec<String>) = redis::cmd("SCAN")
                        .arg(cursor)
                        .arg("MATCH")
                        .arg(&pattern)
                        .arg("COUNT")
                        .arg(1000)
                        .query_async(&mut conn)
                        .await?;
                    if !batch.is_empty() {
                        let _: i64 = redis::cmd("DEL").arg(&batch).query_async(&mut conn).await?;
                    }
                    if next == 0 {
                        break;
                    }
                    cursor = next;
                }
                Ok(())
            };
            tokio::time::timeout(timeout, fut)
                .await
                .unwrap_or_else(|_| {
                    Err(redis::RedisError::from((
                        redis::ErrorKind::IoError,
                        "redis cache clear: operation timeout",
                    )))
                })
        });
        let elapsed = started.elapsed();

        let outcome: &'static str = match &result {
            Ok(()) => "ok",
            Err(err) => {
                self.maybe_emit_backend_audit("invalidate", ns, err);
                "error"
            }
        };
        self.emit_op_observability("invalidate", ns, outcome, elapsed);
        drop(host_span);

        result.map_err(|err| error::map_runtime_error("clear", &err))
    }

    /// Atomic `INCRBY` + `PEXPIRE` via the bundled Lua script.
    /// `redis::Script` handles `EVALSHA` / `EVAL` fallback so a
    /// flushed script cache repairs itself transparently.
    ///
    /// `incr` is a write op (create-or-update); it shares the
    /// `mcpg_cache_redis_put_*` host metric series with `put` so
    /// the operator dashboard's write-latency axis rolls both
    /// paths up. Span name + `op` attr distinguish the two for
    /// forensic drill-down.
    fn incr(&self, ns: &str, key: &str, by: i64, ttl_ms: u64) -> Result<i64, CacheError> {
        let host_span = self.host_handle().map(|h| {
            h.span(
                "cache_redis.put",
                serde_json::json!({ "scope": ns, "op": "incr" }),
            )
        });

        let full_key = self.inner.config.render_key(ns, key);
        let px = if ttl_ms == 0 { 1 } else { ttl_ms };
        let pool = self.inner.pool.pool.clone();
        let script = self.inner.pool.incr_script.clone();
        let timeout = self.op_timeout();

        let started = Instant::now();
        let result: Result<i64, redis::RedisError> = self.inner.runtime.block_on(async move {
            let fut = async move {
                let mut conn = pool.get().await.map_err(error::deadpool_to_redis)?;
                script
                    .key(&full_key)
                    .arg(by)
                    .arg(px)
                    .invoke_async(&mut conn)
                    .await
            };
            tokio::time::timeout(timeout, fut)
                .await
                .unwrap_or_else(|_| {
                    Err(redis::RedisError::from((
                        redis::ErrorKind::IoError,
                        "redis cache incr: operation timeout",
                    )))
                })
        });
        let elapsed = started.elapsed();

        let outcome: &'static str = match &result {
            Ok(_) => "ok",
            Err(err) => {
                self.maybe_emit_backend_audit("put", ns, err);
                "error"
            }
        };
        self.emit_op_observability("put", ns, outcome, elapsed);
        drop(host_span);

        result.map_err(|err| error::map_runtime_error("incr", &err))
    }

    fn shutdown(&self) {
        // Best-effort: deadpool drops connections when the pool
        // itself is dropped (which happens when this `RedisCache`
        // is dropped — the host calls `__mcpg_cache_drop`). No
        // explicit QUIT loop here because deadpool's recycle path
        // already pings; a force-QUIT on every connection would
        // race against in-flight ops and could double-error.
        tracing::info!(
            plugin_id = PLUGIN_ID,
            "redis cache: shutdown signalled — pool will drop on plugin handle drop"
        );
    }
}

impl RedisCache {
    /// Emit the per-op host-observability pair:
    /// latency histogram + outcome counter, through the installed
    /// [`HostHandle`]. Short-circuits to a no-op when no handle is
    /// installed (test paths that construct the plugin directly).
    ///
    /// Cardinality budget for the `outcome` label, per op:
    ///
    /// - `get`        — `hit` / `miss` / `error`
    /// - `put`        — `ok` / `error` (`incr` shares the put series)
    /// - `invalidate` — `ok` / `error` (`delete` + `clear` share)
    ///
    /// `scope` (the binding-level namespace) is intentionally
    /// **not** a metric label — cache namespaces are bounded by
    /// gateway config (operator-declared) but still potentially
    /// numerous in multi-tenant deploys. Stays on the span attrs
    /// for forensic drill-down.
    fn emit_op_observability(
        &self,
        op: &'static str,
        _ns: &str,
        outcome: &'static str,
        duration: std::time::Duration,
    ) {
        let Some(host) = self.host_handle() else {
            return;
        };
        let (hist_name, counter_name) = match op {
            "get" => ("mcpg_cache_redis_get_seconds", "mcpg_cache_redis_get_total"),
            "put" => ("mcpg_cache_redis_put_seconds", "mcpg_cache_redis_put_total"),
            "invalidate" => (
                "mcpg_cache_redis_invalidate_seconds",
                "mcpg_cache_redis_invalidate_total",
            ),
            // Defensive default — unknown op names should never
            // reach here. Route them into the put series so the
            // observation isn't silently dropped, and trace the
            // misuse so it surfaces in code review.
            other => {
                tracing::warn!(
                    plugin_id = PLUGIN_ID,
                    op = other,
                    "redis cache: unknown op in emit_op_observability — routing to put series"
                );
                ("mcpg_cache_redis_put_seconds", "mcpg_cache_redis_put_total")
            }
        };
        host.histogram(hist_name, duration.as_secs_f64(), &[("outcome", outcome)]);
        host.counter(counter_name, 1, &[("outcome", outcome)]);
    }

    /// Sparse backend audit emission. Cache
    /// traffic is high-volume; audit-spam is the risk. We emit a
    /// host audit event ONLY for persistent connection / auth
    /// failures — operator needs to know the cache layer is down
    /// or misconfigured. We do **not** audit transient errors
    /// (`TryAgain`, `BusyLoadingError`) or per-key misses.
    ///
    /// The two audit action names declared by this plugin:
    ///
    /// - `dev.mcpg.cache.redis.connection_failed` — pool acquire
    ///   failed (deadpool error surfaced as IoError); the redis
    ///   instance is unreachable.
    /// - `dev.mcpg.cache.redis.auth_failed` — Redis rejected the
    ///   ACL/AUTH credentials.
    ///
    /// `SyncCachePlugin` methods are dispatched from inside a
    /// tokio worker via `SyncCachePluginAdapter::get/put/...`
    /// (the async adapter calls the sync method directly), so
    /// calling `HostHandle::audit_event` here would re-enter
    /// `Handle::block_on` and panic on "Cannot start a runtime
    /// from within a runtime". We move the call onto a blocking
    /// worker via `spawn_blocking` and detach — audit emission is
    /// best-effort, and joining the handle from a sync method
    /// isn't possible without re-entering the runtime.
    fn maybe_emit_backend_audit(&self, op: &'static str, ns: &str, err: &redis::RedisError) {
        let Some(host) = self.host_handle() else {
            return;
        };

        // Bounded action name — only auth-failed and pool-acquire
        // (== "connection_failed") qualify. Transient retry-able
        // errors are intentionally NOT audited.
        let action: &'static str = match err.kind() {
            redis::ErrorKind::AuthenticationFailed => "dev.mcpg.cache.redis.auth_failed",
            redis::ErrorKind::IoError => {
                // deadpool's pool-acquire failures are mapped into
                // IoError by `error::deadpool_to_redis`. The error
                // message embeds "redis cache pool" so we can
                // distinguish from generic network IO.
                let msg = err.to_string();
                if msg.contains("redis cache pool") {
                    "dev.mcpg.cache.redis.connection_failed"
                } else {
                    // Mid-op network IO — could be transient. We
                    // intentionally do NOT audit this path. Pool-
                    // level acquire failures (above) capture the
                    // "Redis is down" signal at the level
                    // operators actually need.
                    return;
                }
            }
            // BusyLoadingError / TryAgain / TypeError / etc. are
            // transient or operational signals; the per-op metric
            // already exposes them via outcome=error.
            _ => return,
        };

        let details = serde_json::json!({
            "op": op,
            "scope": ns,
            "error": err.to_string(),
            "alias": host.alias(),
        });

        let event = AuditEvent {
            event_id: format!(
                "cache-redis-{}-{}-{}",
                op,
                ns,
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos())
                    .unwrap_or(0),
            ),
            occurred_at: rfc3339_now(),
            actor: synthetic_system_identity(),
            action: action.to_owned(),
            resource: Some(format!("cache://{}/{}", PLUGIN_ID, ns)),
            outcome: AuditOutcome::Failure,
            request_id: None,
            node_id: None,
            details,
            prev_event_hash: None,
        };

        // SyncCachePlugin's `get/put/...` is invoked from inside
        // the gateway's tokio runtime via `SyncCachePluginAdapter`.
        // Calling `HostHandle::audit_event` directly here would
        // re-enter `Handle::block_on` from inside the runtime and
        // panic. Move the call onto a blocking worker and detach —
        // audit emission is best-effort. A planned SDK `_async`
        // variant will retire this detour.
        let host_for_audit = host.clone();
        if let Ok(rt) = tokio::runtime::Handle::try_current() {
            rt.spawn_blocking(move || {
                if let Err(err) = host_for_audit.audit_event(event) {
                    tracing::debug!(
                        target: "mcpg::cache_redis::host_handle",
                        error = %err,
                        "host_handle.audit_event emission failed"
                    );
                }
            });
        } else {
            // Non-runtime path (direct unit-test invocation outside
            // of `#[tokio::test]`). Call the bridge directly —
            // `block_on` will spin up a transient runtime via the
            // host services.
            if let Err(err) = host_for_audit.audit_event(event) {
                tracing::debug!(
                    target: "mcpg::cache_redis::host_handle",
                    error = %err,
                    "host_handle.audit_event emission failed (no runtime)"
                );
            }
        }
    }
}

/// RFC3339 timestamp for audit events. Mirrors the helper in the
/// other observability-enabled plugins so cross-plugin audit
/// lines sort identically. Naïve UTC; no leap-second handling.
fn rfc3339_now() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = now.as_secs() as i64;
    let millis = now.subsec_millis();
    let (year, month, day, hour, min, sec) = epoch_to_ymdhms(secs);
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}.{millis:03}Z")
}

fn epoch_to_ymdhms(secs: i64) -> (i32, u32, u32, u32, u32, u32) {
    let days_since_epoch = secs.div_euclid(86_400);
    let secs_today = secs.rem_euclid(86_400) as u32;
    let hour = secs_today / 3600;
    let min = (secs_today % 3600) / 60;
    let sec = secs_today % 60;
    let z = days_since_epoch + 719_468;
    let era = z.div_euclid(146_097);
    let doe = (z - era * 146_097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe as i32 + era as i32 * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if m <= 2 { y + 1 } else { y };
    (year, m, d, hour, min, sec)
}

/// Synthetic identity for audit events emitted from infrastructure
/// paths with no caller attribution. Mirrors the other
/// observability-enabled plugins so cross-plugin audit search
/// treats system traffic uniformly.
fn synthetic_system_identity() -> PluginIdentity {
    PluginIdentity {
        kind: "system".into(),
        trust_level: "verified".into(),
        subject_id: Some(PLUGIN_ID.into()),
        auth_provider: None,
        issuer: None,
        roles: vec![],
        groups: vec![],
        scopes: vec![],
        attributes: Default::default(),
    }
}

declare_plugin! {
    plugin_id: PLUGIN_ID,
    plugin_version: env!("CARGO_PKG_VERSION"),
    descriptor_yaml: include_str!("../plugin.yaml"),
    capabilities: &[mcpg_plugin_protocol::capability::Capability::NetworkOutbound],
    entities: [
        cache as entity {
            inner_name: "",
            plugin_type: RedisCache,
            // Install the unified `HostHandle` on
            // the plugin so per-op observability (span + latency
            // histogram + outcome counter + sparse audit on
            // connection / auth failure) routes through the
            // gateway's central host-services sink. Idempotent — a
            // second install returns false and the slot remains
            // untouched.
            factory: |cfg: &str, host: ::mcpg_plugin_sdk::HostHandle| -> RedisCache {
                let plugin = RedisCache::from_config_json(cfg);
                let _installed = plugin.set_host_handle(host);
                plugin
            },
        }
    ],
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A factory that points at a guaranteed-unreachable host so
    /// the unit suite doesn't depend on (or interfere with) any
    /// local Redis the developer might have running. `.invalid` is
    /// the RFC 6761 TLD that DNS resolvers MUST refuse to resolve.
    /// `pool::RedisPool::from_config` doesn't open a connection
    /// until the first op runs, so registration succeeds; only the
    /// actual op times out.
    fn cheap_plugin() -> RedisCache {
        RedisCache::from_config_json(
            r#"{"url": "redis://redis.invalid:6379", "connection": {"connect_timeout_ms": 50, "operation_timeout_ms": 50}}"#,
        )
    }

    #[test]
    fn factory_parses_minimal_config() {
        let _plugin = cheap_plugin();
    }

    #[test]
    #[should_panic(expected = "redis cache config parse failed")]
    fn factory_panics_on_unparseable_config() {
        let _ = RedisCache::from_config_json("not-json");
    }

    #[test]
    fn manifest_carries_required_capability() {
        // Typed capabilities live on
        // `PluginRegistration.capabilities` via the SDK macro;
        // manifest's `Vec<String>` is display-only.
        let plugin = cheap_plugin();
        let manifest = plugin.manifest();
        assert_eq!(manifest.id, PLUGIN_ID);
        assert_eq!(manifest.plugin_class, PluginClass::Cache);
    }

    #[test]
    fn serves_any_namespace_is_true() {
        let plugin = cheap_plugin();
        assert!(plugin.serves_any_namespace());
        assert!(plugin.supported_namespaces().is_empty());
    }

    #[test]
    fn descriptor_yaml_is_well_formed() {
        assert!(DESCRIPTOR_YAML.contains(&format!("id: {PLUGIN_ID}")));
        assert!(DESCRIPTOR_YAML.contains("class: cache"));
        assert!(DESCRIPTOR_YAML.contains("runtime: native-cdylib-v1"));
        assert!(DESCRIPTOR_YAML.contains("network_outbound"));
    }

    /// When Redis is unreachable the connection attempt times out
    /// inside `pool.get()`. The trait contract for `get` is
    /// "miss-on-error" — verify we don't panic + return `None`.
    #[test]
    fn get_returns_none_when_redis_unreachable() {
        let plugin = cheap_plugin();
        // Backend not running on this port in CI; the timeout is
        // 50ms so the test stays fast.
        let v = plugin.get("test", "missing");
        assert_eq!(v, None);
    }

    /// When Redis is unreachable `put` must surface `CacheError`
    /// (not silently swallow). Backend kind expected.
    #[test]
    fn put_surfaces_backend_error_when_redis_unreachable() {
        let plugin = cheap_plugin();
        let err = plugin.put("test", "k", b"v".to_vec(), 1_000).unwrap_err();
        assert!(
            matches!(err, CacheError::Backend { .. }),
            "expected CacheError::Backend, got {err:?}"
        );
    }
}
