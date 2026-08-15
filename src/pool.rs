//! Connection pool + Lua script setup.
//!
//! deadpool-redis 0.18 wraps a `redis::Client`; we configure
//! pool sizing + per-op timeouts from the operator config and
//! build the multiplexed connection manager once at plugin
//! registration.

use std::time::Duration;

use deadpool_redis::{Config as PoolConfig, Pool, Runtime};

use crate::config::{ConnectionConfig, RedisCacheConfig};
use crate::error::ConfigError;

/// Pool + scripts. Cheap to clone since the pool is itself
/// `Arc<Inner>` upstream; we expose it by reference everywhere
/// downstream so the redundant clone never happens.
pub(crate) struct RedisPool {
    pub(crate) pool: Pool,
    pub(crate) connection: ConnectionConfig,
    /// Atomic-incr Lua script. Held as a typed `redis::Script` so
    /// the `redis` crate handles `EVALSHA` / `EVAL` fallback
    /// transparently — we don't have to maintain our own NOSCRIPT
    /// recovery loop.
    pub(crate) incr_script: redis::Script,
}

impl RedisPool {
    pub(crate) fn from_config(cfg: &RedisCacheConfig) -> Result<Self, ConfigError> {
        // deadpool builds its `redis::Client` from the URL string +
        // an optional `redis::ConnectionInfo`. The simpler `Config`
        // path takes the URL directly.
        let mut pool_cfg = PoolConfig::from_url(&cfg.url);

        // Pool sizing + timeouts. deadpool's `Pool::new` accepts a
        // `PoolConfig::PoolConfig` — we set what we need and leave
        // the rest at deadpool defaults.
        let mut pool_inner = deadpool_redis::PoolConfig::new(cfg.connection.pool_size);
        pool_inner.timeouts.wait = Some(Duration::from_millis(cfg.connection.connect_timeout_ms));
        pool_inner.timeouts.create = Some(Duration::from_millis(cfg.connection.connect_timeout_ms));
        pool_inner.timeouts.recycle = Some(Duration::from_millis(100));
        pool_cfg.pool = Some(pool_inner);

        // Tokio runtime wired in via `Runtime::Tokio1` — we own the
        // runtime, deadpool just needs the marker so its async
        // primitives know which executor to spawn on.
        let pool = pool_cfg
            .create_pool(Some(Runtime::Tokio1))
            .map_err(|e| ConfigError::Invalid(format!("redis pool: {e}")))?;

        let incr_script = redis::Script::new(
            // KEYS[1]  — fully-qualified key (`{prefix}:{ns}:{key}`)
            // ARGV[1]  — increment delta (signed)
            // ARGV[2]  — TTL in milliseconds
            //
            // INCRBY + PEXPIRE in one server-side call. Without the
            // script the `INCR` and `PEXPIRE` race against a
            // concurrent `DEL`.
            "local v = redis.call('INCRBY', KEYS[1], ARGV[1])\n\
             redis.call('PEXPIRE', KEYS[1], ARGV[2])\n\
             return v",
        );

        Ok(Self {
            pool,
            connection: cfg.connection.clone(),
            incr_script,
        })
    }
}
