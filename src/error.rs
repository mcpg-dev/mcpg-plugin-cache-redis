//! Error mapping. Config errors stay local; Redis-runtime errors
//! translate to [`CacheError`].

use mcpg_plugin_protocol::cache::CacheError;
use thiserror::Error;

/// Failures produced while parsing the operator-supplied config
/// blob. Surface verbatim in the host's startup log; never reaches
/// the trait's `CacheError` (a config failure prevents the plugin
/// from registering at all).
#[derive(Debug, Clone, Error)]
pub enum ConfigError {
    #[error("redis cache config: failed to parse JSON: {0}")]
    ParseError(String),

    #[error("redis cache config: invalid: {0}")]
    Invalid(String),
}

/// Map a deadpool acquire failure (pool exhausted, connect failed,
/// recycle ping failed) into a `redis::RedisError`. Lets the
/// runtime path stay typed on `RedisError` end-to-end so the
/// per-op closures don't have to thread two error types through.
pub(crate) fn deadpool_to_redis(err: deadpool_redis::PoolError) -> redis::RedisError {
    redis::RedisError::from((
        redis::ErrorKind::IoError,
        "redis cache pool",
        err.to_string(),
    ))
}

/// Translate a runtime `RedisError` into the wire-stable
/// [`CacheError`]. `op` names the trait method invoked — included
/// in the `reason` so log readers can correlate without needing
/// the metric label. The mapping is:
///
/// | RedisErrorKind                | CacheError          |
/// |-------------------------------|---------------------|
/// | IoError                       | Backend             |
/// | AuthenticationFailed          | Backend             |
/// | BusyLoadingError              | Throttled           |
/// | TryAgain                      | Throttled           |
/// | (timeout via tokio)           | Backend             |
/// | (anything else)               | Backend             |
pub(crate) fn map_runtime_error(op: &'static str, err: &redis::RedisError) -> CacheError {
    match err.kind() {
        redis::ErrorKind::BusyLoadingError | redis::ErrorKind::TryAgain => CacheError::Throttled,
        _ => CacheError::Backend {
            reason: format!("redis cache {op}: {err}"),
        },
    }
}
