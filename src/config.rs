//! Operator-supplied configuration for the Redis cache plugin.
//!
//! Fields not present in the JSON
//! get conservative defaults (see the field docs). The shape is
//! validated at registration time — `RedisCache::from_config_json`
//! rejects an unparseable blob, refusing to register.

use serde::{Deserialize, Serialize};

use crate::error::ConfigError;

/// Top-level config. Every field except `url` has a default;
/// operators only set what they need to override.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedisCacheConfig {
    /// Connection URL. `redis://` (standalone) or `rediss://`
    /// (standalone over TLS). Required — there's no sane default
    /// location for a Redis instance.
    ///
    /// Sentinel (`redis+sentinel://`) and cluster topologies are
    /// not yet supported — the validator rejects those URL forms
    /// at boot rather than silently dropping into standalone
    /// mode against a Sentinel coordinator. Tracked as v0.3.
    pub url: String,

    /// Optional auth — set when the operator's Redis requires it.
    #[serde(default)]
    pub auth: Option<AuthConfig>,

    /// Optional TLS knobs. Only consulted when `url` uses
    /// `rediss://`. Omitted = system trust roots, peer verification
    /// on.
    #[serde(default)]
    pub tls: Option<TlsConfig>,

    /// Connection-pool sizing + timeouts.
    #[serde(default)]
    pub connection: ConnectionConfig,

    /// Prefix prepended to every Redis key — `{prefix}:{ns}:{key}`.
    /// Operators with multi-tenant Redis override to scope this
    /// plugin's keys to its tenant slice.
    #[serde(default = "default_key_prefix")]
    pub key_prefix: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthConfig {
    /// Optional ACL username. Omit for classic Redis AUTH (password
    /// only).
    #[serde(default)]
    pub username: Option<String>,

    /// Password (or ACL secret). Resolved by the gateway's
    /// secret-ref interpolator before the JSON reaches us, so by
    /// the time we see it the value is plaintext.
    pub password: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    #[serde(default = "default_tls_enabled")]
    pub enabled: bool,
    /// Optional CA path. Omit to use the system trust roots.
    #[serde(default)]
    pub ca_cert: Option<String>,
    /// Disable to permit a self-signed Redis (dev only — RFC
    /// strongly recommends leaving on in production).
    #[serde(default = "default_verify_peer")]
    pub verify_peer: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectionConfig {
    #[serde(default = "default_pool_size")]
    pub pool_size: usize,
    #[serde(default = "default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    #[serde(default = "default_operation_timeout_ms")]
    pub operation_timeout_ms: u64,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            pool_size: default_pool_size(),
            connect_timeout_ms: default_connect_timeout_ms(),
            operation_timeout_ms: default_operation_timeout_ms(),
        }
    }
}

fn default_key_prefix() -> String {
    "mcpg".into()
}

fn default_tls_enabled() -> bool {
    true
}

fn default_verify_peer() -> bool {
    true
}

fn default_pool_size() -> usize {
    16
}

fn default_connect_timeout_ms() -> u64 {
    1_000
}

fn default_operation_timeout_ms() -> u64 {
    5_000
}

impl RedisCacheConfig {
    /// Parse + validate the config blob the host hands to
    /// `__mcpg_cache_make`. Returns a `ConfigError` whose message
    /// the host will surface verbatim in the startup log.
    pub fn parse(config_json: &str) -> Result<Self, ConfigError> {
        let cfg: Self = serde_json::from_str(config_json)
            .map_err(|e| ConfigError::ParseError(e.to_string()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if self.url.trim().is_empty() {
            return Err(ConfigError::Invalid("`url` must not be empty".into()));
        }
        if self.url.starts_with("redis+sentinel://") {
            return Err(ConfigError::Invalid(format!(
                "`url` scheme `redis+sentinel://` is not supported in this version — \
                 Sentinel topology requires a separate pool path that's deferred to v0.3. \
                 Point at the current Sentinel-elected master via `redis://` for now. \
                 (got: `{}`)",
                self.url
            )));
        }
        if !(self.url.starts_with("redis://") || self.url.starts_with("rediss://")) {
            return Err(ConfigError::Invalid(format!(
                "`url` must use scheme redis:// or rediss:// — got `{}`",
                self.url
            )));
        }
        if self.connection.pool_size == 0 {
            return Err(ConfigError::Invalid(
                "`connection.pool_size` must be > 0".into(),
            ));
        }
        if self.connection.connect_timeout_ms == 0 {
            return Err(ConfigError::Invalid(
                "`connection.connect_timeout_ms` must be > 0".into(),
            ));
        }
        if self.connection.operation_timeout_ms == 0 {
            return Err(ConfigError::Invalid(
                "`connection.operation_timeout_ms` must be > 0".into(),
            ));
        }
        if self.key_prefix.trim().is_empty() {
            return Err(ConfigError::Invalid(
                "`key_prefix` must not be empty".into(),
            ));
        }
        Ok(())
    }

    /// Render the canonical Redis key.
    pub fn render_key(&self, ns: &str, key: &str) -> String {
        format!("{}:{}:{}", self.key_prefix, ns, key)
    }

    /// Render the SCAN match pattern for `clear(ns)`.
    pub fn render_namespace_pattern(&self, ns: &str) -> String {
        format!("{}:{}:*", self.key_prefix, ns)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn minimal_config_parses() {
        let cfg = RedisCacheConfig::parse(r#"{"url": "redis://localhost:6379"}"#).unwrap();
        assert_eq!(cfg.url, "redis://localhost:6379");
        assert_eq!(cfg.key_prefix, "mcpg");
        assert_eq!(cfg.connection.pool_size, 16);
        assert!(cfg.auth.is_none());
    }

    #[test]
    fn rediss_url_is_accepted() {
        let cfg = RedisCacheConfig::parse(r#"{"url": "rediss://redis.example:6380"}"#).unwrap();
        assert!(cfg.url.starts_with("rediss://"));
    }

    #[test]
    fn http_url_is_rejected() {
        let err = RedisCacheConfig::parse(r#"{"url": "http://localhost:6379"}"#).unwrap_err();
        assert!(err.to_string().contains("scheme"));
    }

    #[test]
    fn sentinel_url_is_rejected_with_clear_pointer_to_v03() {
        let err =
            RedisCacheConfig::parse(r#"{"url": "redis+sentinel://sentinel-0:26379/mymaster"}"#)
                .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("redis+sentinel"), "got: {msg}");
        assert!(
            msg.contains("v0.3") || msg.contains("not supported"),
            "expected pointer to v0.3 / unsupported state; got: {msg}"
        );
        assert!(
            msg.contains("Sentinel-elected master"),
            "expected workaround hint; got: {msg}"
        );
    }

    #[test]
    fn empty_url_is_rejected() {
        let err = RedisCacheConfig::parse(r#"{"url": ""}"#).unwrap_err();
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn unknown_field_is_rejected() {
        let err = RedisCacheConfig::parse(r#"{"url": "redis://localhost", "bogus_field": 42}"#)
            .unwrap_err();
        assert!(err.to_string().contains("bogus_field"));
    }

    #[test]
    fn zero_pool_size_is_rejected() {
        let err = RedisCacheConfig::parse(
            r#"{"url": "redis://localhost", "connection": {"pool_size": 0}}"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("pool_size"));
    }

    #[test]
    fn render_key_uses_prefix_namespace_key() {
        let cfg = RedisCacheConfig::parse(r#"{"url": "redis://localhost"}"#).unwrap();
        assert_eq!(
            cfg.render_key("rate-limit", "tool:alice"),
            "mcpg:rate-limit:tool:alice"
        );
    }

    #[test]
    fn custom_key_prefix_overrides_default() {
        let cfg =
            RedisCacheConfig::parse(r#"{"url": "redis://localhost", "key_prefix": "tenant-a"}"#)
                .unwrap();
        assert_eq!(cfg.render_key("ns", "k"), "tenant-a:ns:k");
        assert_eq!(cfg.render_namespace_pattern("ns"), "tenant-a:ns:*");
    }

    #[test]
    fn auth_username_is_optional() {
        let cfg =
            RedisCacheConfig::parse(r#"{"url": "redis://localhost", "auth": {"password": "p"}}"#)
                .unwrap();
        let auth = cfg.auth.unwrap();
        assert!(auth.username.is_none());
        assert_eq!(auth.password, "p");
    }
}
