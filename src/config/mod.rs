use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Top-level application configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AppConfig {
    pub scraper: ScraperConfig,
    pub storage: StorageConfig,
    pub pipeline: PipelineConfig,
}

/// Scraper configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ScraperConfig {
    #[serde(default = "default_base_url")]
    pub base_url: String,

    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,

    #[serde(default = "default_request_delay_ms")]
    pub request_delay_ms: u64,

    #[serde(default = "default_jitter_ms")]
    pub jitter_ms: u64,

    #[serde(default = "default_max_retries")]
    pub max_retries: u32,

    #[serde(default = "default_user_agent")]
    pub user_agent: String,
}

/// Storage configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StorageConfig {
    #[serde(default = "default_db_path")]
    pub db_path: PathBuf,

    #[serde(default = "default_true")]
    pub run_migrations: bool,
}

/// Pipeline configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PipelineConfig {
    #[serde(default)]
    pub backfill: bool,

    #[serde(default = "default_concurrency")]
    pub concurrency: usize,

    #[serde(default = "default_true")]
    pub skip_up_to_date: bool,
}

// ── Defaults ─────────────────────────────────────────────────────────────────

fn default_base_url() -> String {
    "https://afx.kwayisi.org/ngx".to_string()
}
fn default_timeout_secs() -> u64 {
    30
}
fn default_request_delay_ms() -> u64 {
    1500
}
fn default_jitter_ms() -> u64 {
    500
}
fn default_max_retries() -> u32 {
    3
}
fn default_user_agent() -> String {
    "ngx-trading-engine/0.1 (research project; full pipleine quantitative research)".to_string()
}
fn default_db_path() -> PathBuf {
    PathBuf::from("data/ngx.duckdb")
}
fn default_true() -> bool {
    true
}
fn default_concurrency() -> usize {
    3
}

// ── Loader ───────────────────────────────────────────────────────────────────

impl AppConfig {
    /// Load configuration from file + environment overrides
    pub fn load() -> Result<Self> {
        dotenv::dotenv().ok();

        let cfg = config::Config::builder()
            .add_source(
                config::File::with_name("config/default")
                    .required(false)
                    .format(config::FileFormat::Toml),
            )
            .add_source(
                config::File::with_name("config/local")
                    .required(false)
                    .format(config::FileFormat::Toml),
            )
            .add_source(config::Environment::with_prefix("NGX").separator("__"))
            .build()?;

        let app_cfg: AppConfig = cfg.try_deserialize().unwrap_or_else(|_| AppConfig::default());
        Ok(app_cfg)
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            scraper: ScraperConfig {
                base_url: default_base_url(),
                timeout_secs: default_timeout_secs(),
                request_delay_ms: default_request_delay_ms(),
                jitter_ms: default_jitter_ms(),
                max_retries: default_max_retries(),
                user_agent: default_user_agent(),
            },
            storage: StorageConfig {
                db_path: default_db_path(),
                run_migrations: true,
            },
            pipeline: PipelineConfig {
                backfill: false,
                concurrency: default_concurrency(),
                skip_up_to_date: true,
            },
        }
    }
}