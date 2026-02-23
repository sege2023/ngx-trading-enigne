//! Pipeline orchestrator - currently minimal (CSV-only mode)
//!
//! This module is a placeholder for future scraper-based daily updates.
//! Right now, all data loading happens via CLI commands (load-tickers, load-equities, load-fx).

use crate::config::AppConfig;
use anyhow::Result;

pub struct Pipeline {
    #[allow(dead_code)]
    config: AppConfig,
}

impl Pipeline {
    pub fn new(config: AppConfig) -> Self {
        Self { config }
    }

    /// Placeholder for future scraper-based updates
    pub async fn run(&self) -> Result<PipelineStats> {
        anyhow::bail!("Scraper mode not implemented yet. Use CSV loading commands instead:\n  \
                       cargo run -- load-tickers data/tickers.csv\n  \
                       cargo run -- load-equities --dir data\n  \
                       cargo run -- load-fx --dir data")
    }
}

#[derive(Debug)]
pub struct PipelineStats {
    pub tickers_processed: usize,
    pub bars_inserted: usize,
    pub errors: usize,
}