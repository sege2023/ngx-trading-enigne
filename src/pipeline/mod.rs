//! Pipeline orchestrator: ties scraper → storage together.
//!
//! ## Run modes
//!
//! `run()` — daily mode (default / cron use):
//!   1. Crawl listing pages → upsert/update tickers table
//!   2. For each known symbol, fetch recent bars (~10 rows) → upsert into daily_bars
//!   Idempotent: re-running the same day inserts 0 new rows (ON CONFLICT DO UPDATE).
//!
//! `run_full_listing()` — use this on first run to populate the tickers table quickly
//!   without hitting every ticker page (useful when you just want the symbol list first).

use crate::config::AppConfig;
use crate::scraper::{KwayisiScraper, MarketDataSource};
use crate::storage::Repository;
use anyhow::{Context, Result};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{error, info, warn};

pub struct Pipeline {
    config: AppConfig,
}

impl Pipeline {
    pub fn new(config: AppConfig) -> Self {
        Self { config }
    }

    pub async fn run(&self) -> Result<PipelineStats> {
        let repo = Arc::new(
            Repository::open(&self.config.storage.db_path)
                .context("Failed to open DuckDB")?,
        );

        if self.config.storage.run_migrations {
            repo.run_migrations()?;
        }

        let scraper = Arc::new(
            KwayisiScraper::new(&self.config.scraper)
                .context("Failed to build scraper")?,
        );

        let run_id = repo.begin_scrape_run().unwrap_or(0);

        // ── 1. Discover / refresh ticker list ─────────────────────────────────
        info!("=== Step 1: Refreshing ticker list ===");
        let tickers = scraper.fetch_ticker_list().await
            .context("Ticker list fetch failed")?;

        repo.upsert_tickers(&tickers)?;
        let symbols: Vec<String> = tickers.iter().map(|t| t.symbol.clone()).collect();
        info!("{} tickers in DB", symbols.len());

        // ── 2. Fetch recent bars for every ticker ──────────────────────────────
        info!("=== Step 2: Fetching recent bars ({} tickers) ===", symbols.len());
        info!("NOTE: kwayisi serves ~10 rows per ticker page. Run daily to accumulate history.");

        let sem = Arc::new(Semaphore::new(self.config.pipeline.concurrency));
        let mut handles = Vec::new();
        let mut total_bars = 0usize;
        let mut errors = 0usize;

        for symbol in &symbols {
            let symbol = symbol.clone();
            let scraper = Arc::clone(&scraper);
            let repo = Arc::clone(&repo);
            let sem = Arc::clone(&sem);

            let handle = tokio::spawn(async move {
                let _permit = sem.acquire().await?;

                let bars = scraper.fetch_recent_bars(&symbol).await
                    .with_context(|| format!("fetch_recent_bars({})", symbol))?;

                let n = bars.len();
                repo.upsert_daily_bars(&bars)
                    .with_context(|| format!("upsert_daily_bars({})", symbol))?;

                info!("{}: {} bars (latest: {:?})",
                    symbol, n,
                    bars.iter().map(|b| b.date).max()
                );

                Ok::<usize, anyhow::Error>(n)
            });

            handles.push((symbol.clone(), handle));
        }

        for (symbol, handle) in handles {
            match handle.await {
                Ok(Ok(n)) => total_bars += n,
                Ok(Err(e)) => { warn!("{}: {:#}", symbol, e); errors += 1; }
                Err(e) => { error!("Task panic for {}: {}", symbol, e); errors += 1; }
            }
        }

        let stats = PipelineStats {
            tickers_processed: symbols.len(),
            bars_inserted: total_bars,
            errors,
        };

        repo.finish_scrape_run(
            run_id,
            stats.tickers_processed,
            stats.bars_inserted,
            if errors > 0 { Some(&format!("{} errors", errors)) } else { None },
        ).ok();

        let (min_date, max_date) = repo.date_range().unwrap_or((None, None));
        info!("=== Done: {} tickers | {} new bars | {} errors | DB range: {:?} → {:?} ===",
            stats.tickers_processed, stats.bars_inserted, stats.errors,
            min_date, max_date,
        );

        Ok(stats)
    }
}

#[derive(Debug)]
pub struct PipelineStats {
    pub tickers_processed: usize,
    pub bars_inserted: usize,
    pub errors: usize,
}