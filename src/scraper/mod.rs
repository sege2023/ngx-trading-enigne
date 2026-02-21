pub mod cleaner;
pub mod http_client;
pub mod parsers;

use crate::config::ScraperConfig;
use crate::models::{DailyBar, Ticker};
use anyhow::{Context, Result};
use async_trait::async_trait;
use tracing::{debug, info, warn};

use self::cleaner::{clean_historical_rows, clean_ticker_rows};
use self::http_client::HttpClient;
use self::parsers::{parse_listing_page, parse_ticker_meta, parse_ticker_page};

// ── Source trait ──────────────────────────────────────────────────────────────

/// Swappable data source abstraction.
#[async_trait]
pub trait MarketDataSource: Send + Sync {
    async fn fetch_ticker_list(&self) -> Result<Vec<Ticker>>;
    async fn fetch_recent_bars(&self, symbol: &str) -> Result<Vec<DailyBar>>;
}

// ── kwayisi scraper ───────────────────────────────────────────────────────────

pub struct KwayisiScraper {
    client: HttpClient,
    base_url: String,
}

impl KwayisiScraper {
    pub fn new(config: &ScraperConfig) -> Result<Self> {
        Ok(Self {
            client: HttpClient::new(config)?,
            base_url: config.base_url.trim_end_matches('/').to_string(),
        })
    }

    /// URL for the listing index page (paginated).
    fn listing_url(&self, page: u32) -> String {
        if page <= 1 {
            format!("{}/", self.base_url)
        } else {
            format!("{}/?page={}", self.base_url, page)
        }
    }

    /// URL for a specific ticker's page.  e.g. DANGCEM → /ngx/dangcem.html
    fn ticker_url(&self, symbol: &str) -> String {
        format!("{}/{}.html", self.base_url, symbol.to_lowercase())
    }
}

#[async_trait]
impl MarketDataSource for KwayisiScraper {
    async fn fetch_ticker_list(&self) -> Result<Vec<Ticker>> {
        let mut all_tickers = Vec::new();
        let mut page = 1u32;

        loop {
            let url = self.listing_url(page);
            info!("Fetching listing page {} ({})", page, url);

            let html = self.client.get_text(&url).await
                .with_context(|| format!("Failed to fetch listing page {}", page))?;

            let (raw_rows, _hrefs) = parse_listing_page(&html)?;

            if raw_rows.is_empty() {
                debug!("Empty page {} — stopping pagination", page);
                break;
            }

            let tickers = clean_ticker_rows(raw_rows);
            info!("  Page {}: {} tickers", page, tickers.len());
            all_tickers.extend(tickers);

            if !parsers::has_next_page(&html) {
                break;
            }

            page += 1;

            if page > 15 {
                warn!("Reached page limit (15), stopping");
                break;
            }
        }

        info!("Total tickers discovered: {}", all_tickers.len());
        Ok(all_tickers)
    }

    
    async fn fetch_recent_bars(&self, symbol: &str) -> Result<Vec<DailyBar>> {
        let url = self.ticker_url(symbol);
        debug!("Fetching ticker page: {}", url);

        let html = self.client.get_text(&url).await
            .with_context(|| format!("Failed to fetch ticker page for {}", symbol))?;

        let raw_rows = parse_ticker_page(&html, symbol)?;

        if raw_rows.is_empty() {
            warn!("{}: no rows found on ticker page", symbol);
        }

        let bars = clean_historical_rows(symbol, raw_rows);

        // Also grab metadata for ticker enrichment
        let meta = parse_ticker_meta(&html);
        debug!("{}: {} bars, sector={:?}", symbol, bars.len(), meta.sector);

        Ok(bars)
    }
}

/// Returns the ticker symbol list extracted from the listing pages.
/// Useful for seeding the DB before scraping individual pages.
pub async fn discover_all_symbols(scraper: &KwayisiScraper) -> Result<Vec<String>> {
    let tickers = scraper.fetch_ticker_list().await?;
    Ok(tickers.into_iter().map(|t| t.symbol).collect())
}