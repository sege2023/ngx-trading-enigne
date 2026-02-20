use chrono::{NaiveDate, NaiveDateTime};
use serde::{Deserialize, Serialize};

// ── Ticker / Company listing ──────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Ticker {
    pub symbol: String,
    pub name: String,
    pub sector: Option<String>,
    pub board: Option<String>,
    pub isin: Option<String>,
    pub scraped_at: NaiveDateTime,
}


#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DailyBar {
    pub symbol: String,
    pub date: NaiveDate,
    pub open: Option<f64>,
    pub high: Option<f64>,
    pub low: Option<f64>,
    /// Close price (investing.com calls this "Price")
    pub close: f64,
    pub change: Option<f64>,
    pub change_pct: Option<f64>,
    pub volume: Option<i64>,
    pub deals: Option<i64>,
    pub scraped_at: NaiveDateTime,
}


#[derive(Debug, Clone, Default)]
pub struct RawCsvRow {
    pub date: Option<String>,       // e.g. "Feb 20, 2024"
    pub price: Option<String>,      // Close price, e.g. "610.00"
    pub open: Option<String>,       // e.g. "605.50"
    pub high: Option<String>,       // e.g. "612.00"
    pub low: Option<String>,        // e.g. "603.00"
    pub volume: Option<String>,     // e.g. "1.2M", "345K", "12345"
    pub change_pct: Option<String>, // e.g. "+2.09%"
}

// ── Market-level snapshot ─────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketSnapshot {
    pub date: NaiveDate,
    pub asi: f64,
    pub asi_change: Option<f64>,
    pub asi_change_pct: Option<f64>,
    pub market_cap_ngn: Option<f64>,
    pub total_volume: Option<i64>,
    pub total_deals: Option<i64>,
    pub gainers: Option<i32>,
    pub losers: Option<i32>,
    pub unchanged: Option<i32>,
    pub scraped_at: NaiveDateTime,
}

// ── Raw scraped types (pre-cleaning) ─────────────────────────────────────────

/// Raw row from the main listing page — all strings, all optional.
#[derive(Debug, Clone, Default)]
pub struct RawEquityRow {
    pub symbol: Option<String>,
    pub name: Option<String>,
    pub price: Option<String>,
    pub change: Option<String>,
    pub change_pct: Option<String>,
    pub volume: Option<String>,
    pub deals: Option<String>,
    pub sector: Option<String>,
}


#[derive(Debug, Clone, Default)]
pub struct RawHistoricalRow {
    pub date: Option<String>,
    pub open: Option<String>,  
    pub high: Option<String>,   
    pub low: Option<String>,   
    pub close: Option<String>,
    pub change: Option<String>,
    pub volume: Option<String>,
    pub deals: Option<String>,
}