//! CSV loaders for investing.com data.

use crate::models::{DailyBar, FxRate, RawCsvRow, RawFxCsvRow, RawTickerRow, Ticker};
use crate::scraper::cleaner::{csv_row_to_bar, fx_csv_row_to_rate, ticker_row_to_ticker};
use anyhow::{Context, Result};
use chrono::Utc;
use std::path::{Path, PathBuf};
use tracing::{debug, info, warn};

// ── Symbol/pair extraction ───────────────────────────────────────────────────

/// Extract ticker symbol from filename: "DANGCEM_historical.csv" → "DANGCEM"
pub fn extract_symbol_from_filename(path: &Path) -> Option<String> {
    let stem = path.file_stem()?.to_str()?;
    let symbol = stem
        .split(|c| c == '_' || c == ' ' || c == '.')
        .next()?
        .trim()
        .to_uppercase();

    if symbol.is_empty() { None } else { Some(symbol) }
}

/// Extract FX pair from filename: "USDNGN_historical.csv" → "USDNGN"
pub fn extract_pair_from_filename(path: &Path) -> Option<String> {
    extract_symbol_from_filename(path)
}

// ── Equity price CSV ──────────────────────────────────────────────────────────

/// Load investing.com equity CSV: Date, Price, Open, High, Low, Volume, Change%
pub fn load_equity_csv(path: &Path) -> Result<(String, Vec<DailyBar>)> {
    let symbol = extract_symbol_from_filename(path)
        .with_context(|| format!("No symbol in filename {:?}", path))?;

    debug!("Loading equity {} from {:?}", symbol, path);

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_path(path)?;

    let now = Utc::now().naive_utc();
    let mut bars = Vec::new();

    for (i, result) in reader.records().enumerate() {
        let record = match result {
            Ok(r) => r,
            Err(e) => {
                warn!("Row {} in {:?}: {}", i + 1, path, e);
                continue;
            }
        };

        let raw = RawCsvRow {
            date: record.get(0).map(|s| s.to_string()),
            price: record.get(1).map(|s| s.to_string()),
            open: record.get(2).map(|s| s.to_string()),
            high: record.get(3).map(|s| s.to_string()),
            low: record.get(4).map(|s| s.to_string()),
            volume: record.get(5).map(|s| s.to_string()),
            change_pct: record.get(6).map(|s| s.to_string()),
        };

        if let Some(bar) = csv_row_to_bar(&symbol, &raw, now) {
            bars.push(bar);
        }
    }

    info!("{}: {} bars loaded", symbol, bars.len());
    Ok((symbol, bars))
}

// ── FX rate CSV ───────────────────────────────────────────────────────────────


pub fn load_fx_csv(path: &Path, source: Option<&str>) -> Result<(String, Vec<FxRate>)> {
    let pair = extract_pair_from_filename(path)
        .with_context(|| format!("No FX pair in filename {:?}", path))?;

    debug!("Loading FX pair {} from {:?}", pair, path);

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_path(path)?;

    let now = Utc::now().naive_utc();
    let mut rates = Vec::new();

    for (i, result) in reader.records().enumerate() {
        let record = match result {
            Ok(r) => r,
            Err(e) => {
                warn!("Row {} in {:?}: {}", i + 1, path, e);
                continue;
            }
        };

        let raw = RawFxCsvRow {
            date: record.get(0).map(|s| s.to_string()),
            price: record.get(1).map(|s| s.to_string()),
            open: record.get(2).map(|s| s.to_string()),
            high: record.get(3).map(|s| s.to_string()),
            low: record.get(4).map(|s| s.to_string()),
            change_pct: record.get(5).map(|s| s.to_string()),
        };

        if let Some(rate) = fx_csv_row_to_rate(&pair, &raw, source, now) {
            rates.push(rate);
        }
    }

    info!("{}: {} rates loaded", pair, rates.len());
    Ok((pair, rates))
}

// ── Ticker metadata CSV ───────────────────────────────────────────────────────

/// Load ticker metadata CSV: symbol, name, sector, industry, exchange
pub fn load_tickers_csv(path: &Path) -> Result<Vec<Ticker>> {
    debug!("Loading tickers from {:?}", path);

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_path(path)?;

    let now = Utc::now().naive_utc();
    let mut tickers = Vec::new();

    for (i, result) in reader.records().enumerate() {
        let record = match result {
            Ok(r) => r,
            Err(e) => {
                warn!("Row {} in {:?}: {}", i + 1, path, e);
                continue;
            }
        };

        let raw = RawTickerRow {
            symbol: record.get(0).map(|s| s.to_string()),
            name: record.get(1).map(|s| s.to_string()),
            sector: record.get(2).map(|s| s.to_string()),
            industry: record.get(3).map(|s| s.to_string()),
            exchange: record.get(4).map(|s| s.to_string()),
        };

        if let Some(ticker) = ticker_row_to_ticker(&raw, now) {
            tickers.push(ticker);
        }
    }

    info!("Loaded {} tickers", tickers.len());
    Ok(tickers)
}

// ── File discovery ────────────────────────────────────────────────────────────

pub fn discover_csv_files(dir: &Path) -> Result<Vec<PathBuf>> {
    if !dir.exists() {
        return Ok(vec![]);
    }

    let mut files = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_file() && path.extension().map(|e| e == "csv").unwrap_or(false) {
            files.push(path);
        }
    }
    Ok(files)
}