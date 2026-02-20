//! CSV loader for bulk-importing investing.com historical data.

use crate::models::{RawCsvRow, DailyBar};
use crate::scraper::cleaner::csv_row_to_bar;
use anyhow::{Context, Result};
use chrono::Utc;
use std::path::{Path, PathBuf};
use tracing::{debug, info, warn};

/// Extract symbol from CSV filename.
pub fn extract_symbol_from_filename(path: &Path) -> Option<String> {
    let stem = path.file_stem()?.to_str()?;
    let symbol = stem
        .split(|c| c == '_' || c == ' ' || c == '.')
        .next()?
        .trim()
        .to_uppercase();

    if symbol.is_empty() { None } else { Some(symbol) }
}

/// Parse an investing.com CSV: Date, Price, Open, High, Low, Volume, Change%
pub fn load_csv(path: &Path) -> Result<(String, Vec<DailyBar>)> {
    let symbol = extract_symbol_from_filename(path)
        .with_context(|| format!("No symbol in filename {:?}", path))?;

    debug!("Loading {} from {:?}", symbol, path);

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