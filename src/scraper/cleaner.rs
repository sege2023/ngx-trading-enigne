//! Data cleaning: raw strings → validated domain types.
use crate::models::{DailyBar, FxRate, RawCsvRow, RawEquityRow, RawFxCsvRow, RawHistoricalRow, RawTickerRow, Ticker};
use chrono::{NaiveDate, NaiveDateTime, Utc};
use tracing::warn;

// ── Parsers ───────────────────────────────────────────────────────────────────


pub fn parse_price(s: &str) -> Option<f64> {
    let s = s.trim();
    if s.is_empty() || s == "N/A" || s == "-" || s == "—" {
        return None;
    }
    let cleaned: String = s
        .chars()
        .filter(|c| c.is_ascii_digit() || *c == '.' || *c == '-')
        .collect();
    cleaned.parse().ok()
}

pub fn parse_volume_shorthand(s: &str) -> Option<i64> {
    let s = s.trim().to_uppercase().replace(',', "");
    
    if s.is_empty() || s == "N/A" || s == "-" || s == "—" {
        return None;
    }

    let (num_str, multiplier) = if s.ends_with('B') {
        (s.trim_end_matches('B'), 1_000_000_000.0)
    } else if s.ends_with('M') {
        (s.trim_end_matches('M'), 1_000_000.0)
    } else if s.ends_with('K') {
        (s.trim_end_matches('K'), 1_000.0)
    } else {
        let cleaned: String = s.chars().filter(|c| c.is_ascii_digit()).collect();
        return cleaned.parse().ok();
    };

    let num: f64 = num_str.trim().parse().ok()?;
    Some((num * multiplier) as i64)
}

/// Parse integer: "1,234,567" → 1234567
pub fn parse_volume(s: &str) -> Option<i64> {
    // Try shorthand first
    if let Some(v) = parse_volume_shorthand(s) {
        return Some(v);
    }
    
    let s = s.trim();
    if s.is_empty() || s == "N/A" || s == "-" {
        return None;
    }
    let cleaned: String = s.chars().filter(|c| c.is_ascii_digit()).collect();
    cleaned.parse().ok()
}

/// Parse percentage: "+2.09%" → 2.09 | "-0.50%" → -0.50
pub fn parse_pct(s: &str) -> Option<f64> {
    let s = s.trim().replace('%', "").replace(',', "");
    if s.is_empty() || s == "N/A" || s == "-" {
        return None;
    }
    s.parse().ok()
}

/// Parse dates from investing.com or other sources.
/// Common formats:
///   "Feb 20, 2024"   ← investing.com
///   "2024-02-20"     ← ISO
///   "20/02/2024"     ← DD/MM/YYYY
pub fn parse_date(s: &str) -> Option<NaiveDate> {
    let s = s.trim();
    
    // investing.com: "Feb 20, 2024"
    if let Ok(d) = NaiveDate::parse_from_str(s, "%b %d, %Y") {
        return Some(d);
    }
    // ISO: "2024-02-20"
    if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return Some(d);
    }
    // DD/MM/YYYY
    if let Ok(d) = NaiveDate::parse_from_str(s, "%d/%m/%Y") {
        return Some(d);
    }
    // MM/DD/YYYY
    if let Ok(d) = NaiveDate::parse_from_str(s, "%m/%d/%Y") {
        return Some(d);
    }
    // "20 Feb 2024"
    if let Ok(d) = NaiveDate::parse_from_str(s, "%d %b %Y") {
        return Some(d);
    }
    
    None
}

pub fn normalise_symbol(s: &str) -> String {
    s.trim().to_uppercase()
}

pub fn normalise_pair(s: &str) -> String {
    s.trim().to_uppercase().replace("/", "").replace(" ", "")
}

// ── Converters: investing.com CSV → DailyBar ─────────────────────────────────

/// Convert a raw CSV row (investing.com format) into a DailyBar.
/// investing.com columns: Date, Price (=close), Open, High, Low, Volume, Change%
pub fn csv_row_to_bar(
    symbol: &str,
    row: &RawCsvRow,
    now: NaiveDateTime,
) -> Option<DailyBar> {
    let date_str = row.date.as_deref()?.trim();
    let date = parse_date(date_str)?;

    let close_str = row.price.as_deref()?.trim();
    let close = parse_price(close_str)?;

    if close <= 0.0 {
        warn!("Invalid close {} for {} on {}", close, symbol, date);
        return None;
    }

    Some(DailyBar {
        symbol: normalise_symbol(symbol),
        date,
        open: row.open.as_deref().and_then(parse_price),
        high: row.high.as_deref().and_then(parse_price),
        low: row.low.as_deref().and_then(parse_price),
        close,
        change_pct: row.change_pct.as_deref().and_then(parse_pct),
        volume: row.volume.as_deref().and_then(parse_volume_shorthand),
        scraped_at: now,
    })
}

// ── FX CSV → FxRate ───────────────────────────────────────────────────────────

pub fn fx_csv_row_to_rate(
    pair: &str,
    row: &RawFxCsvRow,
    source: Option<&str>,
    now: NaiveDateTime,
) -> Option<FxRate> {
    let date_str = row.date.as_deref()?.trim();
    let date = parse_date(date_str)?;

    let close_str = row.price.as_deref()?.trim();
    let close = parse_price(close_str)?;

    if close <= 0.0 {
        warn!("Invalid FX rate {} for {} on {}", close, pair, date);
        return None;
    }

    Some(FxRate {
        pair: normalise_pair(pair),
        date,
        open: row.open.as_deref().and_then(parse_price),
        high: row.high.as_deref().and_then(parse_price),
        low: row.low.as_deref().and_then(parse_price),
        close,
        change_pct: row.change_pct.as_deref().and_then(parse_pct),
        source: source.map(|s| s.to_string()),
        scraped_at: now,
    })
}
// ── Legacy converters (scraper compatibility) ────────────────────────────────

pub fn raw_row_to_ticker(row: &RawEquityRow, now: NaiveDateTime) -> Option<Ticker> {
    let symbol = row.symbol.as_deref().map(normalise_symbol)?;
    if symbol.is_empty() {
        return None;
    }

    Some(Ticker {
        symbol,
        name: row.name.clone().unwrap_or_default().trim().to_string(),
        sector: row.sector.clone(),
        industry: row.sector.clone(),
        exchange: row.exchange.clone(),
        scraped_at: now,
    })
}

pub fn raw_historical_to_bar(
    symbol: &str,
    row: &RawHistoricalRow,
    now: NaiveDateTime,
) -> Option<DailyBar> {
    let date_str = row.date.as_deref()?.trim();
    let date = parse_date(date_str)?;

    let close_str = row.close.as_deref()?.trim();
    let close = parse_price(close_str)?;

    if close <= 0.0 {
        return None;
    }

    Some(DailyBar {
        symbol: normalise_symbol(symbol),
        date,
        open: row.open.as_deref().and_then(parse_price),
        high: row.high.as_deref().and_then(parse_price),
        low: row.low.as_deref().and_then(parse_price),
        close,
        change_pct: None,
        volume: row.volume.as_deref().and_then(parse_volume),
        scraped_at: now,
    })
}

pub fn clean_historical_rows(symbol: &str, rows: Vec<RawHistoricalRow>) -> Vec<DailyBar> {
    let now = Utc::now().naive_utc();
    rows.iter()
        .filter_map(|r| raw_historical_to_bar(symbol, r, now))
        .collect()
}

pub fn clean_ticker_rows(rows: Vec<RawEquityRow>) -> Vec<Ticker> {
    let now = Utc::now().naive_utc();
    rows.iter()
        .filter_map(|r| raw_row_to_ticker(r, now))
        .collect()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_volume_shorthand() {
        assert_eq!(parse_volume_shorthand("1.2M"), Some(1_200_000));
        assert_eq!(parse_volume_shorthand("1.23M"), Some(1_230_000));
        assert_eq!(parse_volume_shorthand("345K"), Some(345_000));
        assert_eq!(parse_volume_shorthand("2.5K"), Some(2_500));
        assert_eq!(parse_volume_shorthand("1.5B"), Some(1_500_000_000));
        assert_eq!(parse_volume_shorthand("12345"), Some(12345));
        assert_eq!(parse_volume_shorthand("1,234,567"), Some(1_234_567));
        assert_eq!(parse_volume_shorthand("N/A"), None);
        assert_eq!(parse_volume_shorthand("-"), None);
    }

    #[test]
    fn test_parse_date_investing() {
        assert_eq!(
            parse_date("Feb 20, 2024"),
            NaiveDate::from_ymd_opt(2024, 2, 20)
        );
        assert_eq!(
            parse_date("2024-02-20"),
            NaiveDate::from_ymd_opt(2024, 2, 20)
        );
        assert_eq!(
            parse_date("20/02/2024"),
            NaiveDate::from_ymd_opt(2024, 2, 20)
        );
    }

    #[test]
    fn test_parse_pct() {
        assert_eq!(parse_pct("+2.09%"), Some(2.09));
        assert_eq!(parse_pct("-0.50%"), Some(-0.50));
        assert_eq!(parse_pct("1.5"), Some(1.5));
        assert_eq!(parse_pct("N/A"), None);
    }

    #[test]
    fn test_normalise_pair() {
        assert_eq!(normalise_pair("USD/NGN"), "USDNGN");
        assert_eq!(normalise_pair("usd ngn"), "USDNGN");
        assert_eq!(normalise_pair("USDNGN"), "USDNGN");
    }
}