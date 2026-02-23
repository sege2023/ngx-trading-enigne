
use crate::models::{DailyBar, FxRate, RawCsvRow, RawFxCsvRow, RawTickerRow, Ticker};
use chrono::{NaiveDate, NaiveDateTime, Utc};
use tracing::warn;

// ── Parsers ───────────────────────────────────────────────────────────────────

/// Parse price: strip everything except digits, dot, minus.
/// "NGN 1,234.56" → 1234.56 | "610.00" → 610.0
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

/// Parse volume with K/M/B suffixes.
/// "1.2M" → 1,200,000 | "345K" → 345,000 | "12345" → 12345
pub fn parse_volume_shorthand(s: &str) -> Option<i64> {
    let s = s.trim().to_uppercase().replace(',', "");
    
    if s.is_empty() || s == "N/A" || s == "-" || s == "—" {
        return None;
    }

    // Check for suffix
    let (num_str, multiplier) = if s.ends_with('B') {
        (s.trim_end_matches('B'), 1_000_000_000.0)
    } else if s.ends_with('M') {
        (s.trim_end_matches('M'), 1_000_000.0)
    } else if s.ends_with('K') {
        (s.trim_end_matches('K'), 1_000.0)
    } else {
        // No suffix — just a plain integer
        let cleaned: String = s.chars().filter(|c| c.is_ascii_digit()).collect();
        return cleaned.parse().ok();
    };

    // Parse the numeric part (can be decimal like "1.2")
    let num: f64 = num_str.trim().parse().ok()?;
    Some((num * multiplier) as i64)
}

pub fn parse_volume(s: &str) -> Option<i64> {
    parse_volume_shorthand(s)
}

pub fn parse_pct(s: &str) -> Option<f64> {
    let s = s.trim().replace('%', "").replace(',', "");
    if s.is_empty() || s == "N/A" || s == "-" {
        return None;
    }
    s.parse().ok()
}

/// Parse dates: "Feb 20, 2024" (investing.com) or ISO
pub fn parse_date(s: &str) -> Option<NaiveDate> {
    let s = s.trim();
    
    if let Ok(d) = NaiveDate::parse_from_str(s, "%b %d, %Y") {
        return Some(d);
    }
    if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return Some(d);
    }
    if let Ok(d) = NaiveDate::parse_from_str(s, "%d/%m/%Y") {
        return Some(d);
    }
    if let Ok(d) = NaiveDate::parse_from_str(s, "%m/%d/%Y") {
        return Some(d);
    }
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

// ── Equity CSV → DailyBar ─────────────────────────────────────────────────────

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

// ── Ticker metadata CSV → Ticker ──────────────────────────────────────────────

pub fn ticker_row_to_ticker(row: &RawTickerRow, now: NaiveDateTime) -> Option<Ticker> {
    let symbol = row.symbol.as_deref()?.trim();
    if symbol.is_empty() {
        return None;
    }

    Some(Ticker {
        symbol: normalise_symbol(symbol),
        name: row.name.clone().unwrap_or_default().trim().to_string(),
        sector: row.sector.clone().and_then(|s| {
            let s = s.trim();
            if s.is_empty() { None } else { Some(s.to_string()) }
        }),
        industry: row.industry.clone().and_then(|s| {
            let s = s.trim();
            if s.is_empty() { None } else { Some(s.to_string()) }
        }),
        exchange: row.exchange.clone().and_then(|s| {
            let s = s.trim();
            if s.is_empty() { None } else { Some(s.to_string()) }
        }),
        scraped_at: now,
    })
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_volume_shorthand() {
        assert_eq!(parse_volume_shorthand("1.2M"), Some(1_200_000));
        assert_eq!(parse_volume_shorthand("345K"), Some(345_000));
        assert_eq!(parse_volume_shorthand("1.5B"), Some(1_500_000_000));
        assert_eq!(parse_volume_shorthand("12345"), Some(12345));
    }

    #[test]
    fn test_normalise_pair() {
        assert_eq!(normalise_pair("USD/NGN"), "USDNGN");
        assert_eq!(normalise_pair("usd ngn"), "USDNGN");
        assert_eq!(normalise_pair("USDNGN"), "USDNGN");
    }
}