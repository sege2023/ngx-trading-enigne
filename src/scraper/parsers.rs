// use crate::models::{RawEquityRow, RawHistoricalRow};
// use anyhow::Result;
// use scraper::{Html, Selector};
// use tracing::warn;

// // ── Listing page ──────────────────────────────────────────────────────────────

// pub fn parse_listing_page(html: &str) -> Result<(Vec<RawEquityRow>, Vec<String>)> {
//     let doc = Html::parse_document(html);

//     let row_sel = Selector::parse("table tbody tr")
//         .map_err(|e| anyhow::anyhow!("row selector: {:?}", e))?;
//     let td_sel = Selector::parse("td")
//         .map_err(|e| anyhow::anyhow!("td selector: {:?}", e))?;
//     let a_sel = Selector::parse("a")
//         .map_err(|e| anyhow::anyhow!("a selector: {:?}", e))?;

//     let mut rows = Vec::new();
//     let mut hrefs = Vec::new();

//     for tr in doc.select(&row_sel) {
//         let cells: Vec<String> = tr
//             .select(&td_sel)
//             .map(|td| td.text().collect::<String>().trim().to_string())
//             .collect();

//         if cells.len() < 2 {
//             continue;
//         }

//         let href = tr
//             .select(&td_sel)
//             .next()
//             .and_then(|td| td.select(&a_sel).next())
//             .and_then(|a| a.value().attr("href"))
//             .map(|h| h.to_string());

//         let symbol = cells.first().map(|s| s.trim().to_uppercase());

//         if let Some(href) = href {
//             hrefs.push(href);
//         }

//         rows.push(RawEquityRow {
//             symbol,
//             name: cells.get(1).cloned(),
//             price: cells.get(2).cloned(),
//             change: cells.get(3).cloned(),
//             change_pct: cells.get(4).cloned(),
//             volume: cells.get(5).cloned(),
//             deals: cells.get(6).cloned(),
//             ..Default::default()
//         });
//     }

//     Ok((rows, hrefs))
// }

// pub fn has_next_page(html: &str) -> bool {
//     html.contains("?page=") && html.contains("Next")
//         || html.contains("next")
//         || html.contains("›")
// }

// // ── Per-ticker page ───────────────────────────────────────────────────────────

// pub fn parse_ticker_page(html: &str, symbol: &str) -> Result<Vec<RawHistoricalRow>> {
//     let doc = Html::parse_document(html);

//     // Find the price history table — kwayisi uses id="t" consistently
//     let row_sel = find_history_rows(&doc);

//     let Some(rows_html) = row_sel else {
//         warn!("No price history table found for {}", symbol);
//         return Ok(vec![]);
//     };

//     Ok(rows_html)
// }

// /// Find and extract raw history rows from the price table.
// fn find_history_rows(doc: &Html) -> Option<Vec<RawHistoricalRow>> {
//     // Try id="t" first (kwayisi convention)
//     let table_candidates = ["table#t", "table.prices", "table"];

//     for selector_str in &table_candidates {
//         let Ok(sel) = Selector::parse(selector_str) else { continue };
//         let Some(table) = doc.select(&sel).next() else { continue };

//         // Check if this table has a date-like header
//         let Ok(th_sel) = Selector::parse("thead th") else { continue };
//         let headers: Vec<String> = table
//             .select(&th_sel)
//             .map(|th| th.text().collect::<String>().to_lowercase())
//             .collect();

//         let has_date = headers.iter().any(|h| h.contains("date"));
//         let has_price = headers.iter().any(|h| {
//             h.contains("price") || h.contains("close") || h.contains("last")
//         });

//         if !has_date && !has_price && *selector_str == "table" {
//             // Generic table without recognisable headers — skip
//             continue;
//         }

//         // Determine column positions from headers
//         let date_idx = headers.iter().position(|h| h.contains("date")).unwrap_or(0);
//         let close_idx = headers
//             .iter()
//             .position(|h| h.contains("close") || h.contains("price") || h.contains("last"))
//             .unwrap_or(1);
//         let change_idx = headers.iter().position(|h| h == "change" || h.contains("chg"));
//         let vol_idx = headers.iter().position(|h| h.contains("volume") || h.contains("vol"));
//         let deals_idx = headers.iter().position(|h| h.contains("deal"));

//         let Ok(tr_sel) = Selector::parse("tbody tr") else { continue };
//         let Ok(td_sel) = Selector::parse("td") else { continue };

//         let mut rows = Vec::new();
//         for tr in table.select(&tr_sel) {
//             let cells: Vec<String> = tr
//                 .select(&td_sel)
//                 .map(|td| td.text().collect::<String>().trim().to_string())
//                 .collect();

//             if cells.is_empty() || cells.iter().all(|c| c.is_empty()) {
//                 continue;
//             }

//             rows.push(RawHistoricalRow {
//                 date: cells.get(date_idx).cloned(),
//                 // kwayisi NGX ticker pages have: Date | Close | Change | Change% | Volume | Deals
//                 // No open/high/low on free pages
//                 open: None,
//                 high: None,
//                 low: None,
//                 close: cells.get(close_idx).cloned(),
//                 change: change_idx.and_then(|i| cells.get(i)).cloned(),
//                 volume: vol_idx.and_then(|i| cells.get(i)).cloned(),
//             });
//         }

//         if !rows.is_empty() {
//             return Some(rows);
//         }
//     }

//     // Last resort: if there are no headers, just try columns positionally
//     // kwayisi fallback layout: Date | Close | Change | Change% | Volume | Deals
//     let Ok(sel) = Selector::parse("table tbody tr") else { return None };
//     let Ok(td_sel) = Selector::parse("td") else { return None };
//     let mut rows = Vec::new();

//     for tr in doc.select(&sel) {
//         let cells: Vec<String> = tr
//             .select(&td_sel)
//             .map(|td| td.text().collect::<String>().trim().to_string())
//             .collect();

//         if cells.len() < 2 { continue; }

//         // Heuristic: first cell looks like a date if it contains a digit and a separator
//         let first = cells[0].as_str();
//         let looks_like_date = first.contains('-') || first.contains('/') || first.len() >= 8;
//         if !looks_like_date { continue; }

//         rows.push(RawHistoricalRow {
//             date: cells.first().cloned(),
//             open: cells.get(1).cloned(), 
//             high: cells.get(2).cloned(),
//             low: cells.get(3).cloned(),
//             close: cells.get(4).cloned(),
//             change: cells.get(5).cloned(),
//             volume: cells.get(6).cloned(),
//             ..Default::default()
//         });
//     }

//     if rows.is_empty() { None } else { Some(rows) }
// }

// // ── Ticker meta (from the detail page header) ─────────────────────────────────

// #[derive(Debug, Default)]
// pub struct TickerMeta {
//     pub name: Option<String>,
//     pub sector: Option<String>,
//     pub isin: Option<String>,
//     pub board: Option<String>,
// }

// pub fn parse_ticker_meta(html: &str) -> TickerMeta {
//     let doc = Html::parse_document(html);
//     let mut meta = TickerMeta::default();

//     for sel_str in &["h1", "h2", ".company-name", "title"] {
//         if let Ok(sel) = Selector::parse(sel_str) {
//             if let Some(el) = doc.select(&sel).next() {
//                 let text = el.text().collect::<String>().trim().to_string();
//                 if !text.is_empty() && !text.to_lowercase().contains("kwayisi") {
//                     meta.name = Some(text);
//                     break;
//                 }
//             }
//         }
//     }

//     let Ok(dt_sel) = Selector::parse("dt") else { return meta };
//     let Ok(dd_sel) = Selector::parse("dd") else { return meta };

//     let dts: Vec<String> = doc
//         .select(&dt_sel)
//         .map(|el| el.text().collect::<String>().to_lowercase())
//         .collect();
//     let dds: Vec<String> = doc
//         .select(&dd_sel)
//         .map(|el| el.text().collect::<String>().trim().to_string())
//         .collect();

//     for (dt, dd) in dts.iter().zip(dds.iter()) {
//         if dt.contains("isin") {
//             meta.isin = Some(dd.clone());
//         } else if dt.contains("sector") || dt.contains("industry") {
//             meta.sector = Some(dd.clone());
//         } else if dt.contains("board") || dt.contains("segment") {
//             meta.board = Some(dd.clone());
//         }
//     }

//     meta
// }