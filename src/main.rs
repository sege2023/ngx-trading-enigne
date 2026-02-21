mod config;
mod loader;
mod models;
mod pipeline;
mod scraper;
mod storage;
mod utils;

use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing::info;
use tracing_subscriber::{fmt, EnvFilter};

use crate::config::AppConfig;
use crate::loader::{discover_csv_files, load_equity_csv, load_fx_csv, load_tickers_csv};
use crate::pipeline::Pipeline;
use crate::storage::Repository;

#[derive(Parser)]
#[command(name = "ngx-etl", about = "NGX market data ETL", version)]
struct Cli {
    #[command(subcommand)]
    command: Command,

    #[arg(short, long, action = clap::ArgAction::Count, global = true)]
    verbose: u8,
}

#[derive(Subcommand)]
enum Command {
    LoadTickers {
        #[arg(default_value = "data/tickers.csv")]
        path: PathBuf,
    },

    LoadEquities {
        #[arg(short, long, default_value = "data")]
        dir: PathBuf,
    },

    LoadFx {
        #[arg(short, long, default_value = "data")]
        dir: PathBuf,

        /// Data source attribution (e.g. "investing.com")
        #[arg(long, default_value = "investing.com")]
        source: String,
    },

    /// Scrape latest bars for all tickers (daily update mode)
    Update,

    /// Show database statistics
    Stats,

    /// List all stored ticker symbols
    Symbols,

    /// Apply schema migrations without loading data
    Migrate,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let filter = match cli.verbose {
        0 => "ngx_etl=info,warn",
        1 => "ngx_etl=debug,info",
        _ => "trace",
    };

    tracing_subscriber::registry()
        .with(fmt::layer().compact().with_target(false))
        .with(EnvFilter::new(filter))
        .init();

    let config = AppConfig::load()?;
    let repo = Repository::open(&config.storage.db_path)?;

    match cli.command {
        Command::LoadTickers { path } => {
            let _t = utils::Timer::start("Load tickers");
            repo.run_migrations()?;

            let tickers = load_tickers_csv(&path)?;
            repo.upsert_tickers(&tickers)?;

            info!("Loaded {} tickers", tickers.len());
        }

        Command::LoadEquities { dir } => {
            let _t = utils::Timer::start("Load equities");
            repo.run_migrations()?;

            let files = discover_csv_files(&dir)?;
            info!("Found {} CSV files in {:?}", files.len(), dir);

            let mut total_bars = 0usize;
            let mut errors = 0usize;

            for path in &files {
                // Skip tickers.csv (metadata file)
                if path.file_name().map(|f| f == "tickers.csv").unwrap_or(false) {
                    continue;
                }
                // Skip FX files (e.g. USDNGN_*.csv)
                if path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .map(|s| s.to_uppercase().contains("USD") || s.to_uppercase().contains("EUR"))
                    .unwrap_or(false)
                {
                    continue;
                }

                match load_equity_csv(path) {
                    Ok((_symbol, bars)) => {
                        repo.upsert_daily_bars(&bars)?;
                        total_bars += bars.len();
                    }
                    Err(e) => {
                        info!("Error loading {:?}: {:#}", path, e);
                        errors += 1;
                    }
                }
            }

            info!("Done: {} bars inserted, {} errors", total_bars, errors);
        }

        Command::LoadFx { dir, source } => {
            let _t = utils::Timer::start("Load FX rates");
            repo.run_migrations()?;

            let files = discover_csv_files(&dir)?;
            info!("Found {} CSV files in {:?}", files.len(), dir);

            let mut total_rates = 0usize;
            let mut errors = 0usize;

            for path in &files {
                // Only process files that look like FX pairs
                let is_fx = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .map(|s| {
                        let s = s.to_uppercase();
                        s.contains("USD") || s.contains("EUR") || s.contains("GBP")
                    })
                    .unwrap_or(false);

                if !is_fx {
                    continue;
                }

                match load_fx_csv(path, Some(&source)) {
                    Ok((_pair, rates)) => {
                        repo.upsert_fx_rates(&rates)?;
                        total_rates += rates.len();
                    }
                    Err(e) => {
                        info!("Error loading {:?}: {:#}", path, e);
                        errors += 1;
                    }
                }
            }

            info!("Done: {} rates inserted, {} errors", total_rates, errors);
        }

        Command::Update => {
            let _t = utils::Timer::start("Daily update");
            let stats = Pipeline::new(config).run().await?;
            info!(
                "Done: {} tickers, {} bars, {} errors",
                stats.tickers_processed, stats.bars_inserted, stats.errors
            );
        }

        Command::Stats => {
            let bars = repo.bar_count()?;
            let tickers = repo.ticker_count()?;
            let fx = repo.fx_count()?;
            let (min_bar, max_bar) = repo.date_range().unwrap_or((None, None));
            let (min_fx, max_fx) = repo.fx_date_range().unwrap_or((None, None));

            println!("─────────────────────────────────");
            println!("  NGX ETL — Database Stats");
            println!("─────────────────────────────────");
            println!("  Tickers     : {}", utils::fmt_number(tickers));
            println!("  Equity bars : {}", utils::fmt_number(bars));
            println!("    From      : {}", min_bar.map(|d| d.to_string()).unwrap_or("—".into()));
            println!("    To        : {}", max_bar.map(|d| d.to_string()).unwrap_or("—".into()));
            println!("  FX rates    : {}", utils::fmt_number(fx));
            println!("    From      : {}", min_fx.map(|d| d.to_string()).unwrap_or("—".into()));
            println!("    To        : {}", max_fx.map(|d| d.to_string()).unwrap_or("—".into()));
            println!("─────────────────────────────────");
        }

        Command::Symbols => {
            let syms = repo.list_symbols()?;
            if syms.is_empty() {
                println!("No symbols — run `ngx-etl load-tickers` first.");
            } else {
                println!("{} symbols:", syms.len());
                for s in &syms {
                    println!("  {}", s);
                }
            }
        }

        Command::Migrate => {
            repo.run_migrations()?;
            println!("Migrations applied.");
        }
    }

    Ok(())
}