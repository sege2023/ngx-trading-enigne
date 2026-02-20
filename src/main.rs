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
use crate::loader::{discover_csv_files, load_csv};
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
    /// Bulk-load CSV files from data/ directory (investing.com format)
    LoadCsv {
        /// Path to directory containing CSV files (default: data/)
        #[arg(short, long, default_value = "data")]
        dir: PathBuf,
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

    match cli.command {
        Command::LoadCsv { dir } => {
            let _t = utils::Timer::start("CSV bulk load");
            let repo = Repository::open(&config.storage.db_path)?;
            repo.run_migrations()?;

            let files = discover_csv_files(&dir)?;
            info!("Found {} CSV files in {:?}", files.len(), dir);

            let mut total_bars = 0usize;
            let mut errors = 0usize;

            for path in &files {
                match load_csv(path) {
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

        Command::Update => {
            let _t = utils::Timer::start("Daily update");
            let stats = Pipeline::new(config).run().await?;
            info!(
                "Done: {} tickers, {} bars, {} errors",
                stats.tickers_processed, stats.bars_inserted, stats.errors
            );
        }

        Command::Stats => {
            let repo = Repository::open(&config.storage.db_path)?;
            let bars = repo.bar_count()?;
            let tickers = repo.ticker_count()?;
            let (min, max) = repo.date_range().unwrap_or((None, None));
            println!("─────────────────────────────────");
            println!("  NGX ETL — Database Stats");
            println!("─────────────────────────────────");
            println!("  Tickers  : {}", utils::fmt_number(tickers));
            println!("  EOD bars : {}", utils::fmt_number(bars));
            println!("  From     : {}", min.map(|d| d.to_string()).unwrap_or("—".into()));
            println!("  To       : {}", max.map(|d| d.to_string()).unwrap_or("—".into()));
            println!("─────────────────────────────────");
        }

        Command::Symbols => {
            let repo = Repository::open(&config.storage.db_path)?;
            let syms = repo.list_symbols()?;
            if syms.is_empty() {
                println!("No symbols — run `ngx-etl load-csv` first.");
            } else {
                println!("{} symbols:", syms.len());
                for s in &syms {
                    println!("  {}", s);
                }
            }
        }

        Command::Migrate => {
            Repository::open(&config.storage.db_path)?.run_migrations()?;
            println!("Migrations applied.");
        }
    }

    Ok(())
}