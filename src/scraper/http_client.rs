// use crate::config::ScraperConfig;
// use anyhow::{Context, Result};
// use rand::thread_rng;
// use rand::Rng;
// use std::time::Duration;
// use tokio::time::sleep;
// use tracing::{debug, warn};


// pub struct HttpClient {
//     inner: reqwest::Client,
//     config: ScraperConfig,
// }

// impl HttpClient {
//     pub fn new(config: &ScraperConfig) -> Result<Self> {
//         let inner = reqwest::Client::builder()
//             .user_agent(&config.user_agent)
//             .timeout(Duration::from_secs(config.timeout_secs))
//             .gzip(true)
//             // Accept cookies so session-based pages work
//             .cookie_store(true)
//             .build()
//             .context("Failed to build HTTP client")?;

//         Ok(Self {
//             inner,
//             config: config.clone(),
//         })
//     }

//     /// Fetch a URL as text with rate-limiting and retry.
//     pub async fn get_text(&self, url: &str) -> Result<String> {
//         self.polite_delay().await;

//         let mut last_err = anyhow::anyhow!("No attempts made");

//         for attempt in 1..=(self.config.max_retries + 1) {
//             debug!("GET {} (attempt {})", url, attempt);

//             match self.inner.get(url).send().await {
//                 Ok(resp) => {
//                     let status = resp.status();
//                     if status.is_success() {
//                         let text = resp
//                             .text()
//                             .await
//                             .context("Failed to read response body")?;
//                         return Ok(text);
//                     } else if status.as_u16() == 429 || status.as_u16() == 503 {
//                         // Rate limited — back off harder
//                         let backoff = Duration::from_millis(
//                             self.config.request_delay_ms * (2u64.pow(attempt)),
//                         );
//                         warn!(
//                             "Rate limited ({}) on attempt {}, sleeping {:?}",
//                             status, attempt, backoff
//                         );
//                         sleep(backoff).await;
//                         last_err = anyhow::anyhow!("HTTP {}", status);
//                     } else {
//                         last_err = anyhow::anyhow!("HTTP error {}", status);
//                         break; // Don't retry 4xx other than 429
//                     }
//                 }
//                 Err(e) => {
//                     last_err = anyhow::anyhow!("Request error: {}", e);
//                     let backoff =
//                         Duration::from_millis(self.config.request_delay_ms * (attempt as u64));
//                     warn!("Request failed on attempt {}: {}", attempt, e);
//                     sleep(backoff).await;
//                 }
//             }
//         }

//         Err(last_err).with_context(|| format!("All retries exhausted for {}", url))
//     }

//     /// Sleep for the configured delay + random jitter.
//     async fn polite_delay(&self) {
//         let jitter = rand::thread_rng().gen_range(0..=self.config.jitter_ms);
//         let total = Duration::from_millis(self.config.request_delay_ms + jitter);
//         sleep(total).await;
//     }
// }