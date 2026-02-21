use std::time::{Duration, Instant};
use tracing::info;

/// A simple wall-clock timer for logging elapsed time.
pub struct Timer {
    label: String,
    start: Instant,
}

impl Timer {
    pub fn start(label: impl Into<String>) -> Self {
        let label = label.into();
        info!("⏱  Starting: {}", label);
        Self {
            label,
            start: Instant::now(),
        }
    }

    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        info!(
            "⏱  Finished: {} (took {:.2?})",
            self.label,
            self.start.elapsed()
        );
    }
}

/// Format a large integer with thousands separators.
pub fn fmt_number(n: i64) -> String {
    let s = n.abs().to_string();
    let mut result = String::new();
    for (i, ch) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(ch);
    }
    if n < 0 {
        result.push('-');
    }
    result.chars().rev().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fmt_number() {
        assert_eq!(fmt_number(1_234_567), "1,234,567");
        assert_eq!(fmt_number(0), "0");
        assert_eq!(fmt_number(-42_000), "-42,000");
        assert_eq!(fmt_number(999), "999");
    }
}