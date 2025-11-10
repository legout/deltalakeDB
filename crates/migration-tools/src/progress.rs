//! Progress reporting for migration operations.

use indicatif::{ProgressBar, ProgressStyle};
use std::time::Instant;

/// Progress tracker for import operations.
pub struct ImportProgress {
    bar: ProgressBar,
    start_time: Instant,
    versions_processed: u64,
}

impl ImportProgress {
    /// Create a new progress tracker.
    pub fn new(total_versions: u64) -> Self {
        let bar = ProgressBar::new(total_versions);

        bar.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{bar:40.cyan/blue}] {pos}/{len} versions ({percent}%) | {msg}")
                .expect("Invalid progress template")
                .progress_chars("#>-"),
        );

        bar.set_message("Importing versions...");

        ImportProgress {
            bar,
            start_time: Instant::now(),
            versions_processed: 0,
        }
    }

    /// Increment progress by one version.
    pub fn inc(&mut self, version: i64) {
        self.bar.inc(1);
        self.versions_processed += 1;
        self.bar
            .set_message(format!("Importing version {}", version));
    }

    /// Set a custom message.
    pub fn set_message(&self, msg: impl Into<String>) {
        self.bar.set_message(msg.into());
    }

    /// Finish progress and return elapsed time.
    pub fn finish(self) -> std::time::Duration {
        self.bar.finish_with_message("Import complete!");
        self.start_time.elapsed()
    }

    /// Finish progress with custom message.
    pub fn finish_with_message(self, msg: impl Into<String>) {
        self.bar.finish_with_message(msg.into());
    }

    /// Get elapsed time so far.
    pub fn elapsed(&self) -> std::time::Duration {
        self.start_time.elapsed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_progress_creation() {
        let progress = ImportProgress::new(100);
        assert!(progress.elapsed().as_secs_f64() < 1.0);
    }
}
