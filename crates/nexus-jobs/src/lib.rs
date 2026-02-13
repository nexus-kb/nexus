mod engine;
pub mod mail;
mod pipeline;
pub mod payloads;
mod scanner;
mod threading;

pub use engine::{ExecutionContext, Phase0Worker, WorkerConfig};

#[derive(Debug)]
pub enum JobExecutionOutcome {
    Success {
        result_json: serde_json::Value,
        metrics: nexus_db::JobStoreMetrics,
    },
    Retryable {
        reason: String,
        kind: String,
        backoff_ms: u64,
        metrics: nexus_db::JobStoreMetrics,
    },
    Terminal {
        reason: String,
        kind: String,
        metrics: nexus_db::JobStoreMetrics,
    },
    Cancelled {
        reason: String,
        metrics: nexus_db::JobStoreMetrics,
    },
}
