//! `TaraContext` — a configured DataFusion `SessionContext` with the
//! `vessel_positions` table pre-registered.
//!
//! This is the single entry point for running SQL against Tara's data.
//! Both the CLI query tool and the HTTP server use this.

use crate::provider::VesselTableProvider;
use anyhow::Result;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use std::sync::Arc;
use tara_store::index::ChunkIndex;

/// A ready-to-use DataFusion context with `vessel_positions` registered.
pub struct TaraContext {
    pub session: SessionContext,
}

impl TaraContext {
    /// Build a `TaraContext` from a loaded `ChunkIndex`.
    /// After this returns, `SELECT * FROM vessel_positions ...` is valid SQL.
    pub async fn new(index: ChunkIndex) -> Result<Self> {
        let session = SessionContext::new();
        let provider = VesselTableProvider::new(Arc::new(index));
        session
            .register_table("vessel_positions", Arc::new(provider))
            .map_err(|e| anyhow::anyhow!("Failed to register table: {}", e))?;
        Ok(Self { session })
    }

    /// Execute a SQL query and return all result batches.
    #[tracing::instrument(skip(self, sql))]
    pub async fn query(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let df = self
            .session
            .sql(sql)
            .await
            .map_err(|e| anyhow::anyhow!("SQL error: {}", e))?;
        let batches = df
            .collect()
            .await
            .map_err(|e| anyhow::anyhow!("Execution error: {}", e))?;
        Ok(batches)
    }
}
