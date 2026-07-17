//! # tara-query
//!
//! DataFusion integration for Tara.
//!
//! Exposes the chunk store as a SQL-queryable table named `vessel_positions`.
//! The entry point is [`context::TaraContext`], which wraps a DataFusion
//! `SessionContext` with the table pre-registered and ready to query.
//!
//! ## Usage
//!
//! ```rust,no_run
//! use tara_query::context::TaraContext;
//! use tara_store::index::ChunkIndex;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let index = ChunkIndex::load("data/chunks/tara.index".as_ref())?;
//!     let ctx = TaraContext::new(index).await?;
//!     let batches = ctx.query("SELECT COUNT(*) FROM vessel_positions").await?;
//!     Ok(())
//! }
//! ```

pub mod context;
pub mod h3_udf;
pub mod provider;
pub mod queries;
