//! # grafeo-engine
//!
//! The engine behind Grafeo. You'll find everything here for creating databases,
//! running queries, and managing transactions.
//!
//! Most users should start with the main `grafeo` crate, which re-exports the
//! key types. If you're here directly, [`GrafeoDB`] is your entry point.
//!
//! ## Modules
//!
//! - [`database`] - Create and manage databases with [`GrafeoDB`]
//! - [`session`] - Lightweight handles for concurrent access
//! - [`config`] - Tune memory, threads, and durability settings
//! - [`transaction`] - MVCC transaction management (snapshot isolation)
//! - [`query`] - The full query pipeline: parsing, planning, optimization, execution
//! - [`catalog`] - Schema metadata: labels, property keys, indexes
//! - [`admin`] - Admin API types for inspection, backup, and maintenance

#![deny(unsafe_code)]

/// The version of the grafeo-engine crate (from Cargo.toml).
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

pub mod admin;
pub mod auth;
pub mod catalog;
#[cfg(feature = "cdc")]
pub mod cdc;
pub mod config;
pub mod database;
#[cfg(feature = "embed")]
pub mod embedding;
pub mod execution;
pub mod export;
pub mod memory_usage;
#[cfg(feature = "metrics")]
pub mod metrics;
#[cfg(feature = "algos")]
pub mod procedures;
pub mod query;
pub mod session;
pub mod transaction;
#[cfg(feature = "shacl")]
pub mod validation;

pub use admin::{
    AdminService, CompactionStats, DatabaseInfo, DatabaseMode, DatabaseStats, DumpFormat,
    DumpMetadata, IndexInfo, LpgSchemaInfo, RdfSchemaInfo, SchemaInfo, ValidationError,
    ValidationResult, ValidationWarning, WalStatus,
};
pub use auth::{Grant, Identity, Role, StatementKind};
pub use catalog::{Catalog, CatalogError, IndexDefinition, IndexType};
pub use config::{AccessMode, Config, ConfigError, DurabilityMode, GraphModel};
#[cfg(all(feature = "grafeo-file", feature = "lpg", feature = "compact-store"))]
pub use database::CompactBacking;
pub use database::GrafeoDB;
#[cfg(all(feature = "lpg", feature = "vector-index"))]
pub use database::IndexedVectorRead;
#[cfg(all(
    feature = "generation",
    feature = "lpg",
    feature = "compact-store",
    feature = "mmap"
))]
pub use database::generation::{
    BaseGeneration, GenerationLease, GenerationLeaseRegistry, GenerationLeaseStats,
    GenerationTransitionError, TransitionReport,
};
#[cfg(all(feature = "generation", feature = "lpg", feature = "compact-store"))]
pub use database::generation::{
    BuildPublication, ExpectedSelection, ManifestSelection, ManifestState, ManifestStateError,
    OrphanClassification, PublicationCrashPoint, PublicationPhase, PublicationPhaseError,
    PublishedGeneration, RecoveryViewError, RootRecovery, WalBoundary, read_manifest_state,
    recover_generation_root,
};
#[cfg(all(feature = "generation", feature = "lpg", feature = "compact-store"))]
pub use database::generation_build::{
    GenerationBuildRequest, PublishedGenerationDescriptor, generation_build_request,
    path_is_generation_root,
};
#[cfg(all(feature = "lpg", feature = "vector-index"))]
pub use database::{VectorIndexBacking, VectorPayloadBacking, VectorTopologyBacking};
pub use grafeo_core::graph::{GraphStore, GraphStoreMut, ProjectionSpec};
pub use memory_usage::MemoryUsage;
#[cfg(feature = "metrics")]
pub use metrics::{MetricsRegistry, MetricsSnapshot};
#[cfg(all(feature = "gql", feature = "lpg"))]
pub use query::executor::stream::{OwnedResultStream, OwnedRowIterator, ResultStream, RowIterator};
pub use session::Session;
#[cfg(feature = "lpg")]
pub use transaction::{CommitInfo, PreparedCommit};
