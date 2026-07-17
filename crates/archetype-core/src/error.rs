use arrow_schema::{ArrowError, DataType};
use thiserror::Error;

pub type Result<T> = std::result::Result<T, ArchetypeCoreError>;

#[derive(Debug, Error)]
pub enum ArchetypeCoreError {
    #[error("missing required column {0}")]
    MissingColumn(String),

    #[error("column {column} has invalid type: expected {expected:?}, got {actual:?}")]
    InvalidColumnType {
        column: &'static str,
        expected: DataType,
        actual: DataType,
    },

    #[error("record batch has {actual} rows, expected {expected}")]
    InvalidRowCount { expected: usize, actual: usize },

    #[error("table schema mismatch for {table_name}")]
    SchemaMismatch { table_name: String },

    #[error("table {0} has not been registered with this world")]
    UnknownTable(String),

    #[error("entity {0} is not registered in this world")]
    UnknownEntity(i32),

    #[error(
        "per-table step for {table_name} requires it to be the only active table; \
         active tables: {active_tables:?}; use step or step_profiled for multi-table ticks"
    )]
    InvalidTableStep {
        table_name: String,
        active_tables: Vec<String>,
    },

    #[error(
        "tick {tick} partially committed after {committed_tables} table(s): {cause}; \
         this world is not safe to retry"
    )]
    PartialTick {
        tick: i32,
        committed_tables: usize,
        cause: String,
    },

    #[error("arrow error: {0}")]
    Arrow(#[from] ArrowError),

    #[error("store error: {0}")]
    Store(String),
}
