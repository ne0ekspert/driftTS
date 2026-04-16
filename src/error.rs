use std::io;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum TsdbError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("config error: {0}")]
    Config(String),
    #[error("manifest error: {0}")]
    Manifest(String),
    #[error("series already exists: {0}")]
    SeriesExists(u64),
    #[error("series not found: {0}")]
    SeriesNotFound(u64),
    #[error("type mismatch for series {data_id}: expected {expected:?}, got {actual:?}")]
    TypeMismatch {
        data_id: u64,
        expected: crate::core::types::SeriesType,
        actual: crate::core::types::SeriesType,
    },
    #[error(
        "timestamp {timestamp_ms} is not newer than flushed max ts {flushed_max_ts} for series {data_id}"
    )]
    TimestampTooOld {
        data_id: u64,
        timestamp_ms: i64,
        flushed_max_ts: i64,
    },
    #[error("invalid query: {0}")]
    InvalidQuery(String),
    #[error("corrupt segment: {0}")]
    CorruptSegment(String),
    #[error("protobuf conversion error: {0}")]
    Proto(String),
    #[error("internal error: {0}")]
    Internal(String),
}

impl From<serde_json::Error> for TsdbError {
    fn from(value: serde_json::Error) -> Self {
        Self::Manifest(value.to_string())
    }
}

impl From<toml::de::Error> for TsdbError {
    fn from(value: toml::de::Error) -> Self {
        Self::Config(value.to_string())
    }
}

pub type Result<T> = std::result::Result<T, TsdbError>;
