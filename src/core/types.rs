use serde::{Deserialize, Serialize};

pub type DataId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SeriesType {
    F64,
    I64,
    Bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    F64(f64),
    I64(i64),
    Bool(bool),
}

impl Value {
    pub fn series_type(&self) -> SeriesType {
        match self {
            Self::F64(_) => SeriesType::F64,
            Self::I64(_) => SeriesType::I64,
            Self::Bool(_) => SeriesType::Bool,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeriesMeta {
    pub data_id: DataId,
    pub series_type: SeriesType,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Sample {
    pub data_id: DataId,
    pub timestamp_ms: i64,
    pub value: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeQuery {
    pub data_id: DataId,
    pub start_ts_ms: i64,
    pub end_ts_ms: i64,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RangeSample {
    pub timestamp_ms: i64,
    pub value: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BufferedSample {
    pub seq_no: u64,
    pub sample: Sample,
}

#[derive(Debug)]
pub struct SeriesState {
    pub meta: SeriesMeta,
    pub memtable: Vec<BufferedSample>,
    pub mem_max_ts: Option<i64>,
    pub flushed_max_ts: Option<i64>,
    pub next_seq_no: u64,
}

impl SeriesState {
    pub fn new(meta: SeriesMeta, flushed_max_ts: Option<i64>) -> Self {
        Self {
            meta,
            memtable: Vec::new(),
            mem_max_ts: None,
            flushed_max_ts,
            next_seq_no: 0,
        }
    }
}
