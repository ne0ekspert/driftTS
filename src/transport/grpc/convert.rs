use tonic::Status;

use crate::core::engine::{AppendBatchDetailed, AppendErrorDetail, EngineStats, HealthStatus};
use crate::core::types::{RangeQuery, RangeSample, Sample, SeriesType, Value};
use crate::error::TsdbError;
use crate::transport::grpc::pb;

pub fn proto_series_type_to_core(raw: i32) -> Result<SeriesType, TsdbError> {
    match pb::SeriesType::try_from(raw) {
        Ok(pb::SeriesType::F64) => Ok(SeriesType::F64),
        Ok(pb::SeriesType::I64) => Ok(SeriesType::I64),
        Ok(pb::SeriesType::Bool) => Ok(SeriesType::Bool),
        _ => Err(TsdbError::Proto(format!(
            "unknown series type value: {raw}"
        ))),
    }
}

pub fn core_series_type_to_proto(series_type: SeriesType) -> i32 {
    match series_type {
        SeriesType::F64 => pb::SeriesType::F64 as i32,
        SeriesType::I64 => pb::SeriesType::I64 as i32,
        SeriesType::Bool => pb::SeriesType::Bool as i32,
    }
}

pub fn proto_sample_to_core(sample: pb::Sample) -> Result<Sample, TsdbError> {
    let value = sample
        .value
        .ok_or_else(|| TsdbError::Proto("sample value is required".to_string()))?;

    let value = match value {
        pb::sample::Value::F64Value(value) => Value::F64(value),
        pb::sample::Value::I64Value(value) => Value::I64(value),
        pb::sample::Value::BoolValue(value) => Value::Bool(value),
    };

    Ok(Sample {
        data_id: sample.data_id,
        timestamp_ms: sample.timestamp_ms,
        value,
    })
}

pub fn core_range_sample_to_proto(sample: RangeSample) -> pb::RangeSample {
    let value = match sample.value {
        Value::F64(value) => pb::range_sample::Value::F64Value(value),
        Value::I64(value) => pb::range_sample::Value::I64Value(value),
        Value::Bool(value) => pb::range_sample::Value::BoolValue(value),
    };

    pb::RangeSample {
        timestamp_ms: sample.timestamp_ms,
        value: Some(value),
    }
}

pub fn proto_query_to_core(request: pb::RangeQueryRequest) -> Result<RangeQuery, TsdbError> {
    Ok(RangeQuery {
        data_id: request.data_id,
        start_ts_ms: request.start_ts_ms,
        end_ts_ms: request.end_ts_ms,
        limit: request.limit.map(|value| value as usize),
    })
}

pub fn append_response(detail: AppendBatchDetailed) -> pb::AppendBatchResponse {
    pb::AppendBatchResponse {
        accepted: detail.accepted,
        rejected: detail.rejected,
        errors: detail
            .errors
            .into_iter()
            .map(append_error_to_proto)
            .collect(),
    }
}

pub fn stats_response(stats: EngineStats) -> pb::StatsResponse {
    pb::StatsResponse {
        storage_bytes: stats.storage_bytes,
        series_count: stats.series_count as u64,
        segment_count: stats.segment_count as u64,
        buffered_samples: stats.buffered_samples as u64,
    }
}

pub fn health_response(status: HealthStatus) -> pb::HealthResponse {
    pb::HealthResponse {
        ok: status.ok,
        message: status.message,
    }
}

pub fn error_to_status(error: TsdbError) -> Status {
    match error {
        TsdbError::SeriesExists(_)
        | TsdbError::TypeMismatch { .. }
        | TsdbError::TimestampTooOld { .. }
        | TsdbError::InvalidQuery(_)
        | TsdbError::Proto(_)
        | TsdbError::Config(_) => Status::invalid_argument(error.to_string()),
        TsdbError::SeriesNotFound(_) => Status::not_found(error.to_string()),
        TsdbError::Io(_) | TsdbError::Manifest(_) | TsdbError::CorruptSegment(_) => {
            Status::internal(error.to_string())
        }
        TsdbError::Internal(_) => Status::internal(error.to_string()),
    }
}

fn append_error_to_proto(error: AppendErrorDetail) -> pb::AppendError {
    pb::AppendError {
        index: error.index as u32,
        reason: error.reason,
    }
}
