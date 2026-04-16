use tempfile::TempDir;
use tonic::Request;

use drift_ts::core::engine::{Engine, EngineConfig};
use drift_ts::transport::grpc::pb::{
    self, admin_service_server::AdminService, ingest_service_server::IngestService,
    query_service_server::QueryService,
};
use drift_ts::transport::grpc::server::TsdbGrpcServer;

#[tokio::test]
async fn grpc_service_round_trip_register_append_query_and_stats() {
    let tempdir = TempDir::new().unwrap();
    let engine = Engine::open(EngineConfig {
        data_dir: tempdir.path().to_path_buf(),
        flush_threshold_count: 2,
        max_storage_bytes: 1_000_000,
    })
    .unwrap();

    let server = TsdbGrpcServer::new(engine);

    IngestService::register_series(
        &server,
        Request::new(pb::RegisterSeriesRequest {
            data_id: 42,
            series_type: pb::SeriesType::I64 as i32,
        }),
    )
    .await
    .unwrap();

    let append = IngestService::append_batch(
        &server,
        Request::new(pb::AppendBatchRequest {
            samples: vec![
                pb::Sample {
                    data_id: 42,
                    timestamp_ms: 100,
                    value: Some(pb::sample::Value::I64Value(1)),
                },
                pb::Sample {
                    data_id: 42,
                    timestamp_ms: 200,
                    value: Some(pb::sample::Value::I64Value(2)),
                },
            ],
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert_eq!(append.accepted, 2);
    assert_eq!(append.rejected, 0);

    let response = QueryService::range_query(
        &server,
        Request::new(pb::RangeQueryRequest {
            data_id: 42,
            start_ts_ms: 0,
            end_ts_ms: 1_000,
            limit: None,
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert_eq!(response.samples.len(), 2);
    assert_eq!(response.samples[0].timestamp_ms, 100);
    assert_eq!(
        response.samples[1].value,
        Some(pb::range_sample::Value::I64Value(2))
    );

    let stats = AdminService::stats(&server, Request::new(pb::StatsRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(stats.series_count, 1);
    assert_eq!(stats.segment_count, 1);

    let health = AdminService::health(&server, Request::new(pb::HealthRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert!(health.ok);
}
