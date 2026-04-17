#[cfg(unix)]
use hyper_util::rt::TokioIo;
use tempfile::TempDir;
use tonic::Request;
#[cfg(unix)]
use tonic::transport::Endpoint;
#[cfg(unix)]
use tower::service_fn;

#[cfg(unix)]
use drift_ts::config::ListenEndpoint;
use drift_ts::core::engine::{Engine, EngineConfig};
use drift_ts::core::segment::SegmentCompressionCodec;
#[cfg(unix)]
use drift_ts::error::TsdbError;
use drift_ts::transport::grpc::pb::{
    self, admin_service_server::AdminService, ingest_service_server::IngestService,
    query_service_server::QueryService,
};
use drift_ts::transport::grpc::server::TsdbGrpcServer;
#[cfg(unix)]
use drift_ts::transport::grpc::server::serve;

#[tokio::test]
async fn grpc_service_round_trip_register_append_query_and_stats() {
    let tempdir = TempDir::new().unwrap();
    let engine = Engine::open(EngineConfig {
        data_dir: tempdir.path().to_path_buf(),
        flush_threshold_count: 2,
        max_storage_bytes: 1_000_000,
        segment_compression: SegmentCompressionCodec::Zstd,
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

#[cfg(unix)]
#[tokio::test]
async fn grpc_server_accepts_unix_domain_socket_connections() {
    let tempdir = TempDir::new().unwrap();
    let socket_path = tempdir.path().join("drift-ts.sock");
    let engine = Engine::open(EngineConfig {
        data_dir: tempdir.path().join("data"),
        flush_threshold_count: 2,
        max_storage_bytes: 1_000_000,
        segment_compression: SegmentCompressionCodec::Zstd,
    })
    .unwrap();

    let server = TsdbGrpcServer::new(engine);
    let endpoint = ListenEndpoint::Unix(socket_path.clone());
    let mut server_task = Some(tokio::spawn(async move { serve(&endpoint, server).await }));

    for _ in 0..100 {
        if socket_path.exists() {
            break;
        }
        if server_task.as_ref().unwrap().is_finished() {
            let result = server_task.take().unwrap().await.unwrap();
            if matches!(&result, Err(TsdbError::Io(err)) if err.kind() == std::io::ErrorKind::PermissionDenied)
            {
                return;
            }
            panic!("uds server exited before creating the socket: {result:?}");
        }
        tokio::task::yield_now().await;
    }

    assert!(socket_path.exists(), "unix socket was not created");

    let channel = Endpoint::try_from("http://[::]:50051")
        .unwrap()
        .connect_with_connector(service_fn({
            let socket_path = socket_path.clone();
            move |_| {
                let socket_path = socket_path.clone();
                async move {
                    let stream = tokio::net::UnixStream::connect(socket_path).await?;
                    Ok::<_, std::io::Error>(TokioIo::new(stream))
                }
            }
        }))
        .await
        .unwrap();

    let mut ingest = pb::ingest_service_client::IngestServiceClient::new(channel.clone());
    let mut query = pb::query_service_client::QueryServiceClient::new(channel.clone());
    let mut admin = pb::admin_service_client::AdminServiceClient::new(channel);

    ingest
        .register_series(pb::RegisterSeriesRequest {
            data_id: 7,
            series_type: pb::SeriesType::I64 as i32,
        })
        .await
        .unwrap();

    let append = ingest
        .append_batch(pb::AppendBatchRequest {
            samples: vec![pb::Sample {
                data_id: 7,
                timestamp_ms: 123,
                value: Some(pb::sample::Value::I64Value(55)),
            }],
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(append.accepted, 1);
    assert_eq!(append.rejected, 0);

    let response = query
        .range_query(pb::RangeQueryRequest {
            data_id: 7,
            start_ts_ms: 0,
            end_ts_ms: 1_000,
            limit: None,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.samples.len(), 1);
    assert_eq!(response.samples[0].timestamp_ms, 123);

    let stats = admin.stats(pb::StatsRequest {}).await.unwrap().into_inner();
    assert_eq!(stats.series_count, 1);

    let server_task = server_task.take().unwrap();
    server_task.abort();
    let _ = server_task.await;
    assert!(
        !socket_path.exists(),
        "unix socket should be cleaned up when the server stops"
    );
}
