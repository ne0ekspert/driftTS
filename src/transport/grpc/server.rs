use std::fs;
#[cfg(unix)]
use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};

#[cfg(unix)]
use tokio_stream::wrappers::TcpListenerStream;
#[cfg(not(unix))]
use tokio_stream::wrappers::TcpListenerStream;
#[cfg(unix)]
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use crate::config::ListenEndpoint;
use crate::core::engine::Engine;
use crate::error::{Result as TsdbResult, TsdbError};
use crate::transport::grpc::convert::{
    append_response, core_range_sample_to_proto, error_to_status, health_response,
    proto_query_to_core, proto_sample_to_core, proto_series_type_to_core, stats_response,
};
use crate::transport::grpc::pb;

#[derive(Clone)]
pub struct TsdbGrpcServer {
    engine: Engine,
}

impl TsdbGrpcServer {
    pub fn new(engine: Engine) -> Self {
        Self { engine }
    }
}

pub async fn serve(endpoint: &ListenEndpoint, grpc: TsdbGrpcServer) -> TsdbResult<()> {
    match endpoint {
        ListenEndpoint::Tcp { host, port } => serve_tcp(host, *port, grpc).await?,
        ListenEndpoint::Unix(path) => serve_unix(path, grpc).await?,
    }

    Ok(())
}

fn grpc_router(grpc: TsdbGrpcServer) -> tonic::transport::server::Router {
    Server::builder()
        .add_service(pb::ingest_service_server::IngestServiceServer::new(
            grpc.clone(),
        ))
        .add_service(pb::query_service_server::QueryServiceServer::new(
            grpc.clone(),
        ))
        .add_service(pb::admin_service_server::AdminServiceServer::new(grpc))
}

async fn serve_tcp(host: &str, port: u16, grpc: TsdbGrpcServer) -> TsdbResult<()> {
    let listener = tokio::net::TcpListener::bind((host, port)).await?;

    grpc_router(grpc)
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await
        .map_err(|err| TsdbError::Internal(err.to_string()))?;

    Ok(())
}

async fn serve_unix(path: &Path, grpc: TsdbGrpcServer) -> TsdbResult<()> {
    #[cfg(not(unix))]
    {
        let _ = grpc;
        return Err(TsdbError::Config(format!(
            "unix sockets are not supported on this platform: {}",
            path.display()
        )));
    }

    #[cfg(unix)]
    {
        cleanup_stale_unix_socket(path)?;
        let listener = tokio::net::UnixListener::bind(path)?;
        let _cleanup = UnixSocketCleanup {
            path: path.to_path_buf(),
        };

        grpc_router(grpc)
            .serve_with_incoming(UnixListenerStream::new(listener))
            .await
            .map_err(|err| TsdbError::Internal(err.to_string()))?;

        Ok(())
    }
}

fn cleanup_stale_unix_socket(path: &Path) -> TsdbResult<()> {
    #[cfg(not(unix))]
    {
        return Err(TsdbError::Config(format!(
            "unix sockets are not supported on this platform: {}",
            path.display()
        )));
    }

    #[cfg(unix)]
    {
        if !path.exists() {
            return Ok(());
        }

        let metadata = fs::symlink_metadata(path)?;
        if metadata.file_type().is_socket() {
            fs::remove_file(path)?;
            return Ok(());
        }

        Err(TsdbError::Config(format!(
            "unix socket path already exists and is not a socket: {}",
            path.display()
        )))
    }
}

struct UnixSocketCleanup {
    path: PathBuf,
}

impl Drop for UnixSocketCleanup {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

#[tonic::async_trait]
impl pb::ingest_service_server::IngestService for TsdbGrpcServer {
    async fn register_series(
        &self,
        request: Request<pb::RegisterSeriesRequest>,
    ) -> Result<Response<pb::RegisterSeriesResponse>, Status> {
        let request = request.into_inner();
        let series_type =
            proto_series_type_to_core(request.series_type).map_err(error_to_status)?;
        self.engine
            .register_series(request.data_id, series_type)
            .map_err(error_to_status)?;
        Ok(Response::new(pb::RegisterSeriesResponse {}))
    }

    async fn append_batch(
        &self,
        request: Request<pb::AppendBatchRequest>,
    ) -> Result<Response<pb::AppendBatchResponse>, Status> {
        let request = request.into_inner();
        let mut samples = Vec::with_capacity(request.samples.len());
        for sample in request.samples {
            samples.push(proto_sample_to_core(sample).map_err(error_to_status)?);
        }
        Ok(Response::new(append_response(
            self.engine.append_batch_detailed(samples),
        )))
    }

    async fn flush_series(
        &self,
        request: Request<pb::FlushSeriesRequest>,
    ) -> Result<Response<pb::FlushSeriesResponse>, Status> {
        let request = request.into_inner();
        let flushed = self
            .engine
            .flush_series(request.data_id)
            .map_err(error_to_status)?;
        Ok(Response::new(pb::FlushSeriesResponse { flushed }))
    }

    async fn flush_all(
        &self,
        _request: Request<pb::FlushAllRequest>,
    ) -> Result<Response<pb::FlushAllResponse>, Status> {
        let flushed_series = self.engine.flush_all().map_err(error_to_status)?;
        Ok(Response::new(pb::FlushAllResponse { flushed_series }))
    }
}

#[tonic::async_trait]
impl pb::query_service_server::QueryService for TsdbGrpcServer {
    async fn range_query(
        &self,
        request: Request<pb::RangeQueryRequest>,
    ) -> Result<Response<pb::RangeQueryResponse>, Status> {
        let query = proto_query_to_core(request.into_inner()).map_err(error_to_status)?;
        let samples = self
            .engine
            .query_range(query)
            .map_err(error_to_status)?
            .into_iter()
            .map(core_range_sample_to_proto)
            .collect();

        Ok(Response::new(pb::RangeQueryResponse { samples }))
    }
}

#[tonic::async_trait]
impl pb::admin_service_server::AdminService for TsdbGrpcServer {
    async fn stats(
        &self,
        _request: Request<pb::StatsRequest>,
    ) -> Result<Response<pb::StatsResponse>, Status> {
        Ok(Response::new(stats_response(self.engine.stats())))
    }

    async fn health(
        &self,
        _request: Request<pb::HealthRequest>,
    ) -> Result<Response<pb::HealthResponse>, Status> {
        Ok(Response::new(health_response(self.engine.health())))
    }
}
