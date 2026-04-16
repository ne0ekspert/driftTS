use tonic::{Request, Response, Status};

use crate::core::engine::Engine;
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
