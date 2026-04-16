use std::net::SocketAddr;

use tonic::transport::Server;

use drift_ts::config::AppConfig;
use drift_ts::core::engine::{Engine, EngineConfig};
use drift_ts::transport::grpc::pb::{
    admin_service_server::AdminServiceServer, ingest_service_server::IngestServiceServer,
    query_service_server::QueryServiceServer,
};
use drift_ts::transport::grpc::server::TsdbGrpcServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = AppConfig::load_from_cli()?;
    let addr: SocketAddr = config.listen_addr.parse()?;
    let engine = Engine::open(EngineConfig::from(&config))?;
    let grpc = TsdbGrpcServer::new(engine);

    Server::builder()
        .add_service(IngestServiceServer::new(grpc.clone()))
        .add_service(QueryServiceServer::new(grpc.clone()))
        .add_service(AdminServiceServer::new(grpc))
        .serve(addr)
        .await?;

    Ok(())
}
