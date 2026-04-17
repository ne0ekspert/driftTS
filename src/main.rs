use drift_ts::config::AppConfig;
use drift_ts::core::engine::{Engine, EngineConfig};
use drift_ts::transport::grpc::server::{TsdbGrpcServer, serve};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = AppConfig::load_from_cli()?;
    let endpoint = config.listen_endpoint()?;
    let engine = Engine::open(EngineConfig::from(&config))?;
    let grpc = TsdbGrpcServer::new(engine);

    serve(&endpoint, grpc).await?;

    Ok(())
}
