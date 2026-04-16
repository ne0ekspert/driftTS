pub mod convert;
pub mod server;

pub mod pb {
    tonic::include_proto!("driftts.v1");
}
