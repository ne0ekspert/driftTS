fn main() {
    let protoc_path = protoc_bin_vendored::protoc_bin_path().expect("failed to resolve protoc");
    // SAFETY: build scripts run in a single process context and setting PROTOC here only
    // affects the current build invocation.
    unsafe {
        std::env::set_var("PROTOC", protoc_path);
    }

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/driftts/v1/driftts.proto"], &["proto"])
        .expect("failed to compile protobuf definitions");
}
