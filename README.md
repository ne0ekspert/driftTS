# drift-ts

`drift-ts` is a small Rust time-series database with a gRPC interface. It stores typed series (`f64`, `i64`, `bool`), buffers incoming samples in memory, flushes them into immutable segment files, and serves range queries over both flushed and buffered data.

The crate ships as both:

- a library crate: `drift_ts`
- a server binary: `drift-ts`
- a benchmark binary: `throughput`

## Features

- Typed series registration by `data_id`
- Batched append path with per-sample rejection details
- Range queries by `data_id` and timestamp window
- Manifest-backed recovery on startup
- CRC32-protected segment files
- Optional `none|zstd` compression for flushed segment files
- Configurable flush threshold and per-series retention cap
- Automatic oldest-segment eviction when a series exceeds its retention budget
- gRPC services generated from [`proto/driftts/v1/driftts.proto`](/home/ne0ekspert/drifTS/proto/driftts/v1/driftts.proto)

## Project Layout

- [`src/main.rs`](/home/ne0ekspert/drifTS/src/main.rs) starts the gRPC server
- [`src/core/engine.rs`](/home/ne0ekspert/drifTS/src/core/engine.rs) implements ingestion, flushing, querying, recovery, and eviction
- [`src/core/segment.rs`](/home/ne0ekspert/drifTS/src/core/segment.rs) defines the on-disk segment format
- [`src/core/manifest.rs`](/home/ne0ekspert/drifTS/src/core/manifest.rs) tracks registered series and flushed segments
- [`src/config.rs`](/home/ne0ekspert/drifTS/src/config.rs) loads the TOML config
- [`src/bin/throughput.rs`](/home/ne0ekspert/drifTS/src/bin/throughput.rs) runs a local ingest/query benchmark
- [`tests/grpc_smoke.rs`](/home/ne0ekspert/drifTS/tests/grpc_smoke.rs) covers the basic register -> append -> query -> stats flow

## Requirements

- Rust toolchain with Cargo
- No system `protoc` is required; the build uses `protoc-bin-vendored`

## Quick Start

Build the project:

```bash
cargo build
```

Use the sample config in [`drift-ts.toml`](/home/ne0ekspert/drifTS/drift-ts.toml):

```toml
# TCP listener:
listen_addr = "127.0.0.1"
listen_port = 50051
data_dir = "data"
flush_threshold_count = 1000
default_series_max_bytes = 104857600
segment_compression = "zstd"
```

`listen_addr` can also be a Unix domain socket path on Unix systems, for example:

```toml
# Unix socket listener:
listen_addr = "/tmp/drift-ts.sock"
```

Run the server:

```bash
cargo run --bin drift-ts -- --config drift-ts.toml
```

If `--config` is omitted, the server looks for `drift-ts.toml` in the current working directory.

## Configuration

The server accepts exactly one CLI flag:

```text
--config <path>
```

Config fields:

- `listen_addr`: TCP host/IP such as `127.0.0.1` or `localhost`, or a Unix socket path such as `/tmp/drift-ts.sock`
- `listen_port`: optional TCP port. When set, the server binds TCP using `listen_addr` + `listen_port`. When omitted, `listen_addr` is treated as a Unix socket path
- `data_dir`: root directory for `manifest.json` and segment files
- `flush_threshold_count`: number of buffered samples per series before an automatic flush
- `default_series_max_bytes`: optional flushed-segment retention ceiling applied to newly registered series when they do not provide an explicit limit
- `segment_compression`: codec for newly written segment files: `none` or `zstd` (defaults to `zstd` if omitted)

Validation rules:

- `listen_addr` must be non-empty
- when `listen_port` is set, `listen_addr` must be a TCP host/IP, not a path
- when `listen_port` is omitted, `listen_addr` must be a Unix socket path
- `flush_threshold_count` must be greater than zero
- `default_series_max_bytes` must be greater than zero when set

## gRPC API

The protobuf definition lives at [`proto/driftts/v1/driftts.proto`](/home/ne0ekspert/drifTS/proto/driftts/v1/driftts.proto).

Services:

- `IngestService`
- `QueryService`
- `AdminService`

RPCs:

- `RegisterSeries(data_id, series_type, max_storage_bytes?)`
- `AppendBatch(samples[])`
- `FlushSeries(data_id)`
- `FlushAll()`
- `RangeQuery(data_id, start_ts_ms, end_ts_ms, limit?)`
- `Stats()`
- `Health()`

### Example With `grpcurl`

Register an `i64` series:

```bash
grpcurl \
  -plaintext \
  -import-path proto \
  -proto proto/driftts/v1/driftts.proto \
  -d '{"data_id":42,"series_type":"SERIES_TYPE_I64","max_storage_bytes":104857600}' \
  127.0.0.1:50051 \
  driftts.v1.IngestService/RegisterSeries
```

Append two samples:

```bash
grpcurl \
  -plaintext \
  -import-path proto \
  -proto proto/driftts/v1/driftts.proto \
  -d '{
    "samples": [
      {"data_id":42,"timestamp_ms":100,"i64_value":1},
      {"data_id":42,"timestamp_ms":200,"i64_value":2}
    ]
  }' \
  127.0.0.1:50051 \
  driftts.v1.IngestService/AppendBatch
```

Query a range:

```bash
grpcurl \
  -plaintext \
  -import-path proto \
  -proto proto/driftts/v1/driftts.proto \
  -d '{"data_id":42,"start_ts_ms":0,"end_ts_ms":1000}' \
  127.0.0.1:50051 \
  driftts.v1.QueryService/RangeQuery
```

Read stats:

```bash
grpcurl \
  -plaintext \
  -import-path proto \
  -proto proto/driftts/v1/driftts.proto \
  -d '{}' \
  127.0.0.1:50051 \
  driftts.v1.AdminService/Stats
```

## Storage Model

On disk, the database stores:

- `manifest.json`: registered series, segment metadata, and storage accounting
- `segments/<data_id>/<segment_id>.seg`: immutable flushed segment files

Segment files contain:

- a fixed header with series metadata and min/max timestamps
- encoded sample records, optionally compressed with `zstd`
- a CRC32 checksum over the uncompressed segment body

Startup recovery:

- removes stale `*.tmp` files
- reloads `manifest.json` if present
- rescans segment files on disk
- restores missing manifest entries for valid segment files
- drops mismatched segment files if their type conflicts with the registered series
- supports mixed legacy uncompressed segments and new compressed segments in the same data directory

## Semantics and Caveats

- Series must be registered before they can accept samples.
- Each series has a fixed type. Type mismatches are rejected.
- Samples older than or equal to the latest flushed timestamp for a series are rejected.
- Duplicate timestamps inside the in-memory buffer are deduplicated on flush; the last write wins.
- Query results merge flushed data with buffered data and also keep the last value for duplicate timestamps.
- Per-series `max_storage_bytes` applies to flushed segments only. When exceeded, the engine deletes the oldest flushed segment in that series until usage is back under the limit.
- `default_series_max_bytes` is copied into a series when it is registered without an explicit limit, including older manifests upgraded during recovery.
- `storage_bytes` and eviction use the actual on-disk size of segment files, so compressed segments count by their compressed byte size.
- The engine does not enforce a node-wide disk ceiling. Total disk usage is the sum of each series' retained flushed segments.
- Buffered, unflushed samples are not persisted across process restarts.
- `Health()` currently returns a static `ok=true` / `"ok"` response.
- There is no authentication, TLS, or compaction layer in the current server.

## Benchmark Binary

The repository includes a local throughput benchmark:

```bash
cargo run --release --bin throughput -- --help
```

Typical run:

```bash
cargo run --release --bin throughput -- \
  --ingest-mode append-batch \
  --samples 1000000 \
  --series 8 \
  --flush-threshold 4096 \
  --batch-size 1024 \
  --queries 2000 \
  --query-span 2048 \
  --segment-compression zstd
```

Supported flags:

- `--ingest-mode append-one|append-batch`
- `--samples N`
- `--series N`
- `--flush-threshold N`
- `--batch-size N`
- `--queries N`
- `--query-span N`
- `--default-series-max-bytes N`
- `--segment-compression none|zstd`
- `--data-dir PATH`

If `--data-dir` is omitted, the benchmark uses a temporary directory and removes it after completion.

## Testing

Run the test suite:

```bash
cargo test
```

The existing smoke test verifies:

- series registration
- batched append
- range query correctness
- admin stats
- health endpoint behavior

## License

This project is licensed under the terms in [`LICENSE`](/home/ne0ekspert/drifTS/LICENSE).
