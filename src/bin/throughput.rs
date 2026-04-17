use std::env;
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use drift_ts::core::engine::{Engine, EngineConfig};
use drift_ts::core::segment::SegmentCompressionCodec;
use drift_ts::core::types::{RangeQuery, Sample, SeriesType, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IngestMode {
    AppendOne,
    AppendBatch,
}

#[derive(Debug, Clone)]
struct ThroughputConfig {
    samples: u64,
    series: u64,
    flush_threshold: usize,
    batch_size: usize,
    queries: u64,
    query_span: i64,
    query_limit: Option<usize>,
    default_series_max_bytes: Option<u64>,
    segment_compression: SegmentCompressionCodec,
    data_dir: Option<PathBuf>,
    ingest_mode: IngestMode,
}

impl Default for ThroughputConfig {
    fn default() -> Self {
        Self {
            samples: 1_000_000,
            series: 8,
            flush_threshold: 4_096,
            batch_size: 1_024,
            queries: 2_000,
            query_span: 2_048,
            query_limit: None,
            default_series_max_bytes: Some(1 << 27),
            segment_compression: SegmentCompressionCodec::Zstd,
            data_dir: None,
            ingest_mode: IngestMode::AppendOne,
        }
    }
}

impl ThroughputConfig {
    fn parse() -> Result<Self, String> {
        let mut config = Self::default();
        let mut args = env::args().skip(1);

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--samples" => config.samples = parse_u64(args.next(), "--samples")?,
                "--series" => config.series = parse_u64(args.next(), "--series")?,
                "--flush-threshold" => {
                    config.flush_threshold = parse_usize(args.next(), "--flush-threshold")?
                }
                "--batch-size" => config.batch_size = parse_usize(args.next(), "--batch-size")?,
                "--queries" => config.queries = parse_u64(args.next(), "--queries")?,
                "--query-span" => config.query_span = parse_i64(args.next(), "--query-span")?,
                "--query-limit" => {
                    config.query_limit = Some(parse_usize(args.next(), "--query-limit")?)
                }
                "--default-series-max-bytes" => {
                    config.default_series_max_bytes =
                        Some(parse_u64(args.next(), "--default-series-max-bytes")?)
                }
                "--segment-compression" => {
                    config.segment_compression =
                        parse_segment_compression(args.next(), "--segment-compression")?
                }
                "--ingest-mode" => {
                    config.ingest_mode = parse_ingest_mode(args.next(), "--ingest-mode")?
                }
                "--data-dir" => {
                    let value = args
                        .next()
                        .ok_or_else(|| "missing value after --data-dir".to_string())?;
                    config.data_dir = Some(PathBuf::from(value));
                }
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                other => {
                    return Err(format!("unsupported argument: {other}"));
                }
            }
        }

        if config.samples == 0 {
            return Err("--samples must be greater than zero".to_string());
        }
        if config.series == 0 {
            return Err("--series must be greater than zero".to_string());
        }
        if config.flush_threshold == 0 {
            return Err("--flush-threshold must be greater than zero".to_string());
        }
        if config.batch_size == 0 {
            return Err("--batch-size must be greater than zero".to_string());
        }
        if config.queries == 0 {
            return Err("--queries must be greater than zero".to_string());
        }
        if config.query_span <= 0 {
            return Err("--query-span must be greater than zero".to_string());
        }
        if config.query_limit == Some(0) {
            return Err("--query-limit must be greater than zero".to_string());
        }
        if config.default_series_max_bytes == Some(0) {
            return Err("--default-series-max-bytes must be greater than zero".to_string());
        }

        Ok(config)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ThroughputConfig::parse().map_err(|message| {
        eprintln!("{message}");
        print_usage();
        std::io::Error::new(std::io::ErrorKind::InvalidInput, message)
    })?;

    let (data_dir, cleanup_dir) = match config.data_dir.clone() {
        Some(path) => {
            fs::create_dir_all(&path)?;
            (path, false)
        }
        None => (default_benchmark_dir(), true),
    };

    if data_dir.exists() {
        fs::create_dir_all(&data_dir)?;
    }

    let engine = Engine::open(EngineConfig {
        data_dir: data_dir.clone(),
        flush_threshold_count: config.flush_threshold,
        default_series_max_bytes: config.default_series_max_bytes,
        segment_compression: config.segment_compression,
    })?;

    let register_start = Instant::now();
    for data_id in 0..config.series {
        engine.register_series(data_id, SeriesType::I64, None)?;
    }
    let register_elapsed = register_start.elapsed();

    let append_start = Instant::now();
    run_ingest(&engine, &config)?;
    let append_elapsed = append_start.elapsed();

    let flush_start = Instant::now();
    let flushed_series = engine.flush_all()?;
    let flush_elapsed = flush_start.elapsed();

    let query_start = Instant::now();
    let mut total_rows_returned = 0_usize;
    let mut rng = Lcg::new(0x5eed_cafe_d00d_beef);
    let points_per_series = ((config.samples + config.series - 1) / config.series) as i64;
    let max_start = (points_per_series - config.query_span).max(1);
    for _ in 0..config.queries {
        let data_id = rng.next_u64() % config.series;
        let start_ts = (rng.next_u64() % (max_start as u64)) as i64;
        let end_ts = (start_ts + config.query_span - 1).min(points_per_series.saturating_sub(1));
        let rows = engine.query_range(RangeQuery {
            data_id,
            start_ts_ms: start_ts,
            end_ts_ms: end_ts,
            limit: config.query_limit,
        })?;
        total_rows_returned += rows.len();
    }
    let query_elapsed = query_start.elapsed();

    let stats = engine.stats();

    println!("driftTS throughput benchmark");
    println!("data_dir: {}", data_dir.display());
    println!(
        "config: ingest_mode={} samples={} series={} flush_threshold={} batch_size={} queries={} query_span_ms={} query_limit={} segment_compression={}",
        config.ingest_mode.as_str(),
        config.samples,
        config.series,
        config.flush_threshold,
        config.batch_size,
        config.queries,
        config.query_span,
        config
            .query_limit
            .map_or_else(|| "none".to_string(), |value| value.to_string()),
        config.segment_compression.as_str()
    );
    println!(
        "register: {:?} ({:.0} series/s)",
        register_elapsed,
        rate(config.series, register_elapsed)
    );
    println!(
        "append: {:?} ({:.0} samples/s, {:.1} ns/sample)",
        append_elapsed,
        rate(config.samples, append_elapsed),
        nanos_per_op(config.samples, append_elapsed)
    );
    println!(
        "flush_all: {:?} (flushed_series={})",
        flush_elapsed, flushed_series
    );
    println!(
        "query: {:?} ({:.0} queries/s, avg_rows/query={:.1})",
        query_elapsed,
        rate(config.queries, query_elapsed),
        total_rows_returned as f64 / config.queries as f64
    );
    println!(
        "final_stats: storage_bytes={} segment_count={} buffered_samples={}",
        stats.storage_bytes, stats.segment_count, stats.buffered_samples
    );

    if cleanup_dir {
        fs::remove_dir_all(&data_dir)?;
    }

    Ok(())
}

fn parse_u64(value: Option<String>, flag: &str) -> Result<u64, String> {
    value
        .ok_or_else(|| format!("missing value after {flag}"))?
        .parse::<u64>()
        .map_err(|error| format!("invalid value for {flag}: {error}"))
}

fn parse_usize(value: Option<String>, flag: &str) -> Result<usize, String> {
    value
        .ok_or_else(|| format!("missing value after {flag}"))?
        .parse::<usize>()
        .map_err(|error| format!("invalid value for {flag}: {error}"))
}

fn parse_i64(value: Option<String>, flag: &str) -> Result<i64, String> {
    value
        .ok_or_else(|| format!("missing value after {flag}"))?
        .parse::<i64>()
        .map_err(|error| format!("invalid value for {flag}: {error}"))
}

fn parse_ingest_mode(value: Option<String>, flag: &str) -> Result<IngestMode, String> {
    match value
        .ok_or_else(|| format!("missing value after {flag}"))?
        .as_str()
    {
        "append-one" => Ok(IngestMode::AppendOne),
        "append-batch" => Ok(IngestMode::AppendBatch),
        other => Err(format!(
            "invalid value for {flag}: {other}. expected append-one or append-batch"
        )),
    }
}

fn parse_segment_compression(
    value: Option<String>,
    flag: &str,
) -> Result<SegmentCompressionCodec, String> {
    match value
        .ok_or_else(|| format!("missing value after {flag}"))?
        .as_str()
    {
        "none" => Ok(SegmentCompressionCodec::None),
        "zstd" => Ok(SegmentCompressionCodec::Zstd),
        other => Err(format!(
            "invalid value for {flag}: {other}. expected none or zstd"
        )),
    }
}

fn print_usage() {
    eprintln!(
        "usage: cargo run --release --bin throughput -- [--ingest-mode append-one|append-batch] [--samples N] [--series N] [--flush-threshold N] [--batch-size N] [--queries N] [--query-span N] [--query-limit N] [--default-series-max-bytes N] [--segment-compression none|zstd] [--data-dir PATH]"
    );
}

fn rate(count: u64, elapsed: Duration) -> f64 {
    count as f64 / elapsed.as_secs_f64()
}

fn nanos_per_op(count: u64, elapsed: Duration) -> f64 {
    elapsed.as_nanos() as f64 / count as f64
}

fn default_benchmark_dir() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    env::temp_dir().join(format!(
        "drift-ts-throughput-{}-{nanos}",
        std::process::id()
    ))
}

fn run_ingest(
    engine: &Engine,
    config: &ThroughputConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    match config.ingest_mode {
        IngestMode::AppendOne => {
            for index in 0..config.samples {
                engine.append_one(sample_for(index, config.series))?;
            }
        }
        IngestMode::AppendBatch => {
            let mut batch = Vec::with_capacity(config.batch_size);
            for index in 0..config.samples {
                batch.push(sample_for(index, config.series));
                if batch.len() == config.batch_size {
                    flush_batch(engine, &mut batch)?;
                }
            }
            if !batch.is_empty() {
                flush_batch(engine, &mut batch)?;
            }
        }
    }
    Ok(())
}

fn flush_batch(engine: &Engine, batch: &mut Vec<Sample>) -> Result<(), Box<dyn std::error::Error>> {
    let detail = engine.append_batch_detailed(std::mem::take(batch));
    if detail.rejected != 0 {
        return Err(format!(
            "unexpected rejected samples during benchmark: {:?}",
            detail.errors
        )
        .into());
    }
    Ok(())
}

fn sample_for(index: u64, series: u64) -> Sample {
    let data_id = index % series;
    Sample {
        data_id,
        timestamp_ms: (index / series) as i64,
        value: Value::I64(index as i64),
    }
}

struct Lcg {
    state: u64,
}

impl Lcg {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_mul(6364136223846793005).wrapping_add(1);
        self.state
    }
}

impl IngestMode {
    fn as_str(self) -> &'static str {
        match self {
            IngestMode::AppendOne => "append-one",
            IngestMode::AppendBatch => "append-batch",
        }
    }
}
