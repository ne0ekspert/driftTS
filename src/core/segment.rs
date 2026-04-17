use std::collections::{HashMap, VecDeque};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use crc32fast::Hasher;
use serde::Deserialize;

use crate::core::manifest::{SegmentMeta, fsync_dir};
use crate::core::types::{DataId, RangeSample, SeriesType, Value};
use crate::error::{Result, TsdbError};

const SEGMENT_MAGIC: &[u8; 4] = b"TSL1";
const SEGMENT_VERSION_V1: u16 = 1;
const SEGMENT_VERSION_V2: u16 = 2;
const SEGMENT_VERSION_CURRENT: u16 = SEGMENT_VERSION_V2;
const HEADER_SIZE: usize = 48;
const COMPRESSED_CODEC_OFFSET: usize = 6;
const UNCOMPRESSED_SIZE_OFFSET: usize = 17;
pub const DEFAULT_SEGMENT_CACHE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SegmentCompressionCodec {
    None,
    Zstd,
}

impl SegmentCompressionCodec {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Zstd => "zstd",
        }
    }
}

impl Default for SegmentCompressionCodec {
    fn default() -> Self {
        Self::Zstd
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentHeader {
    pub version: u16,
    pub compression: SegmentCompressionCodec,
    pub data_id: DataId,
    pub series_type: SeriesType,
    pub min_ts_ms: i64,
    pub max_ts_ms: i64,
    pub count: u32,
    pub uncompressed_size_bytes: u64,
    pub crc32: u32,
}

#[derive(Debug)]
pub struct SegmentReadCache {
    max_bytes: usize,
    total_bytes: usize,
    entries: HashMap<PathBuf, CachedSegment>,
    lru: VecDeque<PathBuf>,
}

#[derive(Debug, Clone)]
struct CachedSegment {
    header: SegmentHeader,
    body: Arc<Vec<u8>>,
    size_bytes: usize,
}

impl SegmentReadCache {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            total_bytes: 0,
            entries: HashMap::new(),
            lru: VecDeque::new(),
        }
    }

    pub fn get(&mut self, path: &Path) -> Option<(SegmentHeader, Arc<Vec<u8>>)> {
        let cached = self.entries.get(path)?.clone();
        self.touch(path);
        Some((cached.header, cached.body))
    }

    pub fn insert(&mut self, path: PathBuf, header: SegmentHeader, body: Vec<u8>) {
        if self.max_bytes == 0 {
            return;
        }

        let size_bytes = body.len();
        if size_bytes > self.max_bytes {
            return;
        }

        if let Some(existing) = self.entries.remove(&path) {
            self.total_bytes = self.total_bytes.saturating_sub(existing.size_bytes);
            self.lru.retain(|entry| entry != &path);
        }

        self.total_bytes += size_bytes;
        self.entries.insert(
            path.clone(),
            CachedSegment {
                header,
                body: Arc::new(body),
                size_bytes,
            },
        );
        self.lru.push_back(path);
        self.evict_if_needed();
    }

    fn touch(&mut self, path: &Path) {
        self.lru.retain(|entry| entry != path);
        self.lru.push_back(path.to_path_buf());
    }

    fn evict_if_needed(&mut self) {
        while self.total_bytes > self.max_bytes {
            let Some(oldest) = self.lru.pop_front() else {
                break;
            };
            if let Some(removed) = self.entries.remove(&oldest) {
                self.total_bytes = self.total_bytes.saturating_sub(removed.size_bytes);
            }
        }
    }
}

pub fn segment_rel_path(data_id: DataId, segment_id: u64) -> String {
    format!("segments/{data_id}/{segment_id:012}.seg")
}

pub fn write_segment_file(
    data_dir: &Path,
    data_id: DataId,
    segment_id: u64,
    series_type: SeriesType,
    compression: SegmentCompressionCodec,
    samples: &[RangeSample],
) -> Result<SegmentMeta> {
    if samples.is_empty() {
        return Err(TsdbError::Internal(
            "attempted to write empty segment".to_string(),
        ));
    }

    let rel_path = segment_rel_path(data_id, segment_id);
    let final_path = data_dir.join(&rel_path);
    let tmp_path = final_path.with_extension("seg.tmp");
    let parent = final_path.parent().ok_or_else(|| {
        TsdbError::Internal(format!("invalid segment path: {}", final_path.display()))
    })?;
    fs::create_dir_all(parent)?;

    let body = encode_body(series_type, samples)?;
    let crc32 = crc32(&body);
    let payload = encode_payload(compression, &body)?;
    let header = SegmentHeader {
        version: SEGMENT_VERSION_CURRENT,
        compression,
        data_id,
        series_type,
        min_ts_ms: samples
            .first()
            .map(|sample| sample.timestamp_ms)
            .unwrap_or(0),
        max_ts_ms: samples
            .last()
            .map(|sample| sample.timestamp_ms)
            .unwrap_or(0),
        count: samples.len() as u32,
        uncompressed_size_bytes: body.len() as u64,
        crc32,
    };

    let mut file = File::create(&tmp_path)?;
    file.write_all(&encode_header(&header))?;
    file.write_all(&payload)?;
    file.sync_all()?;
    drop(file);

    fs::rename(&tmp_path, &final_path)?;
    fsync_dir(parent)?;

    let size_bytes = fs::metadata(&final_path)?.len();
    Ok(SegmentMeta {
        segment_id,
        file: rel_path,
        min_ts_ms: header.min_ts_ms,
        max_ts_ms: header.max_ts_ms,
        count: header.count,
        size_bytes,
    })
}

pub fn read_segment_header_only(path: &Path) -> Result<SegmentHeader> {
    let mut file = File::open(path)?;
    let mut header_buf = [0_u8; HEADER_SIZE];
    file.read_exact(&mut header_buf)?;
    parse_segment_header(&header_buf)
}

pub fn read_segment_file(path: &Path) -> Result<(SegmentHeader, Vec<RangeSample>)> {
    let bytes = fs::read(path)?;
    if bytes.len() < HEADER_SIZE {
        return Err(TsdbError::CorruptSegment(format!(
            "segment too small: {}",
            path.display()
        )));
    }
    let header = parse_segment_header(&bytes[..HEADER_SIZE])?;
    let payload = &bytes[HEADER_SIZE..];
    let body = decode_payload(path, &header, payload)?;
    validate_crc(path, &header, &body)?;
    let samples = decode_body(header.series_type, &body)?;
    Ok((header, samples))
}

pub fn read_segment_range(path: &Path, start_ts: i64, end_ts: i64) -> Result<Vec<RangeSample>> {
    read_segment_range_with_cache(path, start_ts, end_ts, None)
}

pub fn read_segment_range_with_cache(
    path: &Path,
    start_ts: i64,
    end_ts: i64,
    mut cache: Option<&mut SegmentReadCache>,
) -> Result<Vec<RangeSample>> {
    if let Some(cache) = cache.as_deref_mut() {
        if let Some((header, body)) = cache.get(path) {
            if header.max_ts_ms < start_ts || header.min_ts_ms > end_ts {
                return Ok(Vec::new());
            }
            return decode_body_range(header.series_type, body.as_slice(), start_ts, end_ts);
        }
    }

    let bytes = fs::read(path)?;
    if bytes.len() < HEADER_SIZE {
        return Err(TsdbError::CorruptSegment(format!(
            "segment too small: {}",
            path.display()
        )));
    }

    let header = parse_segment_header(&bytes[..HEADER_SIZE])?;
    if header.max_ts_ms < start_ts || header.min_ts_ms > end_ts {
        return Ok(Vec::new());
    }

    let payload = &bytes[HEADER_SIZE..];
    let body = decode_payload(path, &header, payload)?;
    validate_crc(path, &header, &body)?;
    if header.compression != SegmentCompressionCodec::None {
        if let Some(cache) = cache.as_deref_mut() {
            cache.insert(path.to_path_buf(), header.clone(), body.clone());
        }
    }
    decode_body_range(header.series_type, &body, start_ts, end_ts)
}

pub fn parse_segment_header(buf: &[u8]) -> Result<SegmentHeader> {
    if buf.len() != HEADER_SIZE {
        return Err(TsdbError::CorruptSegment(format!(
            "invalid header size: expected {HEADER_SIZE}, got {}",
            buf.len()
        )));
    }
    if &buf[0..4] != SEGMENT_MAGIC {
        return Err(TsdbError::CorruptSegment(
            "invalid segment magic".to_string(),
        ));
    }

    let version = u16::from_le_bytes([buf[4], buf[5]]);
    if version != SEGMENT_VERSION_V1 && version != SEGMENT_VERSION_V2 {
        return Err(TsdbError::CorruptSegment(format!(
            "unsupported segment version: {version}"
        )));
    }

    let data_id = u64::from_le_bytes(buf[8..16].try_into().unwrap());
    let series_type = decode_series_type(buf[16])?;
    let min_ts_ms = i64::from_le_bytes(buf[24..32].try_into().unwrap());
    let max_ts_ms = i64::from_le_bytes(buf[32..40].try_into().unwrap());
    let count = u32::from_le_bytes(buf[40..44].try_into().unwrap());
    let crc32 = u32::from_le_bytes(buf[44..48].try_into().unwrap());
    let expected_uncompressed_size = expected_uncompressed_size(count, series_type)?;

    let (compression, uncompressed_size_bytes) = if version == SEGMENT_VERSION_V1 {
        (
            SegmentCompressionCodec::None,
            expected_uncompressed_size as u64,
        )
    } else {
        let compression = decode_compression_codec(buf[COMPRESSED_CODEC_OFFSET])?;
        let size = decode_u56(&buf[UNCOMPRESSED_SIZE_OFFSET..24]);
        if size != expected_uncompressed_size as u64 {
            return Err(TsdbError::CorruptSegment(format!(
                "invalid uncompressed body size in header: expected {}, got {size}",
                expected_uncompressed_size
            )));
        }
        (compression, size)
    };

    Ok(SegmentHeader {
        version,
        compression,
        data_id,
        series_type,
        min_ts_ms,
        max_ts_ms,
        count,
        uncompressed_size_bytes,
        crc32,
    })
}

fn validate_crc(path: &Path, header: &SegmentHeader, body: &[u8]) -> Result<()> {
    let actual = crc32(body);
    if actual != header.crc32 {
        return Err(TsdbError::CorruptSegment(format!(
            "crc mismatch for {}: expected {}, got {}",
            path.display(),
            header.crc32,
            actual
        )));
    }
    Ok(())
}

fn encode_header(header: &SegmentHeader) -> [u8; HEADER_SIZE] {
    let mut buf = [0_u8; HEADER_SIZE];
    buf[0..4].copy_from_slice(SEGMENT_MAGIC);
    buf[4..6].copy_from_slice(&header.version.to_le_bytes());
    buf[COMPRESSED_CODEC_OFFSET] = encode_compression_codec(header.compression);
    buf[8..16].copy_from_slice(&header.data_id.to_le_bytes());
    buf[16] = encode_series_type(header.series_type);
    encode_u56(
        &mut buf[UNCOMPRESSED_SIZE_OFFSET..24],
        header.uncompressed_size_bytes,
    );
    buf[24..32].copy_from_slice(&header.min_ts_ms.to_le_bytes());
    buf[32..40].copy_from_slice(&header.max_ts_ms.to_le_bytes());
    buf[40..44].copy_from_slice(&header.count.to_le_bytes());
    buf[44..48].copy_from_slice(&header.crc32.to_le_bytes());
    buf
}

fn encode_body(series_type: SeriesType, samples: &[RangeSample]) -> Result<Vec<u8>> {
    let mut body = Vec::with_capacity(samples.len() * record_size(series_type));
    for sample in samples {
        body.extend_from_slice(&sample.timestamp_ms.to_le_bytes());
        match (series_type, &sample.value) {
            (SeriesType::F64, Value::F64(value)) => body.extend_from_slice(&value.to_le_bytes()),
            (SeriesType::I64, Value::I64(value)) => body.extend_from_slice(&value.to_le_bytes()),
            (SeriesType::Bool, Value::Bool(value)) => body.push(u8::from(*value)),
            (_, value) => {
                return Err(TsdbError::CorruptSegment(format!(
                    "series type/value mismatch while encoding: {series_type:?} vs {value:?}"
                )));
            }
        }
    }
    Ok(body)
}

fn decode_body(series_type: SeriesType, body: &[u8]) -> Result<Vec<RangeSample>> {
    let record_size = record_size(series_type);
    let mut result = Vec::with_capacity(body.len() / record_size);

    let mut offset = 0;
    while offset < body.len() {
        let timestamp_ms = i64::from_le_bytes(body[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let value = match series_type {
            SeriesType::F64 => {
                let value = f64::from_le_bytes(body[offset..offset + 8].try_into().unwrap());
                offset += 8;
                Value::F64(value)
            }
            SeriesType::I64 => {
                let value = i64::from_le_bytes(body[offset..offset + 8].try_into().unwrap());
                offset += 8;
                Value::I64(value)
            }
            SeriesType::Bool => {
                let raw = body[offset];
                offset += 1;
                match raw {
                    0 => Value::Bool(false),
                    1 => Value::Bool(true),
                    other => {
                        return Err(TsdbError::CorruptSegment(format!(
                            "invalid bool value encoding: {other}"
                        )));
                    }
                }
            }
        };
        result.push(RangeSample {
            timestamp_ms,
            value,
        });
    }

    Ok(result)
}

fn decode_body_range(
    series_type: SeriesType,
    body: &[u8],
    start_ts: i64,
    end_ts: i64,
) -> Result<Vec<RangeSample>> {
    let record_size = record_size(series_type);
    let count = body.len() / record_size;
    if count == 0 {
        return Ok(Vec::new());
    }

    let start_idx = lower_bound_timestamp(body, record_size, start_ts);
    let end_idx = upper_bound_timestamp(body, record_size, end_ts);
    if start_idx >= end_idx {
        return Ok(Vec::new());
    }

    let mut result = Vec::with_capacity(end_idx - start_idx);
    for idx in start_idx..end_idx {
        result.push(decode_sample_at(series_type, body, idx * record_size)?);
    }
    Ok(result)
}

fn lower_bound_timestamp(body: &[u8], record_size: usize, target_ts: i64) -> usize {
    let mut left = 0;
    let mut right = body.len() / record_size;
    while left < right {
        let mid = left + (right - left) / 2;
        if timestamp_at(body, record_size, mid) < target_ts {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    left
}

fn upper_bound_timestamp(body: &[u8], record_size: usize, target_ts: i64) -> usize {
    let mut left = 0;
    let mut right = body.len() / record_size;
    while left < right {
        let mid = left + (right - left) / 2;
        if timestamp_at(body, record_size, mid) <= target_ts {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    left
}

fn timestamp_at(body: &[u8], record_size: usize, idx: usize) -> i64 {
    let offset = idx * record_size;
    i64::from_le_bytes(body[offset..offset + 8].try_into().unwrap())
}

fn decode_sample_at(series_type: SeriesType, body: &[u8], offset: usize) -> Result<RangeSample> {
    let timestamp_ms = i64::from_le_bytes(body[offset..offset + 8].try_into().unwrap());
    let value_offset = offset + 8;
    let value = match series_type {
        SeriesType::F64 => {
            let value =
                f64::from_le_bytes(body[value_offset..value_offset + 8].try_into().unwrap());
            Value::F64(value)
        }
        SeriesType::I64 => {
            let value =
                i64::from_le_bytes(body[value_offset..value_offset + 8].try_into().unwrap());
            Value::I64(value)
        }
        SeriesType::Bool => match body[value_offset] {
            0 => Value::Bool(false),
            1 => Value::Bool(true),
            other => {
                return Err(TsdbError::CorruptSegment(format!(
                    "invalid bool value encoding: {other}"
                )));
            }
        },
    };

    Ok(RangeSample {
        timestamp_ms,
        value,
    })
}

fn encode_payload(compression: SegmentCompressionCodec, body: &[u8]) -> Result<Vec<u8>> {
    match compression {
        SegmentCompressionCodec::None => Ok(body.to_vec()),
        SegmentCompressionCodec::Zstd => zstd::stream::encode_all(body, 0).map_err(TsdbError::Io),
    }
}

fn decode_payload(path: &Path, header: &SegmentHeader, payload: &[u8]) -> Result<Vec<u8>> {
    let body = match header.compression {
        SegmentCompressionCodec::None => payload.to_vec(),
        SegmentCompressionCodec::Zstd => zstd::stream::decode_all(payload).map_err(|error| {
            TsdbError::CorruptSegment(format!("failed to decompress {}: {error}", path.display()))
        })?,
    };

    if body.len() as u64 != header.uncompressed_size_bytes {
        return Err(TsdbError::CorruptSegment(format!(
            "invalid body size for {}: expected {}, got {}",
            path.display(),
            header.uncompressed_size_bytes,
            body.len()
        )));
    }

    Ok(body)
}

pub fn segment_path(data_dir: &Path, meta: &SegmentMeta) -> Result<PathBuf> {
    resolve_segment_path(data_dir, &meta.file)
}

pub fn resolve_segment_path(data_dir: &Path, rel_path: &str) -> Result<PathBuf> {
    validate_segment_rel_path(rel_path)?;
    Ok(data_dir.join(rel_path))
}

fn validate_segment_rel_path(rel_path: &str) -> Result<()> {
    let path = Path::new(rel_path);
    let mut components = path.components();

    match components.next() {
        Some(Component::Normal(component)) if component == "segments" => {}
        _ => {
            return Err(TsdbError::Manifest(format!(
                "segment path must stay under segments/: {rel_path}"
            )));
        }
    }

    let mut saw_child = false;
    for component in components {
        match component {
            Component::Normal(_) => saw_child = true,
            _ => {
                return Err(TsdbError::Manifest(format!(
                    "segment path contains invalid components: {rel_path}"
                )));
            }
        }
    }

    if !saw_child {
        return Err(TsdbError::Manifest(format!(
            "segment path is incomplete: {rel_path}"
        )));
    }

    Ok(())
}

pub fn record_size(series_type: SeriesType) -> usize {
    match series_type {
        SeriesType::F64 | SeriesType::I64 => 16,
        SeriesType::Bool => 9,
    }
}

fn encode_series_type(series_type: SeriesType) -> u8 {
    match series_type {
        SeriesType::F64 => 1,
        SeriesType::I64 => 2,
        SeriesType::Bool => 3,
    }
}

fn decode_series_type(raw: u8) -> Result<SeriesType> {
    match raw {
        1 => Ok(SeriesType::F64),
        2 => Ok(SeriesType::I64),
        3 => Ok(SeriesType::Bool),
        other => Err(TsdbError::CorruptSegment(format!(
            "unknown series type encoding: {other}"
        ))),
    }
}

fn encode_compression_codec(codec: SegmentCompressionCodec) -> u8 {
    match codec {
        SegmentCompressionCodec::None => 0,
        SegmentCompressionCodec::Zstd => 1,
    }
}

fn decode_compression_codec(raw: u8) -> Result<SegmentCompressionCodec> {
    match raw {
        0 => Ok(SegmentCompressionCodec::None),
        1 => Ok(SegmentCompressionCodec::Zstd),
        other => Err(TsdbError::CorruptSegment(format!(
            "unknown compression encoding: {other}"
        ))),
    }
}

fn expected_uncompressed_size(count: u32, series_type: SeriesType) -> Result<usize> {
    (count as usize)
        .checked_mul(record_size(series_type))
        .ok_or_else(|| TsdbError::CorruptSegment("segment body size overflow".to_string()))
}

fn encode_u56(dst: &mut [u8], value: u64) {
    let bytes = value.to_le_bytes();
    dst.copy_from_slice(&bytes[..7]);
}

fn decode_u56(src: &[u8]) -> u64 {
    let mut bytes = [0_u8; 8];
    bytes[..7].copy_from_slice(src);
    u64::from_le_bytes(bytes)
}

fn crc32(body: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(body);
    hasher.finalize()
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn writes_and_reads_f64_segments() {
        let tempdir = TempDir::new().unwrap();
        let samples = vec![
            RangeSample {
                timestamp_ms: 10,
                value: Value::F64(1.5),
            },
            RangeSample {
                timestamp_ms: 20,
                value: Value::F64(2.5),
            },
        ];

        let meta = write_segment_file(
            tempdir.path(),
            7,
            1,
            SeriesType::F64,
            SegmentCompressionCodec::None,
            &samples,
        )
        .unwrap();
        let path = segment_path(tempdir.path(), &meta).unwrap();
        let (header, read_back) = read_segment_file(&path).unwrap();

        assert_eq!(header.data_id, 7);
        assert_eq!(header.series_type, SeriesType::F64);
        assert_eq!(header.compression, SegmentCompressionCodec::None);
        assert_eq!(read_back, samples);
    }

    #[test]
    fn writes_and_reads_zstd_segments() {
        let tempdir = TempDir::new().unwrap();
        let samples = vec![
            RangeSample {
                timestamp_ms: 10,
                value: Value::I64(100),
            },
            RangeSample {
                timestamp_ms: 20,
                value: Value::I64(200),
            },
        ];

        let meta = write_segment_file(
            tempdir.path(),
            9,
            2,
            SeriesType::I64,
            SegmentCompressionCodec::Zstd,
            &samples,
        )
        .unwrap();
        let path = segment_path(tempdir.path(), &meta).unwrap();
        let (header, read_back) = read_segment_file(&path).unwrap();

        assert_eq!(header.compression, SegmentCompressionCodec::Zstd);
        assert_eq!(read_back, samples);
    }

    #[test]
    fn reads_only_requested_range_from_segment() {
        let tempdir = TempDir::new().unwrap();
        let samples = vec![
            RangeSample {
                timestamp_ms: 10,
                value: Value::I64(100),
            },
            RangeSample {
                timestamp_ms: 20,
                value: Value::I64(200),
            },
            RangeSample {
                timestamp_ms: 30,
                value: Value::I64(300),
            },
            RangeSample {
                timestamp_ms: 40,
                value: Value::I64(400),
            },
        ];

        let meta = write_segment_file(
            tempdir.path(),
            11,
            4,
            SeriesType::I64,
            SegmentCompressionCodec::Zstd,
            &samples,
        )
        .unwrap();
        let path = segment_path(tempdir.path(), &meta).unwrap();

        let range = read_segment_range(&path, 15, 35).unwrap();
        assert_eq!(
            range,
            vec![
                RangeSample {
                    timestamp_ms: 20,
                    value: Value::I64(200),
                },
                RangeSample {
                    timestamp_ms: 30,
                    value: Value::I64(300),
                },
            ]
        );
    }

    #[test]
    fn reads_legacy_v1_segments() {
        let tempdir = TempDir::new().unwrap();
        let samples = vec![RangeSample {
            timestamp_ms: 1,
            value: Value::I64(42),
        }];

        let meta = write_legacy_v1_segment(tempdir.path(), 9, 2, SeriesType::I64, &samples);
        let path = segment_path(tempdir.path(), &meta).unwrap();
        let (header, read_back) = read_segment_file(&path).unwrap();

        assert_eq!(header.version, SEGMENT_VERSION_V1);
        assert_eq!(header.compression, SegmentCompressionCodec::None);
        assert_eq!(read_back, samples);
    }

    #[test]
    fn rejects_corrupt_crc() {
        let tempdir = TempDir::new().unwrap();
        let samples = vec![RangeSample {
            timestamp_ms: 1,
            value: Value::I64(42),
        }];

        let meta = write_segment_file(
            tempdir.path(),
            9,
            2,
            SeriesType::I64,
            SegmentCompressionCodec::None,
            &samples,
        )
        .unwrap();
        let path = segment_path(tempdir.path(), &meta).unwrap();
        let mut bytes = fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        fs::write(&path, bytes).unwrap();

        let error = read_segment_file(&path).unwrap_err();
        assert!(matches!(error, TsdbError::CorruptSegment(_)));
    }

    #[test]
    fn rejects_corrupt_zstd_payload() {
        let tempdir = TempDir::new().unwrap();
        let samples = vec![RangeSample {
            timestamp_ms: 1,
            value: Value::I64(42),
        }];

        let meta = write_segment_file(
            tempdir.path(),
            9,
            3,
            SeriesType::I64,
            SegmentCompressionCodec::Zstd,
            &samples,
        )
        .unwrap();
        let path = segment_path(tempdir.path(), &meta).unwrap();
        let mut bytes = fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        fs::write(&path, bytes).unwrap();

        let error = read_segment_file(&path).unwrap_err();
        assert!(matches!(error, TsdbError::CorruptSegment(_)));
    }

    #[test]
    fn rejects_segment_paths_outside_segments_root() {
        let tempdir = TempDir::new().unwrap();
        let meta = SegmentMeta {
            segment_id: 1,
            file: "../victim.txt".to_string(),
            min_ts_ms: 0,
            max_ts_ms: 1,
            count: 1,
            size_bytes: 1,
        };

        let error = resolve_segment_path(tempdir.path(), &meta.file).unwrap_err();
        assert!(matches!(error, TsdbError::Manifest(_)));
    }

    fn write_legacy_v1_segment(
        data_dir: &Path,
        data_id: DataId,
        segment_id: u64,
        series_type: SeriesType,
        samples: &[RangeSample],
    ) -> SegmentMeta {
        let rel_path = segment_rel_path(data_id, segment_id);
        let path = data_dir.join(&rel_path);
        fs::create_dir_all(path.parent().unwrap()).unwrap();

        let body = encode_body(series_type, samples).unwrap();
        let header = SegmentHeader {
            version: SEGMENT_VERSION_V1,
            compression: SegmentCompressionCodec::None,
            data_id,
            series_type,
            min_ts_ms: samples.first().unwrap().timestamp_ms,
            max_ts_ms: samples.last().unwrap().timestamp_ms,
            count: samples.len() as u32,
            uncompressed_size_bytes: body.len() as u64,
            crc32: crc32(&body),
        };

        let mut bytes = [0_u8; HEADER_SIZE].to_vec();
        bytes[0..4].copy_from_slice(SEGMENT_MAGIC);
        bytes[4..6].copy_from_slice(&header.version.to_le_bytes());
        bytes[8..16].copy_from_slice(&header.data_id.to_le_bytes());
        bytes[16] = encode_series_type(header.series_type);
        bytes[24..32].copy_from_slice(&header.min_ts_ms.to_le_bytes());
        bytes[32..40].copy_from_slice(&header.max_ts_ms.to_le_bytes());
        bytes[40..44].copy_from_slice(&header.count.to_le_bytes());
        bytes[44..48].copy_from_slice(&header.crc32.to_le_bytes());
        bytes.extend_from_slice(&body);
        fs::write(&path, &bytes).unwrap();

        SegmentMeta {
            segment_id,
            file: rel_path,
            min_ts_ms: header.min_ts_ms,
            max_ts_ms: header.max_ts_ms,
            count: header.count,
            size_bytes: bytes.len() as u64,
        }
    }
}
