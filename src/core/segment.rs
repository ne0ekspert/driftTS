use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crc32fast::Hasher;

use crate::core::manifest::{SegmentMeta, fsync_dir};
use crate::core::types::{DataId, RangeSample, SeriesType, Value};
use crate::error::{Result, TsdbError};

const SEGMENT_MAGIC: &[u8; 4] = b"TSL1";
const SEGMENT_VERSION: u16 = 1;
const HEADER_SIZE: usize = 48;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentHeader {
    pub version: u16,
    pub data_id: DataId,
    pub series_type: SeriesType,
    pub min_ts_ms: i64,
    pub max_ts_ms: i64,
    pub count: u32,
    pub crc32: u32,
}

pub fn segment_rel_path(data_id: DataId, segment_id: u64) -> String {
    format!("segments/{data_id}/{segment_id:012}.seg")
}

pub fn write_segment_file(
    data_dir: &Path,
    data_id: DataId,
    segment_id: u64,
    series_type: SeriesType,
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
    let header = SegmentHeader {
        version: SEGMENT_VERSION,
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
        crc32,
    };

    let mut file = File::create(&tmp_path)?;
    file.write_all(&encode_header(&header))?;
    file.write_all(&body)?;
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
    let body = &bytes[HEADER_SIZE..];
    validate_body_size(path, &header, body)?;
    validate_crc(path, &header, body)?;
    let samples = decode_body(header.series_type, body)?;
    Ok((header, samples))
}

pub fn read_segment_range(path: &Path, start_ts: i64, end_ts: i64) -> Result<Vec<RangeSample>> {
    let (header, samples) = read_segment_file(path)?;
    if header.max_ts_ms < start_ts || header.min_ts_ms > end_ts {
        return Ok(Vec::new());
    }

    Ok(samples
        .into_iter()
        .filter(|sample| start_ts <= sample.timestamp_ms && sample.timestamp_ms <= end_ts)
        .collect())
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
    if version != SEGMENT_VERSION {
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

    Ok(SegmentHeader {
        version,
        data_id,
        series_type,
        min_ts_ms,
        max_ts_ms,
        count,
        crc32,
    })
}

fn validate_body_size(path: &Path, header: &SegmentHeader, body: &[u8]) -> Result<()> {
    let expected = header.count as usize * record_size(header.series_type);
    if body.len() != expected {
        return Err(TsdbError::CorruptSegment(format!(
            "invalid body size for {}: expected {expected}, got {}",
            path.display(),
            body.len()
        )));
    }
    Ok(())
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
    buf[8..16].copy_from_slice(&header.data_id.to_le_bytes());
    buf[16] = encode_series_type(header.series_type);
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

pub fn segment_path(data_dir: &Path, meta: &SegmentMeta) -> PathBuf {
    data_dir.join(&meta.file)
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

        let meta = write_segment_file(tempdir.path(), 7, 1, SeriesType::F64, &samples).unwrap();
        let path = segment_path(tempdir.path(), &meta);
        let (header, read_back) = read_segment_file(&path).unwrap();

        assert_eq!(header.data_id, 7);
        assert_eq!(header.series_type, SeriesType::F64);
        assert_eq!(read_back, samples);
    }

    #[test]
    fn rejects_corrupt_crc() {
        let tempdir = TempDir::new().unwrap();
        let samples = vec![RangeSample {
            timestamp_ms: 1,
            value: Value::I64(42),
        }];

        let meta = write_segment_file(tempdir.path(), 9, 2, SeriesType::I64, &samples).unwrap();
        let path = segment_path(tempdir.path(), &meta);
        let mut bytes = fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        fs::write(&path, bytes).unwrap();

        let error = read_segment_file(&path).unwrap_err();
        assert!(matches!(error, TsdbError::CorruptSegment(_)));
    }
}
