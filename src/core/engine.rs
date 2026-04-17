use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use crate::config::AppConfig;
use crate::core::manifest::{Manifest, SegmentMeta, SeriesEntry, manifest_tmp_path};
use crate::core::segment::{
    DEFAULT_SEGMENT_CACHE_BYTES, SegmentCompressionCodec, SegmentHeader, SegmentReadCache,
    read_segment_header_only, read_segment_range_with_cache, resolve_segment_path, segment_path,
    write_segment_file,
};
use crate::core::types::{
    BufferedSample, DataId, RangeQuery, RangeSample, Sample, SeriesMeta, SeriesState, SeriesType,
};
use crate::error::{Result, TsdbError};

#[derive(Debug, Clone)]
pub struct EngineConfig {
    pub data_dir: PathBuf,
    pub flush_threshold_count: usize,
    pub max_storage_bytes: u64,
    pub segment_compression: SegmentCompressionCodec,
}

impl From<&AppConfig> for EngineConfig {
    fn from(value: &AppConfig) -> Self {
        Self {
            data_dir: value.data_dir.clone(),
            flush_threshold_count: value.flush_threshold_count,
            max_storage_bytes: value.max_storage_bytes,
            segment_compression: value.segment_compression,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AppendBatchDetailed {
    pub accepted: u32,
    pub rejected: u32,
    pub errors: Vec<AppendErrorDetail>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendErrorDetail {
    pub index: usize,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineStats {
    pub storage_bytes: u64,
    pub max_storage_bytes: u64,
    pub series_count: usize,
    pub segment_count: usize,
    pub buffered_samples: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthStatus {
    pub ok: bool,
    pub message: String,
}

#[derive(Debug, Clone)]
pub struct Engine {
    pub config: EngineConfig,
    pub manifest: Arc<Mutex<Manifest>>,
    pub series: Arc<RwLock<HashMap<DataId, Arc<Mutex<SeriesState>>>>>,
    pub segment_cache: Arc<Mutex<SegmentReadCache>>,
}

impl Engine {
    pub fn open(config: EngineConfig) -> Result<Self> {
        fs::create_dir_all(config.data_dir.join("segments"))?;
        let manifest = recover_manifest(&config)?;
        let series = build_series_state(&manifest);

        Ok(Self {
            config,
            manifest: Arc::new(Mutex::new(manifest)),
            series: Arc::new(RwLock::new(series)),
            segment_cache: Arc::new(Mutex::new(SegmentReadCache::new(
                DEFAULT_SEGMENT_CACHE_BYTES,
            ))),
        })
    }

    pub fn register_series(&self, data_id: DataId, series_type: SeriesType) -> Result<()> {
        let mut series_map = self.series.write().unwrap();
        if series_map.contains_key(&data_id) {
            return Err(TsdbError::SeriesExists(data_id));
        }

        let mut manifest = self.manifest.lock().unwrap();
        if manifest.series.contains_key(&data_id) {
            return Err(TsdbError::SeriesExists(data_id));
        }

        manifest.series.insert(
            data_id,
            SeriesEntry {
                series_type,
                next_segment_id: 1,
                segments: Vec::new(),
            },
        );
        manifest.save(&self.config.data_dir)?;

        series_map.insert(
            data_id,
            Arc::new(Mutex::new(SeriesState::new(
                SeriesMeta {
                    data_id,
                    series_type,
                },
                None,
            ))),
        );
        Ok(())
    }

    pub fn append_batch_detailed(&self, samples: Vec<Sample>) -> AppendBatchDetailed {
        let mut accepted = 0_u32;
        let mut rejected = 0_u32;
        let mut errors = Vec::new();
        let mut grouped: HashMap<DataId, Vec<(usize, Sample)>> = HashMap::new();
        for (index, sample) in samples.into_iter().enumerate() {
            grouped
                .entry(sample.data_id)
                .or_default()
                .push((index, sample));
        }

        let handles = self.series.read().unwrap();
        let mut to_flush = Vec::new();
        for (data_id, entries) in grouped {
            let Some(handle) = handles.get(&data_id).cloned() else {
                rejected += entries.len() as u32;
                errors.extend(entries.into_iter().map(|(index, _)| AppendErrorDetail {
                    index,
                    reason: TsdbError::SeriesNotFound(data_id).to_string(),
                }));
                continue;
            };

            let mut state = handle.lock().unwrap();
            let mut should_flush = false;
            for (index, sample) in entries {
                match append_to_series_state(&mut state, sample) {
                    Ok(()) => {
                        accepted += 1;
                        should_flush |= state.memtable.len() >= self.config.flush_threshold_count;
                    }
                    Err(error) => {
                        rejected += 1;
                        errors.push(AppendErrorDetail {
                            index,
                            reason: error.to_string(),
                        });
                    }
                }
            }
            if should_flush {
                to_flush.push(data_id);
            }
        }
        drop(handles);

        if let Err(error) = self.flush_serieses(&to_flush) {
            rejected += 1;
            errors.push(AppendErrorDetail {
                index: usize::MAX,
                reason: format!("batched flush failed: {error}"),
            });
        }

        errors.sort_by_key(|error| error.index);

        AppendBatchDetailed {
            accepted,
            rejected,
            errors,
        }
    }

    pub fn append_one(&self, sample: Sample) -> Result<()> {
        let data_id = sample.data_id;
        let handle = self.series_handle(sample.data_id)?;
        let should_flush = {
            let mut state = handle.lock().unwrap();
            append_to_series_state(&mut state, sample)?;
            state.memtable.len() >= self.config.flush_threshold_count
        };

        if should_flush {
            self.flush_series(data_id)?;
        }

        Ok(())
    }

    pub fn flush_series(&self, data_id: DataId) -> Result<bool> {
        Ok(self.flush_serieses(&[data_id])? != 0)
    }

    pub fn flush_all(&self) -> Result<u32> {
        let ids: Vec<DataId> = self.series.read().unwrap().keys().copied().collect();
        self.flush_serieses(&ids)
    }

    pub fn query_range(&self, query: RangeQuery) -> Result<Vec<RangeSample>> {
        if query.start_ts_ms > query.end_ts_ms {
            return Err(TsdbError::InvalidQuery(
                "start_ts_ms must be less than or equal to end_ts_ms".to_string(),
            ));
        }

        let segment_meta = {
            let manifest = self.manifest.lock().unwrap();
            let series_entry = manifest
                .series
                .get(&query.data_id)
                .ok_or(TsdbError::SeriesNotFound(query.data_id))?;
            series_entry
                .segments
                .iter()
                .filter(|segment| {
                    !(segment.max_ts_ms < query.start_ts_ms || segment.min_ts_ms > query.end_ts_ms)
                })
                .cloned()
                .collect::<Vec<_>>()
        };

        let mut disk_samples = Vec::new();
        let mut segment_cache = self.segment_cache.lock().unwrap();
        for meta in segment_meta {
            let path = segment_path(&self.config.data_dir, &meta)?;
            disk_samples.extend(read_segment_range_with_cache(
                &path,
                query.start_ts_ms,
                query.end_ts_ms,
                Some(&mut segment_cache),
            )?);
        }
        drop(segment_cache);

        let mem_samples = {
            let handle = self.series_handle(query.data_id)?;
            let state = handle.lock().unwrap();
            let deduped = prepare_flush_samples(state.memtable.clone());
            deduped
                .into_iter()
                .filter(|sample| {
                    query.start_ts_ms <= sample.timestamp_ms
                        && sample.timestamp_ms <= query.end_ts_ms
                })
                .collect::<Vec<_>>()
        };

        let mut merged = merge_samples(disk_samples, mem_samples);
        if let Some(limit) = query.limit {
            merged.truncate(limit);
        }
        Ok(merged)
    }

    pub fn stats(&self) -> EngineStats {
        let manifest = self.manifest.lock().unwrap();
        let storage_bytes = manifest.storage_bytes;
        let max_storage_bytes = manifest.max_storage_bytes;
        let series_count = manifest.series.len();
        let segment_count = manifest.total_segment_count();
        drop(manifest);

        let buffered_samples = self
            .series
            .read()
            .unwrap()
            .values()
            .map(|state| state.lock().unwrap().memtable.len())
            .sum();

        EngineStats {
            storage_bytes,
            max_storage_bytes,
            series_count,
            segment_count,
            buffered_samples,
        }
    }

    pub fn health(&self) -> HealthStatus {
        HealthStatus {
            ok: true,
            message: "ok".to_string(),
        }
    }

    pub fn evict_if_needed(&self) -> Result<()> {
        loop {
            let maybe_victim = {
                let manifest = self.manifest.lock().unwrap();
                if manifest.storage_bytes <= manifest.max_storage_bytes {
                    None
                } else {
                    manifest
                        .series
                        .iter()
                        .flat_map(|(data_id, entry)| {
                            entry
                                .segments
                                .iter()
                                .cloned()
                                .map(move |segment| (*data_id, segment))
                        })
                        .min_by_key(|(_, segment)| (segment.max_ts_ms, segment.segment_id))
                }
            };

            let Some((data_id, victim)) = maybe_victim else {
                break;
            };

            let path = segment_path(&self.config.data_dir, &victim)?;
            match fs::remove_file(&path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(TsdbError::Io(error)),
            }

            if let Some(parent) = path.parent() {
                if parent != self.config.data_dir.as_path() && is_dir_empty(parent)? {
                    let _ = fs::remove_dir(parent);
                }
            }

            let mut manifest = self.manifest.lock().unwrap();
            if let Some(entry) = manifest.series.get_mut(&data_id) {
                let old_len = entry.segments.len();
                entry
                    .segments
                    .retain(|segment| segment.segment_id != victim.segment_id);
                if entry.segments.len() != old_len {
                    manifest.storage_bytes =
                        manifest.storage_bytes.saturating_sub(victim.size_bytes);
                }
            }
            manifest.save(&self.config.data_dir)?;
        }
        Ok(())
    }

    fn series_handle(&self, data_id: DataId) -> Result<Arc<Mutex<SeriesState>>> {
        self.series
            .read()
            .unwrap()
            .get(&data_id)
            .cloned()
            .ok_or(TsdbError::SeriesNotFound(data_id))
    }

    fn flush_serieses(&self, data_ids: &[DataId]) -> Result<u32> {
        let mut pending = Vec::new();
        for &data_id in data_ids {
            if let Some(flush) = self.prepare_series_flush(data_id)? {
                pending.push(flush);
            }
        }

        if pending.is_empty() {
            return Ok(0);
        }

        {
            let mut manifest = self.manifest.lock().unwrap();
            for flush in &mut pending {
                let series_entry = manifest
                    .series
                    .get_mut(&flush.data_id)
                    .ok_or(TsdbError::SeriesNotFound(flush.data_id))?;
                flush.segment_id = series_entry.next_segment_id;
                series_entry.next_segment_id += 1;
            }
        }

        for flush in &mut pending {
            flush.meta = Some(write_segment_file(
                &self.config.data_dir,
                flush.data_id,
                flush.segment_id,
                flush.series_type,
                self.config.segment_compression,
                &flush.samples,
            )?);
        }

        {
            let mut manifest = self.manifest.lock().unwrap();
            for flush in &pending {
                let meta = flush
                    .meta
                    .clone()
                    .ok_or_else(|| TsdbError::Internal("missing flush metadata".to_string()))?;
                let series_entry = manifest
                    .series
                    .get_mut(&flush.data_id)
                    .ok_or(TsdbError::SeriesNotFound(flush.data_id))?;
                series_entry.segments.push(meta.clone());
                manifest.storage_bytes = manifest.storage_bytes.saturating_add(meta.size_bytes);
            }
            manifest.save(&self.config.data_dir)?;
        }

        for flush in &pending {
            let handle = self.series_handle(flush.data_id)?;
            let mut state = handle.lock().unwrap();
            state.flushed_max_ts = Some(
                state
                    .flushed_max_ts
                    .map_or(flush.max_ts_ms, |current| current.max(flush.max_ts_ms)),
            );
        }

        self.evict_if_needed()?;
        Ok(pending.len() as u32)
    }

    fn prepare_series_flush(&self, data_id: DataId) -> Result<Option<PendingFlush>> {
        let handle = self.series_handle(data_id)?;
        let (series_type, flushed_samples) = {
            let mut state = handle.lock().unwrap();
            if state.memtable.is_empty() {
                return Ok(None);
            }
            let memtable = std::mem::take(&mut state.memtable);
            state.mem_max_ts = None;
            (state.meta.series_type, prepare_flush_samples(memtable))
        };

        if flushed_samples.is_empty() {
            return Ok(None);
        }

        let max_ts_ms = flushed_samples
            .last()
            .map(|sample| sample.timestamp_ms)
            .ok_or_else(|| TsdbError::Internal("flush samples unexpectedly empty".to_string()))?;

        Ok(Some(PendingFlush {
            data_id,
            series_type,
            samples: flushed_samples,
            segment_id: 0,
            max_ts_ms,
            meta: None,
        }))
    }
}

fn append_to_series_state(state: &mut SeriesState, sample: Sample) -> Result<()> {
    let expected = state.meta.series_type;
    let actual = sample.value.series_type();
    if expected != actual {
        return Err(TsdbError::TypeMismatch {
            data_id: sample.data_id,
            expected,
            actual,
        });
    }

    if let Some(flushed_max_ts) = state.flushed_max_ts {
        if sample.timestamp_ms <= flushed_max_ts {
            return Err(TsdbError::TimestampTooOld {
                data_id: sample.data_id,
                timestamp_ms: sample.timestamp_ms,
                flushed_max_ts,
            });
        }
    }

    let seq_no = state.next_seq_no;
    state.next_seq_no += 1;
    state.mem_max_ts = Some(state.mem_max_ts.map_or(sample.timestamp_ms, |current| {
        current.max(sample.timestamp_ms)
    }));
    state.memtable.push(BufferedSample { seq_no, sample });
    Ok(())
}

#[derive(Debug)]
struct PendingFlush {
    data_id: DataId,
    series_type: SeriesType,
    samples: Vec<RangeSample>,
    segment_id: u64,
    max_ts_ms: i64,
    meta: Option<SegmentMeta>,
}

pub fn prepare_flush_samples(mut samples: Vec<BufferedSample>) -> Vec<RangeSample> {
    samples.sort_by_key(|buffered| (buffered.sample.timestamp_ms, buffered.seq_no));

    let mut deduped = Vec::with_capacity(samples.len());
    let mut iter = samples.into_iter().peekable();
    while let Some(current) = iter.next() {
        let mut last = current;
        while let Some(next) = iter.peek() {
            if next.sample.timestamp_ms != last.sample.timestamp_ms {
                break;
            }
            last = iter.next().unwrap();
        }
        deduped.push(RangeSample {
            timestamp_ms: last.sample.timestamp_ms,
            value: last.sample.value,
        });
    }
    deduped
}

fn merge_samples(
    disk_samples: Vec<RangeSample>,
    mem_samples: Vec<RangeSample>,
) -> Vec<RangeSample> {
    let mut combined = Vec::with_capacity(disk_samples.len() + mem_samples.len());
    let mut order = 0_usize;
    for sample in disk_samples {
        combined.push((sample.timestamp_ms, order, sample));
        order += 1;
    }
    for sample in mem_samples {
        combined.push((sample.timestamp_ms, order, sample));
        order += 1;
    }

    combined.sort_by_key(|(timestamp, order, _)| (*timestamp, *order));
    let mut deduped: Vec<RangeSample> = Vec::with_capacity(combined.len());
    for (_, _, sample) in combined {
        if let Some(last) = deduped.last_mut() {
            if last.timestamp_ms == sample.timestamp_ms {
                *last = sample;
                continue;
            }
        }
        deduped.push(sample);
    }
    deduped
}

fn recover_manifest(config: &EngineConfig) -> Result<Manifest> {
    cleanup_stale_tmp_files(&config.data_dir)?;

    let mut manifest = Manifest::load(&config.data_dir, config.max_storage_bytes)?;
    let disk_segments = scan_segments(&config.data_dir)?;

    let mut known_files = HashSet::new();
    for entry in manifest.series.values_mut() {
        entry.segments.retain(|segment| {
            let path = match resolve_segment_path(&config.data_dir, &segment.file) {
                Ok(path) => path,
                Err(_) => return false,
            };
            let exists = path.exists();
            if exists {
                known_files.insert(segment.file.clone());
            }
            exists
        });
    }

    for (data_id, segments) in disk_segments {
        let series_entry = manifest
            .series
            .entry(data_id)
            .or_insert_with(|| SeriesEntry {
                series_type: segments
                    .first()
                    .map(|(_, header, _)| header.series_type)
                    .unwrap_or(SeriesType::I64),
                next_segment_id: 1,
                segments: Vec::new(),
            });

        for (segment_id, header, size_bytes) in segments {
            let rel_path = format!("segments/{data_id}/{segment_id:012}.seg");
            if known_files.contains(&rel_path) {
                continue;
            }
            if series_entry.series_type != header.series_type {
                let _ = fs::remove_file(config.data_dir.join(&rel_path));
                continue;
            }
            series_entry.segments.push(SegmentMeta {
                segment_id,
                file: rel_path.clone(),
                min_ts_ms: header.min_ts_ms,
                max_ts_ms: header.max_ts_ms,
                count: header.count,
                size_bytes,
            });
            known_files.insert(rel_path);
        }
    }

    for entry in manifest.series.values_mut() {
        entry.segments.sort_by_key(|segment| segment.min_ts_ms);
        entry.next_segment_id = entry
            .segments
            .iter()
            .map(|segment| segment.segment_id)
            .max()
            .unwrap_or(0)
            + 1;
    }

    manifest.recompute_storage_bytes();
    manifest.save(&config.data_dir)?;
    Ok(manifest)
}

fn build_series_state(manifest: &Manifest) -> HashMap<DataId, Arc<Mutex<SeriesState>>> {
    manifest
        .series
        .iter()
        .map(|(data_id, entry)| {
            let flushed_max_ts = entry.segments.iter().map(|segment| segment.max_ts_ms).max();
            (
                *data_id,
                Arc::new(Mutex::new(SeriesState::new(
                    SeriesMeta {
                        data_id: *data_id,
                        series_type: entry.series_type,
                    },
                    flushed_max_ts,
                ))),
            )
        })
        .collect()
}

fn cleanup_stale_tmp_files(data_dir: &Path) -> Result<()> {
    fs::create_dir_all(data_dir.join("segments"))?;
    let manifest_tmp = manifest_tmp_path(data_dir);
    if manifest_tmp.exists() {
        fs::remove_file(manifest_tmp)?;
    }

    let segments_root = data_dir.join("segments");
    if !segments_root.exists() {
        return Ok(());
    }

    for data_dir_entry in fs::read_dir(&segments_root)? {
        let data_dir_entry = data_dir_entry?;
        let path = data_dir_entry.path();
        if !path.is_dir() {
            continue;
        }
        for entry in fs::read_dir(&path)? {
            let entry = entry?;
            let entry_path = entry.path();
            if entry_path
                .extension()
                .and_then(|ext| ext.to_str())
                .is_some_and(|ext| ext == "tmp")
            {
                fs::remove_file(entry_path)?;
            }
        }
    }

    Ok(())
}

fn scan_segments(data_dir: &Path) -> Result<HashMap<DataId, Vec<(u64, SegmentHeader, u64)>>> {
    let mut segments: HashMap<DataId, Vec<(u64, SegmentHeader, u64)>> = HashMap::new();
    let root = data_dir.join("segments");
    if !root.exists() {
        return Ok(segments);
    }

    for entry in fs::read_dir(&root)? {
        let entry = entry?;
        let series_dir = entry.path();
        if !series_dir.is_dir() {
            continue;
        }

        for file in fs::read_dir(&series_dir)? {
            let file = file?;
            let path = file.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("seg") {
                continue;
            }

            let segment_id = match path.file_stem().and_then(|stem| stem.to_str()) {
                Some(stem) => match stem.parse::<u64>() {
                    Ok(value) => value,
                    Err(_) => {
                        let _ = fs::remove_file(&path);
                        continue;
                    }
                },
                None => {
                    let _ = fs::remove_file(&path);
                    continue;
                }
            };

            match read_segment_header_only(&path) {
                Ok(header) => {
                    let size_bytes = fs::metadata(&path)?.len();
                    segments
                        .entry(header.data_id)
                        .or_default()
                        .push((segment_id, header, size_bytes));
                }
                Err(_) => {
                    let _ = fs::remove_file(&path);
                }
            }
        }
    }

    Ok(segments)
}

fn is_dir_empty(path: &Path) -> Result<bool> {
    Ok(fs::read_dir(path)?.next().is_none())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tempfile::TempDir;

    use super::*;
    use crate::core::manifest::{Manifest, SegmentMeta, SeriesEntry};
    use crate::core::types::Value;

    fn test_config(path: &Path) -> EngineConfig {
        EngineConfig {
            data_dir: path.to_path_buf(),
            flush_threshold_count: 3,
            max_storage_bytes: 10_000,
            segment_compression: SegmentCompressionCodec::Zstd,
        }
    }

    #[test]
    fn prepare_flush_samples_uses_last_write_wins() {
        let samples = vec![
            BufferedSample {
                seq_no: 0,
                sample: Sample {
                    data_id: 1,
                    timestamp_ms: 10,
                    value: Value::I64(1),
                },
            },
            BufferedSample {
                seq_no: 1,
                sample: Sample {
                    data_id: 1,
                    timestamp_ms: 10,
                    value: Value::I64(2),
                },
            },
            BufferedSample {
                seq_no: 2,
                sample: Sample {
                    data_id: 1,
                    timestamp_ms: 5,
                    value: Value::I64(9),
                },
            },
        ];

        let flushed = prepare_flush_samples(samples);
        assert_eq!(
            flushed,
            vec![
                RangeSample {
                    timestamp_ms: 5,
                    value: Value::I64(9)
                },
                RangeSample {
                    timestamp_ms: 10,
                    value: Value::I64(2)
                }
            ]
        );
    }

    #[test]
    fn append_rejects_old_timestamp_after_flush() {
        let tempdir = TempDir::new().unwrap();
        let engine = Engine::open(test_config(tempdir.path())).unwrap();
        engine.register_series(1, SeriesType::I64).unwrap();
        engine
            .append_one(Sample {
                data_id: 1,
                timestamp_ms: 10,
                value: Value::I64(100),
            })
            .unwrap();
        engine
            .append_one(Sample {
                data_id: 1,
                timestamp_ms: 20,
                value: Value::I64(200),
            })
            .unwrap();
        engine
            .append_one(Sample {
                data_id: 1,
                timestamp_ms: 30,
                value: Value::I64(300),
            })
            .unwrap();

        let error = engine
            .append_one(Sample {
                data_id: 1,
                timestamp_ms: 30,
                value: Value::I64(301),
            })
            .unwrap_err();
        assert!(matches!(error, TsdbError::TimestampTooOld { .. }));
    }

    #[test]
    fn query_merges_disk_and_memtable() {
        let tempdir = TempDir::new().unwrap();
        let mut config = test_config(tempdir.path());
        config.flush_threshold_count = 2;
        let engine = Engine::open(config).unwrap();
        engine.register_series(7, SeriesType::Bool).unwrap();
        engine
            .append_one(Sample {
                data_id: 7,
                timestamp_ms: 10,
                value: Value::Bool(true),
            })
            .unwrap();
        engine
            .append_one(Sample {
                data_id: 7,
                timestamp_ms: 20,
                value: Value::Bool(false),
            })
            .unwrap();
        engine
            .append_one(Sample {
                data_id: 7,
                timestamp_ms: 30,
                value: Value::Bool(true),
            })
            .unwrap();

        let result = engine
            .query_range(RangeQuery {
                data_id: 7,
                start_ts_ms: 0,
                end_ts_ms: 100,
                limit: None,
            })
            .unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result[0].timestamp_ms, 10);
        assert_eq!(result[2].timestamp_ms, 30);
    }

    #[test]
    fn evicts_oldest_segment_when_limit_is_exceeded() {
        let tempdir = TempDir::new().unwrap();
        let mut config = test_config(tempdir.path());
        config.flush_threshold_count = 1;
        config.max_storage_bytes = 80;
        let engine = Engine::open(config).unwrap();
        engine.register_series(1, SeriesType::I64).unwrap();

        engine
            .append_one(Sample {
                data_id: 1,
                timestamp_ms: 1,
                value: Value::I64(10),
            })
            .unwrap();
        engine
            .append_one(Sample {
                data_id: 1,
                timestamp_ms: 2,
                value: Value::I64(20),
            })
            .unwrap();

        let manifest = engine.manifest.lock().unwrap().clone();
        assert_eq!(manifest.total_segment_count(), 1);
        assert_eq!(
            manifest.storage_bytes,
            manifest.series.get(&1).unwrap().segments[0].size_bytes
        );
    }

    #[test]
    fn append_batch_accepts_valid_samples_and_flushes_once_per_series() {
        let tempdir = TempDir::new().unwrap();
        let mut config = test_config(tempdir.path());
        config.flush_threshold_count = 2;
        let engine = Engine::open(config).unwrap();
        engine.register_series(1, SeriesType::I64).unwrap();

        let result = engine.append_batch_detailed(vec![
            Sample {
                data_id: 1,
                timestamp_ms: 10,
                value: Value::I64(100),
            },
            Sample {
                data_id: 1,
                timestamp_ms: 11,
                value: Value::Bool(true),
            },
            Sample {
                data_id: 2,
                timestamp_ms: 12,
                value: Value::I64(200),
            },
            Sample {
                data_id: 1,
                timestamp_ms: 12,
                value: Value::I64(300),
            },
        ]);

        assert_eq!(result.accepted, 2);
        assert_eq!(result.rejected, 2);
        assert_eq!(result.errors.len(), 2);
        assert_eq!(result.errors[0].index, 1);
        assert_eq!(result.errors[1].index, 2);

        let stats = engine.stats();
        assert_eq!(stats.segment_count, 1);
        assert_eq!(stats.buffered_samples, 0);

        let rows = engine
            .query_range(RangeQuery {
                data_id: 1,
                start_ts_ms: 0,
                end_ts_ms: 100,
                limit: None,
            })
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].timestamp_ms, 10);
        assert_eq!(rows[1].timestamp_ms, 12);
    }

    #[test]
    fn open_discards_manifest_segments_that_escape_data_dir() {
        let tempdir = TempDir::new().unwrap();
        let victim_path = tempdir.path().join("victim.txt");
        fs::write(&victim_path, b"TOP_SECRET").unwrap();

        let mut manifest = Manifest::new(10_000);
        manifest.storage_bytes = 1024;
        manifest.series = HashMap::from([(
            1_u64,
            SeriesEntry {
                series_type: SeriesType::I64,
                next_segment_id: 2,
                segments: vec![SegmentMeta {
                    segment_id: 1,
                    file: "../victim.txt".to_string(),
                    min_ts_ms: 0,
                    max_ts_ms: 1,
                    count: 1,
                    size_bytes: 1024,
                }],
            },
        )]);
        manifest.save(tempdir.path()).unwrap();

        let engine = Engine::open(test_config(tempdir.path())).unwrap();
        let recovered_manifest = engine.manifest.lock().unwrap().clone();

        assert!(victim_path.exists());
        assert_eq!(recovered_manifest.total_segment_count(), 0);
        assert_eq!(recovered_manifest.storage_bytes, 0);
    }
}
