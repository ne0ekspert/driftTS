use std::collections::HashMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::core::types::{DataId, SeriesType};
use crate::error::{Result, TsdbError};

pub const MANIFEST_VERSION: u32 = 2;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub version: u32,
    pub series: HashMap<DataId, SeriesEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeriesEntry {
    pub series_type: SeriesType,
    pub next_segment_id: u64,
    #[serde(default)]
    pub max_storage_bytes: Option<u64>,
    #[serde(default)]
    pub storage_bytes: u64,
    pub segments: Vec<SegmentMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SegmentMeta {
    pub segment_id: u64,
    pub file: String,
    pub min_ts_ms: i64,
    pub max_ts_ms: i64,
    pub count: u32,
    pub size_bytes: u64,
}

impl Manifest {
    pub fn new() -> Self {
        Self {
            version: MANIFEST_VERSION,
            series: HashMap::new(),
        }
    }

    pub fn load(data_dir: &Path) -> Result<Self> {
        let path = manifest_path(data_dir);
        if !path.exists() {
            return Ok(Self::new());
        }

        let raw = fs::read_to_string(&path)?;
        let mut manifest: Manifest = serde_json::from_str(&raw)?;
        manifest.version = MANIFEST_VERSION;
        Ok(manifest)
    }

    pub fn save(&self, data_dir: &Path) -> Result<()> {
        fs::create_dir_all(data_dir)?;
        let path = manifest_path(data_dir);
        let tmp = manifest_tmp_path(data_dir);
        let payload = serde_json::to_vec(self)?;
        fs::write(&tmp, payload)?;
        File::open(&tmp)?.sync_all()?;
        fs::rename(&tmp, &path)?;
        fsync_dir(data_dir)?;
        Ok(())
    }

    pub fn recompute_storage_bytes(&mut self) {
        for entry in self.series.values_mut() {
            entry.storage_bytes = entry
                .segments
                .iter()
                .map(|segment| segment.size_bytes)
                .sum();
        }
    }

    pub fn total_storage_bytes(&self) -> u64 {
        self.series.values().map(|entry| entry.storage_bytes).sum()
    }

    pub fn total_segment_count(&self) -> usize {
        self.series.values().map(|entry| entry.segments.len()).sum()
    }
}

pub fn manifest_path(data_dir: &Path) -> PathBuf {
    data_dir.join("manifest.json")
}

pub fn manifest_tmp_path(data_dir: &Path) -> PathBuf {
    data_dir.join("manifest.json.tmp")
}

pub fn fsync_dir(path: &Path) -> Result<()> {
    File::open(path)
        .map_err(|err| TsdbError::Io(err))?
        .sync_all()
        .map_err(TsdbError::Io)
}
