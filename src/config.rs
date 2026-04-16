use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::error::{Result, TsdbError};

pub const DEFAULT_CONFIG_PATH: &str = "drift-ts.toml";

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub listen_addr: String,
    pub data_dir: PathBuf,
    pub flush_threshold_count: usize,
    pub max_storage_bytes: u64,
}

impl AppConfig {
    pub fn load_from_cli() -> Result<Self> {
        let mut args = env::args().skip(1);
        let mut config_path: Option<PathBuf> = None;

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--config" => {
                    let value = args.next().ok_or_else(|| {
                        TsdbError::Config("missing path after --config".to_string())
                    })?;
                    config_path = Some(PathBuf::from(value));
                }
                other => {
                    return Err(TsdbError::Config(format!(
                        "unsupported argument: {other}. Only --config <path> is supported"
                    )));
                }
            }
        }

        let path = config_path.unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_PATH));
        Self::load_from_path(path)
    }

    pub fn load_from_path(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            return Err(TsdbError::Config(format!(
                "config file not found: {}",
                path.display()
            )));
        }

        let raw = fs::read_to_string(path)?;
        let config: AppConfig = toml::from_str(&raw)?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        if self.listen_addr.trim().is_empty() {
            return Err(TsdbError::Config(
                "listen_addr must not be empty".to_string(),
            ));
        }
        if self.flush_threshold_count == 0 {
            return Err(TsdbError::Config(
                "flush_threshold_count must be greater than zero".to_string(),
            ));
        }
        if self.max_storage_bytes == 0 {
            return Err(TsdbError::Config(
                "max_storage_bytes must be greater than zero".to_string(),
            ));
        }
        Ok(())
    }
}
