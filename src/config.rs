use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::core::segment::SegmentCompressionCodec;
use crate::error::{Result, TsdbError};

pub const DEFAULT_CONFIG_PATH: &str = "drift-ts.toml";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListenEndpoint {
    Tcp { host: String, port: u16 },
    Unix(PathBuf),
}

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub listen_addr: String,
    pub listen_port: Option<u16>,
    pub data_dir: PathBuf,
    pub flush_threshold_count: usize,
    pub max_storage_bytes: u64,
    #[serde(default = "default_segment_compression")]
    pub segment_compression: SegmentCompressionCodec,
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

    pub fn listen_endpoint(&self) -> Result<ListenEndpoint> {
        let value = self.listen_addr.trim();

        if let Some(port) = self.listen_port {
            if looks_like_unix_socket_path(value) {
                return Err(TsdbError::Config(format!(
                    "listen_addr must be a TCP host or IP address when listen_port is set, got path-like value: {value}"
                )));
            }

            return Ok(ListenEndpoint::Tcp {
                host: value.to_string(),
                port,
            });
        }

        if looks_like_unix_socket_path(value) {
            return Ok(ListenEndpoint::Unix(PathBuf::from(value)));
        }

        Err(TsdbError::Config(format!(
            "listen_addr must be a Unix socket path when listen_port is omitted, or a TCP host/IP when listen_port is set, got: {value}"
        )))
    }
}

fn default_segment_compression() -> SegmentCompressionCodec {
    SegmentCompressionCodec::default()
}

fn looks_like_unix_socket_path(value: &str) -> bool {
    value.contains(std::path::MAIN_SEPARATOR) || value.starts_with('.') || value.ends_with(".sock")
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    fn write_config(tempdir: &TempDir, body: &str) -> PathBuf {
        let path = tempdir.path().join("drift-ts.toml");
        fs::write(&path, body).unwrap();
        path
    }

    #[test]
    fn defaults_segment_compression_to_zstd() {
        let tempdir = TempDir::new().unwrap();
        let path = write_config(
            &tempdir,
            r#"
listen_addr = "127.0.0.1"
listen_port = 50051
data_dir = "data"
flush_threshold_count = 1000
max_storage_bytes = 104857600
"#,
        );

        let config = AppConfig::load_from_path(path).unwrap();
        assert_eq!(config.segment_compression, SegmentCompressionCodec::Zstd);
    }

    #[test]
    fn parses_explicit_segment_compression() {
        let tempdir = TempDir::new().unwrap();
        let path = write_config(
            &tempdir,
            r#"
listen_addr = "127.0.0.1"
listen_port = 50051
data_dir = "data"
flush_threshold_count = 1000
max_storage_bytes = 104857600
segment_compression = "none"
"#,
        );

        let config = AppConfig::load_from_path(path).unwrap();
        assert_eq!(config.segment_compression, SegmentCompressionCodec::None);
    }

    #[test]
    fn parses_tcp_listen_endpoint() {
        let tempdir = TempDir::new().unwrap();
        let path = write_config(
            &tempdir,
            r#"
listen_addr = "127.0.0.1"
listen_port = 50051
data_dir = "data"
flush_threshold_count = 1000
max_storage_bytes = 104857600
"#,
        );

        let config = AppConfig::load_from_path(path).unwrap();
        assert_eq!(
            config.listen_endpoint().unwrap(),
            ListenEndpoint::Tcp {
                host: "127.0.0.1".to_string(),
                port: 50051
            }
        );
    }

    #[test]
    fn parses_hostname_tcp_listen_endpoint() {
        let tempdir = TempDir::new().unwrap();
        let path = write_config(
            &tempdir,
            r#"
listen_addr = "localhost"
listen_port = 50051
data_dir = "data"
flush_threshold_count = 1000
max_storage_bytes = 104857600
"#,
        );

        let config = AppConfig::load_from_path(path).unwrap();
        assert_eq!(
            config.listen_endpoint().unwrap(),
            ListenEndpoint::Tcp {
                host: "localhost".to_string(),
                port: 50051
            }
        );
    }

    #[test]
    fn parses_unix_socket_listen_endpoint() {
        let tempdir = TempDir::new().unwrap();
        let path = write_config(
            &tempdir,
            r#"
listen_addr = "./run/drift-ts.sock"
data_dir = "data"
flush_threshold_count = 1000
max_storage_bytes = 104857600
"#,
        );

        let config = AppConfig::load_from_path(path).unwrap();
        assert_eq!(
            config.listen_endpoint().unwrap(),
            ListenEndpoint::Unix(PathBuf::from("./run/drift-ts.sock"))
        );
    }

    #[test]
    fn rejects_invalid_listen_endpoint() {
        let tempdir = TempDir::new().unwrap();
        let path = write_config(
            &tempdir,
            r#"
listen_addr = "localhost"
data_dir = "data"
flush_threshold_count = 1000
max_storage_bytes = 104857600
"#,
        );

        let config = AppConfig::load_from_path(path).unwrap();
        let err = config.listen_endpoint().unwrap_err();
        assert!(
            err.to_string()
                .contains("listen_addr must be a Unix socket path when listen_port is omitted")
        );
    }

    #[test]
    fn rejects_socket_addr_syntax_without_listen_port() {
        let tempdir = TempDir::new().unwrap();
        let path = write_config(
            &tempdir,
            r#"
listen_addr = "127.0.0.1:50051"
data_dir = "data"
flush_threshold_count = 1000
max_storage_bytes = 104857600
"#,
        );

        let config = AppConfig::load_from_path(path).unwrap();
        let err = config.listen_endpoint().unwrap_err();
        assert!(
            err.to_string()
                .contains("listen_addr must be a Unix socket path when listen_port is omitted")
        );
    }

    #[test]
    fn rejects_path_like_tcp_host_when_port_is_set() {
        let tempdir = TempDir::new().unwrap();
        let path = write_config(
            &tempdir,
            r#"
listen_addr = "/tmp/drift-ts.sock"
listen_port = 50051
data_dir = "data"
flush_threshold_count = 1000
max_storage_bytes = 104857600
"#,
        );

        let config = AppConfig::load_from_path(path).unwrap();
        let err = config.listen_endpoint().unwrap_err();
        assert!(
            err.to_string()
                .contains("listen_addr must be a TCP host or IP address when listen_port is set")
        );
    }
}
