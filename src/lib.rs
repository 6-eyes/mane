mod config;

use std::{net::SocketAddr, path::PathBuf};
use tracing::Level;
use tracing_subscriber::EnvFilter;
use crate::config::{Error as ConfigError, Config,};

pub fn run() -> Result<(), Error> {
    // check rsync
    which::which("rsync").map_err(Error::RsyncNotFound)?;

    // initialize tracing
    init_tracing();

    // initialize config
    let (config, _debouncer) = match Config::init() {
        Ok(config) => config,
        Err(e) => return Err(Error::Config(e)),
    };

    tracing::debug!("loaded config {config:?}");
    // start tokio executor

    std::thread::sleep(std::time::Duration::MAX);
    todo!("start main executor");
}

fn init_tracing() {
    let env_filter = EnvFilter::try_from_env("LOG_FILTER").unwrap_or_else(|_| EnvFilter::new(Level::INFO.as_str()));
    tracing_subscriber::fmt().with_target(false).with_level(true).with_env_filter(env_filter).init();
}

/// Describes parameters for a synchronization operation.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Sync {
    local_path: PathBuf,
    remote_path: PathBuf,
    remote_addr: SocketAddr,
}

#[derive(Debug)]
pub enum Error {
    RsyncNotFound(which::Error),
    Config(ConfigError),
}