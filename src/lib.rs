mod config;

use tracing::Level;
use tracing_subscriber::EnvFilter;
use crate::config::{Error as ConfigError, Config,};

const NAME: &str = env!("CARGO_PKG_NAME");
const VERSION: &str = env!("CARGO_PKG_VERSION");

pub fn run() -> Result<(), Error> {
    // check rsync
    which::which("rsync").map_err(Error::RsyncNotFound)?;

    // initialize tracing
    init_tracing();
    tracing::info!("starting {}, version: {}, pid: {}", NAME, VERSION, std::process::id());

    // initialize config
    let watcher = match Config::init() {
        Ok(config) => config,
        Err(e) => {
            tracing::error!("error creating config: {e}");
            return Err(Error::Config(e))
        },
    };

    // start tokio executor

    std::thread::sleep(std::time::Duration::MAX);
    todo!("start main executor");
}

fn init_tracing() {
    let env_filter = EnvFilter::try_from_env("LOG_FILTER").unwrap_or_else(|_| EnvFilter::new(Level::INFO.as_str()));
    tracing_subscriber::fmt().with_target(false).with_level(true).with_env_filter(env_filter).init();
}

#[derive(Debug)]
pub enum Error {
    RsyncNotFound(which::Error),
    Config(ConfigError),
}