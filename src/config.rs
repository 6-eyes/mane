//! configuration for the mane synchronization tool.

use std::{env::{args, home_dir}, fmt::Display, io::Error as IoError, path::Path, sync::{Arc, RwLock}, thread::sleep};
use notify_debouncer_mini::{Config as DebouncerConfig, Debouncer, notify::{INotifyWatcher, RecursiveMode, Error as NotifyError}};
use toml::de::Error as TomlError;
use crate::Sync;

// constants
/// The debounce time for reading the configuration file.
const CONFIG_DEBOUNCE: std::time::Duration = std::time::Duration::from_secs(2);
/// The default path to the configuration file relative to the home directory.
const DEFAULT_CONFIG_PATH: &str = ".config/mane/config.toml";
/// The poll interval until the configuration file is initialized.
const CONFIG_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(2);

#[derive(Debug, serde::Deserialize)]
pub(crate) struct Config {
    #[serde(default)]
    syncs: Vec<Sync>,
}

impl Config {
    pub(crate) fn init() -> Result<(Arc<RwLock<Self>>, Debouncer<INotifyWatcher>), Error> {
        // fetch config path
        let mut args = args().skip(1);
        let path = match args.next() {
            Some(arg) if arg == "-c" || arg == "--config" => args.next().ok_or(Error::InvalidArg(None))?.into(),
            Some(arg) => return Err(Error::InvalidArg(Some(arg))),
            None => home_dir().ok_or(Error::HomeDirNotFound)?.join(DEFAULT_CONFIG_PATH),
        };

        /// Reads the config from the given path.
        fn load(path: impl AsRef<Path>) -> Result<Config, Error> {
            let s = std::fs::read_to_string(path)?;

            toml::from_str(&s).map_err(Error::InvalidConfig)
        }

        let config = {
            // wait for file to initialize
            let config = loop {
                match load(&path) {
                    Ok(config) => break config,
                    Err(e) => {
                        tracing::error!("error loading config {}: {}. will retry in {}s", path.display(), e, CONFIG_POLL_INTERVAL.as_secs());
                        sleep(CONFIG_POLL_INTERVAL);
                    }
                }
            };

            Arc::new(RwLock::new(config))
        };

        // create debouncer
        let mut debouncer = {
            let config = Arc::clone(&config);
            let path = path.clone();
            let notify_config = notify_debouncer_mini::notify::Config::default().with_follow_symlinks(false);
            let debouncer_config = DebouncerConfig::default().with_timeout(CONFIG_DEBOUNCE).with_notify_config(notify_config);

            notify_debouncer_mini::new_debouncer_opt(debouncer_config, move |res| {
                if let Err(e) = res {
                    tracing::error!("error fetching debouncer events: {}", e);
                    return;
                }

                match load(&path) {
                    // this runs on a background thread
                    Ok(new_config) => *config.write().expect("config lock poisoned") = new_config,
                    Err(e) => tracing::error!("error loading config: {}", e),
                }

                tracing::info!("config reloaded");
                tracing::debug!("new config: {:?}", config.read().expect("config lock poisoned"));
            }).map_err(Error::DebouncerInit)?
        };

        // start watching
        let parent = path.parent().ok_or(IoError::other(format!("unable to determine parent path for {}", path.display())))?;
        debouncer.watcher().watch(parent, RecursiveMode::Recursive).map_err(Error::WatcherInit)?;

        tracing::info!("watcher on file {} initialized", path.display());

        Ok((config, debouncer))
    }
}

#[derive(Debug)]
pub(crate) enum Error {
    HomeDirNotFound,
    InvalidArg(Option<String>),
    InvalidConfig(TomlError),
    DebouncerInit(NotifyError),
    WatcherInit(NotifyError),
    Io(IoError),
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HomeDirNotFound => write!(f, "home directory not found"),
            Self::InvalidArg(None) => write!(f, "no argument passed"),
            Self::InvalidArg(Some(arg)) => write!(f, "invalid argument: {arg:?}"),
            Self::InvalidConfig(e) => write!(f, "invalid config: {e}"),
            Self::DebouncerInit(e) => write!(f, "error initializing debouncer: {e}"),
            Self::WatcherInit(e) => write!(f, "error initializing watcher: {e}"),
            Self::Io(e) => write!(f, "io error: {}", e),
        }
    }
}

impl From<IoError> for Error {
    fn from(e: IoError) -> Self {
        Self::Io(e)
    }
}
