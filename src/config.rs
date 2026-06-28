//! configuration for the mane synchronization tool.

use std::{env::{args, home_dir}, fmt::Display, fs::read_to_string, io::Error as IoError, net::SocketAddr, path::PathBuf, sync::mpsc::{self, Receiver}, thread::{self, sleep}, time::Duration};
use notify::{Event as NotifyEvent, EventKind, Result as NotifyResult, Watcher, event::{DataChange, ModifyKind, RemoveKind}, recommended_watcher};
use toml::de::Error as TomlError;

// constants
/// default config path
const DEFAULT_CONFIG_PATH: &str = ".config/mane/config.toml";
/// the configuration polling interval
const POLL_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Debug, Default, serde::Deserialize)]
pub(crate) struct Config {
    #[serde(default)]
    syncs: Vec<SyncConfig>,
}

impl Config {
    pub(crate) fn init() -> Result<Receiver<Self>, Error> {
        // fetch config path
        let mut args = args().skip(1);
        let config_path = match args.next() {
            Some(arg) if arg == "-c" || arg == "--config" => args.next().ok_or(Error::InvalidArg(None))?.into(),
            Some(arg) => return Err(Error::InvalidArg(Some(arg))),
            None => home_dir().ok_or(Error::HomeDirNotFound)?.join(DEFAULT_CONFIG_PATH),
        };

        let config_dir = config_path.parent().expect("unable to determine parent").to_path_buf();
        tracing::info!("using configuration path: {}", config_path.display());

        let (tx, rx) = mpsc::sync_channel::<Self>(1);
        // thread to listen to watcher reset requests
        thread::spawn(move || {
            loop {
                // wait for the config directory to exist
                while ! config_dir.exists() {
                    tracing::warn!("directory {} doesn't exist. will check after {} secs", config_dir.display(), POLL_INTERVAL.as_secs());
                    sleep(POLL_INTERVAL);
                }

                // create watcher
                /// Describes the set of events of interest from the `inotfy` on linux target.
                #[derive(Debug)]
                enum Event {
                    ParentRemoved,
                    ConfigChanged(Config),
                }

                impl From<Config> for Event {
                    fn from(value: Config) -> Self {
                        Self::ConfigChanged(value)
                    }
                }

                // create channel to capture events
                let (event_tx, event_rx) = mpsc::sync_channel(1);

                tracing::debug!("creating new watcher");
                let mut watcher = recommended_watcher({
                    let config_path = config_path.clone();
                    let config_dir = config_dir.clone();

                    move |res: NotifyResult<NotifyEvent>| {
                        let Ok(NotifyEvent { kind, paths, .. }) = res else { return };
                        tracing::debug!("received event kind: {kind:?}, paths: {paths:?}");
                        if matches!(kind, EventKind::Remove(RemoveKind::Folder)) && paths.iter().any(|p| p == &config_dir) {
                            tracing::warn!("config directory {} deleted", config_dir.display());
                            event_tx.send(Event::ParentRemoved).expect("unable to notify watcher reset");
                            return;
                        }

                        if matches!(kind, EventKind::Modify(ModifyKind::Data(DataChange::Any))) && paths.iter().any(|p| p == &config_path) {
                            match read_to_string(&config_path) {
                                Ok(contents) => match toml::from_str::<Self>(&contents) {
                                    Ok(config) => {
                                        // vaidate
                                        if config.syncs.is_empty() {
                                            tracing::warn!("no syncs found in configuration");
                                            return;
                                        }

                                        // send
                                        if event_tx.try_send(Event::ConfigChanged(config)).is_err() {
                                            tracing::error!("unable to send config updates");
                                        }

                                        tracing::info!("config updated");
                                    }
                                    Err(e) => tracing::error!("error parsing config file {}: {}", config_path.display(), e),
                                },
                                Err(e) => tracing::error!("unable to read contents of config file {}: {}", config_path.display(), e),
                            }
                        }
                    }
                }).expect("unable to create watcher");

                tracing::debug!("initializing watcher over directory {}", config_dir.display());
                watcher.watch(&config_dir, notify::RecursiveMode::NonRecursive).expect("unable to start watcher");

                tracing::info!("watcher on directory {} initialized", config_dir.display());

                // watcher is kept alive until the while loop exits
                while let Ok(event) = event_rx.recv() {
                    match event {
                        Event::ParentRemoved => {
                            tracing::debug!("dropping watcher");
                            break;
                        },
                        Event::ConfigChanged(config) => {
                            tracing::info!("config changed");
                            tx.try_send(config).unwrap();
                        },
                    }
                }
            }
        });

        Ok(rx)
    }
}

/// Describes parameters for a synchronization operation.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SyncConfig {
    local_path: PathBuf,
    remote_path: PathBuf,
    remote_addr: SocketAddr,
}

#[derive(Debug)]
pub(crate) enum Error {
    HomeDirNotFound,
    InvalidArg(Option<String>),
    InvalidConfig(TomlError),
    Create(IoError),
    Read(IoError),
    Parent(PathBuf),
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HomeDirNotFound => write!(f, "home directory not found"),
            Self::InvalidArg(None) => write!(f, "no argument passed"),
            Self::InvalidArg(Some(arg)) => write!(f, "invalid argument: {arg:?}"),
            Self::InvalidConfig(e) => write!(f, "invalid config: {e}"),
            Self::Create(e) => write!(f, "error creating config file: {}", e),
            Self::Read(e) => write!(f, "error reading from config file: {}", e),
            Self::Parent(path) => write!(f, "error determining parent path for {}", path.display()),
        }
    }
}
