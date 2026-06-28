//! configuration for the mane synchronization tool.

use std::{env::{args, home_dir}, fmt::Display, net::SocketAddr, path::PathBuf, time::Duration};
use notify::{Event as NotifyEvent, EventKind, Result as NotifyResult, Watcher, event::{DataChange, ModifyKind, RemoveKind}, recommended_watcher};
use tokio::{fs::try_exists, sync::{mpsc, watch::{self, Receiver}}, time::sleep};

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
    /// ## Panics
    /// If not called within a tokio runtime.
    pub(crate) fn init() -> Result<Receiver<Self>, Error> {
        // fetch config path
        let mut args = args().skip(1);
        let config_path = match args.next() {
            Some(arg) if arg == "-c" || arg == "--config" => args.next().ok_or(Error::InvalidArg(None))?.into(),
            Some(arg) => return Err(Error::InvalidArg(Some(arg))),
            None => home_dir().ok_or(Error::HomeDirNotFound)?.join(DEFAULT_CONFIG_PATH),
        };

        let config_dir = config_path.parent().ok_or_else(|| Error::ParentNotFound(config_path.clone()))?.to_path_buf();
        tracing::info!("using configuration path: {}", config_path.display());

        let (tx, rx) = watch::channel(Self::default());
        // thread to listen to watcher reset requests
        tokio::spawn(async move {
            loop {
                // wait for the config directory to exist
                loop {
                    match try_exists(&config_dir).await {
                        Ok(true) => break,
                        Ok(false) => tracing::warn!("directory {} doesn't exist. will check after {} secs", config_dir.display(), POLL_INTERVAL.as_secs()),
                        Err(e) => tracing::error!("unable to fetch metadata for directory {}. will check after {} secs. {}", config_dir.display(), POLL_INTERVAL.as_secs(), e),
                    }
                    sleep(POLL_INTERVAL).await;
                }

                // create watcher
                /// Describes the set of events of interest from the `inotfy` on linux target.
                #[derive(Debug)]
                enum Event {
                    /// Event signifies that the parent directory is removed. Recreate the watcher once the directory exists.
                    ParentRemoved,
                    /// Event signifying that the configuration has changed.
                    ConfigChanged,
                }

                // create channel to capture events
                let (event_tx, mut event_rx) = mpsc::channel(1);

                tracing::debug!("creating new watcher");
                let Ok(mut watcher) = recommended_watcher({
                    let config_path = config_path.clone();
                    let config_dir = config_dir.clone();

                    move |res: NotifyResult<NotifyEvent>| {
                        let Ok(NotifyEvent { kind, paths, .. }) = res else { return };
                        tracing::debug!("received event kind: {kind:?}, paths: {paths:?}");
                        if matches!(kind, EventKind::Remove(RemoveKind::Folder)) && paths.iter().any(|p| p == &config_dir) {
                            tracing::warn!("config directory {} deleted", config_dir.display());
                            // this needs to be sent
                            // it is okay to expect because we are blocking the send
                            if let Err(e) = event_tx.blocking_send(Event::ParentRemoved) {
                                tracing::info!("unable to notify the removal to parent directory: {}. terminating.", e);
                                std::process::exit(1);
                            }

                            return;
                        }

                        if matches!(kind, EventKind::Modify(ModifyKind::Data(DataChange::Any))) && paths.iter().any(|p| p == &config_path) && event_tx.try_send(Event::ConfigChanged).is_err() {
                            tracing::error!("unable to send config updates");
                        }
                    }
                }) else {
                    tracing::error!("unable to create a new watcher. will try after {} secs", POLL_INTERVAL.as_secs());
                    sleep(POLL_INTERVAL).await;
                    continue;
                };

                tracing::debug!("initializing watcher over directory {}", config_dir.display());
                if let Err(e) = watcher.watch(&config_dir, notify::RecursiveMode::NonRecursive) {
                    tracing::error!("unable to initialize watcher over the directory {}. will try after {} secs: {}", config_dir.display(), POLL_INTERVAL.as_secs(), e);
                    sleep(POLL_INTERVAL).await;
                    continue;
                }

                tracing::info!("watcher on directory {} initialized", config_dir.display());

                // watcher is kept alive until the while loop exits
                while let Some(event) = event_rx.recv().await {
                    match event {
                        Event::ParentRemoved => {
                            tracing::debug!("dropping watcher");
                            break;
                        },
                        Event::ConfigChanged => {
                            // fetch contents
                            let contents = match tokio::fs::read_to_string(&config_path).await {
                                Ok(c) => c,
                                Err(e) => {
                                    tracing::error!("unable to read contents of config file {}: {}", config_path.display(), e);
                                    continue;
                                },
                            };

                            // parse
                            let new = match toml::from_str::<Self>(&contents) {
                                Ok(c) => c,
                                Err(e) => {
                                    tracing::error!("error parsing config file {}: {}", config_path.display(), e);
                                    continue;
                                },
                            };

                            // validate
                            if new.syncs.is_empty() {
                                tracing::warn!("no syncs to configure");
                                continue;
                            }

                            tracing::info!("config changed");
                            tx.send_if_modified(|config| {
                                if config.syncs != new.syncs {
                                    *config = new;
                                    true
                                }
                                else {
                                    false
                                }
                            });
                        },
                    }
                }
            }
        });

        Ok(rx)
    }
}

/// Describes parameters for a synchronization operation.
#[derive(Debug, serde::Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct SyncConfig {
    local_path: PathBuf,
    remote_path: PathBuf,
    remote_addr: SocketAddr,
}

#[derive(Debug)]
pub(crate) enum Error {
    InvalidArg(Option<String>),
    HomeDirNotFound,
    ParentNotFound(PathBuf),
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidArg(Some(arg)) => write!(f, "invalid argument passed: {arg}"),
            Self::InvalidArg(None) => write!(f, "no argument passed"),
            Self::HomeDirNotFound => write!(f, "unable to determine home directory"),
            Self::ParentNotFound(path) => write!(f, "error determining parent path for {}", path.display()),
        }
    }
}
