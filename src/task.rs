use std::{fmt::Display, net::SocketAddr, path::{Path, PathBuf}, time::Duration};
use notify::{Event as NotifyEvent, EventKind, RecursiveMode, Result as NotifyResult, Watcher, event::RemoveKind, recommended_watcher};
use tokio::{fs::try_exists, process::Command, sync::{self, mpsc}, time::sleep};
use crate::config::SyncConfig;

const POLL_INTERVAL: Duration = Duration::from_secs(3);

#[derive(Debug, Default)]
pub(crate) struct TaskPool(Vec<Task>);

impl TaskPool {
    pub(crate) fn update(&mut self, syncs: &[SyncConfig]) {
        // remove: tasks present in pool not part of sync config
        let (keep, remove) = std::mem::take(&mut self.0).into_iter().partition(|task| syncs.iter().any(|sync| task == sync));
        self.0 = keep;
        remove.into_iter().inspect(|task| tracing::info!(source = %task.local_path.display(), destination = %task.remote_path.display(), address = %task.remote_address, "removing task")).for_each(Task::kill);

        // add: syncs present in sync config not part of pool
        for sync_config in syncs {
            if self.0.iter().all(|task| task != sync_config) {
                tracing::info!(source = %sync_config.local_path.display(), destination = %sync_config.remote_path.display(), address = %sync_config.remote_address, "creating new task");
                match Task::new(sync_config) {
                    Ok(task) => {
                        tracing::debug!(source = %task.local_path.display(), destination = %task.remote_path.display(), address = %task.remote_address, "task created successfully");
                        self.0.push(task);
                    },
                    Err(e) => {
                        tracing::error!(error = %e, "error creating task");
                        continue;
                    },
                };
            }
            else {
                tracing::debug!(source = %sync_config.local_path.display(), destination = %sync_config.remote_path.display(), address = %sync_config.remote_address, "task already running");
            }
        }
    }
}

#[derive(Debug)]
struct Task {
    abort_tx: sync::oneshot::Sender<()>,
    local_path: PathBuf,
    remote_path: PathBuf,
    remote_address: SocketAddr,
}

impl Task {
    fn new(config: &SyncConfig) -> Result<Self, Error> {
        let SyncConfig { local_path, remote_path, remote_address } = config.clone();
        let Some(local_parent) = local_path.parent().map(|p| p.to_path_buf()) else {
            return Err(Error::NoParent);
        };

        // start watcher
        let (abort_tx, abort_rx) = sync::oneshot::channel();
        tokio::spawn(Self::spawn_watcher(local_parent.clone(), local_path.clone(), abort_rx));

        Ok(Self { abort_tx, local_path, remote_path, remote_address })
    }

    /// Spawns a watcher with following fallbacks:
    /// - If the parent directory is removed, we resort to polling the given path.
    async fn spawn_watcher(local_parent: PathBuf, local_path: PathBuf, mut abort_rx: sync::oneshot::Receiver<()>) {
        // reset watcher loop
        loop {
            // wait for the source directory to exist
            loop {
                match try_exists(&local_parent).await {
                    Ok(true) => break,
                    Ok(false) => tracing::warn!(source = %local_path.display(), "directory {} doesn't exist. will check after {} secs", local_parent.display(), POLL_INTERVAL.as_secs()),
                    Err(e) => tracing::error!(source = %local_path.display(), "unable to fetch metadata for directory {}. will check after {} secs. {}", local_parent.display(), POLL_INTERVAL.as_secs(), e),
                }
            }

            #[derive(Debug)]
            enum Event {
                ParentRemoved,
                Created,
            }

            impl Display for Event {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    match self {
                        Event::ParentRemoved => write!(f, "ParentRemoved"),
                        Event::Created => write!(f, "Created"),
                    }
                }
            }

            // create channel to capture events
            let (event_tx, mut event_rx) = mpsc::channel(1);

            let Ok(mut watcher) = recommended_watcher({
                let local_path = local_path.clone();
                let local_parent = local_parent.clone();

                move |res: NotifyResult<NotifyEvent>| {
                    let Ok(NotifyEvent { kind, paths, .. }) = res else {
                        tracing::error!(source = %local_path.display(), "error receiving notification event on path {}: {}", local_parent.display(), res.unwrap_err());
                        return;
                    };

                    if matches!(kind, EventKind::Remove(RemoveKind::Folder)) && paths.iter().any(|p| p == &local_parent) {
                        tracing::warn!(source = %local_path.display(), "parent directory {} deleted", local_parent.display());
                        // this needs to be sent
                        // it is okay o expect because we are blocking the send
                        if let Err(e) = event_tx.blocking_send(Event::ParentRemoved) {
                            tracing::info!(source = %local_path.display(), "unable to notify the removal to parent directory: {}. terminating.", e);
                            // todo: perform task abort
                            std::process::exit(1);
                        }

                        return;
                    }

                    tracing::debug!(source = %local_path.display(), "received event kind: {kind:?}, paths: {paths:?}");
                    // return if the path doesn't contain `local_path`
                    if paths.iter().all(|p| ! p.starts_with(&local_path)) {
                        tracing::debug!(source = %local_path.display(), "event {:?} not applicable", kind);
                        return;
                    }
                }
            }) else {
                tracing::error!(source = %local_path.display(), "unable to create a new watcher. will try after {} secs", POLL_INTERVAL.as_secs());
                sleep(POLL_INTERVAL).await;
                continue;
            };

            tracing::debug!(source = %local_path.display(), "initializing watcher over directory {}", local_parent.display());
            if let Err(e) = watcher.watch(&local_parent, RecursiveMode::Recursive) {
                tracing::error!(source = %local_path.display(), "unable to initialize watcher over the directory {}. will try after {} secs: {}", local_parent.display(), POLL_INTERVAL.as_secs(), e);
                sleep(POLL_INTERVAL).await;
                continue;
            }

            loop {
                tokio::select! {
                    biased;
                    res = &mut abort_rx => {
                        match res {
                            Ok(()) => tracing::info!(source = %local_path.display(), "received task abort signal."),
                            Err(e) => tracing::error!(source = %local_path.display(), error = %e, "the abort sender dropped. aborting."),
                        }
                        return;
                    },
                    maybe_event = event_rx.recv() =>  match maybe_event {
                        None | Some(Event::ParentRemoved) => break,
                        Some(event) => {
                            tracing::info!("received event {}", event);
                        },
                    },
                }
            }
        }

    }

    /// Kills a task.
    fn kill(self) {
        match self.abort_tx.send(()) {
            Ok(()) => tracing::debug!("sending abort signal"),
            Err(()) => tracing::debug!("unable to send abort signal"),
        }
    }

    /// We won't be using `--partial` because we don't want to store partial files on the destination
    /// We won't be using `--progress` since it is irrelevant for our usecase
    fn rsync(source: impl AsRef<Path>, destination: impl AsRef<Path>, destination_user: impl AsRef<str>, destination_address: SocketAddr) -> Command {
        let mut command = Command::new("rsync");
        command.kill_on_drop(true);
        command.arg("-avz");
        command.arg("--delete");
        // check port
        if destination_address.port() != 22 {
            command.arg(format!("-e \"ssh -p {}\"", destination_address.port()));
        }

        command.arg(source.as_ref());
        command.arg(format!("{}@{}:{}", destination_user.as_ref(), destination_address, destination.as_ref().display()));

        command
    }
}

impl PartialEq<SyncConfig> for Task {
    fn eq(&self, other: &SyncConfig) -> bool {
        self.local_path == other.local_path && self.remote_path == other.remote_path && self.remote_address == other.remote_address
    }
}

#[derive(Debug)]
enum Error {
    NoParent,
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::NoParent => write!(f, "no parent found for local path"),
        }
    }
}
