mod config;
mod task;

use tracing::Level;
use tracing_subscriber::EnvFilter;
use crate::{config::Config, task::TaskPool};

const NAME: &str = env!("CARGO_PKG_NAME");
const VERSION: &str = env!("CARGO_PKG_VERSION");

pub fn run() {
    // initialize tracing
    init_tracing();
    tracing::info!("starting {}, version: {}, pid: {}", NAME, VERSION, std::process::id());

    // check rsync
    let Ok(rsync_path) = which::which("rsync") else {
        tracing::error!("unable to find rsync in path");
        std::process::exit(1);
    };

    tracing::debug!("rsync path: {}", rsync_path.display());

    let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
    runtime.block_on(async move {
        // initialize config
        let mut watcher = match Config::init() {
            Ok(rx) => rx,
            Err(e) => {
                tracing::error!("error creating config: {e}");
                std::process::exit(1);
           },
        };

        let mut task_pool = TaskPool::default();

        // listen on watcher
        while let Ok(()) = watcher.changed().await {
            tracing::info!("new config: {:?}", watcher.borrow());
            task_pool.update(watcher.borrow().syncs());
        }
    })
}

fn init_tracing() {
    let env_filter = EnvFilter::try_from_env("LOG_FILTER").unwrap_or_else(|_| EnvFilter::new(Level::INFO.as_str()));
    tracing_subscriber::fmt().with_target(false).with_level(true).with_env_filter(env_filter).init();
}
