use std::sync::Arc;

use object_store::ObjectStore;
use object_store_linux_fs::{LinuxFS, LinuxFSOptions};

use crate::io_debugger::{DebugIOLayer, order::Matcher};

mod fsync;
mod interrupt;
mod invalid_inputs;
mod race;
mod snapshots;
mod syscall_semantics;

struct Fixture {
    tempdir: tempfile::TempDir,
    debugger: Arc<DebugIOLayer>,
    storage: LinuxFS<DebugIOLayer>,
}

impl Fixture {
    async fn new<const N: usize, F>(order: [Matcher; N], f: F) -> Self
    where
        F: for<'a> AsyncFnOnce(&'a dyn ObjectStore),
    {
        Self::new_with_options(LinuxFSOptions::default(), order, f).await
    }

    async fn new_with_options<const N: usize, F>(
        options: LinuxFSOptions,
        order: [Matcher; N],
        f: F,
    ) -> Self
    where
        F: for<'a> AsyncFnOnce(&'a dyn ObjectStore),
    {
        let tempdir = tempfile::tempdir().unwrap();
        let debugger = Arc::new(DebugIOLayer::new().with_order(order.into()));
        let storage = LinuxFS::<DebugIOLayer>::new(
            tempdir.path().to_path_buf(),
            options.with_io(Arc::clone(&debugger)),
        )
        .await
        .unwrap();

        f(&storage).await;
        debugger.wipe_log();

        Self {
            tempdir,
            debugger,
            storage,
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        println!("{}", self.debugger.log());
    }
}
