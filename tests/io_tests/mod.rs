use std::{ops::Deref, sync::Arc};

use object_store::ObjectStore;
use object_store_linux_fs::{
    LinuxFS, LinuxFSOptions,
    io::{RawIOLayer, RetryIntrLayer},
};

use crate::io_debugger::{DebugIOLayer, order::Matcher};

mod fsync;
mod interrupt;
mod invalid_inputs;
mod race;
mod snapshots;
mod syscall_semantics;

type IOLayers = RetryIntrLayer<DebugIOLayer<RawIOLayer>>;

struct Fixture {
    tempdir: tempfile::TempDir,
    debugger: DebuggerRef,
    storage: LinuxFS<IOLayers>,
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

        let io = RawIOLayer;
        let io = DebugIOLayer::new(io).with_order(order.into());
        let io = RetryIntrLayer::new(io);
        let io = Arc::new(io);

        let storage = LinuxFS::<IOLayers>::new(
            tempdir.path().to_path_buf(),
            options.with_io(Arc::clone(&io)),
        )
        .await
        .unwrap();

        f(&storage).await;

        let debugger = DebuggerRef { inner: io };
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

struct DebuggerRef {
    inner: Arc<RetryIntrLayer<DebugIOLayer<RawIOLayer>>>,
}

impl Deref for DebuggerRef {
    type Target = DebugIOLayer<RawIOLayer>;

    fn deref(&self) -> &Self::Target {
        self.inner.inner()
    }
}
