use std::sync::Arc;

use object_store::{ObjectStoreExt, path::Path};
use object_store_linux_fs::{LinuxFS, LinuxFSOptions};

use crate::io_debugger::{DebugIOLayer, with_task_id};

mod io_debugger;

#[tokio::test]
async fn simple_put() {
    let path = tempfile::tempdir().unwrap();
    let debugger = Arc::new(DebugIOLayer::new());
    let storage = with_task_id(1, async {
        LinuxFS::new(
            path.path().to_path_buf(),
            LinuxFSOptions {
                io_layer: Arc::clone(&debugger) as _,
                ..Default::default()
            },
        )
        .await
        .unwrap()
    })
    .await;

    let loc = Path::parse("x").unwrap();
    with_task_id(2, async {
        storage.put(&loc, Default::default()).await.unwrap();
    })
    .await;

    with_task_id(3, async {
        drop(storage);
    })
    .await;

    insta::assert_snapshot!(
        debugger.log(),
        @r#"
    T1 B0 block-start() -> Ok(())
    T1 B0 open(path: ..., flags: DIRECTORY|NOFOLLOW|CLOEXEC, mode: <empty>) -> Ok(...)
    T1 B0 block-end() -> Ok(())
    T1 B- create_mutex() -> Ok(mutex#0)
    T2 B1 block-start() -> Ok(())
    T2 B1 lock(mutex: mutex#0) -> Ok(guard#0)
    T2 B1 unlock(mutex: mutex#0, guard: guard#0) -> Ok(())
    T2 B1 block-end() -> Ok(())
    T2 B2 block-start() -> Ok(())
    T2 B2 openat2(dirfd: ..., path: ".", oflags: DIRECTORY|NOFOLLOW|WRONLY|CLOEXEC|TMPFILE, mode: RUSR|WUSR|RGRP, resolve: NO_XDEV|NO_MAGICLINKS|NO_SYMLINKS|BENEATH) -> Ok(...)
    T2 B2 fsync(fd: ...) -> Ok(())
    T2 B2 openat2(dirfd: ..., path: ".", oflags: NOFOLLOW|CLOEXEC, mode: <empty>, resolve: NO_XDEV|NO_MAGICLINKS|NO_SYMLINKS|BENEATH) -> Ok(...)
    T2 B2 flock(fd: ..., operation: ...) -> Ok(())
    T2 B2 uuid() -> Ok("00000000-0000-0000-0000-000000000000")
    T2 B2 linkat(old_dirfd: ..., old_path: "", new_dirfd: ..., new_path: ".staging.x.00000000-0000-0000-0000-000000000000", flags: EMPTY_PATH) -> Ok(())
    T2 B2 renameat2(old_dirfd: ..., old_path: ".staging.x.00000000-0000-0000-0000-000000000000", new_dirfd: ..., new_path: "x", flags: <empty>) -> Ok(())
    T2 B2 fsync(fd: ...) -> Ok(())
    T2 B2 fstat(fd: ...) -> Ok(...)
    T2 B2 block-end() -> Ok(())
    T3 B- destroy_mutex(mutex: mutex#0) -> Ok(())
    "#,
    );
}
