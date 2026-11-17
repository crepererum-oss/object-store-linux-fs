use std::sync::Arc;

use object_store::{ObjectStoreExt, path::Path};
use object_store_linux_fs::{LinuxFS, LinuxFSOptions};

use crate::io_debugger::DebugIOLayer;

mod io_debugger;

#[tokio::test]
async fn simple_put() {
    let path = tempfile::tempdir().unwrap();
    let debugger = Arc::new(DebugIOLayer::new());
    let storage = LinuxFS::new(
        path.path().to_path_buf(),
        LinuxFSOptions {
            io_layer: Arc::clone(&debugger) as _,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let loc = Path::parse("x").unwrap();
    storage.put(&loc, Default::default()).await.unwrap();

    insta::assert_snapshot!(
        debugger.log(),
        @r#"
    T0 B0 block-start() -> Ok(())
    T0 B0 open(path: ..., flags: ..., mode: ...) -> Ok(...)
    T0 B0 block-end() -> Ok(())
    T0 B1 block-start() -> Ok(())
    T0 B1 openat2(dirfd: ..., path: ".", oflags: ..., mode: ..., resolve: ...) -> Ok(...)
    T0 B1 fsync(fd: ...) -> Ok(())
    T0 B1 openat2(dirfd: ..., path: ".", oflags: ..., mode: ..., resolve: ...) -> Ok(...)
    T0 B1 flock(fd: ..., operation: ...) -> Ok(())
    T0 B1 uuid() -> Ok("00000000-0000-0000-0000-000000000000")
    T0 B1 linkat(old_dirfd: ..., old_path: "", new_dirfd: ..., new_path: ".staging.x.00000000-0000-0000-0000-000000000000", flags: ...) -> Ok(())
    T0 B1 renameat2(old_dirfd: ..., old_path: ".staging.x.00000000-0000-0000-0000-000000000000", new_dirfd: ..., new_path: "x", flags: ...) -> Ok(())
    T0 B1 fsync(fd: ...) -> Ok(())
    T0 B1 fstat(fd: ...) -> Ok(...)
    T0 B1 block-end() -> Ok(())
    "#,
    );
}
