use std::sync::Arc;

use bytes::Bytes;
use object_store::{Attribute, ObjectStore, ObjectStoreExt, PutOptions, path::Path};
use object_store_linux_fs::{LinuxFS, LinuxFSOptions};

use crate::{
    io_debugger::{DebugIOLayer, with_task_id},
    io_tests::Fixture,
};

#[tokio::test]
async fn simple_create_put_drop() {
    let path = tempfile::tempdir().unwrap();
    let debugger = Arc::new(DebugIOLayer::new());
    let storage = with_task_id(1, async {
        LinuxFS::new(
            path.path().to_path_buf(),
            LinuxFSOptions::default().with_io(Arc::clone(&debugger)),
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
    T1 B0 open(path: ..., flags: DIRECTORY|NOFOLLOW|CLOEXEC, mode: <empty>) -> Ok(fd#0)
    T1 B0 block-end() -> Ok(())
    T1 B- create_mutex() -> Ok(mutex#0)
    T2 B1 block-start() -> Ok(())
    T2 B1 lock(mutex: mutex#0) -> Ok(guard#0)
    T2 B1 unlock(mutex: mutex#0, guard: guard#0) -> Ok(())
    T2 B1 openat2(dirfd: fd#0, path: ".", oflags: DIRECTORY|NOFOLLOW|WRONLY|CLOEXEC|TMPFILE, mode: RUSR|WUSR|RGRP, resolve: NO_XDEV|NO_MAGICLINKS|NO_SYMLINKS|BENEATH) -> Ok(fd#1)
    T2 B1 fsync(fd: fd#1) -> Ok(())
    T2 B1 openat2(dirfd: fd#0, path: ".", oflags: NOFOLLOW|CLOEXEC, mode: <empty>, resolve: NO_XDEV|NO_MAGICLINKS|NO_SYMLINKS|BENEATH) -> Ok(fd#2)
    T2 B1 flock(fd: fd#2, operation: ...) -> Ok(())
    T2 B1 uuid() -> Ok("00000000-0000-0000-0000-000000000000")
    T2 B1 linkat(old_dirfd: fd#1, old_path: "", new_dirfd: fd#0, new_path: ".staging.x.00000000-0000-0000-0000-000000000000", flags: EMPTY_PATH) -> Ok(())
    T2 B1 renameat2(old_dirfd: fd#0, old_path: ".staging.x.00000000-0000-0000-0000-000000000000", new_dirfd: fd#0, new_path: "x", flags: <empty>) -> Ok(())
    T2 B1 fsync(fd: fd#0) -> Ok(())
    T2 B1 unlinkat(dirfd: fd#0, path: ".staging.x.00000000-0000-0000-0000-000000000000", flags: <empty>) -> Err(No such file or directory (os error 2))
    T2 B1 fsync(fd: fd#0) -> Ok(())
    T2 B1 fstat(fd: fd#1) -> Ok(...)
    T2 B1 close(fd: fd#1) -> Ok(())
    T2 B1 close(fd: fd#2) -> Ok(())
    T2 B1 block-end() -> Ok(())
    T3 B- close(fd: fd#0) -> Ok(())
    T3 B- destroy_mutex(mutex: mutex#0) -> Ok(())
    "#,
    );
}

#[tokio::test]
async fn complex_put() {
    let fixture = Fixture::new([], async |_| {}).await;

    let loc = Path::parse("x").unwrap();
    with_task_id(2, async {
        fixture
            .storage
            .put_opts(
                &loc,
                [Bytes::from_static(b"foo"), Bytes::from_static(b"bar")]
                    .into_iter()
                    .collect(),
                PutOptions {
                    attributes: [
                        (Attribute::ContentType, "my-content"),
                        (Attribute::Metadata("custom-attr".into()), "custom-value"),
                    ]
                    .into_iter()
                    .collect(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    })
    .await;

    insta::assert_snapshot!(
        fixture.debugger.log(),
        @r#"
    T2 B1 block-start() -> Ok(())
    T2 B1 lock(mutex: mutex#0) -> Ok(guard#0)
    T2 B1 unlock(mutex: mutex#0, guard: guard#0) -> Ok(())
    T2 B1 openat2(dirfd: fd#0, path: ".", oflags: DIRECTORY|NOFOLLOW|WRONLY|CLOEXEC|TMPFILE, mode: RUSR|WUSR|RGRP, resolve: NO_XDEV|NO_MAGICLINKS|NO_SYMLINKS|BENEATH) -> Ok(fd#1)
    T2 B1 fsetxattr(fd: fd#1, name: "user.content-type", value: ..., flags: CREATE) -> Ok(())
    T2 B1 fsetxattr(fd: fd#1, name: "user.md.custom-attr", value: ..., flags: CREATE) -> Ok(())
    T2 B1 pwritev2(fd: fd#1, bufs: ..., offset: 0, flags: <empty>) -> Ok(6)
    T2 B1 fsync(fd: fd#1) -> Ok(())
    T2 B1 openat2(dirfd: fd#0, path: ".", oflags: NOFOLLOW|CLOEXEC, mode: <empty>, resolve: NO_XDEV|NO_MAGICLINKS|NO_SYMLINKS|BENEATH) -> Ok(fd#2)
    T2 B1 flock(fd: fd#2, operation: ...) -> Ok(())
    T2 B1 uuid() -> Ok("00000000-0000-0000-0000-000000000000")
    T2 B1 linkat(old_dirfd: fd#1, old_path: "", new_dirfd: fd#0, new_path: ".staging.x.00000000-0000-0000-0000-000000000000", flags: EMPTY_PATH) -> Ok(())
    T2 B1 renameat2(old_dirfd: fd#0, old_path: ".staging.x.00000000-0000-0000-0000-000000000000", new_dirfd: fd#0, new_path: "x", flags: <empty>) -> Ok(())
    T2 B1 fsync(fd: fd#0) -> Ok(())
    T2 B1 unlinkat(dirfd: fd#0, path: ".staging.x.00000000-0000-0000-0000-000000000000", flags: <empty>) -> Err(No such file or directory (os error 2))
    T2 B1 fsync(fd: fd#0) -> Ok(())
    T2 B1 fstat(fd: fd#1) -> Ok(...)
    T2 B1 close(fd: fd#1) -> Ok(())
    T2 B1 close(fd: fd#2) -> Ok(())
    T2 B1 block-end() -> Ok(())
    "#,
    );
}

#[tokio::test]
async fn complex_get() {
    let loc = Path::parse("x").unwrap();

    let fixture = Fixture::new([], async |storage| {
        storage
            .put_opts(
                &loc,
                [Bytes::from_static(b"foo"), Bytes::from_static(b"bar")]
                    .into_iter()
                    .collect(),
                PutOptions {
                    attributes: [
                        (Attribute::ContentType, "my-content"),
                        (Attribute::Metadata("custom-attr".into()), "custom-value"),
                    ]
                    .into_iter()
                    .collect(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    })
    .await;

    with_task_id(2, async {
        let res = fixture.storage.get(&loc).await.unwrap();
        res.bytes().await.unwrap();
    })
    .await;

    insta::assert_snapshot!(
        fixture.debugger.log(),
        @r#"
    T2 B2 block-start() -> Ok(())
    T2 B2 lock(mutex: mutex#0) -> Ok(guard#1)
    T2 B2 unlock(mutex: mutex#0, guard: guard#1) -> Ok(())
    T2 B2 openat2(dirfd: fd#0, path: "x", oflags: NOFOLLOW|CLOEXEC, mode: <empty>, resolve: NO_XDEV|NO_MAGICLINKS|NO_SYMLINKS|BENEATH) -> Ok(fd#3)
    T2 B2 fstat(fd: fd#3) -> Ok(...)
    T2 B2 flistxattr_query(fd: fd#3) -> Ok(55)
    T2 B2 flistxattr(fd: fd#3, list: ...) -> Ok(...)
    T2 B2 fgetxattr_query(fd: fd#3, name: "user.content-type") -> Ok(10)
    T2 B2 fgetxattr(fd: fd#3, name: "user.content-type", value: ...) -> Ok(...)
    T2 B2 fgetxattr_query(fd: fd#3, name: "user.md.custom-attr") -> Ok(12)
    T2 B2 fgetxattr(fd: fd#3, name: "user.md.custom-attr", value: ...) -> Ok(...)
    T2 B2 block-end() -> Ok(())
    T2 B3 block-start() -> Ok(())
    T2 B3 preadv2(fd: fd#3, bufs: ..., offset: 0, flags: <empty>) -> Ok(6)
    T2 B3 block-end() -> Ok(())
    T2 B4 block-start() -> Ok(())
    T2 B4 close(fd: fd#3) -> Ok(())
    T2 B4 block-end() -> Ok(())
    "#,
    );
}
