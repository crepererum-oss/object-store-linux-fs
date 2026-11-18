use std::{assert_matches, sync::Arc};

use futures_util::TryStreamExt;
use object_store::{
    Error, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, path::Path,
};
use object_store_linux_fs::{LinuxFS, LinuxFSOptions};

use crate::io_debugger::{DebugIOLayer, log::Value, order::Matcher, with_task_id};

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
    T1 B0 open(path: ..., flags: DIRECTORY|NOFOLLOW|CLOEXEC, mode: <empty>) -> Ok(fd#0)
    T1 B0 block-end() -> Ok(())
    T1 B- create_mutex() -> Ok(mutex#0)
    T2 B1 block-start() -> Ok(())
    T2 B1 lock(mutex: mutex#0) -> Ok(guard#0)
    T2 B1 unlock(mutex: mutex#0, guard: guard#0) -> Ok(())
    T2 B1 block-end() -> Ok(())
    T2 B2 block-start() -> Ok(())
    T2 B2 openat2(dirfd: fd#0, path: ".", oflags: DIRECTORY|NOFOLLOW|WRONLY|CLOEXEC|TMPFILE, mode: RUSR|WUSR|RGRP, resolve: NO_XDEV|NO_MAGICLINKS|NO_SYMLINKS|BENEATH) -> Ok(fd#1)
    T2 B2 fsync(fd: fd#1) -> Ok(())
    T2 B2 openat2(dirfd: fd#0, path: ".", oflags: NOFOLLOW|CLOEXEC, mode: <empty>, resolve: NO_XDEV|NO_MAGICLINKS|NO_SYMLINKS|BENEATH) -> Ok(fd#2)
    T2 B2 flock(fd: fd#2, operation: ...) -> Ok(())
    T2 B2 uuid() -> Ok("00000000-0000-0000-0000-000000000000")
    T2 B2 linkat(old_dirfd: fd#1, old_path: "", new_dirfd: fd#0, new_path: ".staging.x.00000000-0000-0000-0000-000000000000", flags: EMPTY_PATH) -> Ok(())
    T2 B2 renameat2(old_dirfd: fd#0, old_path: ".staging.x.00000000-0000-0000-0000-000000000000", new_dirfd: fd#0, new_path: "x", flags: <empty>) -> Ok(())
    T2 B2 fsync(fd: fd#0) -> Ok(())
    T2 B2 fstat(fd: fd#1) -> Ok(...)
    T2 B2 close(fd: fd#1) -> Ok(())
    T2 B2 close(fd: fd#2) -> Ok(())
    T2 B2 block-end() -> Ok(())
    T3 B- close(fd: fd#0) -> Ok(())
    T3 B- destroy_mutex(mutex: mutex#0) -> Ok(())
    "#,
    );
}

#[tokio::test]
async fn race_put_create() {
    let path = tempfile::tempdir().unwrap();
    let debugger = Arc::new(
        DebugIOLayer::new().with_order(
            [
                Matcher {
                    task_id: Some(3),
                    fun: Some("flock"),
                    ..Default::default()
                },
                Matcher {
                    task_id: Some(2),
                    fun: Some("flock"),
                    ..Default::default()
                },
            ]
            .into(),
        ),
    );
    let storage = LinuxFS::new(
        path.path().to_path_buf(),
        LinuxFSOptions {
            io_layer: Arc::clone(&debugger) as _,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    debugger.wipe_log();

    let loc = Path::parse("x").unwrap();

    let fut1 = with_task_id(2, async {
        let err = storage
            .put_opts(
                &loc,
                PutPayload::from_static(b"foo"),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        assert_matches!(err, Error::AlreadyExists { .. });
    });
    let fut2 = with_task_id(3, async {
        storage
            .put_opts(
                &loc,
                PutPayload::from_static(b"bar"),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    });

    tokio::join!(fut1, fut2);

    let log = debugger.log();

    assert_eq!(
        storage
            .get(&loc)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        b"bar"
    );

    println!("{log}");
}

#[tokio::test]
async fn race_list_file_becomes_directory() {
    let path = tempfile::tempdir().unwrap();
    let debugger = Arc::new(
        DebugIOLayer::new().with_order(
            [
                Matcher {
                    task_id: Some(2),
                    fun: Some("getdents"),
                    ..Default::default()
                },
                Matcher {
                    task_id: Some(3),
                    fun: Some("unlinkat"),
                    args: [("path", Value::String("x".to_owned()))].into(),
                    ..Default::default()
                },
                Matcher {
                    task_id: Some(3),
                    fun: Some("mkdirat"),
                    args: [("path", Value::String("x".to_owned()))].into(),
                    ..Default::default()
                },
                Matcher {
                    task_id: Some(2),
                    fun: Some("statat"),
                    args: [("path", Value::String("x".to_owned()))].into(),
                    ..Default::default()
                },
            ]
            .into(),
        ),
    );
    let storage = LinuxFS::new(
        path.path().to_path_buf(),
        LinuxFSOptions {
            io_layer: Arc::clone(&debugger) as _,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let loc = Path::from("x");
    storage.put(&loc, Default::default()).await.unwrap();

    debugger.wipe_log();

    let list = with_task_id(2, storage.list(None).try_collect::<Vec<_>>());
    let replace = with_task_id(3, async {
        storage.delete(&loc).await.unwrap();
        storage
            .put(&Path::from("x/y"), Default::default())
            .await
            .unwrap();
    });
    let (result, ()) = tokio::join!(list, replace);

    // No snapshot is required, but a vanished object must not abort traversal.
    assert!(
        result.is_ok(),
        "listing failed: {result:?}\n{}",
        debugger.log()
    );
    assert!(result.unwrap().is_empty());
}

#[tokio::test]
async fn race_copy_source_overwrite() {
    race_source_overwrite(false).await;
}

#[tokio::test]
async fn race_rename_source_overwrite() {
    race_source_overwrite(true).await;
}

async fn race_source_overwrite(rename: bool) {
    let path = tempfile::tempdir().unwrap();
    let debugger = Arc::new(
        DebugIOLayer::new().with_order(
            [
                // Pin the old inode, replace its last link, then try to publish it.
                Matcher {
                    task_id: Some(3),
                    fun: Some("flock"),
                    ..Default::default()
                },
                Matcher {
                    task_id: Some(3),
                    fun: Some("renameat2"),
                    args: [("new_path", Value::String("x".to_owned()))].into(),
                    ..Default::default()
                },
                Matcher {
                    task_id: Some(2),
                    fun: Some("flock"),
                    ..Default::default()
                },
                Matcher {
                    task_id: Some(2),
                    fun: Some("openat2"),
                    args: [("path", Value::String("x".to_owned()))].into(),
                    ..Default::default()
                },
            ]
            .into(),
        ),
    );
    let storage = LinuxFS::new(
        path.path().to_path_buf(),
        LinuxFSOptions {
            io_layer: Arc::clone(&debugger) as _,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let from = Path::from("x");
    let to = Path::from("y");

    storage
        .put(&from, PutPayload::from_static(b"old"))
        .await
        .unwrap();
    debugger.wipe_log();

    let transfer = with_task_id(2, async {
        if rename {
            storage.rename(&from, &to).await
        } else {
            storage.copy(&from, &to).await
        }
    });
    let overwrite = with_task_id(3, async {
        storage
            .put(&from, PutPayload::from_static(b"new"))
            .await
            .unwrap();
    });
    let (result, ()) = tokio::join!(transfer, overwrite);

    println!("{}", debugger.log());

    result.unwrap();

    if rename {
        let err = storage.get(&from).await.unwrap_err();
        assert_matches!(err, Error::NotFound { .. });
    } else {
        assert_eq!(
            storage
                .get(&from)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .as_ref(),
            b"new",
        );
    }

    assert_eq!(
        storage
            .get(&to)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        b"new",
    );
}
