use std::{assert_matches, sync::Arc};

use bytes::Bytes;
use futures_util::TryStreamExt;
use object_store::{
    Attribute, Error, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, path::Path,
};
use object_store_linux_fs::{LinuxFS, LinuxFSOptions};

use crate::io_debugger::{DebugIOLayer, log::Value, order::Matcher, with_task_id};

mod io_debugger;

#[tokio::test]
async fn simple_create_put_drop() {
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

struct Fixture {
    #[expect(dead_code)]
    tempdir: tempfile::TempDir,
    debugger: Arc<DebugIOLayer>,
    storage: LinuxFS,
}

impl Fixture {
    async fn new<const N: usize, F>(order: [Matcher; N], f: F) -> Self
    where
        F: for<'a> AsyncFnOnce(&'a LinuxFS),
    {
        let tempdir = tempfile::tempdir().unwrap();
        let debugger = Arc::new(DebugIOLayer::new().with_order(order.into()));
        let storage = LinuxFS::new(
            tempdir.path().to_path_buf(),
            LinuxFSOptions {
                io_layer: Arc::clone(&debugger) as _,
                ..Default::default()
            },
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
    T2 B1 block-end() -> Ok(())
    T2 B2 block-start() -> Ok(())
    T2 B2 openat2(dirfd: fd#0, path: ".", oflags: DIRECTORY|NOFOLLOW|WRONLY|CLOEXEC|TMPFILE, mode: RUSR|WUSR|RGRP, resolve: NO_XDEV|NO_MAGICLINKS|NO_SYMLINKS|BENEATH) -> Ok(fd#1)
    T2 B2 fsetxattr(fd: fd#1, name: "user.content-type", value: ..., flags: CREATE) -> Ok(())
    T2 B2 fsetxattr(fd: fd#1, name: "user.custom-attr", value: ..., flags: CREATE) -> Ok(())
    T2 B2 pwrite2(fd: fd#1, bufs: ..., offset: ..., flags: <empty>) -> Ok(...)
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
    "#,
    );
}

#[tokio::test]
async fn race_put_create() {
    let fixture = Fixture::new(
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
        ],
        async |_| {},
    )
    .await;

    let loc = Path::parse("x").unwrap();

    let fut1 = with_task_id(2, async {
        let err = fixture
            .storage
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
        fixture
            .storage
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

    let log = fixture.debugger.log();

    assert_eq!(
        fixture
            .storage
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
    let loc = Path::from("x");

    let fixture = Fixture::new(
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
        ],
        async |storage| {
            storage.put(&loc, Default::default()).await.unwrap();
        },
    )
    .await;

    let list = with_task_id(2, fixture.storage.list(None).try_collect::<Vec<_>>());
    let replace = with_task_id(3, async {
        fixture.storage.delete(&loc).await.unwrap();
        fixture
            .storage
            .put(&Path::from("x/y"), Default::default())
            .await
            .unwrap();
    });
    let (result, ()) = tokio::join!(list, replace);

    // No snapshot is required, but a vanished object must not abort traversal.
    assert!(
        result.is_ok(),
        "listing failed: {result:?}\n{}",
        fixture.debugger.log()
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
    let from = Path::from("x");
    let to = Path::from("y");

    let fixture = Fixture::new(
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
        ],
        async |storage| {
            storage
                .put(&from, PutPayload::from_static(b"old"))
                .await
                .unwrap();
        },
    )
    .await;

    let transfer = with_task_id(2, async {
        if rename {
            fixture.storage.rename(&from, &to).await
        } else {
            fixture.storage.copy(&from, &to).await
        }
    });
    let overwrite = with_task_id(3, async {
        fixture
            .storage
            .put(&from, PutPayload::from_static(b"new"))
            .await
            .unwrap();
    });
    let (result, ()) = tokio::join!(transfer, overwrite);

    println!("{}", fixture.debugger.log());

    result.unwrap();

    if rename {
        let err = fixture.storage.get(&from).await.unwrap_err();
        assert_matches!(err, Error::NotFound { .. });
    } else {
        assert_eq!(
            fixture
                .storage
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
        fixture
            .storage
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

#[tokio::test]
async fn race_copy_opposite_directories() {
    race_transfer_opposite_directories(false).await;
}

#[tokio::test]
async fn race_rename_opposite_directories() {
    race_transfer_opposite_directories(true).await;
}

async fn race_transfer_opposite_directories(rename: bool) {
    let from_a = Path::from("a/x");
    let from_b = Path::from("b/y");

    let fixture = Fixture::new(
        [
            Matcher {
                task_id: Some(2),
                fun: Some("flock"),
                ..Default::default()
            },
            Matcher {
                task_id: Some(2),
                fun: Some("flock"),
                ..Default::default()
            },
            Matcher {
                task_id: Some(3),
                fun: Some("flock"),
                ..Default::default()
            },
            Matcher {
                task_id: Some(3),
                fun: Some("flock"),
                ..Default::default()
            },
        ],
        async |storage| {
            storage.put(&from_a, Default::default()).await.unwrap();
            storage.put(&from_b, Default::default()).await.unwrap();
        },
    )
    .await;

    let to_b = Path::from("b/x");
    let to_a = Path::from("a/y");
    let (a, b) = tokio::join!(
        with_task_id(2, async {
            if rename {
                fixture.storage.rename(&from_a, &to_b).await
            } else {
                fixture.storage.copy(&from_a, &to_b).await
            }
        }),
        with_task_id(3, async {
            if rename {
                fixture.storage.rename(&from_b, &to_a).await
            } else {
                fixture.storage.copy(&from_b, &to_a).await
            }
        }),
    );
    a.unwrap();
    b.unwrap();
}
