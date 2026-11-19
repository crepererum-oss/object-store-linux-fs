use std::{assert_matches, num::NonZeroUsize, sync::Arc};

use bytes::Bytes;
use futures_util::TryStreamExt;
use object_store::{
    Attribute, Error, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion,
    path::Path,
};
use object_store_linux_fs::{LinuxFS, LinuxFSOptions, constants::STAGING_FILE_PREFIX};
use rustix::io::Errno;

use crate::io_debugger::{
    DebugIOLayer,
    log::Value,
    order::{Manipulation, Matcher},
    with_task_id,
};

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
    T2 B2 unlinkat(dirfd: fd#0, path: ".staging.x.00000000-0000-0000-0000-000000000000", flags: <empty>) -> Err(No such file or directory (os error 2))
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
    tempdir: tempfile::TempDir,
    debugger: Arc<DebugIOLayer>,
    storage: LinuxFS,
}

impl Fixture {
    async fn new<const N: usize, F>(order: [Matcher; N], f: F) -> Self
    where
        F: for<'a> AsyncFnOnce(&'a LinuxFS),
    {
        Self::new_with_options(LinuxFSOptions::default(), order, f).await
    }

    async fn new_with_options<const N: usize, F>(
        options: LinuxFSOptions,
        order: [Matcher; N],
        f: F,
    ) -> Self
    where
        F: for<'a> AsyncFnOnce(&'a LinuxFS),
    {
        let tempdir = tempfile::tempdir().unwrap();
        let debugger = Arc::new(DebugIOLayer::new().with_order(order.into()));
        let storage = LinuxFS::new(
            tempdir.path().to_path_buf(),
            LinuxFSOptions {
                io_layer: Arc::clone(&debugger) as _,
                ..options
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
    T2 B2 fsetxattr(fd: fd#1, name: "user.md.custom-attr", value: ..., flags: CREATE) -> Ok(())
    T2 B2 pwritev2(fd: fd#1, bufs: ..., offset: 0, flags: <empty>) -> Ok(6)
    T2 B2 fsync(fd: fd#1) -> Ok(())
    T2 B2 openat2(dirfd: fd#0, path: ".", oflags: NOFOLLOW|CLOEXEC, mode: <empty>, resolve: NO_XDEV|NO_MAGICLINKS|NO_SYMLINKS|BENEATH) -> Ok(fd#2)
    T2 B2 flock(fd: fd#2, operation: ...) -> Ok(())
    T2 B2 uuid() -> Ok("00000000-0000-0000-0000-000000000000")
    T2 B2 linkat(old_dirfd: fd#1, old_path: "", new_dirfd: fd#0, new_path: ".staging.x.00000000-0000-0000-0000-000000000000", flags: EMPTY_PATH) -> Ok(())
    T2 B2 renameat2(old_dirfd: fd#0, old_path: ".staging.x.00000000-0000-0000-0000-000000000000", new_dirfd: fd#0, new_path: "x", flags: <empty>) -> Ok(())
    T2 B2 fsync(fd: fd#0) -> Ok(())
    T2 B2 unlinkat(dirfd: fd#0, path: ".staging.x.00000000-0000-0000-0000-000000000000", flags: <empty>) -> Err(No such file or directory (os error 2))
    T2 B2 fsync(fd: fd#0) -> Ok(())
    T2 B2 fstat(fd: fd#1) -> Ok(...)
    T2 B2 close(fd: fd#1) -> Ok(())
    T2 B2 close(fd: fd#2) -> Ok(())
    T2 B2 block-end() -> Ok(())
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
    T2 B3 block-start() -> Ok(())
    T2 B3 lock(mutex: mutex#0) -> Ok(guard#1)
    T2 B3 unlock(mutex: mutex#0, guard: guard#1) -> Ok(())
    T2 B3 openat2(dirfd: fd#0, path: "x", oflags: NOFOLLOW|CLOEXEC, mode: <empty>, resolve: NO_XDEV|NO_MAGICLINKS|NO_SYMLINKS|BENEATH) -> Ok(fd#3)
    T2 B3 fstat(fd: fd#3) -> Ok(...)
    T2 B3 flistxattr_query(fd: fd#3) -> Ok(55)
    T2 B3 flistxattr(fd: fd#3, list: ...) -> Ok(...)
    T2 B3 fgetxattr_query(fd: fd#3, name: "user.content-type") -> Ok(10)
    T2 B3 fgetxattr(fd: fd#3, name: "user.content-type", value: ...) -> Ok(...)
    T2 B3 fgetxattr_query(fd: fd#3, name: "user.md.custom-attr") -> Ok(12)
    T2 B3 fgetxattr(fd: fd#3, name: "user.md.custom-attr", value: ...) -> Ok(...)
    T2 B3 block-end() -> Ok(())
    T2 B4 block-start() -> Ok(())
    T2 B4 preadv2(fd: fd#3, bufs: ..., offset: 0, flags: <empty>) -> Ok(6)
    T2 B4 block-end() -> Ok(())
    T2 B5 block-start() -> Ok(())
    T2 B5 close(fd: fd#3) -> Ok(())
    T2 B5 block-end() -> Ok(())
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
async fn rename_across_directories_syncs_destination_before_unlinking_source() {
    let from = Path::from("src/source");
    let to = Path::from("dst/destination");
    let fixture = Fixture::new([], async |storage| {
        storage
            .put(&from, PutPayload::from_static(b"source"))
            .await
            .unwrap();
        storage
            .put(&to, PutPayload::from_static(b"old destination"))
            .await
            .unwrap();
    })
    .await;

    fixture.storage.rename(&from, &to).await.unwrap();

    let entries = fixture.debugger.log_entries();
    let published = entries
        .iter()
        .position(|entry| {
            entry.fun == "renameat2"
                && entry.args.get("new_path") == Some(&Value::String("destination".into()))
                && entry.res.is_ok()
        })
        .expect("destination was not published");
    let unlinked = entries
        .iter()
        .position(|entry| {
            entry.fun == "unlinkat"
                && entry.args.get("path") == Some(&Value::String("source".into()))
                && entry.res.is_ok()
        })
        .expect("source was not unlinked");
    let destination_dir = &entries[published].args["new_dirfd"];
    let source_dir = &entries[unlinked].args["dirfd"];
    assert_ne!(source_dir, destination_dir);

    let synced = |entries: &[io_debugger::log::LogEntry], dir| {
        entries.iter().any(|entry| {
            entry.fun == "fsync" && entry.args.get("fd") == Some(dir) && entry.res.is_ok()
        })
    };
    assert!(
        published < unlinked && synced(&entries[published + 1..unlinked], destination_dir),
        "rename must sync the destination directory after publishing and before unlinking the source"
    );
    assert!(
        synced(&entries[unlinked + 1..], source_dir),
        "rename must sync the source directory after unlinking the source"
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

#[tokio::test]
async fn repeated_copy_leaves_no_staging_files() {
    let from = Path::parse("source").unwrap();
    let to = Path::parse("destination").unwrap();

    let fixture = Fixture::new([], async |storage| {
        storage.put(&from, Default::default()).await.unwrap();
    })
    .await;

    for _ in 0..2 {
        fixture.storage.copy(&from, &to).await.unwrap();
    }

    // ObjectStore::list hides staging files, so inspect the directory directly.
    let staging_files: Vec<_> = std::fs::read_dir(fixture.tempdir.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .filter(|name| {
            name.as_encoded_bytes()
                .starts_with(STAGING_FILE_PREFIX.as_bytes())
        })
        .collect();
    assert!(
        staging_files.is_empty(),
        "unexpected staging files: {staging_files:?}"
    );
}

fn payload_exceeding_iov_max() -> PutPayload {
    // Linux accepts at most 1,024 buffers in a single vectored write. The
    // last byte differs so the test also catches a retry of the wrong buffer.
    std::iter::repeat_n(Bytes::from_static(b"a"), 1024)
        .chain([Bytes::from_static(b"b")])
        .collect()
}

#[tokio::test]
async fn put_preserves_payload_after_short_write() {
    let fixture = Fixture::new([], async |_| {}).await;
    let loc = Path::from("x");
    let payload = payload_exceeding_iov_max();
    let expected = Bytes::from(payload.clone());

    fixture.storage.put(&loc, payload).await.unwrap();

    let actual = fixture
        .storage
        .get(&loc)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(
        actual.len(),
        expected.len(),
        "PUT changed the payload length"
    );
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn multipart_short_write_preserves_following_part() {
    let fixture = Fixture::new([], async |_| {}).await;
    let loc = Path::from("x");
    let mut upload = fixture.storage.put_multipart(&loc).await.unwrap();
    let payload = payload_exceeding_iov_max();
    let first_len = payload.content_length();
    let expected_first = Bytes::from(payload.clone());
    let tail = b"tail";

    let first = upload.put_part(payload);
    upload
        .put_part(PutPayload::from_static(tail))
        .await
        .unwrap();
    first.await.unwrap();
    upload.complete().await.unwrap();

    let actual = fixture
        .storage
        .get(&loc)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(&actual[first_len..first_len + tail.len()], tail);
    assert_eq!(actual.len(), first_len + tail.len());
    assert_eq!(&actual[..first_len], expected_first.as_ref());
}

#[tokio::test]
async fn get_ranges_stops_at_eof() {
    let loc = Path::from("x");
    let fixture = Fixture::new([], async |storage| {
        storage
            .put(&loc, PutPayload::from_static(b"abc"))
            .await
            .unwrap();
    })
    .await;

    let ranges = fixture
        .storage
        .get_ranges(&loc, std::slice::from_ref(&(0..4)))
        .await;
    assert_eq!(ranges.unwrap(), [Bytes::from_static(b"abc")]);
}

#[tokio::test]
async fn get_ranges_rejects_invalid_ranges() {
    let loc = Path::from("x");
    let fixture = Fixture::new([], async |storage| {
        storage
            .put(&loc, PutPayload::from_static(b"abc"))
            .await
            .unwrap();
    })
    .await;

    let mut accepted = Vec::new();
    for (start, end) in [(1, 1), (2, 1), (3, 4), (4, 5)] {
        let range = start..end;
        if fixture
            .storage
            .get_ranges(&loc, std::slice::from_ref(&range))
            .await
            .is_ok()
        {
            accepted.push(range);
        }
    }
    assert_eq!(
        accepted,
        [],
        "get_ranges accepted invalid ranges: {accepted:?}"
    );
}

#[tokio::test]
async fn race_rejected_update_does_not_block_create() {
    let fixture = Fixture::new(
        [
            // Resolve the update's parent before the create can publish `x`.
            Matcher {
                task_id: Some(2),
                fun: Some("openat2"),
                args: [("path", Value::String("x".to_owned()))].into(),
                ..Default::default()
            },
            Matcher {
                task_id: Some(3),
                fun: Some("flock"),
                ..Default::default()
            },
        ],
        async |_| {},
    )
    .await;

    let parent = Path::from("x");
    let child = Path::from("x/y");
    let update = with_task_id(
        2,
        fixture.storage.put_opts(
            &child,
            Default::default(),
            PutMode::Update(UpdateVersion {
                e_tag: Some("missing".to_owned()),
                version: None,
            })
            .into(),
        ),
    );
    let create = with_task_id(
        3,
        fixture.storage.put_opts(
            &parent,
            PutPayload::from_static(b"created"),
            PutMode::Create.into(),
        ),
    );

    let (update, create) = tokio::join!(update, create);
    assert_matches!(update, Err(Error::Precondition { .. }));
    create.expect("the rejected update must not leave a directory at x");
    let actual = fixture
        .storage
        .get(&parent)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(actual.as_ref(), b"created");
}

#[tokio::test]
async fn interrupt_get() {
    let loc = Path::parse("x").unwrap();

    let fixture = Fixture::new_with_options(
        LinuxFSOptions {
            read_buffer_size: NonZeroUsize::new(1).unwrap(),
            ..Default::default()
        },
        [
            Matcher {
                task_id: Some(2),
                fun: Some("preadv2"),
                ..Default::default()
            },
            Matcher {
                task_id: Some(2),
                fun: Some("preadv2"),
                manipulation: Some(Manipulation::FailBefore(Errno::INTR)),
                ..Default::default()
            },
        ],
        async |storage| {
            storage
                .put(&loc, PutPayload::from_static(b"foo"))
                .await
                .unwrap();
        },
    )
    .await;

    with_task_id(2, async {
        let res = fixture.storage.get(&loc).await.unwrap();
        let bytes = res.bytes().await.unwrap();
        assert_eq!(bytes.as_ref(), b"foo");
    })
    .await;

    insta::assert_snapshot!(
        fixture.debugger.log(),
        @r#"
    T2 B3 block-start() -> Ok(())
    T2 B3 lock(mutex: mutex#0) -> Ok(guard#1)
    T2 B3 unlock(mutex: mutex#0, guard: guard#1) -> Ok(())
    T2 B3 openat2(dirfd: fd#0, path: "x", oflags: NOFOLLOW|CLOEXEC, mode: <empty>, resolve: NO_XDEV|NO_MAGICLINKS|NO_SYMLINKS|BENEATH) -> Ok(fd#3)
    T2 B3 fstat(fd: fd#3) -> Ok(...)
    T2 B3 flistxattr_query(fd: fd#3) -> Ok(17)
    T2 B3 flistxattr(fd: fd#3, list: ...) -> Ok(...)
    T2 B3 block-end() -> Ok(())
    T2 B4 block-start() -> Ok(())
    T2 B4 preadv2(fd: fd#3, bufs: ..., offset: 0, flags: <empty>) -> Ok(1)
    T2 B4 block-end() -> Ok(())
    T2 B5 block-start() -> Ok(())
    T2 B5 preadv2(fd: fd#3, bufs: ..., offset: 1, flags: <empty>) -> Err(Interrupted system call (os error 4))
    T2 B5 preadv2(fd: fd#3, bufs: ..., offset: 1, flags: <empty>) -> Ok(1)
    T2 B5 block-end() -> Ok(())
    T2 B6 block-start() -> Ok(())
    T2 B6 preadv2(fd: fd#3, bufs: ..., offset: 2, flags: <empty>) -> Ok(1)
    T2 B6 block-end() -> Ok(())
    T2 B7 block-start() -> Ok(())
    T2 B7 close(fd: fd#3) -> Ok(())
    T2 B7 block-end() -> Ok(())
    "#,
    );
}

#[tokio::test]
async fn interrupt_get_ranges() {
    let loc = Path::parse("x").unwrap();

    let fixture = Fixture::new_with_options(
        LinuxFSOptions {
            read_buffer_size: NonZeroUsize::new(1).unwrap(),
            ..Default::default()
        },
        [
            Matcher {
                task_id: Some(2),
                fun: Some("preadv2"),
                ..Default::default()
            },
            Matcher {
                task_id: Some(2),
                fun: Some("preadv2"),
                manipulation: Some(Manipulation::FailBefore(Errno::INTR)),
                ..Default::default()
            },
        ],
        async |storage| {
            storage
                .put(&loc, PutPayload::from_static(b"fox"))
                .await
                .unwrap();
        },
    )
    .await;

    with_task_id(2, async {
        let bytes = fixture
            .storage
            .get_ranges(&loc, &[0..2, 1..3])
            .await
            .unwrap();
        assert_eq!(bytes.len(), 2);
        assert_eq!(bytes[0].as_ref(), b"fo");
        assert_eq!(bytes[1].as_ref(), b"ox");
    })
    .await;

    insta::assert_snapshot!(
        fixture.debugger.log(),
        @r#"
    T2 B3 block-start() -> Ok(())
    T2 B3 lock(mutex: mutex#0) -> Ok(guard#1)
    T2 B3 unlock(mutex: mutex#0, guard: guard#1) -> Ok(())
    T2 B3 openat2(dirfd: fd#0, path: "x", oflags: NOFOLLOW|CLOEXEC, mode: <empty>, resolve: NO_XDEV|NO_MAGICLINKS|NO_SYMLINKS|BENEATH) -> Ok(fd#3)
    T2 B3 fstat(fd: fd#3) -> Ok(...)
    T2 B3 preadv2(fd: fd#3, bufs: ..., offset: 0, flags: <empty>) -> Ok(2)
    T2 B3 preadv2(fd: fd#3, bufs: ..., offset: 1, flags: <empty>) -> Err(Interrupted system call (os error 4))
    T2 B3 preadv2(fd: fd#3, bufs: ..., offset: 1, flags: <empty>) -> Ok(2)
    T2 B3 close(fd: fd#3) -> Ok(())
    T2 B3 block-end() -> Ok(())
    "#,
    );
}
