use std::num::NonZeroUsize;

use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use object_store_linux_fs::LinuxFSOptions;
use rustix::io::Errno;

use crate::{
    io_debugger::{
        order::{Manipulation, Matcher},
        with_task_id,
    },
    io_tests::Fixture,
};

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
    T2 B2 block-start() -> Ok(())
    T2 B2 lock(mutex: mutex#0) -> Ok(guard#1)
    T2 B2 unlock(mutex: mutex#0, guard: guard#1) -> Ok(())
    T2 B2 openat2(dirfd: fd#0, path: "x", oflags: NOFOLLOW|CLOEXEC, mode: <empty>, resolve: NO_XDEV|NO_MAGICLINKS|NO_SYMLINKS|BENEATH) -> Ok(fd#3)
    T2 B2 fstat(fd: fd#3) -> Ok(...)
    T2 B2 flistxattr_query(fd: fd#3) -> Ok(17)
    T2 B2 flistxattr(fd: fd#3, list: ...) -> Ok(...)
    T2 B2 block-end() -> Ok(())
    T2 B3 block-start() -> Ok(())
    T2 B3 preadv2(fd: fd#3, bufs: ..., offset: 0, flags: <empty>) -> Ok(1)
    T2 B3 block-end() -> Ok(())
    T2 B4 block-start() -> Ok(())
    T2 B4 preadv2(fd: fd#3, bufs: ..., offset: 1, flags: <empty>) -> Err(Interrupted system call (os error 4))
    T2 B4 preadv2(fd: fd#3, bufs: ..., offset: 1, flags: <empty>) -> Ok(1)
    T2 B4 block-end() -> Ok(())
    T2 B5 block-start() -> Ok(())
    T2 B5 preadv2(fd: fd#3, bufs: ..., offset: 2, flags: <empty>) -> Ok(1)
    T2 B5 block-end() -> Ok(())
    T2 B6 block-start() -> Ok(())
    T2 B6 close(fd: fd#3) -> Ok(())
    T2 B6 block-end() -> Ok(())
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
    T2 B2 block-start() -> Ok(())
    T2 B2 lock(mutex: mutex#0) -> Ok(guard#1)
    T2 B2 unlock(mutex: mutex#0, guard: guard#1) -> Ok(())
    T2 B2 openat2(dirfd: fd#0, path: "x", oflags: NOFOLLOW|CLOEXEC, mode: <empty>, resolve: NO_XDEV|NO_MAGICLINKS|NO_SYMLINKS|BENEATH) -> Ok(fd#3)
    T2 B2 fstat(fd: fd#3) -> Ok(...)
    T2 B2 preadv2(fd: fd#3, bufs: ..., offset: 0, flags: <empty>) -> Ok(2)
    T2 B2 preadv2(fd: fd#3, bufs: ..., offset: 1, flags: <empty>) -> Err(Interrupted system call (os error 4))
    T2 B2 preadv2(fd: fd#3, bufs: ..., offset: 1, flags: <empty>) -> Ok(2)
    T2 B2 close(fd: fd#3) -> Ok(())
    T2 B2 block-end() -> Ok(())
    "#,
    );
}
