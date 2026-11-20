use bytes::Bytes;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use object_store_linux_fs::constants::STAGING_FILE_PREFIX;

use crate::io_tests::Fixture;

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
