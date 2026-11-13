//! Implementation-specific behavior tests.

use std::assert_matches;

use object_store::Error;
use object_store_linux_fs::LinuxFS;

#[tokio::test]
async fn store_does_not_autocreate_root() {
    let path = tempfile::tempdir().unwrap();
    let err = LinuxFS::new(path.path().join("sub").to_path_buf())
        .await
        .unwrap_err();
    assert_matches!(err, Error::NotFound { .. });
}
