use object_store::{ObjectStoreExt, PutPayload, path::Path};

use crate::{
    io_debugger::log::{LogEntry, Value},
    io_tests::Fixture,
};

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

    let synced = |entries: &[LogEntry], dir| {
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
