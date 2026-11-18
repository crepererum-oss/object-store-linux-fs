//! Implementation-specific behavior tests.

use std::{assert_matches, os::unix::fs::PermissionsExt};

use futures_core::future::BoxFuture;
use futures_util::{StreamExt, TryStreamExt};
use object_store::{
    Attribute, AttributeValue, Attributes, Error, MultipartUpload, ObjectStore, ObjectStoreExt,
    PutOptions, PutPayload, path::Path,
};
use object_store_linux_fs::{LinuxFS, LinuxFSOptions, constants::STAGING_FILE_PREFIX};

#[tokio::test]
async fn store_does_not_autocreate_root() {
    let path = tempfile::tempdir().unwrap();
    let err = LinuxFS::new(
        path.path().join("sub").to_path_buf(),
        LinuxFSOptions::default(),
    )
    .await
    .unwrap_err();
    assert_matches!(err, Error::NotFound { .. });
}

#[tokio::test]
async fn staging_file() {
    let paths = [
        Path::parse(STAGING_FILE_PREFIX).unwrap(),
        Path::parse(format!("{STAGING_FILE_PREFIX}foo")).unwrap(),
        Path::parse(format!("sub/{STAGING_FILE_PREFIX}")).unwrap(),
        Path::parse(format!("sub/{STAGING_FILE_PREFIX}foo")).unwrap(),
        Path::parse(format!("sub2/{STAGING_FILE_PREFIX}/bar")).unwrap(),
        Path::parse(format!("sub2/{STAGING_FILE_PREFIX}foo/bar")).unwrap(),
    ];

    let storeage_path = tempfile::tempdir().unwrap();
    let storage = LinuxFS::new(
        storeage_path.path().to_path_buf(),
        LinuxFSOptions::default(),
    )
    .await
    .unwrap();

    // PUT
    for path in &paths {
        println!("PUT: {path}");
        let err = storage.put(path, Default::default()).await.unwrap_err();
        assert_matches!(err, Error::NotSupported { .. });
    }

    // PUT-multipart
    for path in &paths {
        println!("PUT-multipart: {path}");
        let err = storage.put_multipart(path).await.unwrap_err();
        assert_matches!(err, Error::NotSupported { .. });
    }

    // COPY to
    let copy_source = Path::parse("copy-source").unwrap();
    storage.put(&copy_source, Default::default()).await.unwrap();
    for path in &paths {
        println!("COPY to: {path}");
        let err = storage.copy(&copy_source, path).await.unwrap_err();
        assert_matches!(err, Error::NotSupported { .. });
    }

    // RENAME to
    let rename_source = Path::parse("rename-source").unwrap();
    storage
        .put(&rename_source, Default::default())
        .await
        .unwrap();
    for path in &paths {
        println!("RENAME to: {path}");
        let err = storage.rename(&rename_source, path).await.unwrap_err();
        assert_matches!(err, Error::NotSupported { .. });
    }

    // create the paths anyway
    for path in &paths {
        println!("CREATE: {path}");
        let fs_path = storeage_path.path().join(path.as_ref());
        std::fs::create_dir_all(fs_path.parent().unwrap()).unwrap();
        std::fs::write(fs_path, b"").unwrap();
    }

    // GET
    for path in &paths {
        println!("GET: {path}");
        let err = storage.get(path).await.unwrap_err();
        assert_matches!(err, Error::NotFound { .. });
    }

    // COPY from
    let copy_target = Path::parse("copy-target").unwrap();
    for path in &paths {
        println!("COPY from: {path}");
        let err = storage.copy(path, &copy_target).await.unwrap_err();
        assert_matches!(err, Error::NotFound { .. });
    }

    // RENAME from
    let rename_target = Path::parse("rename-target").unwrap();
    for path in &paths {
        println!("RENAME from: {path}");
        let err = storage.rename(path, &rename_target).await.unwrap_err();
        assert_matches!(err, Error::NotFound { .. });
    }

    // LIST
    println!("LIST");
    let mut paths = storage
        .list(None)
        .map_ok(|meta| meta.location)
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    paths.sort_unstable();
    assert_eq!(paths, &[copy_source.clone(), rename_source.clone()]);

    // LIST-with-delimiter
    println!("LIST-with-delimiter");
    let list_res = storage.list_with_delimiter(None).await.unwrap();
    let mut paths = list_res
        .objects
        .into_iter()
        .map(|meta| meta.location)
        .collect::<Vec<_>>();
    paths.sort_unstable();
    assert_eq!(paths, &[copy_source.clone(), rename_source.clone()]);
    assert_eq!(
        list_res.common_prefixes,
        vec![Path::parse("sub").unwrap(), Path::parse("sub2").unwrap()],
    );
}

#[tokio::test]
async fn permissions() {
    let path = tempfile::tempdir().unwrap();
    let storage = LinuxFS::new(path.path().to_path_buf(), LinuxFSOptions::default())
        .await
        .unwrap();

    storage
        .put(&Path::parse("sub/file").unwrap(), Default::default())
        .await
        .unwrap();

    assert_permission(
        std::fs::metadata(path.path().join("sub"))
            .unwrap()
            .permissions()
            .mode(),
        0o750,
    );

    assert_permission(
        std::fs::metadata(path.path().join("sub").join("file"))
            .unwrap()
            .permissions()
            .mode(),
        0o640,
    );
}

#[track_caller]
fn assert_permission(actual: u32, expected: u32) {
    let actual = actual & 0o777;
    assert!(
        actual == expected,
        "actual:{actual:#o} != expected:{expected:#o}"
    )
}

#[tokio::test]
async fn multipart_upload_small() {
    let path = tempfile::tempdir().unwrap();
    let storage = LinuxFS::new(path.path().to_path_buf(), LinuxFSOptions::default())
        .await
        .unwrap();

    let loc = Path::parse("x").unwrap();

    let mut upload = storage.put_multipart(&loc).await.unwrap();
    let fut1 = upload.put_part(PutPayload::from_static(b"foo"));
    let fut2 = upload.put_part(PutPayload::from_static(b"baaar"));
    let fut3 = upload.put_part(PutPayload::from_static(b"x"));
    fut2.await.unwrap();
    fut1.await.unwrap();
    fut3.await.unwrap();
    upload.complete().await.unwrap();

    assert_eq!(
        storage
            .get(&loc)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        b"foobaaarx",
    );
}

#[tokio::test]
async fn multipart_upload_state_error() {
    let path = tempfile::tempdir().unwrap();
    let storage = LinuxFS::new(path.path().to_path_buf(), LinuxFSOptions::default())
        .await
        .unwrap();

    let loc = Path::parse("x").unwrap();

    type OpFun = Box<
        dyn for<'a> Fn(
            &'a mut dyn MultipartUpload,
        ) -> BoxFuture<'a, Result<(), object_store::Error>>,
    >;
    struct Op {
        name: &'static str,
        f: OpFun,
        finishes: bool,
    }

    let ops = [
        Op {
            name: "put_part",
            f: Box::new(|upload| Box::pin(async { upload.put_part(Default::default()).await })),
            finishes: false,
        },
        Op {
            name: "complete",
            f: Box::new(|upload| Box::pin(async { upload.complete().await.map(|_| ()) })),
            finishes: true,
        },
        Op {
            name: "abort",
            f: Box::new(|upload| Box::pin(async { upload.abort().await })),
            finishes: true,
        },
    ];

    for finish_op in &ops {
        if !finish_op.finishes {
            continue;
        }

        for after_op in &ops {
            println!("{}.await => {}.await", finish_op.name, after_op.name);

            let mut upload = storage.put_multipart(&loc).await.unwrap();
            (finish_op.f)(&mut upload).await.unwrap();

            assert_matches!(
                (after_op.f)(&mut upload).await.unwrap_err(),
                Error::Generic { source, .. } if source.to_string() == "upload no longer running",
            );
        }
    }

    for finish_op in &ops {
        if !finish_op.finishes {
            continue;
        }

        println!("{}.await while running", finish_op.name);

        let mut upload = storage.put_multipart(&loc).await.unwrap();
        let _f = upload.put_part(Default::default());

        assert_matches!(
            (finish_op.f)(&mut upload).await.unwrap_err(),
            Error::Generic { source, .. } if source.to_string() == "upload is still running",
        );
    }
}

#[tokio::test]
async fn broken_attributes() {
    const VFS_LIMIT_NAME: usize = 255;
    const VFS_LIMIT_VALUE: usize = 65_536;

    let path = tempfile::tempdir().unwrap();
    let storage = LinuxFS::new(path.path().to_path_buf(), LinuxFSOptions::default())
        .await
        .unwrap();

    let loc = Path::parse("x").unwrap();

    let err = storage
        .put_opts(
            &loc,
            Default::default(),
            PutOptions {
                attributes: Attributes::from_iter([(
                    Attribute::Metadata(long_string(VFS_LIMIT_NAME + 1).into()),
                    AttributeValue::from("x".to_owned()),
                )]),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert_matches!(err, Error::Generic { .. });

    let err = storage
        .put_opts(
            &loc,
            Default::default(),
            PutOptions {
                attributes: Attributes::from_iter([(
                    Attribute::Metadata("x".into()),
                    AttributeValue::from(long_string(VFS_LIMIT_VALUE + 1)),
                )]),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert_matches!(err, Error::Generic { .. });
}

fn long_string(len: usize) -> String {
    std::iter::repeat_n('a', len).collect()
}

#[tokio::test]
async fn weird_attributes() {
    let path = tempfile::tempdir().unwrap();
    let storage = LinuxFS::new(path.path().to_path_buf(), LinuxFSOptions::default())
        .await
        .unwrap();

    let loc = Path::parse("x").unwrap();
    let attrs = Attributes::from_iter([
        (
            Attribute::Metadata("".into()),
            AttributeValue::from("empty".to_owned()),
        ),
        (
            Attribute::Metadata("x".into()),
            AttributeValue::from("".to_owned()),
        ),
        (
            Attribute::ContentType,
            AttributeValue::from("ct".to_owned()),
        ),
        (
            Attribute::Metadata("content-type".into()),
            AttributeValue::from("md-ct".to_owned()),
        ),
    ]);

    storage
        .put_opts(
            &loc,
            Default::default(),
            PutOptions {
                attributes: attrs.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let get_res = storage.get(&loc).await.unwrap();
    assert_eq!(get_res.attributes, attrs);
}

#[tokio::test]
async fn list_state() {
    const N_FILES: usize = 1_000;

    for prefix in ["", "prefix/"] {
        println!("prefix: {prefix}");

        let path = tempfile::tempdir().unwrap();
        let storage = LinuxFS::new(path.path().to_path_buf(), LinuxFSOptions::default())
            .await
            .unwrap();

        let mut files = (0..N_FILES)
            .map(|idx| Path::parse(format!("{prefix}{idx}")).unwrap())
            .collect::<Vec<_>>();
        files.sort_unstable();

        for f in &files {
            storage.put(f, Default::default()).await.unwrap();
        }

        let mut it1 = storage.list(None);
        let mut res1 = Vec::with_capacity(N_FILES);
        for _ in 0..(N_FILES / 2) {
            res1.push(it1.next().await.unwrap().unwrap().location);
        }

        let mut res2 = storage
            .list(None)
            .map_ok(|meta| meta.location)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        while let Some(meta) = it1.try_next().await.unwrap() {
            res1.push(meta.location);
        }

        res1.sort_unstable();
        res2.sort_unstable();

        assert_eq!(res1, files);
        assert_eq!(res2, files);
    }
}

#[tokio::test]
async fn rename_to_self_preserves_bytes() {
    let path = tempfile::tempdir().unwrap();
    let storage = LinuxFS::new(path.path().to_path_buf(), LinuxFSOptions::default())
        .await
        .unwrap();

    let loc = Path::parse("file").unwrap();
    let bytes = b"original contents";
    storage
        .put(&loc, PutPayload::from_static(bytes))
        .await
        .unwrap();

    storage.rename(&loc, &loc).await.unwrap();

    let result = storage.get(&loc).await.unwrap();
    assert_eq!(result.bytes().await.unwrap().as_ref(), bytes);
}
