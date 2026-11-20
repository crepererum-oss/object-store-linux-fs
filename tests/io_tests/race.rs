use std::assert_matches;

use futures_util::TryStreamExt;
use object_store::{
    Error, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion, path::Path,
};

use crate::{
    io_debugger::{log::Value, order::Matcher, with_task_id},
    io_tests::Fixture,
};

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
