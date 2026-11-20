use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};

use crate::io_tests::Fixture;

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
