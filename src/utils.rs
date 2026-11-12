use std::{os::fd::OwnedFd, sync::Arc};

use object_store::{
    Error, Result,
    path::{DELIMITER_CHAR, Path},
};
use rustix::fs::{Mode, OFlags, ResolveFlags};

use crate::constants::STORE_NAME;

pub(crate) fn dir_oflags() -> OFlags {
    OFlags::DIRECTORY | OFlags::NOFOLLOW
}

pub(crate) fn mode_create() -> Mode {
    Mode::RWXU | Mode::RWXG
}

pub(crate) fn rflags() -> ResolveFlags {
    ResolveFlags::BENEATH
}

pub(crate) async fn spawn_io<F, R>(f: F) -> Result<R>
where
    F: FnOnce() -> Result<R, std::io::Error> + Send + 'static,
    R: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| Error::Generic {
            store: STORE_NAME,
            source: Box::new(e),
        })?
        .map_err(|e| Error::Generic {
            store: STORE_NAME,
            source: Box::new(e),
        })
}

pub(crate) fn split_dirs_and_filename(
    location: &Path,
) -> Result<(impl Iterator<Item = &str>, &str)> {
    location
        .as_ref()
        .rsplit_once(DELIMITER_CHAR)
        .ok_or_else(|| Error::NotFound {
            path: location.to_string(),
            source: "filename cannot be root".to_owned().into(),
        })
        .map(|(dirs, filename)| (dirs.split(DELIMITER_CHAR), filename))
}

pub(crate) fn open_dir_fd(parent: &OwnedFd, child: &str) -> std::io::Result<Arc<OwnedFd>> {
    // we still need to fsync because the creating thread/process might die before that
    // TODO: introduce a read-only flag where we could skip this
    rustix::fs::fsync(parent)?;

    let child_fd = rustix::fs::openat2(parent, child, dir_oflags(), Mode::empty(), rflags())?;
    Ok(Arc::new(child_fd))
}
