use std::{os::fd::OwnedFd, sync::Arc};

use chrono::DateTime;
use object_store::{
    Error, ObjectMeta, Result,
    path::{DELIMITER_CHAR, Path},
};
use rustix::{
    fs::{FileType, Mode, OFlags, ResolveFlags, Stat},
    io::ReadWriteFlags,
};

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

pub(crate) fn rwflags() -> ReadWriteFlags {
    // `RWF_DONTCACHE` is not in rustix yet
    // https://github.com/torvalds/linux/blob/9d66f26fefacebfeed15de31c1123451a17ff119/include/uapi/linux/fs.h#L448-L449
    // ReadWriteFlags::from_bits_retain(128)
    // TODO: "don't cache" is not supported by tmpfs, so this makes testing harder. We'll likely need some feature detection.
    ReadWriteFlags::empty()
}

pub(crate) async fn spawn_io<F, R>(f: F) -> Result<R>
where
    F: FnOnce() -> Result<R> + Send + 'static,
    R: Send + 'static,
{
    tokio::task::spawn_blocking(f).await.map_err(|e| {
        if e.is_panic() {
            std::panic::resume_unwind(e.into_panic())
        } else {
            Error::Generic {
                store: STORE_NAME,
                source: Box::new(e),
            }
        }
    })?
}

pub(crate) fn split_dirs_and_filename<'a>(
    location: &'a Path,
) -> (Box<dyn Iterator<Item = &'a str> + Send + 'a>, &'a str) {
    match location.as_ref().rsplit_once(DELIMITER_CHAR) {
        Some((dirs, filename)) => (Box::new(dirs.split(DELIMITER_CHAR)) as _, filename),
        None => (Box::new(std::iter::empty()), location.as_ref()),
    }
}

pub(crate) fn open_dir_fd(
    parent: &OwnedFd,
    child: &str,
) -> Result<Arc<OwnedFd>, rustix::io::Errno> {
    // we still need to fsync because the creating thread/process might die before that
    // TODO: introduce a read-only flag where we could skip this
    rustix::fs::fsync(parent)?;

    let child_fd = rustix::fs::openat2(parent, child, dir_oflags(), Mode::empty(), rflags())?;
    Ok(Arc::new(child_fd))
}

pub(crate) fn stat_to_meta(stat: Stat, location: Path) -> Result<ObjectMeta> {
    match FileType::from_raw_mode(stat.st_mode) {
        FileType::RegularFile => {}
        FileType::Directory => {
            return Err(Error::NotFound {
                path: location.to_string(),
                source: "is a directory".to_owned().into(),
            });
        }
        other => {
            return Err(Error::Generic {
                store: STORE_NAME,
                source: format!("illegal file type: {other:?}").into(),
            });
        }
    }

    Ok(ObjectMeta {
        location,
        last_modified: DateTime::from_timestamp(stat.st_mtime as _, stat.st_mtime_nsec as _)
            .ok_or_else(|| Error::Generic {
                store: STORE_NAME,
                source: "cannot read mtime".to_owned().into(),
            })?,
        size: stat.st_size as _,
        e_tag: None,
        version: None,
    })
}
