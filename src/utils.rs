use std::{os::fd::OwnedFd, sync::Arc};

use chrono::DateTime;
use object_store::{
    Error, ObjectMeta, Result,
    path::{DELIMITER_CHAR, Path},
};
use rustix::{
    fs::{AtFlags, FileType, FlockOperation, Mode, OFlags, RenameFlags, ResolveFlags, Stat},
    io::ReadWriteFlags,
};
use uuid::Uuid;

use crate::constants::{STAGING_FILE_PREFIX, STORE_NAME};

/// Set `0o750` = `user=rwx + group=rx`
///
/// The `x` flag is required for directory traversal.
pub(crate) fn mode_create_file() -> Mode {
    Mode::RUSR | Mode::WUSR | Mode::RGRP
}

/// Set `0x640` = `user=rw + group=rw`.
pub(crate) fn mode_create_dir() -> Mode {
    Mode::RWXU | Mode::RGRP | Mode::XGRP
}

pub(crate) fn oflags_create() -> OFlags {
    OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::TMPFILE | OFlags::WRONLY
}

pub(crate) fn oflags_dir() -> OFlags {
    OFlags::CLOEXEC | OFlags::DIRECTORY | OFlags::NOFOLLOW
}

pub(crate) fn oflags_read() -> OFlags {
    OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::RDONLY
}

pub(crate) fn rflags() -> ResolveFlags {
    ResolveFlags::BENEATH
        | ResolveFlags::NO_MAGICLINKS
        | ResolveFlags::NO_SYMLINKS
        | ResolveFlags::NO_XDEV
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OpenDirMode {
    Get,
    Put,
    Update,
}

pub(crate) type DirsIt<'a> = Box<dyn Iterator<Item = Result<&'a str>> + Send + 'a>;

pub(crate) fn split_dirs_and_filename<'a>(
    location: &'a Path,
    mode: OpenDirMode,
) -> Result<(DirsIt<'a>, &'a str)> {
    let (dirs, filename) = split_dirs_and_filename_str(location);

    let dirs = match dirs {
        Some(dirs) => Box::new(dirs.split(DELIMITER_CHAR).map(move |s| {
            check_staging_prefix(s, mode, location)?;
            Ok(s)
        })) as _,
        None => Box::new(std::iter::empty()) as _,
    };

    check_staging_prefix(filename, mode, location)?;

    Ok((dirs, filename))
}

fn check_staging_prefix(filename: &str, mode: OpenDirMode, location: &Path) -> Result<()> {
    if filename.starts_with(STAGING_FILE_PREFIX) {
        let source = "cannot use staging prefix".to_owned().into();

        match mode {
            OpenDirMode::Get => Err(Error::NotFound {
                path: location.to_string(),
                source,
            }),
            OpenDirMode::Put | OpenDirMode::Update => Err(Error::NotSupported { source }),
        }
    } else {
        Ok(())
    }
}

pub(crate) fn same_dir(loc1: &Path, loc2: &Path) -> bool {
    let (dirs1, _) = split_dirs_and_filename_str(loc1);
    let (dirs2, _) = split_dirs_and_filename_str(loc2);
    dirs1 == dirs2
}

fn split_dirs_and_filename_str(location: &Path) -> (Option<&str>, &str) {
    match location.as_ref().rsplit_once(DELIMITER_CHAR) {
        Some((dirs, filename)) => (Some(dirs), filename),
        None => (None, location.as_ref()),
    }
}

pub(crate) fn open_dir_fd(
    parent: &OwnedFd,
    child: &str,
) -> Result<Arc<OwnedFd>, rustix::io::Errno> {
    // we still need to fsync because the creating thread/process might die before that
    // TODO: introduce a read-only flag where we could skip this
    rustix::fs::fsync(parent)?;

    let child_fd = rustix::fs::openat2(parent, child, oflags_dir(), Mode::empty(), rflags())?;
    Ok(Arc::new(child_fd))
}

pub(crate) fn stat_to_meta(stat: Stat, location: Path) -> Result<ObjectMeta> {
    // TODO: this is somewhat a hack because `O_REGULAR` does not exist (yet), see
    //       https://uapi-group.org/kernel-features/#ability-to-only-open-regular-files
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
        e_tag: Some(stat.st_ino.to_string()),
        version: None,
    })
}

pub(crate) fn link_at_replace(
    file_fd: &OwnedFd,
    dir_fd: &OwnedFd,
    file: &str,
) -> Result<(), rustix::io::Errno> {
    // TODO: we cannot use `linkat` w/ replacement, see
    //       https://uapi-group.org/kernel-features/#linking-of-o_tmpfile-files-with-replacement
    //
    // Due to the limitation in the Linux API, we need to use an alternative API instead. `renameat2` works, but
    // requires a staging file.
    //
    // Note that "remove old file and link new file" is not acceptable, since it may end up with an intermediate (or
    // permanent -- if the machine crashes) state where a potentially existing target file is gone without replacement.

    // set up staging file
    let staging_file = staging_file_name(file);
    rustix::fs::linkat(file_fd, "", dir_fd, &staging_file, AtFlags::EMPTY_PATH)?;

    // rename to target
    let rename_res =
        rustix::fs::renameat_with(dir_fd, &staging_file, dir_fd, file, RenameFlags::empty());

    match rename_res {
        Ok(()) => {
            // staging file is gone via rename, so we're good
            Ok(())
        }
        Err(e) => {
            // need to clean up staging file
            rustix::fs::unlinkat(dir_fd, &staging_file, AtFlags::empty())?;

            Err(e)
        }
    }
}

fn staging_file_name(filename: &str) -> String {
    let uuid = Uuid::new_v4();

    // NOTE: the prefix already contains a `.`, so we don't need yet another one
    format!("{STAGING_FILE_PREFIX}{filename}.{uuid}")
}

pub(crate) fn flock_dir(dir_fd: &OwnedFd) -> Result<OwnedFd, rustix::io::Errno> {
    // TODO: There is no way to replace-override with a file descriptor. Hence we need to use
    //       directory-based locking to ensure the file doesn't change while we're at it. This is
    //       somewhat similar (but not identical) to:
    //
    //       - https://uapi-group.org/kernel-features/#unlinking-via-two-file-descriptors
    //       - https://bugzilla.kernel.org/show_bug.cgi?id=93441

    // `flock`ing-the same file/directory descriptor twice doesn't block and we have other threads
    // using the descriptor. Hence we need to re-open the descriptor (note that `dup` does NOT work!).
    let lock_fd = rustix::fs::openat2(dir_fd, ".", oflags_read(), Mode::empty(), rflags())?;
    rustix::fs::flock(&lock_fd, FlockOperation::LockExclusive)?;

    Ok(lock_fd)
}
