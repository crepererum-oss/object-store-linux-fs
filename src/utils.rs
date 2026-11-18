use std::sync::Arc;

use chrono::DateTime;
use object_store::{
    Error, ObjectMeta, Result,
    path::{DELIMITER_CHAR, Path},
};
use rustix::fs::{AtFlags, FileType, FlockOperation, Mode, RenameFlags, Stat};

use crate::{
    constants::{OFLAGS_DIR, OFLAGS_READ, RESOLVE_FLAGS, STAGING_FILE_PREFIX, STORE_NAME},
    error::IOErrorExt,
    io::{IOBlock, IOFD},
    state::State,
};

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
    let (dirs_str, filename) = split_dirs_and_filename_str(location);
    process_dirs_and_filename(dirs_str, filename, location, mode)
}

fn process_dirs_and_filename<'a>(
    dirs_str: Option<&'a str>,
    filename: &'a str,
    location: &'a Path,
    mode: OpenDirMode,
) -> Result<(DirsIt<'a>, &'a str)> {
    let dirs = match dirs_str {
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

fn split_dirs_and_filename_str(location: &Path) -> (Option<&str>, &str) {
    match location.as_ref().rsplit_once(DELIMITER_CHAR) {
        Some((dirs, filename)) => (Some(dirs), filename),
        None => (None, location.as_ref()),
    }
}

pub(crate) fn open_dir_fd(
    io: &mut dyn IOBlock,
    parent: &dyn IOFD,
    child: &str,
) -> Result<Arc<dyn IOFD>, rustix::io::Errno> {
    // we still need to fsync because the creating thread/process might die before that
    // TODO: introduce a read-only flag where we could skip this
    io.fsync(parent)?;

    let child_fd = io.openat2(parent, child, OFLAGS_DIR, Mode::empty(), RESOLVE_FLAGS)?;
    Ok(Arc::from(child_fd))
}

pub(crate) fn stat_to_meta(stat: Stat, location: Path, mode: OpenDirMode) -> Result<ObjectMeta> {
    // TODO: this is somewhat a hack because `O_REGULAR` does not exist (yet), see
    //       https://uapi-group.org/kernel-features/#ability-to-only-open-regular-files
    match FileType::from_raw_mode(stat.st_mode) {
        FileType::RegularFile => {}
        FileType::Directory => match mode {
            OpenDirMode::Get => {
                return Err(Error::NotFound {
                    path: location.to_string(),
                    source: "is a directory".to_owned().into(),
                });
            }
            OpenDirMode::Put => unreachable!(),
            OpenDirMode::Update => {
                return Err(Error::Precondition {
                    path: location.to_string(),
                    source: "is a directory".to_owned().into(),
                });
            }
        },
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
    io: &mut dyn IOBlock,
    file_fd: &dyn IOFD,
    dir_fd: &dyn IOFD,
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
    let staging_file = staging_file_name(io, file);
    io.linkat(file_fd, "", dir_fd, &staging_file, AtFlags::EMPTY_PATH)?;

    // rename to target
    let rename_res = io.renameat2(dir_fd, &staging_file, dir_fd, file, RenameFlags::empty());

    // need to clean up staging file
    //
    // Note that this might be required even if renameat2 succeeded because when "from" and "to" are the same inode, it
    // just doesn't do anything.
    let unlink_res = match io.unlinkat(dir_fd, &staging_file, AtFlags::empty()) {
        Ok(()) => Ok(()),
        Err(rustix::io::Errno::NOENT) => {
            // file gone, that's what we wanted
            Ok(())
        }
        Err(e) => Err(e),
    };

    rename_res.and(unlink_res)
}

fn staging_file_name(io: &mut dyn IOBlock, filename: &str) -> String {
    let uuid = io.uuid();

    // NOTE: the prefix already contains a `.`, so we don't need yet another one
    format!("{STAGING_FILE_PREFIX}{filename}.{uuid}")
}

pub(crate) fn flock_dir(
    io: &mut dyn IOBlock,
    dir_fd: &dyn IOFD,
) -> Result<Box<dyn IOFD>, rustix::io::Errno> {
    // TODO: There is no way to replace-override with a file descriptor. Hence we need to use
    //       directory-based locking to ensure the file doesn't change while we're at it. This is
    //       somewhat similar (but not identical) to:
    //
    //       - https://uapi-group.org/kernel-features/#unlinking-via-two-file-descriptors
    //       - https://bugzilla.kernel.org/show_bug.cgi?id=93441

    // `flock`ing-the same file/directory descriptor twice doesn't block and we have other threads
    // using the descriptor. Hence we need to re-open the descriptor (note that `dup` does NOT work!).
    let lock_fd = io.openat2(dir_fd, ".", OFLAGS_READ, Mode::empty(), RESOLVE_FLAGS)?;
    io.flock(lock_fd.as_ref(), FlockOperation::LockExclusive)?;

    Ok(lock_fd)
}

pub(crate) struct TransferFDs<'a> {
    pub(crate) from_dir_fd: Arc<dyn IOFD>,
    pub(crate) from_lock_fd: Option<Box<dyn IOFD>>,
    pub(crate) from_file: &'a str,
    pub(crate) to_dir_fd: Arc<dyn IOFD>,
    pub(crate) to_lock_fd: Option<Box<dyn IOFD>>,
    pub(crate) to_file: &'a str,
}

pub(crate) fn prepare_transfer_dirs<'a>(
    io: &mut dyn IOBlock,
    from: &'a Path,
    to: &'a Path,
    state: &State,
) -> Result<TransferFDs<'a>, Error> {
    let (from_dirs_str, from_file) = split_dirs_and_filename_str(from);
    let (to_dirs_str, to_file) = split_dirs_and_filename_str(to);

    let (from_dirs, from_file) =
        process_dirs_and_filename(from_dirs_str, from_file, from, OpenDirMode::Get)?;
    let (to_dirs, to_file) = process_dirs_and_filename(to_dirs_str, to_file, to, OpenDirMode::Put)?;

    match from_dirs_str.cmp(&to_dirs_str) {
        std::cmp::Ordering::Less => {
            let from_dir_fd = state.open_dir(io, from_dirs, OpenDirMode::Get)?;
            let to_dir_fd = state.open_dir(io, to_dirs, OpenDirMode::Put)?;

            let from_lock_fd = Some(flock_dir(io, from_dir_fd.as_ref()).into_other_err()?);
            let to_lock_fd = Some(flock_dir(io, to_dir_fd.as_ref()).into_other_err()?);

            Ok(TransferFDs {
                from_dir_fd,
                from_lock_fd,
                from_file,
                to_dir_fd,
                to_lock_fd,
                to_file,
            })
        }
        std::cmp::Ordering::Equal => {
            let from_dir_fd = state.open_dir(io, from_dirs, OpenDirMode::Get)?;
            let to_dir_fd = Arc::clone(&from_dir_fd);

            // don't lock twice!
            let from_lock_fd = Some(flock_dir(io, from_dir_fd.as_ref()).into_other_err()?);
            let to_lock_fd = None;

            Ok(TransferFDs {
                from_dir_fd,
                from_lock_fd,
                from_file,
                to_dir_fd,
                to_lock_fd,
                to_file,
            })
        }
        std::cmp::Ordering::Greater => {
            // flip order of operations
            let to_dir_fd = state.open_dir(io, to_dirs, OpenDirMode::Put)?;
            let from_dir_fd = state.open_dir(io, from_dirs, OpenDirMode::Get)?;

            let to_lock_fd = Some(flock_dir(io, to_dir_fd.as_ref()).into_other_err()?);
            let from_lock_fd = Some(flock_dir(io, from_dir_fd.as_ref()).into_other_err()?);

            Ok(TransferFDs {
                from_dir_fd,
                from_lock_fd,
                from_file,
                to_dir_fd,
                to_lock_fd,
                to_file,
            })
        }
    }
}
