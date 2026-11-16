//! Constants that are used throughout the project.

use rustix::{
    fs::{Mode, OFlags, ResolveFlags},
    io::ReadWriteFlags,
};

/// Default buffer size for [`getdents`](https://man.archlinux.org/man/getdents.2)
pub(crate) const GETDENTS_BUFFER_SIZE: usize = 1024;

/// [`Mode`] used to potentially create new directories (it will be ignored for existing directories).
///
/// Set `0o750` = `user=rwx + group=rx`
///
/// The `x` flag is required for directory traversal.
pub(crate) const MODE_DIR_CREATE: Mode = Mode::RWXU.union(Mode::RGRP).union(Mode::XGRP);

/// [`Mode`] used to create new files.
///
/// Set `0x640` = `user=rw + group=rw`.
pub(crate) const MODE_FILE_CREATE: Mode = Mode::RUSR.union(Mode::WUSR).union(Mode::RGRP);

/// [`OFlags`] used to create a new file.
///
/// Set:
///
/// - [`O_CLOEXEC`](https://man.archlinux.org/man/openat.2#O_CLOEXEC)
/// - [`O_NOFOLLOW`](https://man.archlinux.org/man/openat.2#O_NOFOLLOW)
/// - [`O_TMPFILE`](https://man.archlinux.org/man/openat.2#O_TMPFILE)
/// - [`O_WRONLY`](https://man.archlinux.org/man/openat.2#File_access_mode)
pub(crate) const OFLAGS_FILE_CREATE: OFlags = OFlags::CLOEXEC
    .union(OFlags::NOFOLLOW)
    .union(OFlags::TMPFILE)
    .union(OFlags::WRONLY);

/// [`OFlags`] used to open a new or existing directory.
///
/// Set:
///
/// - [`O_CLOEXEC`](https://man.archlinux.org/man/openat.2#O_CLOEXEC)
/// - [`O_DIRECTORY`](https://man.archlinux.org/man/openat.2#O_DIRECTORY)
/// - [`O_NOFOLLOW`](https://man.archlinux.org/man/openat.2#O_NOFOLLOW)
pub(crate) const OFLAGS_DIR: OFlags = OFlags::CLOEXEC
    .union(OFlags::DIRECTORY)
    .union(OFlags::NOFOLLOW);

/// [`OFlags`] used to read an existing files or directories.
///
/// For directories this is mostly used to re-open the directory, e.g. to list its content or to place an [`flock`].
///
/// Set:
///
/// - [`O_CLOEXEC`](https://man.archlinux.org/man/openat.2#O_CLOEXEC)
/// - [`O_NOFOLLOW`](https://man.archlinux.org/man/openat.2#O_NOFOLLOW)
/// - [`O_RDONLY`](https://man.archlinux.org/man/openat.2#File_access_mode)
///
///
/// [`flock`]: https://man.archlinux.org/man/flock.2
pub(crate) const OFLAGS_READ: OFlags = OFlags::CLOEXEC
    .union(OFlags::NOFOLLOW)
    .union(OFlags::RDONLY);

/// Read buffer size for GET requests.
pub const READ_BUFFER_SIZE: usize = 4096;

/// [`ReadWriteFlags`] used for [`preadv2`]/[`pwritev2`]
///
/// This is currently empty.
///
/// [`preadv2`]: https://man.archlinux.org/man/preadv2.2
/// [`pwritev2`]: https://man.archlinux.org/man/pwritev2.2
pub(crate) const READ_WRITE_FLAGS: ReadWriteFlags = ReadWriteFlags::empty();

/// This is [`RWF_DONTCACHE`](https://man.archlinux.org/man/preadv2.2#RWF_DONTCACHE), which is not in [rustix] yet.
///
/// See <https://github.com/torvalds/linux/blob/9d66f26fefacebfeed15de31c1123451a17ff119/include/uapi/linux/fs.h#L448-L449>.
///
/// TODO: "don't cache" is not supported by tmpfs, so this makes testing harder. We'll likely need some feature detection.
#[expect(dead_code)]
const READ_WRITE_FLAGS_DONTCACHE: ReadWriteFlags = ReadWriteFlags::from_bits_retain(128);

/// [`ResolveFlags`] used to open files and directories.
///
/// Set:
///
/// - [`RESOLVE_BENEATH`](https://man.archlinux.org/man/openat2.2#RESOLVE_BENEATH)
/// - [`RESOLVE_NO_MAGICLINKS`](https://man.archlinux.org/man/openat2.2#RESOLVE_NO_MAGICLINKS)
/// - [`RESOLVE_NO_SYMLINKS`](https://man.archlinux.org/man/openat2.2#RESOLVE_NO_SYMLINKS)
/// - [`RESOLVE_NO_XDEV`](https://man.archlinux.org/man/openat2.2#RESOLVE_NO_XDEV)
pub(crate) const RESOLVE_FLAGS: ResolveFlags = ResolveFlags::BENEATH
    .union(ResolveFlags::NO_MAGICLINKS)
    .union(ResolveFlags::NO_SYMLINKS)
    .union(ResolveFlags::NO_XDEV);

/// Prefix used for staging files.
pub const STAGING_FILE_PREFIX: &str = ".staging.";

/// Store name reported in [`Display`] and [`Error`].
///
///
/// [`Display`]: std::fmt::Display
/// [`Error`]: object_store::Error
pub const STORE_NAME: &str = "LinuxFS";
