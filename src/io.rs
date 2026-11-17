use std::{
    any::Any,
    cell::UnsafeCell,
    fmt::Debug,
    io::{IoSlice, IoSliceMut},
    marker::PhantomData,
    mem::{ManuallyDrop, MaybeUninit},
    ops::{Deref, DerefMut},
    os::fd::OwnedFd,
    sync::Arc,
};

use lock_api::{GuardNoSend, RawMutex};
use object_store::Error;
use rustix::{
    fs::{
        AtFlags, FileType, FlockOperation, Mode, OFlags, RenameFlags, ResolveFlags, Stat,
        XattrFlags,
    },
    io::{Errno, ReadWriteFlags},
};
use uuid::Uuid;

use crate::constants::STORE_NAME;

pub trait IOLayer: Debug + Send + Sync {
    fn prepare_block(&self) -> Box<dyn IOBlockPrepared>;
    fn create_raw_mutex(&self) -> Box<dyn IORawMutex>;
}

pub trait IOBlockPrepared: Send {
    fn start(&mut self) -> Box<dyn IOBlock>;
}

pub trait IOBlock {
    /// See <https://man.archlinux.org/man/core/man-pages/fgetxattr.2>.
    fn fgetxattr<'a>(
        &mut self,
        fd: &OwnedFd,
        name: &str,
        value: &'a mut [MaybeUninit<u8>],
    ) -> Result<(&'a mut [u8], &'a mut [MaybeUninit<u8>]), Errno>;

    /// See <https://man.archlinux.org/man/core/man-pages/fgetxattr.2>.
    fn fgetxattr_query(&mut self, fd: &OwnedFd, name: &str) -> Result<usize, Errno>;

    /// See <https://man.archlinux.org/man/core/man-pages/flistxattr.2>.
    fn flistxattr<'a>(
        &mut self,
        fd: &OwnedFd,
        list: &'a mut [MaybeUninit<u8>],
    ) -> Result<(&'a mut [u8], &'a mut [MaybeUninit<u8>]), Errno>;

    /// See <https://man.archlinux.org/man/core/man-pages/flistxattr.2>.
    fn flistxattr_query(&mut self, fd: &OwnedFd) -> Result<usize, Errno>;

    /// See <https://man.archlinux.org/man/flock.2>.
    fn flock(&mut self, fd: &OwnedFd, operation: FlockOperation) -> Result<(), Errno>;

    /// See <https://man.archlinux.org/man/core/man-pages/fstat.2>.
    fn fstat(&mut self, fd: &OwnedFd) -> Result<Stat, Errno>;

    /// See <https://man.archlinux.org/man/core/man-pages/fsetxattr.2>.
    fn fsetxattr(
        &mut self,
        fd: &OwnedFd,
        name: &str,
        value: &[u8],
        flags: XattrFlags,
    ) -> Result<(), Errno>;

    /// See <https://man.archlinux.org/man/fsync.2>.
    fn fsync(&mut self, fd: &OwnedFd) -> Result<(), Errno>;

    /// See <https://man.archlinux.org/man/core/man-pages/getdents.2>.
    fn getdents(
        &mut self,
        fd: &OwnedFd,
        buf: &mut Vec<MaybeUninit<u8>>,
    ) -> Result<Vec<(String, FileType)>, Errno>;

    /// Lock the given lock.
    fn lock(&mut self, raw_lock: &dyn IORawMutex) -> Box<dyn IORawMutexGuard>;

    /// See <https://man.archlinux.org/man/linkat.2>.
    fn linkat(
        &mut self,
        old_dirfd: &OwnedFd,
        old_path: &str,
        new_dirfd: &OwnedFd,
        new_path: &str,
        flags: AtFlags,
    ) -> Result<(), Errno>;

    /// See <https://man.archlinux.org/man/mkdirat.2>.
    fn mkdirat(&mut self, dirfd: &OwnedFd, path: &str, mode: Mode) -> Result<(), Errno>;

    /// See <https://man.archlinux.org/man/open.2>.
    fn open(&mut self, path: &std::path::Path, flags: OFlags, mode: Mode)
    -> Result<OwnedFd, Errno>;

    /// See <https://man.archlinux.org/man/openat2.2>.
    fn openat2(
        &mut self,
        dirfd: &OwnedFd,
        path: &str,
        oflags: OFlags,
        mode: Mode,
        resolve: ResolveFlags,
    ) -> Result<OwnedFd, Errno>;

    /// See <https://man.archlinux.org/man/core/man-pages/preadv2.2>.
    fn preadv2(
        &mut self,
        fd: &OwnedFd,
        bufs: &mut [IoSliceMut<'_>],
        offset: u64,
        flags: ReadWriteFlags,
    ) -> Result<usize, Errno>;

    /// See <https://man.archlinux.org/man/core/man-pages/pwritev2.2>.
    fn pwritev2(
        &mut self,
        fd: &OwnedFd,
        bufs: &[IoSlice<'_>],
        offset: u64,
        flags: ReadWriteFlags,
    ) -> Result<usize, Errno>;

    /// See <https://man.archlinux.org/man/renameat2.2>.
    fn renameat2(
        &mut self,
        old_dirfd: &OwnedFd,
        old_path: &str,
        new_dirfd: &OwnedFd,
        new_path: &str,
        flags: RenameFlags,
    ) -> Result<(), Errno>;

    /// See <https://man.archlinux.org/man/core/man-pages/stat.2>.
    fn statat(&mut self, dirfd: &OwnedFd, path: &str, flags: AtFlags) -> Result<Stat, Errno>;

    /// See <https://man.archlinux.org/man/unlinkat.2>.
    fn unlinkat(&mut self, dirfd: &OwnedFd, path: &str, flags: AtFlags) -> Result<(), Errno>;

    /// Generate unique UUID,
    fn uuid(&mut self) -> Uuid;
}

pub trait IORawMutex: Any + Send + Sync {
    /// # Safety
    /// See [`lock_api::RawMutex::unlock`].
    unsafe fn unlock(&self, guard: Box<dyn IORawMutexGuard>);
}

pub trait IORawMutexGuard: Any {}

#[derive(Debug)]
pub struct RustixIOLayer;

struct RustixIORawLock {
    lock: parking_lot::RawMutex,
}

impl IORawMutex for RustixIORawLock {
    unsafe fn unlock(&self, guard: Box<dyn IORawMutexGuard>) {
        let guard = *(guard as Box<dyn Any>)
            .downcast::<RustixIORawLockGuard>()
            .expect("wrong type");

        // SAFETY: caller
        unsafe {
            self.lock.unlock(guard.guard);
        }
    }
}

struct RustixIORawLockGuard {
    guard: GuardNoSend,
}

impl IORawMutexGuard for RustixIORawLockGuard {}

impl IOLayer for RustixIOLayer {
    fn prepare_block(&self) -> Box<dyn IOBlockPrepared> {
        Box::new(RustixIOBlockPrepared)
    }

    fn create_raw_mutex(&self) -> Box<dyn IORawMutex> {
        Box::new(RustixIORawLock {
            lock: parking_lot::RawMutex::INIT,
        })
    }
}

impl<T> IOLayer for Arc<T>
where
    T: ?Sized + IOLayer,
{
    fn prepare_block(&self) -> Box<dyn IOBlockPrepared> {
        self.as_ref().prepare_block()
    }

    fn create_raw_mutex(&self) -> Box<dyn IORawMutex> {
        self.as_ref().create_raw_mutex()
    }
}

struct RustixIOBlockPrepared;

impl IOBlockPrepared for RustixIOBlockPrepared {
    fn start(&mut self) -> Box<dyn IOBlock> {
        Box::new(RustixIOBlock)
    }
}

struct RustixIOBlock;

impl IOBlock for RustixIOBlock {
    fn fgetxattr<'a>(
        &mut self,
        fd: &OwnedFd,
        name: &str,
        value: &'a mut [MaybeUninit<u8>],
    ) -> Result<(&'a mut [u8], &'a mut [MaybeUninit<u8>]), Errno> {
        rustix::fs::fgetxattr(fd, name, value)
    }

    fn fgetxattr_query(&mut self, fd: &OwnedFd, name: &str) -> Result<usize, Errno> {
        rustix::fs::fgetxattr_query(fd, name)
    }

    fn flistxattr<'a>(
        &mut self,
        fd: &OwnedFd,
        list: &'a mut [MaybeUninit<u8>],
    ) -> Result<(&'a mut [u8], &'a mut [MaybeUninit<u8>]), Errno> {
        rustix::fs::flistxattr(fd, list)
    }

    fn flistxattr_query(&mut self, fd: &OwnedFd) -> Result<usize, Errno> {
        rustix::fs::flistxattr_query(fd)
    }

    fn flock(&mut self, fd: &OwnedFd, operation: FlockOperation) -> Result<(), Errno> {
        rustix::fs::flock(fd, operation)
    }

    fn fstat(&mut self, fd: &OwnedFd) -> Result<Stat, Errno> {
        rustix::fs::fstat(fd)
    }

    fn fsetxattr(
        &mut self,
        fd: &OwnedFd,
        name: &str,
        value: &[u8],
        flags: XattrFlags,
    ) -> Result<(), Errno> {
        rustix::fs::fsetxattr(fd, name, value, flags)
    }

    fn fsync(&mut self, fd: &OwnedFd) -> Result<(), Errno> {
        rustix::fs::fsync(fd)
    }

    fn getdents(
        &mut self,
        fd: &OwnedFd,
        buf: &mut Vec<MaybeUninit<u8>>,
    ) -> Result<Vec<(String, FileType)>, Errno> {
        let mut entries = vec![];

        let mut dir = loop {
            let mut dir = rustix::fs::RawDir::new(fd, buf);

            match dir.next() {
                None => {
                    // no entries left
                    return Ok(entries);
                }
                Some(Ok(first_entry)) => {
                    // buffer filled
                    entries.push((
                        String::from_utf8(first_entry.file_name().to_bytes().to_vec())
                            .map_err(|_| rustix::io::Errno::INVAL)?,
                        first_entry.file_type(),
                    ));
                    break dir;
                }
                Some(Err(rustix::io::Errno::INVAL)) => {
                    // buffer too small
                    buf.resize(buf.len().saturating_mul(2), MaybeUninit::uninit());
                }
                Some(Err(e)) => {
                    return Err(e);
                }
            }
        };

        while !dir.is_buffer_empty() {
            match dir.next() {
                None => {
                    return Ok(entries);
                }
                Some(Err(_)) => unreachable!("no IO performed here"),
                Some(Ok(entry)) => {
                    entries.push((
                        String::from_utf8(entry.file_name().to_bytes().to_vec())
                            .map_err(|_| rustix::io::Errno::INVAL)?,
                        entry.file_type(),
                    ));
                }
            }
        }

        Ok(entries)
    }

    fn lock(&mut self, raw_lock: &dyn IORawMutex) -> Box<dyn IORawMutexGuard> {
        let guard = (raw_lock as &dyn Any)
            .downcast_ref::<RustixIORawLock>()
            .expect("wrong type")
            .lock
            .lock();
        Box::new(RustixIORawLockGuard { guard })
    }

    fn linkat(
        &mut self,
        old_dirfd: &OwnedFd,
        old_path: &str,
        new_dirfd: &OwnedFd,
        new_path: &str,
        flags: AtFlags,
    ) -> Result<(), Errno> {
        rustix::fs::linkat(old_dirfd, old_path, new_dirfd, new_path, flags)
    }

    fn mkdirat(&mut self, dirfd: &OwnedFd, path: &str, mode: Mode) -> Result<(), Errno> {
        rustix::fs::mkdirat(dirfd, path, mode)
    }

    fn open(
        &mut self,
        path: &std::path::Path,
        flags: OFlags,
        mode: Mode,
    ) -> Result<OwnedFd, Errno> {
        rustix::fs::open(path, flags, mode)
    }

    fn openat2(
        &mut self,
        dirfd: &OwnedFd,
        path: &str,
        oflags: OFlags,
        mode: Mode,
        resolve: ResolveFlags,
    ) -> Result<OwnedFd, Errno> {
        rustix::fs::openat2(dirfd, path, oflags, mode, resolve)
    }

    fn preadv2(
        &mut self,
        fd: &OwnedFd,
        bufs: &mut [IoSliceMut<'_>],
        offset: u64,
        flags: ReadWriteFlags,
    ) -> Result<usize, Errno> {
        rustix::io::preadv2(fd, bufs, offset, flags)
    }

    fn pwritev2(
        &mut self,
        fd: &OwnedFd,
        bufs: &[IoSlice<'_>],
        offset: u64,
        flags: ReadWriteFlags,
    ) -> Result<usize, Errno> {
        rustix::io::pwritev2(fd, bufs, offset, flags)
    }

    fn renameat2(
        &mut self,
        old_dirfd: &OwnedFd,
        old_path: &str,
        new_dirfd: &OwnedFd,
        new_path: &str,
        flags: RenameFlags,
    ) -> Result<(), Errno> {
        rustix::fs::renameat_with(old_dirfd, old_path, new_dirfd, new_path, flags)
    }

    fn statat(&mut self, dirfd: &OwnedFd, path: &str, flags: AtFlags) -> Result<Stat, Errno> {
        rustix::fs::statat(dirfd, path, flags)
    }

    fn unlinkat(&mut self, dirfd: &OwnedFd, path: &str, flags: AtFlags) -> Result<(), Errno> {
        rustix::fs::unlinkat(dirfd, path, flags)
    }

    fn uuid(&mut self) -> Uuid {
        Uuid::new_v4()
    }
}

pub(crate) async fn spawn_io<F, R>(io_layer: &dyn IOLayer, f: F) -> Result<R, object_store::Error>
where
    F: for<'a> FnOnce(&'a mut dyn IOBlock) -> Result<R, object_store::Error> + Send + 'static,
    R: Send + 'static,
{
    let mut prepared = io_layer.prepare_block();

    tokio::task::spawn_blocking(move || {
        let mut block = prepared.start();
        f(block.as_mut())
    })
    .await
    .map_err(|e| {
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

pub(crate) struct IOMutex<T> {
    data: UnsafeCell<T>,
    raw: Box<dyn IORawMutex>,
}

impl<T> IOMutex<T> {
    pub(crate) fn new(io: &dyn IOLayer, data: T) -> Self {
        Self {
            data: UnsafeCell::new(data),
            raw: io.create_raw_mutex(),
        }
    }

    pub(crate) fn lock(&self, io: &mut dyn IOBlock) -> IOMutexGuard<'_, T> {
        let guard = io.lock(self.raw.as_ref());

        IOMutexGuard {
            mutex: self,
            marker: PhantomData,
            guard: ManuallyDrop::new(guard),
        }
    }
}

impl<T> std::fmt::Debug for IOMutex<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IOMutex").finish_non_exhaustive()
    }
}

unsafe impl<T: Send> Sync for IOMutex<T> {}

pub(crate) struct IOMutexGuard<'a, T> {
    mutex: &'a IOMutex<T>,
    marker: PhantomData<&'a mut T>,
    guard: ManuallyDrop<Box<dyn IORawMutexGuard>>,
}

impl<'a, T: 'a> Drop for IOMutexGuard<'a, T> {
    #[inline]
    fn drop(&mut self) {
        // Safety: A MutexGuard always holds the lock.
        unsafe {
            self.mutex.raw.unlock(ManuallyDrop::take(&mut self.guard));
        }
    }
}

impl<'a, T: 'a> Deref for IOMutexGuard<'a, T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &T {
        unsafe { self.mutex.data.get().as_ref_unchecked() }
    }
}

impl<'a, T: 'a> DerefMut for IOMutexGuard<'a, T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut T {
        unsafe { self.mutex.data.get().as_mut_unchecked() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[should_panic(expected = "foo")]
    async fn spawn_io_panic() {
        let io_layer = RustixIOLayer;
        spawn_io(&io_layer, |_| -> Result<(), _> { panic!("foo") })
            .await
            .ok();
    }
}
