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

use lock_api::{GuardNoSend, RawMutex, RawRwLock};
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
    fn create_raw_rwlock(&self) -> Box<dyn IORawRwLock>;
}

pub trait IOBlockPrepared: Send {
    fn start(&mut self) -> Box<dyn IOBlock>;
}

pub trait IOBlock {
    /// See <https://man.archlinux.org/man/core/man-pages/fgetxattr.2>.
    fn fgetxattr<'a>(
        &mut self,
        fd: &dyn IOFD,
        name: &str,
        value: &'a mut [MaybeUninit<u8>],
    ) -> Result<(&'a mut [u8], &'a mut [MaybeUninit<u8>]), Errno>;

    /// See <https://man.archlinux.org/man/core/man-pages/fgetxattr.2>.
    fn fgetxattr_query(&mut self, fd: &dyn IOFD, name: &str) -> Result<usize, Errno>;

    /// See <https://man.archlinux.org/man/core/man-pages/flistxattr.2>.
    fn flistxattr<'a>(
        &mut self,
        fd: &dyn IOFD,
        list: &'a mut [MaybeUninit<u8>],
    ) -> Result<(&'a mut [u8], &'a mut [MaybeUninit<u8>]), Errno>;

    /// See <https://man.archlinux.org/man/core/man-pages/flistxattr.2>.
    fn flistxattr_query(&mut self, fd: &dyn IOFD) -> Result<usize, Errno>;

    /// See <https://man.archlinux.org/man/flock.2>.
    fn flock(&mut self, fd: &dyn IOFD, operation: FlockOperation) -> Result<(), Errno>;

    /// See <https://man.archlinux.org/man/core/man-pages/fstat.2>.
    fn fstat(&mut self, fd: &dyn IOFD) -> Result<Stat, Errno>;

    /// See <https://man.archlinux.org/man/core/man-pages/fsetxattr.2>.
    fn fsetxattr(
        &mut self,
        fd: &dyn IOFD,
        name: &str,
        value: &[u8],
        flags: XattrFlags,
    ) -> Result<(), Errno>;

    /// See <https://man.archlinux.org/man/fsync.2>.
    fn fsync(&mut self, fd: &dyn IOFD) -> Result<(), Errno>;

    /// See <https://man.archlinux.org/man/core/man-pages/getdents.2>.
    fn getdents(
        &mut self,
        fd: &dyn IOFD,
        buf: &mut Vec<MaybeUninit<u8>>,
    ) -> Result<Vec<(String, FileType)>, Errno>;

    /// Lock the given lock.
    fn lock(&mut self, raw_lock: &dyn IORawMutex) -> Box<dyn IORawMutexGuard>;

    /// Write-lock the given rw lock.
    fn lock_exclusive(&mut self, raw_lock: &dyn IORawRwLock) -> Box<dyn IORawMutexGuard>;

    /// Read-lock the given rw lock.
    fn lock_shared(&mut self, raw_lock: &dyn IORawRwLock) -> Box<dyn IORawMutexGuard>;

    /// See <https://man.archlinux.org/man/linkat.2>.
    fn linkat(
        &mut self,
        old_dirfd: &dyn IOFD,
        old_path: &str,
        new_dirfd: &dyn IOFD,
        new_path: &str,
        flags: AtFlags,
    ) -> Result<(), Errno>;

    /// See <https://man.archlinux.org/man/mkdirat.2>.
    fn mkdirat(&mut self, dirfd: &dyn IOFD, path: &str, mode: Mode) -> Result<(), Errno>;

    /// See <https://man.archlinux.org/man/open.2>.
    fn open(
        &mut self,
        path: &std::path::Path,
        flags: OFlags,
        mode: Mode,
    ) -> Result<Box<dyn IOFD>, Errno>;

    /// See <https://man.archlinux.org/man/openat2.2>.
    fn openat2(
        &mut self,
        dirfd: &dyn IOFD,
        path: &str,
        oflags: OFlags,
        mode: Mode,
        resolve: ResolveFlags,
    ) -> Result<Box<dyn IOFD>, Errno>;

    /// See <https://man.archlinux.org/man/core/man-pages/preadv2.2>.
    fn preadv2(
        &mut self,
        fd: &dyn IOFD,
        bufs: &mut [IoSliceMut<'_>],
        offset: u64,
        flags: ReadWriteFlags,
    ) -> Result<usize, Errno>;

    /// See <https://man.archlinux.org/man/core/man-pages/pwritev2.2>.
    fn pwritev2(
        &mut self,
        fd: &dyn IOFD,
        bufs: &[IoSlice<'_>],
        offset: u64,
        flags: ReadWriteFlags,
    ) -> Result<usize, Errno>;

    /// See <https://man.archlinux.org/man/renameat2.2>.
    fn renameat2(
        &mut self,
        old_dirfd: &dyn IOFD,
        old_path: &str,
        new_dirfd: &dyn IOFD,
        new_path: &str,
        flags: RenameFlags,
    ) -> Result<(), Errno>;

    /// See <https://man.archlinux.org/man/core/man-pages/stat.2>.
    fn statat(&mut self, dirfd: &dyn IOFD, path: &str, flags: AtFlags) -> Result<Stat, Errno>;

    /// See <https://man.archlinux.org/man/unlinkat.2>.
    fn unlinkat(&mut self, dirfd: &dyn IOFD, path: &str, flags: AtFlags) -> Result<(), Errno>;

    /// Generate unique UUID,
    fn uuid(&mut self) -> Uuid;
}

pub trait IORawMutex: Any + Send + Sync {
    /// # Safety
    /// See [`lock_api::RawMutex::unlock`].
    unsafe fn unlock(&self, guard: Box<dyn IORawMutexGuard>);
}

pub trait IORawMutexGuard: Any {}

pub trait IORawRwLock: Any + Send + Sync {
    /// # Safety
    /// See [`lock_api::RawRwLock::unlock_shared`].
    unsafe fn unlock_shared(&self, guard: Box<dyn IORawMutexGuard>);

    /// # Safety
    /// See [`lock_api::RawRwLock::unlock_exclusive`].
    unsafe fn unlock_exclusive(&self, guard: Box<dyn IORawMutexGuard>);
}

pub trait IOFD: Any + Debug + Send + Sync {}

#[derive(Debug)]
pub struct RustixIOLayer;

struct RustixIORawMutex {
    lock: parking_lot::RawMutex,
}

impl IORawMutex for RustixIORawMutex {
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

struct RustixIORawRwLock {
    lock: parking_lot::RawRwLock,
}

impl IORawRwLock for RustixIORawRwLock {
    unsafe fn unlock_shared(&self, guard: Box<dyn IORawMutexGuard>) {
        let guard = *(guard as Box<dyn Any>)
            .downcast::<RustixIORawLockGuard>()
            .expect("wrong type");

        // SAFETY: caller
        unsafe {
            self.lock.unlock_shared(guard.guard);
        }
    }

    unsafe fn unlock_exclusive(&self, guard: Box<dyn IORawMutexGuard>) {
        let guard = *(guard as Box<dyn Any>)
            .downcast::<RustixIORawLockGuard>()
            .expect("wrong type");

        // SAFETY: caller
        unsafe {
            self.lock.unlock_exclusive(guard.guard);
        }
    }
}

struct RustixIORawLockGuard {
    guard: GuardNoSend,
}

impl IORawMutexGuard for RustixIORawLockGuard {}

#[derive(Debug)]
struct RustixIOFD {
    inner: OwnedFd,
}

impl IOFD for RustixIOFD {}

impl IOLayer for RustixIOLayer {
    fn prepare_block(&self) -> Box<dyn IOBlockPrepared> {
        Box::new(RustixIOBlockPrepared)
    }

    fn create_raw_mutex(&self) -> Box<dyn IORawMutex> {
        Box::new(RustixIORawMutex {
            lock: parking_lot::RawMutex::INIT,
        })
    }

    fn create_raw_rwlock(&self) -> Box<dyn IORawRwLock> {
        Box::new(RustixIORawRwLock {
            lock: parking_lot::RawRwLock::INIT,
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

    fn create_raw_rwlock(&self) -> Box<dyn IORawRwLock> {
        self.as_ref().create_raw_rwlock()
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
        fd: &dyn IOFD,
        name: &str,
        value: &'a mut [MaybeUninit<u8>],
    ) -> Result<(&'a mut [u8], &'a mut [MaybeUninit<u8>]), Errno> {
        let fd = (fd as &dyn Any)
            .downcast_ref::<RustixIOFD>()
            .expect("wrong type");
        rustix::fs::fgetxattr(&fd.inner, name, value)
    }

    fn fgetxattr_query(&mut self, fd: &dyn IOFD, name: &str) -> Result<usize, Errno> {
        let fd = (fd as &dyn Any)
            .downcast_ref::<RustixIOFD>()
            .expect("wrong type");
        rustix::fs::fgetxattr_query(&fd.inner, name)
    }

    fn flistxattr<'a>(
        &mut self,
        fd: &dyn IOFD,
        list: &'a mut [MaybeUninit<u8>],
    ) -> Result<(&'a mut [u8], &'a mut [MaybeUninit<u8>]), Errno> {
        let fd = (fd as &dyn Any)
            .downcast_ref::<RustixIOFD>()
            .expect("wrong type");
        rustix::fs::flistxattr(&fd.inner, list)
    }

    fn flistxattr_query(&mut self, fd: &dyn IOFD) -> Result<usize, Errno> {
        let fd = (fd as &dyn Any)
            .downcast_ref::<RustixIOFD>()
            .expect("wrong type");
        rustix::fs::flistxattr_query(&fd.inner)
    }

    fn flock(&mut self, fd: &dyn IOFD, operation: FlockOperation) -> Result<(), Errno> {
        let fd = (fd as &dyn Any)
            .downcast_ref::<RustixIOFD>()
            .expect("wrong type");
        rustix::fs::flock(&fd.inner, operation)
    }

    fn fstat(&mut self, fd: &dyn IOFD) -> Result<Stat, Errno> {
        let fd = (fd as &dyn Any)
            .downcast_ref::<RustixIOFD>()
            .expect("wrong type");
        rustix::fs::fstat(&fd.inner)
    }

    fn fsetxattr(
        &mut self,
        fd: &dyn IOFD,
        name: &str,
        value: &[u8],
        flags: XattrFlags,
    ) -> Result<(), Errno> {
        let fd = (fd as &dyn Any)
            .downcast_ref::<RustixIOFD>()
            .expect("wrong type");
        rustix::fs::fsetxattr(&fd.inner, name, value, flags)
    }

    fn fsync(&mut self, fd: &dyn IOFD) -> Result<(), Errno> {
        let fd = (fd as &dyn Any)
            .downcast_ref::<RustixIOFD>()
            .expect("wrong type");
        rustix::fs::fsync(&fd.inner)
    }

    fn getdents(
        &mut self,
        fd: &dyn IOFD,
        buf: &mut Vec<MaybeUninit<u8>>,
    ) -> Result<Vec<(String, FileType)>, Errno> {
        let fd = (fd as &dyn Any)
            .downcast_ref::<RustixIOFD>()
            .expect("wrong type");

        let mut entries = vec![];

        let mut dir = loop {
            let mut dir = rustix::fs::RawDir::new(&fd.inner, buf);

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
            .downcast_ref::<RustixIORawMutex>()
            .expect("wrong type")
            .lock
            .lock();
        Box::new(RustixIORawLockGuard { guard })
    }

    fn lock_exclusive(&mut self, raw_lock: &dyn IORawRwLock) -> Box<dyn IORawMutexGuard> {
        let guard = (raw_lock as &dyn Any)
            .downcast_ref::<RustixIORawRwLock>()
            .expect("wrong type")
            .lock
            .lock_exclusive();
        Box::new(RustixIORawLockGuard { guard })
    }

    fn lock_shared(&mut self, raw_lock: &dyn IORawRwLock) -> Box<dyn IORawMutexGuard> {
        let guard = (raw_lock as &dyn Any)
            .downcast_ref::<RustixIORawRwLock>()
            .expect("wrong type")
            .lock
            .lock_shared();
        Box::new(RustixIORawLockGuard { guard })
    }

    fn linkat(
        &mut self,
        old_dirfd: &dyn IOFD,
        old_path: &str,
        new_dirfd: &dyn IOFD,
        new_path: &str,
        flags: AtFlags,
    ) -> Result<(), Errno> {
        let old_dirfd = (old_dirfd as &dyn Any)
            .downcast_ref::<RustixIOFD>()
            .expect("wrong type");
        let new_dirfd = (new_dirfd as &dyn Any)
            .downcast_ref::<RustixIOFD>()
            .expect("wrong type");
        rustix::fs::linkat(
            &old_dirfd.inner,
            old_path,
            &new_dirfd.inner,
            new_path,
            flags,
        )
    }

    fn mkdirat(&mut self, dirfd: &dyn IOFD, path: &str, mode: Mode) -> Result<(), Errno> {
        let dirfd = (dirfd as &dyn Any)
            .downcast_ref::<RustixIOFD>()
            .expect("wrong type");
        rustix::fs::mkdirat(&dirfd.inner, path, mode)
    }

    fn open(
        &mut self,
        path: &std::path::Path,
        flags: OFlags,
        mode: Mode,
    ) -> Result<Box<dyn IOFD>, Errno> {
        let inner = rustix::fs::open(path, flags, mode)?;
        Ok(Box::new(RustixIOFD { inner }))
    }

    fn openat2(
        &mut self,
        dirfd: &dyn IOFD,
        path: &str,
        oflags: OFlags,
        mode: Mode,
        resolve: ResolveFlags,
    ) -> Result<Box<dyn IOFD>, Errno> {
        let dirfd = (dirfd as &dyn Any)
            .downcast_ref::<RustixIOFD>()
            .expect("wrong type");
        let inner = rustix::fs::openat2(&dirfd.inner, path, oflags, mode, resolve)?;
        Ok(Box::new(RustixIOFD { inner }))
    }

    fn preadv2(
        &mut self,
        fd: &dyn IOFD,
        bufs: &mut [IoSliceMut<'_>],
        offset: u64,
        flags: ReadWriteFlags,
    ) -> Result<usize, Errno> {
        let fd = (fd as &dyn Any)
            .downcast_ref::<RustixIOFD>()
            .expect("wrong type");
        rustix::io::preadv2(&fd.inner, bufs, offset, flags)
    }

    fn pwritev2(
        &mut self,
        fd: &dyn IOFD,
        bufs: &[IoSlice<'_>],
        offset: u64,
        flags: ReadWriteFlags,
    ) -> Result<usize, Errno> {
        let fd = (fd as &dyn Any)
            .downcast_ref::<RustixIOFD>()
            .expect("wrong type");
        rustix::io::pwritev2(&fd.inner, bufs, offset, flags)
    }

    fn renameat2(
        &mut self,
        old_dirfd: &dyn IOFD,
        old_path: &str,
        new_dirfd: &dyn IOFD,
        new_path: &str,
        flags: RenameFlags,
    ) -> Result<(), Errno> {
        let old_dirfd = (old_dirfd as &dyn Any)
            .downcast_ref::<RustixIOFD>()
            .expect("wrong type");
        let new_dirfd = (new_dirfd as &dyn Any)
            .downcast_ref::<RustixIOFD>()
            .expect("wrong type");
        rustix::fs::renameat_with(
            &old_dirfd.inner,
            old_path,
            &new_dirfd.inner,
            new_path,
            flags,
        )
    }

    fn statat(&mut self, dirfd: &dyn IOFD, path: &str, flags: AtFlags) -> Result<Stat, Errno> {
        let dirfd = (dirfd as &dyn Any)
            .downcast_ref::<RustixIOFD>()
            .expect("wrong type");
        rustix::fs::statat(&dirfd.inner, path, flags)
    }

    fn unlinkat(&mut self, dirfd: &dyn IOFD, path: &str, flags: AtFlags) -> Result<(), Errno> {
        let dirfd = (dirfd as &dyn Any)
            .downcast_ref::<RustixIOFD>()
            .expect("wrong type");
        rustix::fs::unlinkat(&dirfd.inner, path, flags)
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

pub(crate) struct IORwLock<T> {
    data: UnsafeCell<T>,
    raw: Box<dyn IORawRwLock>,
}

impl<T> IORwLock<T> {
    pub(crate) fn new(io: &dyn IOLayer, data: T) -> Self {
        Self {
            data: UnsafeCell::new(data),
            raw: io.create_raw_rwlock(),
        }
    }

    pub(crate) fn read(&self, io: &mut dyn IOBlock) -> IORwLockReadGuard<'_, T> {
        let guard = io.lock_shared(self.raw.as_ref());

        IORwLockReadGuard {
            mutex: self,
            marker: PhantomData,
            guard: ManuallyDrop::new(guard),
        }
    }

    pub(crate) fn write(&self, io: &mut dyn IOBlock) -> IORwLockWriteGuard<'_, T> {
        let guard = io.lock_exclusive(self.raw.as_ref());

        IORwLockWriteGuard {
            mutex: self,
            marker: PhantomData,
            guard: ManuallyDrop::new(guard),
        }
    }
}

unsafe impl<T: Send> Sync for IORwLock<T> {}

impl<T> std::fmt::Debug for IORwLock<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IORwLock").finish_non_exhaustive()
    }
}

pub(crate) struct IORwLockReadGuard<'a, T> {
    mutex: &'a IORwLock<T>,
    marker: PhantomData<&'a T>,
    guard: ManuallyDrop<Box<dyn IORawMutexGuard>>,
}

impl<'a, T: 'a> Drop for IORwLockReadGuard<'a, T> {
    #[inline]
    fn drop(&mut self) {
        // Safety: A ReadGuard always holds the lock.
        unsafe {
            self.mutex
                .raw
                .unlock_shared(ManuallyDrop::take(&mut self.guard));
        }
    }
}

impl<'a, T: 'a> Deref for IORwLockReadGuard<'a, T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &T {
        unsafe { self.mutex.data.get().as_ref_unchecked() }
    }
}

pub(crate) struct IORwLockWriteGuard<'a, T> {
    mutex: &'a IORwLock<T>,
    marker: PhantomData<&'a mut T>,
    guard: ManuallyDrop<Box<dyn IORawMutexGuard>>,
}

impl<'a, T: 'a> Drop for IORwLockWriteGuard<'a, T> {
    #[inline]
    fn drop(&mut self) {
        // Safety: A WriteGuard always holds the lock.
        unsafe {
            self.mutex
                .raw
                .unlock_exclusive(ManuallyDrop::take(&mut self.guard));
        }
    }
}

impl<'a, T: 'a> Deref for IORwLockWriteGuard<'a, T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &T {
        unsafe { self.mutex.data.get().as_ref_unchecked() }
    }
}

impl<'a, T: 'a> DerefMut for IORwLockWriteGuard<'a, T> {
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
