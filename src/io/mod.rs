use std::{
    any::Any,
    cell::UnsafeCell,
    fmt::Debug,
    io::{IoSlice, IoSliceMut},
    marker::PhantomData,
    mem::{ManuallyDrop, MaybeUninit},
    ops::{Deref, DerefMut},
    sync::Arc,
};

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

mod raw;

pub use raw::RawIOLayer;

pub type DefaultIOLayer = RawIOLayer;

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
    use crate::io::raw::RawIOLayer;

    use super::*;

    #[tokio::test]
    #[should_panic(expected = "foo")]
    async fn spawn_io_panic() {
        let io_layer = RawIOLayer;
        spawn_io(&io_layer, |_| -> Result<(), _> { panic!("foo") })
            .await
            .ok();
    }
}
