use std::{
    fmt::Debug,
    io::{IoSlice, IoSliceMut},
    mem::MaybeUninit,
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
mod retry_intr;

pub use raw::RawIOLayer;
pub use retry_intr::RetryIntrLayer;

pub type DefaultIOLayer = RetryIntrLayer<RawIOLayer>;

pub trait IOLayer: Debug + Send + Sync + 'static {
    type Block: IOBlock<FD = Self::FD>;
    type BlockPrepared: IOBlockPrepared<Block = Self::Block>;
    type FD: IOFD;
    type Mutex<T>: IOMutex<T, Block = Self::Block>
    where
        T: Send + Debug;
    type RwLock<T>: IORwLock<T, Block = Self::Block>
    where
        T: Send + Sync + Debug;

    fn prepare_block(&self) -> Self::BlockPrepared;
    fn create_mutex<T>(&self, data: T) -> Self::Mutex<T>
    where
        T: Send + Debug;
    fn create_rwlock<T>(&self, data: T) -> Self::RwLock<T>
    where
        T: Send + Sync + Debug;
}

pub trait IOBlockPrepared: Send {
    type Block: IOBlock;

    fn start(&mut self) -> Self::Block;
}

pub trait IOBlock {
    type FD: IOFD;

    /// See <https://man.archlinux.org/man/core/man-pages/fgetxattr.2>.
    fn fgetxattr<'a>(
        &mut self,
        fd: &Self::FD,
        name: &str,
        value: &'a mut [MaybeUninit<u8>],
    ) -> Result<(&'a mut [u8], &'a mut [MaybeUninit<u8>]), Errno>;

    /// See <https://man.archlinux.org/man/core/man-pages/fgetxattr.2>.
    fn fgetxattr_query(&mut self, fd: &Self::FD, name: &str) -> Result<usize, Errno>;

    /// See <https://man.archlinux.org/man/core/man-pages/flistxattr.2>.
    fn flistxattr<'a>(
        &mut self,
        fd: &Self::FD,
        list: &'a mut [MaybeUninit<u8>],
    ) -> Result<(&'a mut [u8], &'a mut [MaybeUninit<u8>]), Errno>;

    /// See <https://man.archlinux.org/man/core/man-pages/flistxattr.2>.
    fn flistxattr_query(&mut self, fd: &Self::FD) -> Result<usize, Errno>;

    /// See <https://man.archlinux.org/man/flock.2>.
    fn flock(&mut self, fd: &Self::FD, operation: FlockOperation) -> Result<(), Errno>;

    /// See <https://man.archlinux.org/man/core/man-pages/fstat.2>.
    fn fstat(&mut self, fd: &Self::FD) -> Result<Stat, Errno>;

    /// See <https://man.archlinux.org/man/core/man-pages/fsetxattr.2>.
    fn fsetxattr(
        &mut self,
        fd: &Self::FD,
        name: &str,
        value: &[u8],
        flags: XattrFlags,
    ) -> Result<(), Errno>;

    /// See <https://man.archlinux.org/man/fsync.2>.
    fn fsync(&mut self, fd: &Self::FD) -> Result<(), Errno>;

    /// See <https://man.archlinux.org/man/core/man-pages/getdents.2>.
    fn getdents(
        &mut self,
        fd: &Self::FD,
        buf: &mut Vec<MaybeUninit<u8>>,
    ) -> Result<Vec<(String, FileType)>, Errno>;

    /// See <https://man.archlinux.org/man/linkat.2>.
    fn linkat(
        &mut self,
        old_dirfd: &Self::FD,
        old_path: &str,
        new_dirfd: &Self::FD,
        new_path: &str,
        flags: AtFlags,
    ) -> Result<(), Errno>;

    /// See <https://man.archlinux.org/man/mkdirat.2>.
    fn mkdirat(&mut self, dirfd: &Self::FD, path: &str, mode: Mode) -> Result<(), Errno>;

    /// See <https://man.archlinux.org/man/open.2>.
    fn open(
        &mut self,
        path: &std::path::Path,
        flags: OFlags,
        mode: Mode,
    ) -> Result<Self::FD, Errno>;

    /// See <https://man.archlinux.org/man/openat2.2>.
    fn openat2(
        &mut self,
        dirfd: &Self::FD,
        path: &str,
        oflags: OFlags,
        mode: Mode,
        resolve: ResolveFlags,
    ) -> Result<Self::FD, Errno>;

    /// See <https://man.archlinux.org/man/core/man-pages/preadv2.2>.
    fn preadv2(
        &mut self,
        fd: &Self::FD,
        bufs: &mut [IoSliceMut<'_>],
        offset: u64,
        flags: ReadWriteFlags,
    ) -> Result<usize, Errno>;

    /// See <https://man.archlinux.org/man/core/man-pages/pwritev2.2>.
    fn pwritev2(
        &mut self,
        fd: &Self::FD,
        bufs: &[IoSlice<'_>],
        offset: u64,
        flags: ReadWriteFlags,
    ) -> Result<usize, Errno>;

    /// See <https://man.archlinux.org/man/renameat2.2>.
    fn renameat2(
        &mut self,
        old_dirfd: &Self::FD,
        old_path: &str,
        new_dirfd: &Self::FD,
        new_path: &str,
        flags: RenameFlags,
    ) -> Result<(), Errno>;

    /// See <https://man.archlinux.org/man/core/man-pages/stat.2>.
    fn statat(&mut self, dirfd: &Self::FD, path: &str, flags: AtFlags) -> Result<Stat, Errno>;

    /// See <https://man.archlinux.org/man/unlinkat.2>.
    fn unlinkat(&mut self, dirfd: &Self::FD, path: &str, flags: AtFlags) -> Result<(), Errno>;

    /// Generate unique UUID,
    fn uuid(&mut self) -> Uuid;
}

pub trait IOFD: Debug + Send + Sync {}

impl<T> IOLayer for Arc<T>
where
    T: ?Sized + IOLayer,
{
    type Block = T::Block;
    type BlockPrepared = T::BlockPrepared;
    type FD = T::FD;
    type Mutex<T2>
        = T::Mutex<T2>
    where
        T2: Send + Debug;
    type RwLock<T2>
        = T::RwLock<T2>
    where
        T2: Send + Sync + Debug;

    fn prepare_block(&self) -> Self::BlockPrepared {
        self.as_ref().prepare_block()
    }

    fn create_mutex<T2>(&self, data: T2) -> Self::Mutex<T2>
    where
        T2: Send + Debug,
    {
        self.as_ref().create_mutex(data)
    }

    fn create_rwlock<T2>(&self, data: T2) -> Self::RwLock<T2>
    where
        T2: Send + Sync + Debug,
    {
        self.as_ref().create_rwlock(data)
    }
}

pub(crate) async fn spawn_io<IO, F, R>(io_layer: &IO, f: F) -> Result<R, object_store::Error>
where
    IO: IOLayer,
    F: for<'a> FnOnce(&'a mut IO::Block) -> Result<R, object_store::Error> + Send + 'static,
    R: Send + 'static,
{
    let mut prepared = io_layer.prepare_block();

    tokio::task::spawn_blocking(move || {
        let mut block = prepared.start();
        f(&mut block)
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

pub trait IOMutex<T>: Send + Sync + Debug
where
    T: Send + Debug,
{
    type Block: IOBlock;
    type Guard<'a>: IOMutexGuard<'a, T>
    where
        Self: 'a;

    fn lock<'a>(&'a self, io: &mut Self::Block) -> Self::Guard<'a>;
}

pub trait IOMutexGuard<'a, T>: Deref<Target = T> + DerefMut<Target = T> {}

pub trait IORwLock<T>: Send + Sync + Debug
where
    T: Send + Sync + Debug,
{
    type Block: IOBlock;
    type ReadGuard<'a>: IORwLockReadGuard<'a, T>
    where
        Self: 'a;
    type WriteGuard<'a>: IORwLockWriteGuard<'a, T>
    where
        Self: 'a;

    fn read<'a>(&'a self, io: &mut Self::Block) -> Self::ReadGuard<'a>;
    fn write<'a>(&'a self, io: &mut Self::Block) -> Self::WriteGuard<'a>;
}

pub trait IORwLockReadGuard<'a, T>: Deref<Target = T> {}

pub trait IORwLockWriteGuard<'a, T>: Deref<Target = T> + DerefMut<Target = T> {}

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
