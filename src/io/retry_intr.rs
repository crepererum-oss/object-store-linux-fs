use std::marker::PhantomData;

use crate::io::{IOBlock, IOBlockPrepared, IOLayer, IOMutex, IORwLock};

#[derive(Debug, Default)]
pub struct RetryIntrLayer<IO>
where
    IO: IOLayer,
{
    inner: IO,
}

impl<IO> RetryIntrLayer<IO>
where
    IO: IOLayer,
{
    pub fn new(inner: IO) -> Self {
        Self { inner }
    }

    pub fn inner(&self) -> &IO {
        &self.inner
    }
}

impl<IO> IOLayer for RetryIntrLayer<IO>
where
    IO: IOLayer,
{
    type Block = RetryIntrBlock<IO::Block>;

    type BlockPrepared = RetryIntrPreparedBlock<IO::BlockPrepared>;

    type FD = IO::FD;

    type Mutex<T>
        = RetryIntrMutex<IO::Mutex<T>, T>
    where
        T: Send + std::fmt::Debug;

    type RwLock<T>
        = RetryIntrRwLock<IO::RwLock<T>, T>
    where
        T: Send + Sync + std::fmt::Debug;

    fn prepare_block(&self) -> Self::BlockPrepared {
        RetryIntrPreparedBlock {
            inner: self.inner.prepare_block(),
        }
    }

    fn create_mutex<T>(&self, data: T) -> Self::Mutex<T>
    where
        T: Send + std::fmt::Debug,
    {
        RetryIntrMutex {
            inner: self.inner.create_mutex(data),
            _phantom: PhantomData,
        }
    }

    fn create_rwlock<T>(&self, data: T) -> Self::RwLock<T>
    where
        T: Send + Sync + std::fmt::Debug,
    {
        RetryIntrRwLock {
            inner: self.inner.create_rwlock(data),
            _phantom: PhantomData,
        }
    }
}

pub struct RetryIntrPreparedBlock<P>
where
    P: IOBlockPrepared,
{
    inner: P,
}

impl<P> IOBlockPrepared for RetryIntrPreparedBlock<P>
where
    P: IOBlockPrepared,
{
    type Block = RetryIntrBlock<P::Block>;

    fn start(&mut self) -> Self::Block {
        RetryIntrBlock {
            inner: self.inner.start(),
        }
    }
}

pub struct RetryIntrBlock<B>
where
    B: IOBlock,
{
    inner: B,
}

impl<B> IOBlock for RetryIntrBlock<B>
where
    B: IOBlock,
{
    type FD = B::FD;

    fn fgetxattr<'a>(
        &mut self,
        fd: &Self::FD,
        name: &str,
        value: &'a mut [std::mem::MaybeUninit<u8>],
    ) -> Result<(&'a mut [u8], &'a mut [std::mem::MaybeUninit<u8>]), rustix::io::Errno> {
        // no EINTR
        self.inner.fgetxattr(fd, name, value)
    }

    fn fgetxattr_query(&mut self, fd: &Self::FD, name: &str) -> Result<usize, rustix::io::Errno> {
        // no EINTR
        self.inner.fgetxattr_query(fd, name)
    }

    fn flistxattr<'a>(
        &mut self,
        fd: &Self::FD,
        list: &'a mut [std::mem::MaybeUninit<u8>],
    ) -> Result<(&'a mut [u8], &'a mut [std::mem::MaybeUninit<u8>]), rustix::io::Errno> {
        // no EINTR
        self.inner.flistxattr(fd, list)
    }

    fn flistxattr_query(&mut self, fd: &Self::FD) -> Result<usize, rustix::io::Errno> {
        // no EINTR
        self.inner.flistxattr_query(fd)
    }

    fn flock(
        &mut self,
        fd: &Self::FD,
        operation: rustix::fs::FlockOperation,
    ) -> Result<(), rustix::io::Errno> {
        rustix::io::retry_on_intr(|| self.inner.flock(fd, operation))
    }

    fn fstat(&mut self, fd: &Self::FD) -> Result<rustix::fs::Stat, rustix::io::Errno> {
        // no EINTR
        self.inner.fstat(fd)
    }

    fn fsetxattr(
        &mut self,
        fd: &Self::FD,
        name: &str,
        value: &[u8],
        flags: rustix::fs::XattrFlags,
    ) -> Result<(), rustix::io::Errno> {
        // no EINTR
        self.inner.fsetxattr(fd, name, value, flags)
    }

    fn fsync(&mut self, fd: &Self::FD) -> Result<(), rustix::io::Errno> {
        rustix::io::retry_on_intr(|| self.inner.fsync(fd))
    }

    fn getdents(
        &mut self,
        fd: &Self::FD,
        buf: &mut Vec<std::mem::MaybeUninit<u8>>,
    ) -> Result<Vec<(String, rustix::fs::FileType)>, rustix::io::Errno> {
        // no EINTR
        self.inner.getdents(fd, buf)
    }

    fn linkat(
        &mut self,
        old_dirfd: &Self::FD,
        old_path: &str,
        new_dirfd: &Self::FD,
        new_path: &str,
        flags: rustix::fs::AtFlags,
    ) -> Result<(), rustix::io::Errno> {
        // no EINTR
        self.inner
            .linkat(old_dirfd, old_path, new_dirfd, new_path, flags)
    }

    fn mkdirat(
        &mut self,
        dirfd: &Self::FD,
        path: &str,
        mode: rustix::fs::Mode,
    ) -> Result<(), rustix::io::Errno> {
        // no EINTR
        self.inner.mkdirat(dirfd, path, mode)
    }

    fn open(
        &mut self,
        path: &std::path::Path,
        flags: rustix::fs::OFlags,
        mode: rustix::fs::Mode,
    ) -> Result<Self::FD, rustix::io::Errno> {
        rustix::io::retry_on_intr(|| self.inner.open(path, flags, mode))
    }

    fn openat2(
        &mut self,
        dirfd: &Self::FD,
        path: &str,
        oflags: rustix::fs::OFlags,
        mode: rustix::fs::Mode,
        resolve: rustix::fs::ResolveFlags,
    ) -> Result<Self::FD, rustix::io::Errno> {
        rustix::io::retry_on_intr(|| self.inner.openat2(dirfd, path, oflags, mode, resolve))
    }

    fn preadv2(
        &mut self,
        fd: &Self::FD,
        bufs: &mut [std::io::IoSliceMut<'_>],
        offset: u64,
        flags: rustix::io::ReadWriteFlags,
    ) -> Result<usize, rustix::io::Errno> {
        rustix::io::retry_on_intr(|| self.inner.preadv2(fd, bufs, offset, flags))
    }

    fn pwritev2(
        &mut self,
        fd: &Self::FD,
        bufs: &[std::io::IoSlice<'_>],
        offset: u64,
        flags: rustix::io::ReadWriteFlags,
    ) -> Result<usize, rustix::io::Errno> {
        rustix::io::retry_on_intr(|| self.inner.pwritev2(fd, bufs, offset, flags))
    }

    fn renameat2(
        &mut self,
        old_dirfd: &Self::FD,
        old_path: &str,
        new_dirfd: &Self::FD,
        new_path: &str,
        flags: rustix::fs::RenameFlags,
    ) -> Result<(), rustix::io::Errno> {
        // no EINTR
        self.inner
            .renameat2(old_dirfd, old_path, new_dirfd, new_path, flags)
    }

    fn statat(
        &mut self,
        dirfd: &Self::FD,
        path: &str,
        flags: rustix::fs::AtFlags,
    ) -> Result<rustix::fs::Stat, rustix::io::Errno> {
        // no EINTR
        self.inner.statat(dirfd, path, flags)
    }

    fn unlinkat(
        &mut self,
        dirfd: &Self::FD,
        path: &str,
        flags: rustix::fs::AtFlags,
    ) -> Result<(), rustix::io::Errno> {
        // no EINTR
        self.inner.unlinkat(dirfd, path, flags)
    }

    fn uuid(&mut self) -> uuid::Uuid {
        // cannot fail
        self.inner.uuid()
    }
}

#[derive(Debug)]
pub struct RetryIntrMutex<M, T>
where
    M: IOMutex<T>,
    T: Send + std::fmt::Debug,
{
    inner: M,
    _phantom: PhantomData<fn() -> T>,
}

impl<M, T> IOMutex<T> for RetryIntrMutex<M, T>
where
    M: IOMutex<T>,
    T: Send + std::fmt::Debug,
{
    type Block = RetryIntrBlock<M::Block>;

    type Guard<'a>
        = M::Guard<'a>
    where
        Self: 'a;

    fn lock<'a>(&'a self, io: &mut Self::Block) -> Self::Guard<'a> {
        self.inner.lock(&mut io.inner)
    }
}

#[derive(Debug)]
pub struct RetryIntrRwLock<L, T>
where
    L: IORwLock<T>,
    T: Send + Sync + std::fmt::Debug,
{
    inner: L,
    _phantom: PhantomData<fn() -> T>,
}

impl<L, T> IORwLock<T> for RetryIntrRwLock<L, T>
where
    L: IORwLock<T>,
    T: Send + Sync + std::fmt::Debug,
{
    type Block = RetryIntrBlock<L::Block>;

    type ReadGuard<'a>
        = L::ReadGuard<'a>
    where
        Self: 'a;

    type WriteGuard<'a>
        = L::WriteGuard<'a>
    where
        Self: 'a;

    fn read<'a>(&'a self, io: &mut Self::Block) -> Self::ReadGuard<'a> {
        self.inner.read(&mut io.inner)
    }

    fn write<'a>(&'a self, io: &mut Self::Block) -> Self::WriteGuard<'a> {
        self.inner.write(&mut io.inner)
    }
}
