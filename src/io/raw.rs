use std::{
    io::{IoSlice, IoSliceMut},
    mem::MaybeUninit,
    os::fd::OwnedFd,
};

use rustix::{
    fs::{
        AtFlags, FileType, FlockOperation, Mode, OFlags, RenameFlags, ResolveFlags, Stat,
        XattrFlags,
    },
    io::{Errno, ReadWriteFlags},
};
use uuid::Uuid;

use crate::io::{
    IOBlock, IOBlockPrepared, IOFD, IOLayer, IOMutex, IOMutexGuard, IORwLock, IORwLockReadGuard,
    IORwLockWriteGuard,
};

#[derive(Debug, Default)]
pub struct RawIOLayer;

#[derive(Debug)]
pub struct RawIOFD {
    inner: OwnedFd,
}

impl IOFD for RawIOFD {}

impl IOLayer for RawIOLayer {
    type Block = RawIOBlock;
    type BlockPrepared = RawIOBlockPrepared;
    type FD = RawIOFD;
    type Mutex<T>
        = parking_lot::Mutex<T>
    where
        T: Send + std::fmt::Debug;
    type RwLock<T>
        = parking_lot::RwLock<T>
    where
        T: Send + Sync + std::fmt::Debug;

    fn prepare_block(&self) -> Self::BlockPrepared {
        RawIOBlockPrepared { _priv: () }
    }

    fn create_mutex<T>(&self, data: T) -> Self::Mutex<T>
    where
        T: Send + std::fmt::Debug,
    {
        parking_lot::Mutex::new(data)
    }

    fn create_rwlock<T>(&self, data: T) -> Self::RwLock<T>
    where
        T: Send + Sync + std::fmt::Debug,
    {
        parking_lot::RwLock::new(data)
    }
}

pub struct RawIOBlockPrepared {
    // prevent public construction
    _priv: (),
}

impl IOBlockPrepared for RawIOBlockPrepared {
    type Block = RawIOBlock;

    fn start(&mut self) -> Self::Block {
        RawIOBlock { _priv: () }
    }
}

pub struct RawIOBlock {
    // prevent public construction
    _priv: (),
}

impl IOBlock for RawIOBlock {
    type FD = RawIOFD;

    fn fgetxattr<'a>(
        &mut self,
        fd: &Self::FD,
        name: &str,
        value: &'a mut [MaybeUninit<u8>],
    ) -> Result<(&'a mut [u8], &'a mut [MaybeUninit<u8>]), Errno> {
        rustix::fs::fgetxattr(&fd.inner, name, value)
    }

    fn fgetxattr_query(&mut self, fd: &Self::FD, name: &str) -> Result<usize, Errno> {
        rustix::fs::fgetxattr_query(&fd.inner, name)
    }

    fn flistxattr<'a>(
        &mut self,
        fd: &Self::FD,
        list: &'a mut [MaybeUninit<u8>],
    ) -> Result<(&'a mut [u8], &'a mut [MaybeUninit<u8>]), Errno> {
        rustix::fs::flistxattr(&fd.inner, list)
    }

    fn flistxattr_query(&mut self, fd: &Self::FD) -> Result<usize, Errno> {
        rustix::fs::flistxattr_query(&fd.inner)
    }

    fn flock(&mut self, fd: &Self::FD, operation: FlockOperation) -> Result<(), Errno> {
        rustix::fs::flock(&fd.inner, operation)
    }

    fn fstat(&mut self, fd: &Self::FD) -> Result<Stat, Errno> {
        rustix::fs::fstat(&fd.inner)
    }

    fn fsetxattr(
        &mut self,
        fd: &Self::FD,
        name: &str,
        value: &[u8],
        flags: XattrFlags,
    ) -> Result<(), Errno> {
        rustix::fs::fsetxattr(&fd.inner, name, value, flags)
    }

    fn fsync(&mut self, fd: &Self::FD) -> Result<(), Errno> {
        rustix::fs::fsync(&fd.inner)
    }

    fn getdents(
        &mut self,
        fd: &Self::FD,
        buf: &mut Vec<MaybeUninit<u8>>,
    ) -> Result<Vec<(String, FileType)>, Errno> {
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

    fn linkat(
        &mut self,
        old_dirfd: &Self::FD,
        old_path: &str,
        new_dirfd: &Self::FD,
        new_path: &str,
        flags: AtFlags,
    ) -> Result<(), Errno> {
        rustix::fs::linkat(
            &old_dirfd.inner,
            old_path,
            &new_dirfd.inner,
            new_path,
            flags,
        )
    }

    fn mkdirat(&mut self, dirfd: &Self::FD, path: &str, mode: Mode) -> Result<(), Errno> {
        rustix::fs::mkdirat(&dirfd.inner, path, mode)
    }

    fn open(
        &mut self,
        path: &std::path::Path,
        flags: OFlags,
        mode: Mode,
    ) -> Result<Self::FD, Errno> {
        let inner = rustix::fs::open(path, flags, mode)?;
        Ok(RawIOFD { inner })
    }

    fn openat2(
        &mut self,
        dirfd: &Self::FD,
        path: &str,
        oflags: OFlags,
        mode: Mode,
        resolve: ResolveFlags,
    ) -> Result<Self::FD, Errno> {
        let inner = rustix::fs::openat2(&dirfd.inner, path, oflags, mode, resolve)?;
        Ok(RawIOFD { inner })
    }

    fn preadv2(
        &mut self,
        fd: &Self::FD,
        bufs: &mut [IoSliceMut<'_>],
        offset: u64,
        flags: ReadWriteFlags,
    ) -> Result<usize, Errno> {
        rustix::io::preadv2(&fd.inner, bufs, offset, flags)
    }

    fn pwritev2(
        &mut self,
        fd: &Self::FD,
        bufs: &[IoSlice<'_>],
        offset: u64,
        flags: ReadWriteFlags,
    ) -> Result<usize, Errno> {
        rustix::io::pwritev2(&fd.inner, bufs, offset, flags)
    }

    fn renameat2(
        &mut self,
        old_dirfd: &Self::FD,
        old_path: &str,
        new_dirfd: &Self::FD,
        new_path: &str,
        flags: RenameFlags,
    ) -> Result<(), Errno> {
        rustix::fs::renameat_with(
            &old_dirfd.inner,
            old_path,
            &new_dirfd.inner,
            new_path,
            flags,
        )
    }

    fn statat(&mut self, dirfd: &Self::FD, path: &str, flags: AtFlags) -> Result<Stat, Errno> {
        rustix::fs::statat(&dirfd.inner, path, flags)
    }

    fn unlinkat(&mut self, dirfd: &Self::FD, path: &str, flags: AtFlags) -> Result<(), Errno> {
        rustix::fs::unlinkat(&dirfd.inner, path, flags)
    }

    fn uuid(&mut self) -> Uuid {
        Uuid::new_v4()
    }
}

impl<T> IOMutex<T> for parking_lot::Mutex<T>
where
    T: Send + std::fmt::Debug,
{
    type Block = RawIOBlock;

    type Guard<'a>
        = parking_lot::MutexGuard<'a, T>
    where
        Self: 'a;

    fn lock<'a>(&'a self, _io: &mut Self::Block) -> Self::Guard<'a> {
        self.lock()
    }
}

impl<'a, T> IOMutexGuard<'a, T> for parking_lot::MutexGuard<'a, T> {}

impl<T> IORwLock<T> for parking_lot::RwLock<T>
where
    T: Send + Sync + std::fmt::Debug,
{
    type Block = RawIOBlock;

    type ReadGuard<'a>
        = parking_lot::RwLockReadGuard<'a, T>
    where
        Self: 'a;
    type WriteGuard<'a>
        = parking_lot::RwLockWriteGuard<'a, T>
    where
        Self: 'a;

    fn read<'a>(&'a self, _io: &mut Self::Block) -> Self::ReadGuard<'a> {
        self.read()
    }

    fn write<'a>(&'a self, _io: &mut Self::Block) -> Self::WriteGuard<'a> {
        self.write()
    }
}

impl<'a, T> IORwLockReadGuard<'a, T> for parking_lot::RwLockReadGuard<'a, T> {}

impl<'a, T> IORwLockWriteGuard<'a, T> for parking_lot::RwLockWriteGuard<'a, T> {}
