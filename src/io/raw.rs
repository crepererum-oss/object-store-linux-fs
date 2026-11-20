use std::{
    any::Any,
    io::{IoSlice, IoSliceMut},
    mem::MaybeUninit,
    os::fd::OwnedFd,
};

use lock_api::{GuardNoSend, RawMutex, RawRwLock};
use rustix::{
    fs::{
        AtFlags, FileType, FlockOperation, Mode, OFlags, RenameFlags, ResolveFlags, Stat,
        XattrFlags,
    },
    io::{Errno, ReadWriteFlags},
};
use uuid::Uuid;

use crate::io::{
    IOBlock, IOBlockPrepared, IOFD, IOLayer, IORawMutex, IORawMutexGuard, IORawRwLock,
};

#[derive(Debug, Default)]
pub struct RawIOLayer;

struct ParkingLotIORawMutex {
    lock: parking_lot::RawMutex,
}

impl IORawMutex for ParkingLotIORawMutex {
    unsafe fn unlock(&self, guard: Box<dyn IORawMutexGuard>) {
        let guard = *(guard as Box<dyn Any>)
            .downcast::<ParkingLotIORawLockGuard>()
            .expect("wrong type");

        // SAFETY: caller
        unsafe {
            self.lock.unlock(guard.guard);
        }
    }
}

struct ParkingLotIORawRwLock {
    lock: parking_lot::RawRwLock,
}

impl IORawRwLock for ParkingLotIORawRwLock {
    unsafe fn unlock_shared(&self, guard: Box<dyn IORawMutexGuard>) {
        let guard = *(guard as Box<dyn Any>)
            .downcast::<ParkingLotIORawLockGuard>()
            .expect("wrong type");

        // SAFETY: caller
        unsafe {
            self.lock.unlock_shared(guard.guard);
        }
    }

    unsafe fn unlock_exclusive(&self, guard: Box<dyn IORawMutexGuard>) {
        let guard = *(guard as Box<dyn Any>)
            .downcast::<ParkingLotIORawLockGuard>()
            .expect("wrong type");

        // SAFETY: caller
        unsafe {
            self.lock.unlock_exclusive(guard.guard);
        }
    }
}

struct ParkingLotIORawLockGuard {
    guard: GuardNoSend,
}

impl IORawMutexGuard for ParkingLotIORawLockGuard {}

#[derive(Debug)]
struct RawIOFD {
    inner: OwnedFd,
}

impl IOFD for RawIOFD {}

impl IOLayer for RawIOLayer {
    type BlockPrepared = RawIOBlockPrepared;

    fn prepare_block(&self) -> Self::BlockPrepared {
        RawIOBlockPrepared { _priv: () }
    }

    fn create_raw_mutex(&self) -> Box<dyn IORawMutex> {
        Box::new(ParkingLotIORawMutex {
            lock: parking_lot::RawMutex::INIT,
        })
    }

    fn create_raw_rwlock(&self) -> Box<dyn IORawRwLock> {
        Box::new(ParkingLotIORawRwLock {
            lock: parking_lot::RawRwLock::INIT,
        })
    }
}

pub struct RawIOBlockPrepared {
    // prevent public construction
    _priv: (),
}

impl IOBlockPrepared for RawIOBlockPrepared {
    fn start(&mut self) -> Box<dyn IOBlock> {
        Box::new(RawIOBlock)
    }
}

struct RawIOBlock;

impl IOBlock for RawIOBlock {
    fn fgetxattr<'a>(
        &mut self,
        fd: &dyn IOFD,
        name: &str,
        value: &'a mut [MaybeUninit<u8>],
    ) -> Result<(&'a mut [u8], &'a mut [MaybeUninit<u8>]), Errno> {
        let fd = (fd as &dyn Any)
            .downcast_ref::<RawIOFD>()
            .expect("wrong type");
        rustix::fs::fgetxattr(&fd.inner, name, value)
    }

    fn fgetxattr_query(&mut self, fd: &dyn IOFD, name: &str) -> Result<usize, Errno> {
        let fd = (fd as &dyn Any)
            .downcast_ref::<RawIOFD>()
            .expect("wrong type");
        rustix::fs::fgetxattr_query(&fd.inner, name)
    }

    fn flistxattr<'a>(
        &mut self,
        fd: &dyn IOFD,
        list: &'a mut [MaybeUninit<u8>],
    ) -> Result<(&'a mut [u8], &'a mut [MaybeUninit<u8>]), Errno> {
        let fd = (fd as &dyn Any)
            .downcast_ref::<RawIOFD>()
            .expect("wrong type");
        rustix::fs::flistxattr(&fd.inner, list)
    }

    fn flistxattr_query(&mut self, fd: &dyn IOFD) -> Result<usize, Errno> {
        let fd = (fd as &dyn Any)
            .downcast_ref::<RawIOFD>()
            .expect("wrong type");
        rustix::fs::flistxattr_query(&fd.inner)
    }

    fn flock(&mut self, fd: &dyn IOFD, operation: FlockOperation) -> Result<(), Errno> {
        let fd = (fd as &dyn Any)
            .downcast_ref::<RawIOFD>()
            .expect("wrong type");
        rustix::fs::flock(&fd.inner, operation)
    }

    fn fstat(&mut self, fd: &dyn IOFD) -> Result<Stat, Errno> {
        let fd = (fd as &dyn Any)
            .downcast_ref::<RawIOFD>()
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
            .downcast_ref::<RawIOFD>()
            .expect("wrong type");
        rustix::fs::fsetxattr(&fd.inner, name, value, flags)
    }

    fn fsync(&mut self, fd: &dyn IOFD) -> Result<(), Errno> {
        let fd = (fd as &dyn Any)
            .downcast_ref::<RawIOFD>()
            .expect("wrong type");
        rustix::fs::fsync(&fd.inner)
    }

    fn getdents(
        &mut self,
        fd: &dyn IOFD,
        buf: &mut Vec<MaybeUninit<u8>>,
    ) -> Result<Vec<(String, FileType)>, Errno> {
        let fd = (fd as &dyn Any)
            .downcast_ref::<RawIOFD>()
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
            .downcast_ref::<ParkingLotIORawMutex>()
            .expect("wrong type")
            .lock
            .lock();
        Box::new(ParkingLotIORawLockGuard { guard })
    }

    fn lock_exclusive(&mut self, raw_lock: &dyn IORawRwLock) -> Box<dyn IORawMutexGuard> {
        let guard = (raw_lock as &dyn Any)
            .downcast_ref::<ParkingLotIORawRwLock>()
            .expect("wrong type")
            .lock
            .lock_exclusive();
        Box::new(ParkingLotIORawLockGuard { guard })
    }

    fn lock_shared(&mut self, raw_lock: &dyn IORawRwLock) -> Box<dyn IORawMutexGuard> {
        let guard = (raw_lock as &dyn Any)
            .downcast_ref::<ParkingLotIORawRwLock>()
            .expect("wrong type")
            .lock
            .lock_shared();
        Box::new(ParkingLotIORawLockGuard { guard })
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
            .downcast_ref::<RawIOFD>()
            .expect("wrong type");
        let new_dirfd = (new_dirfd as &dyn Any)
            .downcast_ref::<RawIOFD>()
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
            .downcast_ref::<RawIOFD>()
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
        Ok(Box::new(RawIOFD { inner }))
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
            .downcast_ref::<RawIOFD>()
            .expect("wrong type");
        let inner = rustix::fs::openat2(&dirfd.inner, path, oflags, mode, resolve)?;
        Ok(Box::new(RawIOFD { inner }))
    }

    fn preadv2(
        &mut self,
        fd: &dyn IOFD,
        bufs: &mut [IoSliceMut<'_>],
        offset: u64,
        flags: ReadWriteFlags,
    ) -> Result<usize, Errno> {
        let fd = (fd as &dyn Any)
            .downcast_ref::<RawIOFD>()
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
            .downcast_ref::<RawIOFD>()
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
            .downcast_ref::<RawIOFD>()
            .expect("wrong type");
        let new_dirfd = (new_dirfd as &dyn Any)
            .downcast_ref::<RawIOFD>()
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
            .downcast_ref::<RawIOFD>()
            .expect("wrong type");
        rustix::fs::statat(&dirfd.inner, path, flags)
    }

    fn unlinkat(&mut self, dirfd: &dyn IOFD, path: &str, flags: AtFlags) -> Result<(), Errno> {
        let dirfd = (dirfd as &dyn Any)
            .downcast_ref::<RawIOFD>()
            .expect("wrong type");
        rustix::fs::unlinkat(&dirfd.inner, path, flags)
    }

    fn uuid(&mut self) -> Uuid {
        Uuid::new_v4()
    }
}
