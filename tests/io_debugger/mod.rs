use std::{
    fmt::Write,
    mem::MaybeUninit,
    os::unix::prelude::OwnedFd,
    sync::{Arc, Mutex},
};

use indexmap::IndexMap;
use object_store_linux_fs::io::{IOBlock, IOLayer, RustixIOLayer};
use rustix::io::Errno;
use uuid::Uuid;

use crate::io_debugger::log::{LogEntry, Value};

pub mod log;

#[derive(Debug, Default)]
struct State {
    log: Vec<LogEntry>,
    block_counter: u64,
    uuid_counter: u128,
}

#[derive(Debug)]
pub struct DebugIOLayer {
    inner: RustixIOLayer,
    state: Arc<Mutex<State>>,
}

impl DebugIOLayer {
    pub fn new() -> Self {
        Self {
            inner: RustixIOLayer,
            state: Default::default(),
        }
    }

    pub fn log(&self) -> String {
        let mut out = String::new();
        let guard = self.state.lock().unwrap();
        for entry in &guard.log {
            writeln!(&mut out, "{entry}").unwrap();
        }
        out
    }
}

impl IOLayer for DebugIOLayer {
    fn start_block(&self) -> Box<dyn IOBlock> {
        let mut guard = self.state.lock().unwrap();

        let task_id = 0; // TODO

        let block_id = guard.block_counter;
        guard.block_counter += 1;

        guard.log.push(LogEntry {
            task_id,
            block_id,
            fun: "block-start",
            args: IndexMap::new(),
            res: Ok(Value::Unit),
        });

        Box::new(DebugIOBlock {
            inner: self.inner.start_block(),
            task_id,
            block_id,
            state: Arc::clone(&self.state),
        })
    }
}

struct DebugIOBlock {
    inner: Box<dyn IOBlock>,
    task_id: u64,
    block_id: u64,
    state: Arc<Mutex<State>>,
}

impl Drop for DebugIOBlock {
    fn drop(&mut self) {
        let mut guard = self.state.lock().unwrap();
        guard.log.push(LogEntry {
            task_id: self.task_id,
            block_id: self.block_id,
            fun: "block-end",
            args: IndexMap::new(),
            res: Ok(Value::Unit),
        });
    }
}

impl IOBlock for DebugIOBlock {
    fn fgetxattr<'a>(
        &mut self,
        fd: &OwnedFd,
        name: &str,
        value: &'a mut [MaybeUninit<u8>],
    ) -> Result<(&'a mut [u8], &'a mut [MaybeUninit<u8>]), Errno> {
        let mut guard = self.state.lock().unwrap();
        let res = self.inner.fgetxattr(fd, name, value);
        guard.log.push(LogEntry {
            task_id: self.task_id,
            block_id: self.block_id,
            fun: "fgetxattr",
            args: IndexMap::from([
                ("fd", Value::Ellipsis),
                ("name", Value::Ellipsis),
                ("value", Value::Ellipsis),
            ]),
            res: res.as_ref().map(|_| Value::Ellipsis).map_err(|e| *e),
        });
        res
    }

    fn fgetxattr_query(&mut self, fd: &OwnedFd, name: &str) -> Result<usize, Errno> {
        todo!()
    }

    fn flistxattr<'a>(
        &mut self,
        fd: &OwnedFd,
        list: &'a mut [MaybeUninit<u8>],
    ) -> Result<(&'a mut [u8], &'a mut [MaybeUninit<u8>]), Errno> {
        todo!()
    }

    fn flistxattr_query(&mut self, fd: &OwnedFd) -> Result<usize, Errno> {
        todo!()
    }

    fn flock(&mut self, fd: &OwnedFd, operation: rustix::fs::FlockOperation) -> Result<(), Errno> {
        let mut guard = self.state.lock().unwrap();
        let res = self.inner.flock(fd, operation);
        guard.log.push(LogEntry {
            task_id: self.task_id,
            block_id: self.block_id,
            fun: "flock",
            args: IndexMap::from([("fd", Value::Ellipsis), ("operation", Value::Ellipsis)]),
            res: res.as_ref().map(|()| Value::Unit).map_err(|e| *e),
        });
        res
    }

    fn fstat(&mut self, fd: &OwnedFd) -> Result<rustix::fs::Stat, Errno> {
        let mut guard = self.state.lock().unwrap();
        let res = self.inner.fstat(fd);
        guard.log.push(LogEntry {
            task_id: self.task_id,
            block_id: self.block_id,
            fun: "fstat",
            args: IndexMap::from([("fd", Value::Ellipsis)]),
            res: res.as_ref().map(|_| Value::Ellipsis).map_err(|e| *e),
        });
        res
    }

    fn fsetxattr(
        &mut self,
        fd: &OwnedFd,
        name: &str,
        value: &[u8],
        flags: rustix::fs::XattrFlags,
    ) -> Result<(), Errno> {
        todo!()
    }

    fn fsync(&mut self, fd: &OwnedFd) -> Result<(), Errno> {
        let mut guard = self.state.lock().unwrap();
        let res = self.inner.fsync(fd);
        guard.log.push(LogEntry {
            task_id: self.task_id,
            block_id: self.block_id,
            fun: "fsync",
            args: IndexMap::from([("fd", Value::Ellipsis)]),
            res: res.as_ref().map(|()| Value::Unit).map_err(|e| *e),
        });
        res
    }

    fn getdents(
        &mut self,
        fd: &OwnedFd,
        buf: &mut Vec<MaybeUninit<u8>>,
    ) -> Result<Vec<(String, rustix::fs::FileType)>, Errno> {
        todo!()
    }

    fn linkat(
        &mut self,
        old_dirfd: &OwnedFd,
        old_path: &str,
        new_dirfd: &OwnedFd,
        new_path: &str,
        flags: rustix::fs::AtFlags,
    ) -> Result<(), Errno> {
        let mut guard = self.state.lock().unwrap();
        let res = self
            .inner
            .linkat(old_dirfd, old_path, new_dirfd, new_path, flags);
        guard.log.push(LogEntry {
            task_id: self.task_id,
            block_id: self.block_id,
            fun: "linkat",
            args: IndexMap::from([
                ("old_dirfd", Value::Ellipsis),
                ("old_path", Value::String(old_path.to_owned())),
                ("new_dirfd", Value::Ellipsis),
                ("new_path", Value::String(new_path.to_owned())),
                ("flags", Value::Ellipsis),
            ]),
            res: res.as_ref().map(|()| Value::Unit).map_err(|e| *e),
        });
        res
    }

    fn mkdirat(
        &mut self,
        dirfd: &OwnedFd,
        path: &str,
        mode: rustix::fs::Mode,
    ) -> Result<(), Errno> {
        todo!()
    }

    fn open(
        &mut self,
        path: &std::path::Path,
        flags: rustix::fs::OFlags,
        mode: rustix::fs::Mode,
    ) -> Result<OwnedFd, Errno> {
        let mut guard = self.state.lock().unwrap();
        let res = self.inner.open(path, flags, mode);
        guard.log.push(LogEntry {
            task_id: self.task_id,
            block_id: self.block_id,
            fun: "open",
            args: IndexMap::from([
                ("path", Value::Ellipsis),
                ("flags", Value::Ellipsis),
                ("mode", Value::Ellipsis),
            ]),
            res: res.as_ref().map(|_| Value::Ellipsis).map_err(|e| *e),
        });
        res
    }

    fn openat2(
        &mut self,
        dirfd: &OwnedFd,
        path: &str,
        oflags: rustix::fs::OFlags,
        mode: rustix::fs::Mode,
        resolve: rustix::fs::ResolveFlags,
    ) -> Result<OwnedFd, Errno> {
        let mut guard = self.state.lock().unwrap();
        let res = self.inner.openat2(dirfd, path, oflags, mode, resolve);
        guard.log.push(LogEntry {
            task_id: self.task_id,
            block_id: self.block_id,
            fun: "openat2",
            args: IndexMap::from([
                ("dirfd", Value::Ellipsis),
                ("path", Value::String(path.to_owned())),
                ("oflags", Value::Ellipsis),
                ("mode", Value::Ellipsis),
                ("resolve", Value::Ellipsis),
            ]),
            res: res.as_ref().map(|_| Value::Ellipsis).map_err(|e| *e),
        });
        res
    }

    fn preadv2(
        &mut self,
        fd: &OwnedFd,
        bufs: &mut [std::io::IoSliceMut<'_>],
        offset: u64,
        flags: rustix::io::ReadWriteFlags,
    ) -> Result<usize, Errno> {
        todo!()
    }

    fn pwritev2(
        &mut self,
        fd: &OwnedFd,
        bufs: &[std::io::IoSlice<'_>],
        offset: u64,
        flags: rustix::io::ReadWriteFlags,
    ) -> Result<usize, Errno> {
        todo!()
    }

    fn renameat2(
        &mut self,
        old_dirfd: &OwnedFd,
        old_path: &str,
        new_dirfd: &OwnedFd,
        new_path: &str,
        flags: rustix::fs::RenameFlags,
    ) -> Result<(), Errno> {
        let mut guard = self.state.lock().unwrap();
        let res = self
            .inner
            .renameat2(old_dirfd, old_path, new_dirfd, new_path, flags);
        guard.log.push(LogEntry {
            task_id: self.task_id,
            block_id: self.block_id,
            fun: "renameat2",
            args: IndexMap::from([
                ("old_dirfd", Value::Ellipsis),
                ("old_path", Value::String(old_path.to_owned())),
                ("new_dirfd", Value::Ellipsis),
                ("new_path", Value::String(new_path.to_owned())),
                ("flags", Value::Ellipsis),
            ]),
            res: res.as_ref().map(|()| Value::Unit).map_err(|e| *e),
        });
        res
    }

    fn statat(
        &mut self,
        dirfd: &OwnedFd,
        path: &str,
        flags: rustix::fs::AtFlags,
    ) -> Result<rustix::fs::Stat, Errno> {
        todo!()
    }

    fn unlinkat(
        &mut self,
        dirfd: &OwnedFd,
        path: &str,
        flags: rustix::fs::AtFlags,
    ) -> Result<(), Errno> {
        todo!()
    }

    fn uuid(&mut self) -> uuid::Uuid {
        let mut guard = self.state.lock().unwrap();
        let uuid = Uuid::from_u128(guard.uuid_counter);
        guard.uuid_counter += 1;
        uuid
    }
}
