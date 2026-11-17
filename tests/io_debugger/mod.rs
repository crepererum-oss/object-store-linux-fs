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

impl DebugIOBlock {
    fn event<F, T, O>(&mut self, evt: Event<F, T, O>) -> Result<T, Errno>
    where
        F: for<'a> FnOnce(&'a mut dyn IOBlock, &'a mut State) -> Result<T, Errno>,
        O: for<'a> FnOnce(&'a T) -> Value,
    {
        let Event {
            fun,
            args,
            f,
            map_ok,
        } = evt;

        let mut guard = self.state.lock().unwrap();
        let res = f(self.inner.as_mut(), &mut guard);
        guard.log.push(LogEntry {
            task_id: self.task_id,
            block_id: self.block_id,
            fun,
            args,
            res: res.as_ref().map(map_ok).map_err(|e| *e),
        });
        res
    }
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
        self.event(Event {
            fun: "fgetxattr",
            args: IndexMap::from([
                ("fd", Value::Ellipsis),
                ("name", Value::String(name.to_owned())),
                ("value", Value::Ellipsis),
            ]),
            f: |inner, _| inner.fgetxattr(fd, name, value),
            map_ok: |_| Value::Ellipsis,
        })
    }

    fn fgetxattr_query(&mut self, fd: &OwnedFd, name: &str) -> Result<usize, Errno> {
        self.event(Event {
            fun: "fgetxattr_query",
            args: IndexMap::from([
                ("fd", Value::Ellipsis),
                ("name", Value::String(name.to_owned())),
            ]),
            f: |inner, _| inner.fgetxattr_query(fd, name),
            map_ok: |_| Value::Ellipsis,
        })
    }

    fn flistxattr<'a>(
        &mut self,
        fd: &OwnedFd,
        list: &'a mut [MaybeUninit<u8>],
    ) -> Result<(&'a mut [u8], &'a mut [MaybeUninit<u8>]), Errno> {
        self.event(Event {
            fun: "flistxattr",
            args: IndexMap::from([("fd", Value::Ellipsis), ("list", Value::Ellipsis)]),
            f: |inner, _| inner.flistxattr(fd, list),
            map_ok: |_| Value::Ellipsis,
        })
    }

    fn flistxattr_query(&mut self, fd: &OwnedFd) -> Result<usize, Errno> {
        self.event(Event {
            fun: "flistxattr_query",
            args: IndexMap::from([("fd", Value::Ellipsis)]),
            f: |inner, _| inner.flistxattr_query(fd),
            map_ok: |_| Value::Ellipsis,
        })
    }

    fn flock(&mut self, fd: &OwnedFd, operation: rustix::fs::FlockOperation) -> Result<(), Errno> {
        self.event(Event {
            fun: "flock",
            args: IndexMap::from([("fd", Value::Ellipsis), ("operation", Value::Ellipsis)]),
            f: |inner, _| inner.flock(fd, operation),
            map_ok: |()| Value::Unit,
        })
    }

    fn fstat(&mut self, fd: &OwnedFd) -> Result<rustix::fs::Stat, Errno> {
        self.event(Event {
            fun: "fstat",
            args: IndexMap::from([("fd", Value::Ellipsis)]),
            f: |inner, _| inner.fstat(fd),
            map_ok: |_| Value::Ellipsis,
        })
    }

    fn fsetxattr(
        &mut self,
        fd: &OwnedFd,
        name: &str,
        value: &[u8],
        flags: rustix::fs::XattrFlags,
    ) -> Result<(), Errno> {
        self.event(Event {
            fun: "fstat",
            args: IndexMap::from([
                ("fd", Value::Ellipsis),
                ("name", Value::String(name.to_owned())),
                ("value", Value::Ellipsis),
                ("flags", Value::flags(flags)),
            ]),
            f: |inner, _| inner.fsetxattr(fd, name, value, flags),
            map_ok: |()| Value::Unit,
        })
    }

    fn fsync(&mut self, fd: &OwnedFd) -> Result<(), Errno> {
        self.event(Event {
            fun: "fsync",
            args: IndexMap::from([("fd", Value::Ellipsis)]),
            f: |inner, _| inner.fsync(fd),
            map_ok: |()| Value::Unit,
        })
    }

    fn getdents(
        &mut self,
        fd: &OwnedFd,
        buf: &mut Vec<MaybeUninit<u8>>,
    ) -> Result<Vec<(String, rustix::fs::FileType)>, Errno> {
        self.event(Event {
            fun: "getdents",
            args: IndexMap::from([("fd", Value::Ellipsis), ("buf", Value::Ellipsis)]),
            f: |inner, _| inner.getdents(fd, buf),
            map_ok: |_| Value::Ellipsis,
        })
    }

    fn linkat(
        &mut self,
        old_dirfd: &OwnedFd,
        old_path: &str,
        new_dirfd: &OwnedFd,
        new_path: &str,
        flags: rustix::fs::AtFlags,
    ) -> Result<(), Errno> {
        self.event(Event {
            fun: "linkat",
            args: IndexMap::from([
                ("old_dirfd", Value::Ellipsis),
                ("old_path", Value::String(old_path.to_owned())),
                ("new_dirfd", Value::Ellipsis),
                ("new_path", Value::String(new_path.to_owned())),
                ("flags", Value::flags(flags)),
            ]),
            f: |inner, _| inner.linkat(old_dirfd, old_path, new_dirfd, new_path, flags),
            map_ok: |()| Value::Unit,
        })
    }

    fn mkdirat(
        &mut self,
        dirfd: &OwnedFd,
        path: &str,
        mode: rustix::fs::Mode,
    ) -> Result<(), Errno> {
        self.event(Event {
            fun: "mkdirat",
            args: IndexMap::from([
                ("dirfd", Value::Ellipsis),
                ("path", Value::String(path.to_owned())),
                ("mode", Value::flags(mode)),
            ]),
            f: |inner, _| inner.mkdirat(dirfd, path, mode),
            map_ok: |()| Value::Unit,
        })
    }

    fn open(
        &mut self,
        path: &std::path::Path,
        flags: rustix::fs::OFlags,
        mode: rustix::fs::Mode,
    ) -> Result<OwnedFd, Errno> {
        self.event(Event {
            fun: "open",
            args: IndexMap::from([
                ("path", Value::Ellipsis),
                ("flags", Value::flags(flags)),
                ("mode", Value::flags(mode)),
            ]),
            f: |inner, _| inner.open(path, flags, mode),
            map_ok: |_| Value::Ellipsis,
        })
    }

    fn openat2(
        &mut self,
        dirfd: &OwnedFd,
        path: &str,
        oflags: rustix::fs::OFlags,
        mode: rustix::fs::Mode,
        resolve: rustix::fs::ResolveFlags,
    ) -> Result<OwnedFd, Errno> {
        self.event(Event {
            fun: "openat2",
            args: IndexMap::from([
                ("dirfd", Value::Ellipsis),
                ("path", Value::String(path.to_owned())),
                ("oflags", Value::flags(oflags)),
                ("mode", Value::flags(mode)),
                ("resolve", Value::flags(resolve)),
            ]),
            f: |inner, _| inner.openat2(dirfd, path, oflags, mode, resolve),
            map_ok: |_| Value::Ellipsis,
        })
    }

    fn preadv2(
        &mut self,
        fd: &OwnedFd,
        bufs: &mut [std::io::IoSliceMut<'_>],
        offset: u64,
        flags: rustix::io::ReadWriteFlags,
    ) -> Result<usize, Errno> {
        self.event(Event {
            fun: "pread2",
            args: IndexMap::from([
                ("fd", Value::Ellipsis),
                ("bufs", Value::Ellipsis),
                ("offset", Value::Ellipsis),
                ("flags", Value::flags(flags)),
            ]),
            f: |inner, _| inner.preadv2(fd, bufs, offset, flags),
            map_ok: |_| Value::Ellipsis,
        })
    }

    fn pwritev2(
        &mut self,
        fd: &OwnedFd,
        bufs: &[std::io::IoSlice<'_>],
        offset: u64,
        flags: rustix::io::ReadWriteFlags,
    ) -> Result<usize, Errno> {
        self.event(Event {
            fun: "pwrite2",
            args: IndexMap::from([
                ("fd", Value::Ellipsis),
                ("bufs", Value::Ellipsis),
                ("offset", Value::Ellipsis),
                ("flags", Value::flags(flags)),
            ]),
            f: |inner, _| inner.pwritev2(fd, bufs, offset, flags),
            map_ok: |_| Value::Ellipsis,
        })
    }

    fn renameat2(
        &mut self,
        old_dirfd: &OwnedFd,
        old_path: &str,
        new_dirfd: &OwnedFd,
        new_path: &str,
        flags: rustix::fs::RenameFlags,
    ) -> Result<(), Errno> {
        self.event(Event {
            fun: "renameat2",
            args: IndexMap::from([
                ("old_dirfd", Value::Ellipsis),
                ("old_path", Value::String(old_path.to_owned())),
                ("new_dirfd", Value::Ellipsis),
                ("new_path", Value::String(new_path.to_owned())),
                ("flags", Value::flags(flags)),
            ]),
            f: |inner, _| inner.renameat2(old_dirfd, old_path, new_dirfd, new_path, flags),
            map_ok: |()| Value::Unit,
        })
    }

    fn statat(
        &mut self,
        dirfd: &OwnedFd,
        path: &str,
        flags: rustix::fs::AtFlags,
    ) -> Result<rustix::fs::Stat, Errno> {
        self.event(Event {
            fun: "statat",
            args: IndexMap::from([
                ("dirfd", Value::Ellipsis),
                ("path", Value::String(path.to_owned())),
                ("flags", Value::flags(flags)),
            ]),
            f: |inner, _| inner.statat(dirfd, path, flags),
            map_ok: |_| Value::Ellipsis,
        })
    }

    fn unlinkat(
        &mut self,
        dirfd: &OwnedFd,
        path: &str,
        flags: rustix::fs::AtFlags,
    ) -> Result<(), Errno> {
        self.event(Event {
            fun: "unlinkat",
            args: IndexMap::from([
                ("dirfd", Value::Ellipsis),
                ("path", Value::String(path.to_owned())),
                ("flags", Value::flags(flags)),
            ]),
            f: |inner, _| inner.unlinkat(dirfd, path, flags),
            map_ok: |()| Value::Unit,
        })
    }

    fn uuid(&mut self) -> uuid::Uuid {
        self.event(Event {
            fun: "uuid",
            args: IndexMap::default(),
            f: |_, state| {
                let uuid = Uuid::from_u128(state.uuid_counter);
                state.uuid_counter += 1;
                Ok(uuid)
            },
            map_ok: |uuid| Value::String(uuid.to_string()),
        })
        .expect("never fails")
    }
}

struct Event<F, T, O>
where
    F: for<'a> FnOnce(&'a mut dyn IOBlock, &'a mut State) -> Result<T, Errno>,
    O: for<'a> FnOnce(&'a T) -> Value,
{
    fun: &'static str,
    args: IndexMap<&'static str, Value>,
    f: F,
    map_ok: O,
}
