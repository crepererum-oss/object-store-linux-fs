use std::{
    any::Any,
    cell::Cell,
    fmt::Write,
    marker::PhantomData,
    mem::MaybeUninit,
    sync::{Arc, Mutex},
};

use indexmap::IndexMap;
use object_store_linux_fs::io::{
    DefaultIOLayer, IOBlock, IOBlockPrepared, IOFD, IOLayer, IORawMutex, IORawMutexGuard,
    IORawRwLock,
};
use rustix::io::Errno;
use uuid::Uuid;

use crate::io_debugger::{
    log::{LogEntry, Value},
    order::{Manipulation, Order},
};

pub mod log;
pub mod order;
mod tests;

tokio::task_local! {
    static ASYNC_TASK_ID: u64;
}

thread_local! {
    static SYNC_TASK_ID: Cell<Option<u64>> = const {Cell::new(None)};
    static SYNC_BLOCK_ID: Cell<Option<u64>> = const {Cell::new(None)};
}

pub async fn with_task_id<Fut>(task_id: u64, fut: Fut) -> Fut::Output
where
    Fut: Future,
{
    ASYNC_TASK_ID.scope(task_id, fut).await
}

#[derive(Debug, Default)]
struct State {
    log: Vec<LogEntry>,
    block_counter: u64,
    fd_counter: u64,
    guard_counter: u64,
    mutex_counter: u64,
    rwlock_counter: u64,
    uuid_counter: u128,
}

#[derive(Debug)]
pub struct DebugIOLayer<IO = DefaultIOLayer>
where
    IO: IOLayer,
{
    inner: IO,
    state: Arc<Mutex<State>>,
    order: Arc<Order>,
}

impl DebugIOLayer {
    pub fn new() -> Self {
        Self {
            inner: DefaultIOLayer::default(),
            state: Default::default(),
            order: Default::default(),
        }
    }

    pub fn with_order(self, order: Order) -> Self {
        Self {
            order: Arc::new(order),
            ..self
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

    pub fn log_entries(&self) -> Vec<LogEntry> {
        self.state.lock().unwrap().log.clone()
    }

    pub fn wipe_log(&self) {
        let mut guard = self.state.lock().unwrap();
        guard.log.clear();
    }
}

impl<IO> IOLayer for DebugIOLayer<IO>
where
    IO: IOLayer,
{
    type Block = DebugIOBlock<IO::Block>;
    type BlockPrepared = DebugIOBlockPrepared<IO::BlockPrepared>;
    type FD = DebugIOFD<IO::FD>;

    fn prepare_block(&self) -> Self::BlockPrepared {
        let mut guard = self.state.lock().unwrap();

        let task_id = ASYNC_TASK_ID.try_get().ok();

        let block_id = guard.block_counter;
        guard.block_counter += 1;

        DebugIOBlockPrepared {
            inner: Some(self.inner.prepare_block()),
            task_id,
            block_id,
            state: Arc::clone(&self.state),
            order: Arc::clone(&self.order),
        }
    }

    fn create_raw_mutex(&self) -> Box<dyn IORawMutex> {
        let state = Arc::clone(&self.state);
        let mut guard = self.state.lock().unwrap();

        let id = guard.mutex_counter;
        guard.mutex_counter += 1;

        let task_id = SYNC_TASK_ID.get().or_else(|| ASYNC_TASK_ID.try_get().ok());
        let block_id = SYNC_BLOCK_ID.get();

        guard.log.push(LogEntry {
            task_id,
            block_id,
            fun: "create_mutex",
            args: IndexMap::default(),
            res: Ok(Value::ID("mutex", id)),
        });

        Box::new(DebugIORawMutex {
            id,
            inner: self.inner.create_raw_mutex(),
            state,
        })
    }

    fn create_raw_rwlock(&self) -> Box<dyn IORawRwLock> {
        let state = Arc::clone(&self.state);
        let mut guard = self.state.lock().unwrap();

        let id = guard.rwlock_counter;
        guard.rwlock_counter += 1;

        let task_id = SYNC_TASK_ID.get().or_else(|| ASYNC_TASK_ID.try_get().ok());
        let block_id = SYNC_BLOCK_ID.get();

        guard.log.push(LogEntry {
            task_id,
            block_id,
            fun: "create_rwlock",
            args: IndexMap::default(),
            res: Ok(Value::ID("rwlock", id)),
        });

        Box::new(DebugIORawRwLock {
            id,
            inner: self.inner.create_raw_rwlock(),
            state,
        })
    }
}

pub struct DebugIOBlockPrepared<P>
where
    P: IOBlockPrepared,
{
    inner: Option<P>,
    task_id: Option<u64>,
    block_id: u64,
    state: Arc<Mutex<State>>,
    order: Arc<Order>,
}

impl<P> IOBlockPrepared for DebugIOBlockPrepared<P>
where
    P: IOBlockPrepared,
{
    type Block = DebugIOBlock<<P as IOBlockPrepared>::Block>;

    fn start(&mut self) -> Self::Block {
        let Self {
            inner,
            task_id,
            block_id,
            state,
            order,
        } = self;

        let mut guard = state.lock().unwrap();

        SYNC_TASK_ID.set(*task_id);
        SYNC_BLOCK_ID.set(Some(*block_id));

        guard.log.push(LogEntry {
            task_id: task_id.as_ref().copied(),
            block_id: Some(*block_id),
            fun: "block-start",
            args: IndexMap::new(),
            res: Ok(Value::Unit),
        });

        let mut inner = inner.take().expect("not started");
        let inner = inner.start();
        DebugIOBlock {
            inner,
            task_id: *task_id,
            block_id: *block_id,
            state: Arc::clone(state),
            order: Arc::clone(order),
        }
    }
}

pub struct DebugIOBlock<B>
where
    B: IOBlock,
{
    inner: B,
    task_id: Option<u64>,
    block_id: u64,
    state: Arc<Mutex<State>>,
    order: Arc<Order>,
}

impl<B> DebugIOBlock<B>
where
    B: IOBlock,
{
    fn event<FState, D, FIO, T, O>(
        &mut self,
        evt: Event<FState, D, FIO, T, O, B>,
    ) -> Result<T, Errno>
    where
        FState: for<'a> FnOnce(&'a mut State) -> D,
        FIO: for<'a> FnOnce(&'a mut B, D) -> Result<T, Errno>,
        O: for<'a> FnOnce(&'a T) -> Value,
    {
        let Event {
            fun,
            args,
            state,
            f,
            map_ok,
            phantom: _,
        } = evt;

        // wait for our turn BEFORE capturing the state lock
        let arrange_guard = self.order.arrange(order::Call {
            task_id: self.task_id,
            block_id: Some(self.block_id),
            fun,
            args: &args,
        });

        let mut state_guard = self.state.lock().unwrap();
        let data = state(&mut state_guard);
        drop(state_guard);

        let res = match arrange_guard.manipulation() {
            None => f(&mut self.inner, data),
            Some(Manipulation::FailBefore(e)) => Err(*e),
        };

        let mut state_guard = self.state.lock().unwrap();
        state_guard.log.push(LogEntry {
            task_id: self.task_id,
            block_id: Some(self.block_id),
            fun,
            args,
            res: res.as_ref().map(map_ok).map_err(|e| *e),
        });

        // drop guards in order
        drop(state_guard);
        drop(arrange_guard);

        res
    }
}

impl<B> Drop for DebugIOBlock<B>
where
    B: IOBlock,
{
    fn drop(&mut self) {
        let mut guard = self.state.lock().unwrap();

        SYNC_TASK_ID.set(None);
        SYNC_BLOCK_ID.set(None);

        guard.log.push(LogEntry {
            task_id: self.task_id,
            block_id: Some(self.block_id),
            fun: "block-end",
            args: IndexMap::new(),
            res: Ok(Value::Unit),
        });
    }
}

impl<B> IOBlock for DebugIOBlock<B>
where
    B: IOBlock,
{
    type FD = DebugIOFD<B::FD>;

    fn fgetxattr<'a>(
        &mut self,
        fd: &Self::FD,
        name: &str,
        value: &'a mut [MaybeUninit<u8>],
    ) -> Result<(&'a mut [u8], &'a mut [MaybeUninit<u8>]), Errno> {
        self.event(Event {
            fun: "fgetxattr",
            args: IndexMap::from([
                ("fd", Value::ID("fd", fd.id)),
                ("name", Value::String(name.to_owned())),
                ("value", Value::Ellipsis),
            ]),
            state: |_| (),
            f: |inner, ()| inner.fgetxattr(&fd.inner, name, value),
            map_ok: |_| Value::Ellipsis,
            phantom: PhantomData,
        })
    }

    fn fgetxattr_query(&mut self, fd: &Self::FD, name: &str) -> Result<usize, Errno> {
        self.event(Event {
            fun: "fgetxattr_query",
            args: IndexMap::from([
                ("fd", Value::ID("fd", fd.id)),
                ("name", Value::String(name.to_owned())),
            ]),
            state: |_| (),
            f: |inner, ()| inner.fgetxattr_query(&fd.inner, name),
            map_ok: |n| Value::Size(*n as _),
            phantom: PhantomData,
        })
    }

    fn flistxattr<'a>(
        &mut self,
        fd: &Self::FD,
        list: &'a mut [MaybeUninit<u8>],
    ) -> Result<(&'a mut [u8], &'a mut [MaybeUninit<u8>]), Errno> {
        self.event(Event {
            fun: "flistxattr",
            args: IndexMap::from([("fd", Value::ID("fd", fd.id)), ("list", Value::Ellipsis)]),
            state: |_| (),
            f: |inner, ()| inner.flistxattr(&fd.inner, list),
            map_ok: |_| Value::Ellipsis,
            phantom: PhantomData,
        })
    }

    fn flistxattr_query(&mut self, fd: &Self::FD) -> Result<usize, Errno> {
        self.event(Event {
            fun: "flistxattr_query",
            args: IndexMap::from([("fd", Value::ID("fd", fd.id))]),
            state: |_| (),
            f: |inner, ()| inner.flistxattr_query(&fd.inner),
            map_ok: |n| Value::Size(*n as _),
            phantom: PhantomData,
        })
    }

    fn flock(&mut self, fd: &Self::FD, operation: rustix::fs::FlockOperation) -> Result<(), Errno> {
        self.event(Event {
            fun: "flock",
            args: IndexMap::from([
                ("fd", Value::ID("fd", fd.id)),
                ("operation", Value::Ellipsis),
            ]),
            state: |_| (),
            f: |inner, ()| inner.flock(&fd.inner, operation),
            map_ok: |()| Value::Unit,
            phantom: PhantomData,
        })
    }

    fn fstat(&mut self, fd: &Self::FD) -> Result<rustix::fs::Stat, Errno> {
        self.event(Event {
            fun: "fstat",
            args: IndexMap::from([("fd", Value::ID("fd", fd.id))]),
            state: |_| (),
            f: |inner, ()| inner.fstat(&fd.inner),
            map_ok: |_| Value::Ellipsis,
            phantom: PhantomData,
        })
    }

    fn fsetxattr(
        &mut self,
        fd: &Self::FD,
        name: &str,
        value: &[u8],
        flags: rustix::fs::XattrFlags,
    ) -> Result<(), Errno> {
        self.event(Event {
            fun: "fsetxattr",
            args: IndexMap::from([
                ("fd", Value::ID("fd", fd.id)),
                ("name", Value::String(name.to_owned())),
                ("value", Value::Ellipsis),
                ("flags", Value::flags(flags)),
            ]),
            state: |_| (),
            f: |inner, ()| inner.fsetxattr(&fd.inner, name, value, flags),
            map_ok: |()| Value::Unit,
            phantom: PhantomData,
        })
    }

    fn fsync(&mut self, fd: &Self::FD) -> Result<(), Errno> {
        self.event(Event {
            fun: "fsync",
            args: IndexMap::from([("fd", Value::ID("fd", fd.id))]),
            state: |_| (),
            f: |inner, ()| inner.fsync(&fd.inner),
            map_ok: |()| Value::Unit,
            phantom: PhantomData,
        })
    }

    fn getdents(
        &mut self,
        fd: &Self::FD,
        buf: &mut Vec<MaybeUninit<u8>>,
    ) -> Result<Vec<(String, rustix::fs::FileType)>, Errno> {
        self.event(Event {
            fun: "getdents",
            args: IndexMap::from([("fd", Value::ID("fd", fd.id)), ("buf", Value::Ellipsis)]),
            state: |_| (),
            f: |inner, ()| inner.getdents(&fd.inner, buf),
            map_ok: |_| Value::Ellipsis,
            phantom: PhantomData,
        })
    }

    fn lock(&mut self, raw_lock: &dyn IORawMutex) -> Box<dyn IORawMutexGuard> {
        let lock = (raw_lock as &dyn Any)
            .downcast_ref::<DebugIORawMutex>()
            .expect("wrong type");

        self.event(Event {
            fun: "lock",
            args: IndexMap::from([("mutex", Value::ID("mutex", lock.id))]),
            state: |state| {
                let id = state.guard_counter;
                state.guard_counter += 1;
                id
            },
            f: |inner, id| {
                let inner = inner.lock(lock.inner.as_ref());
                Ok(Box::new(DebugIORawMutexGuard { id, inner }))
            },
            map_ok: |guard| {
                let id = (guard.as_ref() as &dyn Any)
                    .downcast_ref::<DebugIORawMutexGuard>()
                    .expect("just created that type")
                    .id;
                Value::ID("guard", id)
            },
            phantom: PhantomData,
        })
        .expect("never fails")
    }

    fn lock_exclusive(&mut self, raw_lock: &dyn IORawRwLock) -> Box<dyn IORawMutexGuard> {
        let lock = (raw_lock as &dyn Any)
            .downcast_ref::<DebugIORawRwLock>()
            .expect("wrong type");

        self.event(Event {
            fun: "lock_exlusive",
            args: IndexMap::from([("rwlock", Value::ID("rwlock", lock.id))]),
            state: |state| {
                let id = state.guard_counter;
                state.guard_counter += 1;
                id
            },
            f: |inner, id| {
                let inner = inner.lock_exclusive(lock.inner.as_ref());
                Ok(Box::new(DebugIORawMutexGuard { id, inner }))
            },
            map_ok: |guard| {
                let id = (guard.as_ref() as &dyn Any)
                    .downcast_ref::<DebugIORawMutexGuard>()
                    .expect("just created that type")
                    .id;
                Value::ID("guard", id)
            },
            phantom: PhantomData,
        })
        .expect("never fails")
    }

    fn lock_shared(&mut self, raw_lock: &dyn IORawRwLock) -> Box<dyn IORawMutexGuard> {
        let lock = (raw_lock as &dyn Any)
            .downcast_ref::<DebugIORawRwLock>()
            .expect("wrong type");

        self.event(Event {
            fun: "lock_shared",
            args: IndexMap::from([("rwlock", Value::ID("rwlock", lock.id))]),
            state: |state| {
                let id = state.guard_counter;
                state.guard_counter += 1;
                id
            },
            f: |inner, id| {
                let inner = inner.lock_shared(lock.inner.as_ref());
                Ok(Box::new(DebugIORawMutexGuard { id, inner }))
            },
            map_ok: |guard| {
                let id = (guard.as_ref() as &dyn Any)
                    .downcast_ref::<DebugIORawMutexGuard>()
                    .expect("just created that type")
                    .id;
                Value::ID("guard", id)
            },
            phantom: PhantomData,
        })
        .expect("never fails")
    }

    fn linkat(
        &mut self,
        old_dirfd: &Self::FD,
        old_path: &str,
        new_dirfd: &Self::FD,
        new_path: &str,
        flags: rustix::fs::AtFlags,
    ) -> Result<(), Errno> {
        self.event(Event {
            fun: "linkat",
            args: IndexMap::from([
                ("old_dirfd", Value::ID("fd", old_dirfd.id)),
                ("old_path", Value::String(old_path.to_owned())),
                ("new_dirfd", Value::ID("fd", new_dirfd.id)),
                ("new_path", Value::String(new_path.to_owned())),
                ("flags", Value::flags(flags)),
            ]),
            state: |_| (),
            f: |inner, ()| {
                inner.linkat(
                    &old_dirfd.inner,
                    old_path,
                    &new_dirfd.inner,
                    new_path,
                    flags,
                )
            },
            map_ok: |()| Value::Unit,
            phantom: PhantomData,
        })
    }

    fn mkdirat(
        &mut self,
        dirfd: &Self::FD,
        path: &str,
        mode: rustix::fs::Mode,
    ) -> Result<(), Errno> {
        self.event(Event {
            fun: "mkdirat",
            args: IndexMap::from([
                ("dirfd", Value::ID("fd", dirfd.id)),
                ("path", Value::String(path.to_owned())),
                ("mode", Value::flags(mode)),
            ]),
            state: |_| (),
            f: |inner, ()| inner.mkdirat(&dirfd.inner, path, mode),
            map_ok: |()| Value::Unit,
            phantom: PhantomData,
        })
    }

    fn open(
        &mut self,
        path: &std::path::Path,
        flags: rustix::fs::OFlags,
        mode: rustix::fs::Mode,
    ) -> Result<Self::FD, Errno> {
        let state_arc = Arc::clone(&self.state);

        self.event(Event {
            fun: "open",
            args: IndexMap::from([
                ("path", Value::Ellipsis),
                ("flags", Value::flags(flags)),
                ("mode", Value::flags(mode)),
            ]),
            state: |state| {
                let id = state.fd_counter;
                state.fd_counter += 1;
                id
            },
            f: |inner, id| {
                let inner = inner.open(path, flags, mode)?;

                Ok(DebugIOFD {
                    inner,
                    id,
                    state: state_arc,
                })
            },
            map_ok: |fd| Value::ID("fd", fd.id),
            phantom: PhantomData,
        })
    }

    fn openat2(
        &mut self,
        dirfd: &Self::FD,
        path: &str,
        oflags: rustix::fs::OFlags,
        mode: rustix::fs::Mode,
        resolve: rustix::fs::ResolveFlags,
    ) -> Result<Self::FD, Errno> {
        let state_arc = Arc::clone(&self.state);

        self.event(Event {
            fun: "openat2",
            args: IndexMap::from([
                ("dirfd", Value::ID("fd", dirfd.id)),
                ("path", Value::String(path.to_owned())),
                ("oflags", Value::flags(oflags)),
                ("mode", Value::flags(mode)),
                ("resolve", Value::flags(resolve)),
            ]),
            state: |state| {
                let id = state.fd_counter;
                state.fd_counter += 1;
                id
            },
            f: |inner, id| {
                let inner = inner.openat2(&dirfd.inner, path, oflags, mode, resolve)?;

                Ok(DebugIOFD {
                    inner,
                    id,
                    state: state_arc,
                })
            },
            map_ok: |fd| Value::ID("fd", fd.id),
            phantom: PhantomData,
        })
    }

    fn preadv2(
        &mut self,
        fd: &Self::FD,
        bufs: &mut [std::io::IoSliceMut<'_>],
        offset: u64,
        flags: rustix::io::ReadWriteFlags,
    ) -> Result<usize, Errno> {
        self.event(Event {
            fun: "preadv2",
            args: IndexMap::from([
                ("fd", Value::ID("fd", fd.id)),
                ("bufs", Value::Ellipsis),
                ("offset", Value::Size(offset)),
                ("flags", Value::flags(flags)),
            ]),
            state: |_| (),
            f: |inner, ()| inner.preadv2(&fd.inner, bufs, offset, flags),
            map_ok: |n| Value::Size(*n as _),
            phantom: PhantomData,
        })
    }

    fn pwritev2(
        &mut self,
        fd: &Self::FD,
        bufs: &[std::io::IoSlice<'_>],
        offset: u64,
        flags: rustix::io::ReadWriteFlags,
    ) -> Result<usize, Errno> {
        self.event(Event {
            fun: "pwritev2",
            args: IndexMap::from([
                ("fd", Value::ID("fd", fd.id)),
                ("bufs", Value::Ellipsis),
                ("offset", Value::Size(offset)),
                ("flags", Value::flags(flags)),
            ]),
            state: |_| (),
            f: |inner, ()| inner.pwritev2(&fd.inner, bufs, offset, flags),
            map_ok: |n| Value::Size(*n as _),
            phantom: PhantomData,
        })
    }

    fn renameat2(
        &mut self,
        old_dirfd: &Self::FD,
        old_path: &str,
        new_dirfd: &Self::FD,
        new_path: &str,
        flags: rustix::fs::RenameFlags,
    ) -> Result<(), Errno> {
        self.event(Event {
            fun: "renameat2",
            args: IndexMap::from([
                ("old_dirfd", Value::ID("fd", old_dirfd.id)),
                ("old_path", Value::String(old_path.to_owned())),
                ("new_dirfd", Value::ID("fd", new_dirfd.id)),
                ("new_path", Value::String(new_path.to_owned())),
                ("flags", Value::flags(flags)),
            ]),
            state: |_| (),
            f: |inner, ()| {
                inner.renameat2(
                    &old_dirfd.inner,
                    old_path,
                    &new_dirfd.inner,
                    new_path,
                    flags,
                )
            },
            map_ok: |()| Value::Unit,
            phantom: PhantomData,
        })
    }

    fn statat(
        &mut self,
        dirfd: &Self::FD,
        path: &str,
        flags: rustix::fs::AtFlags,
    ) -> Result<rustix::fs::Stat, Errno> {
        self.event(Event {
            fun: "statat",
            args: IndexMap::from([
                ("dirfd", Value::ID("fd", dirfd.id)),
                ("path", Value::String(path.to_owned())),
                ("flags", Value::flags(flags)),
            ]),
            state: |_| (),
            f: |inner, ()| inner.statat(&dirfd.inner, path, flags),
            map_ok: |_| Value::Ellipsis,
            phantom: PhantomData,
        })
    }

    fn unlinkat(
        &mut self,
        dirfd: &Self::FD,
        path: &str,
        flags: rustix::fs::AtFlags,
    ) -> Result<(), Errno> {
        self.event(Event {
            fun: "unlinkat",
            args: IndexMap::from([
                ("dirfd", Value::ID("fd", dirfd.id)),
                ("path", Value::String(path.to_owned())),
                ("flags", Value::flags(flags)),
            ]),
            state: |_| (),
            f: |inner, ()| inner.unlinkat(&dirfd.inner, path, flags),
            map_ok: |()| Value::Unit,
            phantom: PhantomData,
        })
    }

    fn uuid(&mut self) -> uuid::Uuid {
        self.event(Event {
            fun: "uuid",
            args: IndexMap::default(),
            state: |state| {
                let uuid = Uuid::from_u128(state.uuid_counter);
                state.uuid_counter += 1;
                uuid
            },
            f: |_, uuid| Ok(uuid),
            map_ok: |uuid| Value::String(uuid.to_string()),
            phantom: PhantomData,
        })
        .expect("never fails")
    }
}

struct Event<FState, D, FIO, T, O, B>
where
    FState: for<'a> FnOnce(&'a mut State) -> D,
    FIO: for<'a> FnOnce(&'a mut B, D) -> Result<T, Errno>,
    O: for<'a> FnOnce(&'a T) -> Value,
    B: IOBlock,
{
    fun: &'static str,
    args: IndexMap<&'static str, Value>,
    state: FState,
    f: FIO,
    map_ok: O,
    phantom: PhantomData<(T, B)>,
}

struct DebugIORawMutex {
    id: u64,
    inner: Box<dyn IORawMutex>,
    state: Arc<Mutex<State>>,
}

impl Drop for DebugIORawMutex {
    fn drop(&mut self) {
        let mut guard = self.state.lock().unwrap();

        let task_id = SYNC_TASK_ID.get().or_else(|| ASYNC_TASK_ID.try_get().ok());
        let block_id = SYNC_BLOCK_ID.get();

        guard.log.push(LogEntry {
            task_id,
            block_id,
            fun: "destroy_mutex",
            args: IndexMap::from([("mutex", Value::ID("mutex", self.id))]),
            res: Ok(Value::Unit),
        });
    }
}

impl IORawMutex for DebugIORawMutex {
    unsafe fn unlock(&self, guard: Box<dyn IORawMutexGuard>) {
        let dbg_guard = (guard as Box<dyn Any>)
            .downcast::<DebugIORawMutexGuard>()
            .expect("wrong type");

        let mut guard2 = self.state.lock().unwrap();

        // SAFETY: caller
        unsafe {
            self.inner.unlock(dbg_guard.inner);
        }

        let task_id = SYNC_TASK_ID.get();
        let block_id = SYNC_BLOCK_ID.get();

        guard2.log.push(LogEntry {
            task_id,
            block_id,
            fun: "unlock",
            args: IndexMap::from([
                ("mutex", Value::ID("mutex", self.id)),
                ("guard", Value::ID("guard", dbg_guard.id)),
            ]),
            res: Ok(Value::Unit),
        });
    }
}

struct DebugIORawRwLock {
    id: u64,
    inner: Box<dyn IORawRwLock>,
    state: Arc<Mutex<State>>,
}

impl Drop for DebugIORawRwLock {
    fn drop(&mut self) {
        let mut guard = self.state.lock().unwrap();

        let task_id = SYNC_TASK_ID.get().or_else(|| ASYNC_TASK_ID.try_get().ok());
        let block_id = SYNC_BLOCK_ID.get();

        guard.log.push(LogEntry {
            task_id,
            block_id,
            fun: "destroy_rwlock",
            args: IndexMap::from([("rwlock", Value::ID("rwlock", self.id))]),
            res: Ok(Value::Unit),
        });
    }
}

impl IORawRwLock for DebugIORawRwLock {
    unsafe fn unlock_shared(&self, guard: Box<dyn IORawMutexGuard>) {
        let dbg_guard = (guard as Box<dyn Any>)
            .downcast::<DebugIORawMutexGuard>()
            .expect("wrong type");

        let mut guard2 = self.state.lock().unwrap();

        // SAFETY: caller
        unsafe {
            self.inner.unlock_shared(dbg_guard.inner);
        }

        let task_id = SYNC_TASK_ID.get();
        let block_id = SYNC_BLOCK_ID.get();

        guard2.log.push(LogEntry {
            task_id,
            block_id,
            fun: "unlock_shared",
            args: IndexMap::from([
                ("rwlock", Value::ID("rwlock", self.id)),
                ("guard", Value::ID("guard", dbg_guard.id)),
            ]),
            res: Ok(Value::Unit),
        });
    }

    unsafe fn unlock_exclusive(&self, guard: Box<dyn IORawMutexGuard>) {
        let dbg_guard = (guard as Box<dyn Any>)
            .downcast::<DebugIORawMutexGuard>()
            .expect("wrong type");

        let mut guard2 = self.state.lock().unwrap();

        // SAFETY: caller
        unsafe {
            self.inner.unlock_exclusive(dbg_guard.inner);
        }

        let task_id = SYNC_TASK_ID.get();
        let block_id = SYNC_BLOCK_ID.get();

        guard2.log.push(LogEntry {
            task_id,
            block_id,
            fun: "unlock_exclusive",
            args: IndexMap::from([
                ("rwlock", Value::ID("rwlock", self.id)),
                ("guard", Value::ID("guard", dbg_guard.id)),
            ]),
            res: Ok(Value::Unit),
        });
    }
}

struct DebugIORawMutexGuard {
    id: u64,
    inner: Box<dyn IORawMutexGuard>,
}

impl IORawMutexGuard for DebugIORawMutexGuard {}

#[derive(Debug)]
pub struct DebugIOFD<FD>
where
    FD: IOFD,
{
    inner: FD,
    id: u64,
    state: Arc<Mutex<State>>,
}

impl<FD> Drop for DebugIOFD<FD>
where
    FD: IOFD,
{
    fn drop(&mut self) {
        let mut guard = self.state.lock().unwrap();

        let task_id = SYNC_TASK_ID.get().or_else(|| ASYNC_TASK_ID.try_get().ok());
        let block_id = SYNC_BLOCK_ID.get();

        guard.log.push(LogEntry {
            task_id,
            block_id,
            fun: "close",
            args: IndexMap::from([("fd", Value::ID("fd", self.id))]),
            res: Ok(Value::Unit),
        });
    }
}

impl<FD> IOFD for DebugIOFD<FD> where FD: IOFD {}
