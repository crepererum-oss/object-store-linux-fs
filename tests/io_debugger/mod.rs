use std::{
    cell::{Cell, RefCell},
    fmt::Write,
    marker::PhantomData,
    mem::{ManuallyDrop, MaybeUninit},
    ops::{Deref, DerefMut},
    sync::Arc,
};

use indexmap::IndexMap;
use object_store_linux_fs::io::{
    DefaultIOLayer, IOBlock, IOBlockPrepared, IOFD, IOLayer, IOMutex, IOMutexGuard, IORwLock,
    IORwLockReadGuard, IORwLockWriteGuard,
};
use parking_lot::ReentrantMutex;
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

type SharedState = Arc<ReentrantMutex<RefCell<State>>>;

#[derive(Debug)]
pub struct DebugIOLayer<IO = DefaultIOLayer>
where
    IO: IOLayer,
{
    inner: IO,
    state: SharedState,
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
        let guard = self.state.lock();
        let guard = guard.borrow();
        for entry in &guard.log {
            writeln!(&mut out, "{entry}").unwrap();
        }
        out
    }

    pub fn log_entries(&self) -> Vec<LogEntry> {
        let guard = self.state.lock();
        let guard = guard.borrow();
        guard.log.clone()
    }

    pub fn wipe_log(&self) {
        let guard = self.state.lock();
        let mut guard = guard.borrow_mut();
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
    type Mutex<T>
        = DebugIOMutex<T, IO::Mutex<T>>
    where
        T: Send + std::fmt::Debug;
    type RwLock<T>
        = DebugIORwLock<T, IO::RwLock<T>>
    where
        T: Send + Sync + std::fmt::Debug;

    fn prepare_block(&self) -> Self::BlockPrepared {
        let guard = self.state.lock();
        let mut guard = guard.borrow_mut();

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

    fn create_mutex<T>(&self, data: T) -> Self::Mutex<T>
    where
        T: Send + std::fmt::Debug,
    {
        let state = Arc::clone(&self.state);
        let guard = self.state.lock();
        let mut guard = guard.borrow_mut();

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

        DebugIOMutex {
            id,
            inner: ManuallyDrop::new(self.inner.create_mutex(data)),
            state,
            _phantom: PhantomData,
        }
    }

    fn create_rwlock<T>(&self, data: T) -> Self::RwLock<T>
    where
        T: Send + Sync + std::fmt::Debug,
    {
        let state = Arc::clone(&self.state);
        let guard = self.state.lock();
        let mut guard = guard.borrow_mut();

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

        DebugIORwLock {
            id,
            inner: ManuallyDrop::new(self.inner.create_rwlock(data)),
            state,
            _phantom: PhantomData,
        }
    }
}

pub struct DebugIOBlockPrepared<P>
where
    P: IOBlockPrepared,
{
    inner: Option<P>,
    task_id: Option<u64>,
    block_id: u64,
    state: SharedState,
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

        let guard = state.lock();
        let mut guard = guard.borrow_mut();

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
    state: SharedState,
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

        let data = {
            let state_guard = self.state.lock();
            let mut state_guard = state_guard.borrow_mut();
            state(&mut state_guard)
        };

        let res = match arrange_guard.manipulation() {
            None => f(&mut self.inner, data),
            Some(Manipulation::FailBefore(e)) => Err(*e),
        };

        let state_guard = self.state.lock();
        let mut state_guard_mut = state_guard.borrow_mut();
        state_guard_mut.log.push(LogEntry {
            task_id: self.task_id,
            block_id: Some(self.block_id),
            fun,
            args,
            res: res.as_ref().map(map_ok).map_err(|e| *e),
        });

        // drop guards in order
        drop(state_guard_mut);
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
        let guard = self.state.lock();
        let mut guard = guard.borrow_mut();

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

#[derive(Debug)]
pub struct DebugIOMutex<T, M>
where
    M: IOMutex<T>,
    T: Send + std::fmt::Debug,
{
    id: u64,
    inner: ManuallyDrop<M>,
    state: SharedState,
    _phantom: PhantomData<fn() -> T>,
}

impl<T, M> Drop for DebugIOMutex<T, M>
where
    M: IOMutex<T>,
    T: Send + std::fmt::Debug,
{
    fn drop(&mut self) {
        let guard = self.state.lock();

        // drop inner
        //
        // SAFETY: we run in destructor
        unsafe {
            ManuallyDrop::drop(&mut self.inner);
        }

        // get mut ref AFTER inner drop
        let mut guard_mut = guard.borrow_mut();

        let task_id = SYNC_TASK_ID.get().or_else(|| ASYNC_TASK_ID.try_get().ok());
        let block_id = SYNC_BLOCK_ID.get();

        guard_mut.log.push(LogEntry {
            task_id,
            block_id,
            fun: "destroy_mutex",
            args: IndexMap::from([("mutex", Value::ID("mutex", self.id))]),
            res: Ok(Value::Unit),
        });
    }
}

impl<T, M> IOMutex<T> for DebugIOMutex<T, M>
where
    M: IOMutex<T>,
    T: Send + std::fmt::Debug,
{
    type Block = DebugIOBlock<M::Block>;

    type Guard<'a>
        = DebugIOMutexGuard<'a, T, M::Guard<'a>>
    where
        Self: 'a;

    fn lock<'a>(&'a self, io: &mut Self::Block) -> Self::Guard<'a> {
        let state = Arc::clone(&io.state);

        io.event(Event {
            fun: "lock",
            args: IndexMap::from([("mutex", Value::ID("mutex", self.id))]),
            state: |state| {
                let id = state.guard_counter;
                state.guard_counter += 1;
                id
            },
            f: |inner, guard_id| {
                let inner = self.inner.lock(inner);
                Ok(DebugIOMutexGuard {
                    guard_id,
                    mutex_id: self.id,
                    inner: ManuallyDrop::new(inner),
                    state,
                    phantom: PhantomData,
                })
            },
            map_ok: |guard| Value::ID("guard", guard.guard_id),
            phantom: PhantomData,
        })
        .expect("never fails")
    }
}

pub struct DebugIOMutexGuard<'a, T, G>
where
    G: IOMutexGuard<'a, T>,
{
    mutex_id: u64,
    guard_id: u64,
    inner: ManuallyDrop<G>,
    state: SharedState,
    phantom: PhantomData<&'a T>,
}

impl<'a, T, G> Drop for DebugIOMutexGuard<'a, T, G>
where
    G: IOMutexGuard<'a, T>,
{
    fn drop(&mut self) {
        let guard = self.state.lock();

        // drop inner
        //
        // SAFETY: we run in destructor
        unsafe {
            ManuallyDrop::drop(&mut self.inner);
        }

        // get mut ref AFTER inner drop
        let mut guard_mut = guard.borrow_mut();

        let task_id = SYNC_TASK_ID.get();
        let block_id = SYNC_BLOCK_ID.get();

        guard_mut.log.push(LogEntry {
            task_id,
            block_id,
            fun: "unlock",
            args: IndexMap::from([
                ("mutex", Value::ID("mutex", self.mutex_id)),
                ("guard", Value::ID("guard", self.guard_id)),
            ]),
            res: Ok(Value::Unit),
        });
    }
}

impl<'a, T, G> Deref for DebugIOMutexGuard<'a, T, G>
where
    G: IOMutexGuard<'a, T>,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.inner.deref()
    }
}

impl<'a, T, G> DerefMut for DebugIOMutexGuard<'a, T, G>
where
    G: IOMutexGuard<'a, T>,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.deref_mut()
    }
}

impl<'a, T, G> IOMutexGuard<'a, T> for DebugIOMutexGuard<'a, T, G> where G: IOMutexGuard<'a, T> {}

#[derive(Debug)]
pub struct DebugIORwLock<T, L>
where
    L: IORwLock<T>,
    T: Send + Sync + std::fmt::Debug,
{
    id: u64,
    inner: ManuallyDrop<L>,
    state: SharedState,
    _phantom: PhantomData<fn() -> T>,
}

impl<T, L> Drop for DebugIORwLock<T, L>
where
    L: IORwLock<T>,
    T: Send + Sync + std::fmt::Debug,
{
    fn drop(&mut self) {
        let guard = self.state.lock();

        // drop inner
        //
        // SAFETY: we run in destructor
        unsafe {
            ManuallyDrop::drop(&mut self.inner);
        }

        // get mut ref AFTER inner drop
        let mut guard_mut = guard.borrow_mut();

        let task_id = SYNC_TASK_ID.get().or_else(|| ASYNC_TASK_ID.try_get().ok());
        let block_id = SYNC_BLOCK_ID.get();

        guard_mut.log.push(LogEntry {
            task_id,
            block_id,
            fun: "destroy_rwlock",
            args: IndexMap::from([("rwlock", Value::ID("rwlock", self.id))]),
            res: Ok(Value::Unit),
        });
    }
}

impl<T, L> IORwLock<T> for DebugIORwLock<T, L>
where
    L: IORwLock<T>,
    T: Send + Sync + std::fmt::Debug,
{
    type Block = DebugIOBlock<L::Block>;

    type ReadGuard<'a>
        = DebugIORwLockReadGuard<'a, T, L::ReadGuard<'a>>
    where
        Self: 'a;

    type WriteGuard<'a>
        = DebugIORwLockWriteGuard<'a, T, L::WriteGuard<'a>>
    where
        Self: 'a;

    fn read<'a>(&'a self, io: &mut Self::Block) -> Self::ReadGuard<'a> {
        let state = Arc::clone(&io.state);

        io.event(Event {
            fun: "lock_shared",
            args: IndexMap::from([("rwlock", Value::ID("rwlock", self.id))]),
            state: |state| {
                let id = state.guard_counter;
                state.guard_counter += 1;
                id
            },
            f: |inner, guard_id| {
                let inner = self.inner.read(inner);
                Ok(DebugIORwLockReadGuard {
                    guard_id,
                    rwlock_id: self.id,
                    inner: ManuallyDrop::new(inner),
                    state,
                    phantom: PhantomData,
                })
            },
            map_ok: |guard| Value::ID("guard", guard.guard_id),
            phantom: PhantomData,
        })
        .expect("never fails")
    }

    fn write<'a>(&'a self, io: &mut Self::Block) -> Self::WriteGuard<'a> {
        let state = Arc::clone(&io.state);

        io.event(Event {
            fun: "lock_exclusive",
            args: IndexMap::from([("rwlock", Value::ID("rwlock", self.id))]),
            state: |state| {
                let id = state.guard_counter;
                state.guard_counter += 1;
                id
            },
            f: |inner, guard_id| {
                let inner = self.inner.write(inner);
                Ok(DebugIORwLockWriteGuard {
                    guard_id,
                    rwlock_id: self.id,
                    inner: ManuallyDrop::new(inner),
                    state,
                    phantom: PhantomData,
                })
            },
            map_ok: |guard| Value::ID("guard", guard.guard_id),
            phantom: PhantomData,
        })
        .expect("never fails")
    }
}

pub struct DebugIORwLockReadGuard<'a, T, G>
where
    G: IORwLockReadGuard<'a, T>,
{
    rwlock_id: u64,
    guard_id: u64,
    inner: ManuallyDrop<G>,
    state: SharedState,
    phantom: PhantomData<&'a T>,
}

impl<'a, T, G> Drop for DebugIORwLockReadGuard<'a, T, G>
where
    G: IORwLockReadGuard<'a, T>,
{
    fn drop(&mut self) {
        let guard = self.state.lock();

        // drop inner
        //
        // SAFETY: we run in destructor
        unsafe {
            ManuallyDrop::drop(&mut self.inner);
        }

        // get mut ref AFTER inner drop
        let mut guard_mut = guard.borrow_mut();

        let task_id = SYNC_TASK_ID.get();
        let block_id = SYNC_BLOCK_ID.get();

        guard_mut.log.push(LogEntry {
            task_id,
            block_id,
            fun: "unlock_shared",
            args: IndexMap::from([
                ("rwlock", Value::ID("rwlock", self.rwlock_id)),
                ("guard", Value::ID("guard", self.guard_id)),
            ]),
            res: Ok(Value::Unit),
        });
    }
}

impl<'a, T, G> Deref for DebugIORwLockReadGuard<'a, T, G>
where
    G: IORwLockReadGuard<'a, T>,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.inner.deref()
    }
}

impl<'a, T, G> IORwLockReadGuard<'a, T> for DebugIORwLockReadGuard<'a, T, G> where
    G: IORwLockReadGuard<'a, T>
{
}

pub struct DebugIORwLockWriteGuard<'a, T, G>
where
    G: IORwLockWriteGuard<'a, T>,
{
    rwlock_id: u64,
    guard_id: u64,
    inner: ManuallyDrop<G>,
    state: SharedState,
    phantom: PhantomData<&'a T>,
}

impl<'a, T, G> Drop for DebugIORwLockWriteGuard<'a, T, G>
where
    G: IORwLockWriteGuard<'a, T>,
{
    fn drop(&mut self) {
        let guard = self.state.lock();

        // drop inner
        //
        // SAFETY: we run in destructor
        unsafe {
            ManuallyDrop::drop(&mut self.inner);
        }

        // get mut ref AFTER inner drop
        let mut guard_mut = guard.borrow_mut();

        let task_id = SYNC_TASK_ID.get();
        let block_id = SYNC_BLOCK_ID.get();

        guard_mut.log.push(LogEntry {
            task_id,
            block_id,
            fun: "unlock_exclusive",
            args: IndexMap::from([
                ("rwlock", Value::ID("rwlock", self.rwlock_id)),
                ("guard", Value::ID("guard", self.guard_id)),
            ]),
            res: Ok(Value::Unit),
        });
    }
}

impl<'a, T, G> Deref for DebugIORwLockWriteGuard<'a, T, G>
where
    G: IORwLockWriteGuard<'a, T>,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.inner.deref()
    }
}

impl<'a, T, G> DerefMut for DebugIORwLockWriteGuard<'a, T, G>
where
    G: IORwLockWriteGuard<'a, T>,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.deref_mut()
    }
}

impl<'a, T, G> IORwLockWriteGuard<'a, T> for DebugIORwLockWriteGuard<'a, T, G> where
    G: IORwLockWriteGuard<'a, T>
{
}

#[derive(Debug)]
pub struct DebugIOFD<FD>
where
    FD: IOFD,
{
    inner: FD,
    id: u64,
    state: SharedState,
}

impl<FD> Drop for DebugIOFD<FD>
where
    FD: IOFD,
{
    fn drop(&mut self) {
        let guard = self.state.lock();
        let mut guard = guard.borrow_mut();

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
