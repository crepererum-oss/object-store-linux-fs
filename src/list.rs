use std::{mem::MaybeUninit, sync::Arc};

use futures_core::stream::BoxStream;
use futures_util::{StreamExt, TryStreamExt};
use object_store::{
    Error, ListResult, ObjectMeta, Result,
    path::{DELIMITER_CHAR, Path, PathPart},
};
use rustix::fs::{FileType, Mode};

use crate::{
    constants::{
        AT_FLAGS_STATAT_WITH_PATH, GETDENTS_BUFFER_SIZE, OFLAGS_READ, RESOLVE_FLAGS,
        STAGING_FILE_PREFIX, STORE_NAME,
    },
    error::IOErrorExt,
    io::{IOBlock, IOFD, IOLayer, spawn_io},
    state::State,
    utils::{OpenDirMode, open_dir_fd, stat_to_meta},
};

pub(crate) fn list<IO>(
    state: Arc<State<IO>>,
    prefix: Option<Path>,
    offset: Option<Path>,
) -> BoxStream<'static, Result<ObjectMeta>>
where
    IO: IOLayer,
{
    Box::pin(
        futures_util::stream::once(async move {
            match NextTask::start(state, prefix).await {
                Ok(Some(task)) => {
                    let stream = futures_util::stream::try_unfold(
                        (task, offset),
                        async move |(task, offset)| {
                            match task.next().await? {
                                None => Result::<_, Error>::Ok(None),
                                Some((task_next, metas)) => {
                                    // technically we could filter out branches that are not gonna help, but the
                                    // tests in `object_store` is broken because they use `Path=String` for
                                    // comparison, which isn't really hierarchical correct
                                    let metas = metas
                                        .into_iter()
                                        .filter(|meta| {
                                            offset
                                                .as_ref()
                                                .map(|offset| &meta.location > offset)
                                                .unwrap_or(true)
                                        })
                                        .collect::<Vec<_>>();
                                    let metas = futures_util::stream::iter(metas).map(Ok);
                                    Ok(Some((metas, (task_next, offset))))
                                }
                            }
                        },
                    )
                    .try_flatten();

                    Result::<_, Error>::Ok(Box::pin(stream) as BoxStream<'static, _>)
                }
                Ok(None) => Ok(Box::pin(futures_util::stream::empty()) as _),
                Err(e) => Err(e),
            }
        })
        .try_flatten(),
    )
}

pub(crate) async fn list_with_delimiter<IO>(
    state: Arc<State<IO>>,
    prefix: Option<Path>,
) -> Result<ListResult>
where
    IO: IOLayer,
{
    let Some(mut task) = NextTask::start(state, prefix).await? else {
        return Ok(ListResult {
            common_prefixes: vec![],
            objects: vec![],
            extensions: Default::default(),
        });
    };
    let mut metas = vec![];
    loop {
        match task.next().await? {
            None => {
                return Ok(ListResult {
                    common_prefixes: vec![],
                    objects: metas,
                    extensions: Default::default(),
                });
            }
            Some((next_task, mut metas2)) => {
                metas.append(&mut metas2);

                match next_task {
                    NextTask::OpenDir {
                        dir_fds,
                        state: _,
                        getdents_buffer: _,
                    } => {
                        let mut common_prefixes = dir_fds
                            .into_iter()
                            .map(|dir_fd| match dir_fd {
                                DirFd::Eager { .. } => unreachable!(),
                                DirFd::Lazy {
                                    child_prefix,
                                    parent: _,
                                    child: _,
                                } => child_prefix,
                            })
                            .collect::<Vec<_>>();

                        // TODO: technically we don't need to sort `common_prefixes` here, but the `object_store` integration tests are broken
                        common_prefixes.sort_unstable();

                        return Ok(ListResult {
                            common_prefixes,
                            objects: metas,
                            extensions: Default::default(),
                        });
                    }
                    next_task @ NextTask::ReadNextEntry { .. } => {
                        task = next_task;
                    }
                }
            }
        }
    }
}

enum DirFd {
    Eager {
        prefix: Option<Path>,
        dir_fd: Arc<dyn IOFD>,
    },
    Lazy {
        child_prefix: Path,
        parent: Arc<dyn IOFD>,
        child: String,
    },
}

enum NextTask<IO>
where
    IO: IOLayer,
{
    OpenDir {
        dir_fds: Vec<DirFd>,
        state: Arc<State<IO>>,
        getdents_buffer: Vec<MaybeUninit<u8>>,
    },
    ReadNextEntry {
        dir_fds: Vec<DirFd>,
        dir_fd: Arc<dyn IOFD>,
        getdents_buffer: Vec<MaybeUninit<u8>>,
        prefix: Option<Path>,
        state: Arc<State<IO>>,
    },
}

impl<IO> NextTask<IO>
where
    IO: IOLayer,
{
    async fn start(state: Arc<State<IO>>, prefix: Option<Path>) -> Result<Option<Self>> {
        let state2 = Arc::clone(&state);
        let (dir_fd_res, prefix) = spawn_io(&state.options().io_layer, move |io| {
            let dir_fd_res = match &prefix {
                Some(prefix) => state2.open_dir(
                    io,
                    prefix.as_ref().split(DELIMITER_CHAR).map(Ok),
                    OpenDirMode::Get,
                ),
                None => state2.open_dir(io, std::iter::empty(), OpenDirMode::Get),
            };

            Ok((dir_fd_res, prefix))
        })
        .await?;

        match dir_fd_res {
            Ok(dir_fd) => Ok(Some(Self::OpenDir {
                dir_fds: vec![DirFd::Eager { prefix, dir_fd }],
                state,
                getdents_buffer: vec![MaybeUninit::uninit(); GETDENTS_BUFFER_SIZE],
            })),
            Err(Error::NotFound { .. }) => Ok(None),
            Err(Error::Generic { source, .. })
                if source
                    .downcast_ref::<std::io::Error>()
                    .map(|e| e.kind() == std::io::ErrorKind::NotADirectory)
                    .unwrap_or_default() =>
            {
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }

    async fn next(self) -> Result<Option<(Self, Vec<ObjectMeta>)>> {
        match self {
            Self::OpenDir {
                mut dir_fds,
                state,
                getdents_buffer,
            } => {
                let Some(dir_fd) = dir_fds.pop() else {
                    // we're done
                    return Ok(None);
                };

                let maybe_opened = spawn_io(&state.options().io_layer, move |io| {
                    let (prefix, dir_fd) = match dir_fd {
                        DirFd::Eager { prefix, dir_fd } => {
                            // "list" FD has state, hence we need to re-open the directory
                            let Some(dir_fd) = io
                                .openat2(
                                    dir_fd.as_ref(),
                                    ".",
                                    OFLAGS_READ,
                                    Mode::empty(),
                                    RESOLVE_FLAGS,
                                )
                                .into_list_err()
                                .transpose()?
                            else {
                                return Ok(None);
                            };
                            (prefix, Arc::from(dir_fd))
                        }
                        DirFd::Lazy {
                            child_prefix,
                            parent,
                            child,
                        } => {
                            let Some(dir_fd) = open_dir_fd(io, parent.as_ref(), &child)
                                .into_list_err()
                                .transpose()?
                            else {
                                return Ok(None);
                            };
                            (Some(child_prefix), dir_fd)
                        }
                    };

                    Ok(Some((prefix, dir_fd)))
                })
                .await?;

                match maybe_opened {
                    Some((prefix, dir_fd)) => Ok(Some((
                        Self::ReadNextEntry {
                            dir_fds,
                            getdents_buffer,
                            dir_fd,
                            prefix,
                            state,
                        },
                        vec![],
                    ))),
                    None => Ok(Some((
                        Self::OpenDir {
                            dir_fds,
                            state,
                            getdents_buffer,
                        },
                        vec![],
                    ))),
                }
            }
            Self::ReadNextEntry {
                mut dir_fds,
                mut getdents_buffer,
                dir_fd,
                prefix,
                state,
            } => {
                let (mut dir_fd, getdents_buffer, entries) =
                    spawn_io(&state.options().io_layer, move |io| {
                        let entries = io
                            .getdents(dir_fd.as_ref(), &mut getdents_buffer)
                            .into_list_err()
                            .transpose()?
                            .unwrap_or_default();
                        Ok((dir_fd, getdents_buffer, entries))
                    })
                    .await?;

                if entries.is_empty() {
                    // end of directory
                    Ok(Some((
                        Self::OpenDir {
                            dir_fds,
                            state,
                            getdents_buffer,
                        },
                        vec![],
                    )))
                } else {
                    let mut metas = Vec::with_capacity(entries.len());

                    for (file_name, file_type) in entries {
                        // skip special entries
                        if file_name == "."
                            || file_name == ".."
                            || file_name.starts_with(STAGING_FILE_PREFIX)
                        {
                            continue;
                        }

                        let child_path = match &prefix {
                            Some(prefix) => prefix.clone().join(
                                // parse part before handing it to join, because `PathPart::from(str)` will percent-encode it
                                PathPart::parse(&file_name).map_err(|e| Error::InvalidPath {
                                    source: object_store::path::Error::BadSegment {
                                        path: file_name.clone(),
                                        source: e,
                                    },
                                })?,
                            ),
                            None => Path::parse(&file_name)?,
                        };

                        match file_type {
                            FileType::RegularFile => {
                                let maybe_stat;
                                (dir_fd, maybe_stat) =
                                    spawn_io(&state.options().io_layer, move |io| {
                                        let maybe_stat = io
                                            .statat(
                                                dir_fd.as_ref(),
                                                &file_name,
                                                AT_FLAGS_STATAT_WITH_PATH,
                                            )
                                            .into_list_err()
                                            .transpose()?;
                                        Ok((dir_fd, maybe_stat))
                                    })
                                    .await?;

                                if let Some(meta) = maybe_stat
                                    .and_then(|stat| {
                                        match stat_to_meta(stat, child_path, OpenDirMode::Get) {
                                            Ok(meta) => Some(Ok(meta)),
                                            Err(Error::NotFound { .. }) => {
                                                // file became a directory, ignore it
                                                None
                                            }
                                            Err(e) => Some(Err(e)),
                                        }
                                    })
                                    .transpose()?
                                {
                                    metas.push(meta);
                                }
                            }
                            FileType::Directory => {
                                dir_fds.push(DirFd::Lazy {
                                    child_prefix: child_path,
                                    parent: Arc::clone(&dir_fd),
                                    child: file_name,
                                });
                            }
                            other => {
                                return Err(Error::Generic {
                                    store: STORE_NAME,
                                    source: format!("invalid file type: {other:?}").into(),
                                });
                            }
                        }
                    }

                    Ok(Some((
                        Self::ReadNextEntry {
                            dir_fds,
                            getdents_buffer,
                            dir_fd,
                            prefix,
                            state,
                        },
                        metas,
                    )))
                }
            }
        }
    }
}
