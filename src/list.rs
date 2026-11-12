use std::{os::fd::OwnedFd, sync::Arc};

use futures_core::stream::BoxStream;
use futures_util::TryStreamExt;
use object_store::{
    Error, ListResult, ObjectMeta, Result,
    path::{DELIMITER_CHAR, Path},
};
use rustix::fs::{AtFlags, FileType};

use crate::{
    constants::STORE_NAME,
    state::State,
    utils::{open_dir_fd, spawn_io, stat_to_meta},
};

pub(crate) fn list(
    state: Arc<State>,
    prefix: Option<Path>,
) -> BoxStream<'static, Result<ObjectMeta>> {
    Box::pin(
        futures_util::stream::once(async move {
            let stream = futures_util::stream::try_unfold(
                NextTask::start(state, prefix).await?,
                async move |mut task| {
                    loop {
                        match task.next().await? {
                            None => {
                                return Ok(None);
                            }
                            Some((task_next, None)) => {
                                task = task_next;
                            }
                            Some((task_next, Some(meta))) => {
                                return Ok(Some((meta, task_next)));
                            }
                        }
                    }
                },
            );

            Result::<_, Error>::Ok(stream)
        })
        .try_flatten(),
    )
}

pub(crate) async fn list_with_delimiter(
    state: Arc<State>,
    prefix: Option<Path>,
) -> Result<ListResult> {
    let mut task = NextTask::start(state, prefix).await?;
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
            Some((next_task, maybe_meta)) => {
                if let Some(meta) = maybe_meta {
                    metas.push(meta);
                }
                match next_task {
                    NextTask::OpenDir { dir_fds, state: _ } => {
                        return Ok(ListResult {
                            common_prefixes: dir_fds
                                .into_iter()
                                .map(|dir_fd| match dir_fd {
                                    DirFd::Eager { .. } => unreachable!(),
                                    DirFd::Lazy {
                                        child_prefix,
                                        parent: _,
                                        child: _,
                                    } => child_prefix,
                                })
                                .collect(),
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
        dir_fd: Arc<OwnedFd>,
    },
    Lazy {
        child_prefix: Path,
        parent: Arc<OwnedFd>,
        child: String,
    },
}

enum NextTask {
    OpenDir {
        dir_fds: Vec<DirFd>,
        state: Arc<State>,
    },
    ReadNextEntry {
        dir_fds: Vec<DirFd>,
        dir_fd: Arc<OwnedFd>,
        // TODO: we may wanna use `RawDir` instead, because that way we could control buffer and syscall usage better
        dir: rustix::fs::Dir,
        prefix: Option<Path>,
        state: Arc<State>,
    },
}

impl NextTask {
    async fn start(state: Arc<State>, prefix: Option<Path>) -> Result<Self> {
        let dir_fd = match &prefix {
            Some(prefix) => {
                state
                    .open_dir(prefix.as_ref().split(DELIMITER_CHAR), false)
                    .await?
            }
            None => state.open_dir(std::iter::empty(), false).await?,
        };

        Ok(Self::OpenDir {
            dir_fds: vec![DirFd::Eager { prefix, dir_fd }],
            state,
        })
    }

    async fn next(self) -> Result<Option<(Self, Option<ObjectMeta>)>> {
        match self {
            Self::OpenDir { mut dir_fds, state } => {
                let Some(dir_fd) = dir_fds.pop() else {
                    // we're done
                    return Ok(None);
                };

                let (prefix, dir, dir_fd) = spawn_io(move || {
                    let (prefix, dir_fd) = match dir_fd {
                        DirFd::Eager { prefix, dir_fd } => (prefix, dir_fd),
                        DirFd::Lazy {
                            child_prefix,
                            parent,
                            child,
                        } => (Some(child_prefix), open_dir_fd(&parent, &child)?),
                    };
                    let dir = rustix::fs::Dir::read_from(&dir_fd)?;
                    Ok((prefix, dir, dir_fd))
                })
                .await?;

                Ok(Some((
                    Self::ReadNextEntry {
                        dir_fds,
                        dir,
                        dir_fd,
                        prefix,
                        state,
                    },
                    None,
                )))
            }
            Self::ReadNextEntry {
                mut dir_fds,
                mut dir,
                dir_fd,
                prefix,
                state,
            } => {
                let read_res = spawn_io(move || {
                    loop {
                        match dir.read() {
                            // skip special entries
                            Some(Ok(entry)) if entry.file_name().to_bytes() == b"." => {}
                            Some(Ok(entry)) if entry.file_name().to_bytes() == b".." => {}
                            // found something
                            Some(Ok(entry)) => return Ok(Some((dir, entry))),
                            // error
                            Some(Err(e)) => return Err(e.into()),
                            // end
                            None => return Ok(None),
                        }
                    }
                })
                .await?;

                match read_res {
                    None => {
                        // end of directory
                        Ok(Some((Self::OpenDir { dir_fds, state }, None)))
                    }
                    Some((dir, entry)) => {
                        let child_str = entry.file_name().to_str().map_err(|e| Error::Generic {
                            store: STORE_NAME,
                            source: Box::new(e),
                        })?;
                        let child_path = match &prefix {
                            Some(prefix) => prefix.clone().join(child_str),
                            None => Path::parse(child_str)?,
                        };
                        let child_string = child_str.to_owned();

                        match entry.file_type() {
                            FileType::RegularFile => {
                                let (dir_fd, stat) = spawn_io(move || {
                                    let stat = rustix::fs::statat(
                                        &dir_fd,
                                        &child_string,
                                        AtFlags::empty(),
                                    )?;
                                    Ok((dir_fd, stat))
                                })
                                .await?;
                                let meta = stat_to_meta(stat, child_path)?;
                                let task = Self::ReadNextEntry {
                                    dir_fds,
                                    dir,
                                    dir_fd,
                                    prefix,
                                    state,
                                };

                                Ok(Some((task, Some(meta))))
                            }
                            FileType::Directory => {
                                dir_fds.push(DirFd::Lazy {
                                    child_prefix: child_path,
                                    parent: Arc::clone(&dir_fd),
                                    child: child_string,
                                });

                                Ok(Some((
                                    Self::ReadNextEntry {
                                        dir_fds,
                                        dir,
                                        dir_fd,
                                        prefix,
                                        state,
                                    },
                                    None,
                                )))
                            }
                            other => Err(Error::Generic {
                                store: STORE_NAME,
                                source: format!("invalid file type: {other:?}").into(),
                            }),
                        }
                    }
                }
            }
        }
    }
}
