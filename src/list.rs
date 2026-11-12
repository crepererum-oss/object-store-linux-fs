use std::{os::fd::OwnedFd, sync::Arc};

use futures_core::stream::BoxStream;
use futures_util::TryStreamExt;
use object_store::{
    Error, ListResult, ObjectMeta, Result,
    path::{DELIMITER_CHAR, Path, PathPart},
};
use rustix::fs::{AtFlags, FileType};

use crate::{
    constants::STORE_NAME,
    error::IOErrorExt,
    state::{OpenDirMode, State},
    utils::{open_dir_fd, spawn_io, stat_to_meta},
};

pub(crate) fn list(
    state: Arc<State>,
    prefix: Option<Path>,
    offset: Option<Path>,
) -> BoxStream<'static, Result<ObjectMeta>> {
    Box::pin(
        futures_util::stream::once(async move {
            match NextTask::start(state, prefix).await {
                Ok(Some(task)) => {
                    let stream = futures_util::stream::try_unfold(
                        (task, offset),
                        async move |(mut task, offset)| {
                            loop {
                                match task.next().await? {
                                    None => {
                                        return Ok(None);
                                    }
                                    Some((task_next, None)) => {
                                        // technically we could filter out branches that are not gonna help, but the
                                        // tests in `object_store` is broken because they use `Path=String` for
                                        // comparison, which isn't really hierarchical correct
                                        task = task_next;
                                    }
                                    Some((task_next, Some(meta))) => {
                                        if offset
                                            .as_ref()
                                            .map(|offset| &meta.location > offset)
                                            .unwrap_or(true)
                                        {
                                            return Ok(Some((meta, (task_next, offset))));
                                        } else {
                                            task = task_next;
                                        }
                                    }
                                }
                            }
                        },
                    );

                    Result::<_, Error>::Ok(Box::pin(stream) as BoxStream<'static, _>)
                }
                Ok(None) => Ok(Box::pin(futures_util::stream::empty()) as _),
                Err(e) => Err(e),
            }
        })
        .try_flatten(),
    )
}

pub(crate) async fn list_with_delimiter(
    state: Arc<State>,
    prefix: Option<Path>,
) -> Result<ListResult> {
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
            Some((next_task, maybe_meta)) => {
                if let Some(meta) = maybe_meta {
                    metas.push(meta);
                }
                match next_task {
                    NextTask::OpenDir { dir_fds, state: _ } => {
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
    async fn start(state: Arc<State>, prefix: Option<Path>) -> Result<Option<Self>> {
        let dir_fd_res = match &prefix {
            Some(prefix) => {
                state
                    .open_dir(prefix.as_ref().split(DELIMITER_CHAR), OpenDirMode::Get)
                    .await
            }
            None => state.open_dir(std::iter::empty(), OpenDirMode::Get).await,
        };

        match dir_fd_res {
            Ok(dir_fd) => Ok(Some(Self::OpenDir {
                dir_fds: vec![DirFd::Eager { prefix, dir_fd }],
                state,
            })),
            Err(Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn next(self) -> Result<Option<(Self, Option<ObjectMeta>)>> {
        match self {
            Self::OpenDir { mut dir_fds, state } => {
                let Some(dir_fd) = dir_fds.pop() else {
                    // we're done
                    return Ok(None);
                };

                let maybe_opened = spawn_io(move || {
                    let (prefix, dir_fd) = match dir_fd {
                        DirFd::Eager { prefix, dir_fd } => (prefix, dir_fd),
                        DirFd::Lazy {
                            child_prefix,
                            parent,
                            child,
                        } => {
                            let Some(dir_fd) =
                                open_dir_fd(&parent, &child).into_list_err().transpose()?
                            else {
                                return Ok(None);
                            };
                            (Some(child_prefix), dir_fd)
                        }
                    };
                    let Some(dir) = rustix::fs::Dir::read_from(&dir_fd)
                        .into_list_err()
                        .transpose()?
                    else {
                        return Ok(None);
                    };
                    Ok(Some((prefix, dir, dir_fd)))
                })
                .await?;

                match maybe_opened {
                    Some((prefix, dir, dir_fd)) => Ok(Some((
                        Self::ReadNextEntry {
                            dir_fds,
                            dir,
                            dir_fd,
                            prefix,
                            state,
                        },
                        None,
                    ))),
                    None => Ok(Some((Self::OpenDir { dir_fds, state }, None))),
                }
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
                            Some(Err(e)) => match e.into_list_err() {
                                Some(e) => return Err(e),
                                None => return Ok(None),
                            },
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
                            Some(prefix) => prefix.clone().join(
                                // parse part before handing it to join, because `PathPart::from(str)` will percent-encode it
                                PathPart::parse(child_str).map_err(|e| Error::InvalidPath {
                                    source: object_store::path::Error::BadSegment {
                                        path: child_str.to_owned(),
                                        source: e,
                                    },
                                })?,
                            ),
                            None => Path::parse(child_str)?,
                        };
                        let child_string = child_str.to_owned();

                        match entry.file_type() {
                            FileType::RegularFile => {
                                let (dir_fd, maybe_stat) = spawn_io(move || {
                                    let maybe_stat = rustix::fs::statat(
                                        &dir_fd,
                                        &child_string,
                                        AtFlags::empty(),
                                    )
                                    .into_list_err()
                                    .transpose()?;
                                    Ok((dir_fd, maybe_stat))
                                })
                                .await?;

                                let meta = maybe_stat
                                    .map(|stat| stat_to_meta(stat, child_path))
                                    .transpose()?;
                                let task = Self::ReadNextEntry {
                                    dir_fds,
                                    dir,
                                    dir_fd,
                                    prefix,
                                    state,
                                };

                                Ok(Some((task, meta)))
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
