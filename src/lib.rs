use std::{ops::Range, os::fd::OwnedFd, path::PathBuf, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::DateTime;
use futures_core::stream::BoxStream;
use futures_util::TryStreamExt;
use object_store::{
    CopyOptions, Error, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    Result,
    path::{DELIMITER_CHAR, Path},
};
use rustix::fs::{AtFlags, FileType, OFlags};

use crate::{
    constants::STORE_NAME,
    state::State,
    utils::{mode_create, open_dir_fd, rflags, spawn_io, split_dirs_and_filename},
};

mod constants;
mod state;
mod utils;

#[derive(Debug)]
pub struct LinuxFS {
    state: Arc<State>,
}

impl LinuxFS {
    pub async fn new(root: PathBuf) -> Result<Self> {
        Ok(Self {
            state: Arc::new(State::new(root).await?),
        })
    }
}

impl std::fmt::Display for LinuxFS {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{STORE_NAME}(\"{}\")", self.state.root().display())
    }
}

#[async_trait]
impl ObjectStore for LinuxFS {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let PutOptions {
            mode,
            // ignore tags
            tags: _,
            attributes,
            // ignore extensions
            extensions: _,
        } = opts;
        if mode != PutMode::Overwrite {
            return Err(Error::NotImplemented {
                operation: format!("put_opts(mode={mode:?})"),
                implementer: STORE_NAME.to_owned(),
            });
        }
        if !attributes.is_empty() {
            return Err(Error::NotImplemented {
                operation: "put_opts(attributes=<NOT_EMPTY>)".to_owned(),
                implementer: STORE_NAME.to_owned(),
            });
        }

        let (dirs, file) = split_dirs_and_filename(location)?;
        let dir_fd = self.state.open_dir(dirs, true).await?;
        let file = file.to_owned();

        spawn_io(move || {
            let file_fd = rustix::fs::openat2(
                &dir_fd,
                ".",
                OFlags::TMPFILE | OFlags::WRONLY,
                mode_create(),
                rflags(),
            )?;
            for part in payload {
                let mut offset = 0;
                while offset < part.len() {
                    offset += rustix::io::write(&file_fd, &part[offset..])?;
                }
            }
            rustix::fs::fsync(&file_fd)?;

            rustix::fs::linkat(&file_fd, "", &dir_fd, file, AtFlags::EMPTY_PATH)?;
            rustix::fs::fsync(&dir_fd)?;

            Ok(())
        })
        .await?;

        Ok(PutResult {
            e_tag: None,
            version: None,
            extensions: Default::default(),
        })
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        todo!()
    }

    async fn get_opts(&self, _location: &Path, _options: GetOptions) -> Result<GetResult> {
        Err(Error::NotImplemented {
            operation: "get_opts".to_owned(),
            implementer: STORE_NAME.to_owned(),
        })
    }

    async fn get_ranges(&self, _location: &Path, _ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        Err(Error::NotImplemented {
            operation: "get_ranges".to_owned(),
            implementer: STORE_NAME.to_owned(),
        })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let state = Arc::clone(&self.state);

        Box::pin(locations.and_then(move |location| {
            let state = Arc::clone(&state);
            async move {
                let (dirs, file) = split_dirs_and_filename(&location)?;
                let dir_fd = state.open_dir(dirs, false).await?;
                let file = file.to_owned();
                spawn_io(move || {
                    rustix::fs::unlinkat(&dir_fd, file, AtFlags::empty())?;
                    rustix::fs::fsync(&dir_fd)?;
                    Ok(())
                })
                .await?;
                Ok(location)
            }
        }))
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let prefix = prefix.cloned();
        let state = Arc::clone(&self.state);

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

        Box::pin(
            futures_util::stream::once(async move {
                let dir_fd = match &prefix {
                    Some(prefix) => {
                        state
                            .open_dir(prefix.as_ref().split(DELIMITER_CHAR), false)
                            .await?
                    }
                    None => state.open_dir(std::iter::empty(), false).await?,
                };

                let stream = futures_util::stream::try_unfold(
                    NextTask::OpenDir {
                        dir_fds: vec![DirFd::Eager { prefix, dir_fd }],
                        state,
                    },
                    async move |mut task| {
                        loop {
                            task = match task {
                                NextTask::OpenDir { mut dir_fds, state } => {
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
                                            } => {
                                                (Some(child_prefix), open_dir_fd(&parent, &child)?)
                                            }
                                        };
                                        let dir = rustix::fs::Dir::read_from(&dir_fd)?;
                                        Ok((prefix, dir, dir_fd))
                                    })
                                    .await?;

                                    NextTask::ReadNextEntry {
                                        dir_fds,
                                        dir,
                                        dir_fd,
                                        prefix,
                                        state,
                                    }
                                }
                                NextTask::ReadNextEntry {
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
                                                Some(Ok(entry))
                                                    if entry.file_name().to_bytes() == b"." => {}
                                                Some(Ok(entry))
                                                    if entry.file_name().to_bytes() == b".." => {}
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
                                            NextTask::OpenDir { dir_fds, state }
                                        }
                                        Some((dir, entry)) => {
                                            let child_str =
                                                entry.file_name().to_str().map_err(|e| {
                                                    Error::Generic {
                                                        store: STORE_NAME,
                                                        source: Box::new(e),
                                                    }
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
                                                    let meta = ObjectMeta {
                                                        location: child_path,
                                                        last_modified: DateTime::from_timestamp(
                                                            stat.st_mtime as _,
                                                            stat.st_mtime_nsec as _,
                                                        )
                                                        .ok_or_else(|| Error::Generic {
                                                            store: STORE_NAME,
                                                            source: "cannot read mtime"
                                                                .to_owned()
                                                                .into(),
                                                        })?,
                                                        size: stat.st_size as _,
                                                        e_tag: None,
                                                        version: None,
                                                    };
                                                    let task = NextTask::ReadNextEntry {
                                                        dir_fds,
                                                        dir,
                                                        dir_fd,
                                                        prefix,
                                                        state,
                                                    };

                                                    return Ok(Some((meta, task)));
                                                }
                                                FileType::Directory => {
                                                    dir_fds.push(DirFd::Lazy {
                                                        child_prefix: child_path,
                                                        parent: Arc::clone(&dir_fd),
                                                        child: child_string,
                                                    });

                                                    NextTask::ReadNextEntry {
                                                        dir_fds,
                                                        dir,
                                                        dir_fd,
                                                        prefix,
                                                        state,
                                                    }
                                                }
                                                other => {
                                                    return Err(Error::Generic {
                                                        store: STORE_NAME,
                                                        source: format!(
                                                            "invalid file type: {other:?}"
                                                        )
                                                        .into(),
                                                    });
                                                }
                                            }
                                        }
                                    }
                                }
                            };
                        }
                    },
                );

                Result::<_, Error>::Ok(stream)
            })
            .try_flatten(),
        )
    }

    fn list_with_offset(
        &self,
        _prefix: Option<&Path>,
        _offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        Box::pin(futures_util::stream::once(async {
            Err(Error::NotImplemented {
                operation: "list_with_offset".to_owned(),
                implementer: STORE_NAME.to_owned(),
            })
        }))
    }

    async fn list_with_delimiter(&self, _prefix: Option<&Path>) -> Result<ListResult> {
        Err(Error::NotImplemented {
            operation: "list_with_delimiter".to_owned(),
            implementer: STORE_NAME.to_owned(),
        })
    }

    async fn copy_opts(&self, _from: &Path, _to: &Path, _options: CopyOptions) -> Result<()> {
        Err(Error::NotImplemented {
            operation: "copy_opts".to_owned(),
            implementer: STORE_NAME.to_owned(),
        })
    }

    async fn rename_opts(&self, _from: &Path, _to: &Path, _options: RenameOptions) -> Result<()> {
        Err(Error::NotImplemented {
            operation: "rename_opts".to_owned(),
            implementer: STORE_NAME.to_owned(),
        })
    }
}
