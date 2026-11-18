use std::{
    io::IoSliceMut,
    ops::{Deref, DerefMut, Range},
    path::PathBuf,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures_core::stream::BoxStream;
use futures_util::TryStreamExt;
use object_store::{
    CopyMode, CopyOptions, Error, GetOptions, GetRange, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, RenameOptions, RenameTargetMode, Result, UploadPart, path::Path,
};
use rustix::fs::{AtFlags, Mode};

use crate::{
    attributes::read_attrs_from_fd,
    constants::{OFLAGS_READ, READ_WRITE_FLAGS, RESOLVE_FLAGS, STORE_NAME},
    error::IOErrorExt,
    io::{IOLayer, IORwLock, spawn_io},
    put::{PreparedPutOperation, RunningPutOperation},
    state::State,
    utils::{
        OpenDirMode, TransferFDs, flock_dir, link_at_replace, prepare_transfer_dirs,
        split_dirs_and_filename, stat_to_meta,
    },
};

pub use options::LinuxFSOptions;

mod attributes;
pub mod constants;
mod error;
pub mod io;
mod list;
mod options;
mod put;
mod state;
mod utils;

#[derive(Debug)]
pub struct LinuxFS {
    state: Arc<State>,
}

impl LinuxFS {
    pub async fn new(root: PathBuf, options: LinuxFSOptions) -> Result<Self> {
        Ok(Self {
            state: Arc::new(State::new(root, options).await?),
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
        let op = PreparedPutOperation::new(location, opts, &self.state).await?;

        let meta = spawn_io(&self.state.options().io_layer, move |io| {
            let op = op.blocking_io_start(io)?;
            op.blocking_io_put(io, payload, 0)?;
            op.blocking_io_commit(io)
        })
        .await?;

        Ok(PutResult {
            e_tag: meta.e_tag,
            version: None,
            extensions: Default::default(),
        })
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let PutMultipartOptions {
            tags,
            attributes,
            extensions,
        } = opts;
        let opts = PutOptions {
            mode: PutMode::Overwrite,
            tags,
            attributes,
            extensions,
        };
        let op = PreparedPutOperation::new(location, opts, &self.state).await?;
        let op = spawn_io(&self.state.options().io_layer, move |io| {
            op.blocking_io_start(io)
        })
        .await?;
        Ok(Box::new(LinuxFSUpload {
            state: Arc::new(IORwLock::new(
                &self.state.options().io_layer,
                UploadState::Running { op },
            )),
            parts: Arc::new(Mutex::new(UploadParts {
                parts: Default::default(),
                offset: 0,
            })),
            io_layer: Arc::clone(&self.state.options().io_layer),
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        if options.version.is_some() {
            return Err(Error::Precondition {
                path: location.to_string(),
                source: "versions are not used".to_owned().into(),
            });
        }

        let state = Arc::clone(&self.state);
        let location = location.clone();
        let read_buffer_size = self.state.options().read_buffer_size.get();
        let io_layer = Arc::clone(&self.state.options().io_layer);

        spawn_io(&self.state.options().io_layer, move |io| {
            let (dirs, file) = split_dirs_and_filename(&location, OpenDirMode::Get)?;
            let dir_fd = state.open_dir(io, dirs, OpenDirMode::Get)?;

            let file_fd = io
                .openat2(
                    dir_fd.as_ref(),
                    file,
                    OFLAGS_READ,
                    Mode::empty(),
                    RESOLVE_FLAGS,
                )
                .into_get_err(&location)?;

            let stat = io.fstat(file_fd.as_ref()).into_get_err(&location)?;
            let meta = stat_to_meta(stat, location.clone(), OpenDirMode::Get)?;
            options.check_preconditions(&meta)?;

            let attributes = read_attrs_from_fd(io, file_fd.as_ref()).into_get_err(&location)?;

            let range = match options.range {
                None => 0..meta.size,
                Some(range) => range.as_range(meta.size).map_err(|e| Error::Generic {
                    store: STORE_NAME,
                    source: Box::new(e),
                })?,
            };
            let location = Arc::new(location);

            let stream = if options.head || range.is_empty() {
                Box::pin(futures_util::stream::empty()) as BoxStream<'static, Result<Bytes>>
            } else {
                let buf = vec![0; read_buffer_size];
                Box::pin(futures_util::stream::try_unfold(
                    (
                        io_layer,
                        file_fd,
                        buf,
                        range.start as usize,
                        range.end as usize,
                    ),
                    move |(io_layer, file_fd, mut buf, offset, range_end)| {
                        let location = Arc::clone(&location);

                        async move {
                            let res = spawn_io(&io_layer, move |io| {
                                let whats_left = range_end - offset;
                                if whats_left == 0 {
                                    drop(file_fd);
                                    return Ok(None);
                                }

                                let to_read = buf.len().min(whats_left);
                                let got = io
                                    .preadv2(
                                        file_fd.as_ref(),
                                        &mut [IoSliceMut::new(&mut buf[..to_read])],
                                        offset as _,
                                        READ_WRITE_FLAGS,
                                    )
                                    .into_get_err(&location)?;

                                if got == 0 {
                                    drop(file_fd);
                                    Ok(None)
                                } else {
                                    let bytes = Bytes::from(buf[..got].to_owned());
                                    let offset = offset + got;
                                    Ok(Some((bytes, (file_fd, buf, offset, range_end))))
                                }
                            })
                            .await;

                            res.map(|opt| {
                                opt.map(|(bytes, (file_fd, buf, offset, range_end))| {
                                    (bytes, (io_layer, file_fd, buf, offset, range_end))
                                })
                            })
                        }
                    },
                ))
            };

            Ok(GetResult {
                payload: GetResultPayload::Stream(stream),
                meta,
                range,
                attributes,
                extensions: Default::default(),
            })
        })
        .await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let state = Arc::clone(&self.state);
        let location = location.clone();
        let ranges = ranges.to_vec();

        spawn_io(&self.state.options().io_layer, move |io| {
            let (dirs, file) = split_dirs_and_filename(&location, OpenDirMode::Get)?;
            let dir_fd = state.open_dir(io, dirs, OpenDirMode::Get)?;

            let file_fd = io
                .openat2(
                    dir_fd.as_ref(),
                    file,
                    OFLAGS_READ,
                    Mode::empty(),
                    RESOLVE_FLAGS,
                )
                .into_get_err(&location)?;

            let stat = io.fstat(file_fd.as_ref()).into_get_err(&location)?;
            let meta = stat_to_meta(stat, location.clone(), OpenDirMode::Get)?;

            let mut bytes_vec = Vec::with_capacity(ranges.len());

            for range in ranges {
                // validate range using common code for consistent error handling
                let range =
                    GetRange::Bounded(range)
                        .as_range(meta.size)
                        .map_err(|e| Error::Generic {
                            store: STORE_NAME,
                            source: Box::new(e),
                        })?;

                let mut buffer = vec![0u8; range.clone().count()];
                let mut buffer_offset = 0usize;

                let mut file_offset = range.start;

                loop {
                    let to_read = range.end.saturating_sub(file_offset);
                    if to_read == 0 {
                        break;
                    }

                    let got = io
                        .preadv2(
                            file_fd.as_ref(),
                            &mut [IoSliceMut::new(
                                &mut buffer[buffer_offset..(buffer_offset + to_read as usize)],
                            )],
                            file_offset,
                            READ_WRITE_FLAGS,
                        )
                        .into_get_err(&location)?;

                    buffer_offset += got;
                    file_offset += got as u64;

                    if got == 0 {
                        // EOF
                        break;
                    }
                }

                buffer.resize(buffer_offset, 0);
                bytes_vec.push(Bytes::from(buffer));
            }

            drop(file_fd);

            Ok(bytes_vec)
        })
        .await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let state = Arc::clone(&self.state);

        Box::pin(locations.and_then(move |location| {
            let state = Arc::clone(&state);
            async move {
                let state2 = Arc::clone(&state);
                let location = spawn_io(&state.options().io_layer, move |io| {
                    let (dirs, file) = split_dirs_and_filename(&location, OpenDirMode::Get)?;
                    let dir_fd = state2.open_dir(io, dirs, OpenDirMode::Get)?;

                    let _lock_fd = flock_dir(io, dir_fd.as_ref()).into_other_err()?;
                    io.unlinkat(dir_fd.as_ref(), file, AtFlags::empty())
                        .into_get_err(&location)?;
                    io.fsync(dir_fd.as_ref()).into_other_err()?;
                    Ok(location)
                })
                .await?;
                Ok(location)
            }
        }))
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        crate::list::list(Arc::clone(&self.state), prefix.cloned(), None)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        crate::list::list(
            Arc::clone(&self.state),
            prefix.cloned(),
            Some(offset.clone()),
        )
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        crate::list::list_with_delimiter(Arc::clone(&self.state), prefix.cloned()).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        let CopyOptions {
            mode,
            // ignore extensions
            extensions: _,
        } = options;

        let state = Arc::clone(&self.state);
        let from = from.clone();
        let to = to.clone();

        spawn_io(&self.state.options().io_layer, move |io| {
            // flock BEFORE reading file because the source file might move under us and we cannot relink/rematerialize
            // it incl. metadata and attributes otherwise
            let TransferFDs {
                from_dir_fd,
                from_lock_fd: _from_lock_fd,
                from_file,
                to_dir_fd,
                to_lock_fd: _to_lock_fd,
                to_file,
            } = prepare_transfer_dirs(io, &from, &to, &state)?;

            let file_fd = io
                .openat2(
                    from_dir_fd.as_ref(),
                    from_file,
                    OFLAGS_READ,
                    Mode::empty(),
                    RESOLVE_FLAGS,
                )
                .into_get_err(&from)?;

            let stat = io.fstat(file_fd.as_ref()).into_get_err(&from)?;
            stat_to_meta(stat, from.clone(), OpenDirMode::Get)?;

            match mode {
                CopyMode::Overwrite => {
                    link_at_replace(io, file_fd.as_ref(), to_dir_fd.as_ref(), to_file)
                        .into_put_err(&to)?;
                }
                CopyMode::Create => {
                    io.linkat(
                        file_fd.as_ref(),
                        "",
                        to_dir_fd.as_ref(),
                        to_file,
                        AtFlags::EMPTY_PATH,
                    )
                    .into_put_err(&to)?;
                }
            }
            io.fsync(to_dir_fd.as_ref()).into_other_err()?;

            Ok(())
        })
        .await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        let RenameOptions {
            target_mode,
            // ignore extensions
            extensions: _,
        } = options;

        let state = Arc::clone(&self.state);
        let from = from.clone();
        let to = to.clone();

        spawn_io(&self.state.options().io_layer, move |io| {
            // flock BEFORE reading file because the source file might move under us and we cannot relink/rematerialize
            // it incl. metadata and attributes otherwise
            let TransferFDs {
                from_dir_fd,
                from_lock_fd: _from_lock_fd,
                from_file,
                to_dir_fd,
                to_lock_fd: _to_lock_fd,
                to_file,
            } = prepare_transfer_dirs(io, &from, &to, &state)?;

            let file_fd = io
                .openat2(
                    from_dir_fd.as_ref(),
                    from_file,
                    OFLAGS_READ,
                    Mode::empty(),
                    RESOLVE_FLAGS,
                )
                .into_get_err(&from)?;

            let stat = io.fstat(file_fd.as_ref()).into_get_err(from.as_ref())?;
            stat_to_meta(stat, from.clone(), OpenDirMode::Get)?;

            match target_mode {
                RenameTargetMode::Overwrite => {
                    link_at_replace(io, file_fd.as_ref(), to_dir_fd.as_ref(), to_file)
                        .into_put_err(&to)?;
                }
                RenameTargetMode::Create => {
                    io.linkat(
                        file_fd.as_ref(),
                        "",
                        to_dir_fd.as_ref(),
                        to_file,
                        AtFlags::EMPTY_PATH,
                    )
                    .into_put_err(&to)?;
                }
            }
            if to != from {
                io.unlinkat(from_dir_fd.as_ref(), from_file, AtFlags::empty())
                    .into_get_err(&from)?;
            }

            // fsync AFTER both operations complete and in-order
            io.fsync(to_dir_fd.as_ref()).into_other_err()?;
            if !Arc::ptr_eq(&from_dir_fd, &to_dir_fd) {
                io.fsync(from_dir_fd.as_ref()).into_other_err()?;
            }

            Ok(())
        })
        .await
    }
}

#[derive(Debug)]
enum UploadState {
    Running { op: RunningPutOperation },
    Finished { broken: bool },
}

#[derive(Debug)]
struct UploadParts {
    offset: usize,
    parts: Vec<PartState>,
}

impl UploadParts {
    fn any_failed(&self) -> Result<bool> {
        let mut any_failed = false;
        for part in &self.parts {
            match part {
                PartState::Running => {
                    return Err(LinuxFSUpload::err_still_running());
                }
                PartState::Failed => {
                    any_failed = true;
                }
                PartState::Ok => {}
            }
        }
        Ok(any_failed)
    }
}

#[derive(Debug)]
enum PartState {
    Running,
    Failed,
    Ok,
}

#[derive(Debug)]
struct LinuxFSUpload {
    state: Arc<IORwLock<UploadState>>,

    // NOTE: this does NOT count as an I/O lock because we don't do I/O during it
    parts: Arc<Mutex<UploadParts>>,

    io_layer: Arc<dyn IOLayer>,
}

impl LinuxFSUpload {
    fn err_still_running() -> Error {
        Error::Generic {
            store: STORE_NAME,
            source: "upload is still running".to_owned().into(),
        }
    }

    fn err_not_running() -> Error {
        Error::Generic {
            store: STORE_NAME,
            source: "upload no longer running".to_owned().into(),
        }
    }

    fn err_failed() -> Error {
        Error::Generic {
            store: STORE_NAME,
            source: "an upload part failed".to_owned().into(),
        }
    }
}

#[async_trait]
impl MultipartUpload for LinuxFSUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let state = Arc::clone(&self.state);
        let parts = Arc::clone(&self.parts);

        let io_layer = Arc::clone(&self.io_layer);

        // short-lived lock to set the part state
        let (offset, idx) = {
            let mut parts = parts.lock().unwrap();

            let offset = parts.offset;
            parts.offset += data.content_length();

            let idx = parts.parts.len();
            parts.parts.push(PartState::Running);

            (offset, idx)
        };

        Box::pin(async move {
            spawn_io(&io_layer, move |io| {
                let state = state.read(io);

                let op = match state.deref() {
                    UploadState::Running { op } => op,
                    UploadState::Finished { broken: _ } => {
                        return Err(Self::err_not_running());
                    }
                };

                // perform IO
                let res = op.blocking_io_put(io, data, offset);

                // short-lived lock to set the part state
                {
                    let mut parts = parts.lock().unwrap();
                    parts.parts[idx] = match &res {
                        Ok(()) => PartState::Ok,
                        Err(_) => PartState::Failed,
                    };
                }

                res
            })
            .await
        })
    }

    async fn complete(&mut self) -> Result<PutResult> {
        // short-lived lock to get the part state
        let any_failed = {
            let parts = self.parts.lock().unwrap();
            parts.any_failed()?
        };

        let state = Arc::clone(&self.state);

        let meta = spawn_io(&self.io_layer, move |io| {
            let mut state = state.write(io);

            let mut tmp = UploadState::Finished { broken: true };
            std::mem::swap(state.deref_mut(), &mut tmp);

            let res = match tmp {
                UploadState::Running { op } => {
                    if any_failed {
                        drop(op);
                        Err(Self::err_failed())
                    } else {
                        op.blocking_io_commit(io)
                    }
                }
                UploadState::Finished { .. } => Err(Self::err_not_running()),
            };

            *state = UploadState::Finished {
                broken: res.is_err(),
            };
            res
        })
        .await?;

        Ok(PutResult {
            e_tag: meta.e_tag,
            version: None,
            extensions: Default::default(),
        })
    }

    async fn abort(&mut self) -> Result<()> {
        // short-lived lock to get the part state
        let any_failed = {
            let parts = self.parts.lock().unwrap();
            parts.any_failed()?
        };

        let state = Arc::clone(&self.state);

        // drop in I/O runtime
        spawn_io(&self.io_layer, move |io| {
            let mut state = state.write(io);

            let mut tmp = UploadState::Finished { broken: true };
            std::mem::swap(state.deref_mut(), &mut tmp);

            let res = match tmp {
                UploadState::Running { op } => {
                    if any_failed {
                        // allow "abort after fail"
                        Ok(true)
                    } else {
                        drop(op);
                        Ok(false)
                    }
                }
                UploadState::Finished { broken } => {
                    if broken {
                        // allow "abort after fail"
                        Ok(true)
                    } else {
                        Err(Self::err_not_running())
                    }
                }
            };

            *state = UploadState::Finished {
                broken: match res {
                    Ok(true) => true,
                    Ok(false) => false,
                    Err(_) => true,
                },
            };

            res.map(|_| ())
        })
        .await
    }
}
