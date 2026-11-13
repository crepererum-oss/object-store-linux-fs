use std::{io::IoSliceMut, ops::Range, path::PathBuf, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use futures_core::stream::BoxStream;
use futures_util::TryStreamExt;
use object_store::{
    CopyMode, CopyOptions, Error, GetOptions, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, RenameOptions, RenameTargetMode, Result, UploadPart, path::Path,
};
use rustix::fs::{AtFlags, Mode};

use crate::{
    constants::{READ_BUFFER_SIZE, STORE_NAME},
    error::IOErrorExt,
    put::{PreparedPutOperation, RunningPutOperation},
    state::State,
    utils::{
        OpenDirMode, flock_dir, link_at_replace, oflags_read, rflags, rwflags, same_dir, spawn_io,
        split_dirs_and_filename, stat_to_meta,
    },
};

pub mod constants;
mod error;
mod list;
mod put;
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
        let op = PreparedPutOperation::new(location, opts, &self.state).await?;

        let meta = spawn_io(move || {
            let op = op.blocking_io_start()?;
            op.blocking_io_put(payload, 0)?;
            op.blocking_io_commit()
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
        let op = spawn_io(move || op.blocking_io_start()).await?;
        Ok(Box::new(LinuxFSUpload {
            op: Some(Arc::new(op)),
            offset: 0,
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        if options.version.is_some() {
            return Err(Error::Precondition {
                path: location.to_string(),
                source: "versions are not used".to_owned().into(),
            });
        }

        let (dirs, file) = split_dirs_and_filename(location, OpenDirMode::Get)?;
        let dir_fd = self.state.open_dir(dirs, OpenDirMode::Get).await?;
        let file = file.to_owned();
        let location = location.clone();

        spawn_io(move || {
            let file_fd =
                rustix::fs::openat2(&dir_fd, file, oflags_read(), Mode::empty(), rflags())
                    .into_get_err(&location)?;

            let stat = rustix::fs::fstat(&file_fd).into_get_err(&location)?;
            let meta = stat_to_meta(stat, location.clone())?;
            options.check_preconditions(&meta)?;

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
                let buf = vec![0; READ_BUFFER_SIZE];
                Box::pin(futures_util::stream::try_unfold(
                    (file_fd, buf, range.start as usize, range.end as usize),
                    move |(file_fd, mut buf, offset, range_end)| {
                        let location = Arc::clone(&location);
                        spawn_io(move || {
                            let whats_left = range_end - offset;
                            if whats_left == 0 {
                                drop(file_fd);
                                return Ok(None);
                            }

                            let to_read = buf.len().min(whats_left);
                            let got = rustix::io::preadv2(
                                &file_fd,
                                &mut [IoSliceMut::new(&mut buf[..to_read])],
                                offset as _,
                                rwflags(),
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
                    },
                ))
            };

            Ok(GetResult {
                payload: GetResultPayload::Stream(stream),
                meta,
                range,
                attributes: Default::default(),
                extensions: Default::default(),
            })
        })
        .await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let (dirs, file) = split_dirs_and_filename(location, OpenDirMode::Get)?;
        let dir_fd = self.state.open_dir(dirs, OpenDirMode::Get).await?;
        let file = file.to_owned();
        let location = location.clone();
        let ranges = ranges.to_vec();

        spawn_io(move || {
            let file_fd =
                rustix::fs::openat2(&dir_fd, file, oflags_read(), Mode::empty(), rflags())
                    .into_get_err(&location)?;

            let stat = rustix::fs::fstat(&file_fd).into_get_err(&location)?;
            stat_to_meta(stat, location.clone())?;

            let mut bytes_vec = Vec::with_capacity(ranges.len());

            for range in ranges {
                let mut buffer = vec![0u8; range.clone().count()];
                let mut buffer_offset = 0usize;

                let mut file_offset = range.start;

                loop {
                    let to_read = range.end.saturating_sub(file_offset);
                    if to_read == 0 {
                        break;
                    }

                    let got = rustix::io::preadv2(
                        &file_fd,
                        &mut [IoSliceMut::new(
                            &mut buffer[buffer_offset..(buffer_offset + to_read as usize)],
                        )],
                        file_offset,
                        rwflags(),
                    )
                    .into_get_err(&location)?;

                    buffer_offset += got;
                    file_offset += got as u64;
                }

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
                let (dirs, file) = split_dirs_and_filename(&location, OpenDirMode::Get)?;
                let dir_fd = state.open_dir(dirs, OpenDirMode::Get).await?;
                let file = file.to_owned();
                let location = spawn_io(move || {
                    let _lock_fd = flock_dir(&dir_fd).into_other_err()?;
                    rustix::fs::unlinkat(&dir_fd, file, AtFlags::empty())
                        .into_get_err(&location)?;
                    rustix::fs::fsync(&dir_fd).into_other_err()?;
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

        let (from_dirs, from_file) = split_dirs_and_filename(from, OpenDirMode::Get)?;
        let from_dir_fd = self.state.open_dir(from_dirs, OpenDirMode::Get).await?;

        let (to_dir_fd, to_file) = if same_dir(from, to) {
            let (_to_dirs, to_file) = split_dirs_and_filename(to, OpenDirMode::Put)?;
            (Arc::clone(&from_dir_fd), to_file)
        } else {
            let (to_dirs, to_file) = split_dirs_and_filename(to, OpenDirMode::Put)?;
            let to_dir_fd = self.state.open_dir(to_dirs, OpenDirMode::Put).await?;
            (to_dir_fd, to_file)
        };

        let from_file = from_file.to_owned();
        let to_file = to_file.to_owned();
        let from = from.clone();
        let to = to.clone();

        spawn_io(move || {
            let file_fd = rustix::fs::openat2(
                &from_dir_fd,
                from_file,
                oflags_read(),
                Mode::empty(),
                rflags(),
            )
            .into_get_err(&from)?;

            let stat = rustix::fs::fstat(&file_fd).into_get_err(&from)?;
            stat_to_meta(stat, from.clone())?;

            let _to_lock_fd = flock_dir(&to_dir_fd).into_other_err()?;

            match mode {
                CopyMode::Overwrite => {
                    link_at_replace(&file_fd, &to_dir_fd, &to_file).into_other_err()?;
                }
                CopyMode::Create => {
                    rustix::fs::linkat(&file_fd, "", &to_dir_fd, &to_file, AtFlags::EMPTY_PATH)
                        .into_put_err(to)?;
                }
            }
            rustix::fs::fsync(&to_dir_fd).into_other_err()?;

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
        let (from_dirs, from_file) = split_dirs_and_filename(from, OpenDirMode::Get)?;
        let from_dir_fd = self.state.open_dir(from_dirs, OpenDirMode::Get).await?;

        let (to_dir_fd, to_file) = if same_dir(from, to) {
            let (_to_dirs, to_file) = split_dirs_and_filename(to, OpenDirMode::Put)?;
            (Arc::clone(&from_dir_fd), to_file)
        } else {
            let (to_dirs, to_file) = split_dirs_and_filename(to, OpenDirMode::Put)?;
            let to_dir_fd = self.state.open_dir(to_dirs, OpenDirMode::Put).await?;
            (to_dir_fd, to_file)
        };

        let from_file = from_file.to_owned();
        let to_file = to_file.to_owned();
        let from = from.clone();
        let to = to.clone();

        spawn_io(move || {
            let file_fd = rustix::fs::openat2(
                &from_dir_fd,
                &from_file,
                oflags_read(),
                Mode::empty(),
                rflags(),
            )
            .into_get_err(&from)?;

            let stat = rustix::fs::fstat(&file_fd).into_get_err(&from)?;
            stat_to_meta(stat, from.clone())?;

            let _from_lock_fd = flock_dir(&from_dir_fd).into_other_err()?;
            let _to_lock_fd = if Arc::ptr_eq(&from_dir_fd, &to_dir_fd) {
                // don't lock the same directory twice! (deadlock)
                None
            } else {
                Some(flock_dir(&to_dir_fd).into_other_err()?)
            };

            match target_mode {
                RenameTargetMode::Overwrite => {
                    link_at_replace(&file_fd, &to_dir_fd, &to_file).into_other_err()?;
                }
                RenameTargetMode::Create => {
                    rustix::fs::linkat(&file_fd, "", &to_dir_fd, &to_file, AtFlags::EMPTY_PATH)
                        .into_put_err(to)?;
                }
            }
            rustix::fs::unlinkat(&from_dir_fd, from_file, AtFlags::empty()).into_get_err(&from)?;

            // fsync AFTER both operations complete and in-order
            rustix::fs::fsync(&to_dir_fd).into_other_err()?;
            rustix::fs::fsync(&from_dir_fd).into_other_err()?;

            Ok(())
        })
        .await
    }
}

#[derive(Debug)]
struct LinuxFSUpload {
    op: Option<Arc<RunningPutOperation>>,
    offset: usize,
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
}

#[async_trait]
impl MultipartUpload for LinuxFSUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let Some(op) = &self.op else {
            return Box::pin(futures_util::future::ready(Err(Self::err_not_running())));
        };
        let op = Arc::clone(op);

        let offset = self.offset;
        self.offset += data.content_length();

        Box::pin(spawn_io(move || {
            op.blocking_io_put(data, offset)?;
            Ok(())
        }))
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let op = self.op.take().ok_or_else(Self::err_not_running)?;

        let meta = spawn_io(move || {
            let op = Arc::try_unwrap(op).map_err(|_| Self::err_still_running())?;
            op.blocking_io_commit()
        })
        .await?;

        Ok(PutResult {
            e_tag: meta.e_tag,
            version: None,
            extensions: Default::default(),
        })
    }

    async fn abort(&mut self) -> Result<()> {
        let op = self.op.take().ok_or_else(Self::err_not_running)?;

        // drop in I/O runtime
        spawn_io(move || {
            let op = Arc::try_unwrap(op).map_err(|_| Self::err_still_running())?;
            drop(op);
            Ok(())
        })
        .await
    }
}
