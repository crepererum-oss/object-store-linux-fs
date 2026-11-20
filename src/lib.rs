use std::{io::IoSliceMut, ops::Range, path::PathBuf, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use futures_core::stream::BoxStream;
use futures_util::TryStreamExt;
use object_store::{
    CopyMode, CopyOptions, Error, GetOptions, GetRange, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, RenameOptions, RenameTargetMode, Result, path::Path,
};
use rustix::fs::Mode;

use crate::{
    attributes::read_attrs_from_fd,
    constants::{
        AT_FLAGS_LINKAT, AT_FLAGS_UNLINKAT, OFLAGS_READ, READ_WRITE_FLAGS, RESOLVE_FLAGS,
        STORE_NAME,
    },
    error::IOErrorExt,
    io::{DefaultIOLayer, IOBlock, IOLayer, spawn_io},
    put::PreparedPutOperation,
    put_multipart::LinuxFSUpload,
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
mod put_multipart;
mod state;
mod utils;

#[derive(Debug)]
pub struct LinuxFS<IO = DefaultIOLayer>
where
    IO: IOLayer,
{
    state: Arc<State<IO>>,
}

impl<IO> LinuxFS<IO>
where
    IO: IOLayer,
{
    pub async fn new(root: PathBuf, options: LinuxFSOptions<IO>) -> Result<Self> {
        Ok(Self {
            state: Arc::new(State::new(root, options).await?),
        })
    }
}

impl<IO> std::fmt::Display for LinuxFS<IO>
where
    IO: IOLayer,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{STORE_NAME}(\"{}\")", self.state.root().display())
    }
}

#[async_trait]
impl<IO> ObjectStore for LinuxFS<IO>
where
    IO: IOLayer,
{
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let state = Arc::clone(&self.state);
        let location = location.clone();
        let meta = spawn_io(&self.state.options().io_layer, move |io| {
            let op = PreparedPutOperation::new(io, location, opts, &state)?;
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
        let upload = LinuxFSUpload::new(Arc::clone(&self.state), location.clone(), opts).await?;
        Ok(Box::new(upload))
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

            let stat = io.fstat(&file_fd).into_get_err(&location)?;
            let meta = stat_to_meta(stat, location.clone(), OpenDirMode::Get)?;
            options.check_preconditions(&meta)?;

            let attributes = read_attrs_from_fd(io, &file_fd).into_get_err(&location)?;

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
                                let got = rustix::io::retry_on_intr(|| {
                                    io.preadv2(
                                        &file_fd,
                                        &mut [IoSliceMut::new(&mut buf[..to_read])],
                                        offset as _,
                                        READ_WRITE_FLAGS,
                                    )
                                })
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

            let stat = io.fstat(&file_fd).into_get_err(&location)?;
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

                    let got = rustix::io::retry_on_intr(|| {
                        io.preadv2(
                            &file_fd,
                            &mut [IoSliceMut::new(
                                &mut buffer[buffer_offset..(buffer_offset + to_read as usize)],
                            )],
                            file_offset,
                            READ_WRITE_FLAGS,
                        )
                    })
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
                    io.unlinkat(dir_fd.as_ref(), file, AT_FLAGS_UNLINKAT)
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

            let stat = io.fstat(&file_fd).into_get_err(&from)?;
            stat_to_meta(stat, from.clone(), OpenDirMode::Get)?;

            match mode {
                CopyMode::Overwrite => {
                    link_at_replace(io, &file_fd, to_dir_fd.as_ref(), to_file).into_put_err(&to)?;
                }
                CopyMode::Create => {
                    io.linkat(&file_fd, "", to_dir_fd.as_ref(), to_file, AT_FLAGS_LINKAT)
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

            let stat = io.fstat(&file_fd).into_get_err(from.as_ref())?;
            stat_to_meta(stat, from.clone(), OpenDirMode::Get)?;

            match target_mode {
                RenameTargetMode::Overwrite => {
                    link_at_replace(io, &file_fd, to_dir_fd.as_ref(), to_file).into_put_err(&to)?;
                }
                RenameTargetMode::Create => {
                    io.linkat(&file_fd, "", to_dir_fd.as_ref(), to_file, AT_FLAGS_LINKAT)
                        .into_put_err(&to)?;
                }
            }
            if to != from {
                io.unlinkat(from_dir_fd.as_ref(), from_file, AT_FLAGS_UNLINKAT)
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
