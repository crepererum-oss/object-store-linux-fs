use std::{
    io::{IoSlice, IoSliceMut},
    ops::Range,
    path::PathBuf,
    sync::Arc,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures_core::stream::BoxStream;
use futures_util::TryStreamExt;
use object_store::{
    CopyMode, CopyOptions, Error, GetOptions, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, RenameOptions, RenameTargetMode, Result, path::Path,
};
use rustix::fs::{AtFlags, Mode, OFlags};

use crate::{
    constants::{READ_BUFFER_SIZE, STORE_NAME},
    error::IOErrorExt,
    state::{OpenDirMode, State},
    utils::{
        link_at_replace, mode_create, rflags, rwflags, spawn_io, split_dirs_and_filename,
        stat_to_meta,
    },
};

mod constants;
mod error;
mod list;
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

        let (dirs, file) = split_dirs_and_filename(location);
        let dir_fd = self.state.open_dir(dirs, OpenDirMode::Put).await?;
        let file = file.to_owned();
        let location = location.clone();

        let meta = spawn_io(move || {
            let file_fd = rustix::fs::openat2(
                &dir_fd,
                ".",
                OFlags::TMPFILE | OFlags::WRONLY,
                mode_create(),
                rflags(),
            )
            .into_other_err()?;

            let mut slices = payload
                .iter()
                .map(|bytes| IoSlice::new(bytes))
                .collect::<Vec<_>>();
            let len = slices.iter().map(|s| s.len()).sum::<usize>();
            let mut offset = 0;
            while offset < len {
                let written = rustix::io::pwritev2(&file_fd, &slices, offset as _, rwflags())
                    .into_other_err()?;
                offset += written;
                IoSlice::advance_slices(&mut &mut slices[..], written);
            }
            rustix::fs::fsync(&file_fd).into_other_err()?;

            link_at_replace(&file_fd, &dir_fd, &file).into_other_err()?;
            rustix::fs::fsync(&dir_fd).into_other_err()?;

            let stat = rustix::fs::fstat(&file_fd).into_get_err(&location)?;
            let meta = stat_to_meta(stat, location)?;

            drop(file_fd);

            Ok(meta)
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
        _location: &Path,
        _opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        todo!()
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        if options.version.is_some() {
            return Err(Error::Precondition {
                path: location.to_string(),
                source: "versions are not used".to_owned().into(),
            });
        }

        let (dirs, file) = split_dirs_and_filename(location);
        let dir_fd = self.state.open_dir(dirs, OpenDirMode::Get).await?;
        let file = file.to_owned();
        let location = location.clone();

        spawn_io(move || {
            let file_fd =
                rustix::fs::openat2(&dir_fd, file, OFlags::RDONLY, Mode::empty(), rflags())
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
        let (dirs, file) = split_dirs_and_filename(location);
        let dir_fd = self.state.open_dir(dirs, OpenDirMode::Get).await?;
        let file = file.to_owned();
        let location = location.clone();
        let ranges = ranges.to_vec();

        spawn_io(move || {
            let file_fd =
                rustix::fs::openat2(&dir_fd, file, OFlags::RDONLY, Mode::empty(), rflags())
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
                let (dirs, file) = split_dirs_and_filename(&location);
                let dir_fd = state.open_dir(dirs, OpenDirMode::Get).await?;
                let file = file.to_owned();
                let location = spawn_io(move || {
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
        if mode != CopyMode::Overwrite {
            return Err(Error::NotImplemented {
                operation: format!("copy_opts(mode={mode:?})"),
                implementer: STORE_NAME.to_owned(),
            });
        }

        let (from_dirs, from_file) = split_dirs_and_filename(from);
        let from_dir_fd = self.state.open_dir(from_dirs, OpenDirMode::Get).await?;

        let (to_dirs, to_file) = split_dirs_and_filename(to);
        let to_dir_fd = self.state.open_dir(to_dirs, OpenDirMode::Put).await?;

        let from_file = from_file.to_owned();
        let to_file = to_file.to_owned();
        let from = from.clone();

        spawn_io(move || {
            let file_fd = rustix::fs::openat2(
                &from_dir_fd,
                from_file,
                OFlags::RDONLY,
                Mode::empty(),
                rflags(),
            )
            .into_get_err(&from)?;

            let stat = rustix::fs::fstat(&file_fd).into_get_err(&from)?;
            stat_to_meta(stat, from.clone())?;

            link_at_replace(&file_fd, &to_dir_fd, &to_file).into_other_err()?;
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
        if target_mode != RenameTargetMode::Overwrite {
            return Err(Error::NotImplemented {
                operation: format!("rename_opts(target_mode={target_mode:?})"),
                implementer: STORE_NAME.to_owned(),
            });
        }

        let (from_dirs, from_file) = split_dirs_and_filename(from);
        let from_dir_fd = self.state.open_dir(from_dirs, OpenDirMode::Get).await?;

        let (to_dirs, to_file) = split_dirs_and_filename(to);
        let to_dir_fd = self.state.open_dir(to_dirs, OpenDirMode::Put).await?;

        let from_file = from_file.to_owned();
        let to_file = to_file.to_owned();
        let from = from.clone();

        spawn_io(move || {
            let file_fd = rustix::fs::openat2(
                &from_dir_fd,
                &from_file,
                OFlags::RDONLY,
                Mode::empty(),
                rflags(),
            )
            .into_get_err(&from)?;

            let stat = rustix::fs::fstat(&file_fd).into_get_err(&from)?;
            stat_to_meta(stat, from.clone())?;

            link_at_replace(&file_fd, &to_dir_fd, &to_file).into_other_err()?;
            rustix::fs::unlinkat(&from_dir_fd, from_file, AtFlags::empty()).into_get_err(&from)?;

            // fsync AFTER both operations complete and in-order
            rustix::fs::fsync(&to_dir_fd).into_other_err()?;
            rustix::fs::fsync(&from_dir_fd).into_other_err()?;

            Ok(())
        })
        .await
    }
}
