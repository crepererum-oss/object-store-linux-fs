use std::{mem::MaybeUninit, ops::Range, path::PathBuf, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use futures_core::stream::BoxStream;
use futures_util::TryStreamExt;
use object_store::{
    CopyOptions, Error, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    RenameOptions, Result, path::Path,
};
use rustix::fs::{AtFlags, Mode, OFlags};

use crate::{
    constants::{READ_BUFFER_SIZE, STORE_NAME},
    state::State,
    utils::{mode_create, rflags, spawn_io, split_dirs_and_filename, stat_to_meta},
};

mod constants;
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

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let GetOptions {
            if_match,
            if_none_match,
            if_modified_since,
            if_unmodified_since,
            range,
            version,
            head,
            // ignore extensions
            extensions: _,
        } = options;
        if let Some(if_match) = if_match {
            return Err(Error::NotImplemented {
                operation: format!("get_opts(if_match={if_match})"),
                implementer: STORE_NAME.to_owned(),
            });
        }
        if let Some(if_none_match) = if_none_match {
            return Err(Error::NotImplemented {
                operation: format!("get_opts(if_none_match={if_none_match})"),
                implementer: STORE_NAME.to_owned(),
            });
        }
        if let Some(if_modified_since) = if_modified_since {
            return Err(Error::NotImplemented {
                operation: format!("get_opts(if_modified_since={if_modified_since})"),
                implementer: STORE_NAME.to_owned(),
            });
        }
        if let Some(if_unmodified_since) = if_unmodified_since {
            return Err(Error::NotImplemented {
                operation: format!("get_opts(if_unmodified_since={if_unmodified_since})"),
                implementer: STORE_NAME.to_owned(),
            });
        }
        if let Some(range) = range {
            return Err(Error::NotImplemented {
                operation: format!("get_opts(range={range})"),
                implementer: STORE_NAME.to_owned(),
            });
        }
        if let Some(version) = version {
            return Err(Error::NotImplemented {
                operation: format!("get_opts(version={version})"),
                implementer: STORE_NAME.to_owned(),
            });
        }

        let (dirs, file) = split_dirs_and_filename(location)?;
        let dir_fd = self.state.open_dir(dirs, false).await?;
        let file = file.to_owned();
        let location = location.clone();

        spawn_io(move || {
            let file_fd =
                rustix::fs::openat2(&dir_fd, file, OFlags::RDONLY, Mode::empty(), rflags())?;
            let stat = rustix::fs::fstat(&file_fd)?;
            let meta = stat_to_meta(stat, location)?;

            let range = 0..meta.size;

            let stream = if head {
                Box::pin(futures_util::stream::empty()) as BoxStream<'static, Result<Bytes>>
            } else {
                let buf = vec![MaybeUninit::<u8>::uninit(); READ_BUFFER_SIZE];
                Box::pin(futures_util::stream::try_unfold(
                    (file_fd, buf),
                    async |(file_fd, mut buf)| {
                        spawn_io(move || {
                            let (init, _uninit) = rustix::io::read(&file_fd, &mut buf)?;
                            let bytes = Bytes::from(init.to_owned());
                            Ok(Some((bytes, (file_fd, buf))))
                        })
                        .await
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
        crate::list::list(Arc::clone(&self.state), prefix.cloned())
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

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        crate::list::list_with_delimiter(Arc::clone(&self.state), prefix.cloned()).await
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
