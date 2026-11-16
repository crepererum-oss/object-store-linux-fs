use std::{io::IoSlice, os::fd::OwnedFd, sync::Arc};

use object_store::{
    Error, ObjectMeta, PutMode, PutOptions, PutPayload, Result, UpdateVersion, path::Path,
};
use rustix::fs::{AtFlags, Mode};

use crate::{
    constants::{
        MODE_FILE_CREATE, OFLAGS_FILE_CREATE, OFLAGS_READ, READ_WRITE_FLAGS, RESOLVE_FLAGS,
        STORE_NAME,
    },
    error::IOErrorExt,
    state::State,
    utils::{OpenDirMode, flock_dir, link_at_replace, split_dirs_and_filename, stat_to_meta},
};

pub(crate) struct PreparedPutOperation {
    location: Path,
    file: String,
    dir_fd: Arc<OwnedFd>,
    mode: PutMode,
}

impl PreparedPutOperation {
    pub(crate) async fn new(location: &Path, opts: PutOptions, state: &State) -> Result<Self> {
        let PutOptions {
            mode,
            // ignore tags
            tags: _,
            attributes,
            // ignore extensions
            extensions: _,
        } = opts;
        if !attributes.is_empty() {
            return Err(Error::NotImplemented {
                operation: "put_opts(attributes=<NOT_EMPTY>)".to_owned(),
                implementer: STORE_NAME.to_owned(),
            });
        }

        let open_dir_mode = match &mode {
            PutMode::Overwrite | PutMode::Create => OpenDirMode::Put,
            PutMode::Update(_) => OpenDirMode::Update,
        };
        let (dirs, file) = split_dirs_and_filename(location, open_dir_mode)?;
        let dir_fd = state.open_dir(dirs, OpenDirMode::Put).await?;

        Ok(Self {
            location: location.clone(),
            file: file.to_owned(),
            dir_fd,
            mode,
        })
    }

    pub(crate) fn blocking_io_start(self) -> Result<RunningPutOperation> {
        let Self {
            location,
            file,
            dir_fd,
            mode,
        } = self;

        let file_fd = rustix::fs::openat2(
            &dir_fd,
            ".",
            OFLAGS_FILE_CREATE,
            MODE_FILE_CREATE,
            RESOLVE_FLAGS,
        )
        .into_other_err()?;

        Ok(RunningPutOperation {
            file_fd,
            dir_fd,
            file,
            location,
            mode,
        })
    }
}

#[derive(Debug)]
pub(crate) struct RunningPutOperation {
    file_fd: OwnedFd,
    dir_fd: Arc<OwnedFd>,
    file: String,
    location: Path,
    mode: PutMode,
}

impl RunningPutOperation {
    pub(crate) fn blocking_io_put(&self, payload: PutPayload, start_offset: usize) -> Result<()> {
        let Self { file_fd, .. } = self;

        let mut slices = payload
            .iter()
            .map(|bytes| IoSlice::new(bytes))
            .collect::<Vec<_>>();
        let len = slices.iter().map(|s| s.len()).sum::<usize>();
        let mut offset = start_offset;
        while offset < start_offset + len {
            let written = rustix::io::pwritev2(file_fd, &slices, offset as _, READ_WRITE_FLAGS)
                .into_other_err()?;
            offset += written;
            IoSlice::advance_slices(&mut &mut slices[..], written);
        }

        Ok(())
    }

    pub(crate) fn blocking_io_commit(self) -> Result<ObjectMeta> {
        let Self {
            file_fd,
            dir_fd,
            file,
            location,
            mode,
        } = self;

        rustix::fs::fsync(&file_fd).into_other_err()?;

        let lock_fd = flock_dir(&dir_fd).into_other_err()?;

        match mode {
            PutMode::Overwrite => {
                link_at_replace(&file_fd, &dir_fd, &file).into_other_err()?;
            }
            PutMode::Create => {
                rustix::fs::linkat(&file_fd, "", &dir_fd, &file, AtFlags::EMPTY_PATH)
                    .into_put_err(&location)?;
            }
            PutMode::Update(UpdateVersion { e_tag, version }) => {
                let Some(e_tag) = e_tag else {
                    return Err(Error::Precondition {
                        path: location.to_string(),
                        source: "need etag".to_owned().into(),
                    });
                };
                if version.is_some() {
                    return Err(Error::Precondition {
                        path: location.to_string(),
                        source: "versions are not used".to_owned().into(),
                    });
                }

                let existing_fd =
                    rustix::fs::openat2(&dir_fd, &file, OFLAGS_READ, Mode::empty(), RESOLVE_FLAGS)
                        .into_update_err(&location)?;

                let existing_stat = rustix::fs::fstat(&existing_fd).into_get_err(&location)?;
                let existing_meta = stat_to_meta(existing_stat, location.clone())?;
                let existing_etag = existing_meta
                    .e_tag
                    .expect("fs implementation always provides etag");
                if existing_etag != e_tag {
                    return Err(Error::Precondition {
                        path: location.to_string(),
                        source: format!("requested etag='{}', got etag={}", e_tag, existing_etag,)
                            .into(),
                    });
                }

                link_at_replace(&file_fd, &dir_fd, &file).into_other_err()?;
            }
        }
        rustix::fs::fsync(&dir_fd).into_other_err()?;

        let stat = rustix::fs::fstat(&file_fd).into_get_err(&location)?;
        let meta = stat_to_meta(stat, location)?;

        drop(file_fd);
        drop(lock_fd);

        Ok(meta)
    }
}
