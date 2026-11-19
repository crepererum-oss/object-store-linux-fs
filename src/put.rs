use std::{io::IoSlice, sync::Arc};

use object_store::{
    Attributes, Error, ObjectMeta, PutMode, PutOptions, PutPayload, Result, UpdateVersion,
    path::Path,
};
use rustix::fs::{Mode, XattrFlags};

use crate::{
    attributes::attr_to_xattr,
    constants::{
        AT_FLAGS_LINKAT, MODE_FILE_CREATE, OFLAGS_FILE_CREATE, OFLAGS_READ, READ_WRITE_FLAGS,
        RESOLVE_FLAGS,
    },
    error::IOErrorExt,
    io::{IOBlock, IOFD, spawn_io},
    state::State,
    utils::{OpenDirMode, flock_dir, link_at_replace, split_dirs_and_filename, stat_to_meta},
};

pub(crate) struct PreparedPutOperation {
    location: Path,
    file: String,
    dir_fd: Arc<dyn IOFD>,
    mode: PutMode,
    open_dir_mode: OpenDirMode,
    attributes: Attributes,
}

impl PreparedPutOperation {
    pub(crate) async fn new(location: &Path, opts: PutOptions, state: &Arc<State>) -> Result<Self> {
        let PutOptions {
            mode,
            // ignore tags
            tags: _,
            attributes,
            // ignore extensions
            extensions: _,
        } = opts;

        let open_dir_mode = match &mode {
            PutMode::Overwrite | PutMode::Create => OpenDirMode::Put,
            PutMode::Update(_) => OpenDirMode::Update,
        };

        let location = location.clone();
        let state2 = Arc::clone(state);
        let (location, dir_fd, file) = spawn_io(&state.options().io_layer, move |io| {
            let (dirs, file) = split_dirs_and_filename(&location, open_dir_mode)?;
            let file = file.to_owned();
            let dir_fd = state2.open_dir(io, dirs, open_dir_mode)?;
            Ok((location, dir_fd, file))
        })
        .await?;

        Ok(Self {
            location,
            file,
            dir_fd,
            mode,
            open_dir_mode,
            attributes,
        })
    }

    pub(crate) fn blocking_io_start(self, io: &mut dyn IOBlock) -> Result<RunningPutOperation> {
        let Self {
            location,
            file,
            dir_fd,
            mode,
            open_dir_mode,
            attributes,
        } = self;

        let file_fd = io
            .openat2(
                dir_fd.as_ref(),
                ".",
                OFLAGS_FILE_CREATE,
                MODE_FILE_CREATE,
                RESOLVE_FLAGS,
            )
            .into_other_err()?;

        // set attributes in deterministic order
        let mut attrs = attributes
            .iter()
            .map(|(attr, att_val)| attr_to_xattr(attr).map(|attr| (attr, att_val)))
            .collect::<Result<Vec<_>, _>>()?;
        attrs.sort_unstable_by(|(attr1, _attr_val1), (attr2, _attr_val2)| attr1.cmp(attr2));
        for (attr, attr_val) in attrs {
            io.fsetxattr(
                file_fd.as_ref(),
                &attr,
                attr_val.as_bytes(),
                XattrFlags::CREATE,
            )
            .into_other_err()?;
        }

        Ok(RunningPutOperation {
            file_fd,
            dir_fd,
            file,
            location,
            mode,
            open_dir_mode,
        })
    }
}

#[derive(Debug)]
pub(crate) struct RunningPutOperation {
    file_fd: Box<dyn IOFD>,
    dir_fd: Arc<dyn IOFD>,
    file: String,
    location: Path,
    mode: PutMode,
    open_dir_mode: OpenDirMode,
}

impl RunningPutOperation {
    pub(crate) fn blocking_io_put(
        &self,
        io: &mut dyn IOBlock,
        payload: PutPayload,
        start_offset: usize,
    ) -> Result<()> {
        let Self { file_fd, .. } = self;

        let mut slices = payload
            .iter()
            .map(|bytes| IoSlice::new(bytes))
            .collect::<Vec<_>>();
        let mut slices = slices.as_mut_slice();

        let len = slices.iter().map(|s| s.len()).sum::<usize>();
        let mut offset = start_offset;
        while offset < start_offset + len {
            let written = io
                .pwritev2(file_fd.as_ref(), slices, offset as _, READ_WRITE_FLAGS)
                .into_other_err()?;
            offset += written;
            IoSlice::advance_slices(&mut slices, written);
        }

        Ok(())
    }

    pub(crate) fn blocking_io_commit(self, io: &mut dyn IOBlock) -> Result<ObjectMeta> {
        let Self {
            file_fd,
            dir_fd,
            file,
            location,
            mode,
            open_dir_mode,
        } = self;

        io.fsync(file_fd.as_ref()).into_other_err()?;

        let lock_fd = flock_dir(io, dir_fd.as_ref()).into_other_err()?;

        match mode {
            PutMode::Overwrite => {
                link_at_replace(io, file_fd.as_ref(), dir_fd.as_ref(), &file).into_other_err()?;
            }
            PutMode::Create => {
                io.linkat(
                    file_fd.as_ref(),
                    "",
                    dir_fd.as_ref(),
                    &file,
                    AT_FLAGS_LINKAT,
                )
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

                let existing_fd = io
                    .openat2(
                        dir_fd.as_ref(),
                        &file,
                        OFLAGS_READ,
                        Mode::empty(),
                        RESOLVE_FLAGS,
                    )
                    .into_update_err(&location)?;

                let existing_stat = io.fstat(existing_fd.as_ref()).into_get_err(&location)?;
                let existing_meta = stat_to_meta(existing_stat, location.clone(), open_dir_mode)?;
                let existing_etag = existing_meta.e_tag;
                if existing_etag.as_ref() != Some(&e_tag) {
                    return Err(Error::Precondition {
                        path: location.to_string(),
                        source: format!(
                            "requested etag='{}', got etag={}",
                            e_tag,
                            std::fmt::from_fn(|f| {
                                match &existing_etag {
                                    Some(etag) => write!(f, "'{etag}'"),
                                    None => write!(f, "<missing>"),
                                }
                            }),
                        )
                        .into(),
                    });
                }

                link_at_replace(io, file_fd.as_ref(), dir_fd.as_ref(), &file).into_other_err()?;
            }
        }
        io.fsync(dir_fd.as_ref()).into_other_err()?;

        let stat = io.fstat(file_fd.as_ref()).into_get_err(&location)?;
        let meta = stat_to_meta(stat, location, open_dir_mode)?;

        drop(file_fd);
        drop(lock_fd);

        Ok(meta)
    }
}
