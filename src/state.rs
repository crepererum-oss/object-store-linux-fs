use std::{
    collections::{HashMap, hash_map::Entry},
    path::PathBuf,
    sync::Arc,
};

use object_store::{Result, path::Path};
use rustix::fs::Mode;

use crate::{
    LinuxFSOptions,
    constants::{MODE_DIR_CREATE, OFLAGS_DIR},
    error::IOErrorExt,
    io::{IOBlock, IOLayer, IOMutex, spawn_io},
    utils::{OpenDirMode, open_dir_fd},
};

#[derive(Debug)]
pub(crate) struct State<IO>
where
    IO: IOLayer,
{
    root: PathBuf,
    dir_fd_cache: IOMutex<HashMap<PathBuf, Arc<IO::FD>>>,
    options: LinuxFSOptions<IO>,
}

impl<IO> State<IO>
where
    IO: IOLayer,
{
    pub(crate) async fn new(root: PathBuf, options: LinuxFSOptions<IO>) -> Result<Self> {
        let (root, root_fd) = spawn_io(&options.io_layer, move |io| {
            let root_fd = io
                .open(&root, OFLAGS_DIR, Mode::empty())
                .into_get_err(root.display())?;

            Ok((root, root_fd))
        })
        .await?;

        Ok(Self {
            root: root.clone(),
            dir_fd_cache: IOMutex::new(
                &options.io_layer,
                HashMap::from([(root, Arc::from(root_fd))]),
            ),
            options,
        })
    }

    pub(crate) fn open_dir<'a>(
        &self,
        io: &mut IO::Block,
        dirs: impl Iterator<Item = Result<&'a str>>,
        mode: OpenDirMode,
    ) -> Result<Arc<IO::FD>> {
        let mut dir_fd_cache = self.dir_fd_cache.lock(io);
        let mut path = self.root.clone();
        let mut prefix = Path::ROOT;
        let mut dir_fd = Arc::clone(dir_fd_cache.get(&path).expect("root is always cached"));

        // TODO: reverse order for faster probing
        for part in dirs {
            let part = part?;
            path = path.join(part);
            prefix = prefix.join(part);
            dir_fd = match dir_fd_cache.entry(path.clone()) {
                Entry::Occupied(o) => Arc::clone(o.get()),
                Entry::Vacant(v) => {
                    if mode == OpenDirMode::Put {
                        // try to create dir
                        match io.mkdirat(dir_fd.as_ref(), part, MODE_DIR_CREATE) {
                            Ok(()) => {}
                            Err(rustix::io::Errno::EXIST) => {
                                // race
                            }
                            Err(e) => {
                                return Err(e.into_other_err());
                            }
                        }
                    }

                    let sub_fd_res = open_dir_fd(io, dir_fd.as_ref(), part);
                    let sub_fd = match mode {
                        OpenDirMode::Get => sub_fd_res.into_get_err(&prefix)?,
                        OpenDirMode::Put => sub_fd_res.into_other_err()?,
                        OpenDirMode::Update => sub_fd_res.into_update_err(&prefix)?,
                    };
                    v.insert(Arc::clone(&sub_fd));
                    sub_fd
                }
            };
        }

        Ok(dir_fd)
    }

    pub(crate) fn root(&self) -> &std::path::Path {
        &self.root
    }

    pub(crate) fn options(&self) -> &LinuxFSOptions<IO> {
        &self.options
    }
}
