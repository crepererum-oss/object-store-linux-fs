use std::{
    collections::{HashMap, hash_map::Entry},
    io::ErrorKind,
    os::fd::OwnedFd,
    path::PathBuf,
    sync::Arc,
};

use object_store::{Result, path::Path};
use rustix::fs::Mode;
use tokio::sync::Mutex;

use crate::{
    error::IOErrorExt,
    utils::{dir_oflags, mode_create, open_dir_fd, spawn_io},
};

#[derive(Debug)]
pub(crate) struct State {
    root: PathBuf,
    dir_fd_cache: Mutex<HashMap<PathBuf, Arc<OwnedFd>>>,
}

impl State {
    pub(crate) async fn new(root: PathBuf) -> Result<Self> {
        let (root, root_fd) = spawn_io(move || {
            let root_fd = rustix::fs::open(&root, dir_oflags(), Mode::empty())
                .into_get_err(root.display())?;

            Ok((root, root_fd))
        })
        .await?;

        Ok(Self {
            root: root.clone(),
            dir_fd_cache: Mutex::new(HashMap::from([(root, Arc::new(root_fd))])),
        })
    }

    pub(crate) async fn open_dir(
        &self,
        dirs: impl Iterator<Item = &str>,
        mode: OpenDirMode,
    ) -> Result<Arc<OwnedFd>> {
        let mut dir_fd_cache = self.dir_fd_cache.lock().await;
        let mut path = self.root.clone();
        let mut prefix = Path::ROOT;
        let mut dir_fd = Arc::clone(dir_fd_cache.get(&path).expect("root is always cached"));

        // TODO: reverse order for faster probing
        for part in dirs {
            path = path.join(part);
            dir_fd = match dir_fd_cache.entry(path.clone()) {
                Entry::Occupied(o) => Arc::clone(o.get()),
                Entry::Vacant(v) => {
                    let part = part.to_owned();
                    let sub_fd;
                    (sub_fd, prefix) = spawn_io(move || {
                        if mode == OpenDirMode::Put {
                            // try to create dir
                            match rustix::fs::mkdirat(&dir_fd, &part, mode_create()) {
                                Ok(()) => {}
                                Err(e) if e.kind() == ErrorKind::AlreadyExists => {
                                    // race
                                }
                                Err(e) => {
                                    return Err(e.into_other_err());
                                }
                            }
                        }

                        let sub_fd_res = open_dir_fd(&dir_fd, &part);
                        let sub_fd = match mode {
                            OpenDirMode::Get => sub_fd_res.into_get_err(&prefix)?,
                            OpenDirMode::Put => sub_fd_res.into_other_err()?,
                        };
                        Ok((sub_fd, prefix))
                    })
                    .await?;
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OpenDirMode {
    Get,
    Put,
}
