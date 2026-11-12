use std::{
    collections::{HashMap, hash_map::Entry},
    io::ErrorKind,
    os::fd::OwnedFd,
    path::PathBuf,
    sync::Arc,
};

use object_store::Result;
use rustix::fs::Mode;
use tokio::sync::Mutex;

use crate::utils::{dir_oflags, mode_create, open_dir_fd, spawn_io};

#[derive(Debug)]
pub(crate) struct State {
    root: PathBuf,
    dir_fd_cache: Mutex<HashMap<PathBuf, Arc<OwnedFd>>>,
}

impl State {
    pub(crate) async fn new(root: PathBuf) -> Result<Self> {
        let (root, root_fd) = spawn_io(move || {
            std::fs::create_dir_all(&root)?;

            let root_fd = rustix::fs::open(&root, dir_oflags(), Mode::empty())?;

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
        create: bool,
    ) -> Result<Arc<OwnedFd>> {
        let mut dir_fd_cache = self.dir_fd_cache.lock().await;
        let mut path = self.root.clone();
        let mut dir_fd = Arc::clone(dir_fd_cache.get(&path).expect("root is always cached"));

        // TODO: reverse order for faster probing
        for part in dirs {
            path = path.join(part);
            dir_fd = match dir_fd_cache.entry(path.clone()) {
                Entry::Occupied(o) => Arc::clone(o.get()),
                Entry::Vacant(v) => {
                    let part = part.to_owned();
                    let sub_fd = spawn_io(move || {
                        if create {
                            // try to create dir
                            match rustix::fs::mkdirat(&dir_fd, &part, mode_create()) {
                                Ok(()) => {}
                                Err(e) if e.kind() == ErrorKind::AlreadyExists => {
                                    // race
                                }
                                Err(e) => {
                                    return Err(e.into());
                                }
                            }
                        }

                        open_dir_fd(&dir_fd, &part)
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
