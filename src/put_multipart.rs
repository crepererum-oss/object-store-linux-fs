use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};

use async_trait::async_trait;
use object_store::{
    Error, MultipartUpload, PutOptions, PutPayload, PutResult, Result, UploadPart, path::Path,
};
use parking_lot::Mutex;

use crate::{
    constants::STORE_NAME,
    io::{IOLayer, IORwLock, spawn_io},
    put::{PreparedPutOperation, RunningPutOperation},
    state::State,
};

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
                    return Err(err_still_running());
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
pub(crate) struct LinuxFSUpload<IO>
where
    IO: IOLayer,
{
    state: Arc<IORwLock<UploadState>>,

    // NOTE: this does NOT count as an I/O lock because we don't do I/O during it
    parts: Arc<Mutex<UploadParts>>,

    io_layer: Arc<IO>,
}

impl<IO> LinuxFSUpload<IO>
where
    IO: IOLayer,
{
    pub(crate) async fn new(
        state: Arc<State<IO>>,
        location: Path,
        opts: PutOptions,
    ) -> Result<Self> {
        let state2 = Arc::clone(&state);
        let op = spawn_io(&state.options().io_layer, move |io| {
            let op = PreparedPutOperation::new(io, location, opts, &state2)?;
            op.blocking_io_start(io)
        })
        .await?;

        Ok(Self {
            state: Arc::new(IORwLock::new(
                &state.options().io_layer,
                UploadState::Running { op },
            )),
            parts: Arc::new(Mutex::new(UploadParts {
                parts: Default::default(),
                offset: 0,
            })),
            io_layer: Arc::clone(&state.options().io_layer),
        })
    }
}

#[async_trait]
impl<IO> MultipartUpload for LinuxFSUpload<IO>
where
    IO: IOLayer,
{
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let state = Arc::clone(&self.state);
        let parts = Arc::clone(&self.parts);

        let io_layer = Arc::clone(&self.io_layer);

        // short-lived lock to set the part state
        let (offset, idx) = {
            let mut parts = parts.lock();

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
                        return Err(err_not_running());
                    }
                };

                // perform IO
                let res = op.blocking_io_put(io, data, offset);

                // short-lived lock to set the part state
                {
                    let mut parts = parts.lock();
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
            let parts = self.parts.lock();
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
                        Err(err_failed())
                    } else {
                        op.blocking_io_commit(io)
                    }
                }
                UploadState::Finished { .. } => Err(err_not_running()),
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
            let parts = self.parts.lock();
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
                        Err(err_not_running())
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
