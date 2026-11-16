use std::{num::NonZeroUsize, sync::Arc};

use crate::io::{IOLayer, RustixIOLayer};

#[derive(Debug)]
pub struct LinuxFSOptions {
    /// IO-layer.
    pub io_layer: Arc<dyn IOLayer>,

    /// Read buffer size for GET requests, in bytes.
    pub read_buffer_size: NonZeroUsize,
}

impl Default for LinuxFSOptions {
    fn default() -> Self {
        Self {
            io_layer: Arc::new(RustixIOLayer),
            read_buffer_size: NonZeroUsize::new(4096).unwrap(),
        }
    }
}
