use std::{num::NonZeroUsize, sync::Arc};

use crate::io::{DefaultIOLayer, IOLayer};

#[derive(Debug)]
pub struct LinuxFSOptions<IO = DefaultIOLayer>
where
    IO: IOLayer,
{
    /// IO-layer.
    pub io_layer: Arc<IO>,

    /// Read buffer size for GET requests, in bytes.
    pub read_buffer_size: NonZeroUsize,
}

impl Default for LinuxFSOptions<DefaultIOLayer> {
    fn default() -> Self {
        Self {
            io_layer: Arc::new(DefaultIOLayer::default()),
            read_buffer_size: NonZeroUsize::new(4096).unwrap(),
        }
    }
}

impl<IO> LinuxFSOptions<IO>
where
    IO: IOLayer,
{
    pub fn with_io<IO2>(self, io_layer: Arc<IO2>) -> LinuxFSOptions<IO2>
    where
        IO2: IOLayer,
    {
        LinuxFSOptions {
            io_layer,
            read_buffer_size: self.read_buffer_size,
        }
    }
}
