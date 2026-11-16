use std::num::NonZeroUsize;

#[derive(Debug)]
pub struct LinuxFSOptions {
    /// Read buffer size for GET requests, in bytes.
    pub read_buffer_size: NonZeroUsize,
}

impl Default for LinuxFSOptions {
    fn default() -> Self {
        Self {
            read_buffer_size: NonZeroUsize::new(4096).unwrap(),
        }
    }
}
