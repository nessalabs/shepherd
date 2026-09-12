//! Byte-oriented output observation ports; never child ownership.
use std::sync::Arc;

/// Which OS pipe produced a chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputStream {
    Stdout,
    Stderr,
}
/// A byte chunk; decoding belongs to the caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputChunk {
    pub stream: OutputStream,
    pub bytes: Vec<u8>,
}
/// A nonblocking drain of the bounded consumer queue, plus current post-mortem tail.
#[derive(Debug, Clone)]
pub struct OutputSnapshot {
    pub chunks: Vec<OutputChunk>,
    pub tail: Vec<OutputChunk>,
    pub dropped_bytes: u64,
    pub stdout_closed: bool,
    pub stderr_closed: bool,
    pub errors: Vec<String>,
}
/// Application-owned port. Adapters must keep writes bounded and nonblocking.
pub trait OutputSink: Send + Sync {
    fn push(&self, stream: OutputStream, bytes: &[u8]);
    fn close(&self, stream: OutputStream, error: Option<String>);
    fn read(&self) -> OutputSnapshot;
}
/// Cloneable observation handle. Clones share one consuming queue and retained tail.
/// Retaining this handle never keeps a child or supervisor alive.
#[derive(Clone)]
pub struct ProcessOutput(pub Arc<dyn OutputSink>);
impl ProcessOutput {
    /// Takes queued chunks without waiting for more output.
    pub fn read(&self) -> OutputSnapshot {
        self.0.read()
    }
}
