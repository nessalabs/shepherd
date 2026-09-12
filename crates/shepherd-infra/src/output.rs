//! A bounded combined stdout/stderr byte queue with independent capped tail.
use shepherd_app::output::{OutputChunk, OutputSink, OutputSnapshot, OutputStream, ProcessOutput};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncRead, AsyncReadExt};

struct State {
    queue: VecDeque<OutputChunk>,
    tail: VecDeque<OutputChunk>,
    bytes: usize,
    tail_bytes: usize,
    dropped: u64,
    closed: [bool; 2],
    errors: Vec<String>,
}
struct Buffer {
    capacity: usize,
    tail_capacity: usize,
    state: Mutex<State>,
}
fn index(stream: OutputStream) -> usize {
    match stream {
        OutputStream::Stdout => 0,
        OutputStream::Stderr => 1,
    }
}
fn append(
    queue: &mut VecDeque<OutputChunk>,
    size: &mut usize,
    capacity: usize,
    stream: OutputStream,
    bytes: &[u8],
) -> u64 {
    let mut dropped = 0;
    let retain = bytes.len().min(capacity);
    let need = size.saturating_add(retain).saturating_sub(capacity);
    let mut remove = need;
    while remove > 0 {
        let first = queue.front_mut().expect("queue byte accounting");
        let count = remove.min(first.bytes.len());
        first.bytes.drain(..count);
        remove -= count;
        *size -= count;
        dropped += count as u64;
        if first.bytes.is_empty() {
            queue.pop_front();
        }
    }
    dropped += (bytes.len() - retain) as u64;
    if retain > 0 {
        queue.push_back(OutputChunk {
            stream,
            bytes: bytes[bytes.len() - retain..].to_vec(),
        });
        *size += retain;
    }
    dropped
}
impl OutputSink for Buffer {
    fn push(&self, stream: OutputStream, bytes: &[u8]) {
        let mut state = self.state.lock().expect("output mutex");
        let State {
            queue,
            tail,
            bytes: size,
            tail_bytes,
            dropped,
            ..
        } = &mut *state;
        *dropped = dropped.saturating_add(append(queue, size, self.capacity, stream, bytes));
        append(tail, tail_bytes, self.tail_capacity, stream, bytes);
    }
    fn close(&self, stream: OutputStream, error: Option<String>) {
        let mut state = self.state.lock().expect("output mutex");
        if state.closed[index(stream)] {
            return;
        }
        state.closed[index(stream)] = true;
        if let Some(error) = error {
            state.errors.push(error);
        }
    }
    fn read(&self) -> OutputSnapshot {
        let mut state = self.state.lock().expect("output mutex");
        state.bytes = 0;
        OutputSnapshot {
            chunks: state.queue.drain(..).collect(),
            tail: state.tail.iter().cloned().collect(),
            dropped_bytes: state.dropped,
            stdout_closed: state.closed[0],
            stderr_closed: state.closed[1],
            errors: state.errors.clone(),
        }
    }
}
pub(crate) fn capture(capacity: usize, tail_capacity: usize) -> ProcessOutput {
    ProcessOutput(Arc::new(Buffer {
        capacity,
        tail_capacity,
        state: Mutex::new(State {
            queue: VecDeque::new(),
            tail: VecDeque::new(),
            bytes: 0,
            tail_bytes: 0,
            dropped: 0,
            closed: [false; 2],
            errors: Vec::new(),
        }),
    }))
}
pub(crate) async fn drain<R: AsyncRead + Unpin>(
    mut reader: R,
    output: ProcessOutput,
    stream: OutputStream,
) {
    let mut chunk = [0; 8192];
    loop {
        match reader.read(&mut chunk).await {
            Ok(0) => {
                output.0.close(stream, None);
                return;
            }
            Ok(n) => output.0.push(stream, &chunk[..n]),
            Err(error) => {
                output.0.close(stream, Some(error.to_string()));
                return;
            }
        }
    }
}
/// A descendant can inherit a pipe and outlive the root (especially on macOS).
/// Bound that drain after root reap; never await inherited pipes indefinitely.
pub(crate) async fn finish_readers(
    readers: Vec<(OutputStream, tokio::task::JoinHandle<()>)>,
    output: ProcessOutput,
) {
    for (stream, mut reader) in readers {
        match tokio::time::timeout(std::time::Duration::from_millis(100), &mut reader).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => output
                .0
                .close(stream, Some(format!("output reader failed: {error}"))),
            Err(_) => {
                reader.abort();
                let _ = reader.await;
                output.0.close(
                    stream,
                    Some("pipe remained open after root reap; reader stopped".into()),
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn overflow_drops_oldest_exact_bytes_and_preserves_binary_tail() {
        let out = capture(4, 3);
        out.0.push(OutputStream::Stdout, &[0, 255, 1]);
        out.0.push(OutputStream::Stderr, &[2, 3, 4]);
        let s = out.read();
        assert_eq!(s.dropped_bytes, 2);
        assert_eq!(
            s.chunks
                .iter()
                .flat_map(|c| c.bytes.clone())
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );
        assert_eq!(
            s.tail
                .iter()
                .flat_map(|c| c.bytes.clone())
                .collect::<Vec<_>>(),
            vec![2, 3, 4]
        );
        assert!(out.read().chunks.is_empty());
        assert_eq!(out.read().tail, s.tail);
    }
    #[test]
    fn oversized_chunk_and_zero_capacity_remain_bounded() {
        let out = capture(0, 2);
        out.0.push(OutputStream::Stdout, &[1, 2, 3, 4]);
        let s = out.read();
        assert!(s.chunks.is_empty());
        assert_eq!(s.dropped_bytes, 4);
        assert_eq!(s.tail[0].bytes, [3, 4]);
    }
    struct FailedReader;
    impl AsyncRead for FailedReader {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
            _: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Err(std::io::Error::other("injected reader failure")))
        }
    }
    #[tokio::test]
    async fn reader_failure_is_observable() {
        let out = capture(8, 8);
        drain(FailedReader, out.clone(), OutputStream::Stdout).await;
        let s = out.read();
        assert!(s.stdout_closed);
        assert_eq!(s.errors, ["injected reader failure"]);
    }
}
