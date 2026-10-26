//! Kept-process adapters (ADR 0023): a directory record store and the Unix launcher.

mod store;
pub use store::DirectoryKeptStore;

mod unix;
pub use unix::UnixKeptLauncher;
