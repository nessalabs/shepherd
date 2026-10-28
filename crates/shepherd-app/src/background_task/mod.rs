//! One command whose scope outlives its caller's wait.
//!
//! `BackgroundTask::start -> observed scope -> observed spawn -> cleanup -> result`.
//! The arrows transfer ownership. Handles only observe and request cancellation;
//! the worker owns cleanup. Session association and persistence belong to callers.

mod task;

pub use task::{
    BackgroundTask, BackgroundTaskCause, BackgroundTaskOptions, BackgroundTaskOutput,
    BackgroundTaskResult, BackgroundTaskStatus,
};
