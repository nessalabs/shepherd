//! Compile with `cargo check -p shepherd --example background_task_cleanup`.
//! Run with a trusted program and arguments, for example `-- sleep 30` on Unix.
use shepherd::{
    BackgroundTask, BackgroundTaskOptions, ProcessSpec, SupervisorBuilder, SystemClock,
};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args_os().skip(1);
    let Some(program) = args.next() else {
        return Err("supply a trusted program and its arguments".into());
    };
    let mut spec = ProcessSpec::new(program);
    for arg in args {
        spec = spec.arg(arg);
    }
    let supervisor = SupervisorBuilder::new().build();
    let task = BackgroundTask::start(
        supervisor.clone(),
        spec,
        BackgroundTaskOptions::default(),
        Arc::new(SystemClock),
    )?;
    // Request cancellation explicitly. Dropping a wait future does not do this.
    task.cancel();
    let result = task.wait().await;
    println!(
        "cause={:?}, cleanup_verified={}",
        result.cause,
        result.cleanup_verified()
    );
    // A settled task can retain uncertain cleanup; preserve the error to the caller.
    supervisor.shutdown().await?;
    if !result.cleanup_verified() {
        return Err("task cleanup was not verified".into());
    }
    Ok(())
}
