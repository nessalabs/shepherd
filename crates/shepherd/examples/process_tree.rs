//! Run with: cargo run -p shepherd --example process_tree -- <OS_PID>
use std::collections::BTreeMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pid: u32 = std::env::args()
        .nth(1)
        .ok_or("usage: process_tree <OS_PID>")?
        .parse()?;
    let tree = shepherd::process_observer().tree(pid).await?;
    let mut depths = BTreeMap::new();
    for process in tree.processes {
        let depth = if process.identity == tree.root {
            0
        } else {
            process
                .parent_os_pid
                .and_then(|p| depths.get(&p).copied())
                .unwrap_or(0)
                + 1
        };
        depths.insert(process.identity.os_pid, depth);
        println!(
            "{}{} (PID {}, parent {:?})",
            "  ".repeat(depth),
            process.name,
            process.identity.os_pid,
            process.parent_os_pid
        );
    }
    Ok(())
}
