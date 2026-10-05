//! No model credentials or paid services. Needs an installed DDlog build driver.
#[path = "support/program.rs"]
mod program;
use anyhow::{Result, ensure};
use archetype_ddlog::{store::CutStore, world::World};
use serde_json::json;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    ensure!(
        args.len() == 3,
        "Usage: persistent_simulation NEW_DATA_DIRECTORY ABSOLUTE_DDLOG_DRIVER"
    );
    let root = PathBuf::from(&args[1]);
    ensure!(
        !root.exists(),
        "Choose a new data directory; existing evidence is retained"
    );
    std::fs::create_dir_all(&root)?;
    let driver = PathBuf::from(&args[2]);
    let (registry, manifest, components) = program::program(&root)?;
    let store = CutStore::open(&root.join("storage")).await?;
    let mut world = World::create(
        root.join("native"),
        driver,
        &registry,
        &manifest,
        components,
        "demo",
        "run_a",
    )?;
    world.stage("seed", vec![json!(1), json!("first entity")], false)?;
    let first = world.step(&store).await?;
    world.stage("seed", vec![json!(1), json!("first entity")], true)?;
    let second = world.step(&store).await?;
    let historical_rows: usize = store
        .read(&first, "label")
        .await?
        .iter()
        .map(|b| b.num_rows())
        .sum();
    let current_rows: usize = store
        .read(&second, "label")
        .await?
        .iter()
        .map(|b| b.num_rows())
        .sum();
    ensure!(
        historical_rows == 1 && current_rows == 0,
        "Retraction/history contract failed"
    );
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"engine":"DDlog Runtime","ddlog_revision":archetype_ddlog::DDLOG_REVISION,"historical_rows":historical_rows,"current_rows":current_rows,"first":first,"second":second})
        )?
    );
    Ok(())
}
