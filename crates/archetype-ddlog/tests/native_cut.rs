#[path = "../examples/support/program.rs"]
mod program;
use anyhow::{Result, anyhow};
use archetype_ddlog::{
    store::{CutStore, PublicationFault},
    world::World,
};
use arrow_array::StringArray;
use serde_json::json;
use std::path::PathBuf;

#[tokio::test]
#[ignore = "requires ARCHETYPE_DDLOG_DRIVER pointing at an installed native DDlog build driver"]
async fn native_fixed_point_publication_retraction_isolation_and_restore() -> Result<()> {
    let driver = PathBuf::from(std::env::var("ARCHETYPE_DDLOG_DRIVER")?);
    let dir = tempfile::tempdir()?;
    let (registry, manifest, components) = program::program(dir.path())?;
    let store = CutStore::open(&dir.path().join("storage")).await?;
    let mut world = World::create(
        dir.path().join("native_a"),
        driver.clone(),
        &registry,
        &manifest,
        components.clone(),
        "alpha",
        "run_a",
    )?;
    let mut other = World::create(
        dir.path().join("native_b"),
        driver.clone(),
        &registry,
        &manifest,
        components.clone(),
        "beta",
        "run_a",
    )?;
    world.stage("seed", vec![json!(1), json!("alpha")], false)?;
    other.stage("seed", vec![json!(1), json!("beta")], false)?;
    // Independent native owners complete concurrently, with no shared world lock.
    let (mut world, mut other) = std::thread::scope(|scope| {
        let a = scope.spawn(move || -> Result<_> {
            world.freeze()?;
            Ok(world)
        });
        let b = scope.spawn(move || -> Result<_> {
            other.freeze()?;
            Ok(other)
        });
        Ok::<_, anyhow::Error>((
            a.join().map_err(|_| anyhow!("world panicked"))??,
            b.join().map_err(|_| anyhow!("world panicked"))??,
        ))
    })?;
    let frozen_id = world.freeze()?.identity()?;
    assert!(
        store
            .publish_with_fault(world.freeze()?, PublicationFault::AfterComponents)
            .await
            .is_err()
    );
    assert_eq!(world.tick(), 0);
    assert_eq!(world.staged_len(), 1);
    assert!(
        world
            .stage("seed", vec![json!(2), json!("blocked")], false)
            .is_err()
    );
    assert_eq!(world.freeze()?.identity()?, frozen_id);
    let a = world.step(&store).await?;
    let b = other.step(&store).await?;
    assert_eq!(a.cut_id, frozen_id);
    assert_ne!(a.cut_id, b.cut_id);
    assert_eq!(world.staged_len(), 0);
    assert_eq!(store.read(&a, "status").await?[0].num_rows(), 1);
    let a_labels = store.read(&a, "label").await?;
    let b_labels = store.read(&b, "label").await?;
    let label = |batches: &[arrow_array::RecordBatch]| {
        batches[0]
            .column_by_name("label__name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .to_owned()
    };
    assert_eq!(label(&a_labels), "alpha");
    assert_eq!(label(&b_labels), "beta");
    drop(world);
    let mut resumed = World::create(
        dir.path().join("native_resume"),
        driver,
        &registry,
        &manifest,
        components,
        "alpha",
        "run_a",
    )?;
    resumed.restore(&store, &a).await?;
    // A restored nonempty input must still be retractable. Rebuilding from
    // analytical output rows alone cannot satisfy this native input identity.
    resumed.stage("seed", vec![json!(1), json!("alpha")], true)?;
    let retracted = resumed.step(&store).await?;
    assert_eq!(retracted.tick, 2);
    assert_eq!(store.read(&retracted, "label").await?[0].num_rows(), 0);
    assert_eq!(store.read(&a, "label").await?[0].num_rows(), 1);
    resumed.stage("seed", vec![json!(2), json!("resumed")], false)?;
    assert_eq!(resumed.step(&store).await?.tick, 3);
    // Backend validation rejects control strings without poisoning its health.
    // The adapter must never mistake that rejection for an acknowledged tick.
    resumed.stage("seed", vec![json!(3), json!("\u{0000}")], false)?;
    assert!(resumed.freeze().is_err());
    assert!(resumed.freeze().is_err());
    assert_eq!(resumed.tick(), 3);
    assert_eq!(resumed.staged_len(), 1);
    Ok(())
}
