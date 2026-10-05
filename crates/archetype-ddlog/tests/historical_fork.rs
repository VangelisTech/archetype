#![cfg(unix)]
//! Simulated native transport with real Iceberg; the ignored entry uses DDlog.
#[path = "support/hosted.rs"]
mod support;
use anyhow::{Result, anyhow};
use archetype_ddlog::{
    hosted::HostedCutAdapter,
    store::{CutReceipt, CutStore, PublicationFault, origin::Scope},
};
use support::*;

async fn commit(
    a: &HostedCutAdapter,
    store: &CutStore,
    m: &mut ddlog_runtime::worlds::WorldManager,
    id: &str,
    parent: Option<&CutReceipt>,
    change: (&str, &str, bool),
) -> Result<CutReceipt> {
    let (key_name, value, delete) = change;
    let status = m.status(id).map_err(|e| anyhow!(e))?;
    let ticket = a
        .prepare_admission(
            store,
            parent.map(|r| r.cut_id.as_str()),
            request(
                id,
                status["generation"].as_u64().unwrap(),
                status["revision"].as_u64().unwrap(),
                key_name,
                value,
                delete,
            ),
        )
        .await?;
    let k = key(&ticket.submit(m)?)?;
    let cut = capture(a, m, &k).await?;
    let p = a.publish(store, &cut).await?;
    p.confirm(m)?;
    Ok(p.receipt().clone())
}
async fn contract(native: bool) -> Result<()> {
    let f = Fixture::new(native)?;
    let storage = f.root.path().join("storage");
    let store = CutStore::open(&storage).await?;
    let mut m = f.manager()?;
    let (id, a) = f.create(&mut m, "parent")?;
    m.start_async(&id).map_err(|e| anyhow!(e))?;
    running(&mut m, &id).await?;
    let first = commit(&a, &store, &mut m, &id, None, ("one", "alpha", false)).await?;
    let source_checkpoint = store.checkpoint(&first).await?;
    let dest = Scope {
        world: "child".into(),
        run: "run_a".into(),
    };
    let prepared = a
        .prepare_fork(
            &store,
            &first,
            "fork_one".into(),
            dest.clone(),
            "Child".into(),
        )
        .await?;
    // Parent advancement occurs after exact preparation and before reservation.
    let empty = commit(
        &a,
        &store,
        &mut m,
        &id,
        Some(&first),
        ("two", "alpha", true),
    )
    .await?;
    let latest = commit(
        &a,
        &store,
        &mut m,
        &id,
        Some(&empty),
        ("three", "beta", false),
    )
    .await?;
    assert!(a.prepare_restore(&store, &first).await.is_err());
    let mut forged = first.clone();
    forged.components.clear();
    assert!(
        a.prepare_fork(&store, &forged, "bad".into(), dest.clone(), "Child".into())
            .await
            .is_err()
    );
    let reserved = prepared.reserve(&mut m)?;
    let child = reserved.reservation().child_world_id.clone();
    assert_ne!(child, id);
    assert!(m.start_async(&child).is_err());
    reserved.publish_origin(&store).await?;
    reserved.publish_origin(&store).await?; // lost origin acknowledgment
    let child_a = reserved.adapter().clone();
    assert_eq!(store.history("child", "run_a").await?, vec![first.clone()]);
    assert_eq!(store.read(&first, "label").await?[0].num_rows(), 1);
    assert_eq!(store.checkpoint(&first).await?, source_checkpoint);
    // An origin-only child can itself be the selected source. Original receipts
    // remain owned by parent, while requested lineage is child.
    let nested = child_a
        .prepare_fork(
            &store,
            &first,
            "fork_nested".into(),
            Scope {
                world: "nested".into(),
                run: "run_a".into(),
            },
            "Nested".into(),
        )
        .await?
        .reserve(&mut m)?;
    nested.publish_origin(&store).await?;
    assert_eq!(store.history("nested", "run_a").await?, vec![first.clone()]);
    assert!(
        a.prepare_fork(
            &store,
            &empty,
            "fork_one".into(),
            dest.clone(),
            "Child".into()
        )
        .await
        .is_err()
    );
    let changed = a
        .prepare_fork(
            &store,
            &first,
            "fork_one".into(),
            Scope {
                world: "other".into(),
                run: "run_a".into(),
            },
            "Child".into(),
        )
        .await?;
    assert!(changed.reserve(&mut m).is_err());
    reserved.restore(&mut m, 0)?;
    let status = running(&mut m, &child).await?;
    let input = child_a
        .prepare_admission(
            &store,
            Some(&first.cut_id),
            request(
                &child,
                1,
                status["revision"].as_u64().unwrap(),
                "child_retract",
                "alpha",
                true,
            ),
        )
        .await?;
    assert!(input.submit(&mut m).is_err());
    reserved
        .prepare_confirmation(&store)
        .await?
        .confirm(&mut m)?;
    reserved
        .prepare_confirmation(&store)
        .await?
        .confirm(&mut m)?;
    let before = f.commits();
    let k = key(&input.submit(&mut m)?)?;
    let cut = capture(&child_a, &mut m, &k).await?;
    assert!(
        child_a
            .publish_with_fault(&store, &cut, PublicationFault::AfterComponents)
            .await
            .is_err()
    );
    assert_eq!(store.history("child", "run_a").await?, vec![first.clone()]);
    m.stop(&child).map_err(|e| anyhow!(e))?;
    drop(m);
    drop(store);
    let mut m = f.manager()?;
    let store = CutStore::open(&storage).await?;
    let child_a = f.bind(&mut m, &child, "child")?;
    let published = child_a
        .reconcile(&store, 2, Some(&first.cut_id), &k)
        .await?;
    let child_empty = published.receipt().clone();
    assert_eq!(child_empty.tick, 2);
    assert_eq!(child_empty.world, "child");
    assert_eq!(child_empty.parent.as_deref(), Some(first.cut_id.as_str()));
    assert_ne!(child_empty.cut_id, first.cut_id);
    assert_eq!(child_empty.components["label"].rows, 0);
    assert_eq!(
        store
            .read(&child_empty, "label")
            .await?
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>(),
        0
    );
    published.confirm(&mut m)?;
    assert_eq!(
        store.history("child", "run_a").await?,
        vec![first.clone(), child_empty.clone()]
    );
    assert_eq!(
        store.history("parent", "run_a").await?,
        vec![first.clone(), empty.clone(), latest.clone()]
    );
    assert_eq!(store.history("nested", "run_a").await?, vec![first.clone()]);
    if !native {
        assert_eq!(f.commits(), before + 1);
    }
    assert!(child_a.prepare_restore(&store, &first).await.is_err());
    child_a
        .prepare_restore(&store, &child_empty)
        .await?
        .restore(&mut m, 1)?;
    running(&mut m, &child).await?;
    let own = commit(
        &child_a,
        &store,
        &mut m,
        &child,
        Some(&child_empty),
        ("child_new", "gamma", false),
    )
    .await?;
    assert_eq!(own.tick, 3);
    assert_ne!(
        own.components["label"].object,
        latest.components["label"].object
    );
    assert_eq!(store.checkpoint(&first).await?, source_checkpoint);
    // Empty historical source remains empty before any new child publication.
    let a = f.bind(&mut m, &id, "parent")?;
    let from_empty = a
        .prepare_fork(
            &store,
            &empty,
            "fork_empty".into(),
            Scope {
                world: "empty_child".into(),
                run: "run_a".into(),
            },
            "Empty".into(),
        )
        .await?
        .reserve(&mut m)?;
    from_empty.publish_origin(&store).await?;
    assert_eq!(
        store.history("empty_child", "run_a").await?,
        vec![first, empty.clone()]
    );
    assert_eq!(
        store
            .read(&empty, "label")
            .await?
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>(),
        0
    );
    Ok(())
}
#[tokio::test]
async fn historical_fork_origin_recovery_and_independent_full_cuts() -> Result<()> {
    contract(false).await
}
#[tokio::test]
#[ignore = "requires ARCHETYPE_DDLOG_DRIVER and real DDlog compiler"]
async fn installed_compiler_historical_fork() -> Result<()> {
    contract(true).await
}

#[tokio::test]
async fn ancestry_limit_rejects_before_reservation_and_keeps_last_origin_readable() -> Result<()> {
    let f = Fixture::new(false)?;
    let store = CutStore::open(&f.root.path().join("storage")).await?;
    let mut m = f.manager()?;
    let (id, mut a) = f.create(&mut m, "parent")?;
    m.start_async(&id).map_err(|e| anyhow!(e))?;
    running(&mut m, &id).await?;
    let source = commit(&a, &store, &mut m, &id, None, ("one", "alpha", false)).await?;
    for depth in 1..=32 {
        let name = format!("depth_{depth}");
        let r = a
            .prepare_fork(
                &store,
                &source,
                name.clone(),
                Scope {
                    world: name.clone(),
                    run: "run_a".into(),
                },
                name.clone(),
            )
            .await?
            .reserve(&mut m)?;
        r.publish_origin(&store).await?;
        a = r.adapter().clone();
    }
    assert_eq!(
        store.history("depth_32", "run_a").await?,
        vec![source.clone()]
    );
    let before = m
        .inventory(&ddlog_runtime::worlds::InventoryQuery::default())
        .map_err(|e| anyhow!(e))?["worlds"]
        .as_array()
        .unwrap()
        .len();
    let error = a
        .prepare_fork(
            &store,
            &source,
            "too_deep".into(),
            Scope {
                world: "depth_33".into(),
                run: "run_a".into(),
            },
            "Too deep".into(),
        )
        .await
        .err()
        .unwrap();
    assert!(
        error.to_string().contains("Fork ancestry depth"),
        "{error:#}"
    );
    assert_eq!(
        m.inventory(&ddlog_runtime::worlds::InventoryQuery::default())
            .map_err(|e| anyhow!(e))?["worlds"]
            .as_array()
            .unwrap()
            .len(),
        before
    );
    assert!(store.origin("depth_33", "run_a")?.is_none());
    assert_eq!(store.history("depth_32", "run_a").await?, vec![source]);
    Ok(())
}
