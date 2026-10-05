#![cfg(unix)]
#[path = "support/hosted.rs"]
mod support;
use anyhow::{Result, anyhow};
use archetype_ddlog::store::{
    CutStore,
    contexts::{ContextDraft, ContextFault},
    origin::Scope,
};
use support::*;

#[tokio::test]
async fn logical_fork_context_before_origin_recovers_exact_reservation() -> Result<()> {
    use ddlog_runtime::worlds::LogicalDestination;
    use serde_json::json;
    let f = Fixture::new(false)?;
    let mut m = f.manager()?;
    let store = CutStore::open(&f.root.path().join("store")).await?;
    let (parent_id, parent) = f.create(&mut m, "parent")?;
    m.start(&parent_id).map_err(|e| anyhow!(e))?;
    let status = running(&mut m, &parent_id).await?;
    let admission = parent
        .prepare_admission(
            &store,
            None,
            request(
                &parent_id,
                1,
                status["revision"].as_u64().unwrap(),
                "first",
                "a",
                false,
            ),
        )
        .await?;
    let key = key(&admission.submit(&mut m)?)?;
    let cut = capture(&parent, &mut m, &key).await?;
    let published = parent.publish(&store, &cut).await?;
    published.confirm(&mut m)?;
    let source = published.receipt().clone();
    let destination = LogicalDestination {
        resource: "child".into(),
        world: "child".into(),
        run: "run_a".into(),
    };
    let scope = Scope {
        world: destination.world.clone(),
        run: destination.run.clone(),
    };
    let mut ticket = parent
        .prepare_fork_source(
            &store,
            &source,
            "logical-fork".into(),
            scope.clone(),
            "Child".into(),
        )
        .await?;
    // Source verification alone cannot authorize first allocation.
    assert!(ticket.reserve(&mut m).is_err());
    assert!(
        ticket
            .reserve_logical(
                &mut m,
                destination.clone(),
                json!({"components":f.components})
            )
            .is_err()
    );
    ticket.check_new_destination(&store).await?;
    let binding = json!({"components":f.components});
    let (creation, reserved) =
        ticket.reserve_logical(&mut m, destination.clone(), binding.clone())?;
    reserved.check_logical_context(&store).await?;
    assert!(reserved.restore(&mut m, 0).is_err());
    let context = store
        .publish_context(&reserved.adapter().context_draft()?)
        .await?;
    m.confirm_creation_context(creation.clone(), context.context_id.clone())
        .map_err(|e| anyhow!(e))?;
    assert!(store.origin("child", "run_a")?.is_none());
    assert!(m.start_async(&creation.world_id).is_err());
    let commands = f.commits();
    drop(m);
    let mut m = f.manager()?;
    let parent = f.bind(&mut m, &parent_id, "parent")?;
    let retry = parent
        .prepare_fork_source(
            &store,
            &source,
            "logical-fork".into(),
            scope,
            "Child".into(),
        )
        .await?;
    assert!(
        retry
            .reserve_logical(&mut m, destination.clone(), json!({"changed":true}))
            .is_err()
    );
    let (same, reserved) = retry.reserve_logical(&mut m, destination, binding)?;
    assert_eq!(same, creation);
    reserved.check_logical_context(&store).await?;
    assert_eq!(
        store
            .publish_context(&reserved.adapter().context_draft()?)
            .await?,
        context
    );
    assert_eq!(f.commits(), commands);
    reserved.publish_origin(&store).await?;
    reserved.restore(&mut m, 0)?;
    running(&mut m, &creation.world_id).await?;
    reserved
        .prepare_confirmation(&store)
        .await?
        .confirm(&mut m)?;
    assert_eq!(store.history("child", "run_a").await?, vec![source]);
    Ok(())
}

#[tokio::test]
async fn fresh_scope_preflight_includes_unpublished_context_claims() -> Result<()> {
    let root = tempfile::tempdir()?;
    let store = CutStore::open(root.path()).await?;
    store.preflight_unclaimed_scope("fresh", "one").await?;
    assert!(store.lookup_context("fresh", "one").await?.is_none());
    let draft = ContextDraft::artifact_collection("fresh".into(), "one".into())?;
    assert!(
        store
            .publish_context_with_fault(&draft, ContextFault::AfterPreparation)
            .await
            .is_err()
    );
    assert!(store.lookup_context("fresh", "one").await?.is_none());
    assert!(
        store
            .preflight_unclaimed_scope("fresh", "one")
            .await
            .is_err()
    );
    store.publish_context(&draft).await?;
    assert!(
        store
            .preflight_unclaimed_scope("fresh", "one")
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn hosted_context_claim_rejects_fork_before_native_reservation() -> Result<()> {
    let f = Fixture::new(false)?;
    let mut m = f.manager()?;
    let store = CutStore::open(&f.root.path().join("store")).await?;
    let (parent_id, parent) = f.create(&mut m, "parent")?;
    m.start(&parent_id).map_err(|e| anyhow!(e))?;
    let started = running(&mut m, &parent_id).await?;
    let admission = parent
        .prepare_admission(
            &store,
            None,
            request(
                &parent_id,
                1,
                started["revision"].as_u64().unwrap(),
                "first",
                "a",
                false,
            ),
        )
        .await?;
    let key = key(&admission.submit(&mut m)?)?;
    let cut = capture(&parent, &mut m, &key).await?;
    let published = parent.publish(&store, &cut).await?;
    published.confirm(&mut m)?;
    for (name, fault) in [
        ("prepared", ContextFault::AfterPreparation),
        ("published", ContextFault::AfterRoot),
    ] {
        let commits = f.commits();
        let (id, owner) = f.create(&mut m, name)?;
        assert!(
            store
                .publish_context_with_fault(&owner.context_draft()?, fault)
                .await
                .is_err()
        );
        assert!(
            store
                .publish_context(&ContextDraft::artifact_collection(
                    name.into(),
                    "run_a".into()
                )?)
                .await
                .is_err()
        );
        let changed_owner = f.bind(&mut m, &parent_id, name)?;
        assert!(
            store
                .publish_context(&changed_owner.context_draft()?)
                .await
                .is_err()
        );
        let before = m.inventory(&Default::default()).map_err(|e| anyhow!(e))?["worlds"]
            .as_array()
            .unwrap()
            .len();
        assert!(
            parent
                .prepare_fork(
                    &store,
                    published.receipt(),
                    "fork_claim".into(),
                    Scope {
                        world: name.into(),
                        run: "run_a".into()
                    },
                    "child".into()
                )
                .await
                .is_err()
        );
        let after = m.inventory(&Default::default()).map_err(|e| anyhow!(e))?["worlds"]
            .as_array()
            .unwrap()
            .len();
        assert_eq!(before, after);
        assert_eq!(f.commits(), commits);
        assert_eq!(m.status(&id).map_err(|e| anyhow!(e))?["generation"], 0);
        assert!(!f.root.path().join("worlds/forks").exists());
    }
    Ok(())
}
