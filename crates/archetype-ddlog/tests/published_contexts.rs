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
