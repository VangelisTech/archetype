use super::*;
use context_attachments::{ArtifactSelection, ContextAttachmentFault};
use contexts::{ArtifactTarget, ContextDraft, ContextFault};

#[tokio::test]
async fn context_batch_exposes_a_durable_prefix_and_retries_exactly() -> Result<()> {
    let root = tempfile::tempdir()?;
    let store = CutStore::open(root.path()).await?;
    let context = store
        .publish_context(&ContextDraft::artifact_collection(
            "files".into(),
            "run_a".into(),
        )?)
        .await?;
    let target = ArtifactTarget {
        context: context.reference(),
        exact_cut: None,
    };
    let first = "01960000-0000-7000-8000-000000000001";
    let second = "01960000-0000-7000-8000-000000000002";
    let items = vec![
        attachments::tests::attachment(&store, first, "one")?,
        attachments::tests::attachment(&store, second, "two")?,
    ];
    assert!(
        store
            .attach_context_with_fault(&target, &items, ContextAttachmentFault::AfterCommon)
            .await
            .is_err()
    );
    let (partial, total) = store
        .context_attachments(&context.reference(), &ArtifactSelection::All, 0, 32)
        .await?;
    assert_eq!(total, 1);
    assert_eq!(partial[0].receipt.artifact_id, first);
    let first_receipt = partial[0].receipt.clone();
    for id in [first, second] {
        let table = store
            .catalog
            .load_table(&ident("context_artifact_text_v1")?)
            .await?;
        assert!(find_snapshot(&table, id)?.is_some());
    }
    let common = store
        .catalog
        .load_table(&ident("context_artifact_files_v1")?)
        .await?;
    assert!(find_snapshot(&common, second)?.is_none());
    drop(store);
    let store = CutStore::open(root.path()).await?;
    let (partial, total) = store
        .context_attachments(&context.reference(), &ArtifactSelection::All, 0, 32)
        .await?;
    assert_eq!(total, 1);
    assert_eq!(partial[0].receipt, first_receipt);
    let receipts = store.attach_context(&target, &items).await?;
    assert_eq!(receipts.len(), 2);
    assert_eq!(receipts[0], first_receipt);
    assert_eq!(store.attach_context(&target, &items).await?, receipts);
    assert_eq!(
        store
            .context_attachments(&context.reference(), &ArtifactSelection::All, 0, 32)
            .await?
            .1,
        2
    );
    Ok(())
}

#[tokio::test]
async fn unreadable_root_format_rejects_before_context_preparation() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut store = CutStore::open(root.path()).await?;
    store.budget = bounds::Budget::new(bounds::Limits {
        page_bytes: 1,
        ..Default::default()
    });
    let error = store
        .publish_context(&ContextDraft::artifact_collection(
            "files".into(),
            "run_a".into(),
        )?)
        .await
        .unwrap_err();
    assert!(error.chain().any(|e| {
        e.downcast_ref::<bounds::Fault>()
            .is_some_and(|f| f.code == bounds::FaultCode::ResourceLimit)
    }));
    assert!(!root.path().join("contexts/files.run_a.json").exists());
    assert_eq!(fs::read_dir(root.path().join("objects"))?.count(), 0);
    assert!(!root.path().join("warehouse/published_contexts_v1").exists());
    Ok(())
}

#[tokio::test]
async fn lost_preparation_cannot_move_an_occurrence_between_table_versions() -> Result<()> {
    let root = tempfile::tempdir()?;
    let store = CutStore::open(root.path()).await?;
    let cut = store.publish(&crate::store_tests::cut()).await?;
    let context = store
        .publish_context(&ContextDraft::artifact_collection(
            "files".into(),
            "run_a".into(),
        )?)
        .await?;
    let target = ArtifactTarget {
        context: context.reference(),
        exact_cut: None,
    };
    for legacy_first in [true, false] {
        let id = uuid::Uuid::now_v7().to_string();
        let attachment = attachments::tests::attachment(&store, &id, "value")?;
        let list = std::slice::from_ref(&attachment);
        let (old, new) = if legacy_first {
            store.attach(&cut, list).await?;
            ("cut_artifact", "context_artifact")
        } else {
            store.attach_context(&target, list).await?;
            ("context_artifact", "cut_artifact")
        };
        fs::remove_file(
            root.path()
                .join("objects")
                .join(format!("{old}_prepared_v1.{id}.json")),
        )?;
        if legacy_first {
            assert!(store.attach_context(&target, list).await.is_err());
        } else {
            assert!(store.attach(&cut, list).await.is_err());
        }
        assert!(
            !root
                .path()
                .join("objects")
                .join(format!("{new}_prepared_v1.{id}.json"))
                .exists()
        );
    }
    Ok(())
}

#[tokio::test]
async fn context_root_publication_adopts_exact_preparation_after_each_fault() -> Result<()> {
    for fault in [
        ContextFault::AfterPreparation,
        ContextFault::AfterObject,
        ContextFault::AfterRoot,
    ] {
        let root = tempfile::tempdir()?;
        let store = CutStore::open(root.path()).await?;
        let draft = ContextDraft::artifact_collection("files".into(), "run_a".into())?;
        assert!(
            store
                .publish_context_with_fault(&draft, fault)
                .await
                .is_err()
        );
        assert_eq!(
            store.context_at("files", "run_a").await.is_ok(),
            matches!(fault, ContextFault::AfterRoot)
        );
        assert!(store.history("files", "run_a").await?.is_empty());
        assert_eq!(fs::read_dir(root.path().join("cuts"))?.count(), 0);
        let bytes = fs::read(root.path().join("contexts/files.run_a.json"))?;
        drop(store);
        let store = CutStore::open(root.path()).await?;
        let context = store.publish_context(&draft).await?;
        assert_eq!(store.publish_context(&draft).await?, context);
        assert_eq!(store.context(&context.reference()).await?, context);
        assert_eq!(
            fs::read(root.path().join("contexts/files.run_a.json"))?,
            bytes
        );
        fs::remove_file(root.path().join("contexts/files.run_a.json"))?;
        assert!(store.context(&context.reference()).await.is_err());
        assert!(store.publish_context(&draft).await.is_err());
    }
    Ok(())
}

#[tokio::test]
async fn context_occurrences_preserve_target_inventory_and_cold_visibility() -> Result<()> {
    for fault in [
        ContextAttachmentFault::AfterPreparation,
        ContextAttachmentFault::AfterTyped,
        ContextAttachmentFault::AfterCommon,
    ] {
        let root = tempfile::tempdir()?;
        let store = CutStore::open(root.path()).await?;
        let context = store
            .publish_context(&ContextDraft::artifact_collection(
                "files".into(),
                "run_a".into(),
            )?)
            .await?;
        let other = store
            .publish_context(&ContextDraft::artifact_collection(
                "other".into(),
                "run_a".into(),
            )?)
            .await?;
        let target = ArtifactTarget {
            context: context.reference(),
            exact_cut: None,
        };
        let id = uuid::Uuid::now_v7().to_string();
        let attachment = attachments::tests::attachment(&store, &id, "content")?;
        assert!(
            store
                .attach_context_with_fault(&target, std::slice::from_ref(&attachment), fault)
                .await
                .is_err()
        );
        let (_, visible) = store
            .context_attachments(&context.reference(), &ArtifactSelection::All, 0, 32)
            .await?;
        assert_eq!(
            visible,
            usize::from(matches!(fault, ContextAttachmentFault::AfterCommon))
        );
        let mut changed = attachment.clone();
        changed.typed.clear();
        assert!(store.attach_context(&target, &[changed]).await.is_err());
        assert!(
            store
                .attach_context(
                    &ArtifactTarget {
                        context: other.reference(),
                        exact_cut: None
                    },
                    std::slice::from_ref(&attachment)
                )
                .await
                .is_err()
        );
        drop(store);
        let store = CutStore::open(root.path()).await?;
        let receipt = store
            .attach_context(&target, std::slice::from_ref(&attachment))
            .await?;
        assert_eq!(
            store
                .attach_context(&target, std::slice::from_ref(&attachment))
                .await?,
            receipt
        );
        let (rows, total) = store
            .context_attachments(
                &context.reference(),
                &ArtifactSelection::Target { exact_cut: None },
                0,
                32,
            )
            .await?;
        assert_eq!(total, 1);
        assert_eq!(rows[0].receipt, receipt[0]);
        assert!(rows[0].receipt.target.exact_cut.is_none());
        let intent = root
            .path()
            .join("objects")
            .join(format!("context_artifact_prepared_v1.{id}.json"));
        fs::remove_file(&intent)?;
        assert!(
            store
                .context_attachments(&context.reference(), &ArtifactSelection::All, 0, 32)
                .await
                .is_err()
        );
        assert!(
            store
                .attach_context(&target, std::slice::from_ref(&attachment))
                .await
                .is_err()
        );
        assert!(!intent.exists());
    }
    Ok(())
}
