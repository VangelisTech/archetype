use super::*;
use bounds::{Budget, Fault, FaultCode, Limits};

fn code(error: &anyhow::Error) -> Option<FaultCode> {
    error
        .chain()
        .find_map(|error| error.downcast_ref::<Fault>().map(|f| f.code))
}

#[tokio::test]
async fn page_decodes_only_selected_rows_and_cold_reads_keep_exact_cut() -> Result<()> {
    let root = tempfile::tempdir()?;
    let store = CutStore::open(root.path()).await?;
    let mut frozen = crate::store_tests::cut();
    frozen.relations.remove("status");
    frozen.relations.get_mut("label").unwrap().rows = (0..100)
        .map(|i| {
            vec![
                serde_json::json!(9007199254740993i64 + i),
                serde_json::json!(format!("row-{i}")),
            ]
        })
        .collect();
    let cut = store.publish(&frozen).await?;
    drop(store);
    let store = CutStore::open(root.path()).await?;
    let scope = store.read_scope().await?;
    let batches = scope.read_page(&cut, "label", 98, 2).await?;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    let ids = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap();
    assert_eq!(ids.value(0), 9007199254741091);
    // One canonical history receipt and exactly two component rows, not 100.
    assert_eq!(scope.budget.usage().decoded_rows, 3);
    assert!(scope.read_page(&cut, "label", 100, 1).await?.is_empty());
    for (offset, limit) in [(0, 0), (0, 1001), (101, 1)] {
        assert_eq!(
            code(
                &store
                    .read_page(&cut, "label", offset, limit)
                    .await
                    .unwrap_err()
            ),
            Some(FaultCode::InvalidRequest)
        );
    }
    assert_eq!(
        code(&store.read_page(&cut, "missing", 0, 1).await.unwrap_err()),
        Some(FaultCode::InvalidRequest)
    );
    Ok(())
}

#[tokio::test]
async fn scan_stops_at_admitted_files_and_rows_without_partial_history() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut store = CutStore::open(root.path()).await?;
    store.publish(&crate::store_tests::cut()).await?;
    store.budget = Budget::new(Limits {
        files: 2,
        ..Limits::default()
    });
    let scope = store.read_scope().await?;
    assert_eq!(
        code(&scope.history("demo", "run_a").await.unwrap_err()),
        Some(FaultCode::ResourceLimit)
    );
    assert_eq!(scope.budget.usage().files, 2);
    assert_eq!(scope.budget.usage().decoded_rows, 0);
    store.budget = Budget::new(Limits {
        rows: 0,
        ..Limits::default()
    });
    let scope = store.read_scope().await?;
    assert_eq!(
        code(&scope.history("demo", "run_a").await.unwrap_err()),
        Some(FaultCode::ResourceLimit)
    );
    assert_eq!(scope.budget.usage().decoded_rows, 0);
    // A fresh operation receives a new budget; it never resets the old reader.
    store.budget = Budget::new(Limits::default());
    assert_eq!(store.history("demo", "run_a").await?.len(), 1);
    assert_eq!(scope.budget.usage().decoded_rows, 0);
    Ok(())
}

#[tokio::test]
async fn oversized_catalog_and_journal_fail_before_decode() -> Result<()> {
    let root = tempfile::tempdir()?;
    let store = CutStore::open(root.path()).await?;
    let cut = store.publish(&crate::store_tests::cut()).await?;
    let scope = store.read_scope().await?;
    let journal = scope.journal(&cut.world, &cut.run, cut.tick);
    OpenOptions::new()
        .write(true)
        .open(journal)?
        .set_len(3 << 20)?;
    assert_eq!(
        code(
            &scope
                .load_frozen(&cut.world, &cut.run, cut.tick)
                .unwrap_err()
        ),
        Some(FaultCode::ResourceLimit)
    );
    assert_eq!(scope.budget.usage().decodes, 0);
    let table = store.catalog.load_table(&ident("cuts")?).await?;
    let metadata = bounds::local_path(table.metadata_location().unwrap())?;
    OpenOptions::new()
        .write(true)
        .open(metadata)?
        .set_len(3 << 20)?;
    let scope = store.read_scope().await?;
    assert_eq!(
        code(&scope.history("demo", "run_a").await.unwrap_err()),
        Some(FaultCode::ResourceLimit)
    );
    assert_eq!(scope.budget.usage().decodes, 0);
    assert_eq!(scope.budget.usage().bytes, 0);
    Ok(())
}

#[tokio::test]
async fn oversized_journal_is_rejected_before_publication_intent() -> Result<()> {
    let root = tempfile::tempdir()?;
    let store = CutStore::open(root.path()).await?;
    let mut cut = crate::store_tests::cut();
    let mut checkpoint: serde_json::Value = serde_json::from_slice(&cut.checkpoint)?;
    checkpoint["padding"] = serde_json::json!("x".repeat(2 << 20));
    cut.checkpoint = serde_json::to_vec(&checkpoint)?;
    assert_eq!(
        code(&store.publish(&cut).await.unwrap_err()),
        Some(FaultCode::ResourceLimit)
    );
    assert!(store.history(&cut.world, &cut.run).await?.is_empty());
    assert!(!store.journal(&cut.world, &cut.run, cut.tick).exists());
    // An oversized invalid checkpoint is rejected by size before its JSON
    // parser or identity serializer is invoked.
    cut.checkpoint = vec![b'x'; 2 << 20];
    assert_eq!(
        code(&store.publish(&cut).await.unwrap_err()),
        Some(FaultCode::ResourceLimit)
    );
    Ok(())
}
