#![cfg(unix)]
//! Ordinary tests use SIMULATED native transport and real local Iceberg storage.
//! The ignored installed-compiler test separately exercises the actual DDlog graph.
#[path = "support/hosted.rs"]
mod support;
use anyhow::{Result, anyhow};
use archetype_ddlog::store::{
    CutStore, PublicationFault,
    bounds::{Fault, FaultCode},
};
use serde_json::json;
use std::fs;
use support::*;

async fn recovery_contract(native: bool) -> Result<()> {
    let f = Fixture::new(native)?;
    let storage = f.root.path().join("storage");
    let store = CutStore::open(&storage).await?;
    let mut m = f.manager()?;
    let (id, a) = f.create(&mut m, "alpha")?;
    let (other, b) = f.create(&mut m, "beta")?;
    m.start_async(&id).map_err(|e| anyhow!(e))?;
    m.start_async(&other).map_err(|e| anyhow!(e))?;
    let initial = running(&mut m, &id).await?;
    let second = running(&mut m, &other).await?;
    let first = a
        .prepare_admission(
            &store,
            None,
            request(
                &id,
                1,
                initial["revision"].as_u64().unwrap(),
                "one",
                "alpha",
                false,
            ),
        )
        .await?;
    let sibling = b
        .prepare_admission(
            &store,
            None,
            request(
                &other,
                1,
                second["revision"].as_u64().unwrap(),
                "one",
                "beta",
                false,
            ),
        )
        .await?;
    let before = f.commits();
    let k = key(&first.submit(&mut m)?)?;
    let other_key = key(&sibling.submit(&mut m)?)?;
    let frozen = capture(&a, &mut m, &k).await?;
    let sibling_cut = capture(&b, &mut m, &other_key).await?;
    let status = lookup(&mut m, &k)?;
    let applied = status["applied_revision"].as_u64().unwrap();
    assert_eq!(first.submit(&mut m)?["state"], "frozen");
    if !native {
        assert_eq!(f.commits(), before + 2);
    }
    // Two component commits alone confer no complete-cut visibility.
    assert!(
        a.publish_with_fault(&store, &frozen, PublicationFault::AfterComponents)
            .await
            .is_err()
    );
    assert!(store.history("alpha", "run_a").await?.is_empty());
    let later = a
        .prepare_admission(
            &store,
            None,
            request(&id, 1, applied, "too-soon", "later", false),
        )
        .await?;
    assert!(later.submit(&mut m).is_err());
    let sibling_published = b.publish(&store, &sibling_cut).await?;
    sibling_published.confirm(&mut m)?;
    let b_receipt = sibling_published.receipt().clone();
    assert_eq!(b_receipt.components.len(), 2);
    assert_eq!(store.read(&b_receipt, "label").await?[0].num_rows(), 1);
    // Retained upstream freeze and the analytical journal survive both owners.
    m.stop(&id).map_err(|e| anyhow!(e))?;
    drop(m);
    drop(store);
    let mut m = f.manager()?;
    let store = CutStore::open(&storage).await?;
    let a = f.bind(&mut m, &id, "alpha")?;
    assert!(m.start_async(&id).is_err());
    assert!(a.reconcile(&store, 1, Some("stale"), &k).await.is_err());
    let mut forged_key = k.clone();
    forged_key.request_sha256 = "0".repeat(64);
    assert!(a.reconcile(&store, 1, None, &forged_key).await.is_err());
    let published = a.reconcile(&store, 1, None, &k).await?;
    let receipt = published.receipt().clone();
    assert_eq!(receipt.components.len(), 2);
    assert_ne!(receipt.cut_id, b_receipt.cut_id);
    assert_ne!(receipt.cut_id, published.external_receipt().receipt_sha256);
    for name in ["label", "status"] {
        assert_eq!(store.read(&receipt, name).await?[0].num_rows(), 1);
    }
    // Forged receipt cannot mint a verified restore/confirmation ticket.
    let mut forged = receipt.clone();
    forged.components.remove("status");
    assert!(a.prepare_restore(&store, &forged).await.is_err());
    let mut forged = receipt.clone();
    forged.program = "forged".into();
    assert!(a.prepare_restore(&store, &forged).await.is_err());
    published.confirm(&mut m)?;
    assert_eq!(first.submit(&mut m)?["state"], "published");
    if !native {
        assert_eq!(f.commits(), before + 2);
    }
    let stale_restore = a.prepare_restore(&store, &receipt).await?;
    // Same analytical world/run, latest checkpoint, new native generation.
    a.prepare_restore(&store, &receipt)
        .await?
        .restore(&mut m, 1)?;
    let restored = running(&mut m, &id).await?;
    assert_eq!(restored["generation"], 2);
    assert_eq!(
        restored["persistence"]["restored_from"]["origin"]["generation"],
        1
    );
    assert_eq!(
        restored["persistence"]["restored_from"]["origin"]["build"],
        status["boundary"]["manifest"]["checkpoint_receipt"]["origin"]["build"]
    );
    assert!(restored["instance"]["build"]["native_sha256"].is_string());
    assert!(
        a.prepare_admission(
            &store,
            None,
            request(&id, 2, applied, "stale", "alpha", true)
        )
        .await
        .is_err()
    );
    let retract = a
        .prepare_admission(
            &store,
            Some(&receipt.cut_id),
            request(
                &id,
                2,
                restored["revision"].as_u64().unwrap(),
                "retract",
                "alpha",
                true,
            ),
        )
        .await?;
    let retraction_key = key(&retract.submit(&mut m)?)?;
    let empty = capture(&a, &mut m, &retraction_key).await?;
    assert!(
        a.publish_with_fault(&store, &empty, PublicationFault::AfterManifest)
            .await
            .is_err()
    );
    assert_eq!(lookup(&mut m, &retraction_key)?["state"], "frozen");
    // Latest exact journal-backed successor recovers the lost catalog ack.
    let empty_published = a
        .reconcile(&store, 2, Some(&receipt.cut_id), &retraction_key)
        .await?;
    let empty_receipt = empty_published.receipt().clone();
    assert_eq!(
        empty_receipt.parent.as_deref(),
        Some(receipt.cut_id.as_str())
    );
    for name in ["label", "status"] {
        assert_eq!(store.read(&empty_receipt, name).await?[0].num_rows(), 0);
        assert_eq!(store.read(&receipt, name).await?[0].num_rows(), 1);
    }
    assert!(a.prepare_restore(&store, &receipt).await.is_err());
    // Failure to persist native confirmation retains this exact candidate.
    let path = f.root.path().join("worlds").join(&id).join("world.json");
    let saved = fs::read(&path)?;
    fs::remove_file(&path)?;
    fs::create_dir(&path)?;
    assert!(empty_published.confirm(&mut m).is_err());
    assert!(m.start_async(&id).is_err());
    fs::remove_dir(&path)?;
    fs::write(&path, saved)?;
    let after = f.commits();
    drop(m);
    drop(store);
    let mut m = f.manager()?;
    let store = CutStore::open(&storage).await?;
    let a = f.bind(&mut m, &id, "alpha")?;
    let recovered = a
        .reconcile(&store, 2, Some(&receipt.cut_id), &retraction_key)
        .await?;
    assert_eq!(
        recovered.external_receipt(),
        empty_published.external_receipt()
    );
    recovered.confirm(&mut m)?;
    assert_eq!(retract.submit(&mut m)?["state"], "published");
    if !native {
        assert_eq!(f.commits(), after);
    }
    assert_eq!(store.history("alpha", "run_a").await?.len(), 2);
    assert!(
        stale_restore.restore(&mut m, 2).is_err(),
        "held old restore ticket bypassed latest native receipt head"
    );
    // Restore the latest empty cut, then produce nonempty full state again.
    a.prepare_restore(&store, &empty_receipt)
        .await?
        .restore(&mut m, 2)?;
    let restored = running(&mut m, &id).await?;
    let next = a
        .prepare_admission(
            &store,
            Some(&empty_receipt.cut_id),
            request(
                &id,
                3,
                restored["revision"].as_u64().unwrap(),
                "again",
                "again",
                false,
            ),
        )
        .await?;
    let next_key = key(&next.submit(&mut m)?)?;
    let next_cut = capture(&a, &mut m, &next_key).await?;
    let third = a.publish(&store, &next_cut).await?;
    third.confirm(&mut m)?;
    assert_eq!(third.receipt().tick, 3);
    assert_eq!(store.read(third.receipt(), "label").await?[0].num_rows(), 1);
    assert!(
        a.reconcile(&store, 2, Some(&receipt.cut_id), &retraction_key)
            .await
            .is_err()
    );
    assert_eq!(store.read(&b_receipt, "label").await?[0].num_rows(), 1);
    if native {
        let batches = store.read(third.receipt(), "status").await?;
        let values = batches[0]
            .column_by_name("status__state")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(
            values.value(0),
            "ready",
            "actual composed downstream rule must run"
        );
        eprintln!(
            "native hosted publication evidence retained at {}",
            f.root.keep().display()
        );
    }
    Ok(())
}
#[tokio::test]
async fn simulated_native_real_iceberg_hosted_recovery() -> Result<()> {
    recovery_contract(false).await
}
#[tokio::test]
#[ignore = "requires ARCHETYPE_DDLOG_DRIVER and installed DDlog compiler"]
async fn installed_compiler_hosted_iceberg_recovery() -> Result<()> {
    recovery_contract(true).await
}

#[tokio::test]
async fn retained_native_freeze_recovers_without_an_analytical_journal() -> Result<()> {
    let f = Fixture::new(false)?;
    let store = CutStore::open(&f.root.path().join("store")).await?;
    let mut m = f.manager()?;
    let (id, a) = f.create(&mut m, "alpha")?;
    m.start(&id).map_err(|e| anyhow!(e))?;
    let ticket = a
        .prepare_admission(&store, None, request(&id, 1, 1, "one", "a", false))
        .await?;
    let k = key(&ticket.submit(&mut m)?)?;
    finished(&mut m, &k).await?;
    let before = f.commits();
    drop(m);
    let mut m = f.manager()?;
    let a = f.bind(&mut m, &id, "alpha")?;
    assert!(a.reconcile(&store, 1, None, &k).await.is_err());
    let cut = capture(&a, &mut m, &k).await?;
    let published = a.publish(&store, &cut).await?;
    published.confirm(&mut m)?;
    assert_eq!(f.commits(), before);
    assert_eq!(store.history("alpha", "run_a").await?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn wrong_context_cannot_publish_and_uncertain_apply_never_replays() -> Result<()> {
    let f = Fixture::new(false)?;
    let store = CutStore::open(&f.root.path().join("store")).await?;
    let mut m = f.manager()?;
    let (id, a) = f.create(&mut m, "alpha")?;
    m.start(&id).map_err(|e| anyhow!(e))?;
    // A caller bypassing the adapter's preparation cannot make its opaque
    // upstream context valid Archetype evidence, even though native apply ran.
    let wrong = json!({"admission":request(&id,1,1,"wrong","a",false),"binding":{"context":{"world":"wrong"},"parent_receipt_sha256":null}});
    let k = key(&m
        .admit_boundary_async(serde_json::from_value(wrong)?)
        .map_err(|e| anyhow!(e))?)?;
    finished(&mut m, &k).await?;
    assert!(a.capture(&mut m, &k).is_err());
    assert!(store.history("alpha", "run_a").await?.is_empty());
    assert!(m.start_async(&id).is_err());
    let (other, b) = f.create(&mut m, "beta")?;
    m.start(&other).map_err(|e| anyhow!(e))?;
    let ticket = b
        .prepare_admission(&store, None, request(&other, 1, 1, "die", "b", false))
        .await?;
    fs::write(f.root.path().join("die_on_commit"), "")?;
    let before = f.commits();
    let key = key(&ticket.submit(&mut m)?)?;
    assert_eq!(finished(&mut m, &key).await?["state"], "uncertain");
    fs::remove_file(f.root.path().join("die_on_commit"))?;
    drop(m);
    let mut m = f.manager()?;
    assert_eq!(ticket.submit(&mut m)?["state"], "uncertain");
    assert!(b.capture(&mut m, &key).is_err());
    assert!(m.start_async(&other).is_err());
    assert_eq!(f.commits(), before + 1);
    Ok(())
}

#[tokio::test]
async fn corrupt_component_blocks_full_cut_confirmation_recovery() -> Result<()> {
    let f = Fixture::new(false)?;
    let store = CutStore::open(&f.root.path().join("store")).await?;
    let mut m = f.manager()?;
    let (id, a) = f.create(&mut m, "alpha")?;
    m.start(&id).map_err(|e| anyhow!(e))?;
    let ticket = a
        .prepare_admission(&store, None, request(&id, 1, 1, "one", "a", false))
        .await?;
    let k = key(&ticket.submit(&mut m)?)?;
    let cut = capture(&a, &mut m, &k).await?;
    assert!(
        a.publish_with_fault(&store, &cut, PublicationFault::AfterManifest)
            .await
            .is_err()
    );
    let receipt = store.history("alpha", "run_a").await?.pop().unwrap();
    fs::write(
        receipt.components["status"].object.as_ref().unwrap(),
        "corrupt",
    )?;
    assert_eq!(store.read(&receipt, "label").await?[0].num_rows(), 1);
    assert!(a.reconcile(&store, 1, None, &k).await.is_err());
    assert!(a.prepare_restore(&store, &receipt).await.is_err());
    assert_eq!(lookup(&mut m, &k)?["state"], "frozen");
    Ok(())
}

#[tokio::test]
async fn held_native_world_does_not_block_sibling_publication_or_stop() -> Result<()> {
    use std::time::{Duration, Instant};
    let f = Fixture::new(false)?;
    let store = CutStore::open(&f.root.path().join("store")).await?;
    let mut m = f.manager()?;
    let (id, a) = f.create(&mut m, "alpha")?;
    let (other, b) = f.create(&mut m, "beta")?;
    let started = m.start(&id).map_err(|e| anyhow!(e))?;
    m.start(&other).map_err(|e| anyhow!(e))?;
    let pid = started["resources"]["pid"].as_u64().unwrap();
    fs::write(f.root.path().join(format!("hold_commit_{pid}")), "")?;
    let blocked = a
        .prepare_admission(&store, None, request(&id, 1, 1, "held", "a", false))
        .await?;
    let k = key(&blocked.submit(&mut m)?)?;
    let deadline = Instant::now() + Duration::from_secs(5);
    while !f.root.path().join(format!("commit_held_{pid}")).exists() {
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let began = Instant::now();
    assert_eq!(
        m.status(&id).map_err(|e| anyhow!(e))?["external_publication"]["busy"],
        true
    );
    let sibling = b
        .prepare_admission(&store, None, request(&other, 1, 1, "one", "b", false))
        .await?;
    let sibling_key = key(&sibling.submit(&mut m)?)?;
    let cut = capture(&b, &mut m, &sibling_key).await?;
    let published = b.publish(&store, &cut).await?;
    published.confirm(&mut m)?;
    assert_eq!(store.history("beta", "run_a").await?.len(), 1);
    m.stop(&id).map_err(|e| anyhow!(e))?;
    assert!(began.elapsed() < Duration::from_secs(5));
    assert_eq!(finished(&mut m, &k).await?["state"], "uncertain");
    assert!(store.history("alpha", "run_a").await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn multi_page_capture_is_complete_and_can_release_the_manager_between_pages() -> Result<()> {
    let f = Fixture::new(false)?;
    let store = CutStore::open(&f.root.path().join("store")).await?;
    let mut m = f.manager()?;
    let (id, a) = f.create(&mut m, "alpha")?;
    m.start(&id).map_err(|e| anyhow!(e))?;
    fs::write(f.root.path().join("expanded_output"), "")?;
    let admission = a
        .prepare_admission(&store, None, request(&id, 1, 1, "large", "a", false))
        .await?;
    let k = key(&admission.submit(&mut m)?)?;
    assert_eq!(finished(&mut m, &k).await?["state"], "frozen");
    let mut capture = a.capture(&mut m, &k)?;
    let mut pages = 0;
    loop {
        pages += 1;
        let complete = capture.advance(&mut m)?;
        // Capture owns buffers, not a retained manager borrow or mutex guard.
        assert_eq!(
            m.status(&id).map_err(|e| anyhow!(e))?["external_publication"]["busy"],
            false
        );
        if complete {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        pages > 3,
        "checkpoint and two outputs must require continuation pages"
    );
    let cut = capture.finish()?;
    let captured = serde_json::to_value(&cut)?;
    for name in ["label", "status"] {
        let rows = captured["relations"][name]["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 1200);
        let mut ids = std::collections::BTreeSet::new();
        for row in rows {
            ids.insert(row[0].as_u64().unwrap());
            assert_eq!(row[1], "x".repeat(4000));
        }
        assert_eq!(ids, (0..1200).collect());
    }
    // Native capture can span more bytes than the analytical journal budget.
    // Rejection leaves the frozen native boundary retryable and unconfirmed.
    let frozen = lookup(&mut m, &k)?;
    let before = m.status(&id).map_err(|e| anyhow!(e))?;
    let commits = f.commits();
    let error = a
        .publish(&store, &cut)
        .await
        .err()
        .expect("oversized journal");
    assert_eq!(
        error
            .chain()
            .find_map(|e| e.downcast_ref::<Fault>())
            .map(|e| e.code),
        Some(FaultCode::ResourceLimit)
    );
    assert!(store.history("alpha", "run_a").await?.is_empty());
    assert!(!f.root.path().join("store/cuts/alpha.run_a.1.json").exists());
    assert_eq!(
        fs::read_dir(f.root.path().join("store/objects"))?.count(),
        0
    );
    assert_eq!(lookup(&mut m, &k)?, frozen);
    let after = m.status(&id).map_err(|e| anyhow!(e))?;
    assert_eq!(after["generation"], before["generation"]);
    assert_eq!(after["revision"], before["revision"]);
    let blocked = a
        .prepare_admission(
            &store,
            None,
            request(
                &id,
                1,
                after["revision"].as_u64().unwrap(),
                "next",
                "b",
                false,
            ),
        )
        .await?;
    assert!(blocked.submit(&mut m).is_err());
    assert_eq!(f.commits(), commits);
    Ok(())
}
