//! Deterministic physical store remote-data contracts. No cloud credentials or calls.
use super::*;
use base64::{Engine, engine::general_purpose::STANDARD};
use bytes::Bytes;
use context_attachments::ArtifactSelection;
use contexts::{ArtifactTarget, ContextDraft};
use remote::{DataProvider, RemoteBackend};
use std::ops::Range;
use std::process::{Command, Stdio};

#[derive(Default)]
struct Memory(std::sync::Mutex<BTreeMap<String, Bytes>>);
#[async_trait::async_trait]
impl DataProvider for Memory {
    async fn exists(&self, key: &str) -> Result<bool> {
        Ok(self.0.lock().unwrap().contains_key(key))
    }
    async fn size(&self, key: &str) -> Result<u64> {
        self.0
            .lock()
            .unwrap()
            .get(key)
            .map(|v| v.len() as u64)
            .ok_or_else(|| anyhow!("absent"))
    }
    async fn range(&self, key: &str, range: Range<u64>) -> Result<Bytes> {
        Ok(self
            .0
            .lock()
            .unwrap()
            .get(key)
            .ok_or_else(|| anyhow!("absent"))?
            .slice(range.start as usize..range.end as usize))
    }
    async fn create(&self, key: &str, bytes: Bytes) -> Result<()> {
        let mut map = self.0.lock().unwrap();
        if map.contains_key(key) {
            anyhow::bail!("precondition failed");
        }
        map.insert(key.into(), bytes);
        // Every acknowledgement is deliberately lost, including Iceberg metadata.
        anyhow::bail!("lost acknowledgement")
    }
}
fn profile() -> config::RemoteData {
    config::RemoteData {
        version: 1,
        uri: "s3://synthetic-bucket/task/case".into(),
        region: "auto".into(),
        endpoint: None,
        path_style_access: true,
        credential_source: config::CredentialSource::AwsEnvironment,
    }
}
async fn open(root: &Path, provider: Arc<Memory>) -> Result<CutStore> {
    CutStore::open_data(root, Some(profile()), move |profile| {
        RemoteBackend::with_provider(profile, provider)
    })
    .await
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecoveryFixture {
    parent_pid: u32,
    context: contexts::ContextRef,
    receipt: context_attachments::ContextAttachmentReceipt,
    cut: CutReceipt,
    sha256: String,
    checkpoint: String,
    // Synthetic provider state, not a local copy of published control authority.
    // A fresh child loads these bytes into an independent provider instance.
    remote_objects: BTreeMap<String, String>,
}

fn copy_control(source: &Path, destination: &Path) -> Result<()> {
    fs::create_dir_all(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        ensure!(
            !kind.is_symlink(),
            "Synthetic control fixture contains a symlink"
        );
        let target = destination.join(entry.file_name());
        if kind.is_dir() {
            copy_control(&entry.path(), &target)?;
        } else {
            fs::copy(entry.path(), target)?;
        }
    }
    Ok(())
}

fn recovery_process(fixture: &Path, root: &Path, mode: &str) -> Result<()> {
    let mut child = Command::new(std::env::current_exe()?)
        .args([
            "--ignored",
            "--exact",
            "store::remote_tests::remote_recovery_process_entry",
            "--nocapture",
        ])
        .env_clear()
        .env("ARCHETYPE_SYNTHETIC_RECOVERY_FIXTURE", fixture)
        .env("ARCHETYPE_SYNTHETIC_RECOVERY_ROOT", root)
        .env("ARCHETYPE_SYNTHETIC_RECOVERY_MODE", mode)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        if child.try_wait()?.is_some() {
            break;
        }
        if std::time::Instant::now() >= deadline {
            child.kill()?;
            child.wait()?;
            anyhow::bail!("Synthetic fresh-process recovery timed out: {mode}");
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let output = child.wait_with_output()?;
    ensure!(
        output.status.success(),
        "Fresh-process recovery failed: {mode}\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    ensure!(
        String::from_utf8_lossy(&output.stdout).contains(&format!("REMOTE_RECOVERY_OK {mode}")),
        "Fresh-process recovery oracle did not execute: {mode}"
    );
    Ok(())
}

// Invoked only by the parent test below; the marker prevents a nonexistent
// --exact selector from silently passing. All provider bytes are synthetic.
#[tokio::test]
#[ignore = "entry point for bounded fresh-process synthetic recovery tests"]
async fn remote_recovery_process_entry() -> Result<()> {
    let fixture_path = PathBuf::from(
        std::env::var_os("ARCHETYPE_SYNTHETIC_RECOVERY_FIXTURE")
            .ok_or_else(|| anyhow!("Missing synthetic recovery fixture"))?,
    );
    ensure!(
        fs::metadata(&fixture_path)?.len() <= 2 << 20,
        "Recovery fixture exceeds budget"
    );
    let fixture: RecoveryFixture = serde_json::from_slice(&fs::read(fixture_path)?)?;
    ensure!(
        std::process::id() != fixture.parent_pid,
        "Reader is not a fresh process"
    );
    ensure!(
        std::env::var_os("AWS_ACCESS_KEY_ID").is_none(),
        "Provider credentials inherited"
    );
    let root = PathBuf::from(
        std::env::var_os("ARCHETYPE_SYNTHETIC_RECOVERY_ROOT")
            .ok_or_else(|| anyhow!("Missing synthetic recovery root"))?,
    );
    let mode = std::env::var("ARCHETYPE_SYNTHETIC_RECOVERY_MODE")?;
    let objects = fixture
        .remote_objects
        .into_iter()
        .map(|(key, value)| Ok((key, Bytes::from(STANDARD.decode(value)?))))
        .collect::<Result<BTreeMap<_, _>>>()?;
    ensure!(!objects.is_empty(), "Synthetic remote data missing");
    let provider = Arc::new(Memory(std::sync::Mutex::new(objects)));
    // No source files, artifact staging, live manager, native program driver or
    // parent process objects are available to this reader.
    ensure!(
        !root.join("artifact_objects").exists(),
        "Artifact staging survived"
    );
    let cold = open(&root, provider).await?;
    match mode.as_str() {
        "retained" | "missing_journal" => {
            assert_eq!(
                cold.context(&fixture.context).await?.reference(),
                fixture.context
            );
            let (rows, total) = cold
                .context_attachments(&fixture.context, &ArtifactSelection::All, 0, 32)
                .await?;
            assert_eq!(total, 1);
            assert_eq!(rows[0].receipt, fixture.receipt);
            assert_eq!(rows[0].sha256, fixture.sha256);
            assert_eq!(
                rows[0].typed_facts["text"]["text_kind"],
                serde_json::json!({"string": "plain"})
            );
            assert_eq!(
                cold.history(&fixture.cut.world, &fixture.cut.run).await?,
                vec![fixture.cut.clone()]
            );
            if mode == "retained" {
                cold.verified_cut(&fixture.cut).await?;
                assert_eq!(
                    cold.checkpoint(&fixture.cut).await?,
                    STANDARD.decode(fixture.checkpoint)?
                );
            } else {
                // Analytical visibility alone cannot reconstruct a lost native
                // checkpoint journal, even with all remote objects intact.
                assert!(cold.verified_cut(&fixture.cut).await.is_err());
                assert!(cold.checkpoint(&fixture.cut).await.is_err());
            }
        }
        "missing_catalog" | "new_host" => {
            // Opening can initialize a new local catalog. It must not adopt a
            // known receipt/context from remote bytes as recovered authority.
            assert!(
                cold.lookup_context(&fixture.context.world, &fixture.context.run)
                    .await?
                    .is_none()
            );
            assert!(
                cold.history(&fixture.cut.world, &fixture.cut.run)
                    .await?
                    .is_empty()
            );
            assert!(cold.context(&fixture.context).await.is_err());
            assert!(
                cold.context_attachments(&fixture.context, &ArtifactSelection::All, 0, 32)
                    .await
                    .is_err()
            );
            assert!(cold.verified_cut(&fixture.cut).await.is_err());
            assert!(cold.checkpoint(&fixture.cut).await.is_err());
        }
        _ => anyhow::bail!("Unknown synthetic recovery mode"),
    }
    println!("REMOTE_RECOVERY_OK {mode}");
    Ok(())
}

#[tokio::test]
async fn fresh_process_remote_recovery_requires_retained_local_control() -> Result<()> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("authority");
    let provider = Arc::new(Memory::default());
    let store = open(&root, provider.clone()).await?;
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
    let mut attachment = attachments::tests::attachment(
        &store,
        "01960000-0000-7000-8000-000000000002",
        "original bytes",
    )?;
    let digest = crate::hash(b"original bytes");
    let original = store.publish_context_object(&target, &digest, 14).await?;
    let common = attachments::decode_bounded(&attachment.common, &store.budget)?;
    let mut columns = common.columns().to_vec();
    columns[common.schema().index_of("object_uri")?] = Arc::new(StringArray::from(vec![
        original["object_uri"].as_str().unwrap(),
    ]));
    attachment.common = STANDARD.encode(encode_batch(&RecordBatch::try_new(
        common.schema(),
        columns,
    )?)?);
    let receipts = store.attach_context(&target, &[attachment]).await?;
    let frozen = crate::store_tests::cut();
    let cut = store.publish(&frozen).await?;
    let fixture = RecoveryFixture {
        parent_pid: std::process::id(),
        context: context.reference(),
        receipt: receipts[0].clone(),
        cut,
        sha256: digest,
        checkpoint: STANDARD.encode(frozen.checkpoint),
        remote_objects: provider
            .0
            .lock()
            .unwrap()
            .iter()
            .map(|(key, value)| (key.clone(), STANDARD.encode(value)))
            .collect(),
    };
    drop(store);
    fs::remove_dir_all(root.join("artifact_objects"))?;
    let fixture_path = temporary.path().join("synthetic-provider.json");
    let encoded = serde_json::to_vec(&fixture)?;
    ensure!(encoded.len() <= 2 << 20, "Recovery fixture exceeds budget");
    fs::write(&fixture_path, encoded)?;
    recovery_process(&fixture_path, &root, "retained")?;

    // Each loss case starts from a separate copy of the retained control state;
    // missing journals cannot accidentally mask a missing-catalog regression.
    let catalog_lost = temporary.path().join("catalog-lost");
    copy_control(&root, &catalog_lost)?;
    for name in ["catalog.sqlite", "catalog.sqlite-wal", "catalog.sqlite-shm"] {
        let file = catalog_lost.join(name);
        if file.exists() {
            fs::remove_file(file)?;
        }
    }
    recovery_process(&fixture_path, &catalog_lost, "missing_catalog")?;
    let journal_lost = temporary.path().join("journal-lost");
    copy_control(&root, &journal_lost)?;
    fs::remove_dir_all(journal_lost.join("cuts"))?;
    recovery_process(&fixture_path, &journal_lost, "missing_journal")?;
    recovery_process(
        &fixture_path,
        &temporary.path().join("new-host"),
        "new_host",
    )?;
    Ok(())
}
#[tokio::test]
async fn cold_remote_context_verifies_common_typed_and_original_without_staged_bytes() -> Result<()>
{
    let root = tempfile::tempdir()?;
    let provider = Arc::new(Memory::default());
    let store = open(root.path(), provider.clone()).await?;
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
    let mut wrong = target.clone();
    wrong.context.context_id = "f".repeat(64);
    let error = store
        .publish_context_object(&wrong, "invalid", u64::MAX)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("Context reference mismatch"));
    assert!(!root.path().join("artifact_objects").exists());
    let id = "01960000-0000-7000-8000-000000000001";
    let mut attachment = attachments::tests::attachment(&store, id, "original bytes")?;
    let digest = crate::hash(b"original bytes");
    let original = store.publish_context_object(&target, &digest, 14).await?;
    let common = attachments::decode_bounded(&attachment.common, &store.budget)?;
    let mut columns = common.columns().to_vec();
    columns[common.schema().index_of("object_uri")?] = Arc::new(StringArray::from(vec![
        original["object_uri"].as_str().unwrap(),
    ]));
    attachment.common = STANDARD.encode(encode_batch(&RecordBatch::try_new(
        common.schema(),
        columns,
    )?)?);
    let receipts = store
        .attach_context(&target, std::slice::from_ref(&attachment))
        .await?;
    assert_eq!(
        store
            .attach_context(&target, std::slice::from_ref(&attachment))
            .await?,
        receipts
    );
    let cut = store.publish(&crate::store_tests::cut()).await?;
    assert!(
        receipts[0]
            .common
            .object
            .starts_with("s3://synthetic-bucket/task/case/objects/")
    );
    drop(store);
    fs::remove_dir_all(root.path().join("artifact_objects"))?;
    let cold = open(root.path(), provider.clone()).await?;
    let (rows, total) = cold
        .context_attachments(&context.reference(), &ArtifactSelection::All, 0, 32)
        .await?;
    assert_eq!(total, 1);
    assert_eq!(rows[0].receipt, receipts[0]);
    assert_eq!(rows[0].sha256, digest);
    assert_eq!(
        rows[0].typed_facts["text"]["text_kind"],
        serde_json::json!({"string": "plain"})
    );
    assert_eq!(cold.history("test", "run_a").await?.len(), 0); // unrelated scope stays empty
    cold.verified_cut(&cut).await?;
    // Namespace binding verifies table/id/content digest, not a permissive prefix.
    let mut forged = receipts[0].common.clone();
    forged.object = forged.object.replace(&digest, &"a".repeat(64));
    if forged.object == receipts[0].common.object {
        forged.object += ".foreign";
    }
    assert!(
        cold.read_index_record(&forged, id, "artifact_id")
            .await
            .is_err()
    );
    // Published content corruption fails even though common/typed roots still exist.
    let key = format!("artifact_objects/objects/sha256/{}/{digest}", &digest[..2]);
    provider
        .0
        .lock()
        .unwrap()
        .insert(key, Bytes::from_static(b"different data"));
    assert!(
        cold.context_attachments(&context.reference(), &ArtifactSelection::All, 0, 32)
            .await
            .is_err()
    );
    Ok(())
}
#[tokio::test]
async fn invalid_or_changed_profile_rejects_before_provider_construction() -> Result<()> {
    let root = tempfile::tempdir()?;
    let store = CutStore::open(root.path()).await?;
    drop(store);
    let error = CutStore::open_data(root.path(), Some(profile()), |_| {
        panic!("changed profile reached provider")
    })
    .await
    .err()
    .unwrap();
    assert!(
        error
            .downcast_ref::<bounds::Fault>()
            .is_some_and(|f| f.code == bounds::FaultCode::Conflict)
    );
    let remote_root = root.path().join("remote");
    let store = open(&remote_root, Arc::new(Memory::default())).await?;
    drop(store);
    let mut changed = profile();
    changed.uri = "s3://synthetic-bucket/task/other".into();
    let error = CutStore::open_data(&remote_root, Some(changed), |_| {
        panic!("changed remote profile reached provider")
    })
    .await
    .err()
    .unwrap();
    assert!(
        error
            .downcast_ref::<bounds::Fault>()
            .is_some_and(|f| f.code == bounds::FaultCode::Conflict)
    );
    let missing = root.path().join("not-created");
    let mut invalid = profile();
    invalid.uri = "s3://synthetic-bucket/task/../case".into();
    assert!(
        CutStore::open_data(&missing, Some(invalid), |_| panic!(
            "invalid profile reached provider"
        ))
        .await
        .is_err()
    );
    assert!(!missing.exists());
    Ok(())
}
