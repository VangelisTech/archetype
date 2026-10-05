//! Immutable nonexecuting publication contexts. Preparation binds a scope;
//! only the verified Iceberg root grants published visibility.
use super::*;
use crate::hosted::HostedEvidence;
use arrow_schema::{DataType, Field};
use attachments::{IndexReceipt, physical_batch, text};

const TABLE: &str = "published_contexts_v1";

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ContextOrigin {
    ArtifactCollection,
    Hosted { evidence: Box<HostedEvidence> },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishedContext {
    pub version: u32,
    pub world: String,
    pub run: String,
    pub context_id: String,
    pub origin: ContextOrigin,
}

/// Minted by a verified adapter or the explicit nonexecuting constructor.
/// Callers cannot deserialize unverified hosted evidence into a draft.
pub struct ContextDraft(PublishedContext);
impl ContextDraft {
    pub fn context(&self) -> &PublishedContext {
        &self.0
    }
    pub fn artifact_collection(world: String, run: String) -> Result<Self> {
        Self::new(world, run, ContextOrigin::ArtifactCollection)
    }
    pub(crate) fn hosted(evidence: HostedEvidence) -> Result<Self> {
        Self::new(
            evidence.scope.world.clone(),
            evidence.scope.run.clone(),
            ContextOrigin::Hosted {
                evidence: Box::new(evidence),
            },
        )
    }
    fn new(world: String, run: String, origin: ContextOrigin) -> Result<Self> {
        let mut context = PublishedContext {
            version: 1,
            world,
            run,
            context_id: String::new(),
            origin,
        };
        bounds::page(&context, bounds::Limits::default().metadata_bytes)?;
        context.context_id = context.identity()?;
        context.validate()?;
        Ok(Self(context))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextRef {
    pub world: String,
    pub run: String,
    pub context_id: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExactCut {
    pub tick: u64,
    pub cut_id: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactTarget {
    pub context: ContextRef,
    pub exact_cut: Option<ExactCut>,
}
impl PublishedContext {
    pub fn reference(&self) -> ContextRef {
        ContextRef {
            world: self.world.clone(),
            run: self.run.clone(),
            context_id: self.context_id.clone(),
        }
    }
    fn identity(&self) -> Result<String> {
        crate::hosted::canonical_digest(
            &serde_json::json!({"version":self.version,"world":self.world,"run":self.run,"origin":self.origin}),
        )
    }
    fn validate(&self) -> Result<()> {
        bounds::request(
            crate::identifier(&self.world) && crate::identifier(&self.run),
            "Invalid context scope",
        )?;
        ensure!(
            self.version == 1 && self.context_id == self.identity()?,
            "Context identity mismatch"
        );
        if let ContextOrigin::Hosted { evidence } = &self.origin {
            evidence.validate()?;
            ensure!(
                evidence.scope.world == self.world && evidence.scope.run == self.run,
                "Context evidence scope mismatch"
            );
        }
        Ok(())
    }
    fn check_cut(&self, cut: &FrozenCut) -> Result<()> {
        let ContextOrigin::Hosted { evidence } = &self.origin else {
            anyhow::bail!("Artifact collection cannot acquire simulation cuts");
        };
        evidence.check_cut(cut, true)
    }
}

#[derive(Clone, Copy, Default)]
pub enum ContextFault {
    #[default]
    None,
    AfterPreparation,
    AfterObject,
    AfterRoot,
}

impl CutStore {
    fn context_path(&self, world: &str, run: &str) -> Result<PathBuf> {
        bounds::request(
            crate::identifier(world) && crate::identifier(run),
            "Invalid context scope",
        )?;
        Ok(self
            .root
            .join("contexts")
            .join(format!("{world}.{run}.json")))
    }
    fn prepared_context(&self, world: &str, run: &str) -> Result<Option<PublishedContext>> {
        let path = self.context_path(world, run)?;
        if !path.try_exists()? {
            return Ok(None);
        }
        let context: PublishedContext = serde_json::from_slice(&self.budget.read_metadata(&path)?)?;
        context.validate()?;
        ensure!(
            context.world == world && context.run == run,
            "Prepared context scope mismatch"
        );
        Ok(Some(context))
    }
    async fn context_root(&self, world: &str, run: &str) -> Result<Option<(String, IndexReceipt)>> {
        self.context_path(world, run)?;
        if !self.catalog.table_exists(&ident(TABLE)?).await? {
            return Ok(None);
        }
        let table = self.table(TABLE, context_schema()?).await?;
        let mut found = None;
        if table.metadata().current_snapshot().is_some() {
            for batch in self.scan_metadata(&table).await? {
                ensure!(
                    batch.columns().iter().all(|c| c.null_count() == 0),
                    "Null context root fields"
                );
                for row in 0..batch.num_rows() {
                    self.budget.items(1)?;
                    if strings(&batch, "world")?.value(row) == world
                        && strings(&batch, "run")?.value(row) == run
                    {
                        ensure!(found.is_none(), "Duplicate context scope roots");
                        let id = strings(&batch, "context_id")?.value(row).to_owned();
                        let proof = index_proof(&table, TABLE, &id)?;
                        found = Some((id, proof));
                    }
                }
            }
        }
        Ok(found)
    }
    pub async fn context(&self, reference: &ContextRef) -> Result<PublishedContext> {
        let store = self.read_scope().await?;
        let _guard = store.publication.lock().await;
        let context = store
            .context_inner(&reference.world, &reference.run)
            .await?
            .ok_or_else(|| {
                bounds::fault(
                    bounds::FaultCode::InvalidRequest,
                    "Unknown published context",
                )
            })?;
        ensure!(
            context.context_id == reference.context_id,
            "Context reference mismatch"
        );
        Ok(context)
    }
    pub async fn context_at(&self, world: &str, run: &str) -> Result<PublishedContext> {
        self.lookup_context(world, run).await?.ok_or_else(|| {
            bounds::fault(
                bounds::FaultCode::InvalidRequest,
                "Unknown published context",
            )
        })
    }
    /// Optional published visibility; retained preparation is checked separately
    /// by scope admission and never represented as permission to allocate.
    pub async fn lookup_context(&self, world: &str, run: &str) -> Result<Option<PublishedContext>> {
        let store = self.read_scope().await?;
        let _guard = store.publication.lock().await;
        store.context_inner(world, run).await
    }
    pub(super) async fn context_inner(
        &self,
        world: &str,
        run: &str,
    ) -> Result<Option<PublishedContext>> {
        let Some((id, proof)) = self.context_root(world, run).await? else {
            return Ok(None);
        };
        let batch = self.read_index_record(&proof, &id, "context_id").await?;
        let bytes = self.budget.read_metadata(&self.context_path(world, run)?)?;
        let context: PublishedContext = serde_json::from_slice(&bytes)?;
        context.validate()?;
        ensure!(
            context.world == world && context.run == run && context.context_id == id,
            "Published context scope mismatch"
        );
        ensure!(
            text(&batch, "descriptor_json")?.as_bytes() == bytes,
            "Context preparation changed"
        );
        ensure!(
            crate::hash(&encode_batch(
                &physical_batch(&context_batch(&context)?)?.1
            )?) == proof.object_sha256,
            "Context root differs from descriptor"
        );
        Ok(Some(context))
    }
    pub(super) async fn context_claim(
        &self,
        world: &str,
        run: &str,
    ) -> Result<Option<PublishedContext>> {
        if let Some(context) = self.context_inner(world, run).await? {
            return Ok(Some(context));
        }
        self.prepared_context(world, run)
    }
    /// Reject an already claimed scope before a host asks its native owner to
    /// allocate a world. Publication still rechecks compatibility atomically;
    /// this preflight neither reserves the scope nor substitutes for that check.
    pub async fn preflight_unclaimed_scope(&self, world: &str, run: &str) -> Result<()> {
        origin::Scope {
            world: world.into(),
            run: run.into(),
        }
        .validate()?;
        let store = self.read_scope().await?;
        ensure!(
            store.context_claim(world, run).await?.is_none(),
            "Scope already has a context claim"
        );
        ensure!(
            store.origin(world, run)?.is_none(),
            "Scope already has a fork origin"
        );
        ensure!(
            store.history_inner(world, run).await?.is_empty(),
            "Scope already has cuts"
        );
        let prefix = format!("{world}.{run}.");
        for entry in fs::read_dir(store.root.join("cuts"))? {
            store.budget.items(1)?;
            ensure!(
                !entry?.file_name().to_string_lossy().starts_with(&prefix),
                "Scope already has pending publication"
            );
        }
        Ok(())
    }
    pub(crate) async fn check_context_cut(&self, cut: &FrozenCut) -> Result<()> {
        if let Some(context) = self.context_claim(&cut.world, &cut.run).await? {
            context.check_cut(cut)?;
        }
        Ok(())
    }
    pub(crate) async fn check_context_origin(&self, origin: &origin::ForkOrigin) -> Result<()> {
        let scope = origin.destination()?;
        if let Some(context) = self.context_claim(&scope.world, &scope.run).await? {
            let ContextOrigin::Hosted { evidence } = context.origin else {
                anyhow::bail!("Artifact collection cannot acquire a fork origin");
            };
            ensure!(
                evidence.scope.native_world == origin.reservation.child_world_id,
                "Context belongs to another native child"
            );
            evidence.check_cut(&self.verified_cut(&origin.source).await?, false)?;
        }
        Ok(())
    }
    pub async fn publish_context(&self, draft: &ContextDraft) -> Result<PublishedContext> {
        self.publish_context_with_fault(draft, ContextFault::None)
            .await
    }
    pub async fn publish_context_with_fault(
        &self,
        draft: &ContextDraft,
        fault: ContextFault,
    ) -> Result<PublishedContext> {
        let store = self.read_scope().await?;
        let _guard = store.publication.lock().await;
        let context = &draft.0;
        bounds::page(context, store.budget.limits.metadata_bytes)?;
        context.validate()?;
        if let Some(existing) = store.context_claim(&context.world, &context.run).await? {
            ensure!(
                existing == *context,
                "Context scope already binds a different descriptor"
            );
        }
        if let Some(origin) = store.origin(&context.world, &context.run)? {
            let ContextOrigin::Hosted { evidence } = &context.origin else {
                anyhow::bail!("Artifact collection scope already has a fork origin");
            };
            ensure!(
                evidence.scope.native_world == origin.reservation.child_world_id,
                "Context belongs to another native child"
            );
            evidence.check_cut(&store.verified_cut(&origin.source).await?, false)?;
        }
        let prefix = format!("{}.{}.", context.world, context.run);
        for entry in fs::read_dir(store.root.join("cuts"))? {
            store.budget.items(1)?;
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with(&prefix) && name.ends_with(".json") {
                let tick: u64 = name[prefix.len()..name.len() - 5].parse()?;
                context.check_cut(&store.load_frozen(&context.world, &context.run, tick)?)?;
            }
        }
        // A visible cut with lost journal must fail, not look like empty scope.
        for cut in store.history_inner(&context.world, &context.run).await? {
            let frozen = store.verified_cut(&cut).await?;
            if cut.world == context.world && cut.run == context.run {
                context.check_cut(&frozen)?;
            }
        }
        let bytes = bounds::encode_metadata(context, store.budget.limits.metadata_bytes)?;
        let (schema, batch) = physical_batch(&context_batch(context)?)?;
        let root_bytes = encode_batch(&batch)?;
        preflight::parquet(root_bytes.clone().into(), &store.budget)?;
        let path = store.context_path(&context.world, &context.run)?;
        immutable(&path, &bytes)?;
        File::open(&path)?.sync_all()?;
        ensure!(
            !matches!(fault, ContextFault::AfterPreparation),
            "Injected failure after context preparation"
        );
        let table = store.table(TABLE, schema).await?;
        let object = store.stage_bytes(TABLE, &context.context_id, &root_bytes)?;
        ensure!(
            !matches!(fault, ContextFault::AfterObject),
            "Injected failure after context object"
        );
        store
            .register(&table, TABLE, &context.context_id, &object, 1)
            .await?;
        ensure!(
            !matches!(fault, ContextFault::AfterRoot),
            "Injected failure after context root"
        );
        let published = store
            .context_inner(&context.world, &context.run)
            .await?
            .ok_or_else(|| anyhow!("Context root missing after publication"))?;
        ensure!(published == *context, "Published context changed");
        Ok(published)
    }
    pub(super) async fn verify_target_inner(
        &self,
        target: &ArtifactTarget,
    ) -> Result<PublishedContext> {
        let context = self
            .context_inner(&target.context.world, &target.context.run)
            .await?
            .ok_or_else(|| anyhow!("Unknown published context"))?;
        ensure!(
            context.reference() == target.context,
            "Context reference mismatch"
        );
        if let Some(exact) = &target.exact_cut {
            let ContextOrigin::Hosted { evidence } = &context.origin else {
                anyhow::bail!("Artifact collection has no simulation cuts");
            };
            bounds::request(
                exact.tick > 0 && exact.tick <= i64::MAX as u64,
                "Invalid exact cut tick",
            )?;
            let receipt = self
                .history_inner(&context.world, &context.run)
                .await?
                .into_iter()
                .find(|c| c.tick == exact.tick && c.cut_id == exact.cut_id)
                .ok_or_else(|| anyhow!("Cut outside context lineage"))?;
            let cut = self.verified_cut(&receipt).await?;
            evidence.check_cut(
                &cut,
                receipt.world == context.world && receipt.run == context.run,
            )?;
        }
        Ok(context)
    }
    pub async fn context_artifact_root(&self, target: &ArtifactTarget) -> Result<PathBuf> {
        let store = self.read_scope().await?;
        let _guard = store.publication.lock().await;
        store.verify_target_inner(target).await?;
        Ok(store.root.join("artifact_objects"))
    }
}

fn context_batch(context: &PublishedContext) -> Result<RecordBatch> {
    let values = [
        ("context_id", context.context_id.clone()),
        ("world", context.world.clone()),
        ("run", context.run.clone()),
        ("descriptor_json", serde_json::to_string(context)?),
    ];
    let fields = values
        .iter()
        .map(|(name, _)| Field::new(*name, DataType::Utf8, false))
        .collect::<Vec<_>>();
    let columns = values
        .into_iter()
        .map(|(_, value)| Arc::new(StringArray::from(vec![value])) as ArrayRef)
        .collect();
    Ok(RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::new(fields)),
        columns,
    )?)
}
fn context_schema() -> Result<Schema> {
    let fields = ["context_id", "world", "run", "descriptor_json"]
        .into_iter()
        .map(|name| Field::new(name, DataType::Utf8, false))
        .collect::<Vec<_>>();
    Ok(iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(
        &arrow_schema::Schema::new(fields),
    )?)
}
pub(super) fn index_proof(table: &Table, name: &str, id: &str) -> Result<IndexReceipt> {
    let snapshot =
        find_snapshot(table, id)?.ok_or_else(|| anyhow!("Missing published root snapshot"))?;
    let props = &table
        .metadata()
        .snapshot_by_id(snapshot)
        .unwrap()
        .summary()
        .additional_properties;
    Ok(IndexReceipt {
        table: name.into(),
        table_uuid: table.metadata().uuid().to_string(),
        snapshot,
        object: props
            .get("archetype.object")
            .ok_or_else(|| anyhow!("Missing root object"))?
            .clone(),
        object_sha256: props
            .get("archetype.object-sha256")
            .ok_or_else(|| anyhow!("Missing root digest"))?
            .clone(),
        schema_sha256: crate::digest(table.metadata().current_schema().as_ref())?,
    })
}
