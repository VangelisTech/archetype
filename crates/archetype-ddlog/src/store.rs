//! Local, cooperative single-owner publication. Component table appends are
//! individually atomic. Only the final cut table grants world visibility.
use std::{
    collections::{BTreeMap, HashMap},
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Result, anyhow, ensure};
use arrow_array::{Array, ArrayRef, RecordBatch, StringArray};
use futures::TryStreamExt;
use iceberg::{
    Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent,
    arrow::schema_to_arrow_schema,
    spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, FormatVersion, ManifestStatus,
        NestedField, PrimitiveType, Schema, Type,
    },
    table::Table,
    transaction::{ApplyTransactionAction, Transaction},
};
use iceberg_catalog_sql::{SqlBindStyle, SqlCatalogBuilder};
use parquet::arrow::ArrowWriter;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::{component::ComponentSchema, world::FrozenCut};

pub mod attachments;
mod bounded_io;
#[cfg(test)]
mod bounded_reads;
pub mod bounds;
pub mod context_attachments;
#[cfg(test)]
mod context_tests;
pub mod contexts;
pub mod origin;
mod preflight;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableCut {
    pub table: String,
    pub table_uuid: String,
    pub snapshot: Option<i64>,
    pub object: Option<String>,
    pub object_sha256: Option<String>,
    pub rows: usize,
    pub schema: ComponentSchema,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CutReceipt {
    pub cut_id: String,
    pub world: String,
    pub run: String,
    pub tick: u64,
    pub program: String,
    pub parent: Option<String>,
    pub checkpoint_sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub frozen_manifest_sha256: Option<String>,
    pub components: BTreeMap<String, TableCut>,
}

/// Fault seam at publication boundaries, used by deterministic storage tests.
/// Production callers use `publish`, with no injected fault.
#[derive(Clone, Copy, Default)]
pub enum PublicationFault {
    #[default]
    None,
    AfterComponents,
    AfterManifest,
}

#[derive(Clone)]
pub struct CutStore {
    root: PathBuf,
    catalog: Arc<dyn Catalog>,
    _owner: Arc<File>,
    publication: Arc<Mutex<()>>,
    budget: Arc<bounds::Budget>,
    scoped: bool,
}

impl CutStore {
    pub async fn open(root: &Path) -> Result<Self> {
        fs::create_dir_all(root)?;
        let root = root.canonicalize()?;
        let owner = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.join("owner.lock"))?;
        owner
            .try_lock()
            .map_err(|e| anyhow!("Store already owned: {e}"))?;
        for dir in ["warehouse", "objects", "cuts", "origins", "contexts"] {
            fs::create_dir_all(root.join(dir))?;
        }
        File::open(&root)?.sync_all()?;
        File::open(root.parent().unwrap())?.sync_all()?;
        let budget = bounds::Budget::new(bounds::Limits::default());
        let catalog = Self::catalog(&root, budget.clone()).await?;
        let namespace = NamespaceIdent::new("archetype".into());
        if !catalog.namespace_exists(&namespace).await? {
            catalog.create_namespace(&namespace, HashMap::new()).await?;
        }
        let store = Self {
            root,
            catalog,
            _owner: Arc::new(owner),
            publication: Arc::new(Mutex::new(())),
            budget,
            scoped: false,
        };
        store.table("cuts", cut_schema()?).await?;
        Ok(store)
    }

    async fn catalog(root: &Path, budget: Arc<bounds::Budget>) -> Result<Arc<dyn Catalog>> {
        let catalog = SqlCatalogBuilder::default()
            .uri(format!(
                "sqlite://{}?mode=rwc",
                root.join("catalog.sqlite").display()
            ))
            .warehouse_location(root.join("warehouse").to_string_lossy().into_owned())
            .sql_bind_style(SqlBindStyle::QMark)
            .prop("pool.max-connections", "1")
            .with_storage_factory(Arc::new(bounded_io::BoundedIo {
                root: root.to_path_buf(),
                budget,
            }))
            .load("archetype", HashMap::new())
            .await?;
        Ok(Arc::new(catalog))
    }

    /// A new client for the same physical catalog, borrowing the same exclusive
    /// owner and publication lock. Its FileIO budget remains bound to spawned IO.
    pub async fn read_scope(&self) -> Result<Self> {
        if self.scoped {
            return Ok(self.clone());
        }
        let budget = bounds::Budget::new(self.budget.limits.clone());
        Ok(Self {
            catalog: Self::catalog(&self.root, budget.clone()).await?,
            budget,
            scoped: true,
            ..self.clone()
        })
    }

    async fn table(&self, name: &str, schema: Schema) -> Result<Table> {
        let ident = ident(name)?;
        let table = if self.catalog.table_exists(&ident).await? {
            self.catalog.load_table(&ident).await?
        } else {
            self.catalog
                .create_table(
                    &NamespaceIdent::new("archetype".into()),
                    TableCreation::builder()
                        .name(name.into())
                        .location(
                            self.root
                                .join("warehouse")
                                .join(name)
                                .to_string_lossy()
                                .into_owned(),
                        )
                        .schema(schema.clone())
                        .format_version(FormatVersion::V3)
                        .properties(HashMap::from([(
                            "commit.retry.num-retries".into(),
                            "0".into(),
                        )]))
                        .build(),
                )
                .await?
        };
        ensure!(
            table.metadata().current_schema().as_ref() == &schema
                && table.metadata().default_partition_spec().is_unpartitioned()
                && table.metadata().format_version() == FormatVersion::V3,
            "Table schema/format mismatch"
        );
        ensure!(
            table
                .metadata()
                .properties()
                .get("commit.retry.num-retries")
                .map(String::as_str)
                == Some("0"),
            "Catalog retries must be disabled"
        );
        Ok(table)
    }

    pub async fn publish(&self, cut: &FrozenCut) -> Result<CutReceipt> {
        self.publish_with_fault(cut, PublicationFault::None).await
    }

    pub async fn publish_with_fault(
        &self,
        cut: &FrozenCut,
        fault: PublicationFault,
    ) -> Result<CutReceipt> {
        self.read_scope().await?.publish_inner(cut, fault).await
    }

    async fn publish_inner(&self, cut: &FrozenCut, fault: PublicationFault) -> Result<CutReceipt> {
        let _guard = self.publication.lock().await;
        #[derive(Serialize)]
        struct Journal<'a> {
            cut: &'a FrozenCut,
            sha256: &'a str,
        }
        // Count the exact envelope first: identity serializes the cut and
        // validation parses its checkpoint. Neither may precede admission.
        bounds::page(
            &Journal {
                cut,
                sha256: "0000000000000000000000000000000000000000000000000000000000000000",
            },
            self.budget.limits.metadata_bytes,
        )?;
        cut.validate()?;
        self.check_context_cut(cut).await?;
        let cut_id = cut.identity()?;
        let journal = bounds::encode_metadata(
            &Journal {
                cut,
                sha256: &cut_id,
            },
            self.budget.limits.metadata_bytes,
        )?;
        preflight::json(&journal, &self.budget)?;
        // Reject a cut whose own local v1 outputs cannot be read before any
        // durable publication intent or component commit is created.
        for relation in cut.relations.values() {
            if !relation.rows.is_empty() {
                let bytes = encode_batch(&relation.schema.batch(&cut_id, &relation.rows)?)?;
                bounds::cap(
                    bytes.len() as u64,
                    self.budget.limits.file_bytes,
                    "Component object bytes",
                )?;
                preflight::parquet(bytes.into(), &self.budget)?;
            }
        }
        let history = self.history(&cut.world, &cut.run).await?;
        if let Some(origin) = self.origin(&cut.world, &cut.run)? {
            ensure!(
                cut.hosted
                    .as_ref()
                    .is_some_and(|m| m.key.world_id == origin.reservation.child_world_id)
                    && cut.program == origin.source.program,
                "Cut does not own fork scope"
            );
        }
        if let Some(existing) = history.iter().find(|r| r.tick == cut.tick) {
            ensure!(
                existing.cut_id == cut_id,
                "Different cut already occupies this tick"
            );
            self.verify_cut(existing).await?;
            return Ok(existing.clone());
        }
        ensure!(
            Some(cut.tick) == history.last().map_or(Some(1), |r| r.tick.checked_add(1))
                && cut.parent == history.last().map(|r| r.cut_id.clone()),
            "Cut is not the next published tick"
        );
        // Persist frozen outputs AND DDlog checkpoint before any table commit.
        // A different payload cannot steal the same world/run/tick on retry.
        immutable(&self.journal(&cut.world, &cut.run, cut.tick), &journal)?;
        let mut components = BTreeMap::new();
        for (name, relation) in &cut.relations {
            let table_name = format!("component_{}", relation.schema.identity()?);
            let table = self
                .table(&table_name, relation.schema.iceberg_schema()?)
                .await?;
            let mut result = TableCut {
                table: table_name.clone(),
                table_uuid: table.metadata().uuid().to_string(),
                snapshot: None,
                object: None,
                object_sha256: None,
                rows: relation.rows.len(),
                schema: relation.schema.clone(),
            };
            if !relation.rows.is_empty() {
                let batch = relation.schema.batch(&cut_id, &relation.rows)?;
                let object = self.stage_batch(&table_name, &cut_id, &batch)?;
                result.snapshot = Some(
                    self.register(&table, &table_name, &cut_id, &object, batch.num_rows())
                        .await?,
                );
                result.object = Some(object.0);
                result.object_sha256 = Some(object.1);
            }
            // Empty outputs are an explicit zero-row inventory entry, never an
            // invitation to read a previous table snapshot (which resurrects data).
            components.insert(name.clone(), result);
        }
        ensure!(
            !matches!(fault, PublicationFault::AfterComponents),
            "Injected failure after component registration"
        );
        let receipt = CutReceipt {
            cut_id: cut_id.clone(),
            world: cut.world.clone(),
            run: cut.run.clone(),
            tick: cut.tick,
            program: cut.program.clone(),
            parent: cut.parent.clone(),
            checkpoint_sha256: crate::hash(&cut.checkpoint),
            frozen_manifest_sha256: cut
                .hosted
                .as_ref()
                .map(crate::hosted::canonical_digest)
                .transpose()?,
            components,
        };
        let table = self.table("cuts", cut_schema()?).await?;
        let receipt_json = serde_json::to_string(&receipt)?;
        let batch = RecordBatch::try_new(
            Arc::new(schema_to_arrow_schema(&cut_schema()?)?),
            vec![
                Arc::new(StringArray::from(vec![cut_id.as_str()])) as ArrayRef,
                Arc::new(StringArray::from(vec![receipt_json.as_str()])),
            ],
        )?;
        let object = self.stage_batch("cuts", &cut_id, &batch)?;
        self.register(&table, "cuts", &cut_id, &object, 1).await?;
        ensure!(
            !matches!(fault, PublicationFault::AfterManifest),
            "Injected lost acknowledgement after manifest publication"
        );
        self.verify_cut(&receipt).await?;
        Ok(receipt)
    }

    fn journal(&self, world: &str, run: &str, tick: u64) -> PathBuf {
        self.root
            .join("cuts")
            .join(format!("{world}.{run}.{tick}.json"))
    }

    /// Recover publication after a process restart without executing DDlog.
    pub async fn retry(&self, world: &str, run: &str, tick: u64) -> Result<CutReceipt> {
        self.read_scope().await?.retry_inner(world, run, tick).await
    }
    async fn retry_inner(&self, world: &str, run: &str, tick: u64) -> Result<CutReceipt> {
        ensure!(
            crate::identifier(world) && crate::identifier(run),
            "Invalid world/run"
        );
        let cut = self.load_frozen(world, run, tick)?;
        ensure!(
            cut.world == world && cut.run == run && cut.tick == tick,
            "Journal identity mismatch"
        );
        self.publish(&cut).await
    }

    /// Recovery payload is separate from analytical tables and keeps its exact
    /// upstream managed checkpoint envelope. No rows reconstruct native inputs.
    pub async fn checkpoint(&self, receipt: &CutReceipt) -> Result<Vec<u8>> {
        self.read_scope().await?.checkpoint_inner(receipt).await
    }
    async fn checkpoint_inner(&self, receipt: &CutReceipt) -> Result<Vec<u8>> {
        self.require_visible(receipt).await?;
        let cut = self.load_frozen(&receipt.world, &receipt.run, receipt.tick)?;
        validate_receipt(receipt, &cut)?;
        Ok(cut.checkpoint)
    }

    pub(crate) fn load_frozen(&self, world: &str, run: &str, tick: u64) -> Result<FrozenCut> {
        ensure!(
            crate::identifier(world) && crate::identifier(run),
            "Invalid world/run"
        );
        let cut = FrozenCut::decode_journal(
            &self.budget.read_metadata(&self.journal(world, run, tick))?,
        )?;
        ensure!(
            cut.world == world && cut.run == run && cut.tick == tick,
            "Journal attribution mismatch"
        );
        Ok(cut)
    }

    /// Complete journal, final-manifest and pinned component verification.
    pub(crate) async fn verified_cut(&self, receipt: &CutReceipt) -> Result<FrozenCut> {
        let scope = self.read_scope().await?;
        scope.verify_cut(receipt).await?;
        scope.load_frozen(&receipt.world, &receipt.run, receipt.tick)
    }

    pub async fn history(&self, world: &str, run: &str) -> Result<Vec<CutReceipt>> {
        self.read_scope().await?.history_inner(world, run).await
    }
    pub async fn history_page(
        &self,
        world: &str,
        run: &str,
        offset: usize,
        limit: usize,
    ) -> Result<(Vec<CutReceipt>, usize)> {
        bounds::request((1..=100).contains(&limit), "History limit must be 1..100")?;
        let scope = self.read_scope().await?;
        let history = scope.history_inner(world, run).await?;
        bounds::request(offset <= history.len(), "Offset past history")?;
        let end = offset.saturating_add(limit).min(history.len());
        bounds::page(&history[offset..end], scope.budget.limits.page_bytes)?;
        Ok((history[offset..end].to_vec(), history.len()))
    }
    async fn history_inner(&self, world: &str, run: &str) -> Result<Vec<CutReceipt>> {
        bounds::request(
            crate::identifier(world) && crate::identifier(run),
            "Invalid world/run",
        )?;
        let table = self.catalog.load_table(&ident("cuts")?).await?;
        let mut receipts: Vec<CutReceipt> = vec![];
        for batch in self.scan_metadata(&table).await? {
            let ids = strings(&batch, "cut_id")?;
            let values = strings(&batch, "receipt_json")?;
            for i in 0..batch.num_rows() {
                ensure!(
                    !ids.is_null(i) && !values.is_null(i),
                    "Invalid manifest row"
                );
                bounds::cap(
                    values.value(i).len() as u64,
                    self.budget.limits.metadata_bytes,
                    "Receipt bytes",
                )?;
                preflight::json(values.value(i).as_bytes(), &self.budget)?;
                self.budget.items(1)?;
                let receipt: CutReceipt = serde_json::from_str(values.value(i))
                    .map_err(|e| bounds::fault(bounds::FaultCode::CorruptData, e.to_string()))?;
                ensure!(receipt.cut_id == ids.value(i), "Manifest identity mismatch");
                receipts.push(receipt);
            }
        }
        self.resolve_history(world, run, receipts)
    }

    async fn require_visible(&self, receipt: &CutReceipt) -> Result<()> {
        ensure!(
            crate::identifier(&receipt.world) && crate::identifier(&receipt.run),
            "Invalid world/run"
        );
        ensure!(
            self.history(&receipt.world, &receipt.run)
                .await?
                .iter()
                .any(|r| r == receipt),
            "Cut is not catalog-visible"
        );
        Ok(())
    }

    async fn verify_cut(&self, receipt: &CutReceipt) -> Result<()> {
        self.require_visible(receipt).await?;
        let cut = self.load_frozen(&receipt.world, &receipt.run, receipt.tick)?;
        validate_receipt(receipt, &cut)?;
        for (name, selected) in &receipt.components {
            self.read(receipt, name).await?;
            if selected.rows > 0 {
                let batch = cut.relations[name]
                    .schema
                    .batch(&receipt.cut_id, &cut.relations[name].rows)?;
                ensure!(
                    selected.object_sha256.as_deref() == Some(&crate::hash(&encode_batch(&batch)?)),
                    "Component object differs from frozen journal"
                );
            }
        }
        Ok(())
    }

    /// Sanctioned read path: validate the final manifest, pin each relation's
    /// snapshot, then select exactly this full cut. Never use latest-row-wins.
    pub async fn read(&self, receipt: &CutReceipt, component: &str) -> Result<Vec<RecordBatch>> {
        self.read_scope()
            .await?
            .read_inner(receipt, component, 0, None)
            .await
    }

    /// Decode only the requested rows of the exact hash-verified full-cut object.
    /// Physical proof reads and structural admission still consume one scope.
    pub async fn read_page(
        &self,
        receipt: &CutReceipt,
        component: &str,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<RecordBatch>> {
        bounds::request((1..=1000).contains(&limit), "Read limit must be 1..1000")?;
        self.read_scope()
            .await?
            .read_inner(receipt, component, offset, Some(limit))
            .await
    }

    async fn read_inner(
        &self,
        receipt: &CutReceipt,
        component: &str,
        offset: usize,
        limit: Option<usize>,
    ) -> Result<Vec<RecordBatch>> {
        self.require_visible(receipt).await?;
        let selected = receipt.components.get(component).ok_or_else(|| {
            bounds::fault(
                bounds::FaultCode::InvalidRequest,
                "Component absent from cut",
            )
        })?;
        bounds::request(offset <= selected.rows, "Offset past component")?;
        bounds::cap(
            selected.rows as u64,
            self.budget.limits.rows,
            "Selected rows",
        )?;
        let table = self.catalog.load_table(&ident(&selected.table)?).await?;
        ensure!(
            table.metadata().uuid().to_string() == selected.table_uuid
                && table.metadata().current_schema().as_ref()
                    == &selected.schema.iceberg_schema()?,
            "Component table identity changed"
        );
        if selected.rows == 0 {
            ensure!(
                selected.snapshot.is_none()
                    && selected.object.is_none()
                    && selected.object_sha256.is_none(),
                "Invalid empty inventory"
            );
            return Ok(vec![selected.schema.batch(&receipt.cut_id, &[])?]);
        }
        let snapshot = selected
            .snapshot
            .ok_or_else(|| anyhow!("Missing snapshot"))?;
        let object = selected
            .object
            .as_ref()
            .ok_or_else(|| anyhow!("Missing object"))?;
        let digest = selected
            .object_sha256
            .as_ref()
            .ok_or_else(|| anyhow!("Missing object digest"))?;
        let bytes = self
            .verify_snapshot(
                &table,
                snapshot,
                &receipt.cut_id,
                &(object.clone(), digest.clone()),
                selected.rows,
            )
            .await?;
        self.budget.decoding()?;
        let count = limit.unwrap_or(selected.rows).min(selected.rows - offset);
        let scan = decode_object(bytes, offset, count, &self.budget)?;
        let mut batches = vec![];
        let mut rows = 0;
        let mut page_bytes = 0u64;
        for batch in scan {
            page_bytes = page_bytes
                .checked_add(batch.get_array_memory_size() as u64)
                .ok_or_else(|| {
                    bounds::fault(bounds::FaultCode::ResourceLimit, "Page size overflow")
                })?;
            if limit.is_some() {
                bounds::cap(
                    page_bytes,
                    self.budget.limits.page_bytes,
                    "Decoded page bytes",
                )?;
            }
            let ids = strings(&batch, "cut_id")?;
            bounds::corrupt(
                ids.iter().all(|id| id == Some(receipt.cut_id.as_str())),
                "Object contains another cut",
            )?;
            rows += batch.num_rows();
            batches.push(batch);
        }
        bounds::corrupt(rows == count, "Published row inventory mismatch")?;
        Ok(batches)
    }

    fn stage_batch(
        &self,
        table: &str,
        cut_id: &str,
        batch: &RecordBatch,
    ) -> Result<(String, String)> {
        let bytes = encode_batch(batch)?;
        self.stage_bytes(table, cut_id, &bytes)
    }
    fn stage_bytes(&self, table: &str, cut_id: &str, bytes: &[u8]) -> Result<(String, String)> {
        let digest = crate::hash(bytes);
        let path = self
            .root
            .join("objects")
            .join(format!("{table}.{cut_id}.{digest}.parquet"));
        immutable(&path, bytes)?;
        Ok((path.to_string_lossy().into_owned(), digest))
    }

    /// Local append-only metadata tables. File planning remains Iceberg-owned;
    /// each admitted file is decoded sequentially with the same operation budget.
    async fn scan_metadata(&self, table: &Table) -> Result<Vec<RecordBatch>> {
        let mut tasks = table
            .scan()
            .with_concurrency_limit(1)
            .with_manifest_entry_concurrency_limit(1)
            .build()?
            .plan_files()
            .await?;
        let mut batches = Vec::new();
        while let Some(task) = tasks.try_next().await? {
            self.budget.items(1)?;
            bounds::corrupt(
                task.start() == 0
                    && task.length() == task.file_size_in_bytes()
                    && task.deletes().is_empty()
                    && task.data_file_format() == DataFileFormat::Parquet,
                "Unsupported local metadata scan task",
            )?;
            bounds::cap(
                task.file_size_in_bytes(),
                self.budget.limits.file_bytes,
                "Planned file bytes",
            )?;
            let rows = task.record_count().ok_or_else(|| {
                bounds::fault(bounds::FaultCode::CorruptData, "Missing scan row count")
            })?;
            bounds::cap(rows, self.budget.limits.rows, "Planned file rows")?;
            let bytes = table
                .file_io()
                .new_input(task.data_file_path())?
                .read()
                .await?;
            let builder = preflight::builder(bytes.clone())?;
            bounds::corrupt(
                builder.metadata().file_metadata().num_rows() as u64 == rows
                    && bytes.len() as u64 == task.file_size_in_bytes(),
                "Scan inventory mismatch",
            )?;
            bounds::corrupt(
                builder.schema().as_ref()
                    == &schema_to_arrow_schema(table.metadata().current_schema())?,
                "Scan schema changed",
            )?;
            let decoded = decode_object(bytes, 0, rows as usize, &self.budget)?;
            bounds::corrupt(
                decoded.iter().map(RecordBatch::num_rows).sum::<usize>() == rows as usize,
                "Decoded scan inventory mismatch",
            )?;
            batches.extend(decoded);
        }
        Ok(batches)
    }

    async fn register(
        &self,
        table: &Table,
        name: &str,
        cut_id: &str,
        object: &(String, String),
        rows: usize,
    ) -> Result<i64> {
        if let Some(snapshot) = find_snapshot(table, cut_id)? {
            self.verify_snapshot(table, snapshot, cut_id, object, rows)
                .await?;
            return Ok(snapshot);
        }
        let bytes = table.file_io().new_input(&object.0)?.read().await?;
        ensure!(crate::hash(&bytes) == object.1, "Staged object changed");
        let descriptor = descriptor(table, &object.0, bytes.len(), rows, None)?;
        let tx = Transaction::new(table);
        let outcome = tx
            .fast_append()
            .with_check_duplicate(true)
            .add_data_files(vec![descriptor])
            .set_snapshot_properties(HashMap::from([
                ("archetype.cut-id".into(), cut_id.into()),
                ("archetype.object-sha256".into(), object.1.clone()),
                ("archetype.object".into(), object.0.clone()),
            ]))
            .apply(tx)?
            .commit(self.catalog.as_ref())
            .await;
        // A failed response is uncertain. Fresh exact readback may adopt it;
        // otherwise the durable journal supplies precisely the same next retry.
        let fresh = self.catalog.load_table(&ident(name)?).await?;
        if let Some(snapshot) = find_snapshot(&fresh, cut_id)? {
            self.verify_snapshot(&fresh, snapshot, cut_id, object, rows)
                .await?;
            return Ok(snapshot);
        }
        Err(anyhow!(
            "Unconfirmed Iceberg publication; retain cut journal: {:?}",
            outcome.err()
        ))
    }

    async fn verify_snapshot(
        &self,
        table: &Table,
        snapshot_id: i64,
        cut_id: &str,
        object: &(String, String),
        rows: usize,
    ) -> Result<bytes::Bytes> {
        let snapshot = table
            .metadata()
            .snapshot_by_id(snapshot_id)
            .ok_or_else(|| anyhow!("Pinned snapshot no longer retained"))?;
        let properties = &snapshot.summary().additional_properties;
        ensure!(
            properties.get("archetype.cut-id").map(String::as_str) == Some(cut_id)
                && properties.get("archetype.object-sha256") == Some(&object.1)
                && properties.get("archetype.object") == Some(&object.0),
            "Snapshot receipt mismatch"
        );
        let bytes = table.file_io().new_input(&object.0)?.read().await?;
        ensure!(crate::hash(&bytes) == object.1, "Immutable object changed");
        let builder = preflight::builder(bytes.clone())?;
        ensure!(
            builder.metadata().file_metadata().num_rows() == rows as i64,
            "Parquet row count mismatch"
        );
        ensure!(
            builder.schema().as_ref()
                == &schema_to_arrow_schema(table.metadata().current_schema())?,
            "Parquet schema/field IDs mismatch: actual {:?}, expected {:?}",
            builder.schema(),
            schema_to_arrow_schema(table.metadata().current_schema())?
        );
        let mut added = vec![];
        let manifests = table.manifest_list_reader(snapshot).load().await?;
        for manifest in manifests.entries() {
            for entry in table.manifest_reader().read(manifest).await?.entries() {
                if entry.status == ManifestStatus::Added && entry.snapshot_id() == Some(snapshot_id)
                {
                    added.push(entry.data_file.clone());
                }
            }
        }
        let row_id = snapshot
            .first_row_id()
            .map(i64::try_from)
            .transpose()?
            .ok_or_else(|| anyhow!("Missing v3 row allocation"))?;
        ensure!(
            added
                == vec![descriptor(
                    table,
                    &object.0,
                    bytes.len(),
                    rows,
                    Some(row_id)
                )?],
            "Snapshot contains a different added file set"
        );
        Ok(bytes)
    }
}

fn decode_object(
    bytes: bytes::Bytes,
    offset: usize,
    count: usize,
    budget: &bounds::Budget,
) -> Result<Vec<RecordBatch>> {
    budget.decoding()?;
    budget.decoded_rows(count as u64)?;
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        preflight::builder(bytes)?
            .with_offset(offset)
            .with_limit(count)
            .with_batch_size(count.clamp(1, 1000))
            .build()?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(anyhow::Error::from)
    }))
    .map_err(|_| {
        bounds::fault(
            bounds::FaultCode::CorruptData,
            "Parquet value decoder panicked",
        )
    })?
    .map_err(|e| {
        bounds::fault(
            bounds::FaultCode::CorruptData,
            format!("Parquet value decode: {e}"),
        )
    })
}

fn ident(name: &str) -> Result<TableIdent> {
    Ok(TableIdent::from_strs(["archetype", name])?)
}
fn strings<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StringArray> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| anyhow!("Invalid {name} column"))
}
fn cut_schema() -> Result<Schema> {
    Ok(Schema::builder()
        .with_fields([
            Arc::new(NestedField::required(
                1,
                "cut_id",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::required(
                2,
                "receipt_json",
                Type::Primitive(PrimitiveType::String),
            )),
        ])
        .build()?)
}
fn find_snapshot(table: &Table, cut_id: &str) -> Result<Option<i64>> {
    let matches: Vec<_> = table
        .metadata()
        .snapshots()
        .filter(|s| {
            s.summary()
                .additional_properties
                .get("archetype.cut-id")
                .map(String::as_str)
                == Some(cut_id)
        })
        .map(|s| s.snapshot_id())
        .collect();
    ensure!(matches.len() <= 1, "Ambiguous duplicate cut snapshots");
    Ok(matches.first().copied())
}
fn descriptor(
    table: &Table,
    path: &str,
    size: usize,
    rows: usize,
    first_row_id: Option<i64>,
) -> Result<DataFile> {
    Ok(DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(path.into())
        .file_format(DataFileFormat::Parquet)
        .record_count(rows as u64)
        .file_size_in_bytes(size as u64)
        .first_row_id(first_row_id)
        .partition_spec_id(table.metadata().default_partition_spec_id())
        .build()?)
}
fn immutable(path: &Path, bytes: &[u8]) -> Result<()> {
    // Publish a fully fsynced temp inode with a no-clobber hard link. A crash
    // cannot leave a truncated file at the durable identity's canonical path.
    if path.exists() {
        let mut existing = File::open(path)?;
        bounds::corrupt(
            existing.metadata()?.len() == bytes.len() as u64,
            "Immutable object length changed",
        )?;
        let mut offset = 0;
        let mut block = [0; 65536];
        while offset < bytes.len() {
            let n = block.len().min(bytes.len() - offset);
            existing.read_exact(&mut block[..n])?;
            bounds::corrupt(
                block[..n] == bytes[offset..offset + n],
                "Immutable object identity reused with different bytes",
            )?;
            offset += n;
        }
        bounds::corrupt(
            existing.read(&mut block[..1])? == 0,
            "Immutable object grew during comparison",
        )?;
        File::open(path.parent().unwrap())?.sync_all()?;
        return Ok(());
    }
    let temp = path.with_extension(format!("{}.tmp", std::process::id()));
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&temp)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::hard_link(&temp, path)?;
    fs::remove_file(temp)?;
    File::open(path.parent().unwrap())?.sync_all()?;
    Ok(())
}

fn encode_batch(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut bytes = vec![];
    let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), None)?;
    writer.write(batch)?;
    writer.close()?;
    Ok(bytes)
}

pub(crate) fn validate_receipt(receipt: &CutReceipt, cut: &FrozenCut) -> Result<()> {
    ensure!(
        receipt.cut_id == cut.identity()?
            && receipt.world == cut.world
            && receipt.run == cut.run
            && receipt.tick == cut.tick
            && receipt.program == cut.program
            && receipt.parent == cut.parent
            && receipt.checkpoint_sha256 == crate::hash(&cut.checkpoint)
            && receipt.frozen_manifest_sha256
                == cut
                    .hosted
                    .as_ref()
                    .map(crate::hosted::canonical_digest)
                    .transpose()?
            && receipt.components.len() == cut.relations.len(),
        "Receipt/journal identity or inventory mismatch"
    );
    for (name, relation) in &cut.relations {
        let selected = receipt
            .components
            .get(name)
            .ok_or_else(|| anyhow!("Missing component inventory"))?;
        ensure!(
            selected.schema == relation.schema
                && selected.rows == relation.rows.len()
                && selected.table == format!("component_{}", relation.schema.identity()?),
            "Receipt/journal component mismatch"
        );
    }
    Ok(())
}

#[cfg(test)]
mod corrupted_catalog_contract {
    use super::*;

    #[tokio::test]
    async fn stored_receipt_cannot_omit_inventory_or_change_program() -> Result<()> {
        for corruption in ["nonempty", "empty", "program"] {
            let root = tempfile::tempdir()?;
            let store = CutStore::open(root.path()).await?;
            let mut cut = crate::store_tests::cut();
            if corruption == "empty" {
                cut.relations.get_mut("label").unwrap().rows.clear();
            }
            let mut receipt = store.publish(&cut).await?;
            if corruption == "program" {
                receipt.program = "forged".into();
            } else {
                receipt.components.remove("label");
            }
            // Corrupt the stored final-manifest row while retaining cut/checkpoint
            // identities. Previously duplicate publication trusted its inventory.
            let batch = RecordBatch::try_new(
                Arc::new(schema_to_arrow_schema(&cut_schema()?)?),
                vec![
                    Arc::new(StringArray::from(vec![receipt.cut_id.as_str()])),
                    Arc::new(StringArray::from(vec![serde_json::to_string(&receipt)?])),
                ],
            )?;
            let object = fs::read_dir(store.root.join("objects"))?
                .map(|e| e.unwrap().path())
                .find(|p| {
                    p.file_name()
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .starts_with("cuts.")
                })
                .unwrap();
            fs::write(object, encode_batch(&batch)?)?;
            assert!(
                store.publish(&cut).await.is_err(),
                "accepted corrupt {corruption} receipt"
            );
            assert!(store.verified_cut(&receipt).await.is_err());
        }
        Ok(())
    }
}
