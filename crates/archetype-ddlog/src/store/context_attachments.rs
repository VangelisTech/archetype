//! Versioned occurrence publication for immutable contexts, with optional cut
//! attribution. Existing cut_artifact_*_v1 tables retain their exact contract.
use super::*;
use anyhow::Context;
use arrow_array::{Array, BooleanArray, Float64Array, Int64Array, TimestampMicrosecondArray};
use arrow_schema::{DataType, Field};
use attachments::{
    Attachment, IndexReceipt, append_string, decode_bounded, physical_batch, text, validate_common,
    validate_schema,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use contexts::{ArtifactTarget, ContextRef, ExactCut, index_proof};

const COMMON: &str = "context_artifact_files_v1";
const TYPED: &[&str] = &["audio", "diff", "images", "pdf", "text", "video"];

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextAttachmentReceipt {
    pub version: u32,
    pub target: ArtifactTarget,
    pub artifact_id: String,
    pub common: IndexReceipt,
}
#[derive(Serialize)]
pub struct ContextAttachmentRead {
    pub receipt: ContextAttachmentReceipt,
    pub common: String,
    pub typed: BTreeMap<String, String>,
    pub sha256: String,
    pub media_type: String,
    pub size_bytes: u64,
    pub facts: BTreeMap<String, serde_json::Value>,
    pub typed_facts: BTreeMap<String, BTreeMap<String, serde_json::Value>>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ArtifactSelection {
    All,
    Target { exact_cut: Option<ExactCut> },
}
#[derive(Clone, Copy, Default)]
pub enum ContextAttachmentFault {
    #[default]
    None,
    AfterPreparation,
    AfterTyped,
    AfterCommon,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Preparation {
    version: u32,
    target: ArtifactTarget,
    attachment: Attachment,
}
struct Prepared {
    id: String,
    common: RecordBatch,
    typed: BTreeMap<String, RecordBatch>,
    bytes: Vec<u8>,
}

impl CutStore {
    fn context_attachment_path(&self, id: &str) -> PathBuf {
        self.root
            .join("objects")
            .join(format!("context_artifact_prepared_v1.{id}.json"))
    }
    pub async fn attach_context(
        &self,
        target: &ArtifactTarget,
        attachments: &[Attachment],
    ) -> Result<Vec<ContextAttachmentReceipt>> {
        self.attach_context_with_fault(target, attachments, ContextAttachmentFault::None)
            .await
    }
    pub async fn attach_context_with_fault(
        &self,
        target: &ArtifactTarget,
        attachments: &[Attachment],
        fault: ContextAttachmentFault,
    ) -> Result<Vec<ContextAttachmentReceipt>> {
        let store = self.read_scope().await?;
        let _guard = store.publication.lock().await;
        store.verify_target_inner(target).await?;
        bounds::request(
            attachments.len() <= 32,
            "At most 32 context occurrences per call",
        )?;
        let mut prepared = vec![];
        let mut ids = std::collections::BTreeSet::new();
        for attachment in attachments {
            let common = decode_bounded(&attachment.common, &store.budget)?;
            validate_common(&common)?;
            let id = text(&common, "artifact_id")?.to_owned();
            let uuid = uuid::Uuid::parse_str(&id)?;
            ensure!(
                uuid.get_version_num() == 7
                    && uuid.get_variant() == uuid::Variant::RFC4122
                    && uuid.to_string() == id
                    && ids.insert(id.clone()),
                "Expected distinct canonical UUIDv7 occurrences"
            );
            ensure!(
                !store
                    .root
                    .join("objects")
                    .join(format!("cut_artifact_prepared_v1.{id}.json"))
                    .try_exists()?,
                "Occurrence already belongs to legacy cut attribution"
            );
            ensure!(
                !store.occurrence_exists("cut_artifact", &id).await?,
                "Occurrence already has legacy index evidence"
            );
            store.verify_content(&common, true)?;
            let mut typed = BTreeMap::new();
            for (name, encoded) in &attachment.typed {
                ensure!(TYPED.contains(&name.as_str()), "Unknown typed index");
                let batch = decode_bounded(encoded, &store.budget)?;
                validate_schema(name, &batch)?;
                ensure!(
                    text(&batch, "artifact_id")? == id,
                    "Typed occurrence mismatch"
                );
                typed.insert(name.clone(), stamp(&batch, target)?);
            }
            let bytes = bounds::encode_metadata(
                &Preparation {
                    version: 1,
                    target: target.clone(),
                    attachment: attachment.clone(),
                },
                store.budget.limits.metadata_bytes,
            )?;
            prepared.push(Prepared {
                id,
                common: stamp(&common, target)?,
                typed,
                bytes,
            });
        }
        for item in &prepared {
            let path = store.context_attachment_path(&item.id);
            if !path.try_exists()? {
                for name in std::iter::once(COMMON.to_owned())
                    .chain(TYPED.iter().map(|n| format!("context_artifact_{n}_v1")))
                {
                    if store.catalog.table_exists(&ident(&name)?).await? {
                        let table = store.catalog.load_table(&ident(&name)?).await?;
                        ensure!(
                            find_snapshot(&table, &item.id)?.is_none(),
                            "Published occurrence has lost its preparation"
                        );
                    }
                }
            }
            immutable(&path, &item.bytes)?;
            File::open(path)?.sync_all()?;
        }
        ensure!(
            !matches!(fault, ContextAttachmentFault::AfterPreparation),
            "Injected failure after occurrence preparation"
        );
        let mut proofs = vec![];
        for item in &prepared {
            let mut typed = BTreeMap::new();
            for (name, batch) in &item.typed {
                typed.insert(
                    name.clone(),
                    store
                        .append_index(&format!("context_artifact_{name}_v1"), &item.id, batch)
                        .await?,
                );
            }
            proofs.push(typed);
        }
        ensure!(
            !matches!(fault, ContextAttachmentFault::AfterTyped),
            "Injected failure after context typed indexes"
        );
        let mut receipts = vec![];
        for (item, typed) in prepared.iter().zip(proofs) {
            let common = append_string(&item.common, "intent_sha256", &crate::hash(&item.bytes))?;
            let common = append_string(
                &common,
                "typed_receipts_json",
                &serde_json::to_string(&typed)?,
            )?;
            let proof = store.append_index(COMMON, &item.id, &common).await?;
            receipts.push(ContextAttachmentReceipt {
                version: 1,
                target: target.clone(),
                artifact_id: item.id.clone(),
                common: proof,
            });
            ensure!(
                !matches!(fault, ContextAttachmentFault::AfterCommon),
                "Injected failure after context common root"
            );
        }
        Ok(receipts)
    }
    pub async fn context_attachments(
        &self,
        context: &ContextRef,
        selection: &ArtifactSelection,
        offset: usize,
        limit: usize,
    ) -> Result<(Vec<ContextAttachmentRead>, usize)> {
        let store = self.read_scope().await?;
        let _guard = store.publication.lock().await;
        bounds::request(
            (1..=32).contains(&limit),
            "Context occurrence limit must be 1..32",
        )?;
        let selected = match selection {
            ArtifactSelection::All => None,
            ArtifactSelection::Target { exact_cut } => Some(exact_cut),
        };
        store
            .verify_target_inner(&ArtifactTarget {
                context: context.clone(),
                exact_cut: selected.cloned().flatten(),
            })
            .await?;
        if !store.catalog.table_exists(&ident(COMMON)?).await? {
            bounds::request(offset == 0, "Offset past context occurrences")?;
            return Ok((vec![], 0));
        }
        let table = store.catalog.load_table(&ident(COMMON)?).await?;
        let mut ids = std::collections::BTreeSet::new();
        if table.metadata().current_snapshot().is_some() {
            for batch in store.scan_metadata(&table).await? {
                for row in 0..batch.num_rows() {
                    store.budget.items(1)?;
                    if strings(&batch, "context_id")?.value(row) != context.context_id {
                        continue;
                    }
                    let row_batch = batch.slice(row, 1);
                    let target = target_from_row(&row_batch)?;
                    ensure!(
                        target.context == *context,
                        "Context occurrence scope mismatch"
                    );
                    if selected.is_some_and(|exact| *exact != target.exact_cut) {
                        continue;
                    }
                    ensure!(
                        ids.insert(text(&row_batch, "artifact_id")?.to_owned()),
                        "Duplicate visible context occurrence"
                    );
                }
            }
        }
        let total = ids.len();
        bounds::request(offset <= total, "Offset past context occurrences")?;
        let mut results = vec![];
        for id in ids.into_iter().skip(offset).take(limit) {
            let proof = index_proof(&table, COMMON, &id)?;
            let common = store.read_index_record(&proof, &id, "artifact_id").await?;
            let target = target_from_row(&common)?;
            ensure!(
                target.context == *context
                    && selected.is_none_or(|exact| *exact == target.exact_cut),
                "Context attribution changed"
            );
            store.verify_target_inner(&target).await?;
            store.verify_content(&common, false)?;
            let proof_json = text(&common, "typed_receipts_json")?;
            preflight::json(proof_json.as_bytes(), &store.budget)?;
            let typed_proofs: BTreeMap<String, IndexReceipt> = serde_json::from_str(proof_json)?;
            let bytes = store
                .budget
                .read_metadata(&store.context_attachment_path(&id))?;
            ensure!(
                crate::hash(&bytes) == text(&common, "intent_sha256")?,
                "Context preparation changed"
            );
            let prepared: Preparation = serde_json::from_slice(&bytes)?;
            ensure!(
                prepared.version == 1
                    && prepared.target == target
                    && prepared.attachment.typed.keys().eq(typed_proofs.keys()),
                "Context preparation target/inventory mismatch"
            );
            let intrinsic = decode_bounded(&prepared.attachment.common, &store.budget)?;
            validate_common(&intrinsic)?;
            let expected = stamp(&intrinsic, &target)?;
            let expected = append_string(&expected, "intent_sha256", &crate::hash(&bytes))?;
            let expected = append_string(
                &expected,
                "typed_receipts_json",
                &serde_json::to_string(&typed_proofs)?,
            )?;
            ensure!(
                crate::hash(&encode_batch(&physical_batch(&expected)?.1)?) == proof.object_sha256,
                "Context common differs from prepared metadata"
            );
            let mut typed = BTreeMap::new();
            let mut typed_facts = BTreeMap::new();
            for (name, proof) in typed_proofs {
                ensure!(
                    TYPED.contains(&name.as_str())
                        && proof.table == format!("context_artifact_{name}_v1"),
                    "Wrong context typed proof"
                );
                let batch = store.read_index_record(&proof, &id, "artifact_id").await?;
                ensure!(
                    target_from_row(&batch)? == target,
                    "Context typed attribution mismatch"
                );
                let intrinsic = decode_bounded(&prepared.attachment.typed[&name], &store.budget)?;
                validate_schema(&name, &intrinsic)?;
                let expected = stamp(&intrinsic, &target)?;
                ensure!(
                    crate::hash(&encode_batch(&physical_batch(&expected)?.1)?)
                        == proof.object_sha256,
                    "Context typed differs from preparation"
                );
                typed_facts.insert(name.clone(), public_facts(&batch)?);
                typed.insert(name, STANDARD.encode(encode_batch(&batch)?));
            }
            results.push(ContextAttachmentRead {
                sha256: text(&common, "sha256")?.into(),
                media_type: text(&common, "mime_type")?.into(),
                size_bytes: u64::try_from(attachments::integer(&common, "size_bytes")?)?,
                receipt: ContextAttachmentReceipt {
                    version: 1,
                    target,
                    artifact_id: id,
                    common: proof,
                },
                facts: public_facts(&common)?,
                typed_facts,
                common: STANDARD.encode(encode_batch(&common)?),
                typed,
            });
            bounds::page(&results, store.budget.limits.page_bytes)?;
        }
        Ok((results, total))
    }
}
fn stamp(batch: &RecordBatch, target: &ArtifactTarget) -> Result<RecordBatch> {
    let mut result = batch.clone();
    for (name, value) in [
        ("context_id", &target.context.context_id),
        ("world", &target.context.world),
        ("run", &target.context.run),
    ] {
        result = append_string(&result, name, value)?;
    }
    for name in ["tick", "cut_id", "intent_sha256", "typed_receipts_json"] {
        ensure!(
            result.column_by_name(name).is_none(),
            "Reserved context metadata column"
        );
    }
    let mut fields = result.schema().fields().to_vec();
    let mut columns = result.columns().to_vec();
    fields.push(Arc::new(Field::new("tick", DataType::Int64, true)));
    fields.push(Arc::new(Field::new("cut_id", DataType::Utf8, true)));
    columns.push(Arc::new(Int64Array::from(vec![
        target
            .exact_cut
            .as_ref()
            .map(|c| i64::try_from(c.tick))
            .transpose()?,
    ])));
    columns.push(Arc::new(StringArray::from(vec![
        target.exact_cut.as_ref().map(|c| c.cut_id.as_str()),
    ])));
    Ok(RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::new(fields)),
        columns,
    )?)
}
fn target_from_row(batch: &RecordBatch) -> Result<ArtifactTarget> {
    let tick = batch
        .column_by_name("tick")
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
        .ok_or_else(|| anyhow!("Invalid context tick"))?;
    let cut = strings(batch, "cut_id")?;
    ensure!(tick.is_null(0) == cut.is_null(0), "Partial cut attribution");
    let exact_cut = if tick.is_null(0) {
        None
    } else {
        ensure!(tick.value(0) > 0, "Invalid context tick");
        Some(ExactCut {
            tick: tick.value(0) as u64,
            cut_id: cut.value(0).into(),
        })
    };
    Ok(ArtifactTarget {
        context: ContextRef {
            world: text(batch, "world")?.into(),
            run: text(batch, "run")?.into(),
            context_id: text(batch, "context_id")?.into(),
        },
        exact_cut,
    })
}

/// Factual, bounded public projection from the same verified one-row indexes.
/// Physical publication proofs and source/object locations stay private.
fn public_facts(batch: &RecordBatch) -> Result<BTreeMap<String, serde_json::Value>> {
    ensure!(batch.num_rows() == 1, "Expected one occurrence fact row");
    let mut result = BTreeMap::new();
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        if matches!(
            field.name().as_str(),
            "source_uri" | "object_uri" | "intent_sha256" | "typed_receipts_json"
        ) {
            continue;
        }
        let value = if column.is_null(0) {
            serde_json::Value::Null
        } else {
            match field.data_type() {
                DataType::Utf8 => {
                    serde_json::json!({"string": column.as_any().downcast_ref::<StringArray>().context("String fact")?.value(0)})
                }
                DataType::Int64 => {
                    serde_json::json!({"int64": column.as_any().downcast_ref::<Int64Array>().context("Integer fact")?.value(0).to_string()})
                }
                DataType::Boolean => {
                    serde_json::json!({"bool": column.as_any().downcast_ref::<BooleanArray>().context("Boolean fact")?.value(0)})
                }
                DataType::Float64 => {
                    let value = column
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .context("Float fact")?
                        .value(0);
                    ensure!(value.is_finite(), "Nonfinite artifact fact");
                    let value = if value == 0.0 { 0.0 } else { value };
                    serde_json::json!({"float64": format!("{:016x}", value.to_bits())})
                }
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, _) => {
                    serde_json::json!({"timestamp_us": column.as_any().downcast_ref::<TimestampMicrosecondArray>().context("Timestamp fact")?.value(0).to_string()})
                }
                _ => anyhow::bail!("Unsupported artifact fact type"),
            }
        };
        result.insert(field.name().clone(), value);
    }
    Ok(result)
}
