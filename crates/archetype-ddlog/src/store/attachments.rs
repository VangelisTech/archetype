//! Versioned append-only file indexes. An occurrence, not its simulation cut,
//! identifies a physical append. Only the common row grants visibility.
use super::*;
use arrow_array::Int64Array;
use arrow_schema::{DataType, Field};
use base64::{Engine, engine::general_purpose::STANDARD};
use bytes::Bytes;
use iceberg::arrow::arrow_schema_to_schema_auto_assign_ids;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sha2::{Digest, Sha256};
use std::io::Read;

const COMMON: &str = "cut_artifact_files_v1";
const TYPED: &[&str] = &["audio", "diff", "images", "pdf", "text", "video"];

/// Already-materialized metadata, retained verbatim by the submitting workflow
/// for exact retry. Content remains in the operator-owned object namespace.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Attachment {
    pub common: String,
    pub typed: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexReceipt {
    pub table: String,
    pub table_uuid: String,
    pub snapshot: i64,
    pub object: String,
    pub object_sha256: String,
    pub schema_sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttachmentReceipt {
    pub version: u32,
    pub world: String,
    pub run: String,
    pub tick: u64,
    pub cut_id: String,
    pub artifact_id: String,
    pub common: IndexReceipt,
}

#[derive(Serialize)]
pub struct AttachmentRead {
    pub receipt: AttachmentReceipt,
    pub common: String,
    pub typed: BTreeMap<String, String>,
}

/// Deterministic fault seam; never exposed by the production C ABI.
#[derive(Clone, Copy, Default)]
pub enum AttachmentFault {
    #[default]
    None,
    AfterTyped,
    AfterCommon,
}

struct Prepared {
    id: String,
    common: RecordBatch,
    typed: BTreeMap<String, RecordBatch>,
    intent: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PreparedObject {
    version: u32,
    world: String,
    run: String,
    tick: u64,
    cut_id: String,
    attachment: Attachment,
}

impl CutStore {
    pub async fn attachment_root(&self, cut: &CutReceipt) -> Result<PathBuf> {
        self.verified_cut(cut).await?;
        Ok(self.root.join("artifact_objects"))
    }

    pub async fn attach(
        &self,
        cut: &CutReceipt,
        attachments: &[Attachment],
    ) -> Result<Vec<AttachmentReceipt>> {
        self.attach_with_fault(cut, attachments, AttachmentFault::None)
            .await
    }

    pub async fn attach_with_fault(
        &self,
        cut: &CutReceipt,
        attachments: &[Attachment],
        fault: AttachmentFault,
    ) -> Result<Vec<AttachmentReceipt>> {
        let _guard = self.publication.lock().await;
        self.verified_cut(cut).await?;
        ensure!(
            attachments.len() <= 32,
            "At most 32 occurrences per attachment call"
        );
        let mut prepared = Vec::new();
        let mut ids = std::collections::BTreeSet::new();
        // Validate the entire submission before any index commits.
        for attachment in attachments {
            let common = decode(&attachment.common)?;
            let id = text(&common, "artifact_id")?.to_owned();
            let uuid = uuid::Uuid::parse_str(&id)?;
            ensure!(
                uuid.get_version_num() == 7
                    && uuid.get_variant() == uuid::Variant::RFC4122
                    && uuid.to_string() == id
                    && ids.insert(id.clone()),
                "Expected distinct canonical UUIDv7 occurrence identities"
            );
            validate_common(&common)?;
            self.verify_content(&common, true)?;
            let mut typed = BTreeMap::new();
            for (name, encoded) in &attachment.typed {
                ensure!(TYPED.contains(&name.as_str()), "Unknown typed index");
                let batch = decode(encoded)?;
                validate_schema(name, &batch)?;
                ensure!(
                    text(&batch, "artifact_id")? == id,
                    "Typed occurrence mismatch"
                );
                typed.insert(name.clone(), stamp(&batch, cut)?);
            }
            prepared.push(Prepared {
                id,
                common: stamp(&common, cut)?,
                typed,
                intent: serde_json::to_vec(&PreparedObject {
                    version: 1,
                    world: cut.world.clone(),
                    run: cut.run.clone(),
                    tick: cut.tick,
                    cut_id: cut.cut_id.clone(),
                    attachment: attachment.clone(),
                })?,
            });
        }
        // Bind the complete exact payload before any append, including indexes
        // that a changed retry might omit. This private object grants no visibility.
        for item in &prepared {
            let path = self.attachment_intent(&item.id);
            immutable(&path, &item.intent)?;
            File::open(path)?.sync_all()?;
        }
        let mut proofs = Vec::new();
        for item in &prepared {
            let mut typed = BTreeMap::new();
            for (name, batch) in &item.typed {
                typed.insert(
                    name.clone(),
                    self.append_index(&format!("cut_artifact_{name}_v1"), &item.id, batch)
                        .await?,
                );
            }
            proofs.push(typed);
        }
        ensure!(
            !matches!(fault, AttachmentFault::AfterTyped),
            "Injected failure after typed indexes"
        );
        let mut receipts = Vec::new();
        for (item, typed) in prepared.iter().zip(proofs) {
            let common = append_string(&item.common, "intent_sha256", &crate::hash(&item.intent))?;
            let common = append_string(
                &common,
                "typed_receipts_json",
                &serde_json::to_string(&typed)?,
            )?;
            let proof = self.append_index(COMMON, &item.id, &common).await?;
            receipts.push(receipt(cut, item.id.clone(), proof));
            ensure!(
                !matches!(fault, AttachmentFault::AfterCommon),
                "Injected failure after common index"
            );
        }
        Ok(receipts)
    }

    async fn append_index(
        &self,
        name: &str,
        id: &str,
        batch: &RecordBatch,
    ) -> Result<IndexReceipt> {
        let (schema, batch) = physical_batch(batch)?;
        let table = self.table(name, schema).await?;
        let object = self.stage_batch(name, id, &batch)?;
        // Existing physical append machinery keys exact adoption by occurrence.
        // The simulation cut is separately stamped into the immutable row.
        let snapshot = self.register(&table, name, id, &object, 1).await?;
        Ok(IndexReceipt {
            table: name.into(),
            table_uuid: table.metadata().uuid().to_string(),
            snapshot,
            object: object.0,
            object_sha256: object.1,
            schema_sha256: crate::digest(table.metadata().current_schema().as_ref())?,
        })
    }

    async fn read_index(&self, proof: &IndexReceipt, id: &str) -> Result<RecordBatch> {
        ensure!(
            proof.table == COMMON
                || TYPED
                    .iter()
                    .any(|n| proof.table == format!("cut_artifact_{n}_v1")),
            "Unknown index table"
        );
        let table = self.catalog.load_table(&ident(&proof.table)?).await?;
        ensure!(
            table.metadata().uuid().to_string() == proof.table_uuid
                && crate::digest(table.metadata().current_schema().as_ref())?
                    == proof.schema_sha256
                && self.root.join("objects").join(format!(
                    "{}.{id}.{}.parquet",
                    proof.table, proof.object_sha256
                )) == Path::new(&proof.object),
            "Index table/object identity mismatch"
        );
        self.verify_snapshot(
            &table,
            proof.snapshot,
            id,
            &(proof.object.clone(), proof.object_sha256.clone()),
            1,
        )
        .await?;
        let batch = decode_bytes(fs::read(&proof.object)?.into())?;
        ensure!(
            text(&batch, "artifact_id")? == id,
            "Index occurrence mismatch"
        );
        Ok(batch)
    }

    /// Read roots from the cut's common index, then verify every pinned typed
    /// proof and content object. No standalone typed scan grants visibility.
    pub async fn attachments(
        &self,
        cut: &CutReceipt,
        offset: usize,
        limit: usize,
    ) -> Result<(Vec<AttachmentRead>, usize)> {
        let _guard = self.publication.lock().await;
        self.verified_cut(cut).await?;
        ensure!(
            (1..=32).contains(&limit),
            "Attachment read limit must be 1..32"
        );
        if !self.catalog.table_exists(&ident(COMMON)?).await? {
            ensure!(offset == 0, "Offset past attachments");
            return Ok((vec![], 0));
        }
        let table = self.catalog.load_table(&ident(COMMON)?).await?;
        if table.metadata().current_snapshot().is_none() {
            ensure!(offset == 0, "Offset past attachments");
            return Ok((vec![], 0));
        }
        let mut scan = table
            .scan()
            .with_filter(Reference::new("cut_id").equal_to(Datum::string(&cut.cut_id)))
            .build()?
            .to_arrow()
            .await?;
        let mut ids = std::collections::BTreeSet::new();
        while let Some(batch) = scan.try_next().await? {
            for id in strings(&batch, "artifact_id")?.iter() {
                ensure!(
                    ids.insert(id.ok_or_else(|| anyhow!("Null occurrence"))?.to_owned()),
                    "Duplicate visible occurrence"
                );
            }
        }
        let total = ids.len();
        ensure!(offset <= total, "Offset past attachments");
        let mut results = Vec::new();
        for id in ids.into_iter().skip(offset).take(limit) {
            let snapshot_id = find_snapshot(&table, &id)?
                .ok_or_else(|| anyhow!("Missing occurrence snapshot"))?;
            let props = &table
                .metadata()
                .snapshot_by_id(snapshot_id)
                .unwrap()
                .summary()
                .additional_properties;
            let proof = IndexReceipt {
                table: COMMON.into(),
                table_uuid: table.metadata().uuid().to_string(),
                snapshot: snapshot_id,
                object: props
                    .get("archetype.object")
                    .ok_or_else(|| anyhow!("Missing object"))?
                    .clone(),
                object_sha256: props
                    .get("archetype.object-sha256")
                    .ok_or_else(|| anyhow!("Missing object digest"))?
                    .clone(),
                schema_sha256: crate::digest(table.metadata().current_schema().as_ref())?,
            };
            let common = self.read_index(&proof, &id).await?;
            verify_scope(&common, cut)?;
            self.verify_content(&common, false)?;
            let typed_proofs: BTreeMap<String, IndexReceipt> =
                serde_json::from_str(text(&common, "typed_receipts_json")?)?;
            let intent_bytes = fs::read(self.attachment_intent(&id))?;
            ensure!(
                crate::hash(&intent_bytes) == text(&common, "intent_sha256")?,
                "Prepared metadata changed"
            );
            let intent: PreparedObject = serde_json::from_slice(&intent_bytes)?;
            ensure!(
                intent.version == 1
                    && intent.world == cut.world
                    && intent.run == cut.run
                    && intent.tick == cut.tick
                    && intent.cut_id == cut.cut_id,
                "Prepared cut attribution mismatch"
            );
            ensure!(
                intent.attachment.typed.keys().eq(typed_proofs.keys()),
                "Typed inventory mismatch"
            );
            let expected = stamp(&decode(&intent.attachment.common)?, cut)?;
            let expected = append_string(&expected, "intent_sha256", &crate::hash(&intent_bytes))?;
            let expected = append_string(
                &expected,
                "typed_receipts_json",
                &serde_json::to_string(&typed_proofs)?,
            )?;
            ensure!(
                crate::hash(&encode_batch(&physical_batch(&expected)?.1)?) == proof.object_sha256,
                "Common object differs from prepared metadata"
            );
            let mut typed = BTreeMap::new();
            for (name, proof) in typed_proofs {
                ensure!(
                    TYPED.contains(&name.as_str())
                        && proof.table == format!("cut_artifact_{name}_v1"),
                    "Wrong typed proof"
                );
                let batch = self.read_index(&proof, &id).await?;
                verify_scope(&batch, cut)?;
                let expected = stamp(&decode(&intent.attachment.typed[&name])?, cut)?;
                ensure!(
                    crate::hash(&encode_batch(&physical_batch(&expected)?.1)?)
                        == proof.object_sha256,
                    "Typed object differs from prepared metadata"
                );
                typed.insert(name, STANDARD.encode(encode_batch(&batch)?));
            }
            results.push(AttachmentRead {
                receipt: receipt(cut, id, proof),
                common: STANDARD.encode(encode_batch(&common)?),
                typed,
            });
        }
        Ok((results, total))
    }

    fn attachment_intent(&self, id: &str) -> PathBuf {
        self.root
            .join("objects")
            .join(format!("cut_artifact_prepared_v1.{id}.json"))
    }

    fn verify_content(&self, common: &RecordBatch, sync: bool) -> Result<()> {
        let digest = text(common, "sha256")?;
        ensure!(
            digest.len() == 64
                && digest
                    .bytes()
                    .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c)),
            "Invalid content digest"
        );
        let path = self
            .root
            .join("artifact_objects/objects/sha256")
            .join(&digest[..2])
            .join(digest);
        ensure!(
            url::Url::parse(text(common, "object_uri")?)?
                .to_file_path()
                .ok()
                .as_ref()
                == Some(&path),
            "Object outside content namespace"
        );
        ensure!(
            path.canonicalize()? == path,
            "Content path must not use symlinks"
        );
        let mut file = File::open(&path)?;
        let mut hasher = Sha256::new();
        let mut size = 0i64;
        let mut buffer = [0u8; 65536];
        loop {
            let count = file.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            hasher.update(&buffer[..count]);
            size += count as i64;
        }
        ensure!(
            format!("{:x}", hasher.finalize()) == digest && integer(common, "size_bytes")? == size,
            "Content size/digest mismatch"
        );
        if sync {
            file.sync_all()?;
            let mut directory = path.parent().unwrap();
            loop {
                File::open(directory)?.sync_all()?;
                if directory == self.root {
                    break;
                }
                directory = directory
                    .parent()
                    .ok_or_else(|| anyhow!("Invalid content root"))?;
            }
        }
        Ok(())
    }
}

fn physical_batch(batch: &RecordBatch) -> Result<(Schema, RecordBatch)> {
    let schema = arrow_schema_to_schema_auto_assign_ids(batch.schema().as_ref())?;
    let physical = Arc::new(schema_to_arrow_schema(&schema)?);
    let columns = batch
        .columns()
        .iter()
        .zip(physical.fields())
        .map(|(column, field)| arrow_cast::cast(column, field.data_type()))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok((schema, RecordBatch::try_new(physical, columns)?))
}

fn receipt(cut: &CutReceipt, artifact_id: String, common: IndexReceipt) -> AttachmentReceipt {
    AttachmentReceipt {
        version: 1,
        world: cut.world.clone(),
        run: cut.run.clone(),
        tick: cut.tick,
        cut_id: cut.cut_id.clone(),
        artifact_id,
        common,
    }
}
fn decode(encoded: &str) -> Result<RecordBatch> {
    ensure!(
        encoded.len() <= 192 * 1024,
        "Occurrence metadata exceeds 192 KiB"
    );
    decode_bytes(STANDARD.decode(encoded)?.into())
}
fn decode_bytes(bytes: Bytes) -> Result<RecordBatch> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
    ensure!(
        builder.metadata().file_metadata().num_rows() == 1,
        "Expected one occurrence per metadata object"
    );
    ensure!(
        builder.schema().fields().len() <= 64,
        "Too many metadata columns"
    );
    let mut reader = builder.with_batch_size(1).build()?;
    let batch = reader
        .next()
        .ok_or_else(|| anyhow!("Missing occurrence"))??;
    ensure!(reader.next().is_none(), "Unexpected metadata batches");
    Ok(batch)
}
fn text<'a>(batch: &'a RecordBatch, column: &str) -> Result<&'a str> {
    let array = strings(batch, column)?;
    ensure!(!array.is_null(0), "Null {column}");
    Ok(array.value(0))
}
fn integer(batch: &RecordBatch, column: &str) -> Result<i64> {
    let array = batch
        .column_by_name(column)
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
        .ok_or_else(|| anyhow!("Invalid {column}"))?;
    ensure!(!array.is_null(0), "Null {column}");
    Ok(array.value(0))
}
fn validate_common(batch: &RecordBatch) -> Result<()> {
    validate_schema("files", batch)?;
    ensure!(
        batch.columns().iter().all(|c| !c.is_null(0)),
        "Null common metadata"
    );
    for name in [
        "source_uri",
        "logical_path",
        "mime_type",
        "media_family",
        "xxhash3_64",
    ] {
        text(batch, name)?;
    }
    ensure!(integer(batch, "size_bytes")? >= 0, "Negative content size");
    Ok(())
}

/// Closed v1 file-index schemas, independent of the live DDlog relation types.
/// A malformed first caller must never determine the shared physical schema.
fn validate_schema(name: &str, batch: &RecordBatch) -> Result<()> {
    use DataType::{Boolean as B, Float64 as F, Int64 as I, Utf8 as S};
    let tail = match name {
        "files" => vec![
            (
                "ingested_at",
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("+00:00".into())),
            ),
            ("source_uri", S),
            ("logical_path", S),
            ("object_uri", S),
            ("size_bytes", I),
            ("mime_type", S),
            ("media_family", S),
            ("sha256", S),
            ("xxhash3_64", S),
        ],
        "images" => vec![("width", I), ("height", I), ("format", S), ("mode", S)],
        "audio" => vec![
            ("sample_rate", I),
            ("channels", I),
            ("frames", F),
            ("format", S),
            ("subtype", S),
            ("duration_seconds", F),
        ],
        "video" => vec![
            ("width", I),
            ("height", I),
            ("fps", F),
            ("frame_count", I),
            ("time_base", F),
            ("duration_seconds", F),
        ],
        "pdf" => vec![
            ("page_count", I),
            ("encrypted", B),
            ("title", S),
            ("author", S),
        ],
        "text" => vec![
            ("text_kind", S),
            ("language", S),
            ("line_count", I),
            ("utf8", B),
        ],
        "diff" => vec![
            ("format", S),
            ("file_count", I),
            ("hunk_count", I),
            ("additions", I),
            ("deletions", I),
            ("binary_file_count", I),
        ],
        _ => anyhow::bail!("Unknown index schema"),
    };
    let fields: Vec<_> = std::iter::once(("artifact_id", S)).chain(tail).collect();
    let (_, canonical) = physical_batch(batch)?;
    ensure!(
        canonical.num_columns() == fields.len()
            && canonical
                .schema()
                .fields()
                .iter()
                .zip(fields)
                .all(|(field, (name, dtype))| field.name() == name
                    && field.data_type() == &dtype
                    && field.is_nullable()),
        "Unsupported {name} v1 schema"
    );
    Ok(())
}
fn append_string(batch: &RecordBatch, name: &str, value: &str) -> Result<RecordBatch> {
    ensure!(
        batch.column_by_name(name).is_none(),
        "Reserved metadata column: {name}"
    );
    let mut fields = batch.schema().fields().to_vec();
    let mut columns = batch.columns().to_vec();
    fields.push(Arc::new(Field::new(name, DataType::Utf8, false)));
    columns.push(Arc::new(StringArray::from(vec![value])) as ArrayRef);
    Ok(RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::new(fields)),
        columns,
    )?)
}
fn stamp(batch: &RecordBatch, cut: &CutReceipt) -> Result<RecordBatch> {
    let mut result = batch.clone();
    for (name, value) in [
        ("world", &cut.world),
        ("run", &cut.run),
        ("cut_id", &cut.cut_id),
    ] {
        result = append_string(&result, name, value)?;
    }
    ensure!(
        result.column_by_name("intent_sha256").is_none()
            && result.column_by_name("tick").is_none()
            && result.column_by_name("typed_receipts_json").is_none(),
        "Reserved metadata column"
    );
    let mut fields = result.schema().fields().to_vec();
    let mut columns = result.columns().to_vec();
    fields.push(Arc::new(Field::new("tick", DataType::Int64, false)));
    columns.push(Arc::new(Int64Array::from(vec![i64::try_from(cut.tick)?])));
    Ok(RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::new(fields)),
        columns,
    )?)
}
fn verify_scope(batch: &RecordBatch, cut: &CutReceipt) -> Result<()> {
    ensure!(
        text(batch, "world")? == cut.world
            && text(batch, "run")? == cut.run
            && text(batch, "cut_id")? == cut.cut_id
            && integer(batch, "tick")? == i64::try_from(cut.tick)?,
        "Attachment cut attribution mismatch"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::TimestampMicrosecondArray;

    fn attachment(store: &CutStore, id: &str, value: &str) -> Result<Attachment> {
        let digest = crate::hash(value.as_bytes());
        let path = store
            .root
            .join("artifact_objects/objects/sha256")
            .join(&digest[..2])
            .join(&digest);
        fs::create_dir_all(path.parent().unwrap())?;
        fs::write(&path, value)?;
        let names = [
            "artifact_id",
            "ingested_at",
            "source_uri",
            "logical_path",
            "object_uri",
            "size_bytes",
            "mime_type",
            "media_family",
            "sha256",
            "xxhash3_64",
        ];
        let values: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![id])),
            Arc::new(TimestampMicrosecondArray::from(vec![1]).with_timezone("UTC")),
            Arc::new(StringArray::from(vec!["file:///source.txt"])),
            Arc::new(StringArray::from(vec!["source.txt"])),
            Arc::new(StringArray::from(vec![
                url::Url::from_file_path(path).unwrap().to_string(),
            ])),
            Arc::new(Int64Array::from(vec![value.len() as i64])),
            Arc::new(StringArray::from(vec!["text/plain"])),
            Arc::new(StringArray::from(vec!["text"])),
            Arc::new(StringArray::from(vec![digest])),
            Arc::new(StringArray::from(vec!["0000000000000000"])),
        ];
        let fields: Vec<_> = names
            .iter()
            .zip(&values)
            .map(|(name, a)| Field::new(*name, a.data_type().clone(), true))
            .collect();
        let common = RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(fields)), values)?;
        let values: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![id])),
            Arc::new(StringArray::from(vec!["plain"])),
            Arc::new(StringArray::from(vec![""])),
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(arrow_array::BooleanArray::from(vec![true])),
        ];
        let names = ["artifact_id", "text_kind", "language", "line_count", "utf8"];
        let fields: Vec<_> = names
            .iter()
            .zip(&values)
            .map(|(n, a)| Field::new(*n, a.data_type().clone(), true))
            .collect();
        let typed = RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(fields)), values)?;
        Ok(Attachment {
            common: STANDARD.encode(encode_batch(&common)?),
            typed: BTreeMap::from([("text".into(), STANDARD.encode(encode_batch(&typed)?))]),
        })
    }

    #[tokio::test]
    async fn occurrence_retry_survives_each_commit_boundary_and_restart() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let store = CutStore::open(dir.path()).await?;
        let cut = store.publish(&crate::store_tests::cut()).await?;
        let first = attachment(&store, "019a1111-1111-7111-8111-111111111111", "one")?;
        let second = attachment(&store, "019a1111-1111-7111-8111-222222222222", "two")?;
        let items = vec![first.clone(), second];
        assert!(
            store
                .attach_with_fault(&cut, &items, AttachmentFault::AfterTyped)
                .await
                .is_err()
        );
        assert_eq!(store.attachments(&cut, 0, 32).await?.1, 0);
        let mut omitted = first.clone();
        omitted.typed.clear();
        assert!(store.attach(&cut, &[omitted]).await.is_err());
        let mut changed = first.clone();
        let original = decode(&changed.typed["text"])?;
        let mut columns = original.columns().to_vec();
        columns[3] = Arc::new(Int64Array::from(vec![2]));
        changed.typed.insert(
            "text".into(),
            STANDARD.encode(encode_batch(&RecordBatch::try_new(
                original.schema(),
                columns,
            )?)?),
        );
        assert!(store.attach(&cut, &[changed]).await.is_err());
        let added = attachment(&store, "019a1111-1111-7111-8111-333333333333", "three")?;
        let mut empty = added.clone();
        empty.typed.clear();
        assert!(
            store
                .attach_with_fault(&cut, &[empty], AttachmentFault::AfterTyped)
                .await
                .is_err()
        );
        assert!(store.attach(&cut, &[added]).await.is_err());
        assert_eq!(store.attachments(&cut, 0, 32).await?.1, 0);
        drop(store);
        let store = CutStore::open(dir.path()).await?;
        assert!(
            store
                .attach_with_fault(&cut, &items, AttachmentFault::AfterCommon)
                .await
                .is_err()
        );
        assert_eq!(store.attachments(&cut, 0, 32).await?.1, 1);
        drop(store);
        let store = CutStore::open(dir.path()).await?;
        let receipts = store.attach(&cut, &items).await?;
        assert_eq!(store.attach(&cut, &items).await?, receipts);
        let (read, total) = store.attachments(&cut, 0, 32).await?;
        assert_eq!(total, 2);
        assert_eq!(read[0].receipt, receipts[0]);
        assert_eq!(text(&decode(&read[0].common)?, "world")?, cut.world);
        assert!(read[0].typed.contains_key("text"));
        assert_eq!(
            store.history(&cut.world, &cut.run).await?,
            vec![cut.clone()]
        );
        let changed = attachment(&store, &receipts[0].artifact_id, "changed")?;
        assert!(store.attach(&cut, &[changed]).await.is_err());
        assert_eq!(store.attachments(&cut, 0, 32).await?.1, 2);
        Ok(())
    }

    #[tokio::test]
    async fn malformed_first_schema_cannot_poison_shared_indexes() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let store = CutStore::open(dir.path()).await?;
        let cut = store.publish(&crate::store_tests::cut()).await?;
        let item = attachment(&store, "019a1111-1111-7111-8111-111111111111", "one")?;
        let alias = attachment(&store, "019A1111-1111-7111-8111-111111111111", "one")?;
        assert!(store.attach(&cut, &[alias]).await.is_err());
        let mut bad = item.clone();
        let batch = decode(&bad.common)?;
        let mut fields = batch.schema().fields().to_vec();
        fields[1] = Arc::new(Field::new("ingested_at", DataType::Utf8, true));
        let mut columns = batch.columns().to_vec();
        columns[1] = Arc::new(StringArray::from(vec!["wrong type"]));
        bad.common = STANDARD.encode(encode_batch(&RecordBatch::try_new(
            Arc::new(arrow_schema::Schema::new(fields)),
            columns,
        )?)?);
        assert!(store.attach(&cut, &[bad]).await.is_err());
        let mut bad = item.clone();
        let malformed = RecordBatch::try_from_iter([(
            "artifact_id",
            Arc::new(StringArray::from(vec![
                "019a1111-1111-7111-8111-111111111111",
            ])) as ArrayRef,
        )])?;
        bad.typed
            .insert("text".into(), STANDARD.encode(encode_batch(&malformed)?));
        assert!(store.attach(&cut, &[bad]).await.is_err());
        assert!(!store.catalog.table_exists(&ident(COMMON)?).await?);
        assert!(
            !store
                .catalog
                .table_exists(&ident("cut_artifact_text_v1")?)
                .await?
        );
        store.attach(&cut, &[item]).await?;
        assert_eq!(store.attachments(&cut, 0, 32).await?.1, 1);
        Ok(())
    }

    #[tokio::test]
    async fn cut_and_content_corruption_fail_closed() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let store = CutStore::open(dir.path()).await?;
        let cut = store.publish(&crate::store_tests::cut()).await?;
        let item = attachment(&store, "019a1111-1111-7111-8111-111111111111", "one")?;
        let mut wrong = cut.clone();
        wrong.cut_id = "0".repeat(64);
        assert!(store.attachment_root(&wrong).await.is_err());
        assert!(
            store
                .attach(&wrong, std::slice::from_ref(&item))
                .await
                .is_err()
        );
        assert_eq!(store.attachments(&cut, 0, 32).await?.1, 0);
        let receipts = store.attach(&cut, std::slice::from_ref(&item)).await?;
        let root = store
            .read_index(&receipts[0].common, &receipts[0].artifact_id)
            .await?;
        let typed: BTreeMap<String, IndexReceipt> =
            serde_json::from_str(text(&root, "typed_receipts_json")?)?;
        let proof = &typed["text"];
        let original = fs::read(&proof.object)?;
        fs::write(&proof.object, b"corrupt typed metadata")?;
        assert!(store.attachments(&cut, 0, 32).await.is_err());
        fs::write(&proof.object, original)?;
        let content = url::Url::parse(text(&root, "object_uri")?)?
            .to_file_path()
            .unwrap();
        fs::write(&content, b"bad")?;
        assert!(store.attachments(&cut, 0, 32).await.is_err());
        assert!(store.attach(&cut, &[item]).await.is_err());
        fs::write(&content, b"one")?;
        fs::write(
            store.journal(&cut.world, &cut.run, cut.tick),
            b"corrupt journal",
        )?;
        assert!(store.attachment_root(&cut).await.is_err());
        assert!(store.attachments(&cut, 0, 32).await.is_err());
        Ok(())
    }
}
