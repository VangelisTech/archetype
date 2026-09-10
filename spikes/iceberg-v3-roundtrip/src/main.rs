//! Standalone synthetic change-history oracle; no Archetype or DDlog runtime.
mod faults;
use anyhow::{Context, Result, bail, ensure};
use arrow_array::{ArrayRef, BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray};
use futures::TryStreamExt;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::io::{LocalFsStorageFactory, StorageFactory};
use iceberg::spec::{
    DataFile, FormatVersion, ManifestStatus, NestedField, PrimitiveType, Schema, Type,
    read_data_files_from_avro, write_data_files_to_avro,
};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::file_writer::{FileWriter, FileWriterBuilder, ParquetWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, ErrorKind, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_sql::{SqlBindStyle, SqlCatalog, SqlCatalogBuilder};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{Row as SqlRow, SqlitePool, sqlite::SqlitePoolOptions};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

const GROUP: &str = "aabc_wfixture_r1";
const PUB_KEY: &str = "spike.publication-id";
const DIGEST_KEY: &str = "spike.publication-sha256";
const COLUMN_NAMES: [&str; 11] = [
    "world_id",
    "run_id",
    "entity_id",
    "tick",
    "is_active",
    "publication_id",
    "source_tx",
    "source_row",
    "logical_time",
    "difference",
    "value",
];
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Change {
    entity: i32,
    value: i32,
    difference: i64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CompletedTransaction {
    id: i64,
    logical_time: i64,
    changes: Vec<Change>,
}
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct ChangeRow {
    world_id: String,
    run_id: String,
    entity_id: i32,
    tick: i32,
    is_active: bool,
    publication_id: String,
    source_tx: i64,
    source_row: i32,
    logical_time: i64,
    difference: i64,
    value: i32,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct FileEvidence {
    uri: String,
    size: u64,
    rows: u64,
    sha256: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Manifest {
    table_uuid: String,
    publication_id: String,
    grouping_prefix: String,
    schema_id: i32,
    partition_spec_id: i32,
    computation_version: String,
    representation: String,
    completed_from_inclusive: i64,
    completed_through_exclusive: i64,
    transactions: Vec<CompletedTransaction>,
    files: Vec<FileEvidence>,
    descriptors_avro: Vec<u8>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Receipt {
    digest: String,
    manifest: Manifest,
}

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn uri(path: &Path) -> Result<String> {
    ensure!(path.is_absolute(), "expected absolute local path");
    let text = path.to_str().context("non-UTF8 path")?;
    ensure!(
        !text.chars().any(|c| ['%', '#', '?'].contains(&c)),
        "URI-special path characters"
    );
    Ok(format!("file://{text}"))
}
fn local_path(location: &str) -> Result<PathBuf> {
    let path = location
        .strip_prefix("file://")
        .context("only local file:// URIs supported")?;
    ensure!(path.starts_with('/'), "nonabsolute file URI");
    Ok(PathBuf::from(path))
}
fn table_ident() -> Result<TableIdent> {
    Ok(TableIdent::from_strs(["spike", "change_history"])?)
}
fn schema() -> Result<Schema> {
    let types = [
        PrimitiveType::String,
        PrimitiveType::String,
        PrimitiveType::Int,
        PrimitiveType::Int,
        PrimitiveType::Boolean,
        PrimitiveType::String,
        PrimitiveType::Long,
        PrimitiveType::Int,
        PrimitiveType::Long,
        PrimitiveType::Long,
        PrimitiveType::Int,
    ];
    Ok(Schema::builder()
        .with_schema_id(0)
        .with_fields(
            COLUMN_NAMES
                .iter()
                .zip(types)
                .enumerate()
                .map(|(i, (name, ty))| {
                    Arc::new(NestedField::required(
                        (i + 1) as i32,
                        *name,
                        Type::Primitive(ty),
                    ))
                }),
        )
        .build()?)
}
fn transactions(id: &str) -> Result<Vec<CompletedTransaction>> {
    let tx = |id, changes| CompletedTransaction {
        id,
        logical_time: id,
        changes,
    };
    let change = |entity, value, difference| Change {
        entity,
        value,
        difference,
    };
    Ok(match id {
        "p1" => vec![
            tx(10, vec![change(42, 3, 2), change(7, 9, 1)]),
            tx(11, vec![]),
        ],
        "p2" => vec![
            tx(12, vec![change(42, 3, -2), change(42, 4, 1)]),
            tx(13, vec![change(7, 9, -1)]),
        ],
        _ => bail!("fixture publication must be p1 or p2"),
    })
}
fn expected_rows(id: &str, transactions: &[CompletedTransaction]) -> Vec<ChangeRow> {
    let mut rows = Vec::new();
    for tx in transactions {
        for (ordinal, change) in tx.changes.iter().enumerate() {
            rows.push(ChangeRow {
                world_id: "fixture".into(),
                run_id: "1".into(),
                entity_id: change.entity,
                tick: tx.logical_time as i32,
                is_active: true,
                publication_id: id.into(),
                source_tx: tx.id,
                source_row: ordinal as i32,
                logical_time: tx.logical_time,
                difference: change.difference,
                value: change.value,
            });
        }
    }
    rows.sort();
    rows
}
fn batch(rows: &[ChangeRow]) -> Result<RecordBatch> {
    let strings = |get: fn(&ChangeRow) -> &str| {
        Arc::new(StringArray::from_iter_values(rows.iter().map(get))) as ArrayRef
    };
    let ints = |get: fn(&ChangeRow) -> i32| {
        Arc::new(Int32Array::from_iter_values(rows.iter().map(get))) as ArrayRef
    };
    let longs = |get: fn(&ChangeRow) -> i64| {
        Arc::new(Int64Array::from_iter_values(rows.iter().map(get))) as ArrayRef
    };
    Ok(RecordBatch::try_new(
        Arc::new(schema_to_arrow_schema(&schema()?)?),
        vec![
            strings(|r| &r.world_id),
            strings(|r| &r.run_id),
            ints(|r| r.entity_id),
            ints(|r| r.tick),
            Arc::new(BooleanArray::from_iter(
                rows.iter().map(|r| Some(r.is_active)),
            )),
            strings(|r| &r.publication_id),
            longs(|r| r.source_tx),
            ints(|r| r.source_row),
            longs(|r| r.logical_time),
            longs(|r| r.difference),
            ints(|r| r.value),
        ],
    )?)
}
fn rows_from_batches(batches: Vec<RecordBatch>) -> Result<Vec<ChangeRow>> {
    let mut result = Vec::new();
    for b in batches {
        ensure!(
            b.num_columns() == COLUMN_NAMES.len(),
            "unexpected column count"
        );
        for (i, name) in COLUMN_NAMES.iter().enumerate() {
            ensure!(
                b.schema().field(i).name() == name && b.column(i).null_count() == 0,
                "unexpected field/nulls: {name}"
            );
        }
        let s = |c: usize, r: usize| {
            b.column(c)
                .as_any()
                .downcast_ref::<StringArray>()
                .context("string type")
                .map(|a| a.value(r).to_owned())
        };
        let i = |c: usize, r: usize| {
            b.column(c)
                .as_any()
                .downcast_ref::<Int32Array>()
                .context("int32 type")
                .map(|a| a.value(r))
        };
        let l = |c: usize, r: usize| {
            b.column(c)
                .as_any()
                .downcast_ref::<Int64Array>()
                .context("int64 type")
                .map(|a| a.value(r))
        };
        let active = b
            .column(4)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .context("bool type")?;
        for r in 0..b.num_rows() {
            result.push(ChangeRow {
                world_id: s(0, r)?,
                run_id: s(1, r)?,
                entity_id: i(2, r)?,
                tick: i(3, r)?,
                is_active: active.value(r),
                publication_id: s(5, r)?,
                source_tx: l(6, r)?,
                source_row: i(7, r)?,
                logical_time: l(8, r)?,
                difference: l(9, r)?,
                value: i(10, r)?,
            });
        }
    }
    result.sort();
    Ok(result)
}
async fn catalog(root: &Path, factory: Option<Arc<dyn StorageFactory>>) -> Result<SqlCatalog> {
    Ok(SqlCatalogBuilder::default()
        .uri(format!(
            "sqlite://{}?mode=rwc",
            root.join("catalog.sqlite").display()
        ))
        .warehouse_location(uri(&root.join("warehouse"))?)
        .sql_bind_style(SqlBindStyle::QMark)
        .prop("pool.max-connections", "1")
        .with_storage_factory(factory.unwrap_or_else(|| Arc::new(LocalFsStorageFactory)))
        .load("roundtrip", HashMap::new())
        .await?)
}
async fn ledger(root: &Path) -> Result<SqlitePool> {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&format!(
            "sqlite://{}?mode=rwc",
            root.join("receipts.sqlite").display()
        ))
        .await?;
    sqlx::query("PRAGMA synchronous=FULL")
        .execute(&pool)
        .await?;
    sqlx::query("CREATE TABLE IF NOT EXISTS receipts (id TEXT PRIMARY KEY, digest TEXT NOT NULL, receipt TEXT NOT NULL, snapshot INTEGER, status TEXT NOT NULL)").execute(&pool).await?;
    Ok(pool)
}
fn publication_lock(root: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(root.join("publication.lock"))?;
    file.lock()?;
    Ok(file)
}
async fn init(root: &Path) -> Result<()> {
    fs::create_dir_all(root)?;
    let c = catalog(root, None).await?;
    c.create_namespace(&NamespaceIdent::new("spike".into()), HashMap::new())
        .await?;
    c.create_table(
        &NamespaceIdent::new("spike".into()),
        TableCreation::builder()
            .name("change_history".into())
            .location(uri(&root.join("warehouse/change_history"))?)
            .schema(schema()?)
            .format_version(FormatVersion::V3)
            .properties(HashMap::from([(
                "commit.retry.num-retries".into(),
                "0".into(),
            )]))
            .build(),
    )
    .await?;
    validate_table(&c.load_table(&table_ident()?).await?)?;
    ledger(root).await?.close().await;
    Ok(())
}
fn validate_table(table: &Table) -> Result<()> {
    ensure!(
        table.metadata().format_version() == FormatVersion::V3,
        "table is not V3"
    );
    ensure!(
        table.metadata().current_schema().as_ref() == &schema()?,
        "schema changed"
    );
    ensure!(
        table
            .metadata()
            .default_partition_spec()
            .fields()
            .is_empty(),
        "table must be unpartitioned"
    );
    Ok(())
}
async fn load_receipt(pool: &SqlitePool, id: &str) -> Result<Receipt> {
    let raw: String = sqlx::query("SELECT receipt FROM receipts WHERE id=?")
        .bind(id)
        .fetch_one(pool)
        .await?
        .try_get(0)?;
    let receipt: Receipt = serde_json::from_str(&raw)?;
    ensure!(
        digest(&serde_json::to_vec(&receipt.manifest)?) == receipt.digest,
        "receipt digest mismatch"
    );
    Ok(receipt)
}
async fn status(pool: &SqlitePool, id: &str) -> Result<(String, Option<i64>)> {
    let row = sqlx::query("SELECT status,snapshot FROM receipts WHERE id=?")
        .bind(id)
        .fetch_one(pool)
        .await?;
    Ok((row.try_get(0)?, row.try_get(1)?))
}
async fn produce(root: &Path, id: &str, conflict: bool) -> Result<Receipt> {
    let _lock = publication_lock(root)?;
    let table = catalog(root, None)
        .await?
        .load_table(&table_ident()?)
        .await?;
    validate_table(&table)?;
    let pool = ledger(root).await?;
    let mut txs = transactions(id)?;
    if conflict {
        txs[0].changes[0].value += 1;
    }
    let exists: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM receipts WHERE id=?")
        .bind(id)
        .fetch_one(&pool)
        .await?;
    if exists != 0 {
        let receipt = load_receipt(&pool, id).await?;
        ensure!(
            receipt.manifest.transactions == txs,
            "publication identity/content conflict"
        );
        verify_files(&table, &receipt)?;
        return Ok(receipt);
    }
    let rows = expected_rows(id, &txs);
    ensure!(
        !rows.is_empty(),
        "empty flush outside fixture; empty source transactions are retained"
    );
    let group = root.join("external").join(GROUP);
    fs::create_dir_all(&group)?;
    let directory = group.join(id);
    fs::create_dir(&directory)
        .context("publication directory exists; refuse overwrite/adoption of unknown bytes")?;
    let staging = directory.join("part-00000.inprogress");
    let final_path = directory.join("part-00000.parquet");
    let mut writer = ParquetWriterBuilder::new(
        WriterProperties::builder().build(),
        table.metadata().current_schema().clone(),
    )
    .build(table.file_io().new_output(uri(&staging)?)?)
    .await?;
    // A flush buffers multiple source transactions; it is not a per-tick file.
    for tx in &txs {
        if !tx.changes.is_empty() {
            writer
                .write(&batch(&expected_rows(id, std::slice::from_ref(tx)))?)
                .await?;
        }
    }
    let builders = writer.close().await?;
    fs::rename(&staging, &final_path)?;
    File::open(&directory)?.sync_all()?;
    let final_uri = uri(&final_path)?;
    let mut files = Vec::new();
    for mut builder in builders {
        files.push(
            builder
                .file_path(final_uri.clone())
                .partition_spec_id(table.metadata().default_partition_spec_id())
                .build()?,
        );
    }
    ensure!(
        files.len() == 1,
        "fixture expects one immutable file per flush"
    );
    let file_evidence = FileEvidence {
        uri: final_uri,
        size: fs::metadata(&final_path)?.len(),
        rows: rows.len() as u64,
        sha256: digest(&fs::read(&final_path)?),
    };
    let mut descriptors = Vec::new();
    write_data_files_to_avro(
        &mut descriptors,
        files,
        table.metadata().default_partition_type(),
        FormatVersion::V3,
    )?;
    let manifest = Manifest {
        table_uuid: table.metadata().uuid().to_string(),
        publication_id: id.into(),
        grouping_prefix: GROUP.into(),
        schema_id: table.metadata().current_schema_id(),
        partition_spec_id: table.metadata().default_partition_spec_id(),
        computation_version: "synthetic-transactions-v1".into(),
        representation: "changes".into(),
        completed_from_inclusive: txs.first().context("empty transaction list")?.logical_time,
        completed_through_exclusive: txs.last().context("empty transaction list")?.logical_time + 1,
        transactions: txs,
        files: vec![file_evidence],
        descriptors_avro: descriptors,
    };
    let receipt = Receipt {
        digest: digest(&serde_json::to_vec(&manifest)?),
        manifest,
    };
    verify_files(&table, &receipt)?;
    sqlx::query("INSERT INTO receipts(id,digest,receipt,status) VALUES(?,?,?,'files_durable')")
        .bind(id)
        .bind(&receipt.digest)
        .bind(serde_json::to_string(&receipt)?)
        .execute(&pool)
        .await?;
    Ok(receipt)
}
fn descriptors(table: &Table, receipt: &Receipt) -> Result<Vec<DataFile>> {
    Ok(read_data_files_from_avro(
        &mut Cursor::new(&receipt.manifest.descriptors_avro),
        table.metadata().current_schema(),
        receipt.manifest.partition_spec_id,
        table.metadata().default_partition_type(),
        FormatVersion::V3,
    )?)
}
fn verify_files(table: &Table, receipt: &Receipt) -> Result<()> {
    validate_table(table)?;
    let m = &receipt.manifest;
    ensure!(
        m.table_uuid == table.metadata().uuid().to_string()
            && m.schema_id == table.metadata().current_schema_id()
            && m.partition_spec_id == table.metadata().default_partition_spec_id(),
        "receipt/table identity or schema conflict"
    );
    ensure!(
        m.grouping_prefix == GROUP
            && m.representation == "changes"
            && m.computation_version == "synthetic-transactions-v1",
        "wrong receipt meaning"
    );
    ensure!(
        m.transactions == transactions(&m.publication_id)?
            && m.completed_from_inclusive
                == m.transactions
                    .first()
                    .context("empty transactions")?
                    .logical_time
            && m.completed_through_exclusive
                == m.transactions
                    .last()
                    .context("empty transactions")?
                    .logical_time
                    + 1,
        "source transaction boundaries or completion interval changed"
    );
    let files = descriptors(table, receipt)?;
    ensure!(
        files.len() == m.files.len() && !files.is_empty(),
        "incomplete file set"
    );
    let mut paths = HashSet::new();
    let mut actual = Vec::new();
    for (file, evidence) in files.iter().zip(&m.files) {
        ensure!(
            paths.insert(file.file_path()),
            "duplicate file in publication"
        );
        ensure!(
            file.file_path() == evidence.uri
                && file.file_size_in_bytes() == evidence.size
                && file.record_count() == evidence.rows,
            "descriptor/evidence mismatch"
        );
        let path = local_path(&evidence.uri)?;
        ensure!(
            path.to_string_lossy()
                .contains(&format!("/{GROUP}/{}/", m.publication_id)),
            "file outside publication grouping"
        );
        ensure!(
            fs::metadata(&path)?.len() == evidence.size
                && digest(&fs::read(&path)?) == evidence.sha256,
            "immutable file changed"
        );
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&path)?)?;
        let physical = reader.metadata().file_metadata().schema_descr();
        ensure!(
            physical.num_columns() == COLUMN_NAMES.len(),
            "unexpected parquet field count"
        );
        for (i, column) in physical.columns().iter().enumerate() {
            use parquet::basic::{Repetition, Type as PhysicalType};
            let types = [
                PhysicalType::BYTE_ARRAY,
                PhysicalType::BYTE_ARRAY,
                PhysicalType::INT32,
                PhysicalType::INT32,
                PhysicalType::BOOLEAN,
                PhysicalType::BYTE_ARRAY,
                PhysicalType::INT64,
                PhysicalType::INT32,
                PhysicalType::INT64,
                PhysicalType::INT64,
                PhysicalType::INT32,
            ];
            ensure!(
                column.self_type().get_basic_info().has_id()
                    && column.self_type().get_basic_info().id() == (i + 1) as i32,
                "missing/wrong physical Parquet field ID"
            );
            ensure!(
                column.name() == COLUMN_NAMES[i]
                    && column.physical_type() == types[i]
                    && column.self_type().get_basic_info().repetition() == Repetition::REQUIRED,
                "wrong physical Parquet field name, type, or requiredness"
            );
        }
        actual.extend(rows_from_batches(
            reader
                .build()?
                .collect::<std::result::Result<Vec<_>, _>>()?,
        )?);
    }
    actual.sort();
    ensure!(
        actual == expected_rows(&m.publication_id, &m.transactions),
        "direct Parquet rows differ from source transactions"
    );
    Ok(())
}
fn preflight_rejection_checks(table: &Table, receipt: &Receipt) -> Result<Value> {
    let mut duplicate = receipt.clone();
    let files = descriptors(table, receipt)?;
    let mut duplicated_files = files.clone();
    duplicated_files.extend(files);
    duplicate
        .manifest
        .files
        .extend(receipt.manifest.files.clone());
    duplicate.manifest.descriptors_avro.clear();
    write_data_files_to_avro(
        &mut duplicate.manifest.descriptors_avro,
        duplicated_files,
        table.metadata().default_partition_type(),
        FormatVersion::V3,
    )?;
    let duplicate_error = verify_files(table, &duplicate)
        .expect_err("duplicate files accepted")
        .to_string();
    ensure!(
        duplicate_error.contains("duplicate file"),
        "wrong duplicate rejection: {duplicate_error}"
    );
    let mut wrong_hash = receipt.clone();
    wrong_hash.manifest.files[0].sha256 = "0".repeat(64);
    let hash_error = verify_files(table, &wrong_hash)
        .expect_err("wrong digest accepted")
        .to_string();
    ensure!(
        hash_error.contains("immutable file changed"),
        "wrong hash rejection: {hash_error}"
    );
    Ok(json!({"duplicate_file_set":duplicate_error,"file_hash_mismatch":hash_error}))
}
async fn scan(table: &Table, snapshot: Option<i64>) -> Result<Vec<ChangeRow>> {
    let mut builder = table
        .scan()
        .select(COLUMN_NAMES)
        .with_concurrency_limit(1)
        .with_data_file_concurrency_limit(1);
    if let Some(snapshot) = snapshot {
        builder = builder.snapshot_id(snapshot);
    }
    rows_from_batches(builder.build()?.to_arrow().await?.try_collect().await?)
}
async fn committed_publication(table: &Table, receipt: &Receipt) -> Result<Option<i64>> {
    let mut found = None;
    for snapshot in table.metadata().snapshots() {
        let props = &snapshot.summary().additional_properties;
        if props.get(PUB_KEY) != Some(&receipt.manifest.publication_id) {
            continue;
        }
        ensure!(found.is_none(), "publication appears in multiple snapshots");
        ensure!(
            props.get(DIGEST_KEY) == Some(&receipt.digest),
            "committed identity/content conflict"
        );
        let list = table.manifest_list_reader(snapshot).load().await?;
        let mut added = Vec::new();
        for manifest in list.entries() {
            ensure!(
                manifest.manifest_path.starts_with("file:///"),
                "nonabsolute manifest URI"
            );
            if manifest.added_snapshot_id == snapshot.snapshot_id() {
                ensure!(
                    manifest.first_row_id == snapshot.first_row_id()
                        && manifest.first_row_id.is_some()
                        && manifest.added_rows_count == snapshot.added_rows_count()
                        && manifest.added_files_count == Some(1),
                    "new v3 manifest has wrong lineage or file allocation"
                );
            }
            for entry in manifest.load_manifest(table.file_io()).await?.entries() {
                if entry.status == ManifestStatus::Added
                    && entry.snapshot_id() == Some(snapshot.snapshot_id())
                {
                    added.push((
                        entry.data_file.file_path().to_string(),
                        entry.data_file.record_count(),
                        entry.data_file.file_size_in_bytes(),
                    ));
                }
            }
        }
        let mut expected: Vec<_> = receipt
            .manifest
            .files
            .iter()
            .map(|f| (f.uri.clone(), f.rows, f.size))
            .collect();
        added.sort();
        expected.sort();
        ensure!(
            added == expected,
            "committed added-file set incomplete or duplicated"
        );
        ensure!(
            snapshot
                .row_range()
                .context("snapshot has no v3 row range")?
                .1
                == receipt.manifest.files.iter().map(|f| f.rows).sum::<u64>(),
            "wrong row allocation"
        );
        let rows: Vec<_> = scan(table, Some(snapshot.snapshot_id()))
            .await?
            .into_iter()
            .filter(|row| row.publication_id == receipt.manifest.publication_id)
            .collect();
        ensure!(
            rows == expected_rows(
                &receipt.manifest.publication_id,
                &receipt.manifest.transactions
            ),
            "snapshot rows differ from receipt"
        );
        found = Some(snapshot.snapshot_id());
    }
    Ok(found)
}
async fn append(c: &SqlCatalog, table: &Table, receipt: &Receipt) -> iceberg::Result<Table> {
    let files = descriptors(table, receipt)
        .map_err(|e| iceberg::Error::new(ErrorKind::DataInvalid, e.to_string()))?;
    let tx = Transaction::new(table);
    tx.fast_append()
        .with_check_duplicate(true)
        .add_data_files(files)
        .set_snapshot_properties(HashMap::from([
            (PUB_KEY.into(), receipt.manifest.publication_id.clone()),
            (DIGEST_KEY.into(), receipt.digest.clone()),
        ]))
        .apply(tx)?
        .commit(c)
        .await
}
async fn reconcile(root: &Path, id: &str, fault: &str) -> Result<Value> {
    let _lock = publication_lock(root)?;
    let pool = ledger(root).await?;
    let receipt = load_receipt(&pool, id).await?;
    let c = catalog(root, None).await?;
    let table = c.load_table(&table_ident()?).await?;
    verify_files(&table, &receipt)?;
    let prior = committed_publication(&table, &receipt).await?;
    if prior.is_none() {
        if fault == "before-commit" {
            std::process::exit(72);
        }
        // Even Ok is not authority: the pinned SQL catalog drops DB COMMIT errors.
        let outcome = append(&c, &table, &receipt).await;
        if fault == "after-commit" && outcome.is_ok() {
            std::process::exit(73);
        }
        if let Err(error) = outcome {
            eprintln!("commit returned error; checking authoritative state: {error}");
        }
    }
    let authoritative = catalog(root, None)
        .await?
        .load_table(&table_ident()?)
        .await?;
    let snapshot = committed_publication(&authoritative, &receipt)
        .await?
        .context("publication not authoritative; receipt remains pending")?;
    let acknowledged = sqlx::query(
        "UPDATE receipts SET snapshot=?,status='iceberg_visible' WHERE id=? AND digest=?",
    )
    .bind(snapshot)
    .bind(id)
    .bind(&receipt.digest)
    .execute(&pool)
    .await?;
    ensure!(
        acknowledged.rows_affected() == 1,
        "receipt acknowledgment identity changed"
    );
    Ok(
        json!({"publication_id": id, "snapshot": snapshot, "adopted": prior.is_some(), "digest": receipt.digest}),
    )
}
async fn evidence(root: &Path) -> Result<Value> {
    let table = catalog(root, None)
        .await?
        .load_table(&table_ident()?)
        .await?;
    let rows = scan(&table, None).await?;
    let files: Vec<_> = table
        .scan()
        .build()?
        .plan_files()
        .await?
        .try_collect()
        .await?;
    let mut snapshots = Vec::new();
    for snapshot in table.metadata().snapshots() {
        let list = table.manifest_list_reader(snapshot).load().await?;
        let manifests: Vec<_> = list
            .entries()
            .iter()
            .map(|manifest| {
                json!({
                    "path":manifest.manifest_path,"first_row_id":manifest.first_row_id,
                    "added_snapshot_id":manifest.added_snapshot_id,
                    "added_rows_count":manifest.added_rows_count,
                    "added_files_count":manifest.added_files_count
                })
            })
            .collect();
        snapshots.push(
            json!({"id":snapshot.snapshot_id(), "parent":snapshot.parent_snapshot_id(),
            "row_range":snapshot.row_range(), "summary":snapshot.summary().additional_properties,
            "manifests":manifests}),
        );
    }
    snapshots.sort_by_key(|s| s["row_range"][0].as_u64());
    let mut next_row = 0;
    let mut parent = None;
    for snapshot in &snapshots {
        ensure!(
            snapshot["row_range"][0].as_u64() == Some(next_row),
            "row lineage gap or overlap"
        );
        ensure!(
            snapshot["parent"].as_i64() == parent,
            "snapshot parent chain changed"
        );
        next_row += snapshot["row_range"][1]
            .as_u64()
            .context("missing added rows")?;
        parent = snapshot["id"].as_i64();
    }
    ensure!(
        next_row == table.metadata().next_row_id(),
        "lineage allocations differ from table counter"
    );
    ensure!(
        parent == table.metadata().current_snapshot_id(),
        "current snapshot differs from chain head"
    );
    let mut paths: Vec<_> = files.iter().map(|f| f.data_file_path.clone()).collect();
    paths.sort();
    Ok(
        json!({"table_uuid":table.metadata().uuid().to_string(), "metadata_location":table.metadata_location(), "format_version":3, "next_row_id":table.metadata().next_row_id(), "snapshots":snapshots, "files":paths, "rows":rows}),
    )
}
fn child(root: &Path, mode: &str, id: &str, fault: &str, expected_code: i32) -> Result<Value> {
    let result = Command::new(std::env::current_exe()?)
        .args([mode, root.to_str().context("root path")?, id, fault])
        .output()?;
    ensure!(
        result.status.code() == Some(expected_code),
        "child {mode}/{id}/{fault}: expected {expected_code}, got {:?}: {}",
        result.status.code(),
        String::from_utf8_lossy(&result.stderr)
    );
    if expected_code == 0 {
        Ok(serde_json::from_slice(&result.stdout)?)
    } else {
        Ok(
            json!({"fault":fault,"exit_code":expected_code,"stderr":String::from_utf8_lossy(&result.stderr)}),
        )
    }
}
async fn cas_oracle(root: &Path) -> Result<Value> {
    init(root).await?;
    child(root, "produce", "p1", "", 0)?;
    child(root, "produce", "p2", "", 0)?;
    let pool = ledger(root).await?;
    let p1 = load_receipt(&pool, "p1").await?;
    let p2 = load_receipt(&pool, "p2").await?;
    let gate = Arc::new(faults::GateFactory::default());
    let a = catalog(root, Some(gate.clone())).await?;
    let b = catalog(root, Some(gate.clone())).await?;
    let ta = a.load_table(&table_ident()?).await?;
    let tb = b.load_table(&table_ident()?).await?;
    gate.arm();
    let (ra, rb) = tokio::join!(append(&a, &ta, &p1), append(&b, &tb, &p2));
    ensure!(
        gate.arrivals() == 2,
        "CAS gate did not stage two metadata files"
    );
    let outcomes = [&ra, &rb].map(|r| {
        r.as_ref()
            .map(|_| "ok".to_string())
            .unwrap_or_else(|e| format!("{:?}", e.kind()))
    });
    ensure!(
        ra.is_ok() != rb.is_ok(),
        "expected exactly one CAS success: {outcomes:?}"
    );
    let err = if let Err(e) = &ra {
        e
    } else {
        rb.as_ref().unwrap_err()
    };
    ensure!(
        err.kind() == ErrorKind::CatalogCommitConflicts,
        "expected actual SQL CAS conflict: {err}"
    );
    let first = evidence(root).await?;
    ensure!(
        first["snapshots"].as_array().context("snapshots")?.len() == 1,
        "CAS exposed more than one snapshot"
    );
    child(root, "reconcile", "p1", "", 0)?;
    child(root, "reconcile", "p2", "", 0)?;
    let final_evidence = evidence(root).await?;
    let mut expected = expected_rows("p1", &transactions("p1")?);
    expected.extend(expected_rows("p2", &transactions("p2")?));
    expected.sort();
    ensure!(
        serde_json::from_value::<Vec<ChangeRow>>(final_evidence["rows"].clone())? == expected,
        "CAS lost/duplicated rows"
    );
    ensure!(
        final_evidence["next_row_id"] == 5,
        "CAS lineage counter drift"
    );
    Ok(
        json!({"outcomes":outcomes,"metadata_barrier_arrivals":gate.arrivals(),"after_first_cas":first,"after_reconciliation":final_evidence}),
    )
}
async fn oracle(root: &Path) -> Result<Value> {
    ensure!(!root.exists(), "oracle requires a fresh directory");
    fs::create_dir_all(root)?;
    let root = root.canonicalize()?;
    let history = root.join("history");
    init(&history).await?;
    let baseline = evidence(&history).await?;
    let p1 = child(&history, "produce", "p1", "", 0)?;
    let initial_table = catalog(&history, None)
        .await?
        .load_table(&table_ident()?)
        .await?;
    let initial_pool = ledger(&history).await?;
    let preflight =
        preflight_rejection_checks(&initial_table, &load_receipt(&initial_pool, "p1").await?)?;
    initial_pool.close().await;
    let invisible = evidence(&history).await?;
    ensure!(
        invisible == baseline,
        "external file leaked into Iceberg before commit"
    );
    let p1_commit = child(&history, "reconcile", "p1", "", 0)?;
    let first = evidence(&history).await?;
    ensure!(
        first["rows"].as_array().context("rows")?.len() == 2 && first["next_row_id"] == 2,
        "P1 rows/lineage mismatch"
    );
    let p2 = child(&history, "produce", "p2", "", 0)?;
    ensure!(
        evidence(&history).await? == first,
        "P2 leaked before commit"
    );
    let lost_ack = child(&history, "reconcile", "p2", "after-commit", 73)?;
    let pool = ledger(&history).await?;
    ensure!(
        status(&pool, "p2").await? == ("files_durable".into(), None),
        "lost-ack fault acknowledged receipt"
    );
    let committed_before_recovery = evidence(&history).await?;
    let recovery = child(&history, "reconcile", "p2", "", 0)?;
    ensure!(
        recovery["adopted"] == true,
        "lost acknowledgment was not adopted"
    );
    ensure!(
        evidence(&history).await? == committed_before_recovery,
        "lost-ack recovery appended twice"
    );
    let retry1 = child(&history, "reconcile", "p1", "", 0)?;
    let retry2 = child(&history, "reconcile", "p2", "", 0)?;
    ensure!(
        retry1["adopted"] == true && retry2["adopted"] == true,
        "retries were not adopted"
    );
    ensure!(
        evidence(&history).await? == committed_before_recovery,
        "retry changed table"
    );
    child(&history, "produce", "p1", "", 0)?;
    let conflict = child(&history, "produce", "p1", "conflict", 1)?;
    ensure!(
        evidence(&history).await? == committed_before_recovery,
        "identity conflict changed table"
    );
    let table = catalog(&history, None)
        .await?
        .load_table(&table_ident()?)
        .await?;
    ensure!(
        scan(&table, p1_commit["snapshot"].as_i64()).await?
            == expected_rows("p1", &transactions("p1")?),
        "old snapshot changed"
    );
    let rows = scan(&table, None).await?;
    let mut expected = expected_rows("p1", &transactions("p1")?);
    expected.extend(expected_rows("p2", &transactions("p2")?));
    expected.sort();
    ensure!(
        rows == expected
            && table.metadata().next_row_id() == 5
            && table.metadata().snapshots().len() == 2,
        "exact history/snapshot/lineage mismatch"
    );
    let mut state = BTreeMap::new();
    for row in &rows {
        *state.entry((row.entity_id, row.value)).or_insert(0i64) += row.difference;
    }
    state.retain(|_, count| *count != 0);
    ensure!(
        state == BTreeMap::from([((42, 4), 1)]),
        "signed history reconstructed wrong state"
    );
    let before_dir = root.join("before-commit");
    init(&before_dir).await?;
    child(&before_dir, "produce", "p1", "", 0)?;
    let before_fault = evidence(&before_dir).await?;
    child(&before_dir, "reconcile", "p1", "before-commit", 72)?;
    ensure!(
        evidence(&before_dir).await? == before_fault,
        "precommit fault changed authority"
    );
    child(&before_dir, "reconcile", "p1", "", 0)?;
    let cas = cas_oracle(&root.join("cas")).await?;
    let result = json!({"status":"passed","scope":"local synthetic signed change-history; not Archetype integration","versions":{"iceberg":"0.10.1","iceberg-catalog-sql":"0.10.1","arrow-array":"58.3.0","parquet":"58.1.0","sqlx":"0.8.6","thrift":"0.17.0"},"run_directory":root,"flushes":[p1,p2],"preflight_rejections":preflight,"invisible_before_commit":invisible,"p1_commit":p1_commit,"lost_ack":lost_ack,"recovery":recovery,"retry_results":[retry1,retry2],"identity_conflict":conflict,"history":evidence(&history).await?,"before_commit_recovered":evidence(&before_dir).await?,"cas":cas,"omissions":["virtual lineage columns","injected SQLite COMMIT failure","power-loss durability","cloud storage","multi-host reconcilers","multi-table atomicity","empty entire flush","Archetype/DDlog integration"]});
    fs::write(
        root.join("evidence.json"),
        serde_json::to_vec_pretty(&result)?,
    )?;
    Ok(result)
}
#[tokio::main(worker_threads = 2)]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    let mode = args.get(1).map(String::as_str).unwrap_or("oracle");
    let path = args.get(2).context(
        "usage: iceberg-v3-roundtrip-spike oracle FRESH_DIR | produce/reconcile DIR p1/p2 [fault]",
    )?;
    let root = std::env::current_dir()?.join(path);
    let id = args.get(3).map(String::as_str).unwrap_or("p1");
    let fault = args.get(4).map(String::as_str).unwrap_or("");
    let result = match mode {
        "oracle" => oracle(&root).await?,
        "produce" => {
            let receipt = produce(&root, id, fault == "conflict").await?;
            json!({"publication_id":id,"digest":receipt.digest,"transactions":receipt.manifest.transactions,"files":receipt.manifest.files,"status":"files_durable","completed_through_exclusive":receipt.manifest.completed_through_exclusive})
        }
        "reconcile" => reconcile(&root, id, fault).await?,
        "verify" => evidence(&root).await?,
        _ => bail!("unknown mode"),
    };
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}
