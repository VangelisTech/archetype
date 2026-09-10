//! One controlled all-input checkpoint, using the previously tested Iceberg path.
use anyhow::{Context, Result, ensure};
use arrow_array::{ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray};
use futures::TryStreamExt;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::io::LocalFsStorageFactory;
use iceberg::spec::{FormatVersion, NestedField, PrimitiveType, Schema, Type};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::file_writer::{FileWriter, FileWriterBuilder, ParquetWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_sql::{SqlBindStyle, SqlCatalog, SqlCatalogBuilder};
use parquet::file::properties::WriterProperties;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
};

fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn uri(path: &Path) -> Result<String> {
    let value = path.to_str().context("UTF8 path")?;
    ensure!(
        path.is_absolute() && !value.contains(['%', '?', '#', ' ']),
        "simple absolute local path required"
    );
    Ok(format!("file://{value}"))
}
fn local(uri: &str) -> Result<PathBuf> {
    Ok(PathBuf::from(
        uri.strip_prefix("file:///")
            .map(|p| format!("/{p}"))
            .context("absolute local URI")?,
    ))
}
fn ident() -> Result<TableIdent> {
    Ok(TableIdent::from_strs(["recovery", "input_checkpoint"])?)
}
fn schema() -> Result<Schema> {
    Ok(Schema::builder()
        .with_schema_id(0)
        .with_fields(
            [
                ("relation", PrimitiveType::String),
                ("arity", PrimitiveType::Int),
                ("f0", PrimitiveType::Long),
                ("f1", PrimitiveType::Long),
            ]
            .into_iter()
            .enumerate()
            .map(|(id, (name, ty))| {
                Arc::new(NestedField::required(
                    id as i32 + 1,
                    name,
                    Type::Primitive(ty),
                ))
            }),
        )
        .build()?)
}
async fn catalog(root: &Path) -> Result<SqlCatalog> {
    Ok(SqlCatalogBuilder::default()
        .uri(format!(
            "sqlite://{}?mode=rwc",
            root.join("catalog.sqlite").display()
        ))
        .warehouse_location(uri(&root.join("warehouse"))?)
        .sql_bind_style(SqlBindStyle::QMark)
        .prop("pool.max-connections", "1")
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("recovery", HashMap::new())
        .await?)
}
fn canonical_facts(value: &Value) -> Result<BTreeMap<String, Vec<Vec<i64>>>> {
    let mut facts: BTreeMap<String, Vec<Vec<i64>>> = serde_json::from_value(value.clone())?;
    ensure!(
        facts.keys().map(String::as_str).collect::<Vec<_>>() == ["edges", "vertices"],
        "checkpoint must contain ALL and only the two input relations"
    );
    for (relation, rows) in &mut facts {
        let arity = if relation == "vertices" { 1 } else { 2 };
        ensure!(
            rows.iter().all(|row| row.len() == arity),
            "wrong input arity"
        );
        rows.sort();
        ensure!(
            rows.iter().collect::<HashSet<_>>().len() == rows.len(),
            "duplicate input fact in set checkpoint"
        );
    }
    Ok(facts)
}
async fn read(root: &Path) -> Result<Value> {
    let table = catalog(root).await?.load_table(&ident()?).await?;
    ensure!(
        table.metadata().format_version() == FormatVersion::V3
            && table.metadata().current_schema().as_ref() == &schema()?,
        "wrong checkpoint table schema/version"
    );
    ensure!(
        table.metadata().snapshots().len() == 1,
        "fixture requires exactly one committed checkpoint"
    );
    let snapshot = table
        .metadata()
        .current_snapshot()
        .context("checkpoint not visible")?;
    let props = &snapshot.summary().additional_properties;
    let manifest_uri = props
        .get("recovery.manifest-uri")
        .context("missing manifest URI")?;
    let manifest_bytes = fs::read(local(manifest_uri)?)?;
    ensure!(
        Some(&hash(&manifest_bytes)) == props.get("recovery.manifest-sha256"),
        "recovery manifest hash mismatch"
    );
    let manifest: Value = serde_json::from_slice(&manifest_bytes)?;
    ensure!(
        manifest["checkpoint_id"].as_str()
            == props.get("recovery.checkpoint-id").map(String::as_str),
        "checkpoint identity mismatch"
    );
    let evidence = &manifest["data_file"];
    let data_uri = evidence["uri"].as_str().context("data URI")?;
    let data_bytes = fs::read(local(data_uri)?)?;
    ensure!(
        hash(&data_bytes) == evidence["sha256"]
            && data_bytes.len() as u64 == evidence["bytes"].as_u64().context("file size")?,
        "immutable checkpoint data changed"
    );
    let tasks: Vec<_> = table
        .scan()
        .snapshot_id(snapshot.snapshot_id())
        .build()?
        .plan_files()
        .await?
        .try_collect()
        .await?;
    ensure!(
        tasks.len() == 1 && tasks[0].data_file_path == data_uri,
        "catalog file set differs from checkpoint manifest"
    );
    let batches: Vec<RecordBatch> = table
        .scan()
        .snapshot_id(snapshot.snapshot_id())
        .select(["relation", "arity", "f0", "f1"])
        .build()?
        .to_arrow()
        .await?
        .try_collect()
        .await?;
    let mut facts = BTreeMap::from([
        ("edges".to_string(), vec![]),
        ("vertices".to_string(), vec![]),
    ]);
    for batch in batches {
        ensure!(
            batch.columns().iter().all(|a| a.null_count() == 0),
            "null checkpoint field"
        );
        let relation = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("relation type")?;
        let arity = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .context("arity type")?;
        let f0 = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .context("f0 type")?;
        let f1 = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .context("f1 type")?;
        for i in 0..batch.num_rows() {
            let name = relation.value(i);
            let n = arity.value(i);
            ensure!(
                (name == "vertices" && n == 1 && f1.value(i) == 0) || (name == "edges" && n == 2),
                "invalid tagged relation row"
            );
            let row = if n == 1 {
                vec![f0.value(i)]
            } else {
                vec![f0.value(i), f1.value(i)]
            };
            facts.get_mut(name).context("unknown relation")?.push(row);
        }
    }
    let facts = canonical_facts(&serde_json::to_value(facts)?)?;
    let fact_bytes = serde_json::to_vec(&facts)?;
    ensure!(
        hash(&fact_bytes) == manifest["input_facts_sha256"],
        "Iceberg input facts differ from committed checkpoint"
    );
    let count = facts.values().map(Vec::len).sum::<usize>();
    ensure!(
        snapshot.row_range() == Some((0, count as u64))
            && table.metadata().next_row_id() == count as u64,
        "wrong checkpoint lineage allocation"
    );
    for (name, rows) in &facts {
        ensure!(
            manifest["input_relations"][name]["count"].as_u64() == Some(rows.len() as u64),
            "input completeness count mismatch"
        );
    }
    Ok(
        json!({"manifest":manifest,"facts":facts,"catalog_snapshot":snapshot.snapshot_id(),"catalog_table_uuid":table.metadata().uuid().to_string(),"manifest_sha256":hash(&manifest_bytes),"metadata_location":table.metadata_location(),"row_range":snapshot.row_range(),"source":"Iceberg-pinned snapshot Arrow scan"}),
    )
}
async fn publish(root: &Path, input: &Path) -> Result<Value> {
    ensure!(
        !root.exists(),
        "single checkpoint publisher requires fresh directory; no implicit retry"
    );
    let input: Value = serde_json::from_slice(&fs::read(input)?)?;
    let facts = canonical_facts(&input["facts"])?;
    let mut manifest = input["manifest"].clone();
    ensure!(manifest.is_object(), "manifest object required");
    ensure!(
        manifest["kind"] == "controlled-checkpoint-v1",
        "unexpected recovery contract"
    );
    let checkpoint = manifest["checkpoint_id"]
        .as_str()
        .context("checkpoint ID")?
        .to_owned();
    ensure!(
        checkpoint
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-'),
        "unsafe checkpoint name"
    );
    ensure!(
        facts.values().any(|rows| !rows.is_empty()),
        "empty whole checkpoint omitted"
    );
    fs::create_dir_all(root)?;
    let c = catalog(root).await?;
    c.create_namespace(&NamespaceIdent::new("recovery".into()), HashMap::new())
        .await?;
    let table = c
        .create_table(
            &NamespaceIdent::new("recovery".into()),
            TableCreation::builder()
                .name("input_checkpoint".into())
                .location(uri(&root.join("warehouse/input_checkpoint"))?)
                .schema(schema()?)
                .format_version(FormatVersion::V3)
                .build(),
        )
        .await?;
    let directory = root.join("external").join(&checkpoint);
    fs::create_dir_all(&directory)?;
    let path = directory.join("inputs.parquet");
    let mut names = vec![];
    let mut arities = vec![];
    let mut first = vec![];
    let mut second = vec![];
    for (name, rows) in &facts {
        for row in rows {
            names.push(name.as_str());
            arities.push(row.len() as i32);
            first.push(row[0]);
            second.push(*row.get(1).unwrap_or(&0));
        }
    }
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(names)),
        Arc::new(Int32Array::from(arities)),
        Arc::new(Int64Array::from(first)),
        Arc::new(Int64Array::from(second)),
    ];
    let batch = RecordBatch::try_new(Arc::new(schema_to_arrow_schema(&schema()?)?), arrays)?;
    let mut writer = ParquetWriterBuilder::new(
        WriterProperties::builder().build(),
        table.metadata().current_schema().clone(),
    )
    .build(table.file_io().new_output(uri(&path)?)?)
    .await?;
    writer.write(&batch).await?;
    let builders = writer.close().await?;
    File::open(&directory)?.sync_all()?;
    let mut files = vec![];
    for mut builder in builders {
        files.push(
            builder
                .partition_spec_id(table.metadata().default_partition_spec_id())
                .build()?,
        );
    }
    ensure!(
        files.len() == 1 && files[0].record_count() == batch.num_rows() as u64,
        "checkpoint file count mismatch"
    );
    manifest["input_facts_sha256"] = json!(hash(&serde_json::to_vec(&facts)?));
    manifest["data_file"] = json!({"uri":uri(&path)?,"bytes":fs::metadata(&path)?.len(),"rows":batch.num_rows(),"sha256":hash(&fs::read(&path)?)});
    manifest["input_relations"] = serde_json::to_value(
        facts
            .iter()
            .map(|(name, rows)| {
                (
                    name.clone(),
                    json!({"arity":if name=="vertices" {1}else{2},"count":rows.len()}),
                )
            })
            .collect::<BTreeMap<_, _>>(),
    )?;
    let manifest_path = directory.join("manifest.json");
    let manifest_bytes = serde_json::to_vec_pretty(&manifest)?;
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&manifest_path)?;
    file.write_all(&manifest_bytes)?;
    file.sync_all()?;
    File::open(&directory)?.sync_all()?;
    // A single explicit flush. Visibility and acknowledgment follow the catalog.
    let tx = Transaction::new(&table);
    let outcome = tx
        .fast_append()
        .with_check_duplicate(true)
        .add_data_files(files)
        .set_snapshot_properties(HashMap::from([
            ("recovery.checkpoint-id".into(), checkpoint),
            ("recovery.manifest-uri".into(), uri(&manifest_path)?),
            ("recovery.manifest-sha256".into(), hash(&manifest_bytes)),
        ]))
        .apply(tx)?
        .commit(&c)
        .await;
    // The pinned SQL catalog ignores DB COMMIT errors; returned Table is not authority.
    if let Err(error) = outcome {
        eprintln!("catalog call failed; verifying authoritative state: {error}");
    }
    read(root).await
}
#[tokio::main(worker_threads = 2)]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    let mode = args.get(1).context("publish ROOT INPUT | read ROOT")?;
    let root = std::env::current_dir()?.join(args.get(2).context("root")?);
    let result = match mode.as_str() {
        "publish" => publish(&root, Path::new(args.get(3).context("input")?)).await?,
        "read" => read(&root).await?,
        _ => anyhow::bail!("unknown mode"),
    };
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}
