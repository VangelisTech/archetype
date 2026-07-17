use std::fs::{self, File};
use std::path::{Path, PathBuf};

use archetype_core::archetype::{ENTITY_ID, IS_ACTIVE, RUN_ID, TICK, WORLD_ID};
use archetype_core::{ArchetypeCoreError, ReadFilter, Result, Store};
use arrow_array::{Array, BooleanArray, Int32Array, RecordBatch, StringArray};
use arrow_select::concat::concat_batches;
use async_trait::async_trait;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_writer::ArrowWriter;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct ParquetStore {
    root: PathBuf,
    namespace: String,
}

impl ParquetStore {
    pub fn new(root: impl Into<PathBuf>, namespace: impl Into<String>) -> Self {
        Self {
            root: root.into(),
            namespace: namespace.into(),
        }
    }

    fn table_dir(&self, table_name: &str) -> PathBuf {
        self.root.join(&self.namespace).join(table_name)
    }

    fn part_paths(&self, table_name: &str) -> (PathBuf, PathBuf) {
        let id = Uuid::now_v7();
        let table_dir = self.table_dir(table_name);
        (
            table_dir.join(format!("part-{id}.parquet")),
            table_dir.join(format!("part-{id}.parquet.tmp")),
        )
    }
}

#[async_trait]
impl Store for ParquetStore {
    async fn append(&self, table_name: &str, batch: &RecordBatch) -> Result<()> {
        let table_dir = self.table_dir(table_name);
        fs::create_dir_all(&table_dir).map_err(|err| ArchetypeCoreError::Store(err.to_string()))?;

        let (path, staging_path) = self.part_paths(table_name);
        let result = write_part(&staging_path, batch)
            .and_then(|()| fs::rename(&staging_path, path).map_err(store_error));
        if result.is_err() {
            let _ = fs::remove_file(staging_path);
        }
        result
    }

    async fn read_table(&self, table_name: &str, filter: ReadFilter) -> Result<Vec<RecordBatch>> {
        let table_dir = self.table_dir(table_name);
        if !table_dir.exists() {
            return Ok(Vec::new());
        }

        let mut batches = Vec::new();
        for entry in
            fs::read_dir(table_dir).map_err(|err| ArchetypeCoreError::Store(err.to_string()))?
        {
            let path = entry
                .map_err(|err| ArchetypeCoreError::Store(err.to_string()))?
                .path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("parquet") {
                continue;
            }
            batches.extend(read_part(&path, &filter)?);
        }

        if batches.is_empty() {
            return Ok(batches);
        }

        let schema = batches[0].schema();
        let merged = concat_batches(&schema, &batches).map_err(ArchetypeCoreError::from)?;
        Ok(vec![merged])
    }
}

fn write_part(path: &Path, batch: &RecordBatch) -> Result<()> {
    let file = File::create(path).map_err(store_error)?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).map_err(store_error)?;
    writer.write(batch).map_err(store_error)?;
    writer.close().map_err(store_error)?;
    Ok(())
}

fn store_error(error: impl std::fmt::Display) -> ArchetypeCoreError {
    ArchetypeCoreError::Store(error.to_string())
}

fn read_part(path: &Path, filter: &ReadFilter) -> Result<Vec<RecordBatch>> {
    let file = File::open(path).map_err(|err| ArchetypeCoreError::Store(err.to_string()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|err| ArchetypeCoreError::Store(err.to_string()))?;
    let reader = builder
        .build()
        .map_err(|err| ArchetypeCoreError::Store(err.to_string()))?;

    reader
        .map(|batch| {
            batch
                .map_err(|err| ArchetypeCoreError::Store(err.to_string()))
                .and_then(|batch| filter_batch(batch, filter))
        })
        .collect()
}

fn filter_batch(batch: RecordBatch, filter: &ReadFilter) -> Result<RecordBatch> {
    if batch.num_rows() == 0 {
        return Ok(batch);
    }

    let mut keep = vec![true; batch.num_rows()];
    if let Some(world_id) = &filter.world_id {
        apply_string_filter(&batch, &mut keep, WORLD_ID, |value| value == world_id)?;
    }
    if let Some(run_id) = &filter.run_id {
        apply_string_filter(&batch, &mut keep, RUN_ID, |value| value == run_id)?;
    }
    if let Some(ticks) = &filter.ticks {
        apply_i32_filter(&batch, &mut keep, TICK, |value| ticks.contains(&value))?;
    }
    if let Some(entity_ids) = &filter.entity_ids {
        apply_i32_filter(&batch, &mut keep, ENTITY_ID, |value| {
            entity_ids.contains(&value)
        })?;
    }
    if filter.active_only {
        apply_bool_filter(&batch, &mut keep, IS_ACTIVE, |value| value)?;
    }

    let indices = arrow_array::UInt32Array::from_iter_values(
        keep.iter()
            .enumerate()
            .filter_map(|(idx, keep)| keep.then_some(idx as u32)),
    );
    let columns = batch
        .columns()
        .iter()
        .map(|column| arrow_select::take::take(column.as_ref(), &indices, None))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    RecordBatch::try_new(batch.schema(), columns).map_err(Into::into)
}

fn apply_string_filter(
    batch: &RecordBatch,
    keep: &mut [bool],
    column: &'static str,
    predicate: impl Fn(&str) -> bool,
) -> Result<()> {
    let array = batch
        .column(batch.schema().index_of(column)?)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| ArchetypeCoreError::Store(format!("column {column} is not Utf8")))?;
    for (idx, keep_row) in keep.iter_mut().enumerate() {
        *keep_row = *keep_row && !array.is_null(idx) && predicate(array.value(idx));
    }
    Ok(())
}

fn apply_i32_filter(
    batch: &RecordBatch,
    keep: &mut [bool],
    column: &'static str,
    predicate: impl Fn(i32) -> bool,
) -> Result<()> {
    let array = batch
        .column(batch.schema().index_of(column)?)
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| ArchetypeCoreError::Store(format!("column {column} is not Int32")))?;
    for (idx, keep_row) in keep.iter_mut().enumerate() {
        *keep_row = *keep_row && !array.is_null(idx) && predicate(array.value(idx));
    }
    Ok(())
}

fn apply_bool_filter(
    batch: &RecordBatch,
    keep: &mut [bool],
    column: &'static str,
    predicate: impl Fn(bool) -> bool,
) -> Result<()> {
    let array = batch
        .column(batch.schema().index_of(column)?)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| ArchetypeCoreError::Store(format!("column {column} is not Boolean")))?;
    for (idx, keep_row) in keep.iter_mut().enumerate() {
        *keep_row = *keep_row && !array.is_null(idx) && predicate(array.value(idx));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use archetype_core::archetype::ArchetypeTable;
    use arrow_array::{Float64Array, Int32Array};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    #[tokio::test]
    async fn append_writes_parts_and_read_filters_active_rows() {
        let root = std::env::temp_dir().join(format!("archetype-parquet-test-{}", Uuid::now_v7()));
        let store = ParquetStore::new(&root, "ecs");
        let component = Arc::new(Schema::new(vec![Field::new(
            "position__x",
            DataType::Float64,
            false,
        )]));
        let table = ArchetypeTable::new("position", component).unwrap();
        let batch = RecordBatch::try_new(
            table.schema(),
            vec![
                Arc::new(StringArray::from(vec!["world-a", "world-b"])),
                Arc::new(StringArray::from(vec!["run-a", "run-a"])),
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int32Array::from(vec![0, 0])),
                Arc::new(BooleanArray::from(vec![true, false])),
                Arc::new(Float64Array::from(vec![1.0, 2.0])),
            ],
        )
        .unwrap();

        store.append(table.table_name(), &batch).await.unwrap();
        store.append(table.table_name(), &batch).await.unwrap();

        let parts = fs::read_dir(store.table_dir(table.table_name()))
            .unwrap()
            .count();
        assert_eq!(parts, 2);

        let rows = store
            .read_table(
                table.table_name(),
                ReadFilter {
                    world_id: Some("world-a".to_string()),
                    run_id: Some("run-a".to_string()),
                    active_only: true,
                    ..ReadFilter::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(rows[0].num_rows(), 2);

        fs::remove_dir_all(root).unwrap();
    }
}
