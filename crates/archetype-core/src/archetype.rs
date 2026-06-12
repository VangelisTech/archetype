use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::error::{ArchetypeCoreError, Result};

pub const WORLD_ID: &str = "world_id";
pub const RUN_ID: &str = "run_id";
pub const ENTITY_ID: &str = "entity_id";
pub const TICK: &str = "tick";
pub const IS_ACTIVE: &str = "is_active";

pub const BASE_COLUMN_NAMES: [&str; 5] = [WORLD_ID, RUN_ID, ENTITY_ID, TICK, IS_ACTIVE];

pub fn base_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(WORLD_ID, DataType::Utf8, false),
        Field::new(RUN_ID, DataType::Utf8, false),
        Field::new(ENTITY_ID, DataType::Int32, false),
        Field::new(TICK, DataType::Int32, false),
        Field::new(IS_ACTIVE, DataType::Boolean, false),
    ]))
}

#[derive(Debug, Clone)]
pub struct ArchetypeTable {
    table_name: String,
    schema: SchemaRef,
}

impl ArchetypeTable {
    pub fn new(table_name: impl Into<String>, component_schema: SchemaRef) -> Result<Self> {
        let mut fields =
            Vec::with_capacity(BASE_COLUMN_NAMES.len() + component_schema.fields().len());
        fields.extend(base_schema().fields().iter().cloned());
        fields.extend(component_schema.fields().iter().cloned());
        let table = Self {
            table_name: table_name.into(),
            schema: Arc::new(Schema::new(fields)),
        };
        table.validate_base_columns()?;
        Ok(table)
    }

    pub fn from_full_schema(table_name: impl Into<String>, schema: SchemaRef) -> Result<Self> {
        let table = Self {
            table_name: table_name.into(),
            schema,
        };
        table.validate_base_columns()?;
        Ok(table)
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    pub fn validate_base_columns(&self) -> Result<()> {
        expect_type(&self.schema, WORLD_ID, DataType::Utf8)?;
        expect_type(&self.schema, RUN_ID, DataType::Utf8)?;
        expect_type(&self.schema, ENTITY_ID, DataType::Int32)?;
        expect_type(&self.schema, TICK, DataType::Int32)?;
        expect_type(&self.schema, IS_ACTIVE, DataType::Boolean)?;
        Ok(())
    }
}

pub fn expect_type(schema: &Schema, column: &'static str, expected: DataType) -> Result<()> {
    let field = schema
        .field_with_name(column)
        .map_err(|_| ArchetypeCoreError::MissingColumn(column.to_string()))?;
    if field.data_type() != &expected {
        return Err(ArchetypeCoreError::InvalidColumnType {
            column,
            expected,
            actual: field.data_type().clone(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn component_schema_is_composed_with_base_columns() {
        let component = Arc::new(Schema::new(vec![Field::new(
            "position__x",
            DataType::Float64,
            false,
        )]));
        let table = ArchetypeTable::new("position", component).unwrap();

        assert_eq!(table.table_name(), "position");
        assert_eq!(
            table
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>(),
            vec![WORLD_ID, RUN_ID, ENTITY_ID, TICK, IS_ACTIVE, "position__x"]
        );
    }
}
