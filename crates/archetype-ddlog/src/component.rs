use std::{collections::BTreeSet, sync::Arc};

use anyhow::{Result, bail, ensure};
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use ddlog_runtime::Schema;
use iceberg::{
    arrow::schema_to_arrow_schema,
    spec::{NestedField, PrimitiveType, Type},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Only explicitly declared public outputs are durable ECS components. Other
/// DDlog relations remain derived execution state. Fields are ordered by DDlog
/// position, with exactly one non-null signed 64-bit entity key.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Component {
    pub name: String,
    pub output: String,
    pub fields: Vec<String>,
    pub entity_field: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComponentSchema {
    pub component: Component,
    pub types: Vec<String>,
}

impl ComponentSchema {
    pub fn new(component: Component, relation: &Schema) -> Result<Self> {
        ensure!(crate::identifier(&component.name), "Invalid component name");
        ensure!(
            !relation.input,
            "Persistent relation must be a public output"
        );
        ensure!(
            !component.fields.is_empty() && component.fields.len() == relation.fields.len(),
            "Field arity mismatch"
        );
        ensure!(
            component.entity_field < component.fields.len(),
            "Missing entity key"
        );
        ensure!(
            relation.fields[component.entity_field] == "int",
            "Entity key must be int"
        );
        let mut names = BTreeSet::new();
        for (index, (name, kind)) in component.fields.iter().zip(&relation.fields).enumerate() {
            ensure!(
                crate::identifier(name) && names.insert(name),
                "Invalid or duplicate field name"
            );
            ensure!(
                matches!(kind.as_str(), "int" | "string"),
                "Unsupported DDlog field type: {kind}"
            );
            ensure!(
                (name == "entity_id") == (index == component.entity_field),
                "Entity key must be named entity_id"
            );
        }
        Ok(Self {
            component,
            types: relation.fields.clone(),
        })
    }

    pub fn identity(&self) -> Result<String> {
        crate::digest(self)
    }

    pub fn iceberg_schema(&self) -> Result<iceberg::spec::Schema> {
        self.validate_layout()?;
        let mut fields = vec![Arc::new(NestedField::required(
            1,
            "cut_id",
            Type::Primitive(PrimitiveType::String),
        ))];
        for (index, (field, kind)) in self.component.fields.iter().zip(&self.types).enumerate() {
            let name = if index == self.component.entity_field {
                "entity_id".into()
            } else {
                format!("{}__{field}", self.component.name)
            };
            let ty = match kind.as_str() {
                "int" => PrimitiveType::Long,
                "string" => PrimitiveType::String,
                _ => bail!("Unsupported type"),
            };
            fields.push(Arc::new(NestedField::required(
                (index + 2).try_into()?,
                name,
                Type::Primitive(ty),
            )));
        }
        Ok(iceberg::spec::Schema::builder()
            .with_fields(fields)
            .build()?)
    }

    pub fn validate_rows(&self, rows: &[Vec<Value>]) -> Result<()> {
        self.validate_layout()?;
        let mut keys = BTreeSet::new();
        for row in rows {
            ensure!(
                row.len() == self.types.len(),
                "Component row arity mismatch"
            );
            for (kind, value) in self.types.iter().zip(row) {
                ensure!(
                    match kind.as_str() {
                        "int" => value.as_i64().is_some(),
                        "string" => value.as_str().is_some(),
                        _ => false,
                    },
                    "Component value type/nullability mismatch"
                );
            }
            ensure!(
                keys.insert(row[self.component.entity_field].as_i64().unwrap()),
                "Multiple component records for one entity"
            );
        }
        Ok(())
    }

    fn validate_layout(&self) -> Result<()> {
        Self::new(
            self.component.clone(),
            &Schema {
                input: false,
                fields: self.types.clone(),
            },
        )?;
        Ok(())
    }

    pub fn batch(&self, cut_id: &str, rows: &[Vec<Value>]) -> Result<RecordBatch> {
        self.validate_rows(rows)?;
        let mut columns: Vec<ArrayRef> =
            vec![Arc::new(StringArray::from(vec![cut_id; rows.len()]))];
        for (index, kind) in self.types.iter().enumerate() {
            columns.push(match kind.as_str() {
                "int" => Arc::new(Int64Array::from(
                    rows.iter()
                        .map(|r| r[index].as_i64().unwrap())
                        .collect::<Vec<_>>(),
                )),
                "string" => Arc::new(StringArray::from(
                    rows.iter()
                        .map(|r| r[index].as_str().unwrap())
                        .collect::<Vec<_>>(),
                )),
                _ => bail!("Unsupported type"),
            });
        }
        Ok(RecordBatch::try_new(
            Arc::new(schema_to_arrow_schema(&self.iceberg_schema()?)?),
            columns,
        )?)
    }
}
