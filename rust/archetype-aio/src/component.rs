// Copyright 2025 Vangelis Technologies Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Component trait and utilities for ECS components.
//!
//! Components are pure data containers attached to entities.
//! Each component type defines an Arrow schema for columnar storage.

use arrow_schema::{Field, Schema};
use serde::Serialize;
use std::any::TypeId;
use std::collections::HashMap;

/// Trait for ECS components.
///
/// Components are pure data containers that can be attached to entities.
/// They define an Arrow schema for efficient columnar storage.
pub trait Component: Send + Sync + 'static {
    /// Returns the type name of this component.
    fn type_name() -> &'static str
    where
        Self: Sized;

    /// Returns the Arrow schema for this component's fields.
    fn schema() -> Schema
    where
        Self: Sized;

    /// Returns the prefix used for field names in the archetype schema.
    /// Default is lowercase type name + "__".
    fn prefix() -> String
    where
        Self: Sized,
    {
        format!("{}__", Self::type_name().to_lowercase())
    }

    /// Returns the prefixed schema for use in archetype tables.
    fn prefixed_schema() -> Schema
    where
        Self: Sized,
    {
        let schema = Self::schema();
        let prefix = Self::prefix();
        let prefixed_fields: Vec<Field> = schema
            .fields()
            .iter()
            .map(|f| {
                Field::new(
                    format!("{}{}", prefix, f.name()),
                    f.data_type().clone(),
                    f.is_nullable(),
                )
            })
            .collect();
        Schema::new(prefixed_fields)
    }

    /// Converts component data to a row dictionary for archetype storage.
    fn to_row_dict(&self) -> HashMap<String, serde_json::Value>
    where
        Self: Sized + Serialize,
    {
        let prefix = Self::prefix();
        let value = serde_json::to_value(self).unwrap_or(serde_json::Value::Null);
        if let serde_json::Value::Object(map) = value {
            map.into_iter()
                .map(|(k, v)| (format!("{}{}", prefix, k), v))
                .collect()
        } else {
            HashMap::new()
        }
    }
}

/// Component type identifier for runtime type comparisons.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ComponentTypeId(pub TypeId);

impl ComponentTypeId {
    /// Creates a new ComponentTypeId from a Component type.
    pub fn of<C: Component>() -> Self {
        ComponentTypeId(TypeId::of::<C>())
    }
}

/// Component metadata for runtime introspection (schema stored separately).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ComponentMeta {
    /// Type name of the component.
    pub name: String,
    /// Prefix for field names.
    pub prefix: String,
}

impl ComponentMeta {
    /// Creates metadata for a component type.
    pub fn of<C: Component>() -> Self {
        ComponentMeta {
            name: C::type_name().to_string(),
            prefix: C::prefix(),
        }
    }

    /// Creates a new ComponentMeta with explicit values.
    pub fn new(name: impl Into<String>, prefix: impl Into<String>) -> Self {
        ComponentMeta {
            name: name.into(),
            prefix: prefix.into(),
        }
    }
}

/// Component registry for component types, allowing runtime lookup.
#[derive(Debug, Default)]
pub struct ComponentRegistry {
    /// Map from type name to metadata.
    by_name: HashMap<String, (ComponentMeta, Schema)>,
    /// Map from TypeId to type name.
    by_type_id: HashMap<TypeId, String>,
}

impl ComponentRegistry {
    /// Creates a new empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a component type.
    pub fn register<C: Component>(&mut self) {
        let meta = ComponentMeta::of::<C>();
        let schema = C::prefixed_schema();
        self.by_name.insert(meta.name.clone(), (meta, schema));
        self.by_type_id
            .insert(TypeId::of::<C>(), C::type_name().to_string());
    }

    /// Gets component metadata by type name.
    pub fn get_by_name(&self, name: &str) -> Option<(&ComponentMeta, &Schema)> {
        self.by_name.get(name).map(|(m, s)| (m, s))
    }

    /// Gets component type name by TypeId.
    pub fn get_name_by_type_id(&self, type_id: TypeId) -> Option<&str> {
        self.by_type_id.get(&type_id).map(|s| s.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::DataType;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestPosition {
        x: f64,
        y: f64,
    }

    impl Component for TestPosition {
        fn type_name() -> &'static str {
            "TestPosition"
        }

        fn schema() -> Schema {
            Schema::new(vec![
                Field::new("x", DataType::Float64, false),
                Field::new("y", DataType::Float64, false),
            ])
        }
    }

    #[test]
    fn test_component_prefix() {
        assert_eq!(TestPosition::prefix(), "testposition__");
    }

    #[test]
    fn test_component_prefixed_schema() {
        let schema = TestPosition::prefixed_schema();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "testposition__x");
        assert_eq!(schema.field(1).name(), "testposition__y");
    }

    #[test]
    fn test_component_to_row_dict() {
        let pos = TestPosition { x: 1.0, y: 2.0 };
        let row = pos.to_row_dict();
        assert_eq!(row.get("testposition__x"), Some(&serde_json::json!(1.0)));
        assert_eq!(row.get("testposition__y"), Some(&serde_json::json!(2.0)));
    }

    #[test]
    fn test_component_registry() {
        let mut registry = ComponentRegistry::new();
        registry.register::<TestPosition>();

        let (meta, _schema) = registry.get_by_name("TestPosition").unwrap();
        assert_eq!(meta.name, "TestPosition");
        assert_eq!(meta.prefix, "testposition__");
    }
}
