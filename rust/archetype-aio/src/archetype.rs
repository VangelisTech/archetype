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

//! Archetype management for the ECS engine.
//!
//! An archetype is a unique combination of component types that defines
//! the structure of entities. Entities with the same components share
//! the same archetype and are stored together for cache efficiency.

use arrow_schema::{DataType, Field, Schema, SchemaBuilder};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::sync::Arc;

use crate::component::{ComponentMeta, ComponentTypeId};

/// A signature identifying an archetype by its component types.
///
/// The signature is a sorted set of component type IDs, ensuring
/// consistent ordering regardless of insertion order.
/// 
/// Note: Schema is stored in an Arc to allow Hash/Eq implementation.
#[derive(Debug, Clone)]
pub struct ArchetypeSignature {
    /// Sorted component type IDs.
    components: BTreeSet<ComponentTypeId>,
    /// Component metadata (name, prefix).
    metadata: Vec<ComponentMeta>,
    /// Prefixed schemas per component (in same order as metadata).
    schemas: Vec<Arc<Schema>>,
}

impl PartialEq for ArchetypeSignature {
    fn eq(&self, other: &Self) -> bool {
        self.components == other.components && self.metadata == other.metadata
    }
}

impl Eq for ArchetypeSignature {}

impl std::hash::Hash for ArchetypeSignature {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.components.hash(state);
        self.metadata.hash(state);
    }
}

impl ArchetypeSignature {
    /// Creates a new archetype signature from component metadata and schemas.
    pub fn new(components: Vec<(ComponentMeta, Schema)>) -> Self {
        let metadata: Vec<_> = components.iter().map(|(m, _)| m.clone()).collect();
        let schemas: Vec<_> = components.iter().map(|(_, s)| Arc::new(s.clone())).collect();
        ArchetypeSignature {
            components: BTreeSet::new(),
            metadata,
            schemas,
        }
    }

    /// Creates an empty signature.
    pub fn empty() -> Self {
        ArchetypeSignature {
            components: BTreeSet::new(),
            metadata: Vec::new(),
            schemas: Vec::new(),
        }
    }

    /// Adds a component type to the signature.
    pub fn with_component(mut self, type_id: ComponentTypeId, meta: ComponentMeta, schema: Schema) -> Self {
        self.components.insert(type_id);
        self.metadata.push(meta);
        self.schemas.push(Arc::new(schema));
        self
    }

    /// Returns the number of component types in this signature.
    pub fn len(&self) -> usize {
        self.metadata.len()
    }

    /// Returns true if this signature has no components.
    pub fn is_empty(&self) -> bool {
        self.metadata.is_empty()
    }

    /// Returns true if this signature contains all component types from another.
    pub fn contains(&self, other: &ArchetypeSignature) -> bool {
        other.components.is_subset(&self.components)
    }

    /// Returns the component metadata.
    pub fn metadata(&self) -> &[ComponentMeta] {
        &self.metadata
    }

    /// Returns the component schemas.
    pub fn schemas(&self) -> &[Arc<Schema>] {
        &self.schemas
    }

    /// Returns the component type IDs.
    pub fn type_ids(&self) -> &BTreeSet<ComponentTypeId> {
        &self.components
    }
}

/// Archetype operations and schema management.
#[derive(Debug)]
pub struct Archetype;

impl Archetype {
    /// Base schema fields present in all archetype tables.
    pub fn base_schema() -> Schema {
        Schema::new(vec![
            Field::new("world_id", DataType::Utf8, false),
            Field::new("run_id", DataType::Utf8, false),
            Field::new("entity_id", DataType::Int32, false),
            Field::new("tick", DataType::Int32, false),
            Field::new("is_active", DataType::Boolean, false),
        ])
    }

    /// Partition keys for archetype tables.
    pub fn partition_keys() -> Vec<&'static str> {
        vec!["world_id", "run_id", "tick"]
    }

    /// Generates the combined schema for an archetype signature.
    ///
    /// The schema includes base fields plus all component fields with prefixes.
    pub fn get_schema(sig: &ArchetypeSignature) -> Schema {
        let base = Self::base_schema();
        let mut builder = SchemaBuilder::from(&base);

        for schema in sig.schemas() {
            for field in schema.fields() {
                builder.push(field.clone());
            }
        }

        builder.finish()
    }

    /// Generates a compact, filesystem-safe name for an archetype.
    ///
    /// Uses a hash of the schema to ensure uniqueness without exceeding
    /// path length limits.
    pub fn get_name(sig: &ArchetypeSignature) -> String {
        let schema = Self::get_schema(sig);
        let schema_str = format!("{:?}", schema);

        let mut hasher = Sha256::new();
        hasher.update(schema_str.as_bytes());
        let hash = hasher.finalize();
        let hash_hex = hex::encode(&hash[..8]); // 16-char hex

        format!("a_{}c_s{}", sig.len(), hash_hex)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::DataType;

    fn test_component_schema() -> Schema {
        Schema::new(vec![
            Field::new("position__x", DataType::Float64, false),
            Field::new("position__y", DataType::Float64, false),
        ])
    }

    fn test_component_meta() -> ComponentMeta {
        ComponentMeta::new("Position", "position__")
    }

    #[test]
    fn test_base_schema() {
        let schema = Archetype::base_schema();
        assert_eq!(schema.fields().len(), 5);
        assert!(schema.field_with_name("world_id").is_ok());
        assert!(schema.field_with_name("entity_id").is_ok());
        assert!(schema.field_with_name("tick").is_ok());
        assert!(schema.field_with_name("is_active").is_ok());
    }

    #[test]
    fn test_archetype_signature() {
        let meta = test_component_meta();
        let schema = test_component_schema();
        let sig = ArchetypeSignature::new(vec![(meta, schema)]);

        assert_eq!(sig.len(), 1);
        assert!(!sig.is_empty());
    }

    #[test]
    fn test_archetype_schema() {
        let meta = test_component_meta();
        let schema = test_component_schema();
        let sig = ArchetypeSignature::new(vec![(meta, schema)]);
        let combined = Archetype::get_schema(&sig);

        // 5 base fields + 2 component fields
        assert_eq!(combined.fields().len(), 7);
        assert!(combined.field_with_name("position__x").is_ok());
        assert!(combined.field_with_name("position__y").is_ok());
    }

    #[test]
    fn test_archetype_name() {
        let meta = test_component_meta();
        let schema = test_component_schema();
        let sig = ArchetypeSignature::new(vec![(meta, schema)]);
        let name = Archetype::get_name(&sig);

        assert!(name.starts_with("a_1c_s"));
        assert!(name.len() <= 24);
    }

    #[test]
    fn test_signature_equality() {
        let meta1 = test_component_meta();
        let schema1 = test_component_schema();
        let sig1 = ArchetypeSignature::new(vec![(meta1.clone(), schema1.clone())]);
        
        let meta2 = test_component_meta();
        let schema2 = test_component_schema();
        let sig2 = ArchetypeSignature::new(vec![(meta2, schema2)]);

        assert_eq!(sig1, sig2);
    }
}
