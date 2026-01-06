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

//! Resources: Type-safe dependency injection for world-level shared state.
//!
//! Resources are not entity data—they're the scaffolding around it:
//! - Environment parameters (gravity, reward scale)
//! - Shared services (CommandBroker)
//! - Simulation context (recursion depth, budget)
//!
//! In RL terms: MDP parameters, hyperparameters, shared infrastructure.

use std::any::{Any, TypeId};
use std::collections::HashMap;

use crate::error::{ArchetypeError, Result};

/// Type-safe container for world-level shared state.
///
/// # Example
///
/// ```rust,ignore
/// use archetype_aio::Resources;
///
/// struct SimConfig { gravity: f64 }
///
/// let mut resources = Resources::new();
/// resources.insert(SimConfig { gravity: 9.8 });
///
/// let config = resources.require::<SimConfig>().unwrap();
/// assert_eq!(config.gravity, 9.8);
/// ```
#[derive(Default)]
pub struct Resources {
    store: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
}

impl Resources {
    /// Creates a new empty resource container.
    pub fn new() -> Self {
        Resources {
            store: HashMap::new(),
        }
    }

    /// Inserts a resource, keyed by its type.
    ///
    /// If a resource of the same type already exists, it is replaced.
    pub fn insert<T: Any + Send + Sync>(&mut self, resource: T) {
        self.store.insert(TypeId::of::<T>(), Box::new(resource));
    }

    /// Gets a reference to a resource, or None if not present.
    pub fn get<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.store
            .get(&TypeId::of::<T>())
            .and_then(|r| r.downcast_ref())
    }

    /// Gets a mutable reference to a resource, or None if not present.
    pub fn get_mut<T: Any + Send + Sync>(&mut self) -> Option<&mut T> {
        self.store
            .get_mut(&TypeId::of::<T>())
            .and_then(|r| r.downcast_mut())
    }

    /// Gets a reference to a resource, returning an error if not present.
    pub fn require<T: Any + Send + Sync>(&self) -> Result<&T> {
        self.get::<T>().ok_or_else(|| {
            ArchetypeError::ResourceNotFound(std::any::type_name::<T>().to_string())
        })
    }

    /// Gets a mutable reference to a resource, returning an error if not present.
    pub fn require_mut<T: Any + Send + Sync>(&mut self) -> Result<&mut T> {
        let type_name = std::any::type_name::<T>().to_string();
        self.get_mut::<T>()
            .ok_or_else(|| ArchetypeError::ResourceNotFound(type_name))
    }

    /// Removes and returns a resource, or None if not present.
    pub fn remove<T: Any + Send + Sync>(&mut self) -> Option<T> {
        self.store
            .remove(&TypeId::of::<T>())
            .and_then(|r| r.downcast().ok())
            .map(|b| *b)
    }

    /// Checks if a resource type is registered.
    pub fn contains<T: Any + Send + Sync>(&self) -> bool {
        self.store.contains_key(&TypeId::of::<T>())
    }

    /// Returns the number of registered resources.
    pub fn len(&self) -> usize {
        self.store.len()
    }

    /// Returns true if no resources are registered.
    pub fn is_empty(&self) -> bool {
        self.store.is_empty()
    }

    /// Clears all resources.
    pub fn clear(&mut self) {
        self.store.clear();
    }
}

impl std::fmt::Debug for Resources {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Resources({} types)", self.store.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq)]
    struct TestConfig {
        value: i32,
    }

    #[derive(Debug, PartialEq)]
    struct AnotherConfig {
        name: String,
    }

    #[test]
    fn test_insert_and_get() {
        let mut resources = Resources::new();
        resources.insert(TestConfig { value: 42 });

        let config = resources.get::<TestConfig>().unwrap();
        assert_eq!(config.value, 42);
    }

    #[test]
    fn test_get_mut() {
        let mut resources = Resources::new();
        resources.insert(TestConfig { value: 42 });

        {
            let config = resources.get_mut::<TestConfig>().unwrap();
            config.value = 100;
        }

        let config = resources.get::<TestConfig>().unwrap();
        assert_eq!(config.value, 100);
    }

    #[test]
    fn test_require() {
        let mut resources = Resources::new();
        resources.insert(TestConfig { value: 42 });

        let config = resources.require::<TestConfig>().unwrap();
        assert_eq!(config.value, 42);

        let result = resources.require::<AnotherConfig>();
        assert!(result.is_err());
    }

    #[test]
    fn test_remove() {
        let mut resources = Resources::new();
        resources.insert(TestConfig { value: 42 });

        assert!(resources.contains::<TestConfig>());

        let removed = resources.remove::<TestConfig>().unwrap();
        assert_eq!(removed.value, 42);
        assert!(!resources.contains::<TestConfig>());
    }

    #[test]
    fn test_multiple_types() {
        let mut resources = Resources::new();
        resources.insert(TestConfig { value: 42 });
        resources.insert(AnotherConfig {
            name: "test".to_string(),
        });

        assert_eq!(resources.len(), 2);
        assert_eq!(resources.get::<TestConfig>().unwrap().value, 42);
        assert_eq!(
            resources.get::<AnotherConfig>().unwrap().name,
            "test".to_string()
        );
    }

    #[test]
    fn test_replace() {
        let mut resources = Resources::new();
        resources.insert(TestConfig { value: 42 });
        resources.insert(TestConfig { value: 100 });

        assert_eq!(resources.len(), 1);
        assert_eq!(resources.get::<TestConfig>().unwrap().value, 100);
    }

    #[test]
    fn test_clear() {
        let mut resources = Resources::new();
        resources.insert(TestConfig { value: 42 });
        resources.insert(AnotherConfig {
            name: "test".to_string(),
        });

        resources.clear();
        assert!(resources.is_empty());
    }
}
