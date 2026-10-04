use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};

use anyhow::{Result, anyhow, ensure};
use ddlog_runtime::{
    Backend, BoundedQuery, composition::CompositionManifest, registry::ProcessorRegistry,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    component::{Component, ComponentSchema},
    store::{CutReceipt, CutStore},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelationState {
    pub schema: ComponentSchema,
    pub rows: Vec<Vec<Value>>,
}

/// Frozen full state after one acknowledged outer transaction. Checkpoint bytes
/// carry DDlog inputs/source; analytical output rows are not recovery state.
#[derive(Clone, Debug, Serialize)]
pub struct FrozenCut {
    pub(crate) world: String,
    pub(crate) run: String,
    pub(crate) tick: u64,
    pub(crate) program: String,
    pub(crate) ddlog_revision: u64,
    pub(crate) parent: Option<String>,
    pub(crate) relations: BTreeMap<String, RelationState>,
    pub(crate) checkpoint: Vec<u8>,
}

impl FrozenCut {
    pub fn identity(&self) -> Result<String> {
        crate::digest(self)
    }
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            crate::identifier(&self.world) && crate::identifier(&self.run),
            "Invalid world/run identity"
        );
        ensure!(
            self.tick > 0 && !self.relations.is_empty(),
            "Invalid cut inventory"
        );
        for (name, relation) in &self.relations {
            ensure!(
                name == &relation.schema.component.name,
                "Component identity mismatch"
            );
            let checked = ComponentSchema::new(
                relation.schema.component.clone(),
                &ddlog_runtime::Schema {
                    input: false,
                    fields: relation.schema.types.clone(),
                },
            )?;
            checked.validate_rows(&relation.rows)?;
        }
        let checkpoint: Value = serde_json::from_slice(&self.checkpoint)?;
        ensure!(
            checkpoint["state"]["metadata"]
                == json!({"abi":crate::ADAPTER_ABI,"program":self.program,"world":self.world,"run":self.run,"tick":self.tick}),
            "Checkpoint attribution mismatch"
        );
        ensure!(
            checkpoint["state"]["revision"].as_u64() == Some(self.ddlog_revision),
            "Checkpoint revision mismatch"
        );
        Ok(())
    }

    // No public constructor or Deserialize implementation: only a successful
    // native freeze creates a cut. Disk recovery additionally verifies the
    // journal checksum and checkpoint attribution before reusing that cut.
    pub(crate) fn decode_journal(bytes: &[u8]) -> Result<Self> {
        let mut envelope: Value = serde_json::from_slice(bytes)?;
        let root = envelope
            .as_object_mut()
            .ok_or_else(|| anyhow!("Invalid journal envelope"))?;
        let checksum = root
            .remove("sha256")
            .ok_or_else(|| anyhow!("Missing journal checksum"))?;
        let v = root
            .get_mut("cut")
            .and_then(Value::as_object_mut)
            .ok_or_else(|| anyhow!("Invalid journal cut"))?;
        let mut take = |key: &str| {
            v.remove(key)
                .ok_or_else(|| anyhow!("Missing cut field: {key}"))
        };
        let cut = Self {
            world: serde_json::from_value(take("world")?)?,
            run: serde_json::from_value(take("run")?)?,
            tick: serde_json::from_value(take("tick")?)?,
            program: serde_json::from_value(take("program")?)?,
            ddlog_revision: serde_json::from_value(take("ddlog_revision")?)?,
            parent: serde_json::from_value(take("parent")?)?,
            relations: serde_json::from_value(take("relations")?)?,
            checkpoint: serde_json::from_value(take("checkpoint")?)?,
        };
        ensure!(v.is_empty() && root.len() == 1, "Unknown journal fields");
        ensure!(
            checksum.as_str() == Some(&cut.identity()?),
            "Cut journal checksum mismatch"
        );
        cut.validate()?;
        Ok(cut)
    }
}

/// A single native Backend owns all execution. Mutable access serializes a
/// world's boundaries; separate Worlds can execute concurrently. Pure composed
/// programs only in v1: no host effects or registered operations are accepted.
pub struct World {
    backend: Backend,
    native_root: PathBuf,
    driver: PathBuf,
    world: String,
    run: String,
    program: String,
    source_sha256: String,
    inputs: BTreeMap<String, String>,
    components: BTreeMap<String, (String, ComponentSchema)>,
    staged: Vec<Value>,
    applied: bool,
    apply_failed: bool,
    frozen: Option<FrozenCut>,
    published: Option<CutReceipt>,
}

impl World {
    pub fn create(
        root: PathBuf,
        driver: PathBuf,
        registry: &ProcessorRegistry,
        manifest: &CompositionManifest,
        components: Vec<Component>,
        world: &str,
        run: &str,
    ) -> Result<Self> {
        ensure!(
            crate::identifier(world) && crate::identifier(run),
            "Invalid world/run identity"
        );
        let compiled = registry
            .compile_composition_versioned(manifest, 2)
            .map_err(|e| anyhow!(e))?;
        let mut declared = BTreeMap::new();
        let mut outputs = BTreeSet::new();
        for component in components {
            ensure!(
                outputs.insert(component.output.clone()),
                "An output can back only one component"
            );
            let physical = compiled
                .resolution
                .outputs
                .get(&component.output)
                .ok_or_else(|| anyhow!("Undeclared public output: {}", component.output))?;
            let schema = ComponentSchema::new(
                component.clone(),
                compiled
                    .schemas
                    .get(physical)
                    .ok_or_else(|| anyhow!("Missing schema"))?,
            )?;
            ensure!(
                declared
                    .insert(component.name.clone(), (physical.clone(), schema))
                    .is_none(),
                "Duplicate component name"
            );
        }
        ensure!(
            !declared.is_empty(),
            "Declare at least one persistent component"
        );
        let program = crate::digest(
            &json!({"abi":crate::ADAPTER_ABI,"ddlog":crate::DDLOG_REVISION,"manifest":manifest,"resolution":compiled.resolution,"components":declared}),
        )?;
        let mut backend = Backend::new(root.clone(), driver.clone());
        backend.set_lowering_version(2).map_err(|e| anyhow!(e))?;
        let resolution = backend
            .install_composition(registry, manifest)
            .map_err(|e| anyhow!(e))?;
        // Upstream validates checkpoint eligibility; reject effectful definitions
        // before accepting inputs instead of pretending their state is recoverable.
        backend
            .checkpoint_bytes(json!({"program":program}))
            .map_err(|e| anyhow!(e))?;
        let source_sha256 = backend.source_sha256();
        Ok(Self {
            backend,
            native_root: root,
            driver,
            world: world.into(),
            run: run.into(),
            program,
            source_sha256,
            inputs: resolution.inputs,
            components: declared,
            staged: vec![],
            applied: false,
            apply_failed: false,
            frozen: None,
            published: None,
        })
    }

    pub fn tick(&self) -> u64 {
        self.published.as_ref().map_or(0, |r| r.tick)
    }
    pub fn pending(&self) -> bool {
        self.applied || self.apply_failed || self.frozen.is_some()
    }
    pub fn staged_len(&self) -> usize {
        self.staged.len()
    }

    /// Resume an exact published pure-program checkpoint in a fresh owner.
    /// Publication-only recovery (`CutStore::retry`) needs no native execution.
    pub async fn restore(&mut self, store: &CutStore, receipt: &CutReceipt) -> Result<()> {
        ensure!(
            self.tick() == 0 && !self.pending() && self.staged.is_empty(),
            "Restore requires a fresh owner"
        );
        ensure!(
            receipt.world == self.world
                && receipt.run == self.run
                && receipt.program == self.program,
            "Restore identity mismatch"
        );
        let bytes = store.checkpoint(receipt).await?;
        let envelope: Value = serde_json::from_slice(&bytes)?;
        let metadata = json!({"abi":crate::ADAPTER_ABI,"program":self.program,"world":self.world,"run":self.run,"tick":receipt.tick});
        ensure!(
            envelope["state"]["metadata"] == metadata
                && envelope["state"]["source"]
                    .as_str()
                    .is_some_and(|s| crate::hash(s.as_bytes()) == self.source_sha256),
            "Checkpoint program/metadata mismatch"
        );
        let mut candidate = Backend::new(
            self.native_root.join("restore_candidate"),
            self.driver.clone(),
        );
        candidate
            .restore_checkpoint_bytes(&bytes)
            .map_err(|e| anyhow!(e))?;
        self.backend = candidate;
        self.published = Some(receipt.clone());
        Ok(())
    }

    /// Staged input is retained through every publication failure. Public input
    /// names are resolved against the exact composition; internal ports are hidden.
    pub fn stage(&mut self, input: &str, values: Vec<Value>, delete: bool) -> Result<()> {
        ensure!(
            !self.pending(),
            "Complete or reconcile the pending cut before mutation"
        );
        let predicate = self
            .inputs
            .get(input)
            .ok_or_else(|| anyhow!("Unknown public input"))?;
        let schema = &self.backend.schemas()[predicate];
        ensure!(schema.fields.len() == values.len(), "Input arity mismatch");
        for (kind, value) in schema.fields.iter().zip(&values) {
            ensure!(
                match kind.as_str() {
                    "int" => value.as_i64().is_some(),
                    "string" => value.as_str().is_some(),
                    _ => false,
                },
                "Input type/nullability mismatch"
            );
        }
        self.staged.push(json!({"op":if delete {"delete"} else {"insert"},"predicate":predicate,"values":values}));
        Ok(())
    }

    pub fn freeze(&mut self) -> Result<&FrozenCut> {
        ensure!(
            !self.apply_failed,
            "Input transaction failed; explicit recovery from the last published cut is required"
        );
        if self.frozen.is_none() {
            if !self.applied {
                // Set before exchange: even an uncertain acknowledgement must
                // never cause another execution of this transaction.
                self.apply_failed = true;
                self.backend
                    .apply_without_deltas(&json!(self.staged))
                    .map_err(|e| anyhow!(e))?;
                self.apply_failed = false;
                self.applied = true;
            }
            ensure!(
                self.backend.health() == "ready",
                "Native transaction uncertain; explicit recovery required"
            );
            let mut relations = BTreeMap::new();
            for (name, (predicate, schema)) in &self.components {
                let mut query = BoundedQuery {
                    max_rows: 10000,
                    max_bytes: 4 * 1024 * 1024,
                    ..Default::default()
                };
                let mut rows = vec![];
                loop {
                    let page = self
                        .backend
                        .query_typed_bounded(predicate, &query)
                        .map_err(|e| anyhow!(e))?;
                    rows.extend(page.rows);
                    ensure!(
                        rows.len() <= 1_000_000,
                        "Component exceeds v1 snapshot row limit"
                    );
                    query.continuation = page.continuation;
                    if query.continuation.is_none() {
                        break;
                    }
                }
                schema.validate_rows(&rows)?;
                rows.sort_by_key(|r| r[schema.component.entity_field].as_i64().unwrap());
                relations.insert(
                    name.clone(),
                    RelationState {
                        schema: schema.clone(),
                        rows,
                    },
                );
            }
            let tick = self
                .tick()
                .checked_add(1)
                .ok_or_else(|| anyhow!("Tick overflow"))?;
            let checkpoint = self.backend.checkpoint_bytes(json!({"abi":crate::ADAPTER_ABI,"program":self.program,"world":self.world,"run":self.run,"tick":tick})).map_err(|e| anyhow!(e))?;
            self.frozen = Some(FrozenCut {
                world: self.world.clone(),
                run: self.run.clone(),
                tick,
                program: self.program.clone(),
                ddlog_revision: self.backend.revision(),
                parent: self.published.as_ref().map(|r| r.cut_id.clone()),
                relations,
                checkpoint,
            });
        }
        Ok(self.frozen.as_ref().unwrap())
    }

    /// Durable visibility, not native acknowledgement, advances Archetype time.
    /// Repeated calls after failure reuse the same frozen cut without DDlog work.
    pub async fn step(&mut self, store: &CutStore) -> Result<CutReceipt> {
        let cut = self.freeze()?;
        let receipt = store.publish(cut).await?;
        self.published = Some(receipt.clone());
        self.staged.clear();
        self.frozen = None;
        self.applied = false;
        Ok(receipt)
    }
}
