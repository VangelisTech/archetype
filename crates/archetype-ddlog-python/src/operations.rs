//! Calls existing authorities. No cached head, generation, admission or job state.
use anyhow::{Result, anyhow, ensure};
use archetype_ddlog::{
    component::Component,
    hosted::{HostedCutAdapter, HostedScope, publication_policy},
    store::{CutReceipt, CutStore, attachments::Attachment},
};
use arrow_array::{Array, Int64Array, StringArray};
use ddlog_runtime::{
    registry::ProcessorReference,
    worlds::{
        AdmissionQuery, AdmitInputs, BoundaryKey, InventoryQuery, RegisterRequest, WorldDefinition,
        WorldManager,
    },
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    sync::{Mutex, MutexGuard},
};
use tokio::runtime::Runtime;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Open {
    registry_root: PathBuf,
    build_root: PathBuf,
    driver: PathBuf,
    store_root: PathBuf,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    scope: HostedScope,
    components: Vec<Component>,
}
/// Compact immutable catalog identity, never caller-supplied table authority.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReceiptRef {
    world: String,
    run: String,
    tick: u64,
    cut_id: String,
}
#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Operation {
    Register {
        request: RegisterRequest,
    },
    Definitions {},
    Create {
        label: String,
        processor: ProcessorReference,
        outputs: Vec<String>,
    },
    Bind {
        binding: Binding,
    },
    Start {
        id: String,
    },
    Status {
        id: String,
    },
    Stop {
        id: String,
    },
    Inventory {},
    Admit {
        binding: Binding,
        expected_head: Option<String>,
        admission: AdmitInputs,
    },
    AdmissionStatus {
        query: AdmissionQuery,
    },
    Publish {
        binding: Binding,
        key: BoundaryKey,
    },
    Reconcile {
        binding: Binding,
        key: BoundaryKey,
        tick: u64,
        expected_parent: Option<String>,
    },
    Confirm {
        binding: Binding,
        key: BoundaryKey,
        tick: u64,
        expected_parent: Option<String>,
    },
    History {
        world: String,
        run: String,
        offset: usize,
        limit: usize,
    },
    Read {
        receipt: ReceiptRef,
        component: String,
        offset: usize,
        limit: usize,
    },
    Restore {
        binding: Binding,
        receipt: ReceiptRef,
        expected_generation: u64,
    },
    ArtifactCut {
        binding: Binding,
        receipt: ReceiptRef,
    },
    AttachArtifacts {
        binding: Binding,
        receipt: ReceiptRef,
        attachments: Vec<Attachment>,
    },
    ReadArtifacts {
        binding: Binding,
        receipt: ReceiptRef,
        offset: usize,
        limit: usize,
    },
}
pub struct Resources {
    // Declaration order ensures native owner and store drop before executor.
    pub manager: Mutex<WorldManager>,
    store: CutStore,
    runtime: Runtime,
}
fn native<T>(value: Result<T, String>) -> Result<T> {
    value.map_err(|e| anyhow!(e))
}
impl Resources {
    pub fn open(config: Open) -> Result<Self> {
        ensure!(
            [
                &config.registry_root,
                &config.build_root,
                &config.driver,
                &config.store_root
            ]
            .iter()
            .all(|p| p.is_absolute()),
            "All operator paths must be absolute"
        );
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?;
        let store = runtime.block_on(CutStore::open(&config.store_root))?;
        let manager = Mutex::new(native(WorldManager::new(
            config.registry_root,
            config.build_root,
            config.driver,
        ))?);
        Ok(Self {
            manager,
            store,
            runtime,
        })
    }
    pub fn manager(&self) -> Result<MutexGuard<'_, WorldManager>> {
        self.manager
            .lock()
            .map_err(|_| anyhow!("Native owner poisoned"))
    }
    fn bind(&self, binding: Binding) -> Result<HostedCutAdapter> {
        HostedCutAdapter::bind(&mut *self.manager()?, binding.scope, binding.components)
    }
    pub fn inventory(&self) -> Result<Value> {
        native(self.manager()?.inventory(&InventoryQuery {
            summary: true,
            ..Default::default()
        }))
    }
    fn receipt(&self, key: ReceiptRef) -> Result<CutReceipt> {
        let history = self
            .runtime
            .block_on(self.store.history(&key.world, &key.run))?;
        history
            .into_iter()
            .find(|r| r.tick == key.tick && r.cut_id == key.cut_id)
            .ok_or_else(|| anyhow!("Unknown exact published cut identity"))
    }
    pub fn call(&self, op: Operation) -> Result<Value> {
        match op {
            Operation::Register { request } => native(self.manager()?.register(request)),
            Operation::Definitions {} => native(self.manager()?.definitions()),
            Operation::Create {
                label,
                processor,
                outputs,
            } => {
                let components: Vec<_> = outputs
                    .into_iter()
                    .map(|output| Component {
                        name: String::new(),
                        output,
                        fields: vec![],
                        entity_field: 0,
                    })
                    .collect();
                let id = native(self.manager()?.create(WorldDefinition {
                    label,
                    processor,
                    external_publication: Some(publication_policy(&components)),
                    purpose: "instance".into(),
                    scenarios: vec![],
                }))?;
                Ok(json!({"id":id}))
            }
            Operation::Bind { binding } => {
                self.bind(binding)?;
                Ok(json!({"bound":true}))
            }
            Operation::Start { id } => native(self.manager()?.start_async(&id)),
            Operation::Status { id } => native(self.manager()?.status(&id)),
            Operation::Stop { id } => native(self.manager()?.stop(&id)),
            Operation::Inventory {} => self.inventory(),
            Operation::AdmissionStatus { query } => native(self.manager()?.admission_status(query)),
            Operation::Admit {
                binding,
                expected_head,
                admission,
            } => {
                ensure!(
                    admission
                        .changes
                        .iter()
                        .flat_map(|c| &c.values)
                        .all(|v| v.is_string() || v.as_i64().is_some()),
                    "Cells must be signed Int64 or string; no bool, null or float"
                );
                let a = self.bind(binding)?;
                let ticket = self.runtime.block_on(a.prepare_admission(
                    &self.store,
                    expected_head.as_deref(),
                    admission,
                ))?;
                ticket.submit(&mut *self.manager()?)
            }
            Operation::Publish { binding, key } => {
                let a = self.bind(binding)?;
                let mut capture = a.capture(&mut *self.manager()?, &key)?;
                loop {
                    if capture.advance(&mut *self.manager()?)? {
                        break;
                    }
                    std::thread::yield_now();
                }
                let cut = capture.finish()?;
                let published = self.runtime.block_on(a.publish(&self.store, &cut))?;
                Ok(json!(published.receipt()))
            }
            Operation::Reconcile {
                binding,
                key,
                tick,
                expected_parent,
            } => {
                let a = self.bind(binding)?;
                let published = self.runtime.block_on(a.reconcile(
                    &self.store,
                    tick,
                    expected_parent.as_deref(),
                    &key,
                ))?;
                Ok(json!(published.receipt()))
            }
            Operation::Confirm {
                binding,
                key,
                tick,
                expected_parent,
            } => {
                // Confirmation cannot make an invisible cut visible. Reconcile
                // only reconstructs private authority for the existing exact head.
                let history = self
                    .runtime
                    .block_on(self.store.history(&binding.scope.world, &binding.scope.run))?;
                ensure!(
                    history
                        .last()
                        .is_some_and(|r| r.tick == tick && r.parent == expected_parent),
                    "Confirmation requires the visible latest cut"
                );
                let a = self.bind(binding)?;
                let published = self.runtime.block_on(a.reconcile(
                    &self.store,
                    tick,
                    expected_parent.as_deref(),
                    &key,
                ))?;
                published.confirm(&mut *self.manager()?)
            }
            Operation::History {
                world,
                run,
                offset,
                limit,
            } => {
                ensure!((1..=100).contains(&limit), "History limit must be 1..100");
                let history = self.runtime.block_on(self.store.history(&world, &run))?;
                ensure!(offset <= history.len(), "Offset past history");
                let end = offset.saturating_add(limit).min(history.len());
                Ok(
                    json!({"receipts":history[offset..end],"next_offset":(end<history.len()).then_some(end),"total":history.len()}),
                )
            }
            Operation::Read {
                receipt,
                component,
                offset,
                limit,
            } => {
                ensure!((1..=1000).contains(&limit), "Read limit must be 1..1000");
                let receipt = self.receipt(receipt)?;
                let batches = self
                    .runtime
                    .block_on(self.store.read(&receipt, &component))?;
                let table = &receipt.components[&component];
                ensure!(offset <= table.rows, "Offset past component");
                let mut rows = Vec::new();
                let mut seen = 0;
                for batch in batches {
                    for index in 0..batch.num_rows() {
                        if seen >= offset && rows.len() < limit {
                            let mut row = Vec::new();
                            for column in batch.columns().iter().skip(1) {
                                ensure!(!column.is_null(index), "Null component cell");
                                if let Some(c) = column.as_any().downcast_ref::<Int64Array>() {
                                    row.push(json!(c.value(index)));
                                } else if let Some(c) =
                                    column.as_any().downcast_ref::<StringArray>()
                                {
                                    row.push(json!(c.value(index)));
                                } else {
                                    anyhow::bail!("Unsupported Arrow component type");
                                }
                            }
                            rows.push(row);
                        }
                        seen += 1;
                    }
                }
                let end = offset + rows.len();
                Ok(
                    json!({"schema":table.schema,"rows":rows,"next_offset":(end<table.rows).then_some(end),"total_rows":table.rows,"cut_id":receipt.cut_id}),
                )
            }
            Operation::Restore {
                binding,
                receipt,
                expected_generation,
            } => {
                let a = self.bind(binding)?;
                let receipt = self.receipt(receipt)?;
                let ticket = self
                    .runtime
                    .block_on(a.prepare_restore(&self.store, &receipt))?;
                ticket.restore(&mut *self.manager()?, expected_generation)
            }
            Operation::ArtifactCut { binding, receipt } => {
                let a = self.bind(binding)?;
                let receipt = self.receipt(receipt)?;
                self.runtime
                    .block_on(a.verify_attachment_cut(&self.store, &receipt))?;
                let root = self
                    .runtime
                    .block_on(self.store.attachment_root(&receipt))?;
                Ok(json!({"object_root": root}))
            }
            Operation::AttachArtifacts {
                binding,
                receipt,
                attachments,
            } => {
                let a = self.bind(binding)?;
                let receipt = self.receipt(receipt)?;
                self.runtime
                    .block_on(a.verify_attachment_cut(&self.store, &receipt))?;
                Ok(json!(
                    self.runtime
                        .block_on(self.store.attach(&receipt, &attachments))?
                ))
            }
            Operation::ReadArtifacts {
                binding,
                receipt,
                offset,
                limit,
            } => {
                let a = self.bind(binding)?;
                let receipt = self.receipt(receipt)?;
                self.runtime
                    .block_on(a.verify_attachment_cut(&self.store, &receipt))?;
                let (items, total) = self
                    .runtime
                    .block_on(self.store.attachments(&receipt, offset, limit))?;
                let end = offset + items.len();
                Ok(
                    json!({"items": items, "total": total, "next_offset": (end < total).then_some(end)}),
                )
            }
        }
    }
}
