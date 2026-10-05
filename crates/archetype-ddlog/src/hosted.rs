//! A storage adapter over a caller-owned DDlog WorldManager. No native owner,
//! admission ledger, worker, or task lives here. Release the manager between
//! capture pages; all async catalog work takes only CutStore.
use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Result, anyhow, ensure};
use ddlog_runtime::{
    Schema,
    instance::public_relations,
    registry::{ProcessorDefinition, ProcessorReference, ProcessorRegistry},
    worlds::{
        AdmissionQuery, AdmitInputs, BoundCheckpointRestore, BoundaryAdmission, BoundaryKey,
        CreationRequest, CreationReservation, ExternalPublicationPolicy, ExternalReceipt,
        ForkRequest, ForkReservation, FrozenBlob, FrozenBlobRead, FrozenManifest,
        LogicalDestination, PublicationBinding, WorldDefinition, WorldManager,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    component::{Component, ComponentSchema},
    store::{
        CutReceipt, CutStore, PublicationFault,
        origin::{ForkOrigin, Scope},
    },
    world::{FrozenCut, RelationState},
};

const ABI: &str = "archetype-ddlog-hosted-cut-v1";
const MAX_BYTES: usize = 64 * 1024 * 1024;
const PAGE_BYTES: usize = 4 * 1024 * 1024;

/// Trusted composition supplies this scope, never a transport-selected path.
/// One native binding per analytical world/run is a host composition invariant.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostedScope {
    pub native_world: String,
    pub world: String,
    pub run: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Context {
    abi: String,
    scope: HostedScope,
    program: String,
    tick: u64,
    parent_cut: Option<String>,
}

/// Immutable binding only. Reconstruct from the exact registry definition on
/// reopen; it can bind a stopped world without creating or activating anything.
#[derive(Clone)]
pub struct HostedCutAdapter {
    scope: HostedScope,
    processor: ProcessorReference,
    schemas: BTreeMap<String, ComponentSchema>,
    native_program: Value,
    policy: ExternalPublicationPolicy,
    program: String,
}

/// Validated declaration, independent of an allocated native world. It owns no
/// execution or durable state and can only bind the exact retained program.
pub struct HostedDeclaration {
    processor: ProcessorReference,
    schemas: BTreeMap<String, ComponentSchema>,
    native_program: Value,
    policy: ExternalPublicationPolicy,
    program: String,
}
impl HostedDeclaration {
    pub fn validate_inputs(&self, inputs: &BTreeMap<String, Vec<String>>) -> Result<()> {
        validate_inputs(&self.native_program, inputs)
    }
    pub fn bind(self, scope: HostedScope) -> Result<HostedCutAdapter> {
        ensure!(
            crate::identifier(&scope.world)
                && crate::identifier(&scope.run)
                && !scope.native_world.is_empty(),
            "Invalid analytical scope"
        );
        Ok(HostedCutAdapter {
            scope,
            processor: self.processor,
            schemas: self.schemas,
            native_program: self.native_program,
            policy: self.policy,
            program: self.program,
        })
    }
}
fn validate_inputs(native: &Value, inputs: &BTreeMap<String, Vec<String>>) -> Result<()> {
    let expected: BTreeMap<String, Vec<String>> = native["public_relations"]
        .as_array()
        .ok_or_else(|| anyhow!("Missing native relations"))?
        .iter()
        .filter(|r| r["input"] == true)
        .map(|r| {
            Ok((
                r["name"]
                    .as_str()
                    .ok_or_else(|| anyhow!("Invalid native relation"))?
                    .into(),
                serde_json::from_value(r["fields"].clone())?,
            ))
        })
        .collect::<Result<_>>()?;
    ensure!(
        inputs == &expected
            && inputs.len() <= 64
            && inputs.values().all(|fields| !fields.is_empty()
                && fields.len() <= 64
                && fields
                    .iter()
                    .all(|kind| matches!(kind.as_str(), "int" | "string"))),
        "Input declarations differ from the exact program"
    );
    Ok(())
}

/// Complete immutable declaration evidence for storage-only cold verification.
/// It contains no tick, live revision, checkpoint or admission state.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostedEvidence {
    pub(crate) scope: HostedScope,
    abi: String,
    ddlog_revision: String,
    processor: ProcessorReference,
    schemas: BTreeMap<String, ComponentSchema>,
    native_program: Value,
    policy: ExternalPublicationPolicy,
    program: String,
}
impl HostedEvidence {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            self.abi == ABI
                && self.ddlog_revision.len() == 40
                && self.ddlog_revision.bytes().all(|b| b.is_ascii_hexdigit())
                && crate::identifier(&self.scope.world)
                && crate::identifier(&self.scope.run)
                && !self.scope.native_world.is_empty(),
            "Invalid hosted context provenance"
        );
        ensure!(
            !self.schemas.is_empty() && self.schemas.len() <= 64,
            "Invalid context declarations"
        );
        let mut outputs = BTreeSet::new();
        for (name, schema) in &self.schemas {
            let checked = ComponentSchema::new(
                schema.component.clone(),
                &Schema {
                    input: false,
                    fields: schema.types.clone(),
                },
            )?;
            ensure!(
                *name == schema.component.name
                    && checked == *schema
                    && outputs.insert(schema.component.output.clone()),
                "Invalid context component schema"
            );
            ensure!(
                self.native_program["public_relations"]
                    .as_array()
                    .is_some_and(|relations| relations.iter().any(|r| r["name"]
                        == schema.component.output
                        && r["input"] == false
                        && r["fields"]
                            == serde_json::to_value(&schema.types).unwrap_or(Value::Null))),
                "Context declarations differ from native program"
            );
        }
        let components = self
            .schemas
            .values()
            .map(|s| s.component.clone())
            .collect::<Vec<_>>();
        ensure!(
            self.policy == publication_policy(&components)
                && self.native_program["processor"] == serde_json::to_value(&self.processor)?,
            "Context publication policy/processor mismatch"
        );
        ensure!(
            self.program
                == canonical_digest(
                    &json!({"abi":self.abi,"ddlog":self.ddlog_revision,"native_program":self.native_program,"components":self.schemas})
                )?,
            "Context program digest mismatch"
        );
        Ok(())
    }
    pub(crate) fn check_cut(&self, cut: &FrozenCut, own_scope: bool) -> Result<()> {
        cut.validate()?;
        let manifest = cut
            .hosted
            .as_ref()
            .ok_or_else(|| anyhow!("Hosted cut evidence required"))?;
        ensure!(
            manifest.policy == self.policy
                && cut.program == self.program
                && manifest.checkpoint_receipt["program"] == self.native_program
                && cut
                    .relations
                    .iter()
                    .map(|(n, r)| (n, &r.schema))
                    .collect::<BTreeMap<_, _>>()
                    == self.schemas.iter().collect::<BTreeMap<_, _>>(),
            "Cut differs from published context"
        );
        if own_scope {
            ensure!(
                cut.world == self.scope.world
                    && cut.run == self.scope.run
                    && manifest.key.world_id == self.scope.native_world
                    && manifest.binding.context
                        == json!({"abi":self.abi,"scope":self.scope,"program":self.program,"tick":cut.tick,"parent_cut":cut.parent}),
                "Cut belongs to another hosted context"
            );
        }
        Ok(())
    }
}

pub fn publication_policy(components: &[Component]) -> ExternalPublicationPolicy {
    let mut outputs: Vec<_> = components.iter().map(|c| c.output.clone()).collect();
    outputs.sort();
    ExternalPublicationPolicy {
        namespace: "archetype".into(),
        outputs,
        max_rows: 1_000_000,
        max_bytes: MAX_BYTES,
    }
}

pub(crate) fn canonical_digest(value: &impl Serialize) -> Result<String> {
    crate::digest(&serde_json::to_value(value)?)
}
fn program_digest(native: &Value, schemas: &BTreeMap<String, ComponentSchema>) -> Result<String> {
    canonical_digest(&json!({"abi":ABI,"ddlog":crate::DDLOG_REVISION,
        "native_program":native,"components":schemas}))
}

impl HostedCutAdapter {
    pub fn validate_inputs(&self, inputs: &BTreeMap<String, Vec<String>>) -> Result<()> {
        validate_inputs(&self.native_program, inputs)
    }
    pub fn context_draft(&self) -> Result<crate::store::contexts::ContextDraft> {
        crate::store::contexts::ContextDraft::hosted(HostedEvidence {
            scope: self.scope.clone(),
            abi: ABI.into(),
            ddlog_revision: crate::DDLOG_REVISION.into(),
            processor: self.processor.clone(),
            schemas: self.schemas.clone(),
            native_program: self.native_program.clone(),
            policy: self.policy.clone(),
            program: self.program.clone(),
        })
    }
    pub fn bind(
        manager: &mut WorldManager,
        scope: HostedScope,
        components: Vec<Component>,
    ) -> Result<Self> {
        ensure!(
            crate::identifier(&scope.world) && crate::identifier(&scope.run),
            "Invalid analytical scope"
        );
        let status = manager
            .status(&scope.native_world)
            .map_err(|e| anyhow!(e))?;
        let definition: WorldDefinition = serde_json::from_value(status["definition"].clone())?;
        Self::preflight(
            &manager.registry().map_err(|e| anyhow!(e))?,
            definition,
            components,
        )?
        .bind(scope)
    }
    /// Validate the exact retained program and all persistent declarations
    /// before the native owner allocates a destination.
    pub fn preflight(
        registry: &ProcessorRegistry,
        definition: WorldDefinition,
        components: Vec<Component>,
    ) -> Result<HostedDeclaration> {
        let policy = publication_policy(&components);
        ensure!(
            definition.external_publication.as_ref() == Some(&policy),
            "Hosted publication policy mismatch"
        );
        let mut record = registry
            .get(
                &definition.processor.processor_id,
                Some(&definition.processor.version),
            )
            .map_err(|e| anyhow!(e))?;
        let ProcessorDefinition::Composition(composition) = &record.definition else {
            anyhow::bail!("Hosted ECS adapter requires a pinned composition");
        };
        let compiled = registry
            .compile_composition_versioned(&composition.composition, 2)
            .map_err(|e| anyhow!(e))?;
        record.composition = Some(compiled.resolution.clone());
        let relations = public_relations(&record).map_err(|e| anyhow!(e))?;
        let native_program = json!({"processor":definition.processor,
            "dependencies":compiled.resolution.dependencies,"public_relations":relations,
            "lowering_version":2,"source_sha256":crate::hash(compiled.source.as_bytes())});
        let mut schemas = BTreeMap::new();
        let mut outputs = BTreeSet::new();
        for component in components {
            let relation = relations
                .iter()
                .find(|r| r.name == component.output && !r.input)
                .ok_or_else(|| anyhow!("Unknown persistent output"))?;
            ensure!(
                outputs.insert(component.output.clone()),
                "Duplicate persistent output"
            );
            let schema = ComponentSchema::new(
                component,
                &Schema {
                    input: false,
                    fields: relation.fields.clone(),
                },
            )?;
            ensure!(
                schemas
                    .insert(schema.component.name.clone(), schema)
                    .is_none(),
                "Duplicate component"
            );
        }
        ensure!(
            !schemas.is_empty() && schemas.len() <= 64,
            "Invalid component inventory"
        );
        let program = program_digest(&native_program, &schemas)?;
        Ok(HostedDeclaration {
            processor: definition.processor,
            schemas,
            native_program,
            policy,
            program,
        })
    }

    fn context(&self, tick: u64, parent_cut: Option<String>) -> Context {
        Context {
            abi: ABI.into(),
            scope: self.scope.clone(),
            program: self.program.clone(),
            tick,
            parent_cut,
        }
    }
    fn check_owner(&self, manager: &mut WorldManager) -> Result<()> {
        let status = manager
            .status(&self.scope.native_world)
            .map_err(|e| anyhow!(e))?;
        let def: WorldDefinition = serde_json::from_value(status["definition"].clone())?;
        ensure!(
            def.processor == self.processor
                && def.external_publication.as_ref() == Some(&self.policy),
            "Native owner binding changed"
        );
        Ok(())
    }
    fn check_cut(&self, cut: &FrozenCut) -> Result<()> {
        self.check_compatible(cut)?;
        let manifest = cut.hosted.as_ref().unwrap();
        ensure!(
            manifest.binding.context == json!(self.context(cut.tick, cut.parent.clone()))
                && manifest.key.world_id == self.scope.native_world,
            "Hosted scope mismatch"
        );
        Ok(())
    }
    fn check_compatible(&self, cut: &FrozenCut) -> Result<()> {
        cut.validate()?;
        let manifest = cut
            .hosted
            .as_ref()
            .ok_or_else(|| anyhow!("Hosted evidence required"))?;
        ensure!(
            manifest.policy == self.policy
                && cut.program == self.program
                && manifest.checkpoint_receipt["program"] == self.native_program
                && cut
                    .relations
                    .iter()
                    .map(|(n, r)| (n, &r.schema))
                    .collect::<BTreeMap<_, _>>()
                    == self.schemas.iter().collect::<BTreeMap<_, _>>(),
            "Hosted scope/program/schema mismatch"
        );
        Ok(())
    }

    async fn scoped_cut(&self, store: &CutStore, receipt: &CutReceipt) -> Result<FrozenCut> {
        ensure!(
            store
                .history(&self.scope.world, &self.scope.run)
                .await?
                .contains(receipt),
            "Cut is outside bound lineage"
        );
        let cut = store.verified_cut(receipt).await?;
        if receipt.world == self.scope.world && receipt.run == self.scope.run {
            self.check_cut(&cut)?;
        } else {
            let origin = store
                .origin(&self.scope.world, &self.scope.run)?
                .ok_or_else(|| anyhow!("Missing inherited origin"))?;
            ensure!(
                origin.reservation.child_world_id == self.scope.native_world,
                "Origin belongs to another native child"
            );
            self.check_compatible(&cut)?;
        }
        Ok(cut)
    }

    /// Historical selection is separate from latest-only resume. All source
    /// catalog, snapshot and checkpoint verification precedes native reservation.
    pub async fn prepare_fork(
        &self,
        store: &CutStore,
        source: &CutReceipt,
        request_key: String,
        destination: Scope,
        label: String,
    ) -> Result<PreparedFork> {
        let mut prepared = self
            .prepare_fork_source(store, source, request_key, destination, label)
            .await?;
        prepared.check_new_destination(store).await?;
        Ok(prepared)
    }
    /// Verify the source without claiming the destination. Logical creation
    /// checks a new destination before reserve, or reconciles the exact retained
    /// reservation before accepting a context-before-origin retry.
    pub async fn prepare_fork_source(
        &self,
        store: &CutStore,
        source: &CutReceipt,
        request_key: String,
        destination: Scope,
        label: String,
    ) -> Result<PreparedFork> {
        let store = store.read_scope().await?;
        destination.validate()?;
        let source_scope = Scope {
            world: self.scope.world.clone(),
            run: self.scope.run.clone(),
        };
        ensure!(destination != source_scope, "Cannot fork onto source scope");
        let cut = self.scoped_cut(&store, source).await?;
        let external = external_receipt(&cut, source)?;
        let request = ForkRequest {
            request_key,
            destination: json!(destination),
            source_context: json!(source_scope),
            definition: WorldDefinition {
                label,
                processor: self.processor.clone(),
                external_publication: Some(self.policy.clone()),
                purpose: "instance".into(),
                scenarios: vec![],
            },
            manifest: cut.hosted.clone().unwrap(),
            published: external.clone(),
            checkpoint_bytes: cut.checkpoint.clone(),
        };
        Ok(PreparedFork {
            adapter: self.clone(),
            source: source.clone(),
            external,
            request,
            destination_checked: false,
        })
    }
    pub async fn verify_origin(&self, store: &CutStore) -> Result<ForkOrigin> {
        let origin = store
            .origin(&self.scope.world, &self.scope.run)?
            .ok_or_else(|| anyhow!("Missing fork origin"))?;
        ensure!(
            origin.reservation.child_world_id == self.scope.native_world,
            "Origin native child mismatch"
        );
        self.scoped_cut(store, &origin.source).await?;
        Ok(origin)
    }

    /// Verify an exact committed cut, including historical cuts, without moving
    /// the native generation or the analytical publication head.
    pub async fn verify_attachment_cut(
        &self,
        store: &CutStore,
        receipt: &CutReceipt,
    ) -> Result<()> {
        self.check_cut(&store.verified_cut(receipt).await?)
    }

    /// Storage read/verification finishes before the returned ticket borrows the
    /// native manager. The native owner still checks generation/revision/parent.
    pub async fn prepare_admission(
        &self,
        store: &CutStore,
        expected_head: Option<&str>,
        admission: AdmitInputs,
    ) -> Result<PreparedAdmission> {
        ensure!(
            admission.id == self.scope.native_world
                && admission.effect.is_none()
                && admission.worker_id.is_none(),
            "Admission scope/effect mismatch"
        );
        let history = store.history(&self.scope.world, &self.scope.run).await?;
        let latest = history.last();
        ensure!(
            latest.map(|r| r.cut_id.as_str()) == expected_head,
            "Stale analytical parent"
        );
        let parent_receipt_sha256 = if let Some(receipt) = latest {
            let cut = self.scoped_cut(store, receipt).await?;
            Some(external_receipt(&cut, receipt)?.receipt_sha256)
        } else {
            None
        };
        let tick = latest
            .map_or(Some(1), |r| r.tick.checked_add(1))
            .ok_or_else(|| anyhow!("Tick overflow"))?;
        let context = self.context(tick, expected_head.map(str::to_owned));
        Ok(PreparedAdmission {
            adapter: self.clone(),
            request: BoundaryAdmission {
                admission,
                binding: PublicationBinding {
                    context: json!(context),
                    parent_receipt_sha256,
                },
            },
        })
    }

    /// Start an immutable capture. Each advance reads at most one page and
    /// releases its manager borrow before the caller schedules further work.
    pub fn capture(&self, manager: &mut WorldManager, key: &BoundaryKey) -> Result<Capture> {
        self.check_owner(manager)?;
        ensure!(
            key.world_id == self.scope.native_world,
            "Boundary world mismatch"
        );
        let status = manager
            .admission_status(AdmissionQuery {
                id: key.world_id.clone(),
                generation: key.generation,
                admission_key: key.admission_key.clone(),
            })
            .map_err(|e| anyhow!(e))?;
        ensure!(
            status["boundary"]["key"] == json!(key),
            "Boundary request identity mismatch"
        );
        let manifest: FrozenManifest =
            serde_json::from_value(status["boundary"]["manifest"].clone())?;
        let context = validate_manifest(&manifest)?;
        ensure!(
            manifest.binding.context == json!(self.context(context.tick, context.parent_cut))
                && manifest.policy == self.policy
                && manifest.checkpoint_receipt["program"] == self.native_program,
            "Frozen context/policy/program mismatch"
        );
        let blobs = std::iter::once(manifest.checkpoint.clone())
            .chain(manifest.outputs.iter().map(|o| o.blob.clone()))
            .collect();
        Ok(Capture {
            adapter: self.clone(),
            manifest,
            blobs,
            completed: vec![],
            current: vec![],
        })
    }

    /// No native manager is borrowed while any catalog work is awaited.
    pub async fn publish(&self, store: &CutStore, cut: &FrozenCut) -> Result<PublishedCut> {
        self.publish_with_fault(store, cut, PublicationFault::None)
            .await
    }
    pub async fn publish_with_fault(
        &self,
        store: &CutStore,
        cut: &FrozenCut,
        fault: PublicationFault,
    ) -> Result<PublishedCut> {
        self.check_cut(cut)?;
        self.check_parent(store, cut).await?;
        let receipt = store.publish_with_fault(cut, fault).await?;
        self.verified_publication(store, &receipt).await
    }
    async fn check_parent(&self, store: &CutStore, cut: &FrozenCut) -> Result<()> {
        let history = store.history(&self.scope.world, &self.scope.run).await?;
        let latest = history.last();
        let already_visible =
            latest.is_some_and(|r| r.cut_id == cut.identity().unwrap_or_default());
        ensure!(
            if already_visible {
                latest.unwrap().tick == cut.tick
            } else {
                latest.map(|r| &r.cut_id) == cut.parent.as_ref()
                    && latest.map_or(Some(1), |r| r.tick.checked_add(1)) == Some(cut.tick)
            },
            "Publication is not the exact latest successor"
        );
        let parent = if cut.tick == 1 {
            None
        } else {
            history.iter().find(|r| r.tick == cut.tick - 1)
        };
        ensure!(
            parent.map(|r| &r.cut_id) == cut.parent.as_ref(),
            "Cut parent mismatch"
        );
        let expected = if let Some(receipt) = parent {
            let parent_cut = self.scoped_cut(store, receipt).await?;
            Some(external_receipt(&parent_cut, receipt)?.receipt_sha256)
        } else {
            None
        };
        ensure!(
            cut.hosted.as_ref().unwrap().binding.parent_receipt_sha256 == expected,
            "Native receipt parent mismatch"
        );
        Ok(())
    }
    async fn verified_publication(
        &self,
        store: &CutStore,
        receipt: &CutReceipt,
    ) -> Result<PublishedCut> {
        let cut = store.verified_cut(receipt).await?;
        self.check_cut(&cut)?;
        self.check_parent(store, &cut).await?;
        let external = external_receipt(&cut, receipt)?;
        Ok(PublishedCut {
            adapter: self.clone(),
            receipt: receipt.clone(),
            cut,
            external,
        })
    }

    /// Exact journal only, either the next cut from expected_parent or its
    /// already-visible immediate successor after a lost acknowledgment.
    pub async fn reconcile(
        &self,
        store: &CutStore,
        tick: u64,
        expected_parent: Option<&str>,
        key: &BoundaryKey,
    ) -> Result<PublishedCut> {
        let cut = store.load_frozen(&self.scope.world, &self.scope.run, tick)?;
        self.check_cut(&cut)?;
        ensure!(
            cut.parent.as_deref() == expected_parent && cut.hosted.as_ref().unwrap().key == *key,
            "Recovery parent/boundary mismatch"
        );
        self.publish(store, &cut).await
    }

    /// Only the verified latest same-world/run cut can prepare native resume.
    pub async fn prepare_restore(
        &self,
        store: &CutStore,
        receipt: &CutReceipt,
    ) -> Result<RestoreTicket> {
        ensure!(
            store
                .history(&self.scope.world, &self.scope.run)
                .await?
                .last()
                == Some(receipt),
            "Restore requires exact latest analytical head"
        );
        Ok(RestoreTicket {
            published: self.verified_publication(store, receipt).await?,
        })
    }
}

pub struct PreparedFork {
    adapter: HostedCutAdapter,
    source: CutReceipt,
    external: ExternalReceipt,
    request: ForkRequest,
    destination_checked: bool,
}
impl PreparedFork {
    pub async fn check_new_destination(&mut self, store: &CutStore) -> Result<()> {
        let store = store.read_scope().await?;
        store
            .check_fork_destination(
                &serde_json::from_value(self.request.destination.clone())?,
                &self.request.request_key,
                &self.source,
                &serde_json::from_value(self.request.source_context.clone())?,
            )
            .await?;
        self.destination_checked = true;
        Ok(())
    }
    pub fn reserve_logical(
        &self,
        manager: &mut WorldManager,
        destination: LogicalDestination,
        binding: Value,
    ) -> Result<(CreationReservation, ReservedFork)> {
        self.adapter.check_owner(manager)?;
        ensure!(
            self.destination_checked
                || manager
                    .lookup_creation(&destination)
                    .map_err(|e| anyhow!(e))?
                    .is_some(),
            "New logical fork requires storage destination preflight"
        );
        ensure!(
            json!({"world":destination.world,"run":destination.run}) == self.request.destination,
            "Logical fork destination differs from verified source request"
        );
        let reservation = manager
            .reserve_creation(CreationRequest {
                request_key: self.request.request_key.clone(),
                destination: destination.clone(),
                definition: self.request.definition.clone(),
                binding,
                fork: Some(Box::new(self.request.clone())),
            })
            .map_err(|e| anyhow!(e))?;
        let fork = manager
            .resolve_creation(&destination)
            .map_err(|e| anyhow!(e))?
            .fork
            .ok_or_else(|| anyhow!("Logical reservation has no fork source"))?;
        Ok((reservation, self.reserved(manager, fork)?))
    }
    pub fn reserve(&self, manager: &mut WorldManager) -> Result<ReservedFork> {
        ensure!(
            self.destination_checked,
            "Fork requires storage destination preflight"
        );
        self.adapter.check_owner(manager)?;
        let reservation = manager
            .reserve_fork(self.request.clone())
            .map_err(|e| anyhow!(e))?;
        self.reserved(manager, reservation)
    }
    fn reserved(
        &self,
        manager: &mut WorldManager,
        reservation: ForkReservation,
    ) -> Result<ReservedFork> {
        let scope: Scope = serde_json::from_value(reservation.destination.clone())?;
        let adapter = HostedCutAdapter::bind(
            manager,
            HostedScope {
                native_world: reservation.child_world_id.clone(),
                world: scope.world,
                run: scope.run,
            },
            self.adapter
                .schemas
                .values()
                .map(|s| s.component.clone())
                .collect(),
        )?;
        ensure!(
            adapter.program == self.adapter.program,
            "Fork program changed"
        );
        let origin = ForkOrigin {
            version: 1,
            lineage_sha256: reservation.lineage_sha256().map_err(|e| anyhow!(e))?,
            reservation,
            source: self.source.clone(),
            external: self.external.clone(),
        };
        Ok(ReservedFork {
            adapter,
            origin,
            checkpoint: self.request.checkpoint_bytes.clone(),
        })
    }
}
pub struct ReservedFork {
    adapter: HostedCutAdapter,
    origin: ForkOrigin,
    checkpoint: Vec<u8>,
}
impl ReservedFork {
    pub async fn check_logical_context(&self, store: &CutStore) -> Result<()> {
        store
            .read_scope()
            .await?
            .check_logical_fork_context(&self.origin, &self.adapter.context_draft()?)
            .await
    }
    pub fn adapter(&self) -> &HostedCutAdapter {
        &self.adapter
    }
    pub fn origin(&self) -> &ForkOrigin {
        &self.origin
    }
    pub fn reservation(&self) -> &ForkReservation {
        &self.origin.reservation
    }
    pub async fn publish_origin(&self, store: &CutStore) -> Result<()> {
        store.read_scope().await?.publish_origin(&self.origin).await
    }
    pub fn restore(&self, manager: &mut WorldManager, expected_generation: u64) -> Result<Value> {
        self.adapter.check_owner(manager)?;
        manager
            .restore_fork_async(
                self.origin.reservation.clone(),
                expected_generation,
                self.checkpoint.clone(),
            )
            .map_err(|e| anyhow!(e))
    }
    /// Storage proof finishes before manager access; the returned ticket only
    /// acknowledges this exact immutable origin, never advances an input head.
    pub async fn prepare_confirmation(&self, store: &CutStore) -> Result<ForkConfirmation> {
        let store = store.read_scope().await?;
        let dest = self.origin.destination()?;
        ensure!(
            store.origin(&dest.world, &dest.run)?.as_ref() == Some(&self.origin),
            "Fork origin is not durable"
        );
        store.verified_cut(&self.origin.source).await?;
        Ok(ForkConfirmation {
            origin: self.origin.clone(),
        })
    }
}
pub struct ForkConfirmation {
    origin: ForkOrigin,
}
impl ForkConfirmation {
    pub fn confirm(&self, manager: &mut WorldManager) -> Result<Value> {
        manager
            .confirm_fork_lineage(
                self.origin.reservation.clone(),
                self.origin.lineage_sha256.clone(),
            )
            .map_err(|e| anyhow!(e))
    }
}

pub struct PreparedAdmission {
    adapter: HostedCutAdapter,
    request: BoundaryAdmission,
}
impl PreparedAdmission {
    /// Repeating this exact ticket is an upstream lookup, never input replay.
    pub fn submit(&self, manager: &mut WorldManager) -> Result<Value> {
        self.adapter.check_owner(manager)?;
        manager
            .admit_boundary_async(self.request.clone())
            .map_err(|e| anyhow!(e))
    }
}

/// Owned page assembly, not execution state. Dropping it cannot release a
/// native barrier. Restart capture reads the same immutable upstream objects.
pub struct Capture {
    adapter: HostedCutAdapter,
    manifest: FrozenManifest,
    blobs: Vec<FrozenBlob>,
    completed: Vec<Vec<u8>>,
    current: Vec<u8>,
}
impl Capture {
    pub fn advance(&mut self, manager: &mut WorldManager) -> Result<bool> {
        if self.completed.len() == self.blobs.len() {
            return Ok(true);
        }
        let blob = &self.blobs[self.completed.len()];
        let page = manager
            .read_boundary_blob(FrozenBlobRead {
                key: self.manifest.key.clone(),
                blob_sha256: blob.sha256.clone(),
                offset: self.current.len() as u64,
                max_bytes: PAGE_BYTES,
            })
            .map_err(|e| anyhow!(e))?;
        ensure!(
            page.bytes.len() <= PAGE_BYTES
                && self.current.len() as u64 + page.bytes.len() as u64 <= blob.bytes,
            "Frozen page exceeds descriptor"
        );
        let previous = self.current.len() as u64;
        self.current.extend(page.bytes);
        if let Some(next) = page.next_offset {
            ensure!(
                next == self.current.len() as u64 && next < blob.bytes && next > previous,
                "Invalid frozen page continuation"
            );
        } else {
            verify_blob(blob, &self.current)?;
            self.completed.push(std::mem::take(&mut self.current));
        }
        Ok(self.completed.len() == self.blobs.len())
    }
    pub fn finish(self) -> Result<FrozenCut> {
        ensure!(
            self.completed.len() == self.blobs.len(),
            "Incomplete immutable capture"
        );
        let mut bytes = self.completed.into_iter();
        let checkpoint = bytes.next().ok_or_else(|| anyhow!("Missing checkpoint"))?;
        let mut relations = BTreeMap::new();
        for (output, bytes) in self.manifest.outputs.iter().zip(bytes) {
            let schema = self
                .adapter
                .schemas
                .values()
                .find(|s| s.component.output == output.name)
                .ok_or_else(|| anyhow!("Undeclared output"))?
                .clone();
            let rows = serde_json::from_slice(&bytes)?;
            relations.insert(
                schema.component.name.clone(),
                RelationState { schema, rows },
            );
        }
        let context = validate_manifest(&self.manifest)?;
        let cut = FrozenCut {
            world: context.scope.world,
            run: context.scope.run,
            tick: context.tick,
            program: context.program,
            ddlog_revision: self.manifest.revision,
            parent: context.parent_cut,
            relations,
            checkpoint,
            hosted: Some(self.manifest),
        };
        self.adapter.check_cut(&cut)?;
        Ok(cut)
    }
}

/// Cannot be constructed or deserialized from an unverified caller receipt.
/// Retain this ticket across a failed confirmation, or reconstruct via reconcile.
pub struct PublishedCut {
    adapter: HostedCutAdapter,
    receipt: CutReceipt,
    cut: FrozenCut,
    external: ExternalReceipt,
}
impl PublishedCut {
    pub fn receipt(&self) -> &CutReceipt {
        &self.receipt
    }
    pub fn external_receipt(&self) -> &ExternalReceipt {
        &self.external
    }
    pub fn confirm(&self, manager: &mut WorldManager) -> Result<Value> {
        self.adapter.check_owner(manager)?;
        manager
            .confirm_boundary_published(
                self.cut.hosted.as_ref().unwrap().key.clone(),
                self.external.clone(),
            )
            .map_err(|e| anyhow!(e))
    }
}
pub struct RestoreTicket {
    published: PublishedCut,
}
impl RestoreTicket {
    pub fn restore(self, manager: &mut WorldManager, expected_generation: u64) -> Result<Value> {
        self.published.adapter.check_owner(manager)?;
        manager
            .restore_boundary_async(BoundCheckpointRestore {
                target_world_id: self.published.adapter.scope.native_world,
                expected_generation,
                manifest: self.published.cut.hosted.unwrap(),
                checkpoint_bytes: self.published.cut.checkpoint,
                published: self.published.external,
            })
            .map_err(|e| anyhow!(e))
    }
}

pub(crate) fn external_receipt(cut: &FrozenCut, receipt: &CutReceipt) -> Result<ExternalReceipt> {
    crate::store::validate_receipt(receipt, cut)?;
    let frozen_manifest_sha256 = canonical_digest(
        cut.hosted
            .as_ref()
            .ok_or_else(|| anyhow!("Missing hosted manifest"))?,
    )?;
    let body = acknowledgment_body(receipt)?;
    let receipt_sha256 =
        canonical_digest(&json!({"frozen_manifest_sha256":frozen_manifest_sha256,"receipt":body}))?;
    Ok(ExternalReceipt {
        frozen_manifest_sha256,
        receipt_sha256,
        receipt: body,
    })
}
// The native 1 MiB receipt limit must not depend on component schema width.
// All fields of the full verified catalog receipt remain bound by this digest.
fn acknowledgment_body(receipt: &CutReceipt) -> Result<Value> {
    Ok(
        json!({"abi":ABI,"world":receipt.world,"run":receipt.run,"tick":receipt.tick,
        "cut_id":receipt.cut_id,"cut_receipt_sha256":canonical_digest(receipt)?}),
    )
}

fn verify_blob(blob: &FrozenBlob, bytes: &[u8]) -> Result<()> {
    ensure!(
        blob.bytes <= MAX_BYTES as u64
            && bytes.len() as u64 == blob.bytes
            && crate::hash(bytes) == blob.sha256,
        "Frozen blob length/digest mismatch"
    );
    Ok(())
}
fn valid_hash(value: &Value) -> bool {
    value.as_str().is_some_and(|s| {
        s.len() == 64
            && s.bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
    })
}
fn validate_manifest(manifest: &FrozenManifest) -> Result<Context> {
    let context: Context = serde_json::from_value(manifest.binding.context.clone())?;
    ensure!(
        context.abi == ABI
            && context.tick > 0
            && context.scope.native_world == manifest.key.world_id
            && crate::identifier(&context.scope.world)
            && crate::identifier(&context.scope.run)
            && (context.tick == 1) == context.parent_cut.is_none()
            && context.parent_cut.is_none() == manifest.binding.parent_receipt_sha256.is_none(),
        "Invalid hosted context"
    );
    let receipt = &manifest.checkpoint_receipt;
    let origin = &receipt["origin"];
    ensure!(
        manifest.schema_version == 1
            && receipt["schema_version"] == 1
            && receipt["format"] == "json"
            && receipt.get("storage").is_none()
            && manifest.key.generation > 0
            && origin["world_id"] == manifest.key.world_id
            && origin["generation"] == manifest.key.generation
            && origin["revision"] == manifest.revision
            && receipt["boundary"]
                == json!({"key":manifest.key,"policy":manifest.policy,"binding":manifest.binding})
            && receipt["program"]["lowering_version"] == 2
            && origin["build"]["source_sha256"] == receipt["program"]["source_sha256"]
            && origin["build"]["program_version"] == origin["program_version"]
            && origin["build"]["lowering_version"] == 2
            && valid_hash(&origin["build"]["native_sha256"]),
        "Managed checkpoint manifest/provenance mismatch"
    );
    ensure!(
        manifest.policy.namespace == "archetype"
            && !manifest.outputs.is_empty()
            && manifest.outputs.len() <= 64
            && manifest.policy.max_rows <= 1_000_000
            && manifest.policy.max_rows > 0
            && manifest.policy.max_bytes <= MAX_BYTES
            && manifest.policy.max_bytes > 0
            && manifest.outputs.iter().map(|o| &o.name).collect::<Vec<_>>()
                == manifest.policy.outputs.iter().collect::<Vec<_>>()
            && manifest
                .policy
                .outputs
                .iter()
                .collect::<BTreeSet<_>>()
                .len()
                == manifest.outputs.len()
            && manifest
                .outputs
                .iter()
                .try_fold(0u64, |n, o| n.checked_add(o.blob.bytes))
                .is_some_and(|n| n <= manifest.policy.max_bytes as u64)
            && manifest.checkpoint.bytes <= MAX_BYTES as u64,
        "Invalid frozen output inventory/bounds"
    );
    Ok(context)
}

pub(crate) fn validate_frozen(cut: &FrozenCut) -> Result<()> {
    let manifest = cut
        .hosted
        .as_ref()
        .ok_or_else(|| anyhow!("Missing hosted evidence"))?;
    let context = validate_manifest(manifest)?;
    ensure!(
        context.scope.world == cut.world
            && context.scope.run == cut.run
            && context.tick == cut.tick
            && context.program == cut.program
            && context.parent_cut == cut.parent
            && manifest.revision == cut.ddlog_revision
            && manifest.outputs.len() == cut.relations.len(),
        "Frozen analytical attribution mismatch"
    );
    let schemas = cut
        .relations
        .iter()
        .map(|(name, r)| (name.clone(), r.schema.clone()))
        .collect();
    ensure!(
        program_digest(&manifest.checkpoint_receipt["program"], &schemas)? == cut.program,
        "Program/declaration digest mismatch"
    );
    let public = manifest.checkpoint_receipt["program"]["public_relations"]
        .as_array()
        .ok_or_else(|| anyhow!("Missing public schema inventory"))?;
    for output in &manifest.outputs {
        let relation = cut
            .relations
            .values()
            .find(|r| r.schema.component.output == output.name)
            .ok_or_else(|| anyhow!("Missing frozen output"))?;
        ensure!(
            output.fields == relation.schema.types
                && output.rows == relation.rows.len()
                && output.rows <= manifest.policy.max_rows
                && public.iter().any(|r| r["name"] == output.name
                    && r["input"] == false
                    && r["fields"] == json!(output.fields)),
            "Frozen output schema/count mismatch"
        );
        relation.schema.validate_rows(&relation.rows)?;
        // Preserve upstream row order so a reopened journal reproduces its blob.
        verify_blob(&output.blob, &serde_json::to_vec(&relation.rows)?)?;
    }
    verify_blob(&manifest.checkpoint, &cut.checkpoint)?;
    let checkpoint: Value = serde_json::from_slice(&cut.checkpoint)?;
    let receipt = &manifest.checkpoint_receipt;
    let state = &checkpoint["state"];
    ensure!(
        checkpoint["sha256"] == canonical_digest(state)?
            && checkpoint["sha256"] == receipt["checkpoint_sha256"]
            && state["format_version"] == 1
            && state["revision"] == manifest.revision
            && state["program_version"] == receipt["origin"]["program_version"]
            && state["lowering_version"] == receipt["program"]["lowering_version"]
            && state["source"].as_str().is_some_and(
                |s| json!(crate::hash(s.as_bytes())) == receipt["program"]["source_sha256"]
            )
            && state["metadata"]
                == json!({"world_checkpoint":{"schema_version":receipt["schema_version"],
            "receipt_id":receipt["receipt_id"],"program":receipt["program"],"origin":receipt["origin"],"boundary":receipt["boundary"]}}),
        "Managed checkpoint digest/attribution mismatch"
    );
    Ok(())
}

#[cfg(test)]
mod receipt_bounds {
    use super::*;
    #[test]
    fn wide_catalog_receipt_has_a_compact_exact_acknowledgment() -> Result<()> {
        let schema = ComponentSchema {
            component: Component {
                name: "wide".into(),
                output: "wide".into(),
                fields: std::iter::once("entity_id".into())
                    .chain((1..100_000).map(|n| format!("field_{n}")))
                    .collect(),
                entity_field: 0,
            },
            types: vec!["int".into(); 100_000],
        };
        let mut receipt = CutReceipt {
            cut_id: "a".repeat(64),
            world: "world".into(),
            run: "run".into(),
            tick: 1,
            program: "b".repeat(64),
            parent: None,
            checkpoint_sha256: "c".repeat(64),
            frozen_manifest_sha256: Some("d".repeat(64)),
            components: BTreeMap::from([(
                "wide".into(),
                crate::store::TableCut {
                    table: "component_wide".into(),
                    table_uuid: "uuid".into(),
                    snapshot: Some(1),
                    object: Some("object".into()),
                    object_sha256: Some("e".repeat(64)),
                    rows: 1,
                    schema,
                },
            )]),
        };
        assert!(serde_json::to_vec(&receipt)?.len() > 1024 * 1024);
        let first = acknowledgment_body(&receipt)?;
        assert!(serde_json::to_vec(&first)?.len() < 1024);
        receipt.components.get_mut("wide").unwrap().snapshot = Some(2);
        assert_ne!(
            first["cut_receipt_sha256"],
            acknowledgment_body(&receipt)?["cut_receipt_sha256"]
        );
        Ok(())
    }
}
