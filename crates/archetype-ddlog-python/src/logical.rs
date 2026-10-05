//! Typed calls into the existing registry, native creation catalog and CutStore.
//! No identity, phase, or execution inventory is retained by this adapter.
use super::*;
use ddlog_runtime::{
    instance::public_relations,
    registry::{LogicalProgram, LogicalProgramRequest, ProcessorDefinition, ProcessorRegistry},
    worlds::{CreationRequest, LogicalDestination, ResolvedCreation},
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProgramRef {
    pub resource: String,
    pub processor: ProcessorReference,
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Declarations {
    pub components: Vec<Component>,
    pub inputs: BTreeMap<String, Vec<String>>,
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CreationBinding {
    program_resource: Option<String>,
    declarations: Declarations,
}
#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Request {
    ProgramPublish {
        request: LogicalProgramRequest,
    },
    ProgramResolve {
        resource: String,
    },
    ProgramDescribe {
        resource: String,
    },
    ProgramList {
        limit: usize,
        after: Option<String>,
        include_archived: bool,
    },
    WorldCreate {
        destination: LogicalDestination,
        request_key: String,
        label: String,
        program: ProgramRef,
        declarations: Declarations,
    },
    WorldResolve {
        destination: LogicalDestination,
    },
    WorldFork {
        source_binding: Binding,
        receipt: ReceiptRef,
        destination: LogicalDestination,
        request_key: String,
        label: String,
        inputs: BTreeMap<String, Vec<String>>,
        expected_generation: u64,
    },
}
impl Request {
    pub fn uses_store(&self) -> bool {
        matches!(
            self,
            Self::WorldCreate { .. } | Self::WorldResolve { .. } | Self::WorldFork { .. }
        )
    }
}
fn selected(registry: &ProcessorRegistry, resource: &str) -> Result<LogicalProgram> {
    let program = native(registry.resolve_logical_program(resource))?;
    ensure!(
        program.phase == "published",
        "Logical program publication is pending"
    );
    Ok(program)
}
impl Resources {
    pub(super) fn logical(&self, store: &CutStore, request: Request) -> Result<Value> {
        match request {
            Request::ProgramPublish { request } => {
                let registry = native(self.manager()?.registry())?;
                Ok(serde_json::to_value(native(
                    registry.publish_logical_program(request),
                )?)?)
            }
            Request::ProgramResolve { resource } => {
                let registry = native(self.manager()?.registry())?;
                Ok(serde_json::to_value(native(
                    registry.resolve_logical_program(&resource),
                )?)?)
            }
            Request::ProgramDescribe { resource } => {
                let registry = native(self.manager()?.registry())?;
                let program = selected(&registry, &resource)?;
                let record = native(registry.get(
                    &program.processor.processor_id,
                    Some(&program.processor.version),
                ))?;
                let relations = native(public_relations(&record))?;
                ensure!(
                    relations.len() <= 128 && relations.iter().all(|r| r.fields.len() <= 64),
                    "Program description exceeds relation/field limit"
                );
                Ok(
                    json!({"program":program,"kind":if matches!(record.definition, ProcessorDefinition::Composition(_)) {"composition"} else {"program"},"relations":relations}),
                )
            }
            Request::ProgramList {
                limit,
                after,
                include_archived,
            } => {
                let registry = native(self.manager()?.registry())?;
                Ok(serde_json::to_value(native(registry.list(
                    limit,
                    after.as_deref(),
                    include_archived,
                ))?)?)
            }
            Request::WorldCreate {
                destination,
                request_key,
                label,
                program,
                declarations,
            } => {
                let registry = native(self.manager()?.registry())?;
                let retained = selected(&registry, &program.resource)?;
                ensure!(
                    retained.processor == program.processor,
                    "Program reference does not belong to the logical resource"
                );
                let definition = WorldDefinition {
                    label,
                    processor: program.processor,
                    external_publication: Some(publication_policy(&declarations.components)),
                    purpose: "instance".into(),
                    scenarios: vec![],
                };
                let prepared = HostedCutAdapter::preflight(
                    &registry,
                    definition.clone(),
                    declarations.components.clone(),
                )?;
                prepared.validate_inputs(&declarations.inputs)?;
                let binding = CreationBinding {
                    program_resource: Some(program.resource),
                    declarations,
                };
                if native(self.manager()?.lookup_creation(&destination))?.is_none() {
                    self.runtime.block_on(
                        store.preflight_unclaimed_scope(&destination.world, &destination.run),
                    )?;
                }
                let reservation = native(self.manager()?.reserve_creation(CreationRequest {
                    destination: destination.clone(),
                    request_key,
                    definition,
                    binding: serde_json::to_value(&binding)?,
                    fork: None,
                }))?;
                let adapter = prepared.bind(HostedScope {
                    native_world: reservation.world_id.clone(),
                    world: destination.world.clone(),
                    run: destination.run.clone(),
                })?;
                let context = self
                    .runtime
                    .block_on(store.publish_context(&adapter.context_draft()?))?;
                ensure!(
                    self.runtime.block_on(store.context(&context.reference()))? == context,
                    "Creation context readback differs from publication"
                );
                native(
                    self.manager()?
                        .confirm_creation_context(reservation, context.context_id),
                )?;
                self.resolve_logical_world(store, destination)
            }
            Request::WorldResolve { destination } => self.resolve_logical_world(store, destination),
            Request::WorldFork {
                source_binding,
                receipt,
                destination,
                request_key,
                label,
                inputs,
                expected_generation,
            } => {
                let source = self
                    .runtime
                    .block_on(
                        store.history(&source_binding.scope.world, &source_binding.scope.run),
                    )?
                    .into_iter()
                    .find(|r| {
                        r.world == receipt.world
                            && r.run == receipt.run
                            && r.tick == receipt.tick
                            && r.cut_id == receipt.cut_id
                    })
                    .ok_or_else(|| anyhow!("Cut is outside bound source lineage"))?;
                let components = source_binding.components.clone();
                let adapter = self.bind(source_binding)?;
                adapter.validate_inputs(&inputs)?;
                let binding = CreationBinding {
                    program_resource: None,
                    declarations: Declarations { components, inputs },
                };
                let mut prepared = self.runtime.block_on(adapter.prepare_fork_source(
                    store,
                    &source,
                    request_key,
                    Scope {
                        world: destination.world.clone(),
                        run: destination.run.clone(),
                    },
                    label,
                ))?;
                if native(self.manager()?.lookup_creation(&destination))?.is_none() {
                    self.runtime
                        .block_on(prepared.check_new_destination(store))?;
                }
                let (creation, reserved) = prepared.reserve_logical(
                    &mut *self.manager()?,
                    destination.clone(),
                    serde_json::to_value(binding)?,
                )?;
                self.runtime
                    .block_on(reserved.check_logical_context(store))?;
                let context = self
                    .runtime
                    .block_on(store.publish_context(&reserved.adapter().context_draft()?))?;
                ensure!(
                    self.runtime.block_on(store.context(&context.reference()))? == context,
                    "Logical fork context readback mismatch"
                );
                native(
                    self.manager()?
                        .confirm_creation_context(creation, context.context_id),
                )?;
                self.runtime.block_on(reserved.publish_origin(store))?;
                let id = &reserved.reservation().child_world_id;
                let status = native(self.manager()?.status(id))?;
                // Creation retry reconciles this same fork; only its explicit
                // generation fence can request the initial retained restore.
                if status["external_publication"]["fork"]["ready"] != true {
                    let status = reserved.restore(&mut *self.manager()?, expected_generation)?;
                    if status["state"] == "running" {
                        let confirmation = self
                            .runtime
                            .block_on(reserved.prepare_confirmation(store))?;
                        confirmation.confirm(&mut *self.manager()?)?;
                    }
                }
                self.resolve_logical_world(store, destination)
            }
        }
    }
    fn resolve_logical_world(
        &self,
        store: &CutStore,
        destination: LogicalDestination,
    ) -> Result<Value> {
        let resolved = native(self.manager()?.resolve_creation(&destination))?;
        self.logical_world_value(store, resolved)
    }
    fn logical_world_value(&self, store: &CutStore, resolved: ResolvedCreation) -> Result<Value> {
        let binding: CreationBinding = serde_json::from_value(resolved.binding.clone())?;
        let scope = HostedScope {
            native_world: resolved.reservation.world_id.clone(),
            world: resolved.reservation.destination.world.clone(),
            run: resolved.reservation.destination.run.clone(),
        };
        let registry = native(self.manager()?.registry())?;
        let prepared = HostedCutAdapter::preflight(
            &registry,
            resolved.definition.clone(),
            binding.declarations.components.clone(),
        )?;
        prepared.validate_inputs(&binding.declarations.inputs)?;
        let adapter = prepared.bind(scope.clone())?;
        let context = self
            .runtime
            .block_on(store.lookup_context(&scope.world, &scope.run))?;
        if let Some(context) = &context {
            ensure!(
                context == adapter.context_draft()?.context(),
                "Logical world context differs from retained declarations"
            );
        }
        if let Some(id) = &resolved.context_id {
            ensure!(
                context
                    .as_ref()
                    .is_some_and(|context| context.context_id == *id),
                "Native context acknowledgment has lost its publication"
            );
        }
        let status = native(self.manager()?.status(&scope.native_world))?;
        let origin = store.origin(&scope.world, &scope.run)?;
        if let Some(fork) = &resolved.fork {
            if let Some(origin) = &origin {
                ensure!(
                    &origin.reservation == fork,
                    "Logical fork origin differs from native reservation"
                );
                self.runtime.block_on(adapter.verify_origin(store))?;
            } else {
                ensure!(
                    status["external_publication"]["fork"]["ready"] == false,
                    "Confirmed logical fork has lost its origin"
                );
            }
        } else {
            ensure!(
                origin.is_none(),
                "Fresh logical world has acquired a fork origin"
            );
        }
        // Return only the request-bound analytical reference and immutable
        // lineage proof, not the complete source cut's storage metadata.
        let origin = origin.map(|origin| {
            json!({"reservation":origin.reservation,"lineage_sha256":origin.lineage_sha256,
                "source":{"world":origin.source.world,"run":origin.source.run,
                    "tick":origin.source.tick,"cut_id":origin.source.cut_id}})
        });
        Ok(
            json!({"creation":resolved,"context":context,"status":status,"origin":origin,
            "program_resource":binding.program_resource,"inputs":binding.declarations.inputs,
            "binding":{"scope":scope,"components":binding.declarations.components}}),
        )
    }
}
