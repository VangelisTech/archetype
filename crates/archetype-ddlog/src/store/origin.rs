//! Immutable analytical inheritance. An origin is not a child cut: source
//! receipts, snapshots and native checkpoint provenance retain their owners.
use super::*;
use ddlog_runtime::worlds::{ExternalReceipt, ForkReservation};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scope {
    pub world: String,
    pub run: String,
}
impl Scope {
    pub fn validate(&self) -> Result<()> {
        bounds::request(
            crate::identifier(&self.world) && crate::identifier(&self.run),
            "Invalid fork scope",
        )
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForkOrigin {
    pub version: u32,
    pub reservation: ForkReservation,
    pub source: CutReceipt,
    pub external: ExternalReceipt,
    pub lineage_sha256: String,
}
impl ForkOrigin {
    pub fn destination(&self) -> Result<Scope> {
        Ok(serde_json::from_value(
            self.reservation.destination.clone(),
        )?)
    }
    pub fn source_scope(&self) -> Result<Scope> {
        Ok(serde_json::from_value(
            self.reservation.source_context.clone(),
        )?)
    }
}
impl CutStore {
    /// Called only with a native-reserved fork and its verified complete draft.
    /// Unlike legacy admission, this permits context publication before origin.
    pub(crate) async fn check_logical_fork_context(
        &self,
        origin: &ForkOrigin,
        draft: &super::contexts::ContextDraft,
    ) -> Result<()> {
        self.validate_origin(origin)?;
        let dest = origin.destination()?;
        let source = origin.source_scope()?;
        self.check_new_origin_depth(&source)?;
        ensure!(
            draft.context().world == dest.world && draft.context().run == dest.run,
            "Logical fork context scope mismatch"
        );
        if let Some(context) = self.context_claim(&dest.world, &dest.run).await? {
            ensure!(
                &context == draft.context(),
                "Logical fork context differs from retained reservation"
            );
        }
        if let Some(existing) = self.origin(&dest.world, &dest.run)? {
            ensure!(
                &existing == origin,
                "Logical fork origin differs from reservation"
            );
        } else {
            ensure!(
                self.history_inner(&dest.world, &dest.run).await?.is_empty(),
                "Fork destination already has cuts"
            );
            let prefix = format!("{}.{}.", dest.world, dest.run);
            for entry in fs::read_dir(self.root.join("cuts"))? {
                self.budget.items(1)?;
                ensure!(
                    !entry?.file_name().to_string_lossy().starts_with(&prefix),
                    "Fork destination has pending publication"
                );
            }
        }
        Ok(())
    }
    fn check_new_origin_depth(&self, source: &Scope) -> Result<()> {
        let mut scope = source.clone();
        let mut depth = 1u64;
        let mut visited = std::collections::BTreeSet::new();
        loop {
            bounds::cap(depth, 32, "Fork ancestry depth")?;
            ensure!(
                visited.insert((scope.world.clone(), scope.run.clone())),
                "Cyclic fork lineage"
            );
            let Some(origin) = self.origin(&scope.world, &scope.run)? else {
                return Ok(());
            };
            scope = origin.source_scope()?;
            depth += 1;
        }
    }
    pub(crate) async fn check_fork_destination(
        &self,
        dest: &Scope,
        request_key: &str,
        source: &CutReceipt,
        source_scope: &Scope,
    ) -> Result<()> {
        if let Some(context) = self.context_claim(&dest.world, &dest.run).await? {
            ensure!(
                matches!(
                    context.origin,
                    super::contexts::ContextOrigin::Hosted { .. }
                ),
                "Artifact collection cannot become a fork destination"
            );
            let existing = self.origin(&dest.world, &dest.run)?.ok_or_else(|| {
                anyhow!("Hosted context already owns the destination before fork reservation")
            })?;
            self.check_context_origin(&existing).await?;
        }
        self.check_new_origin_depth(source_scope)?;
        if let Some(existing) = self.origin(&dest.world, &dest.run)? {
            ensure!(
                existing.reservation.request_key == request_key
                    && existing.source == *source
                    && existing.source_scope()? == *source_scope,
                "Fork destination already binds different contents"
            );
        } else {
            ensure!(
                self.history_inner(&dest.world, &dest.run).await?.is_empty(),
                "Fork destination already has cuts"
            );
            let prefix = format!("{}.{}.", dest.world, dest.run);
            for entry in fs::read_dir(self.root.join("cuts"))? {
                self.budget.items(1)?;
                ensure!(
                    !entry?.file_name().to_string_lossy().starts_with(&prefix),
                    "Fork destination has a pending publication"
                );
            }
        }
        Ok(())
    }
    fn origin_path(&self, world: &str, run: &str) -> Result<PathBuf> {
        Scope {
            world: world.into(),
            run: run.into(),
        }
        .validate()?;
        Ok(self
            .root
            .join("origins")
            .join(format!("{world}.{run}.json")))
    }
    /// Bounded immutable metadata; selected data still follows normal verified
    /// receipt reads. No parent manager or live registry is required.
    pub fn origin(&self, world: &str, run: &str) -> Result<Option<ForkOrigin>> {
        let path = self.origin_path(world, run)?;
        if !path.try_exists()? {
            return Ok(None);
        }
        let origin: ForkOrigin = serde_json::from_slice(&self.budget.read_metadata(&path)?)?;
        self.validate_origin(&origin)?;
        ensure!(
            origin.destination()?
                == (Scope {
                    world: world.into(),
                    run: run.into()
                }),
            "Origin scope mismatch"
        );
        Ok(Some(origin))
    }
    fn validate_origin(&self, origin: &ForkOrigin) -> Result<()> {
        let dest = origin.destination()?;
        let source = origin.source_scope()?;
        dest.validate()?;
        source.validate()?;
        ensure!(
            origin.version == 1
                && dest != source
                && origin.lineage_sha256
                    == origin
                        .reservation
                        .lineage_sha256()
                        .map_err(|e| anyhow!(e))?
                && origin.reservation.source_receipt_sha256 == origin.external.receipt_sha256
                && origin.reservation.source_manifest_sha256
                    == origin.external.frozen_manifest_sha256,
            "Origin reservation mismatch"
        );
        let cut = self.load_frozen(&origin.source.world, &origin.source.run, origin.source.tick)?;
        ensure!(
            crate::hosted::external_receipt(&cut, &origin.source)? == origin.external,
            "Origin source evidence changed"
        );
        ensure!(
            cut.hosted
                .as_ref()
                .is_some_and(|m| m.key.world_id != origin.reservation.child_world_id),
            "Origin must have a distinct native child"
        );
        Ok(())
    }
    /// The hosted bridge is the only producer; it proves native reservation and
    /// compatible source before presenting this record. fsync is read authority.
    pub(crate) async fn publish_origin(&self, origin: &ForkOrigin) -> Result<()> {
        let _guard = self.publication.lock().await;
        bounds::page(origin, self.budget.limits.metadata_bytes)?;
        self.validate_origin(origin)?;
        self.check_context_origin(origin).await?;
        let dest = origin.destination()?;
        let source = origin.source_scope()?;
        self.check_new_origin_depth(&source)?;
        let history = self.history_inner(&source.world, &source.run).await?;
        ensure!(
            history.contains(&origin.source),
            "Source cut is outside requested lineage"
        );
        self.verify_cut(&origin.source).await?;
        if let Some(existing) = self.origin(&dest.world, &dest.run)? {
            ensure!(
                &existing == origin,
                "Fork origin already binds different contents"
            );
        } else {
            ensure!(
                self.history_inner(&dest.world, &dest.run).await?.is_empty(),
                "Fork destination already has cuts"
            );
            // Any retained journal claims a destination, even before visibility.
            let prefix = format!("{}.{}.", dest.world, dest.run);
            for (count, entry) in fs::read_dir(self.root.join("cuts"))?.enumerate() {
                self.budget.items(1)?;
                bounds::cap(count as u64, self.budget.limits.items, "Journal inventory")?;
                ensure!(
                    !entry?.file_name().to_string_lossy().starts_with(&prefix),
                    "Fork destination has a pending publication"
                );
            }
        }
        let bytes = bounds::encode_metadata(origin, self.budget.limits.metadata_bytes)?;
        immutable(&self.origin_path(&dest.world, &dest.run)?, &bytes)?;
        ensure!(
            self.origin(&dest.world, &dest.run)?.as_ref() == Some(origin),
            "Origin readback mismatch"
        );
        Ok(())
    }
    pub(super) fn resolve_history(
        &self,
        world: &str,
        run: &str,
        receipts: Vec<CutReceipt>,
    ) -> Result<Vec<CutReceipt>> {
        let mut scope = Scope {
            world: world.into(),
            run: run.into(),
        };
        let mut chain = Vec::new();
        let mut visited = std::collections::BTreeSet::new();
        loop {
            bounds::cap(chain.len() as u64, 32, "Fork ancestry depth")?;
            ensure!(
                visited.insert((scope.world.clone(), scope.run.clone())),
                "Cyclic fork lineage"
            );
            let Some(origin) = self.origin(&scope.world, &scope.run)? else {
                break;
            };
            scope = origin.source_scope()?;
            chain.push(origin);
        }
        let mut resolved = Vec::new();
        loop {
            let mut own: Vec<_> = receipts
                .iter()
                .filter(|r| r.world == scope.world && r.run == scope.run)
                .cloned()
                .collect();
            own.sort_by_key(|r| r.tick);
            for r in own {
                ensure!(
                    Some(r.tick)
                        == resolved
                            .last()
                            .map_or(Some(1), |p: &CutReceipt| p.tick.checked_add(1))
                        && r.parent == resolved.last().map(|p| p.cut_id.clone()),
                    "Broken or duplicate cut history"
                );
                resolved.push(r);
            }
            let Some(origin) = chain.pop() else {
                break;
            };
            ensure!(
                resolved.iter().any(|r| r == &origin.source),
                "Origin source cut is not visible in its lineage"
            );
            resolved.retain(|r| r.tick <= origin.source.tick);
            scope = origin.destination()?;
        }
        Ok(resolved)
    }
}
