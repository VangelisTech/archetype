use anyhow::{Result, anyhow, ensure};
use archetype_ddlog::{
    component::Component,
    hosted::{HostedCutAdapter, HostedScope, publication_policy},
    world::FrozenCut,
};
use ddlog_runtime::{
    registry::{ProcessorDefinition, ProcessorReference},
    worlds::{AdmissionQuery, AdmitInputs, BoundaryKey, WorldDefinition, WorldManager},
};
use serde_json::{Value, json};
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    time::{Duration, Instant},
};

#[path = "../../examples/support/program.rs"]
mod program;

pub struct Fixture {
    pub root: tempfile::TempDir,
    driver: PathBuf,
    processor: ProcessorReference,
    pub components: Vec<Component>,
}
impl Fixture {
    pub fn new(native: bool) -> Result<Self> {
        let root = tempfile::tempdir()?;
        let (registry, composition, components) = program::program(root.path())?;
        let record = registry
            .create(
                serde_json::from_value::<ProcessorDefinition>(json!({"composition":composition}))?,
                None,
            )
            .map_err(|e| anyhow!(e))?;
        let driver = if native {
            std::env::var_os("ARCHETYPE_DDLOG_DRIVER")
                .ok_or_else(|| anyhow!("Set ARCHETYPE_DDLOG_DRIVER"))?
                .into()
        } else {
            let driver = root.path().join("build.py");
            let source = include_str!("../fixtures/hosted_native.py")
                .replace("__CONTROL__", &serde_json::to_string(root.path())?);
            fs::write(
                &driver,
                format!(
                    "#!/usr/bin/env python3\nimport sys\nfrom pathlib import Path\nPath(sys.argv[2]).write_text({})\nPath(sys.argv[2]).chmod(0o700)\n",
                    json!(source)
                ),
            )?;
            fs::set_permissions(&driver, fs::Permissions::from_mode(0o700))?;
            driver
        };
        Ok(Self {
            root,
            driver,
            processor: ProcessorReference {
                processor_id: record.processor_id,
                version: record.version,
            },
            components,
        })
    }
    pub fn manager(&self) -> Result<WorldManager> {
        WorldManager::new(
            self.root.path().join("registry"),
            self.root.path().join("worlds"),
            self.driver.clone(),
        )
        .map_err(|e| anyhow!(e))
    }
    pub fn create(&self, m: &mut WorldManager, name: &str) -> Result<(String, HostedCutAdapter)> {
        let id = m
            .create(WorldDefinition {
                label: name.into(),
                processor: self.processor.clone(),
                external_publication: Some(publication_policy(&self.components)),
                purpose: "instance".into(),
                scenarios: vec![],
            })
            .map_err(|e| anyhow!(e))?;
        Ok((id.clone(), self.bind(m, &id, name)?))
    }
    pub fn bind(&self, m: &mut WorldManager, id: &str, name: &str) -> Result<HostedCutAdapter> {
        HostedCutAdapter::bind(
            m,
            HostedScope {
                native_world: id.into(),
                world: name.into(),
                run: "run_a".into(),
            },
            self.components.clone(),
        )
    }
    pub fn commits(&self) -> usize {
        fs::read_to_string(self.root.path().join("commands"))
            .unwrap_or_default()
            .lines()
            .filter(|s| s.starts_with("commit"))
            .count()
    }
}
pub fn request(
    id: &str,
    generation: u64,
    revision: u64,
    name: &str,
    value: &str,
    delete: bool,
) -> AdmitInputs {
    serde_json::from_value(json!({"id":id,"expected_generation":generation,"expected_revision":revision,
        "admission_key":name,"changes":[{"op":if delete {"delete"} else {"insert"},"predicate":"seed","values":[1,value]}]})).unwrap()
}
pub fn key(value: &Value) -> Result<BoundaryKey> {
    Ok(serde_json::from_value(value["boundary"]["key"].clone())?)
}
pub fn lookup(m: &mut WorldManager, key: &BoundaryKey) -> Result<Value> {
    m.admission_status(AdmissionQuery {
        id: key.world_id.clone(),
        generation: key.generation,
        admission_key: key.admission_key.clone(),
    })
    .map_err(|e| anyhow!(e))
}
pub async fn finished(m: &mut WorldManager, key: &BoundaryKey) -> Result<Value> {
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let value = lookup(m, key)?;
        if value["state"] != "pending" {
            return Ok(value);
        }
        ensure!(Instant::now() < deadline, "Admission timeout: {value}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}
pub async fn running(m: &mut WorldManager, id: &str) -> Result<Value> {
    let deadline = Instant::now() + Duration::from_secs(150);
    loop {
        let value = m.status(id).map_err(|e| anyhow!(e))?;
        if value["state"] != "starting" {
            ensure!(value["state"] == "running", "Activation failed: {value}");
            return Ok(value);
        }
        ensure!(Instant::now() < deadline, "Activation timeout");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
pub async fn capture(
    adapter: &HostedCutAdapter,
    m: &mut WorldManager,
    key: &BoundaryKey,
) -> Result<FrozenCut> {
    let state = finished(m, key).await?;
    ensure!(
        matches!(state["state"].as_str(), Some("frozen" | "published")),
        "Not frozen: {state}"
    );
    let mut pages = adapter.capture(m, key)?;
    while !pages.advance(m)? {
        tokio::task::yield_now().await;
    }
    pages.finish()
}
