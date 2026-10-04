use anyhow::{Result, anyhow};
use archetype_ddlog::component::Component;
use ddlog_runtime::{
    composition::CompositionManifest,
    registry::{ProcessorDefinition, ProcessorRegistry},
};
use serde_json::json;
use std::path::Path;

pub fn program(root: &Path) -> Result<(ProcessorRegistry, CompositionManifest, Vec<Component>)> {
    let registry = ProcessorRegistry::open(root.join("registry")).map_err(|e| anyhow!(e))?;
    let first: ProcessorDefinition = serde_json::from_value(json!({
        "rules":"label(E,N) :- seed(E,N).",
        "schemas":{"seed":{"input":true,"fields":["int","string"]},"label":{"input":false,"fields":["int","string"]}},
        "interface":{"inputs":["seed"],"outputs":["label"]}
    }))?;
    let second: ProcessorDefinition = serde_json::from_value(json!({
        "rules":"status(E,\"ready\") :- label(E,N).",
        "schemas":{"label":{"input":true,"fields":["int","string"]},"status":{"input":false,"fields":["int","string"]}},
        "interface":{"inputs":["label"],"outputs":["status"]}
    }))?;
    let a = registry.create(first, None).map_err(|e| anyhow!(e))?;
    let b = registry.create(second, None).map_err(|e| anyhow!(e))?;
    let manifest = serde_json::from_value(json!({
        "nodes":{"labels":{"processor_id":a.processor_id,"version":a.version},"statuses":{"processor_id":b.processor_id,"version":b.version}},
        "inputs":{"seed":{"fields":["int","string"],"targets":[{"node":"labels","relation":"seed"}]}},
        "bindings":[{"from":{"node":"labels","relation":"label"},"to":{"node":"statuses","relation":"label"}}],
        "outputs":{"labels":{"node":"labels","relation":"label"},"statuses":{"node":"statuses","relation":"status"}}
    }))?;
    let components = vec![
        Component {
            name: "label".into(),
            output: "labels".into(),
            fields: vec!["entity_id".into(), "name".into()],
            entity_field: 0,
        },
        Component {
            name: "status".into(),
            output: "statuses".into(),
            fields: vec!["entity_id".into(), "state".into()],
            entity_field: 0,
        },
    ];
    Ok((registry, manifest, components))
}
