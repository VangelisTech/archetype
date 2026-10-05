use std::collections::BTreeMap;

use crate::{
    component::{Component, ComponentSchema},
    store::{CutStore, PublicationFault},
    world::{FrozenCut, RelationState},
};
use anyhow::Result;
use ddlog_runtime::Schema;
use serde_json::json;

fn schema(name: &str) -> ComponentSchema {
    ComponentSchema::new(
        Component {
            name: name.into(),
            output: name.into(),
            fields: vec!["entity_id".into(), "value".into()],
            entity_field: 0,
        },
        &Schema {
            input: false,
            fields: vec!["int".into(), "string".into()],
        },
    )
    .unwrap()
}

// Storage tests intentionally supply a synthetic opaque checkpoint. Native
// checkpoint validity/restore are independently tested by native_cut.rs.
pub(crate) fn cut() -> FrozenCut {
    FrozenCut {
        hosted: None,
        world: "demo".into(),
        run: "run_a".into(),
        tick: 1,
        program: "fixture".into(),
        ddlog_revision: 2,
        parent: None,
        relations: BTreeMap::from([
            (
                "label".into(),
                RelationState {
                    schema: schema("label"),
                    rows: vec![vec![json!(1), json!("one")]],
                },
            ),
            (
                "status".into(),
                RelationState {
                    schema: schema("status"),
                    rows: vec![vec![json!(1), json!("ready")]],
                },
            ),
        ]),
        checkpoint: checkpoint("demo", 1),
    }
}

fn checkpoint(world: &str, tick: u64) -> Vec<u8> {
    serde_json::to_vec(&json!({"state":{"revision":2,"metadata":{"abi":crate::ADAPTER_ABI,"program":"fixture","world":world,"run":"run_a","tick":tick}}})).unwrap()
}

fn live_schema() -> ComponentSchema {
    ComponentSchema::new(
        Component {
            name: "live".into(),
            output: "live".into(),
            fields: vec!["entity_id".into(), "enabled".into(), "value".into()],
            entity_field: 0,
        },
        &Schema {
            input: false,
            fields: vec!["int".into(), "bool".into(), "double".into()],
        },
    )
    .unwrap()
}

#[test]
fn live_component_cells_are_exact_and_arrow_keeps_binary64_bits() -> Result<()> {
    use arrow_array::{BooleanArray, Float64Array};
    let s = live_schema();
    let values = [
        0.0,
        1.0,
        f64::from_bits(1.0f64.to_bits() + 1),
        f64::MAX,
        -f64::MAX,
        f64::from_bits(1),
        -f64::from_bits(1),
    ];
    let rows: Vec<_> = values
        .iter()
        .enumerate()
        .map(|(i, v)| vec![json!(i as i64), json!(i % 2 == 0), json!(v)])
        .collect();
    let batch = s.batch("cut", &rows)?;
    let booleans = batch
        .column(2)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    let doubles = batch
        .column(3)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    for (i, value) in values.iter().enumerate() {
        assert_eq!(booleans.value(i), i % 2 == 0);
        assert_eq!(doubles.value(i).to_bits(), value.to_bits());
    }
    for invalid in [
        json!(1),
        json!(true),
        json!(null),
        json!("1.0"),
        json!(-0.0),
    ] {
        assert!(
            s.validate_rows(&[vec![json!(1), json!(true), invalid]])
                .is_err()
        );
    }
    for invalid in [json!(1), json!(null), json!("true")] {
        assert!(
            s.validate_rows(&[vec![json!(1), invalid, json!(1.0)]])
                .is_err()
        );
    }
    assert_eq!(s.batch("empty", &[])?.num_rows(), 0);
    Ok(())
}

#[tokio::test]
async fn cold_store_retains_live_types_and_empty_cut_retraction() -> Result<()> {
    use arrow_array::{BooleanArray, Float64Array};
    let dir = tempfile::tempdir()?;
    let store = CutStore::open(dir.path()).await?;
    let mut first = cut();
    first.relations = BTreeMap::from([(
        "live".into(),
        RelationState {
            schema: live_schema(),
            rows: vec![
                vec![json!(1), json!(false), json!(f64::from_bits(1))],
                vec![json!(2), json!(true), json!(f64::MAX)],
            ],
        },
    )]);
    let receipt = store.publish(&first).await?;
    let mut second = first.clone();
    second.tick = 2;
    second.parent = Some(receipt.cut_id.clone());
    second.checkpoint = checkpoint("demo", 2);
    second.relations.get_mut("live").unwrap().rows.clear();
    let empty = store.publish(&second).await?;
    drop(store);
    let cold = CutStore::open(dir.path()).await?;
    let batches = cold.read(&receipt, "live").await?;
    let mut actual = BTreeMap::new();
    for batch in batches {
        let keys = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        let bools = batch
            .column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let doubles = batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            actual.insert(keys.value(i), (bools.value(i), doubles.value(i).to_bits()));
        }
    }
    assert_eq!(
        actual,
        BTreeMap::from([(1, (false, 1)), (2, (true, f64::MAX.to_bits()))])
    );
    assert_eq!(
        cold.read(&empty, "live")
            .await?
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>(),
        0
    );
    Ok(())
}

#[test]
fn typed_component_contract() -> Result<()> {
    let s = schema("label");
    assert!(s.validate_rows(&[vec![json!(1), json!(null)]]).is_err());
    assert!(
        s.validate_rows(&[vec![json!(1), json!("a")], vec![json!(1), json!("b")]])
            .is_err()
    );
    let batch = s.batch("cut", &[vec![json!(1), json!("a")]])?;
    assert_eq!(batch.schema().field(2).name(), "label__value");
    assert!(!batch.schema().field(2).is_nullable());
    let mut invalid = s.component.clone();
    invalid.fields[1] = "entity_id".into();
    assert!(
        ComponentSchema::new(
            invalid,
            &Schema {
                input: false,
                fields: s.types.clone()
            }
        )
        .is_err()
    );
    assert!(
        ComponentSchema::new(
            s.component.clone(),
            &Schema {
                input: true,
                fields: s.types.clone()
            }
        )
        .is_err()
    );
    assert!(
        ComponentSchema::new(
            s.component,
            &Schema {
                input: false,
                fields: vec!["int".into(), "float".into()]
            }
        )
        .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn partial_registration_is_invisible_and_restart_adopts_exact_cut() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store = CutStore::open(dir.path()).await?;
    assert!(CutStore::open(dir.path()).await.is_err());
    let frozen = cut();
    assert!(
        store
            .publish_with_fault(&frozen, PublicationFault::AfterComponents)
            .await
            .is_err()
    );
    assert!(store.history("demo", "run_a").await?.is_empty());
    drop(store);
    let store = CutStore::open(dir.path()).await?;
    let receipt = store.retry("demo", "run_a", 1).await?;
    assert_eq!(receipt, store.publish(&frozen).await?);
    assert_eq!(store.history("demo", "run_a").await?.len(), 1);
    assert_eq!(
        store
            .read(&receipt, "label")
            .await?
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>(),
        1
    );
    assert_eq!(store.checkpoint(&receipt).await?, frozen.checkpoint);
    Ok(())
}

#[tokio::test]
async fn lost_acknowledgement_does_not_duplicate_and_empty_cut_does_not_resurrect() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store = CutStore::open(dir.path()).await?;
    let first = cut();
    assert!(
        store
            .publish_with_fault(&first, PublicationFault::AfterManifest)
            .await
            .is_err()
    );
    let receipt = store.publish(&first).await?;
    assert_eq!(store.history("demo", "run_a").await?.len(), 1);
    let mut next = first.clone();
    next.tick = 2;
    next.checkpoint = checkpoint("demo", 2);
    next.parent = Some(receipt.cut_id.clone());
    next.relations.get_mut("label").unwrap().rows.clear();
    let second = store.publish(&next).await?;
    assert_eq!(
        store
            .read(&second, "label")
            .await?
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>(),
        0
    );
    assert_eq!(
        store
            .read(&receipt, "label")
            .await?
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>(),
        1
    );
    let mut conflicting = next;
    conflicting.relations.get_mut("status").unwrap().rows[0][1] = json!("different");
    assert!(store.publish(&conflicting).await.is_err());
    Ok(())
}

#[tokio::test]
async fn staged_intent_cannot_be_replaced_and_corruption_fails_closed() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store = CutStore::open(dir.path()).await?;
    let first = cut();
    assert!(
        store
            .publish_with_fault(&first, PublicationFault::AfterComponents)
            .await
            .is_err()
    );
    let mut conflicting = first.clone();
    conflicting.relations.get_mut("label").unwrap().rows[0][1] = json!("conflicting value");
    conflicting.validate()?;
    assert!(store.publish(&conflicting).await.is_err());
    let receipt = store.publish(&first).await?;
    std::fs::write(
        receipt.components["label"].object.as_ref().unwrap(),
        b"corrupt",
    )?;
    assert!(store.read(&receipt, "label").await.is_err());
    assert!(store.publish(&first).await.is_err());
    Ok(())
}

#[test]
fn malformed_journal_returns_error_without_panicking() {
    for bytes in [b"[]".as_slice(), b"{\"cut\":[]}", b"null", b"{\"cut\":{}}"] {
        assert!(FrozenCut::decode_journal(bytes).is_err());
    }
}

#[tokio::test]
async fn distinct_worlds_share_schema_but_not_visible_rows() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store = CutStore::open(dir.path()).await?;
    let left = cut();
    let mut right = cut();
    right.world = "other".into();
    right.checkpoint = checkpoint("other", 1);
    right.relations.get_mut("label").unwrap().rows[0][1] = json!("other value");
    let (a, b) = tokio::join!(store.publish(&left), store.publish(&right));
    let (a, b) = (a?, b?);
    assert_ne!(a.cut_id, b.cut_id);
    assert_eq!(a.components["label"].table, b.components["label"].table);
    assert_eq!(
        store
            .read(&a, "label")
            .await?
            .iter()
            .map(|x| x.num_rows())
            .sum::<usize>(),
        1
    );
    assert_eq!(
        store
            .read(&b, "label")
            .await?
            .iter()
            .map(|x| x.num_rows())
            .sum::<usize>(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn rejected_future_cut_cannot_reserve_a_legitimate_successors_journal() -> Result<()> {
    let root = tempfile::tempdir()?;
    let store = CutStore::open(root.path()).await?;
    let first = cut();
    let mut future = first.clone();
    future.tick = 2;
    future.parent = Some("unknown".into());
    future.checkpoint = checkpoint("demo", 2);
    assert!(store.publish(&future).await.is_err());
    assert!(!root.path().join("cuts/demo.run_a.2.json").exists());
    let parent = store.publish(&first).await?;
    future.parent = Some(parent.cut_id);
    assert_eq!(store.publish(&future).await?.tick, 2);
    Ok(())
}
