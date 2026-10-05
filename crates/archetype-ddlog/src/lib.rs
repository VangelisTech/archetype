//! ECS persistence and tick visibility over DDlog Runtime. No rule evaluation,
//! operator scheduling, or Daft execution belongs in this adapter.
pub mod component;
pub mod hosted;
pub mod store;
pub mod world;

#[cfg(test)]
mod store_tests;

use serde::Serialize;
use sha2::{Digest, Sha256};

pub const DDLOG_REVISION: &str = "3451df0ce968c6c2dc2a24265cf6434d108edccf";
pub const ADAPTER_ABI: &str = "archetype-ddlog-cut-v1";

pub(crate) fn digest(value: &impl Serialize) -> anyhow::Result<String> {
    Ok(hash(&serde_json::to_vec(value)?))
}

pub(crate) fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

pub(crate) fn identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value.bytes().next().is_some_and(|c| c.is_ascii_lowercase())
        && value
            .bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_')
}
