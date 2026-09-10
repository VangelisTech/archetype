//! Test-only barrier: two real SQL catalogs stage metadata before either CAS.
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, InputFile, LocalFsStorage, OutputFile, Storage,
    StorageConfig, StorageFactory,
};
use iceberg::{Error, ErrorKind, Result};
use serde::{Deserialize, Serialize};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio::sync::Barrier;

#[derive(Debug)]
struct Gate {
    armed: AtomicBool,
    arrivals: AtomicUsize,
    barrier: Barrier,
}
impl Default for Gate {
    fn default() -> Self {
        Self {
            armed: AtomicBool::new(false),
            arrivals: AtomicUsize::new(0),
            barrier: Barrier::new(2),
        }
    }
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GateFactory {
    #[serde(skip)]
    gate: Arc<Gate>,
}
impl GateFactory {
    pub fn arm(&self) {
        self.gate.arrivals.store(0, Ordering::SeqCst);
        self.gate.armed.store(true, Ordering::SeqCst);
    }
    pub fn arrivals(&self) -> usize {
        self.gate.arrivals.load(Ordering::SeqCst)
    }
}
#[typetag::serde]
impl StorageFactory for GateFactory {
    fn build(&self, _config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        Ok(Arc::new(GatedStorage {
            gate: self.gate.clone(),
            inner: LocalFsStorage::new(),
        }))
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct GatedStorage {
    #[serde(skip)]
    gate: Arc<Gate>,
    inner: LocalFsStorage,
}
#[async_trait]
#[typetag::serde]
impl Storage for GatedStorage {
    async fn exists(&self, p: &str) -> Result<bool> {
        self.inner.exists(p).await
    }
    async fn metadata(&self, p: &str) -> Result<FileMetadata> {
        self.inner.metadata(p).await
    }
    async fn read(&self, p: &str) -> Result<Bytes> {
        self.inner.read(p).await
    }
    async fn reader(&self, p: &str) -> Result<Box<dyn FileRead>> {
        self.inner.reader(p).await
    }
    async fn writer(&self, p: &str) -> Result<Box<dyn FileWrite>> {
        self.inner.writer(p).await
    }
    async fn delete(&self, p: &str) -> Result<()> {
        self.inner.delete(p).await
    }
    async fn delete_prefix(&self, p: &str) -> Result<()> {
        self.inner.delete_prefix(p).await
    }
    async fn delete_stream(&self, paths: BoxStream<'static, String>) -> Result<()> {
        self.inner.delete_stream(paths).await
    }
    fn new_input(&self, p: &str) -> Result<InputFile> {
        Ok(InputFile::new(Arc::new(self.clone()), p.into()))
    }
    fn new_output(&self, p: &str) -> Result<OutputFile> {
        Ok(OutputFile::new(Arc::new(self.clone()), p.into()))
    }
    async fn write(&self, p: &str, bytes: Bytes) -> Result<()> {
        self.inner.write(p, bytes).await?;
        if p.ends_with(".metadata.json") && self.gate.armed.load(Ordering::SeqCst) {
            let ticket = self.gate.arrivals.fetch_add(1, Ordering::SeqCst);
            if ticket < 2 {
                if ticket == 1 {
                    self.gate.armed.store(false, Ordering::SeqCst);
                }
                tokio::time::timeout(Duration::from_secs(15), self.gate.barrier.wait())
                    .await
                    .map_err(|_| {
                        Error::new(
                            ErrorKind::Unexpected,
                            "CAS fixture metadata barrier timed out",
                        )
                    })?;
            }
        }
        Ok(())
    }
}
