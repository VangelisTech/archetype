use super::bounds::{Budget, corrupt, local_path};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use iceberg::{
    Error, ErrorKind, Result,
    io::{
        FileMetadata, FileRead, FileWrite, InputFile, LocalFsStorage, OutputFile, Storage,
        StorageConfig, StorageFactory,
    },
};
use std::{
    ops::Range,
    path::{Path, PathBuf},
    sync::Arc,
};

#[derive(Debug, Clone)]
pub(super) struct BoundedIo {
    pub root: PathBuf,
    pub budget: Arc<Budget>,
}

// Budgets are operation-local authority. Serialization must not reset counters
// or hand an unbounded FileIO to background/distributed work.
impl serde::Serialize for BoundedIo {
    fn serialize<S: serde::Serializer>(&self, _: S) -> std::result::Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom(
            "Bounded local FileIO is not portable",
        ))
    }
}
impl<'de> serde::Deserialize<'de> for BoundedIo {
    fn deserialize<D: serde::Deserializer<'de>>(_: D) -> std::result::Result<Self, D::Error> {
        Err(serde::de::Error::custom(
            "Bounded local FileIO is not portable",
        ))
    }
}
fn io_error(e: anyhow::Error) -> Error {
    Error::new(ErrorKind::DataInvalid, "Bounded local storage read failed").with_source(e)
}
impl BoundedIo {
    fn path(&self, path: &str, existing: bool) -> Result<PathBuf> {
        let p = local_path(path).map_err(io_error)?;
        corrupt(
            p.starts_with(&self.root)
                && !p
                    .components()
                    .any(|p| matches!(p, std::path::Component::ParentDir)),
            "Catalog path outside store",
        )
        .map_err(io_error)?;
        if existing {
            corrupt(
                p.canonicalize()
                    .map_err(anyhow::Error::from)
                    .map_err(io_error)?
                    .starts_with(&self.root),
                "Catalog symlink outside store",
            )
            .map_err(io_error)?;
        }
        Ok(p)
    }
    fn load(&self, path: &Path) -> Result<Bytes> {
        let limit = if path.extension().is_some_and(|s| s == "parquet") {
            self.budget.limits.file_bytes
        } else {
            self.budget.limits.metadata_bytes
        };
        let bytes = self.budget.read(path, limit).map_err(io_error)?;
        super::preflight::file(bytes.clone(), path, &self.budget).map_err(io_error)?;
        Ok(bytes)
    }
}
#[typetag::serde(name = "archetype_bounded_local_v1")]
impl StorageFactory for BoundedIo {
    fn build(&self, _: &StorageConfig) -> Result<Arc<dyn Storage>> {
        Ok(Arc::new(self.clone()))
    }
}
#[async_trait]
#[typetag::serde(name = "archetype_bounded_local_v1")]
impl Storage for BoundedIo {
    async fn exists(&self, path: &str) -> Result<bool> {
        self.path(path, false)?;
        LocalFsStorage.exists(path).await
    }
    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let p = self.path(path, true)?;
        let size = std::fs::metadata(p)
            .map_err(anyhow::Error::from)
            .map_err(io_error)?
            .len();
        super::bounds::cap(size, self.budget.limits.file_bytes, "file bytes").map_err(io_error)?;
        Ok(FileMetadata { size })
    }
    async fn read(&self, path: &str) -> Result<Bytes> {
        self.load(&self.path(path, true)?)
    }
    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        Ok(Box::new(BoundedRead {
            bytes: self.load(&self.path(path, true)?)?,
            budget: self.budget.clone(),
        }))
    }
    async fn write(&self, path: &str, bytes: Bytes) -> Result<()> {
        self.path(path, false)?;
        LocalFsStorage.write(path, bytes).await
    }
    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        self.path(path, false)?;
        LocalFsStorage.writer(path).await
    }
    async fn delete(&self, path: &str) -> Result<()> {
        self.path(path, false)?;
        LocalFsStorage.delete(path).await
    }
    async fn delete_prefix(&self, path: &str) -> Result<()> {
        self.path(path, false)?;
        LocalFsStorage.delete_prefix(path).await
    }
    async fn delete_stream(&self, paths: BoxStream<'static, String>) -> Result<()> {
        use futures::StreamExt;
        use futures::TryStreamExt;
        paths
            .map(Ok)
            .try_for_each(|p| async move { self.delete(&p).await })
            .await
    }
    fn new_input(&self, path: &str) -> Result<InputFile> {
        self.path(path, false)?;
        Ok(InputFile::new(Arc::new(self.clone()), path.into()))
    }
    fn new_output(&self, path: &str) -> Result<OutputFile> {
        self.path(path, false)?;
        Ok(OutputFile::new(Arc::new(self.clone()), path.into()))
    }
}
struct BoundedRead {
    bytes: Bytes,
    budget: Arc<Budget>,
}
#[async_trait]
impl FileRead for BoundedRead {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        corrupt(
            range.start <= range.end && range.end <= self.bytes.len() as u64,
            "Invalid bounded file range",
        )
        .map_err(io_error)?;
        self.budget
            .bytes(range.end - range.start)
            .map_err(io_error)?;
        Ok(self.bytes.slice(range.start as usize..range.end as usize))
    }
}
