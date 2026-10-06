//! Bounded S3 data access behind one native CutStore; no remote control claim.
mod transport;
use super::{
    bounds::{self, Budget},
    config::RemoteData,
};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use bytes::Bytes;
use opendal::{HttpTransporter, OperationContext, Operator, services::S3};
use std::{ops::Range, sync::Arc, time::Duration};

const DEADLINE: Duration = Duration::from_secs(30);
#[derive(Clone)]
pub(super) struct RemoteBackend {
    pub profile: RemoteData,
    provider: Arc<dyn DataProvider>,
}
impl std::fmt::Debug for RemoteBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteBackend")
            .field("profile", &self.profile)
            .finish_non_exhaustive()
    }
}
impl RemoteBackend {
    #[cfg(test)]
    pub(super) fn with_provider(
        profile: RemoteData,
        provider: Arc<dyn DataProvider>,
    ) -> Result<Arc<Self>> {
        profile.validate()?;
        Ok(Arc::new(Self { profile, provider }))
    }
    pub fn new(profile: RemoteData) -> Result<Arc<Self>> {
        profile.validate()?;
        // Snapshot only this explicit credential route. No home/profile discovery,
        // anonymous mode, role creation, metadata service or public credential JSON.
        let required = |name| {
            std::env::var(name)
                .ok()
                .filter(|v| !v.is_empty())
                .ok_or_else(|| anyhow!("Configured environment credentials unavailable"))
        };
        let url = url::Url::parse(&profile.uri)?;
        let mut builder = S3::default()
            .bucket(url.host_str().unwrap())
            .root(url.path())
            .region(&profile.region)
            .access_key_id(&required("AWS_ACCESS_KEY_ID")?)
            .secret_access_key(&required("AWS_SECRET_ACCESS_KEY")?)
            .disable_config_load()
            .disable_ec2_metadata();
        if !profile.path_style_access {
            builder = builder.enable_virtual_host_style();
        }
        if let Some(endpoint) = &profile.endpoint {
            builder = builder.endpoint(endpoint);
        }
        if let Ok(token) = std::env::var("AWS_SESSION_TOKEN")
            && !token.is_empty()
        {
            builder = builder.session_token(&token);
        }
        let operator =
            Operator::new(builder).map_err(|_| anyhow!("Provider configuration unavailable"))?;
        let transport = transport::BoundedTransport::new()?;
        let operator = operator.with_context(
            OperationContext::new().with_http_transport(HttpTransporter::new(transport)),
        );
        Ok(Arc::new(Self {
            profile,
            provider: Arc::new(S3Provider(operator)),
        }))
    }
    fn key<'a>(&self, path: &'a str) -> Result<&'a str> {
        self.profile.object(path)?;
        path.strip_prefix(&(self.profile.uri.clone() + "/"))
            .ok_or_else(|| anyhow!("Invalid remote object namespace"))
    }
    pub async fn exists(&self, path: &str) -> Result<bool> {
        let key = self.key(path)?;
        tokio::time::timeout(DEADLINE, self.provider.exists(key))
            .await
            .map_err(|_| anyhow!("Provider operation deadline exceeded"))?
            .map_err(|_| anyhow!("Provider object lookup unavailable"))
    }
    pub async fn size(&self, path: &str) -> Result<u64> {
        let key = self.key(path)?;
        tokio::time::timeout(DEADLINE, self.provider.size(key))
            .await
            .map_err(|_| anyhow!("Provider operation deadline exceeded"))?
            .map_err(|_| anyhow!("Provider object metadata unavailable"))
    }
    pub async fn read(&self, path: &str, budget: &Budget, limit: u64) -> Result<Bytes> {
        let key = self.key(path)?;
        tokio::time::timeout(DEADLINE, async {
            let size = self
                .provider
                .size(key)
                .await
                .map_err(|_| anyhow!("Provider object metadata unavailable"))?;
            budget.file(size, limit)?;
            budget.bytes(size)?;
            // Metadata admission precedes allocation and every ranged provider read.
            let mut bytes = Vec::with_capacity(usize::try_from(size)?);
            let mut offset = 0;
            while offset < size {
                let end = size.min(offset.saturating_add(65536));
                let block = self
                    .provider
                    .range(key, offset..end)
                    .await
                    .map_err(|_| anyhow!("Provider bounded read unavailable"))?;
                bounds::corrupt(
                    block.len() as u64 == end - offset,
                    "Remote object changed length during read",
                )?;
                bytes.extend_from_slice(&block);
                offset = end;
            }
            let after = self
                .provider
                .size(key)
                .await
                .map_err(|_| anyhow!("Provider object metadata unavailable"))?;
            bounds::corrupt(
                after == size && bytes.len() as u64 == size,
                "Remote object changed length during read",
            )?;
            Ok(bytes.into())
        })
        .await
        .map_err(|_| anyhow!("Provider operation deadline exceeded"))?
    }
    pub async fn immutable(
        &self,
        path: &str,
        bytes: Bytes,
        budget: &Budget,
        limit: u64,
    ) -> Result<()> {
        self.profile.object(path)?;
        bounds::cap(bytes.len() as u64, limit, "Remote write bytes")?;
        // Conditional creation is issued even for an existing key. A lost ack
        // or precondition failure is adopted only by bounded exact readback.
        let key = self.key(path)?;
        let write = tokio::time::timeout(DEADLINE, self.provider.create(key, bytes.clone())).await;
        let readback = self.read(path, budget, limit).await;
        match readback {
            Ok(actual) => bounds::corrupt(actual == bytes, "Remote immutable write differs"),
            Err(error) => {
                if matches!(write, Ok(Ok(()))) {
                    Err(error)
                } else {
                    Err(anyhow!("Provider write outcome unknown"))
                }
            }
        }
    }
}

#[async_trait]
pub(super) trait DataProvider: Send + Sync {
    async fn exists(&self, key: &str) -> Result<bool>;
    async fn size(&self, key: &str) -> Result<u64>;
    async fn range(&self, key: &str, range: Range<u64>) -> Result<Bytes>;
    /// MUST conditionally create, never overwrite an existing key.
    async fn create(&self, key: &str, bytes: Bytes) -> Result<()>;
}
struct S3Provider(Operator);
#[async_trait]
impl DataProvider for S3Provider {
    async fn exists(&self, key: &str) -> Result<bool> {
        self.0
            .exists(key)
            .await
            .map_err(|_| anyhow!("Provider lookup unavailable"))
    }
    async fn size(&self, key: &str) -> Result<u64> {
        Ok(self
            .0
            .stat(key)
            .await
            .map_err(|_| anyhow!("Provider metadata unavailable"))?
            .content_length())
    }
    async fn range(&self, key: &str, range: Range<u64>) -> Result<Bytes> {
        Ok(self
            .0
            .read_with(key)
            .range(range)
            .await
            .map_err(|_| anyhow!("Provider range unavailable"))?
            .to_bytes())
    }
    async fn create(&self, key: &str, bytes: Bytes) -> Result<()> {
        self.0
            .write_with(key, bytes)
            .if_not_exists(true)
            .await
            .map(|_| ())
            .map_err(|_| anyhow!("Provider conditional create outcome unknown"))
    }
}

#[cfg(test)]
mod tests {
    use super::super::bounds::Limits;
    use super::*;
    use std::sync::Mutex;
    #[derive(Default)]
    struct Fake {
        bytes: Mutex<Option<Bytes>>,
        lost_ack: bool,
        await_ack: bool,
        reads_fail: bool,
        creates: Mutex<u32>,
        ranges: Mutex<Vec<Range<u64>>>,
    }
    #[async_trait]
    impl DataProvider for Fake {
        async fn exists(&self, _: &str) -> Result<bool> {
            panic!("Immutable publication must not HEAD then PUT")
        }
        async fn size(&self, _: &str) -> Result<u64> {
            if self.reads_fail {
                anyhow::bail!("synthetic-provider-secret");
            }
            self.bytes
                .lock()
                .unwrap()
                .as_ref()
                .map(|v| v.len() as u64)
                .ok_or_else(|| anyhow!("absent"))
        }
        async fn range(&self, _: &str, range: Range<u64>) -> Result<Bytes> {
            self.ranges.lock().unwrap().push(range.clone());
            Ok(self
                .bytes
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .slice(range.start as usize..range.end as usize))
        }
        async fn create(&self, _: &str, bytes: Bytes) -> Result<()> {
            *self.creates.lock().unwrap() += 1;
            {
                let mut existing = self.bytes.lock().unwrap();
                if existing.is_some() {
                    anyhow::bail!("precondition failed");
                }
                *existing = Some(bytes);
            }
            if self.await_ack {
                std::future::pending::<()>().await;
            }
            if self.lost_ack {
                anyhow::bail!("synthetic-provider-secret");
            }
            Ok(())
        }
    }
    fn backend(fake: Arc<Fake>) -> RemoteBackend {
        RemoteBackend {
            profile: RemoteData {
                version: 1,
                uri: "s3://synthetic-bucket/task/case".into(),
                endpoint: None,
                region: "auto".into(),
                path_style_access: true,
                credential_source: super::super::config::CredentialSource::AwsEnvironment,
            },
            provider: fake,
        }
    }
    #[tokio::test]
    async fn conditional_create_adopts_exact_lost_ack_and_existing_bytes() -> Result<()> {
        let fake = Arc::new(Fake {
            lost_ack: true,
            ..Default::default()
        });
        let store = backend(fake.clone());
        let bytes = Bytes::from(vec![7; 65537]);
        for _ in 0..2 {
            store
                .immutable(
                    "s3://synthetic-bucket/task/case/objects/test",
                    bytes.clone(),
                    &Budget::new(Limits::default()),
                    64 << 20,
                )
                .await?;
        }
        assert_eq!(*fake.creates.lock().unwrap(), 2);
        assert!(
            fake.ranges
                .lock()
                .unwrap()
                .iter()
                .all(|r| r.end - r.start <= 65536)
        );
        Ok(())
    }
    #[tokio::test]
    async fn existing_mismatch_is_never_overwritten_or_adopted() {
        let fake = Arc::new(Fake {
            bytes: Mutex::new(Some(Bytes::from_static(b"old"))),
            ..Default::default()
        });
        let error = backend(fake.clone())
            .immutable(
                "s3://synthetic-bucket/task/case/objects/test",
                Bytes::from_static(b"new"),
                &Budget::new(Limits::default()),
                64 << 20,
            )
            .await
            .unwrap_err();
        assert_eq!(
            fake.bytes.lock().unwrap().as_ref().unwrap().as_ref(),
            b"old"
        );
        assert!(
            error
                .downcast_ref::<bounds::Fault>()
                .is_some_and(|f| f.code == bounds::FaultCode::CorruptData)
        );
    }
    #[tokio::test]
    async fn missing_readback_retains_unknown_outcome_and_masks_provider_error() {
        let fake = Arc::new(Fake {
            lost_ack: true,
            reads_fail: true,
            ..Default::default()
        });
        let error = backend(fake)
            .immutable(
                "s3://synthetic-bucket/task/case/objects/test",
                Bytes::from_static(b"new"),
                &Budget::new(Limits::default()),
                64 << 20,
            )
            .await
            .unwrap_err();
        assert_eq!(error.to_string(), "Provider write outcome unknown");
    }
    #[tokio::test]
    async fn remote_metadata_limit_precedes_every_range() {
        let fake = Arc::new(Fake {
            bytes: Mutex::new(Some(Bytes::from(vec![0; 1025]))),
            ..Default::default()
        });
        assert!(
            backend(fake.clone())
                .read(
                    "s3://synthetic-bucket/task/case/objects/test",
                    &Budget::new(Limits::default()),
                    1024
                )
                .await
                .is_err()
        );
        assert!(fake.ranges.lock().unwrap().is_empty());
    }
    #[tokio::test(start_paused = true)]
    async fn provider_timeout_requires_exact_readback_and_retains_unreadable_outcome() {
        for reads_fail in [false, true] {
            let fake = Arc::new(Fake {
                await_ack: true,
                reads_fail,
                ..Default::default()
            });
            let result = backend(fake.clone())
                .immutable(
                    "s3://synthetic-bucket/task/case/objects/test",
                    Bytes::from_static(b"new"),
                    &Budget::new(Limits::default()),
                    64 << 20,
                )
                .await;
            if reads_fail {
                assert_eq!(
                    result.unwrap_err().to_string(),
                    "Provider write outcome unknown"
                );
            } else {
                result.unwrap();
            }
            assert_eq!(*fake.creates.lock().unwrap(), 1);
        }
    }
    #[tokio::test]
    async fn invalid_path_and_oversized_write_reject_before_provider_call() {
        let fake = Arc::new(Fake::default());
        let backend = backend(fake.clone());
        assert!(
            backend
                .exists("s3://synthetic-bucket/task/case_alias/object")
                .await
                .is_err()
        );
        assert!(
            backend
                .immutable(
                    "s3://synthetic-bucket/task/case/objects/test",
                    Bytes::from(vec![0; 1025]),
                    &Budget::new(Limits::default()),
                    1024
                )
                .await
                .is_err()
        );
        assert_eq!(*fake.creates.lock().unwrap(), 0);
    }
}
