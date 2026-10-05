//! Operator-only remote data identity. Local catalog and journals stay local.
use super::bounds;
use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteData {
    pub version: u32,
    /// One exclusively assigned bucket prefix, never a bucket root.
    pub uri: String,
    pub endpoint: Option<String>,
    pub region: String,
    pub path_style_access: bool,
    pub credential_source: CredentialSource,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CredentialSource {
    AwsEnvironment,
}
impl RemoteData {
    pub fn validate(&self) -> Result<()> {
        bounds::request(self.version == 1, "Unsupported remote data configuration")?;
        self.check_uri(&self.uri, false)?;
        bounds::request(
            !self.region.is_empty()
                && self.region.len() <= 64
                && self
                    .region
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-'),
            "Invalid provider region",
        )?;
        if let Some(value) = &self.endpoint {
            let url = url::Url::parse(value).map_err(|_| {
                bounds::fault(
                    bounds::FaultCode::InvalidRequest,
                    "Invalid provider endpoint",
                )
            })?;
            bounds::request(
                value.len() <= 1024
                    && !value.chars().any(|c| c.is_whitespace() || c.is_control())
                    && url.scheme() == "https"
                    && url.host_str().is_some()
                    && url.port() != Some(0)
                    && url.username().is_empty()
                    && url.password().is_none()
                    && url.query().is_none()
                    && url.fragment().is_none()
                    && matches!(url.path(), "" | "/"),
                "Expected HTTPS provider origin without credentials",
            )?;
        }
        Ok(())
    }
    fn check_uri(&self, value: &str, child: bool) -> Result<()> {
        let url = url::Url::parse(value).map_err(|_| {
            bounds::fault(
                bounds::FaultCode::InvalidRequest,
                "Invalid remote object URI",
            )
        })?;
        let bucket = url.host_str().unwrap_or_default();
        let key = value
            .strip_prefix(&format!("s3://{bucket}/"))
            .unwrap_or_default();
        bounds::request(
            value.len() <= 2048
                && url.scheme() == "s3"
                && (3..=63).contains(&bucket.len())
                && bucket.bytes().all(|b| {
                    b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'.'
                })
                && url.username().is_empty()
                && url.password().is_none()
                && url.port().is_none()
                && url.query().is_none()
                && url.fragment().is_none()
                && !key.is_empty()
                && key.split('/').all(|p| {
                    !p.is_empty()
                        && p != "."
                        && p != ".."
                        && p.bytes()
                            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
                })
                && (!child || value.starts_with(&(self.uri.clone() + "/"))),
            "Remote object outside canonical owned namespace",
        )
    }
    pub(super) fn object(&self, value: &str) -> Result<()> {
        self.check_uri(value, true)
    }
    pub(super) fn location(&self, key: &str) -> String {
        format!("{}/{key}", self.uri)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    pub(super) fn profile() -> RemoteData {
        RemoteData {
            version: 1,
            uri: "s3://synthetic-bucket/task/case".into(),
            endpoint: Some("https://objects.example.test".into()),
            region: "auto".into(),
            path_style_access: true,
            credential_source: CredentialSource::AwsEnvironment,
        }
    }
    #[test]
    fn canonical_namespace_rejects_aliases_and_foreign_paths() -> Result<()> {
        let config = profile();
        config.validate()?;
        config.object("s3://synthetic-bucket/task/case/warehouse/a.metadata.json")?;
        for path in [
            "s3://synthetic-bucket/task/case_other/x",
            "s3://other-bucket/task/case/x",
            "s3://synthetic-bucket/task/case/../x",
            "s3://synthetic-bucket/task/case/%2e%2e/x",
            "s3://synthetic-bucket/task/case//x",
            "s3://synthetic-bucket/task/case/x?secret=yes",
            "file:///tmp/x",
        ] {
            assert!(config.object(path).is_err(), "{path}");
        }
        for uri in [
            "s3://synthetic-bucket",
            "s3://synthetic-bucket/",
            "s3://synthetic-bucket/task/../case",
            "s3://synthetic-bucket/task/case/",
        ] {
            let mut changed = config.clone();
            changed.uri = uri.into();
            assert!(changed.validate().is_err());
        }
        Ok(())
    }
    #[test]
    fn closed_configuration_cannot_carry_credentials() {
        let mut value = serde_json::to_value(profile()).unwrap();
        value["secret_access_key"] = serde_json::json!("synthetic-secret");
        assert!(serde_json::from_value::<RemoteData>(value).is_err());
        let mut config = profile();
        config.endpoint = Some("https://key:secret@example.test".into());
        assert!(config.validate().is_err());
    }
}
