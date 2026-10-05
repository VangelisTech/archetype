//! Versioned local read admission. Each catalog view owns one immutable budget;
//! background readers retain it and can never consume a later request's budget.
use anyhow::{Result, anyhow};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

pub const VERSION: u32 = 1;

#[derive(Clone, Debug)]
pub struct Limits {
    pub files: u64,
    pub bytes: u64,
    pub file_bytes: u64,
    pub metadata_bytes: u64,
    pub rows: u64,
    pub items: u64,
    pub page_bytes: u64,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            files: 512,
            bytes: 256 << 20,
            file_bytes: 64 << 20,
            metadata_bytes: 2 << 20,
            rows: 250_000,
            items: 500_000,
            page_bytes: 2 << 20,
        }
    }
}

/// Facts about this boundary only. No variant establishes a mutation outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultCode {
    ResourceLimit,
    CorruptData,
    InvalidRequest,
    Conflict,
    UnsupportedFormat,
}
impl FaultCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ResourceLimit => "resource_limit",
            Self::CorruptData => "corrupt_data",
            Self::InvalidRequest => "invalid_request",
            Self::Conflict => "conflict",
            Self::UnsupportedFormat => "unsupported_format",
        }
    }
}
#[derive(Debug)]
pub struct Fault {
    pub code: FaultCode,
    detail: String,
}
impl std::fmt::Display for Fault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code.as_str(), self.detail)
    }
}
impl std::error::Error for Fault {}
pub fn fault(code: FaultCode, detail: impl Into<String>) -> anyhow::Error {
    Fault {
        code,
        detail: detail.into(),
    }
    .into()
}
pub fn corrupt(condition: bool, detail: &str) -> Result<()> {
    if !condition {
        return Err(fault(FaultCode::CorruptData, detail));
    }
    Ok(())
}
pub fn request(condition: bool, detail: &str) -> Result<()> {
    if !condition {
        return Err(fault(FaultCode::InvalidRequest, detail));
    }
    Ok(())
}
pub fn cap(value: u64, limit: u64, name: &str) -> Result<()> {
    if value > limit {
        return Err(fault(
            FaultCode::ResourceLimit,
            format!("{name}: {value} exceeds {limit}"),
        ));
    }
    Ok(())
}

pub fn page<T: Serialize + ?Sized>(value: &T, limit: u64) -> Result<()> {
    struct Counter {
        bytes: u64,
        limit: u64,
    }
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() as u64 > self.limit.saturating_sub(self.bytes) {
                return Err(std::io::Error::other("page limit"));
            }
            self.bytes += bytes.len() as u64;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    serde_json::to_writer(Counter { bytes: 0, limit }, value)
        .map_err(|_| fault(FaultCode::ResourceLimit, "Encoded page exceeds byte limit"))
}

pub fn encode_metadata<T: Serialize>(value: &T, limit: u64) -> Result<Vec<u8>> {
    struct Output {
        bytes: Vec<u8>,
        limit: u64,
    }
    impl std::io::Write for Output {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() as u64 > self.limit.saturating_sub(self.bytes.len() as u64) {
                return Err(std::io::Error::other("metadata limit"));
            }
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut output = Output {
        bytes: Vec::new(),
        limit,
    };
    serde_json::to_writer(&mut output, value).map_err(|_| {
        fault(
            FaultCode::ResourceLimit,
            "Encoded metadata exceeds byte limit",
        )
    })?;
    Ok(output.bytes)
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Usage {
    pub files: u64,
    pub bytes: u64,
    pub rows: u64,
    pub items: u64,
    pub decodes: u64,
    pub decoded_bytes: u64,
    pub decoded_rows: u64,
}
#[derive(Debug)]
pub struct Budget {
    pub limits: Limits,
    used: Mutex<Usage>,
}
impl Budget {
    pub fn new(limits: Limits) -> Arc<Self> {
        Arc::new(Self {
            limits,
            used: Mutex::new(Usage::default()),
        })
    }
    pub fn usage(&self) -> Usage {
        self.used.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }
    fn charge(
        &self,
        amount: u64,
        name: &str,
        limit: u64,
        select: fn(&mut Usage) -> &mut u64,
    ) -> Result<()> {
        let mut used = self
            .used
            .lock()
            .map_err(|_| anyhow!("Read accounting poisoned"))?;
        let counter = select(&mut used);
        let next = counter
            .checked_add(amount)
            .ok_or_else(|| fault(FaultCode::ResourceLimit, name))?;
        cap(next, limit, name)?;
        *counter = next;
        Ok(())
    }
    pub fn bytes(&self, n: u64) -> Result<()> {
        self.charge(n, "read bytes", self.limits.bytes, |u| &mut u.bytes)
    }
    pub fn rows(&self, n: u64) -> Result<()> {
        self.charge(n, "scanned rows", self.limits.rows, |u| &mut u.rows)
    }
    pub fn decoded_bytes(&self, n: u64) -> Result<()> {
        self.charge(n, "decoded bytes", self.limits.bytes, |u| {
            &mut u.decoded_bytes
        })
    }
    pub fn decoded_rows(&self, n: u64) -> Result<()> {
        self.charge(n, "decoded rows", self.limits.rows, |u| &mut u.decoded_rows)
    }
    pub fn items(&self, n: u64) -> Result<()> {
        self.charge(n, "metadata items", self.limits.items, |u| &mut u.items)
    }
    pub fn decoding(&self) -> Result<()> {
        self.charge(1, "decode calls", self.limits.files * 8, |u| &mut u.decodes)
    }
    pub fn file(&self, size: u64, limit: u64) -> Result<()> {
        cap(size, limit, "file bytes")?;
        self.charge(1, "file opens", self.limits.files, |u| &mut u.files)
    }
    /// Check size on the opened descriptor before allocation. A concurrent
    /// growth cannot bypass the cap: reads stop at the admitted length + 1.
    pub fn read(&self, path: &Path, limit: u64) -> Result<Bytes> {
        let mut file = File::open(path)?;
        let size = file.metadata()?.len();
        self.file(size, limit)?;
        self.bytes(
            size.checked_add(1)
                .ok_or_else(|| fault(FaultCode::ResourceLimit, "file size overflow"))?,
        )?;
        let mut bytes = Vec::with_capacity(usize::try_from(size)?);
        (&mut file).take(size + 1).read_to_end(&mut bytes)?;
        corrupt(
            bytes.len() as u64 == size,
            "File changed length during bounded read",
        )?;
        Ok(bytes.into())
    }
    pub fn read_metadata(&self, path: &Path) -> Result<Bytes> {
        let bytes = self.read(path, self.limits.metadata_bytes)?;
        super::preflight::json(&bytes, self)?;
        self.decoding()?;
        Ok(bytes)
    }
    /// Streaming content verification has the same file and byte admission.
    pub fn content(&self, path: &Path, mut consume: impl FnMut(&[u8])) -> Result<u64> {
        let mut file = File::open(path)?;
        let size = file.metadata()?.len();
        self.file(size, self.limits.file_bytes)?;
        self.bytes(size + 1)?;
        let mut reader = (&mut file).take(size + 1);
        let mut block = [0; 65536];
        let mut read = 0;
        loop {
            let n = reader.read(&mut block)?;
            if n == 0 {
                break;
            }
            read += n as u64;
            consume(&block[..n]);
        }
        corrupt(read == size, "Content changed length during bounded read")?;
        Ok(size)
    }
}

pub fn local_path(path: &str) -> Result<PathBuf> {
    let p = if path.starts_with("file:") {
        url::Url::parse(path)?
            .to_file_path()
            .map_err(|_| fault(FaultCode::InvalidRequest, "Nonlocal file URI"))?
    } else {
        PathBuf::from(path)
    };
    request(p.is_absolute(), "Absolute local path required")?;
    Ok(p)
}

/// Footer length is checked before Parquet allocates or decodes its metadata.
pub fn parquet_footer(bytes: &[u8], budget: &Budget) -> Result<std::ops::Range<usize>> {
    corrupt(
        bytes.len() >= 12 && &bytes[..4] == b"PAR1" && &bytes[bytes.len() - 4..] == b"PAR1",
        "Invalid unencrypted Parquet envelope",
    )?;
    let n =
        u32::from_le_bytes(bytes[bytes.len() - 8..bytes.len() - 4].try_into().unwrap()) as usize;
    cap(
        n as u64,
        budget.limits.metadata_bytes,
        "Parquet footer bytes",
    )?;
    corrupt(n <= bytes.len() - 12, "Parquet footer exceeds object")?;
    Ok(bytes.len() - 8 - n..bytes.len() - 8)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sparse_oversize_fails_before_buffer_or_decode() -> Result<()> {
        let f = tempfile::NamedTempFile::new()?;
        f.as_file().set_len(1 << 30)?;
        let b = Budget::new(Limits::default());
        let e = b.read(f.path(), 1024).unwrap_err();
        assert_eq!(
            e.downcast_ref::<Fault>().unwrap().code,
            FaultCode::ResourceLimit
        );
        assert_eq!(
            (b.usage().files, b.usage().bytes, b.usage().decodes),
            (0, 0, 0)
        );
        Ok(())
    }
}
