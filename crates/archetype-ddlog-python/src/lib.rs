//! Versioned local/trusted ABI. Inputs are borrowed for one call; output buffers
//! belong to this library until arct_ddlog_buffer_free. Never unload a live host.
mod json;
mod lifetime;
mod operations;
use lifetime::Host;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU32, AtomicU64, Ordering},
    },
};
const MAX_INPUT: usize = 1024 * 1024;
const MAX_OUTPUT: usize = 16 * 1024 * 1024;
static PID: AtomicU32 = AtomicU32::new(0);
static NEXT: AtomicU64 = AtomicU64::new(1);
static HOSTS: OnceLock<Mutex<BTreeMap<u64, Arc<Host>>>> = OnceLock::new();
#[repr(C)]
#[derive(Default)]
pub struct Buffer {
    pub data: *mut u8,
    pub len: usize,
}
#[derive(Debug)]
struct Error {
    kind: &'static str,
    code: Option<&'static str>,
    message: String,
}
impl Error {
    fn new(kind: &'static str, e: impl std::fmt::Display) -> Self {
        Self {
            kind,
            code: None,
            message: e.to_string(),
        }
    }
    fn operation(error: anyhow::Error) -> Self {
        let code = error.chain().find_map(|source| {
            source
                .downcast_ref::<archetype_ddlog::store::bounds::Fault>()
                .map(|fault| fault.code.as_str())
        });
        Self {
            kind: "operation",
            code,
            message: format!("{error:#}"),
        }
    }
}
type Result<T> = std::result::Result<T, Error>;
struct LimitedOutput(Vec<u8>);
impl std::io::Write for LimitedOutput {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > MAX_OUTPUT.saturating_sub(self.0.len()) {
            return Err(std::io::Error::other("Response byte limit"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn owner() -> Result<()> {
    let pid = std::process::id();
    let recorded = PID
        .compare_exchange(0, pid, Ordering::SeqCst, Ordering::SeqCst)
        .unwrap_or_else(|v| v);
    if recorded != 0 && recorded != pid {
        return Err(Error::new(
            "forked",
            "Inherited native library cannot be used after fork; exec a new process",
        ));
    }
    Ok(())
}
fn hosts() -> &'static Mutex<BTreeMap<u64, Arc<Host>>> {
    HOSTS.get_or_init(|| Mutex::new(BTreeMap::new()))
}
fn host(id: u64) -> Result<Arc<Host>> {
    owner()?;
    hosts()
        .lock()
        .map_err(|e| Error::new("poisoned", e))?
        .get(&id)
        .cloned()
        .ok_or_else(|| Error::new("closed", "Unknown or closed handle"))
}
unsafe fn input<'a>(data: *const u8, len: usize) -> Result<&'a [u8]> {
    if data.is_null() || len == 0 || len > MAX_INPUT {
        return Err(Error::new("request", "Input must contain 1..1048576 bytes"));
    }
    // SAFETY: ABI caller guarantees a readable allocation of len bytes.
    Ok(unsafe { std::slice::from_raw_parts(data, len) })
}
unsafe fn emit(out: *mut Buffer, result: Result<Value>) -> i32 {
    if out.is_null() {
        return 1;
    }
    let failed = result.is_err();
    let value = match result {
        Ok(value) => json!({"ok":true,"value":value}),
        Err(e) => {
            let mut error = json!({"kind":e.kind,"message":e.message});
            if let Some(code) = e.code {
                error["code"] = json!(code);
            }
            json!({"ok":false,"error":error})
        }
    };
    let mut writer = LimitedOutput(Vec::new());
    let overflow = serde_json::to_writer(&mut writer, &value).is_err();
    let mut bytes = writer.0;
    if overflow {
        bytes=br#"{"ok":false,"error":{"kind":"response_limit","message":"Response exceeds 16 MiB; operation may have completed; inspect exact identity"}}"#.to_vec();
    }
    let mut bytes = bytes.into_boxed_slice();
    let buffer = Buffer {
        data: bytes.as_mut_ptr(),
        len: bytes.len(),
    };
    std::mem::forget(bytes);
    // SAFETY: ABI caller guarantees writable output, initially zeroed.
    unsafe {
        out.write(buffer);
    }
    i32::from(failed || overflow)
}
unsafe fn boundary(out: *mut Buffer, f: impl FnOnce() -> Result<Value>) -> i32 {
    if out.is_null() {
        return 1;
    }
    unsafe {
        out.write(Buffer::default());
    }
    let result = catch_unwind(AssertUnwindSafe(f)).unwrap_or_else(|_| {
        Err(Error::new(
            "panic",
            "Rust panic; operation outcome may be uncertain",
        ))
    });
    unsafe { emit(out, result) }
}
#[unsafe(no_mangle)]
pub extern "C" fn arct_ddlog_abi_version() -> u32 {
    1
}
/// Pure scalar compatibility probe: no owner initialization, filesystem,
/// locks, storage, registry access or world admission.
#[unsafe(no_mangle)]
pub extern "C" fn arct_ddlog_contract_version() -> u32 {
    2
}
/// # Safety
/// data points to len readable bytes; out is writable and holds no live buffer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn arct_ddlog_open(data: *const u8, len: usize, out: *mut Buffer) -> i32 {
    unsafe {
        boundary(out, || {
            owner()?;
            let config = json::decode(input(data, len)?).map_err(|e| Error::new("request", e))?;
            let resources =
                operations::Resources::open(config).map_err(|e| Error::new("open", e))?;
            let host = Arc::new(Host::new(resources).map_err(|e| Error::new("open", e))?);
            let id = NEXT
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| v.checked_add(1))
                .map_err(|_| Error::new("open", "Handle space exhausted"))?;
            hosts()
                .lock()
                .map_err(|e| Error::new("poisoned", e))?
                .insert(id, host);
            Ok(json!({"handle":id,"abi":1,"ddlog_revision":archetype_ddlog::DDLOG_REVISION}))
        })
    }
}
/// # Safety
/// Same buffer contract as open. Handles may be shared by threads, never forks.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn arct_ddlog_call(
    id: u64,
    data: *const u8,
    len: usize,
    out: *mut Buffer,
) -> i32 {
    unsafe {
        boundary(out, || {
            let host = host(id)?;
            let op = json::decode(input(data, len)?).map_err(|e| Error::new("request", e))?;
            let lease = host.enter().map_err(|e| Error::new("closed", e))?;
            let result = catch_unwind(AssertUnwindSafe(|| {
                lease.resources.as_ref().unwrap().call(op)
            }));
            match result {
                Ok(result) => result.map_err(Error::operation),
                Err(_) => {
                    host.poison();
                    Err(Error::new(
                        "panic",
                        "Host poisoned and shutdown requested; inspect durable evidence in a new process",
                    ))
                }
            }
        })
    }
}
/// # Safety
/// out is writable and holds no live buffer. Failure retains closing ownership.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn arct_ddlog_close(id: u64, out: *mut Buffer) -> i32 {
    unsafe {
        boundary(out, || {
            owner()?;
            let host = hosts()
                .lock()
                .map_err(|e| Error::new("poisoned", e))?
                .get(&id)
                .cloned();
            if let Some(host) = host {
                host.close().map_err(|e| Error::new("close", e))?;
                hosts()
                    .lock()
                    .map_err(|e| Error::new("poisoned", e))?
                    .remove(&id);
            }
            Ok(json!({"closed":true}))
        })
    }
}
/// # Safety
/// buffer is writable and contains either zeroes or this library's unfreed
/// allocation. Do not copy ownership or free from another library/allocator.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn arct_ddlog_buffer_free(buffer: *mut Buffer) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if buffer.is_null() {
            return;
        }
        let b = unsafe { &mut *buffer };
        if !b.data.is_null() {
            let data = std::ptr::slice_from_raw_parts_mut(b.data, b.len);
            *b = Buffer::default();
            drop(unsafe { Box::from_raw(data) });
        }
    }));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_owned_faults_receive_factual_codes() {
        use archetype_ddlog::store::bounds::{FaultCode, fault};
        for code in [
            FaultCode::ResourceLimit,
            FaultCode::CorruptData,
            FaultCode::InvalidRequest,
            FaultCode::UnsupportedFormat,
        ] {
            let error = Error::operation(fault(code, "/private/token").context("catalog context"));
            assert_eq!(error.code, Some(code.as_str()));
            assert!(error.message.contains("/private/token"));
        }
        assert!(
            Error::operation(anyhow::anyhow!(
                "resource_limit corrupt_data unknown world rollback"
            ))
            .code
            .is_none()
        );
    }

    #[test]
    fn output_writer_rejects_before_extending_past_limit() {
        use std::io::Write;
        let mut output = LimitedOutput(vec![0; MAX_OUTPUT - 1]);
        assert!(output.write_all(&[1, 2]).is_err());
        assert_eq!(output.0.len(), MAX_OUTPUT - 1);
    }

    #[test]
    fn boundary_contains_panic_and_returns_owned_error() {
        let mut output = Buffer::default();
        // SAFETY: valid initially empty output; the injected unwind stays inside
        // the same boundary used by the public C entry points.
        let status = unsafe { boundary(&mut output, || panic!("injected boundary panic")) };
        assert_eq!(status, 1);
        let response: Value =
            serde_json::from_slice(unsafe { std::slice::from_raw_parts(output.data, output.len) })
                .unwrap();
        assert_eq!(response["error"]["kind"], "panic");
        unsafe {
            arct_ddlog_buffer_free(&mut output);
            arct_ddlog_buffer_free(&mut output);
        }
        assert!(output.data.is_null());
        assert_eq!(output.len, 0);
    }
}
