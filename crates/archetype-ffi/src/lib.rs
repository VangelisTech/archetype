//! C ABI boundary for Archetype native core.
//!
//! Record batches cross this boundary as top-level Arrow `StructArray` values.
//! The public helpers in this crate convert between Arrow C Data Interface
//! structs and safe `arrow-rs` `RecordBatch` values; exported ABI functions
//! should remain thin wrappers around these helpers.

use std::cell::RefCell;
use std::ffi::CString;
use std::os::raw::c_char;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;

use archetype_core::{
    NativeSystem, ProcessorContext,
    processors::movement::{MOVEMENT_REQUIRED_COLUMNS, MovementProcessor},
};
use arrow_array::ffi::{FFI_ArrowArray, from_ffi, to_ffi};
use arrow_array::{Array, RecordBatch, StructArray};
use arrow_schema::ffi::FFI_ArrowSchema;
use arrow_schema::{ArrowError, DataType};

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
}

fn clear_last_error() {
    LAST_ERROR.with(|slot| {
        *slot.borrow_mut() = None;
    });
}

fn set_last_error(message: String) {
    let sanitized = message.replace('\0', "\\0");
    LAST_ERROR.with(|slot| {
        *slot.borrow_mut() = CString::new(sanitized).ok();
    });
}

fn ffi_status(result: std::thread::Result<Result<(), ArrowError>>) -> i32 {
    match result {
        Ok(Ok(())) => {
            clear_last_error();
            0
        }
        Ok(Err(err)) => {
            set_last_error(err.to_string());
            1
        }
        Err(_) => {
            set_last_error("panic crossing Archetype FFI boundary".to_string());
            2
        }
    }
}

/// ABI version for callers that need to check binary compatibility.
#[unsafe(no_mangle)]
pub extern "C" fn arct_ffi_version() -> u32 {
    1
}

/// Return the last error message for the current thread.
///
/// The returned pointer is valid until the next Archetype FFI call on the same
/// thread. Callers must not free it.
#[unsafe(no_mangle)]
pub extern "C" fn arct_last_error_message() -> *const c_char {
    LAST_ERROR.with(|slot| match slot.borrow().as_ref() {
        Some(message) => message.as_ptr(),
        None => ptr::null(),
    })
}

/// Clear the last error message for the current thread.
#[unsafe(no_mangle)]
pub extern "C" fn arct_clear_last_error() {
    clear_last_error();
}

/// Returns true when the pointer shape looks usable.
#[unsafe(no_mangle)]
pub extern "C" fn arct_arrow_c_data_is_present(
    schema: *const FFI_ArrowSchema,
    array: *const FFI_ArrowArray,
) -> bool {
    !schema.is_null() && !array.is_null()
}

/// Move a top-level Arrow C `StructArray` into a Rust `RecordBatch`.
///
/// # Safety
///
/// `array` and `schema` must be valid, aligned, initialized Arrow C Data
/// structs. This function consumes them according to the Arrow C Data Interface
/// move contract and replaces both inputs with empty released structs.
pub unsafe fn record_batch_from_raw(
    array: *mut FFI_ArrowArray,
    schema: *mut FFI_ArrowSchema,
) -> Result<RecordBatch, ArrowError> {
    if array.is_null() || schema.is_null() {
        return Err(ArrowError::CDataInterface(
            "null Arrow C Data pointer".to_string(),
        ));
    }

    let array = unsafe { FFI_ArrowArray::from_raw(array) };
    let schema = unsafe { FFI_ArrowSchema::from_raw(schema) };
    let data = unsafe { from_ffi(array, &schema)? };
    if !matches!(data.data_type(), DataType::Struct(_)) {
        return Err(ArrowError::CDataInterface(
            "record batches must be passed as top-level StructArray values".to_string(),
        ));
    }

    Ok(RecordBatch::from(StructArray::from(data)))
}

/// Export a Rust `RecordBatch` into caller-provided Arrow C Data structs.
///
/// # Safety
///
/// `out_array` and `out_schema` must be valid, aligned pointers to writable
/// memory. They are overwritten with initialized Arrow C Data structs that the
/// caller must later release through the standard Arrow release callbacks.
pub unsafe fn record_batch_into_raw(
    batch: RecordBatch,
    out_array: *mut FFI_ArrowArray,
    out_schema: *mut FFI_ArrowSchema,
) -> Result<(), ArrowError> {
    if out_array.is_null() || out_schema.is_null() {
        return Err(ArrowError::CDataInterface(
            "null Arrow C Data output pointer".to_string(),
        ));
    }

    let struct_array = StructArray::from(batch);
    let (array, schema) = to_ffi(&struct_array.to_data())?;
    unsafe {
        ptr::write(out_array, array);
        ptr::write(out_schema, schema);
    }
    Ok(())
}

/// ABI smoke function: import a record batch and export it back unchanged.
///
/// The function returns `0` on success and non-zero on failure. It consumes the
/// input Arrow C structs and writes initialized output structs.
///
/// # Safety
///
/// All pointers must satisfy the safety requirements of `record_batch_from_raw`
/// and `record_batch_into_raw`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn arct_record_batch_roundtrip(
    in_array: *mut FFI_ArrowArray,
    in_schema: *mut FFI_ArrowSchema,
    out_array: *mut FFI_ArrowArray,
    out_schema: *mut FFI_ArrowSchema,
) -> i32 {
    clear_last_error();
    let result = catch_unwind(AssertUnwindSafe(|| {
        let batch = unsafe { record_batch_from_raw(in_array, in_schema)? };
        unsafe { record_batch_into_raw(batch, out_array, out_schema)? };
        Ok::<(), ArrowError>(())
    }));

    ffi_status(result)
}

fn validate_required_columns(batch: &RecordBatch, names: &[&str]) -> Result<(), ArrowError> {
    for name in names {
        if batch.schema().index_of(name).is_err() {
            return Err(ArrowError::SchemaError(format!(
                "missing required movement column: {name}"
            )));
        }
    }
    Ok(())
}

/// Import a record batch, execute the native movement processor, and export the
/// processed batch.
///
/// This is intentionally a narrow vertical-slice ABI. Generic processor
/// registration across FFI will need a separate function-pointer or plugin
/// design; this function proves the batch boundary and Rust trait path together.
///
/// # Safety
///
/// All pointers must satisfy the safety requirements of `record_batch_from_raw`
/// and `record_batch_into_raw`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn arct_movement_process(
    in_array: *mut FFI_ArrowArray,
    in_schema: *mut FFI_ArrowSchema,
    out_array: *mut FFI_ArrowArray,
    out_schema: *mut FFI_ArrowSchema,
    dt: f64,
    tick: i32,
) -> i32 {
    clear_last_error();
    let result = catch_unwind(AssertUnwindSafe(|| {
        let batch = unsafe { record_batch_from_raw(in_array, in_schema)? };
        validate_required_columns(&batch, MOVEMENT_REQUIRED_COLUMNS)?;
        let mut system = NativeSystem::new();
        system.add_processor(MovementProcessor { dt });
        let out = system
            .execute(batch, &ProcessorContext::new("ffi-world", "ffi-run", tick))
            .map_err(|err| ArrowError::ExternalError(Box::new(err)))?;
        unsafe { record_batch_into_raw(out, out_array, out_schema)? };
        Ok::<(), ArrowError>(())
    }));

    ffi_status(result)
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;
    use std::sync::Arc;

    use arrow_array::{Float64Array, Int32Array};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    fn sample_batch() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("entity_id", DataType::Int32, false),
                Field::new("position__x", DataType::Float64, false),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Float64Array::from(vec![10.0, 20.0])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn ffi_version_is_stable_for_initial_boundary() {
        assert_eq!(arct_ffi_version(), 1);
    }

    fn last_error_message() -> Option<String> {
        let ptr = arct_last_error_message();
        if ptr.is_null() {
            None
        } else {
            Some(
                unsafe { CStr::from_ptr(ptr) }
                    .to_string_lossy()
                    .into_owned(),
            )
        }
    }

    #[test]
    fn record_batch_round_trips_through_arrow_c_data() {
        let batch = sample_batch();
        let mut in_array = FFI_ArrowArray::empty();
        let mut in_schema = FFI_ArrowSchema::empty();
        unsafe {
            record_batch_into_raw(batch, &mut in_array, &mut in_schema).unwrap();
        }

        let mut out_array = FFI_ArrowArray::empty();
        let mut out_schema = FFI_ArrowSchema::empty();
        let code = unsafe {
            arct_record_batch_roundtrip(
                &mut in_array,
                &mut in_schema,
                &mut out_array,
                &mut out_schema,
            )
        };
        assert_eq!(code, 0);

        let out = unsafe { record_batch_from_raw(&mut out_array, &mut out_schema) }.unwrap();
        assert_eq!(out.num_rows(), 2);
        assert_eq!(out.schema().field(1).name(), "position__x");
    }

    #[test]
    fn movement_process_updates_position_columns() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("position__x", DataType::Float64, false),
                Field::new("position__y", DataType::Float64, false),
                Field::new("velocity__dx", DataType::Float64, false),
                Field::new("velocity__dy", DataType::Float64, false),
            ])),
            vec![
                Arc::new(Float64Array::from(vec![0.0, 10.0])),
                Arc::new(Float64Array::from(vec![1.0, 2.0])),
                Arc::new(Float64Array::from(vec![2.0, 3.0])),
                Arc::new(Float64Array::from(vec![-0.5, 1.5])),
            ],
        )
        .unwrap();
        let mut in_array = FFI_ArrowArray::empty();
        let mut in_schema = FFI_ArrowSchema::empty();
        unsafe {
            record_batch_into_raw(batch, &mut in_array, &mut in_schema).unwrap();
        }

        let mut out_array = FFI_ArrowArray::empty();
        let mut out_schema = FFI_ArrowSchema::empty();
        let code = unsafe {
            arct_movement_process(
                &mut in_array,
                &mut in_schema,
                &mut out_array,
                &mut out_schema,
                1.0,
                0,
            )
        };
        assert_eq!(code, 0);

        let out = unsafe { record_batch_from_raw(&mut out_array, &mut out_schema) }.unwrap();
        let x = out
            .column(out.schema().index_of("position__x").unwrap())
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let y = out
            .column(out.schema().index_of("position__y").unwrap())
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        assert_eq!(x.value(0), 2.0);
        assert_eq!(x.value(1), 13.0);
        assert_eq!(y.value(0), 0.5);
        assert_eq!(y.value(1), 3.5);
    }

    #[test]
    fn movement_process_reports_missing_required_column() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("position__x", DataType::Float64, false),
                Field::new("position__y", DataType::Float64, false),
                Field::new("velocity__dx", DataType::Float64, false),
            ])),
            vec![
                Arc::new(Float64Array::from(vec![0.0])),
                Arc::new(Float64Array::from(vec![1.0])),
                Arc::new(Float64Array::from(vec![2.0])),
            ],
        )
        .unwrap();
        let mut in_array = FFI_ArrowArray::empty();
        let mut in_schema = FFI_ArrowSchema::empty();
        unsafe {
            record_batch_into_raw(batch, &mut in_array, &mut in_schema).unwrap();
        }

        let mut out_array = FFI_ArrowArray::empty();
        let mut out_schema = FFI_ArrowSchema::empty();
        let code = unsafe {
            arct_movement_process(
                &mut in_array,
                &mut in_schema,
                &mut out_array,
                &mut out_schema,
                1.0,
                0,
            )
        };

        assert_eq!(code, 1);
        assert_eq!(
            last_error_message().as_deref(),
            Some("Schema error: missing required movement column: velocity__dy")
        );
    }
}
