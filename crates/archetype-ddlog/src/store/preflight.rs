//! Allocation-free structural admission before the pinned format decoders.
//! Local v1 writers use uncompressed JSON, Avro and Parquet. Compression is
//! rejected here rather than claiming the upstream decoders have a memory cap.
use super::bounds::{Budget, FaultCode, cap, corrupt, fault};
use anyhow::Result;
use bytes::Bytes;
use parquet::{arrow::arrow_reader::ParquetRecordBatchReaderBuilder, basic::Compression};
use serde_json::Value;
use std::collections::BTreeMap;
#[path = "thrift_guard.rs"]
mod thrift_guard;

pub(super) fn builder(
    bytes: Bytes,
) -> Result<parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder<Bytes>> {
    // The embedded Arrow IPC schema is independent of the admitted Thrift
    // schema. Infer the local flat physical schema solely from Parquet instead.
    std::panic::catch_unwind(|| {
        let options =
            parquet::arrow::arrow_reader::ArrowReaderOptions::new().with_skip_arrow_metadata(true);
        let inferred =
            ParquetRecordBatchReaderBuilder::try_new_with_options(bytes.clone(), options)?;
        // Iceberg spells UTC as +00:00. Normalize only that equivalent spelling;
        // every name, physical/logical type, nullability and field ID still comes
        // from the admitted physical Parquet schema, never an embedded hint.
        let fields: Vec<_> = inferred
            .schema()
            .fields()
            .iter()
            .map(|field| match field.data_type() {
                arrow_schema::DataType::Timestamp(unit, Some(tz)) if tz.as_ref() == "UTC" => {
                    std::sync::Arc::new(field.as_ref().clone().with_data_type(
                        arrow_schema::DataType::Timestamp(*unit, Some("+00:00".into())),
                    ))
                }
                _ => field.clone(),
            })
            .collect();
        let schema = std::sync::Arc::new(arrow_schema::Schema::new_with_metadata(
            fields,
            inferred.schema().metadata().clone(),
        ));
        let metadata = parquet::arrow::arrow_reader::ArrowReaderMetadata::try_new(
            inferred.metadata().clone(),
            parquet::arrow::arrow_reader::ArrowReaderOptions::new().with_schema(schema),
        )?;
        Ok::<_, parquet::errors::ParquetError>(ParquetRecordBatchReaderBuilder::new_with_metadata(
            bytes, metadata,
        ))
    })
    .map_err(|_| fault(FaultCode::CorruptData, "Parquet metadata decoder panicked"))?
    .map_err(|e| fault(FaultCode::CorruptData, format!("Parquet metadata: {e}")))
}

struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}
impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        corrupt(
            n <= self.bytes.len().saturating_sub(self.pos),
            "Truncated metadata value",
        )?;
        let start = self.pos;
        self.pos += n;
        Ok(&self.bytes[start..self.pos])
    }
    fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn unsigned(&mut self) -> Result<u64> {
        let mut result = 0;
        for i in 0..10 {
            let b = self.byte()?;
            corrupt(i < 9 || b <= 1, "Invalid metadata integer")?;
            result |= ((b & 127) as u64) << (7 * i);
            if b < 128 {
                return Ok(result);
            }
        }
        Err(fault(FaultCode::CorruptData, "Invalid metadata varint"))
    }
    fn signed(&mut self) -> Result<i64> {
        let n = self.unsigned()?;
        Ok((n >> 1) as i64 ^ -((n & 1) as i64))
    }
    fn blob(&mut self, budget: &Budget) -> Result<&'a [u8]> {
        let n = self.signed()?;
        corrupt(n >= 0, "Negative metadata length")?;
        cap(
            n as u64,
            budget.limits.metadata_bytes,
            "Metadata value bytes",
        )?;
        self.take(usize::try_from(n)?)
    }
}

pub(super) fn json(bytes: &[u8], budget: &Budget) -> Result<()> {
    let mut depth: u64 = 0;
    let mut quoted = false;
    let mut escaped = false;
    for &b in bytes {
        if quoted {
            if escaped {
                escaped = false;
            } else if b == b'\\' {
                escaped = true;
            } else if b == b'"' {
                quoted = false;
            }
        } else {
            match b {
                b'"' => {
                    quoted = true;
                    budget.items(1)?;
                }
                b'{' | b'[' => {
                    depth += 1;
                    cap(depth, 32, "JSON depth")?;
                    budget.items(1)?;
                }
                b'}' | b']' => {
                    corrupt(depth > 0, "Invalid JSON nesting")?;
                    depth -= 1;
                }
                b',' => budget.items(1)?,
                _ => {}
            }
        }
    }
    corrupt(!quoted && depth == 0, "Incomplete JSON metadata")?;
    serde_json::from_slice::<serde::de::IgnoredAny>(bytes).map_err(|e| {
        fault(
            FaultCode::CorruptData,
            format!("Invalid JSON metadata: {e}"),
        )
    })?;
    Ok(())
}

fn compact_value(
    c: &mut Cursor<'_>,
    kind: u8,
    in_field: bool,
    depth: u64,
    budget: &Budget,
) -> Result<Option<i64>> {
    cap(depth, 32, "Thrift depth")?;
    budget.items(1)?;
    match kind {
        1 | 2 => {
            if !in_field {
                let b = c.byte()?;
                corrupt(b == 1 || b == 2, "Invalid Thrift bool")?;
            }
        }
        3 => {
            c.take(1)?;
        }
        4..=6 => return Ok(Some(c.signed()?)),
        7 => {
            c.take(8)?;
        }
        8 => {
            let n = c.unsigned()?;
            cap(n, budget.limits.metadata_bytes, "Thrift binary bytes")?;
            c.take(usize::try_from(n)?)?;
        }
        9 | 10 => {
            let h = c.byte()?;
            let n = if h >> 4 == 15 {
                c.unsigned()?
            } else {
                (h >> 4) as u64
            };
            cap(n, budget.limits.items, "Thrift collection count")?;
            for _ in 0..n {
                compact_value(c, h & 15, false, depth + 1, budget)?;
            }
        }
        11 => {
            let n = c.unsigned()?;
            cap(n, budget.limits.items, "Thrift map count")?;
            if n > 0 {
                let kinds = c.byte()?;
                for _ in 0..n {
                    compact_value(c, kinds >> 4, false, depth + 1, budget)?;
                    compact_value(c, kinds & 15, false, depth + 1, budget)?;
                }
            }
        }
        12 => {
            compact_struct(c, depth + 1, budget, false)?;
        }
        _ => return Err(fault(FaultCode::CorruptData, "Unknown Thrift value kind")),
    }
    Ok(None)
}
fn compact_struct(
    c: &mut Cursor<'_>,
    depth: u64,
    budget: &Budget,
    page: bool,
) -> Result<BTreeMap<i64, i64>> {
    cap(depth, 32, "Thrift depth")?;
    let mut last = 0i64;
    let mut numbers = BTreeMap::new();
    loop {
        let h = c.byte()?;
        if h == 0 {
            if page {
                corrupt(
                    matches!(
                        (numbers.get(&1), numbers.get(&101)),
                        (Some(0), Some(5)) | (Some(2), Some(7))
                    ),
                    "Page type/header mismatch",
                )?;
            }
            return Ok(numbers);
        }
        let id = if h >> 4 == 0 {
            c.signed()?
        } else {
            last + (h >> 4) as i64
        };
        corrupt((1..=32767).contains(&id), "Invalid Thrift field ID")?;
        last = id;
        if page && [5, 7, 8].contains(&id) && h & 15 == 12 {
            numbers.insert(101, id);
            let fields = compact_struct(c, depth + 1, budget, false)?;
            if let Some(&n) = fields.get(&1) {
                corrupt(n >= 0, "Negative Parquet page value count")?;
                cap(n as u64, budget.limits.rows, "Parquet page values")?;
            }
            if (id == 5 || id == 8)
                && let Some(&n) = fields.get(&1)
            {
                numbers.insert(100, n);
            }
            let encoding = fields.get(&if id == 8 { 4 } else { 2 });
            if !encoding.is_some_and(|n| {
                if id == 7 {
                    *n == 0
                } else {
                    [0, 2, 8].contains(n)
                }
            }) {
                return Err(fault(
                    FaultCode::UnsupportedFormat,
                    "Parquet encoding outside local read format v1",
                ));
            }
            if id == 5 {
                corrupt(
                    fields.get(&3) == Some(&3) && fields.get(&4) == Some(&3),
                    "Unsupported page level encoding",
                )?;
            }
        } else if let Some(n) = compact_value(c, h & 15, true, depth + 1, budget)? {
            numbers.insert(id, n);
        }
    }
}

// SchemaElement.num_children is a scalar allocation claim in the library.
// Check the local writer's flat schema before its metadata decoder runs.
fn footer_schema(c: &mut Cursor<'_>, budget: &Budget) -> Result<()> {
    let mut last = 0i64;
    let mut found = false;
    loop {
        let h = c.byte()?;
        if h == 0 {
            break;
        }
        let id = if h >> 4 == 0 {
            c.signed()?
        } else {
            last + (h >> 4) as i64
        };
        last = id;
        if id == 2 {
            corrupt(!found && h & 15 == 9, "Invalid Parquet schema list")?;
            found = true;
            let header = c.byte()?;
            let count = if header >> 4 == 15 {
                c.unsigned()?
            } else {
                (header >> 4) as u64
            };
            cap(count, 257, "Parquet schema elements")?;
            corrupt(
                count >= 2 && header & 15 == 12,
                "Invalid Parquet schema elements",
            )?;
            for index in 0..count {
                let fields = compact_struct(c, 0, budget, false)?;
                if index == 0 {
                    corrupt(
                        fields.get(&5) == Some(&((count - 1) as i64)),
                        "Invalid Parquet root children",
                    )?;
                } else {
                    if fields.get(&5).is_some_and(|&n| n != 0) {
                        return Err(fault(
                            FaultCode::UnsupportedFormat,
                            "Nested Parquet outside local read format v1",
                        ));
                    }
                    corrupt(
                        fields.get(&1).is_some_and(|&n| (0..=7).contains(&n)),
                        "Invalid Parquet primitive",
                    )?;
                    corrupt(
                        fields.get(&3).is_some_and(|&n| n == 0 || n == 1),
                        "Repeated Parquet field outside local format",
                    )?;
                    if let Some(&n) = fields.get(&2) {
                        corrupt(n > 0, "Invalid fixed byte width")?;
                        cap(n as u64, budget.limits.page_bytes, "Fixed byte width")?;
                    }
                }
            }
        } else {
            compact_value(c, h & 15, true, 0, budget)?;
        }
    }
    corrupt(found, "Missing Parquet schema")
}

/// Inspect footer collection claims before the library parses it, then inspect
/// every uncompressed page header before Arrow can allocate dictionary/value buffers.
pub(super) fn parquet(bytes: Bytes, budget: &Budget) -> Result<()> {
    let footer = super::bounds::parquet_footer(&bytes, budget)?;
    let mut grammar = Cursor::new(&bytes[footer.clone()]);
    thrift_guard::structure(&mut grammar, "file", budget, 0)?;
    corrupt(
        grammar.pos == grammar.bytes.len(),
        "Trailing typed footer bytes",
    )?;
    let mut cursor = Cursor::new(&bytes[footer.clone()]);
    footer_schema(&mut cursor, budget)?;
    corrupt(
        cursor.pos == cursor.bytes.len(),
        "Trailing Parquet footer bytes",
    )?;
    budget.decoding()?;
    let builder = builder(bytes.clone())?;
    let metadata = builder.metadata();
    let rows = metadata.file_metadata().num_rows();
    corrupt(rows >= 0, "Negative Parquet row count")?;
    budget.rows(rows as u64)?;
    cap(metadata.num_row_groups() as u64, 1024, "Parquet row groups")?;
    cap(
        metadata.file_metadata().schema_descr().num_columns() as u64,
        256,
        "Parquet columns",
    )?;
    let mut total_uncompressed = 0u64;
    let mut decoded_bound = 0u64;
    let mut group_rows = 0u64;
    for group in metadata.row_groups() {
        corrupt(group.num_rows() >= 0, "Negative row-group count")?;
        cap(
            group.num_rows() as u64,
            budget.limits.rows,
            "Row-group rows",
        )?;
        group_rows = group_rows
            .checked_add(group.num_rows() as u64)
            .ok_or_else(|| fault(FaultCode::ResourceLimit, "Row-group count overflow"))?;
        cap(group_rows, budget.limits.rows, "Total row-group rows")?;
        for column in group.columns() {
            if column.compression() != Compression::UNCOMPRESSED {
                return Err(fault(
                    FaultCode::UnsupportedFormat,
                    "Compressed Parquet is outside local read format v1",
                ));
            }
            corrupt(
                column.uncompressed_size() >= 0
                    && column.compressed_size() >= 0
                    && column.num_values() >= 0,
                "Negative Parquet column bounds",
            )?;
            total_uncompressed = total_uncompressed
                .checked_add(column.uncompressed_size() as u64)
                .ok_or_else(|| fault(FaultCode::ResourceLimit, "Parquet size overflow"))?;
            cap(
                total_uncompressed,
                budget.limits.file_bytes,
                "Parquet uncompressed bytes",
            )?;
            cap(
                column.num_values() as u64,
                budget.limits.rows,
                "Parquet column values",
            )?;
            let width = match column.column_type() {
                parquet::basic::Type::BYTE_ARRAY | parquet::basic::Type::FIXED_LEN_BYTE_ARRAY => {
                    column.uncompressed_size() as u64 + 8
                }
                _ => 16,
            };
            decoded_bound =
                decoded_bound
                    .checked_add(width.checked_mul(column.num_values() as u64).ok_or_else(
                        || fault(FaultCode::ResourceLimit, "Decoded column overflow"),
                    )?)
                    .ok_or_else(|| fault(FaultCode::ResourceLimit, "Decoded file overflow"))?;
            cap(
                decoded_bound,
                budget.limits.file_bytes,
                "Conservative decoded file bytes",
            )?;
            let (start, size) = column.byte_range();
            let end = start
                .checked_add(size)
                .ok_or_else(|| fault(FaultCode::CorruptData, "Parquet column overflow"))?;
            corrupt(
                start >= 4 && end <= footer.start as u64,
                "Parquet column outside data object",
            )?;
            let mut pages = Cursor::new(&bytes[usize::try_from(start)?..usize::try_from(end)?]);
            let mut page_values = 0u64;
            while pages.pos < pages.bytes.len() {
                let header_start = pages.pos;
                let mut grammar = Cursor::new(&pages.bytes[header_start..]);
                thrift_guard::structure(&mut grammar, "page", budget, 0)?;
                let values = compact_struct(&mut pages, 0, budget, true)?;
                corrupt(
                    grammar.pos == pages.pos - header_start,
                    "Page decoder boundary mismatch",
                )?;
                if let Some(&n) = values.get(&100) {
                    page_values = page_values
                        .checked_add(n as u64)
                        .ok_or_else(|| fault(FaultCode::ResourceLimit, "Page values overflow"))?;
                    cap(page_values, budget.limits.rows, "Column page values")?;
                }
                cap(
                    (pages.pos - header_start) as u64,
                    budget.limits.metadata_bytes,
                    "Parquet page header bytes",
                )?;
                let decoded = *values
                    .get(&2)
                    .ok_or_else(|| fault(FaultCode::CorruptData, "Missing page size"))?;
                let encoded = *values
                    .get(&3)
                    .ok_or_else(|| fault(FaultCode::CorruptData, "Missing encoded page size"))?;
                corrupt(
                    decoded >= 0 && decoded == encoded,
                    "Invalid uncompressed page sizes",
                )?;
                cap(
                    decoded as u64,
                    budget.limits.page_bytes,
                    "Parquet page bytes",
                )?;
                pages.take(usize::try_from(encoded)?)?;
            }
            corrupt(
                page_values == column.num_values() as u64 && page_values == group.num_rows() as u64,
                "Page/column row inventory mismatch",
            )?;
            corrupt(
                size == column.uncompressed_size() as u64,
                "Uncompressed column byte inventory mismatch",
            )?;
        }
    }
    corrupt(
        group_rows == rows as u64,
        "File/row-group inventory mismatch",
    )?;
    budget.decoded_bytes(decoded_bound)?;
    Ok(())
}

fn annotations(schema: &Value) -> Result<()> {
    match schema {
        Value::Array(items) => {
            for item in items {
                annotations(item)?;
            }
        }
        Value::Object(fields) => {
            if fields.contains_key("namespace") || fields.contains_key("aliases") {
                return Err(fault(
                    FaultCode::UnsupportedFormat,
                    "Avro name indirection outside local format v1",
                ));
            }
            if let Some(logical) = fields.get("logicalType")
                && (logical.as_str() != Some("map")
                    || fields.get("type").and_then(Value::as_str) != Some("array"))
            {
                return Err(fault(
                    FaultCode::UnsupportedFormat,
                    "Avro logical type outside local format v1",
                ));
            }
            for value in fields.values() {
                annotations(value)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn names<'a>(
    schema: &'a Value,
    found: &mut BTreeMap<&'a str, &'a Value>,
    budget: &Budget,
    depth: u64,
) -> Result<()> {
    cap(depth, 32, "Avro schema depth")?;
    budget.items(1)?;
    match schema {
        Value::Array(a) => {
            for item in a {
                names(item, found, budget, depth + 1)?;
            }
        }
        Value::Object(o) => {
            if o.contains_key("namespace") {
                return Err(fault(
                    FaultCode::UnsupportedFormat,
                    "Namespaced Avro outside local format v1",
                ));
            }
            if let Some(name) = o.get("name").and_then(Value::as_str) {
                corrupt(
                    name.bytes().enumerate().all(|(index, c)| {
                        c.is_ascii_alphabetic() || c == b'_' || (index > 0 && c.is_ascii_digit())
                    }) && !name.is_empty(),
                    "Invalid Avro name",
                )?;
                corrupt(
                    !name.contains('.') && !found.contains_key(name),
                    "Ambiguous Avro named type",
                )?;
                found.insert(name, schema);
            }
            if let Some(a) = o.get("fields").and_then(Value::as_array) {
                for field in a {
                    names(&field["type"], found, budget, depth + 1)?;
                }
            }
            for key in ["items", "values"] {
                if let Some(s) = o.get(key) {
                    names(s, found, budget, depth + 1)?;
                }
            }
        }
        _ => {}
    }
    Ok(())
}
fn avro_value(
    schema: &Value,
    registry: &BTreeMap<&str, &Value>,
    c: &mut Cursor<'_>,
    budget: &Budget,
    depth: u64,
) -> Result<()> {
    cap(depth, 32, "Avro value depth")?;
    budget.items(1)?;
    if let Some(branches) = schema.as_array() {
        let branch = c.signed()?;
        corrupt(
            branch >= 0 && (branch as usize) < branches.len(),
            "Invalid Avro union",
        )?;
        return avro_value(&branches[branch as usize], registry, c, budget, depth + 1);
    }
    let name = schema
        .as_str()
        .or_else(|| schema["type"].as_str())
        .ok_or_else(|| fault(FaultCode::CorruptData, "Invalid Avro schema type"))?;
    match name {
        "null" => {}
        "boolean" => {
            corrupt(c.byte()? <= 1, "Invalid Avro bool")?;
        }
        "int" | "long" => {
            c.signed()?;
        }
        "float" => {
            c.take(4)?;
        }
        "double" => {
            c.take(8)?;
        }
        "bytes" | "string" => {
            c.blob(budget)?;
        }
        "fixed" => {
            let n = schema["size"]
                .as_u64()
                .ok_or_else(|| fault(FaultCode::CorruptData, "Invalid Avro fixed size"))?;
            cap(n, budget.limits.metadata_bytes, "Avro fixed bytes")?;
            c.take(usize::try_from(n)?)?;
        }
        "enum" => {
            let n = c.signed()?;
            corrupt(
                n >= 0 && (n as usize) < schema["symbols"].as_array().map_or(0, Vec::len),
                "Invalid Avro enum",
            )?;
        }
        "record" => {
            let fields = schema["fields"]
                .as_array()
                .ok_or_else(|| fault(FaultCode::CorruptData, "Invalid Avro record"))?;
            for field in fields {
                avro_value(&field["type"], registry, c, budget, depth + 1)?;
            }
        }
        "array" | "map" => loop {
            let mut n = c.signed()?;
            if n == 0 {
                break;
            }
            let end = if n < 0 {
                n = n
                    .checked_neg()
                    .ok_or_else(|| fault(FaultCode::CorruptData, "Invalid Avro count"))?;
                let size = c.signed()?;
                corrupt(
                    size >= 0 && size as usize <= c.bytes.len() - c.pos,
                    "Invalid Avro block size",
                )?;
                Some(c.pos + size as usize)
            } else {
                None
            };
            cap(n as u64, budget.limits.items, "Avro collection count")?;
            for _ in 0..n {
                if name == "map" {
                    c.blob(budget)?;
                }
                avro_value(
                    &schema[if name == "map" { "values" } else { "items" }],
                    registry,
                    c,
                    budget,
                    depth + 1,
                )?;
            }
            if let Some(end) = end {
                corrupt(c.pos == end, "Avro block length mismatch")?;
            }
        },
        reference => {
            let target = registry
                .get(reference)
                .ok_or_else(|| fault(FaultCode::CorruptData, "Unknown Avro named type"))?;
            avro_value(target, registry, c, budget, depth + 1)?;
        }
    }
    Ok(())
}

fn avro(bytes: &[u8], budget: &Budget) -> Result<()> {
    let mut c = Cursor::new(bytes);
    corrupt(c.take(4)? == b"Obj\x01", "Invalid Avro magic")?;
    let mut schema = None;
    loop {
        let mut n = c.signed()?;
        if n == 0 {
            break;
        }
        if n < 0 {
            n = n
                .checked_neg()
                .ok_or_else(|| fault(FaultCode::CorruptData, "Invalid Avro header count"))?;
            let size = c.signed()?;
            corrupt(
                size >= 0 && size as usize <= bytes.len() - c.pos,
                "Invalid Avro header size",
            )?;
        }
        cap(n as u64, 64, "Avro header fields")?;
        for _ in 0..n {
            let key = c.blob(budget)?;
            let value = c.blob(budget)?;
            if key == b"avro.codec" && value != b"null" {
                return Err(fault(
                    FaultCode::UnsupportedFormat,
                    "Compressed Avro is outside local read format v1",
                ));
            }
            if key == b"avro.schema" {
                corrupt(schema.is_none(), "Duplicate Avro schema")?;
                json(value, budget)?;
                schema = Some(
                    serde_json::from_slice::<Value>(value)
                        .map_err(|e| fault(FaultCode::CorruptData, e.to_string()))?,
                );
            }
        }
    }
    let marker = c.take(16)?;
    let schema = schema.ok_or_else(|| fault(FaultCode::CorruptData, "Missing Avro schema"))?;
    let mut registry = BTreeMap::new();
    annotations(&schema)?;
    names(&schema, &mut registry, budget, 0)?;
    while c.pos < bytes.len() {
        let n = c.signed()?;
        corrupt(n > 0, "Invalid Avro object count")?;
        cap(n as u64, budget.limits.items, "Avro object count")?;
        let block = c.blob(budget)?;
        let mut rows = Cursor::new(block);
        for _ in 0..n {
            avro_value(&schema, &registry, &mut rows, budget, 0)?;
        }
        corrupt(
            rows.pos == block.len() && c.take(16)? == marker,
            "Invalid Avro block framing",
        )?;
    }
    Ok(())
}

pub(super) fn file(bytes: Bytes, path: &std::path::Path, budget: &Budget) -> Result<()> {
    if bytes.starts_with(b"\x1f\x8b") {
        return Err(fault(
            FaultCode::UnsupportedFormat,
            "Compressed JSON metadata is outside local read format v1",
        ));
    }
    if bytes.starts_with(b"PAR1") || path.extension().is_some_and(|s| s == "parquet") {
        parquet(bytes, budget)
    } else if bytes.starts_with(b"Obj\x01") || path.extension().is_some_and(|s| s == "avro") {
        avro(&bytes, budget)
    } else {
        json(&bytes, budget)
    }
}

#[cfg(test)]
mod tests {
    use super::super::bounds::{Fault, Limits};
    use super::*;

    fn long(value: i64) -> Vec<u8> {
        let mut value = ((value as u64) << 1) ^ ((value >> 63) as u64);
        let mut bytes = Vec::new();
        while value >= 128 {
            bytes.push((value as u8 & 127) | 128);
            value >>= 7;
        }
        bytes.push(value as u8);
        bytes
    }
    fn blob(bytes: &[u8]) -> Vec<u8> {
        let mut result = long(bytes.len() as i64);
        result.extend(bytes);
        result
    }
    fn parquet_envelope(footer: &[u8]) -> Bytes {
        let mut bytes = b"PAR1".to_vec();
        bytes.extend(footer);
        bytes.extend((footer.len() as u32).to_le_bytes());
        bytes.extend(b"PAR1");
        bytes.into()
    }

    #[test]
    fn schema_scalar_and_collection_bombs_fail_before_library_decode() {
        // FileMetaData.schema: list<SchemaElement>, root claims huge children.
        let mut footer = vec![0x29, 0x2c, 0x55];
        footer.extend(long(i32::MAX as i64));
        footer.extend([0, 0, 0]);
        for footer in [footer, vec![0x29, 0xfc, 0xff, 0xff, 0xff, 0xff, 0x07]] {
            let budget = Budget::new(Limits::default());
            assert!(
                parquet(parquet_envelope(&footer), &budget)
                    .unwrap_err()
                    .downcast_ref::<Fault>()
                    .is_some()
            );
            assert_eq!(budget.usage().decodes, 0);
        }
    }

    #[test]
    fn avro_array_claim_and_compression_rejected_before_decoder() {
        let schema = br#"{"type":"array","items":"null"}"#;
        let mut header = b"Obj\x01".to_vec();
        header.extend(long(1));
        header.extend(blob(b"avro.schema"));
        header.extend(blob(schema));
        header.extend(long(0));
        header.extend([0; 16]);
        let block = long(i32::MAX as i64);
        header.extend(long(1));
        header.extend(blob(&block));
        header.extend([0; 16]);
        let budget = Budget::new(Limits::default());
        assert_eq!(
            avro(&header, &budget)
                .unwrap_err()
                .downcast_ref::<Fault>()
                .unwrap()
                .code,
            FaultCode::ResourceLimit
        );
        assert_eq!(budget.usage().decodes, 0);
        let mut compressed = b"Obj\x01".to_vec();
        compressed.extend(long(1));
        compressed.extend(blob(b"avro.codec"));
        compressed.extend(blob(b"deflate"));
        assert_eq!(
            avro(&compressed, &budget)
                .unwrap_err()
                .downcast_ref::<Fault>()
                .unwrap()
                .code,
            FaultCode::UnsupportedFormat
        );
    }

    #[test]
    fn footer_and_json_depth_rejected_before_decode() {
        let budget = Budget::new(Limits::default());
        let mut bytes = b"PAR1".to_vec();
        bytes.extend(u32::MAX.to_le_bytes());
        bytes.extend(b"PAR1");
        assert_eq!(
            parquet(bytes.into(), &budget)
                .unwrap_err()
                .downcast_ref::<Fault>()
                .unwrap()
                .code,
            FaultCode::ResourceLimit
        );
        assert!(json(&[b'['; 33], &budget).is_err());
        assert_eq!(budget.usage().decodes, 0);
    }

    #[test]
    fn unsafe_delta_encoding_rejected_before_value_decoder() {
        // PageHeader.data_page_header { num_values: 1, encoding: DELTA_LENGTH_BYTE_ARRAY }
        let bytes = [0x5c, 0x15, 2, 0x15, 12, 0, 0];
        let budget = Budget::new(Limits::default());
        assert_eq!(
            compact_struct(&mut Cursor::new(&bytes), 0, &budget, true)
                .unwrap_err()
                .downcast_ref::<Fault>()
                .unwrap()
                .code,
            FaultCode::UnsupportedFormat
        );
        assert_eq!(budget.usage().decoded_rows, 0);
    }

    #[test]
    fn wrong_known_wire_type_cannot_smuggle_child_allocation_claim() {
        let mut footer = vec![0x29, 0x2c, 0x48, 1, b'r', 0x15, 2, 0x28, 7];
        footer.extend([0x05, 0x0a, 0xfe, 0xff, 0xff, 0xff, 0x0f, 0]);
        footer.extend([0x15, 4, 0x25, 0, 0x18, 1, b'c', 0, 0]);
        let budget = Budget::new(Limits::default());
        assert_eq!(
            parquet(parquet_envelope(&footer), &budget)
                .unwrap_err()
                .downcast_ref::<Fault>()
                .unwrap()
                .code,
            FaultCode::CorruptData
        );
        assert_eq!(budget.usage().decodes, 0);
    }

    #[test]
    fn avro_named_types_cannot_shadow_qualified_references() -> Result<()> {
        let budget = Budget::new(Limits::default());
        let schema: Value = serde_json::from_str(
            r#"[{"type":"record","name":"x","namespace":"A","fields":[]},{"type":"record","name":"x","namespace":"B","fields":[]}]"#,
        )?;
        assert!(names(&schema, &mut BTreeMap::new(), &budget, 0).is_err());
        let schema: Value = serde_json::from_str(
            r#"[{"type":"record","name":"x","fields":[]},{"type":"record","name":"x","fields":[]}]"#,
        )?;
        assert!(names(&schema, &mut BTreeMap::new(), &budget, 0).is_err());
        assert_eq!(budget.usage().decodes, 0);
        Ok(())
    }

    #[test]
    fn invalid_avro_names_fail_before_library_parser() -> Result<()> {
        for name in ["bad-name", "", "7name", "é", "a.b"] {
            let budget = Budget::new(Limits::default());
            let schema = serde_json::json!({"type":"record","name":name,"fields":[]});
            assert_eq!(
                names(&schema, &mut BTreeMap::new(), &budget, 0)
                    .unwrap_err()
                    .downcast_ref::<Fault>()
                    .unwrap()
                    .code,
                FaultCode::CorruptData
            );
            assert_eq!(budget.usage().decodes, 0);
        }
        Ok(())
    }

    #[test]
    fn avro_logical_types_and_aliases_cannot_change_payload_framing() -> Result<()> {
        for schema in [
            r#"{"type":"bytes","logicalType":"big-decimal"}"#,
            r#"{"type":"fixed","name":"x","size":16,"logicalType":"uuid"}"#,
            r#"{"type":"record","name":"x","aliases":["y"],"fields":[]}"#,
            r#"{"type":"record","name":"x","fields":[{"name":"a","type":"bytes","logicalType":"big-decimal"}]}"#,
        ] {
            let budget = Budget::new(Limits::default());
            let mut bytes = b"Obj\x01".to_vec();
            bytes.extend(long(1));
            bytes.extend(blob(b"avro.schema"));
            bytes.extend(blob(schema.as_bytes()));
            bytes.extend(long(0));
            bytes.extend([0; 16]);
            assert_eq!(
                avro(&bytes, &budget)
                    .unwrap_err()
                    .downcast_ref::<Fault>()
                    .unwrap()
                    .code,
                FaultCode::UnsupportedFormat
            );
            assert_eq!(budget.usage().decodes, 0);
        }
        assert!(
            annotations(&serde_json::json!({"type":"array","items":"null","logicalType":"map"}))
                .is_ok()
        );
        Ok(())
    }

    #[test]
    fn understated_file_rows_cannot_hide_row_groups() -> Result<()> {
        let cut = crate::store_tests::cut();
        let relation = &cut.relations["label"];
        let batch = relation.schema.batch("cut", &relation.rows)?;
        let mut bytes = super::super::encode_batch(&batch)?;
        let budget = Budget::new(Limits::default());
        let footer = super::super::bounds::parquet_footer(&bytes, &budget)?;
        let mut c = Cursor::new(&bytes[footer.clone()]);
        let mut last = 0;
        let at = loop {
            let h = c.byte()?;
            assert_ne!(h, 0);
            let id = if h >> 4 == 0 {
                c.signed()?
            } else {
                last + (h >> 4) as i64
            };
            last = id;
            if id == 3 {
                break c.pos;
            }
            compact_value(&mut c, h & 15, true, 0, &budget)?;
        };
        bytes[footer.start + at] = 0;
        assert_eq!(
            parquet(bytes.into(), &budget)
                .unwrap_err()
                .downcast_ref::<Fault>()
                .unwrap()
                .code,
            FaultCode::CorruptData
        );
        assert_eq!(budget.usage().decoded_rows, 0);
        Ok(())
    }
}
