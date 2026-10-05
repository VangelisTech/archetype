//! Strict wire grammar for the pinned local Parquet writer. The upstream
//! decoder reads known fields by their declared type, not their wire tag;
//! validating both prevents a binary payload being reinterpreted as metadata.
use super::{Budget, Cursor, FaultCode, cap, corrupt, fault};
use anyhow::Result;

#[derive(Clone, Copy)]
enum Shape {
    Scalar(u8),
    Struct(&'static str),
    List(u8, &'static str, u64),
}

fn shape(context: &str, id: i64) -> Option<Shape> {
    use Shape::*;
    Some(match (context, id) {
        ("file", 1) => Scalar(5),
        ("file", 2) => List(12, "schema", 257),
        ("file", 3) => Scalar(6),
        ("file", 4) => List(12, "group", 1024),
        ("file", 5) => List(12, "key", 256),
        ("file", 6) => Scalar(8),
        ("file", 7) => List(12, "order", 256),
        ("schema", 1..=3 | 5..=9) => Scalar(5),
        ("schema", 4) => Scalar(8),
        ("schema", 10) => Struct("logical"),
        ("logical", 1 | 2 | 3 | 4 | 6 | 11 | 12 | 13 | 14 | 15) => Struct("empty"),
        ("logical", 5) => Struct("decimal"),
        ("logical", 7 | 8) => Struct("time"),
        ("logical", 10) => Struct("integer"),
        ("decimal", 1 | 2) => Scalar(5),
        ("time", 1) => Scalar(1),
        ("time", 2) => Struct("unit"),
        ("unit", 1..=3) => Struct("empty"),
        ("integer", 1) => Scalar(3),
        ("integer", 2) => Scalar(1),
        ("group", 1) => List(12, "chunk", 256),
        ("group", 2 | 3 | 5 | 6) => Scalar(6),
        ("group", 4) => List(12, "sort", 256),
        ("group", 7) => Scalar(4),
        ("sort", 1) => Scalar(5),
        ("sort", 2 | 3) => Scalar(1),
        ("chunk", 1) => Scalar(8),
        ("chunk", 2 | 4 | 6) => Scalar(6),
        ("chunk", 3) => Struct("column"),
        ("chunk", 5 | 7) => Scalar(5),
        ("column", 1 | 4 | 15) => Scalar(5),
        ("column", 2) => List(5, "", 16),
        ("column", 3) => List(8, "", 1),
        ("column", 5..=7 | 9..=11 | 14) => Scalar(6),
        ("column", 8) => List(12, "key", 256),
        ("column", 12) => Struct("stats"),
        ("column", 13) => List(12, "encoding", 16),
        ("column", 16) => Struct("size"),
        ("key", 1 | 2) => Scalar(8),
        ("order", 1) => Struct("empty"),
        ("stats", 1 | 2 | 5 | 6) => Scalar(8),
        ("stats", 3 | 4) => Scalar(6),
        ("stats", 7 | 8) => Scalar(1),
        ("encoding", 1..=3) => Scalar(5),
        ("size", 1) => Scalar(6),
        ("size", 2 | 3) => List(6, "", 2),
        ("page", 1..=4) => Scalar(5),
        ("page", 5) => Struct("data"),
        ("page", 7) => Struct("dictionary"),
        ("data", 1..=4) => Scalar(5),
        ("data", 5) => Struct("stats"),
        ("dictionary", 1 | 2) => Scalar(5),
        ("dictionary", 3) => Scalar(1),
        ("data_v2", 1..=6) => Scalar(5),
        ("data_v2", 7) => Scalar(1),
        ("data_v2", 8) => Struct("stats"),
        _ => return None,
    })
}

fn scalar(c: &mut Cursor<'_>, kind: u8, budget: &Budget) -> Result<()> {
    match kind {
        1 => {} // Boolean value is carried by the field header.
        3 => {
            c.take(1)?;
        }
        4..=6 => {
            let value = c.signed()?;
            corrupt(
                match kind {
                    4 => i16::try_from(value).is_ok(),
                    5 => i32::try_from(value).is_ok(),
                    _ => true,
                },
                "Thrift scalar overflow",
            )?;
        }
        8 => {
            let count = c.unsigned()?;
            cap(count, budget.limits.metadata_bytes, "Thrift binary bytes")?;
            c.take(usize::try_from(count)?)?;
        }
        _ => {
            return Err(fault(
                FaultCode::UnsupportedFormat,
                "Unsupported local Thrift scalar",
            ));
        }
    }
    Ok(())
}

pub(super) fn structure(
    c: &mut Cursor<'_>,
    context: &'static str,
    budget: &Budget,
    depth: u64,
) -> Result<()> {
    cap(depth, 32, "Thrift depth")?;
    let mut last = 0i64;
    let mut fields = 0;
    let mut page_headers = 0;
    loop {
        let h = c.byte()?;
        if h == 0 {
            if matches!(context, "logical" | "unit" | "order") {
                corrupt(fields == 1, "Empty Thrift union")?;
            }
            return Ok(());
        }
        let id = if h >> 4 == 0 {
            c.signed()?
        } else {
            last + (h >> 4) as i64
        };
        corrupt(
            id > last && id <= 32767,
            "Unordered or duplicate Thrift field",
        )?;
        last = id;
        fields += 1;
        if matches!(context, "logical" | "unit" | "order") {
            corrupt(fields == 1, "Ambiguous Thrift union")?;
        }
        if context == "page" && id >= 5 {
            page_headers += 1;
            corrupt(page_headers == 1, "Ambiguous page header")?;
        }
        budget.items(1)?;
        let shape = shape(context, id).ok_or_else(|| {
            fault(
                FaultCode::UnsupportedFormat,
                format!("Unsupported local Parquet field {context}.{id}"),
            )
        })?;
        let wire = h & 15;
        match shape {
            Shape::Scalar(expected) => {
                corrupt(
                    wire == expected || (expected == 1 && wire == 2),
                    "Thrift scalar wire type mismatch",
                )?;
                scalar(c, expected, budget)?;
            }
            Shape::Struct(nested) => {
                corrupt(wire == 12, "Thrift struct wire type mismatch")?;
                structure(c, nested, budget, depth + 1)?;
            }
            Shape::List(expected, nested, limit) => {
                corrupt(wire == 9, "Thrift list wire type mismatch")?;
                let h = c.byte()?;
                corrupt(h & 15 == expected, "Thrift list element type mismatch")?;
                let count = if h >> 4 == 15 {
                    c.unsigned()?
                } else {
                    (h >> 4) as u64
                };
                cap(count, limit, "Thrift typed list count")?;
                budget.items(count)?;
                for _ in 0..count {
                    if expected == 12 {
                        structure(c, nested, budget, depth + 1)?;
                    } else {
                        scalar(c, expected, budget)?;
                    }
                }
            }
        }
    }
}
