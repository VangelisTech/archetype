use std::sync::Arc;

use arrow_array::{Array, Float64Array, RecordBatch};
use arrow_schema::ArrowError;

use crate::{Processor, ProcessorContext, Result};

pub const MOVEMENT_REQUIRED_COLUMNS: &[&str] =
    &["position__x", "position__y", "velocity__dx", "velocity__dy"];

pub struct MovementProcessor {
    pub dt: f64,
}

impl Processor for MovementProcessor {
    fn name(&self) -> &str {
        "movement"
    }

    fn priority(&self) -> i32 {
        10
    }

    fn required_columns(&self) -> &[&'static str] {
        MOVEMENT_REQUIRED_COLUMNS
    }

    fn process(&self, batch: RecordBatch, _ctx: &ProcessorContext) -> Result<RecordBatch> {
        process_movement_batch(batch, self.dt)
    }
}

pub fn process_movement_batch(batch: RecordBatch, dt: f64) -> Result<RecordBatch> {
    let schema = batch.schema();
    let x = float64_column(&batch, "position__x")?;
    let y = float64_column(&batch, "position__y")?;
    let dx = float64_column(&batch, "velocity__dx")?;
    let dy = float64_column(&batch, "velocity__dy")?;

    let next_x = (0..batch.num_rows())
        .map(|idx| x.value(idx) + dx.value(idx) * dt)
        .collect::<Vec<_>>();
    let next_y = (0..batch.num_rows())
        .map(|idx| y.value(idx) + dy.value(idx) * dt)
        .collect::<Vec<_>>();

    let mut cols = batch.columns().to_vec();
    cols[schema.index_of("position__x")?] = Arc::new(Float64Array::from(next_x));
    cols[schema.index_of("position__y")?] = Arc::new(Float64Array::from(next_y));
    RecordBatch::try_new(schema, cols).map_err(Into::into)
}

fn float64_column<'a>(batch: &'a RecordBatch, name: &'static str) -> Result<&'a Float64Array> {
    let schema = batch.schema();
    batch
        .column(schema.index_of(name)?)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| ArrowError::CastError(format!("{name} must be Float64")).into())
}

#[cfg(test)]
mod tests {
    use arrow_array::Float64Array;
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn movement_updates_position_columns() {
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

        let out = process_movement_batch(batch, 1.0).unwrap();
        let x = float64_column(&out, "position__x").unwrap();
        let y = float64_column(&out, "position__y").unwrap();

        assert_eq!(x.value(0), 2.0);
        assert_eq!(x.value(1), 13.0);
        assert_eq!(y.value(0), 0.5);
        assert_eq!(y.value(1), 3.5);
    }
}
