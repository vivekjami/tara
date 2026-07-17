//! `h3_cell_res5` — DataFusion scalar UDF computing the H3 resolution-5 cell
//! index for a given (latitude, longitude) pair, using `h3o`.
//!
//! Resolution 5 matches the resolution already used for `ChunkMeta.h3_cells`
//! (see tara-store::chunk), so results here are directly comparable to
//! chunk-level spatial pruning.
//!
//! ## DataFusion 54 note
//! Confirmed against the actual compiler (datafusion-expr 54.0.0):
//! `ScalarUDFImpl` does NOT have `as_any` — same trait-upcasting change as
//! `TableProvider`/`ExecutionPlan`. It DOES require `Eq + Hash` (via the
//! `DynEq`/`DynHash` supertrait bounds on `ScalarUDFImpl`), which
//! `TableProvider` does not require — so this is not a uniform rule across
//! DF54 traits, it's per-trait, and worth checking each time rather than
//! assumed from one prior example.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Float64Array, UInt64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DFResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use h3o::{LatLng, Resolution};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct H3CellRes5 {
    signature: Signature,
}

impl H3CellRes5 {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Float64, DataType::Float64],
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for H3CellRes5 {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for H3CellRes5 {
    fn name(&self) -> &str {
        "h3_cell_res5"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::UInt64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let arrays: Vec<ArrayRef> = ColumnarValue::values_to_arrays(&args.args)?;

        let lat = arrays[0]
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("h3_cell_res5 arg 0 must be Float64 (latitude)");
        let lon = arrays[1]
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("h3_cell_res5 arg 1 must be Float64 (longitude)");

        let cells: UInt64Array = lat
            .iter()
            .zip(lon.iter())
            .map(|(la, lo)| match (la, lo) {
                (Some(la), Some(lo)) => LatLng::new(la, lo)
                    .ok()
                    .map(|ll| u64::from(ll.to_cell(Resolution::Five))),
                _ => None,
            })
            .collect();

        Ok(ColumnarValue::Array(Arc::new(cells)))
    }
}
