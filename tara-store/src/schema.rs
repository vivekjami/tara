use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use std::sync::Arc;

/// The canonical Arrow schema for a cleaned AIS position report.
/// Every field maps directly to a column in the raw CSV, with three exceptions:
///   - Timestamp is parsed from "DD/MM/YYYY HH:MM:SS" into microseconds since Unix epoch UTC
///   - Invalid positions (lat=91.0) are filtered out before this schema is ever written
///   - Duplicates on (mmsi, timestamp_us) are dropped — first occurrence wins
pub fn vessel_position_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        // Identity — never null, every valid row has these
        Field::new("mmsi", DataType::UInt32, false),
        Field::new(
            "timestamp_us",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        // Position — never null after filtering (lat=91 rows are dropped)
        Field::new("latitude", DataType::Float64, false),
        Field::new("longitude", DataType::Float64, false),
        // Kinematics — nullable: Class B vessels often omit these
        Field::new("sog", DataType::Float32, true), // speed over ground, knots
        Field::new("cog", DataType::Float32, true), // course over ground, degrees
        Field::new("heading", DataType::UInt16, true), // true heading, 0-359; 511=unavailable
        // Classification — nullable: not all message types carry these
        Field::new("mobile_type", DataType::Utf8, false), // "Class A", "Class B"
        Field::new("nav_status", DataType::Utf8, true),
        Field::new("ship_type", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, true),
    ]))
}
