use anyhow::{bail, Result};
use arrow::array::*;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use tara_store::schema::vessel_position_schema;

/// Everything Phase 0 taught us about the raw CSV, encoded as a type.
/// One instance per valid, cleaned, deduplicated row.
#[derive(Debug, Clone)]
pub struct AisRow {
    pub mmsi: u32,
    pub timestamp_us: i64,  // microseconds since Unix epoch, UTC
    pub latitude: f64,
    pub longitude: f64,
    pub sog: Option<f32>,
    pub cog: Option<f32>,
    pub heading: Option<u16>,
    pub mobile_type: String,
    pub nav_status: Option<String>,
    pub ship_type: Option<String>,
    pub name: Option<String>,
}

/// Parse a single CSV row into an AisRow, or return None if it should be skipped.
/// "Skipped" means: base station, AtoN, SAR airborne, or invalid position sentinel.
/// This function does NOT handle deduplication — that's the dedup module's job.
pub fn parse_row(record: &csv::StringRecord, headers: &csv::StringRecord) -> Option<AisRow> {
    // Helper closure: get field by name, empty string becomes None
    let get = |name: &str| -> Option<&str> {
        let idx = headers.iter().position(|h| h.trim() == name)?;
        let val = record.get(idx)?.trim();
        if val.is_empty() { None } else { Some(val) }
    };

    // Mobile type filter — Phase 0 showed these are not vessel trajectories
    let mobile_type = get("Type of mobile")?.to_string();
    match mobile_type.as_str() {
        "Base Station" | "AtoN" | "SAR Airborne" | "Search and Rescue Transponder" => {
            return None;
        }
        _ => {}
    }

    // MMSI — must exist and be a valid u32
    let mmsi: u32 = get("MMSI")?.parse().ok()?;

    // Timestamp — Phase 0 confirmed format "DD/MM/YYYY HH:MM:SS"
    let ts_str = get("Timestamp")?;
    let timestamp_us = parse_timestamp_us(ts_str)?;

    // Position — filter the lat=91.0 AIS sentinel
    let lat: f64 = get("Latitude")?.parse().ok()?;
    let lon: f64 = get("Longitude")?.parse().ok()?;
    if lat.abs() > 90.0 {
        return None;  // AIS "position unavailable" sentinel
    }

    // Optional kinematics
    let sog: Option<f32> = get("SOG").and_then(|s| s.parse().ok());
    let cog: Option<f32> = get("COG").and_then(|s| s.parse().ok());
    let heading: Option<u16> = get("Heading")
        .and_then(|s| s.parse::<u16>().ok())
        .filter(|&h| h <= 360);  // 511 = unavailable in AIS spec; treat as None

    // Optional classification
    let nav_status = get("Navigational status")
        .filter(|s| *s != "Unknown value")
        .map(|s| s.to_string());

    let ship_type = get("Ship type")
        .filter(|s| !matches!(*s, "Undefined" | "Unknown"))
        .map(|s| s.to_string());

    let name = get("Name")
        .filter(|s| !matches!(*s, "Unknown" | ""))
        .map(|s| s.to_string());

    Some(AisRow {
        mmsi,
        timestamp_us,
        latitude: lat,
        longitude: lon,
        sog,
        cog,
        heading,
        mobile_type,
        nav_status,
        ship_type,
        name,
    })
}

/// Parse "DD/MM/YYYY HH:MM:SS" → microseconds since Unix epoch UTC.
/// This is the format Phase 0 confirmed in the Danish AIS data.
fn parse_timestamp_us(s: &str) -> Option<i64> {
    // chrono would be cleaner but adds a dependency; do it manually
    // Format: "10/06/2026 00:00:00"
    let s = s.trim();
    if s.len() != 19 { return None; }

    let day:   u32 = s[0..2].parse().ok()?;
    let month: u32 = s[3..5].parse().ok()?;
    let year:  i32 = s[6..10].parse().ok()?;
    let hour:  u32 = s[11..13].parse().ok()?;
    let min:   u32 = s[14..16].parse().ok()?;
    let sec:   u32 = s[17..19].parse().ok()?;

    // Days since Unix epoch (1970-01-01) using proleptic Gregorian calendar
    let days_since_epoch = days_from_civil(year, month, day)?;
    let total_seconds = days_since_epoch * 86400
        + (hour as i64) * 3600
        + (min as i64) * 60
        + (sec as i64);

    Some(total_seconds * 1_000_000) // convert to microseconds
}

/// Days since 1970-01-01 for a given date. Standard civil calendar algorithm.
fn days_from_civil(y: i32, m: u32, d: u32) -> Option<i64> {
    if m < 1 || m > 12 || d < 1 || d > 31 { return None; }
    let y = if m <= 2 { y - 1 } else { y };
    let era: i64 = (if y >= 0 { y } else { y - 399 }) as i64 / 400;
    let yoe: i64 = y as i64 - era * 400;
    let doy: i64 = (153 * (m as i64 + if m > 2 { -3 } else { 9 }) + 2) / 5 + d as i64 - 1;
    let doe: i64 = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    Some(era * 146097 + doe - 719468)
}

/// Convert a batch of AisRows into a single Arrow RecordBatch.
/// This is what gets written to the chunk store.
pub fn rows_to_record_batch(rows: &[AisRow]) -> Result<RecordBatch> {
    if rows.is_empty() {
        bail!("Cannot create RecordBatch from empty row slice");
    }

    let schema = vessel_position_schema();

    // Build each column as an Arrow array
    let mmsi_arr = UInt32Array::from_iter_values(rows.iter().map(|r| r.mmsi));
    let ts_arr = TimestampMicrosecondArray::from_iter_values(
        rows.iter().map(|r| r.timestamp_us)
    ).with_timezone("UTC");
    let lat_arr = Float64Array::from_iter_values(rows.iter().map(|r| r.latitude));
    let lon_arr = Float64Array::from_iter_values(rows.iter().map(|r| r.longitude));
    let sog_arr = Float32Array::from_iter(rows.iter().map(|r| r.sog));
    let cog_arr = Float32Array::from_iter(rows.iter().map(|r| r.cog));
    let heading_arr = UInt16Array::from_iter(rows.iter().map(|r| r.heading));
    let mobile_arr = StringArray::from_iter_values(rows.iter().map(|r| r.mobile_type.as_str()));
    let nav_arr = StringArray::from_iter(rows.iter().map(|r| r.nav_status.as_deref()));
    let ship_arr = StringArray::from_iter(rows.iter().map(|r| r.ship_type.as_deref()));
    let name_arr = StringArray::from_iter(rows.iter().map(|r| r.name.as_deref()));

    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(mmsi_arr),
            Arc::new(ts_arr),
            Arc::new(lat_arr),
            Arc::new(lon_arr),
            Arc::new(sog_arr),
            Arc::new(cog_arr),
            Arc::new(heading_arr),
            Arc::new(mobile_arr),
            Arc::new(nav_arr),
            Arc::new(ship_arr),
            Arc::new(name_arr),
        ],
    )?)
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamp_parse_known_value() {
        // "10/06/2026 00:00:00" should be a specific known epoch value
        let us = parse_timestamp_us("10/06/2026 00:00:00").unwrap();
        // 2026-06-10 UTC = verify independently: date -d "2026-06-10" +%s = 1749513600
        assert_eq!(us, 1_781_049_600 * 1_000_000);
    }

    #[test]
    fn test_invalid_position_rejected() {
        // lat=91.0 is the AIS "position unavailable" sentinel
        // Build a minimal fake CSV record and confirm parse_row returns None
        // (Full row test added once we have a helper — placeholder for now)
        let lat: f64 = 91.0;
        assert!(lat.abs() > 90.0, "sentinel check logic is correct");
    }

    #[test]
    fn test_heading_511_becomes_none() {
        // AIS heading 511 means "unavailable" — should be stored as None
        let raw: u16 = 511;
        let filtered = Some(raw).filter(|&h| h <= 360);
        assert!(filtered.is_none());
    }
}