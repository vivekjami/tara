#!/usr/bin/env python3
"""
Phase 0: Ground truth inspection of the Danish AIS dataset.
Run this before writing any Rust code. Its job is to tell you the
real shape of the data so every architectural decision downstream
is grounded in measurement, not assumption.

Usage: python3 scripts/phase0_inspect.py data/ais_raw.csv
"""

import sys
import csv
from collections import defaultdict, Counter
from datetime import datetime

def parse_float(s):
    try:
        return float(s)
    except (ValueError, TypeError):
        return None

def parse_timestamp(s):
    for fmt in ("%d/%m/%Y %H:%M:%S", "%m/%d/%Y %H:%M:%S"):
        try:
            return datetime.strptime(s.strip(), fmt)
        except ValueError:
            continue
    return None

def percentile(sorted_list, p):
    if not sorted_list:
        return None
    idx = int(len(sorted_list) * p / 100)
    return sorted_list[min(idx, len(sorted_list)-1)]

def main(path):
    stats = {
        "total_rows": 0,
        "duplicate_rows": 0,
        "invalid_position_rows": 0,
        "mobile_type_counts": Counter(),
        "nav_status_counts": Counter(),
        "ship_type_counts": Counter(),
        "distinct_mmsi": set(),
        "base_station_mmsi": set(),
        "rows_with_sog": 0,
        "rows_with_name": 0,
        "rows_with_ship_type": 0,
        "rows_with_heading": 0,
    }

    mmsi_timestamps = defaultdict(list)
    seen_keys = set()

    print(f"Reading {path}...")

    with open(path, newline="", errors="replace") as f:
        # Handle the leading '# ' on the header line
        first_line = f.readline().lstrip("# ").strip()
        fieldnames = [h.strip() for h in first_line.split(",")]
        reader = csv.DictReader(f, fieldnames=fieldnames)

        for row in reader:
            stats["total_rows"] += 1

            mmsi = row.get("MMSI", "").strip()
            ts_str = row.get("Timestamp", "").strip()
            lat_str = row.get("Latitude", "").strip()
            lon_str = row.get("Longitude", "").strip()
            mobile_type = row.get("Type of mobile", "").strip()
            nav_status = row.get("Navigational status", "").strip()
            ship_type = row.get("Ship type", "").strip()
            sog_str = row.get("SOG", "").strip()
            heading_str = row.get("Heading", "").strip()
            name = row.get("Name", "").strip()

            # Dedup
            dedup_key = (mmsi, ts_str)
            if dedup_key in seen_keys:
                stats["duplicate_rows"] += 1
            else:
                seen_keys.add(dedup_key)

            stats["mobile_type_counts"][mobile_type] += 1
            if mobile_type == "Base Station":
                stats["base_station_mmsi"].add(mmsi)

            stats["nav_status_counts"][nav_status] += 1
            if ship_type and ship_type not in ("Undefined", "Unknown", ""):
                stats["ship_type_counts"][ship_type] += 1
                stats["rows_with_ship_type"] += 1

            # Position validity — AIS sentinel is lat=91.0
            lat = parse_float(lat_str)
            lon = parse_float(lon_str)
            invalid_pos = (lat is None or lon is None or abs(lat) > 90.0)

            if invalid_pos:
                stats["invalid_position_rows"] += 1

            if mmsi and mobile_type != "Base Station" and not invalid_pos:
                stats["distinct_mmsi"].add(mmsi)
                ts = parse_timestamp(ts_str)
                if ts:
                    mmsi_timestamps[mmsi].append(ts)

            if sog_str:
                stats["rows_with_sog"] += 1
            if heading_str:
                stats["rows_with_heading"] += 1
            if name and name not in ("Unknown", ""):
                stats["rows_with_name"] += 1

    print("Computing gap distribution...")
    all_gaps = []
    for mmsi, timestamps in mmsi_timestamps.items():
        if len(timestamps) < 2:
            continue
        timestamps.sort()
        for i in range(len(timestamps) - 1):
            all_gaps.append((timestamps[i+1] - timestamps[i]).total_seconds())
    all_gaps.sort()
    n = len(all_gaps)
    total = stats["total_rows"]

    print("\n" + "="*60)
    print("PHASE 0: DATASET GROUND TRUTH")
    print("="*60)

    print(f"\n--- Volume ---")
    print(f"  Total rows:              {total:,}")
    print(f"  Duplicate rows:          {stats['duplicate_rows']:,}  ({100*stats['duplicate_rows']/max(total,1):.1f}%)")
    print(f"  Invalid position rows:   {stats['invalid_position_rows']:,}  ({100*stats['invalid_position_rows']/max(total,1):.1f}%)")
    print(f"  Distinct vessel MMSIs:   {len(stats['distinct_mmsi']):,}")
    print(f"  Base station MMSIs:      {len(stats['base_station_mmsi']):,}")

    print(f"\n--- Mobile Types ---")
    for k, v in stats["mobile_type_counts"].most_common():
        print(f"  {k:<30} {v:,}")

    print(f"\n--- Field Completeness ---")
    print(f"  SOG present:             {stats['rows_with_sog']:,} / {total:,}  ({100*stats['rows_with_sog']/max(total,1):.1f}%)")
    print(f"  Heading present:         {stats['rows_with_heading']:,} / {total:,}  ({100*stats['rows_with_heading']/max(total,1):.1f}%)")
    print(f"  Name present:            {stats['rows_with_name']:,} / {total:,}  ({100*stats['rows_with_name']/max(total,1):.1f}%)")
    print(f"  Ship type (non-Unknown): {stats['rows_with_ship_type']:,} / {total:,}  ({100*stats['rows_with_ship_type']/max(total,1):.1f}%)")

    print(f"\n--- Top Navigational Statuses ---")
    for k, v in stats["nav_status_counts"].most_common(8):
        print(f"  {k:<40} {v:,}")

    print(f"\n--- Top Ship Types ---")
    for k, v in stats["ship_type_counts"].most_common(10):
        print(f"  {k:<40} {v:,}")

    print(f"\n--- Inter-report Gap Distribution ---")
    if all_gaps:
        print(f"  Total gap intervals:     {n:,}")
        print(f"  Min gap:                 {all_gaps[0]:.0f}s")
        print(f"  Median (p50):            {percentile(all_gaps, 50):.0f}s")
        print(f"  p90:                     {percentile(all_gaps, 90):.0f}s")
        print(f"  p95:                     {percentile(all_gaps, 95):.0f}s")
        print(f"  p99:                     {percentile(all_gaps, 99):.0f}s")
        print(f"  Max gap:                 {all_gaps[-1]:.0f}s  ({all_gaps[-1]/3600:.1f}h)")
        over_6h = sum(1 for g in all_gaps if g > 21600)
        print(f"  Gaps > 6 hours:          {over_6h:,}  (candidate dark-vessel events)")
        p50 = percentile(all_gaps, 50)
        print(f"\n--- Chunk Size Recommendation ---")
        if p50 < 60:
            print(f"  Median gap {p50:.0f}s → use 1-hour time buckets")
        elif p50 < 600:
            print(f"  Median gap {p50:.0f}s → use 6-hour time buckets")
        else:
            print(f"  Median gap {p50:.0f}s → use 24-hour time buckets")
    else:
        print("  Insufficient data for gap analysis in this sample.")
        print("  Run on full dataset for meaningful chunk sizing.")

    print(f"\n--- Arrow Schema Notes ---")
    print(f"  Timestamp format:  DD/MM/YYYY HH:MM:SS → parse to UTC microseconds")
    print(f"  Invalid position:  lat=91.0 is AIS sentinel → filter before Arrow write")
    print(f"  Dedup key:         (MMSI, Timestamp) → deduplicate at ingest boundary")
    print(f"  Nullable columns:  SOG, COG, Heading, Name, Ship type, IMO, Width, Length, Draught")
    print(f"  Exclude:           Base Station rows — not vessel trajectories")
    print("="*60)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 scripts/phase0_inspect.py <path_to_ais_csv>")
        sys.exit(1)
    main(sys.argv[1])
