# ais-io

Plain-Java AIS ingestion pipeline (Phase 1–3). Reads NMEA sentences or pre-decoded CSV, validates records, writes partitioned GeoParquet, and builds voyage tracks.

## Quick start

```bash
# Build fat JAR
mvn package -q

# Run on a local NMEA file
java -jar target/ais-io-1.0-SNAPSHOT-jar-with-dependencies.jar \
  path/to/input.nmea data/output

# Run on a local CSV file
java -jar target/ais-io-1.0-SNAPSHOT-jar-with-dependencies.jar \
  path/to/input.csv data/output --format csv

# Download and ingest a day of DMA data directly
java -jar target/ais-io-1.0-SNAPSHOT-jar-with-dependencies.jar \
  2024-01-15 data/output --format dma
```

## Usage

```
java -jar ais-io.jar <input> <output-dir> [--format csv|nmea|dma] [--date YYYY-MM-DD] [--limit N]
```

| Argument | Description |
|---|---|
| `input` | Path to a CSV or NMEA file (plain or ZIP), or a date string (`YYYY-MM-DD`) when `--format dma` |
| `output-dir` | Root directory for all output |
| `--format` | `csv`, `nmea`, or `dma`. Auto-detected from file extension if omitted |
| `--date` | Fallback UTC date for NMEA messages without a tag-block timestamp. Defaults to today |
| `--limit N` | Process at most N input lines (0 = no limit). Useful for sampling large files |

`--format dma` downloads `http://aisdata.ais.dk/aisdk-<date>.zip` and extracts the CSV inside.

## Pipeline stages

```
Input (NMEA / CSV / DMA)
  │
  ├─ Step 1: Parse       NmeaParser / CsvParser
  ├─ Step 2: Validate    Validator  (drop bad MMSI, coords, SOG, duplicates)
  ├─ Step 3: Write       GeoParquetWriter  → positions/
  ├─ Step 4: Write       VesselMetadataWriter → vessels/
  └─ Step 5: Build+Write TrackBuilder + TrackWriter → tracks/
```

## Output layout

```
<output-dir>/
  positions/
    date=YYYY-MM-DD/
      h3_r3=<cell>/
        part-00000.parquet        ← GeoParquet 1.1.0, Snappy-compressed
  vessels/
    part-00000.parquet
  tracks/
    date=YYYY-MM/
      part-00000.parquet          ← GeoParquet, LineString geometries
```

Positions are partitioned by UTC date and H3 resolution-3 cell (~500 km edge). This enables efficient time-range and spatial filter push-down in Apache Sedona and DuckDB.

## Input formats

### NMEA (`--format nmea`)

Raw `!AIVDM` sentences. Multi-part messages are assembled automatically. Timestamps are read from tag-block `c:` fields when present; `--date` is used as fallback.

Decoded message types:
- **Types 1/2/3** — Class A position report
- **Type 18** — Class B position report
- **Type 5** — Class A static & voyage data (vessel metadata)
- **Type 24** — Class B static data (vessel metadata)

### CSV (`--format csv`)

Pre-decoded CSV with a header row. Column names are matched case-insensitively. Accepted synonyms:

| Field | Accepted names |
|---|---|
| MMSI | `mmsi` |
| Timestamp | `timestamp`, `basedatetime`, `time`, `datetime` |
| Latitude | `lat`, `latitude` |
| Longitude | `lon`, `lng`, `longitude` |
| SOG | `sog` |
| COG | `cog` |
| Heading | `heading` |
| Nav status | `nav_status`, `navstatus`, `status` |
| ROT | `rot` |
| Message type | `msg_type`, `msgtype`, `type` |

### ZIP files

Both formats accept `.zip` archives. The first matching entry (by extension) is used automatically.

## Validation rules

| Rule | Details |
|---|---|
| MMSI range | Drop records outside `[100000000, 999999999]` |
| Coordinate bounds | Drop lat outside `[-90, 90]` or lon outside `[-180, 180]` |
| Null island | Drop `lat=0, lon=0` |
| SOG cap | Drop SOG > 102.2 knots |
| Deduplication | Drop duplicate `(mmsi, timestamp)` pairs — keep first |

## Track building

`TrackBuilder` groups validated positions by MMSI, then for each vessel:

1. **Sort** by timestamp.
2. **Downsample** — keep one position per 10-second window.
3. **Segment** — split a track on any of:
   - Time gap > 6 hours
   - Navigation-status change (anchored/moored ↔ underway)
   - Port call — ≥ 6 consecutive positions with SOG < 0.5 kn spanning ≥ 30 min
4. **Emit** — skip single-point segments; encode remaining segments as WKB `LineString`.

Each track record carries `voyage_id`, `start_time`, `end_time`, `point_count`, `avg_sog`, and `distance_nm` (haversine sum in nautical miles).

## Schemas

### positions (Avro → Parquet)

| Column | Type | Notes |
|---|---|---|
| `mmsi` | `long` | 9-digit vessel identifier |
| `timestamp_us` | `timestamp-micros` | UTC |
| `geometry` | `bytes` | WKB Point (lon, lat), WGS 84 |
| `sog` | `float?` | Speed over ground, knots |
| `cog` | `float?` | Course over ground, degrees |
| `heading` | `int?` | True heading 0–359; 511 = unavailable |
| `nav_status` | `int?` | 0 = underway, 1 = anchored, 5 = moored, … |
| `rot` | `float?` | Rate of turn |
| `msg_type` | `int?` | AIS message type (1/2/3/18) |

### vessels (Avro → Parquet)

| Column | Type | Notes |
|---|---|---|
| `mmsi` | `long` | |
| `imo` | `long?` | IMO number |
| `vessel_name` | `string?` | |
| `callsign` | `string?` | |
| `ship_type` | `int?` | ITU ship type code |
| `ship_type_desc` | `string?` | Human-readable ship type |
| `length_m` | `float?` | Bow + stern dimensions |
| `beam_m` | `float?` | Port + starboard dimensions |
| `draught_m` | `float?` | |
| `destination` | `string?` | |
| `last_seen_us` | `timestamp-micros?` | UTC timestamp of static message |

### tracks (Avro → Parquet)

| Column | Type | Notes |
|---|---|---|
| `mmsi` | `long` | |
| `voyage_id` | `string` | `<mmsi>_<start_epoch_sec>` |
| `geometry` | `bytes` | WKB LineString (lon, lat), WGS 84 |
| `start_time` | `timestamp-micros` | |
| `end_time` | `timestamp-micros` | |
| `point_count` | `int` | Points after downsampling |
| `avg_sog` | `float?` | Knots |
| `distance_nm` | `float?` | Haversine sum in nautical miles |

## GeoParquet compliance

All geometry columns use **WKB encoding** and carry the `geo` file-level metadata key per the [GeoParquet 1.1.0 spec](https://geoparquet.org/releases/v1.1.0/). CRS is EPSG:4326 (WGS 84). This makes the files readable by Apache Sedona, DuckDB spatial, QGIS, and geopandas without any extra configuration.

## Dependencies

| Purpose | Artifact | Version |
|---|---|---|
| NMEA parsing | `dk.tbsalling:aismessages` | 3.0.4 |
| Geometry (WKB) | `org.locationtech.jts:jts-core` | 1.20.0 |
| H3 indexing | `com.uber:h3` | 4.1.1 |
| Parquet writer | `org.apache.parquet:parquet-avro` | 1.14.1 |
| Hadoop FS | `org.apache.hadoop:hadoop-common` | 3.4.1 |
| Logging | `ch.qos.logback:logback-classic` | 1.4.14 |
| Lombok | `org.projectlombok:lombok` | 1.18.44 |

Java 11+. Built with Maven; the `maven-assembly-plugin` produces a fat JAR at `target/ais-io-*-jar-with-dependencies.jar`.

## Tests

```bash
mvn test
```

Unit tests cover `CsvParser`, `NmeaParser`, `Validator`, and `TrackBuilder`. Test fixtures live in `../test-files/`.
