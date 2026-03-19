# ais-backend

REST API for importing and querying AIS maritime data. Built with [Quarkus](https://quarkus.io/) 3.32.1, it orchestrates the `ais-io` pipeline library for ingestion and connects to a remote Apache Spark / Sedona cluster for spatial queries.

## Architecture

```
Client
  │
  ├── POST /api/import/*  ──→  AisImportService
  │                               ├── ais-io: parse (NMEA/CSV)
  │                               ├── ais-io: validate & enrich
  │                               ├── ais-io: write GeoParquet (positions, vessels)
  │                               └── ais-io: build & write tracks
  │
  └── GET  /api/*         ──→  SedonaQueryService
                                  └── Spark Connect → Spark + Sedona SQL
                                        └── reads GeoParquet from shared /data volume
```

## Endpoints

### Import

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/import/file` | Upload an NMEA or CSV file for a given date |
| `POST` | `/api/import/dma` | Fetch and import from the Danish Maritime Authority for a given date |

**File upload parameters** (multipart form):

| Field | Type | Required | Description |
|---|---|---|---|
| `file` | file | yes | NMEA sentences or decoded CSV |
| `format` | string | no | `nmea` or `csv` (auto-detected if omitted) |
| `date` | string | no | `YYYY-MM-DD` (defaults to today) |

**DMA import parameters** (JSON body):

| Field | Type | Required | Description |
|---|---|---|---|
| `date` | string | yes | `YYYY-MM-DD` |

**Response** (`ImportResult`):

```json
{
  "positionsWritten": 42130,
  "vesselRecords": 317,
  "tracksBuilt": 89,
  "durationMs": 4812
}
```

**Examples:**

```bash
# Upload a CSV file
curl -X POST http://localhost:8080/api/import/file \
  -F "file=@ais-io/data/sample/aisdk-2026-03-14.zip" \
  -F "format=csv"

# Upload an NMEA file for a specific date
curl -X POST http://localhost:8080/api/import/file \
  -F "file=@data.nmea" \
  -F "format=nmea" \
  -F "date=2026-03-14"

# Import from the Danish Maritime Authority
curl -X POST http://localhost:8080/api/import/dma \
  -H "Content-Type: application/json" \
  -d '{"date":"2026-03-14"}'
```

### Positions

`GET /api/positions`

Query AIS position messages with optional spatial and temporal filters.

| Parameter | Type | Description |
|---|---|---|
| `minLon`, `minLat`, `maxLon`, `maxLat` | double | Bounding box filter |
| `mmsi` | long | Filter by vessel MMSI |
| `startDate` | string | Start timestamp (`YYYY-MM-DD` or `YYYY-MM-DD HH:MM:SS`) |
| `endDate` | string | End timestamp (`YYYY-MM-DD` or `YYYY-MM-DD HH:MM:SS`) |
| `limit` | integer | Max rows returned (default 1000) |

**Response**: array of position objects:

```json
[
  {
    "mmsi": 123456789,
    "ts": "2026-03-14 00:00:00.0",
    "geomWkt": "POINT (10.123 55.456)",
    "sog": 12.3,
    "cog": 275.0,
    "heading": 274,
    "navStatus": 0,
    "rot": 0.0,
    "msgType": 1
  }
]
```

**Examples:**

```bash
# All positions (up to 1000)
curl "http://localhost:8080/api/positions"

# Bounding box around Denmark
curl "http://localhost:8080/api/positions?minLon=8&minLat=54&maxLon=16&maxLat=58"

# Single vessel by MMSI
curl "http://localhost:8080/api/positions?mmsi=219006113"

# Date range with limit
curl "http://localhost:8080/api/positions?startDate=2026-03-14&endDate=2026-03-15&limit=50"

# Combined: bbox + date + limit
curl "http://localhost:8080/api/positions?minLon=8&minLat=54&maxLon=16&maxLat=58&startDate=2026-03-14&limit=100"
```

### Vessels

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/vessels` | Query vessel metadata by `mmsi` or `name` (partial, case-insensitive) |
| `GET` | `/api/vessels/{mmsi}` | Fetch a single vessel by MMSI |

**Response**: array of vessel objects:

```json
[
  {
    "mmsi": 123456789,
    "imo": 9876543,
    "vesselName": "NORDIC STAR",
    "callsign": "OXAB",
    "shipType": 70,
    "shipTypeDesc": "Cargo",
    "lengthM": 185.0,
    "beamM": 28.0,
    "draughtM": 9.5,
    "destination": "AARHUS",
    "lastSeen": "2026-03-14 10:23:45.0"
  }
]
```

**Examples:**

```bash
# All vessels
curl "http://localhost:8080/api/vessels"

# Search by name (partial, case-insensitive)
curl "http://localhost:8080/api/vessels?name=nordic"

# Lookup by MMSI
curl "http://localhost:8080/api/vessels/219006113"
curl "http://localhost:8080/api/vessels?mmsi=219006113"
```

### Tracks

`GET /api/tracks`

Query voyage segments as a GeoJSON FeatureCollection.

| Parameter | Type | Description |
|---|---|---|
| `minLon`, `minLat`, `maxLon`, `maxLat` | double | Spatial filter |
| `mmsi` | long | Filter by vessel MMSI |
| `startDate` | string | Start date (`YYYY-MM-DD`) |
| `endDate` | string | End date (`YYYY-MM-DD`) |

**Response**: GeoJSON FeatureCollection with LineString geometries.

**Examples:**

```bash
# All tracks
curl "http://localhost:8080/api/tracks"

# Tracks within a bounding box
curl "http://localhost:8080/api/tracks?minLon=8&minLat=54&maxLon=16&maxLat=58"

# Tracks for a specific vessel
curl "http://localhost:8080/api/tracks?mmsi=219006113"
```

### Health

`GET /q/health` — Quarkus SmallRye Health endpoint (liveness + readiness).

### API Documentation

| URL | Description |
|---|---|
| `GET /q/swagger-ui` | Interactive Swagger UI |
| `GET /q/openapi` | OpenAPI 3.0 JSON spec |

## Configuration

`src/main/resources/application.properties`:

```properties
# Directory where GeoParquet files are written and read from
ais.data.dir=/data

# Spark Connect server URL
spark.connect.url=sc://localhost:15002

# HTTP
quarkus.http.port=8080
quarkus.http.limits.max-body-size=2G

# Worker threads for blocking import endpoints
quarkus.vertx.worker-pool-size=4
```

Environment variables override properties (Quarkus convention, uppercased with `_` instead of `.`):

```
AIS_DATA_DIR=/data
SPARK_CONNECT_URL=sc://spark:15002
```

## Running locally

### With Docker Compose (recommended)

From the repository root:

```bash
# Build the fat JAR first
cd ais-backend && mvn package -DskipTests && cd ..

docker compose up
```

This starts:
- **spark** — Spark 4.0 with Sedona 1.8.1 and the Spark Connect server on port 15002
- **backend** — ais-backend on port 8080, sharing the `/data` volume with Spark

### Without Docker

Prerequisites: Java 17, a running Spark Connect server with Sedona.

```bash
mvn quarkus:dev
```

The dev server starts on `http://localhost:8080` with live reload.

## Building

```bash
# JVM mode (produces fast-jar layout in target/quarkus-app/)
mvn package

# Native executable (requires GraalVM)
mvn package -Pnative
```

The runnable JAR is `target/quarkus-app/quarkus-run.jar` (Quarkus fast-jar format). All required dependencies are in `target/quarkus-app/`.

## Testing

```bash
mvn test
```

Tests use Mockito to stub `AisImportService` and `SedonaQueryService`. A separate Spark Connect port (`sc://localhost:19999`) is configured for the test profile so tests do not require a live Spark cluster.

## Data layout

Shared with the `ais-io` library and readable by the Spark cluster:

```
/data
  positions/date=YYYY-MM-DD/h3_r3=<cell>/part-*.parquet
  vessels/part-*.parquet
  tracks/date=YYYY-MM/part-*.parquet
```

Positions are partitioned by date and H3 resolution-3 cell (~500 km), enabling efficient Sedona predicate pushdown for spatial and temporal range queries.

## Module relationships

```
ais-backend   (this module)
   └── depends on ais-io (sibling Maven module)
          └── uses: NmeaParser, CsvParser, Validator, GeoParquetWriter,
                    VesselMetadataWriter, TrackBuilder, TrackWriter
```
