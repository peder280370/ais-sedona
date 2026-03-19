# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

AIS maritime data pipeline: ingest raw AIS data → write GeoParquet → query with Apache Sedona → visualize on a map.

Three modules:
- **`ais-io`** — ingestion pipeline (fat JAR, Java 11+)
- **`ais-backend`** — REST API (Quarkus 3.9.5, Java 17)
- **`ais-frontend`** — map UI (React 18 + OpenLayers 9 + TypeScript)

## Architecture

```
Input (NMEA / CSV / DMA download)
  │
  ├── NmeaParser / CsvParser
  ├── Validator
  ├── GeoParquetWriter  →  data/positions/date=YYYY-MM-DD/h3_r3=<cell>/part-*.parquet
  ├── VesselMetadataWriter  →  data/vessels/part-*.parquet
  └── TrackBuilder + TrackWriter  →  data/tracks/date=YYYY-MM/part-*.parquet

Backend (Quarkus)
  ├── POST /api/import/file   — file upload, runs ais-io pipeline
  ├── POST /api/import/dma    — downloads from aisdata.ais.dk and imports
  ├── GET  /api/positions     — spatial + temporal query via Spark Connect
  ├── GET  /api/vessels       — vessel metadata lookup (cached)
  └── GET  /api/tracks        — voyage segments as GeoJSON

Frontend (React + OpenLayers)
  ├── OlMap        — full-screen map, OSM base layer
  ├── VesselLayer  — position markers (chevron, colored by ship type)
  ├── TracksLayer  — voyage LineStrings
  ├── FilterPanel  — date/time picker, play/pause, MMSI, vessel name search
  └── VesselPopup  — click-to-inspect vessel details
```

## Module Details

### ais-io

Key classes in `dk.carolus.ais.io.pipeline`:

| Class | Responsibility |
|---|---|
| `NmeaParser` | Decodes `!AIVDM`/`!AIVDO`, multi-sentence assembly, tag-block timestamps |
| `CsvParser` | Flexible column matching, streaming (`streamFull`) and batch (`parseFull`) modes |
| `Validator` | MMSI/coord/SOG/dedup checks; stateful with packed `long` dedup key |
| `GeoParquetWriter` | Writes GeoParquet 1.1.0 with WKB Point geometry, Snappy compressed |
| `VesselMetadataWriter` | One row per MMSI, latest-win merge |
| `TrackBuilder` | Downsamples (10 s), segments voyages, computes haversine distance |
| `TrackWriter` | Writes tracks as WKB LineString, monthly partitions |

`Main` supports NMEA, CSV, and DMA formats; auto-detects from extension; transparent ZIP handling.

### ais-backend

Key classes in `dk.carolus.ais.backend`:

| Class | Responsibility |
|---|---|
| `ImportResource` | REST endpoints for file upload and DMA import |
| `AisImportService` | Coordinates pipeline; 60 s pre-accumulation downsampling for CSV |
| `SedonaQueryService` | Spark Connect session, temp views, spatial SQL queries |
| `VesselCacheService` | In-memory cache for vessel metadata |

Spark Connect URL: `sc://localhost:15002` (or `sc://spark:15002` in Docker).
Data dir: `/data` (shared volume with Spark container).

### ais-frontend

| File | Responsibility |
|---|---|
| `App.tsx` | Root; owns `FilterState` and selected vessel |
| `OlMap.tsx` | Map init; composes layers and hooks |
| `FilterPanel.tsx` | Date picker, play/pause (5 min/5 s), MMSI input, name autocomplete |
| `hooks/usePositions.ts` | Fetches positions; AbortController-based cancellation |
| `hooks/useTracks.ts` | Fetches track GeoJSON |
| `hooks/useVesselSearch.ts` | Autocomplete against `/api/vessels` |
| `api/client.ts` | Generic fetch wrapper |

## Key Validation Rules

- MMSI: 100,000,000–999,999,999
- Lat: [-90, 90], Lon: [-180, 180]
- Reject null island (0, 0)
- SOG ≤ 102.2 kn
- Dedup on (mmsi, timestamp) — keep first

## Track Segmentation

Split a vessel's positions into a new voyage on:
- Time gap > 6 hours
- Nav status change (anchored/moored ↔ underway)
- Port call: ≥ 6 positions at SOG < 0.5 kn over ≥ 30 min

Voyage ID format: `<mmsi>_<start_epoch_seconds>`

## Dependencies

### ais-io (`pom.xml`)

| Purpose | Artifact | Version |
|---|---|---|
| NMEA parsing | `dk.tbsalling:aismessages` | 3.0.4 |
| Geometry | `org.locationtech.jts:jts-core` | 1.20.0 |
| H3 indexing | `com.uber:h3` | 4.1.1 |
| Parquet | `org.apache.parquet:parquet-avro` | 1.14.1 |
| Hadoop FS | `org.apache.hadoop:hadoop-client` | 3.4.1 |

### ais-backend (`pom.xml`)

| Purpose | Artifact | Version |
|---|---|---|
| Framework | `io.quarkus:quarkus-rest` | 3.9.5 |
| Spark Connect | `org.apache.spark:spark-connect-client-jvm_2.12` | 3.5.1 |
| Pipeline | `dk.carolus:ais-io` | 1.0-SNAPSHOT |
| Lombok | `org.projectlombok:lombok` | 1.18.38 |

### ais-frontend (`package.json`)

| Purpose | Package | Version |
|---|---|---|
| Map | `ol` | ^9.2.4 |
| UI | `react` / `react-dom` | ^18.3.1 |
| Build | `vite` | ^5.4.2 |

## Running Locally

```bash
# Full stack (recommended)
docker compose up

# Services:
#   Frontend:  http://localhost:3000
#   Backend:   http://localhost:8080
#   Spark UI:  http://localhost:4040
```

```bash
# Backend dev (hot reload)
cd ais-backend && mvn quarkus:dev

# Frontend dev (Vite, proxies /api/* to backend)
cd ais-frontend && npm run dev   # http://localhost:5173
```

## Building

```bash
cd ais-io      && mvn package -q                    # produces target/ais-io-*-jar-with-dependencies.jar
cd ais-backend && mvn package -DskipTests           # produces target/quarkus-app/quarkus-run.jar
cd ais-frontend && npm ci && npm run build          # produces dist/
```

## Ingestion CLI

```bash
./ais-io.sh input.nmea /data           # NMEA file
./ais-io.sh input.csv /data            # CSV file (auto-detected)
./ais-io.sh 2024-01-15 /data --format dma  # Download from aisdata.ais.dk
./ais-io.sh input.nmea /data --limit 1000  # Sample first 1000 lines
```

## Spark / Sedona Configuration

```conf
spark.serializer                   org.apache.spark.serializer.KryoSerializer
spark.kryo.registrator             org.apache.sedona.core.serde.SedonaKryoRegistrator
spark.sql.extensions               org.apache.sedona.sql.SedonaSqlExtensions
spark.sql.files.maxPartitionBytes  536870912
spark.sql.parquet.filterPushdown   true
spark.sql.parquet.mergeSchema      false
```

Sedona version in Docker: `sedona-spark-3.5_2.12:1.8.1`
